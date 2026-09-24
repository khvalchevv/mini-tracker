"""Alchemy WebSocket subscription for the hot wallet — sub-second
detection of incoming ERC20 transfers.

For each EVM chain we open one WS connection subscribed to the
ERC20 `Transfer(address,address,uint256)` topic filtered to logs
whose `to` field matches HOT_WALLET_ADDRESS. When a matching log
lands we:
  1. Update _last_arrival[(chain, token)] with (ts, tx_hash)
  2. Set _arrival_event[(chain, token)] so any awaiting coroutine
     is woken instantly.

Callers use `await wait_for_transfer(chain, token, min_amount, timeout)`
to get near-instant push (up to ~200ms from log emission) rather than
polling every 5s.
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict

import aiohttp

log = logging.getLogger(__name__)

# ERC20 Transfer topic0 — keccak256("Transfer(address,address,uint256)")
_TRANSFER_TOPIC = ("0xddf252ad1be2c89b69c2b068fc378daa"
                   "952ba7f163c4a11628f55a4df523b3ef")

# Alchemy chain-slug map. Trimmed to chains where Bitvavo actually
# has active WD/DEP pairs — L2s like scroll/blast/linea/zksync see near
# zero volume through this bot. Each extra chain adds one persistent WS
# subscription that idles-bills through 429 reconnect cycles.
_ALCHEMY_SLUG = {
    "ethereum":  "eth-mainnet",
    "bsc":       "bnb-mainnet",
    "polygon":   "polygon-mainnet",
    "arbitrum":  "arb-mainnet",
    "optimism":  "opt-mainnet",
    "base":      "base-mainnet",
    "avalanche": "avax-mainnet",
}

_HOT_WALLET: str | None = None
_ALCHEMY_KEY: str | None = None
_tasks: dict[str, asyncio.Task] = {}                  # chain → task
# (chain, token_contract_lower) → {"ts": epoch, "tx": hash, "amount": int, "from": addr}
_last_arrival: dict[tuple[str, str], dict] = {}
_arrival_event: dict[tuple[str, str], asyncio.Event] = defaultdict(asyncio.Event)
# Same shape but for MEMPOOL detection (sub-block latency)
_last_pending: dict[tuple[str, str], dict] = {}
_pending_event: dict[tuple[str, str], asyncio.Event] = defaultdict(asyncio.Event)
# Per-chain heartbeat — last WS message (event OR ping) timestamp
_last_ws_activity: dict[str, float] = {}

# ERC-20 transfer(address,uint256) selector — first 4 bytes of keccak
_TRANSFER_SELECTOR = "0xa9059cbb"

# Known CEX hot-wallet EVM addresses. Alchemy pending subscription
# filters on `fromAddress`; every WD from these wallets pushes to us
# ~1-3s after broadcast (vs ~12s wait for mined block).
_CEX_HOT_WALLETS_EVM = [
    # Binance
    "0xf977814e90da44bfa03b6295a0616a897441acec",   # Binance 14
    "0x28c6c06298d514db089934071355e5743bf21d60",   # Binance 15
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",   # Binance 16
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",   # Binance 17 (seen in T tx)
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f",   # Binance 18
    "0x9696f59e4d72e237be84ffd425dcad154bf96976",   # Binance 19
    "0x4d9ff58f31dbf87c58b40767c19af18b78dbeddb",   # Binance 20
    "0x161ba15a5f335c9f06bb5bbb0a9ce14076fbb645",   # Binance 21
    "0xd551234ae421e3bcba99a0da6d736074f22192ff",   # Binance 23
    "0x564286362092d8e7936f0549571a803b203aaced",   # Binance 25
    "0x0681d8db095565fe8a346fa0277bffde9c0edbbf",   # Binance 26
    "0xfe9e8709d3215310075d67e3ed32a380ccf451c8",   # Binance 27
    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe",   # Gate.io 1
    "0xd793281182a0e3e023116004778f45c29fc14f19",   # Gate.io 2
    "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c",   # Gate.io hot
    "0x7793cd85c11a924478d358d49b05b37e91b5810f",   # Gate.io hot 2
    # Bitvavo
    "0x30a2ebf10f34c6c4874b0bdd5740690fd2f3b70c",   # Bitvavo hot
    "0x489a8756c18c0b8b24ec2a2b9ff3d4d447f79bec",   # Bitvavo hot 2
]


def ws_health() -> dict[str, float]:
    """Return {chain: seconds since last WS message} — used by watchdog."""
    now = time.time()
    return {c: now - ts for c, ts in _last_ws_activity.items()}


def _pad_address(addr: str) -> str:
    """Pad ETH address to 32-byte topic form."""
    return "0x" + "0" * 24 + addr.lower().lstrip("0x")


def get_last_arrival(chain: str, token: str) -> dict | None:
    """Return the last recorded incoming transfer for this (chain, token)."""
    return _last_arrival.get((chain, token.lower()))


def find_any_since(chain: str, since_ts: float,
                    min_amount_raw: int = 0,
                    expected_qty: float | None = None,
                    qty_tolerance: float = 0.10) -> dict | None:
    """Return the most recent arrival on ANY contract on `chain` newer
    than `since_ts`. Used as a fallback when the caller does not know
    the exact ERC-20 contract the source exchange withdrew.

    When `expected_qty` is provided (base units, e.g. 1118 for 1118 AGI),
    only arrivals whose raw amount converted with 18 OR 6 decimals lands
    within `[expected_qty*(1-tol), expected_qty*(1+tol)]` are eligible —
    this prevents accidentally binding to a random USDT-dust transfer
    that MEV bots or refunds sometimes drop on the hot wallet mid-cycle."""
    best = None
    for (c, tok), arrival in _last_arrival.items():
        if c != chain:
            continue
        if arrival["ts"] < since_ts:
            continue
        if min_amount_raw and arrival["amount"] < min_amount_raw:
            continue
        if expected_qty is not None and expected_qty > 0:
            raw = arrival["amount"]
            lo = expected_qty * (1 - qty_tolerance)
            hi = expected_qty * (1 + qty_tolerance)
            candidates = (raw / 1e18, raw / 1e6, raw / 1e8)   # common decimals
            if not any(lo <= q <= hi for q in candidates):
                continue                                        # amount mismatch
        if best is None or arrival["ts"] > best["ts"]:
            best = {**arrival, "token": tok}
    return best


async def wait_for_pending(chain: str, token: str, timeout_sec: float = 60,
                             since_ts: float | None = None) -> dict | None:
    """Wait for a MEMPOOL-detected pending transfer to HW (sub-block
    latency, typically 1-3s from CEX broadcast). Only fires for txs
    from known CEX hot wallets — see _CEX_HOT_WALLETS_EVM."""
    key = (chain, token.lower())
    ref_ts = since_ts if since_ts is not None else time.time()
    ev = _pending_event[key]
    p = _last_pending.get(key)
    if p and p["ts"] >= ref_ts:
        return p
    ev.clear()
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        left = deadline - time.time()
        try:
            await asyncio.wait_for(ev.wait(), timeout=left)
        except asyncio.TimeoutError:
            return None
        p = _last_pending.get(key)
        if p and p["ts"] >= ref_ts:
            return p
        ev.clear()
    return None


async def wait_for_transfer(chain: str, token: str, timeout_sec: float = 300,
                             min_amount_raw: int = 0,
                             since_ts: float | None = None) -> dict | None:
    """Wait for a fresh incoming Transfer on (chain, token).
    If a matching arrival was RECEIVED BEFORE this call but AFTER
    `since_ts`, it's returned immediately (doesn't require a new event).

    `since_ts` — floor timestamp; arrivals older than this are ignored.
    Defaults to `time.time()` (only truly-new arrivals count)."""
    key = (chain, token.lower())
    ref_ts = since_ts if since_ts is not None else time.time()
    ev = _arrival_event[key]
    # First — check if there's already a matching arrival on record
    arrival = _last_arrival.get(key)
    if arrival and arrival["ts"] >= ref_ts and \
            (min_amount_raw == 0 or arrival["amount"] >= min_amount_raw):
        return arrival
    ev.clear()
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        left = deadline - time.time()
        try:
            await asyncio.wait_for(ev.wait(), timeout=left)
        except asyncio.TimeoutError:
            return None
        arrival = _last_arrival.get(key)
        if arrival and arrival["ts"] >= ref_ts and \
                (min_amount_raw == 0 or arrival["amount"] >= min_amount_raw):
            return arrival
        ev.clear()
    return None


async def _feed_chain(chain: str):
    slug = _ALCHEMY_SLUG.get(chain)
    if not slug or not _ALCHEMY_KEY or not _HOT_WALLET:
        return
    url = f"wss://{slug}.g.alchemy.com/v2/{_ALCHEMY_KEY}"
    padded = _pad_address(_HOT_WALLET)
    backoff = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(url, heartbeat=30,
                                            max_msg_size=8 * 1024 * 1024) as ws:
                    # Subscribe to Transfer events to our wallet
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 1,
                        "method": "eth_subscribe",
                        "params": ["logs", {
                            "topics": [_TRANSFER_TOPIC, None, padded],
                        }],
                    })
                    log.info("walletfeed: %s WS subscribed to hot wallet %s",
                             chain, _HOT_WALLET[:10] + "…")
                    backoff = 1.0
                    _last_ws_activity[chain] = time.time()
                    # Poll with short timeout so idle chains still bump
                    # the heartbeat — otherwise watchdog fires false-
                    # positive silence alerts when no Transfers happen.
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.receive(),
                                                          timeout=60)
                        except asyncio.TimeoutError:
                            _last_ws_activity[chain] = time.time()
                            continue
                        if msg.type in (aiohttp.WSMsgType.CLOSED,
                                        aiohttp.WSMsgType.CLOSING,
                                        aiohttp.WSMsgType.ERROR):
                            break
                        _last_ws_activity[chain] = time.time()
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                            params = d.get("params") or {}
                            result = params.get("result") or {}
                            token = (result.get("address") or "").lower()
                            if not token:
                                continue
                            data = result.get("data", "0x")
                            amount = int(data, 16) if data and data != "0x" else 0
                            tx = result.get("transactionHash", "")
                            topics = result.get("topics") or []
                            frm = ""
                            if len(topics) >= 3:
                                frm = "0x" + topics[1][-40:].lower()
                            key = (chain, token)
                            _last_arrival[key] = {
                                "ts": time.time(), "tx": tx,
                                "amount": amount, "from": frm,
                            }
                            _arrival_event[key].set()
                            log.info("walletfeed: %s ← %s tx=%s from %s amount=%d",
                                     chain, token[:10] + "…",
                                     tx[:20] + "…", frm[:10] + "…", amount)
                        except Exception as e:
                            log.debug("walletfeed parse: %s", e)
        except Exception as e:
            # 429 = Alchemy quota / rate limit → back off HARD (5 min)
            # so we don't hammer the endpoint. Cheap connection errors
            # keep the fast 1→30s ladder.
            is_429 = "429" in str(e)
            if is_429:
                wait = 300
                backoff = 30
            else:
                wait = backoff
                backoff = min(backoff * 2, 30)
            log.warning("walletfeed %s WS err: %s (reconnect %.0fs%s)",
                        chain, e, wait, " [429 slow-mode]" if is_429 else "")
            await asyncio.sleep(wait)


async def _pending_feed_chain(chain: str):
    """Alchemy `alchemy_pendingTransactions` subscription filtered by
    `fromAddress = known CEX hot wallets`. Decodes each tx's `input`; if
    it's an ERC-20 transfer(HW, amount) call, marks the pending arrival.

    Uses a SEPARATE WS connection from the mined-logs feed to avoid
    coupling their reconnect cycles. Silent-fails on non-Alchemy chains."""
    slug = _ALCHEMY_SLUG.get(chain)
    if not slug or not _ALCHEMY_KEY or not _HOT_WALLET:
        return
    url = f"wss://{slug}.g.alchemy.com/v2/{_ALCHEMY_KEY}"
    hw_lower = _HOT_WALLET.lower().lstrip("0x")           # 40 hex chars
    backoff = 2.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(url, heartbeat=30,
                                            max_msg_size=8 * 1024 * 1024) as ws:
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 2,
                        "method": "eth_subscribe",
                        "params": ["alchemy_pendingTransactions", {
                            "fromAddress": _CEX_HOT_WALLETS_EVM,
                            "hashesOnly": False,
                        }],
                    })
                    log.info("walletfeed: %s PENDING sub (mempool) for %d CEX wallets",
                             chain, len(_CEX_HOT_WALLETS_EVM))
                    backoff = 2.0
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=90)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type in (aiohttp.WSMsgType.CLOSED,
                                        aiohttp.WSMsgType.CLOSING,
                                        aiohttp.WSMsgType.ERROR):
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                            result = ((d.get("params") or {}).get("result") or {})
                            input_data = (result.get("input") or "0x").lower()
                            # ERC-20 transfer: 0xa9059cbb + 32B to + 32B amount
                            if not input_data.startswith(_TRANSFER_SELECTOR) or \
                                    len(input_data) < 10 + 128:
                                continue
                            to_field = input_data[10:74]         # padded HW addr
                            if not to_field.endswith(hw_lower):  # not to us
                                continue
                            amount = int(input_data[74:138], 16)
                            token_contract = (result.get("to") or "").lower()
                            tx_hash = result.get("hash", "")
                            key = (chain, token_contract)
                            _last_pending[key] = {
                                "ts": time.time(), "tx": tx_hash,
                                "amount": amount,
                                "from": (result.get("from") or "").lower(),
                            }
                            _pending_event[key].set()
                            log.info("walletfeed PENDING: %s ← %s tx=%s… amount=%d "
                                     "from %s (mempool)",
                                     chain, token_contract[:10] + "…",
                                     tx_hash[:20], amount,
                                     (result.get("from") or "")[:10] + "…")
                        except Exception as e:
                            log.debug("walletfeed pending parse: %s", e)
        except Exception as e:
            log.warning("walletfeed %s PENDING err: %s (reconnect %.0fs)",
                        chain, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def _feed_solana():
    """Solana WS: use `logsSubscribe` with `mentions` filter for our
    SOL wallet address. Any tx that includes our wallet pushes a log
    message; we mark arrival for 'solana' chain (contract left blank
    since Solana has native + SPL under the same event stream)."""
    key = os.getenv("ALCHEMY_KEY", "").strip()
    if not key:
        return
    try:
        import sol
        sol_addr = sol.HOT_WALLET_ADDRESS
    except Exception:
        return
    if not sol_addr:
        return
    url = f"wss://solana-mainnet.g.alchemy.com/v2/{key}"
    backoff = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(url, heartbeat=30) as ws:
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 1,
                        "method": "logsSubscribe",
                        "params": [{"mentions": [sol_addr]},
                                   {"commitment": "confirmed"}],
                    })
                    log.info("walletfeed: solana WS subscribed to hot wallet %s…",
                             sol_addr[:10])
                    backoff = 1.0
                    _last_ws_activity["solana"] = time.time()
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.receive(),
                                                          timeout=60)
                        except asyncio.TimeoutError:
                            _last_ws_activity["solana"] = time.time()
                            continue
                        if msg.type in (aiohttp.WSMsgType.CLOSED,
                                        aiohttp.WSMsgType.CLOSING,
                                        aiohttp.WSMsgType.ERROR):
                            break
                        _last_ws_activity["solana"] = time.time()
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                            params = d.get("params") or {}
                            result = ((params.get("result") or {}).get("value") or {})
                            sig = result.get("signature", "")
                            if result.get("err"):
                                continue                    # failed tx
                            # We don't know the mint/amount from logs alone —
                            # just mark generic solana arrival. Caller can
                            # query specific SPL balance to confirm delta.
                            key_g = ("solana", "*")
                            _last_arrival[key_g] = {
                                "ts": time.time(), "tx": sig,
                                "amount": 0, "from": "",
                            }
                            _arrival_event[key_g].set()
                            log.info("walletfeed: solana ← sig=%s", sig[:20] + "…")
                        except Exception as e:
                            log.debug("walletfeed sol parse: %s", e)
        except Exception as e:
            is_429 = "429" in str(e)
            if is_429:
                wait = 300; backoff = 30
            else:
                wait = backoff; backoff = min(backoff * 2, 30)
            log.warning("walletfeed solana WS err: %s (reconnect %.0fs%s)",
                        e, wait, " [429 slow-mode]" if is_429 else "")
            await asyncio.sleep(wait)


async def start(hot_wallet: str):
    """Boot WS subscriptions for every supported EVM chain + Solana."""
    global _HOT_WALLET, _ALCHEMY_KEY
    _HOT_WALLET = hot_wallet
    _ALCHEMY_KEY = os.getenv("ALCHEMY_KEY", "").strip()
    if not _ALCHEMY_KEY or not hot_wallet:
        log.warning("walletfeed: missing ALCHEMY_KEY or hot wallet — skipping")
        return
    for chain in _ALCHEMY_SLUG:
        if chain not in _tasks:
            _tasks[chain] = asyncio.create_task(_feed_chain(chain))
        # Mempool feed for sub-block latency (opt-in via env; default ON)
        if os.getenv("PENDING_MEMPOOL_FEED", "1") == "1":
            pend_key = f"{chain}_pending"
            if pend_key not in _tasks:
                _tasks[pend_key] = asyncio.create_task(_pending_feed_chain(chain))
    if "solana" not in _tasks:
        _tasks["solana"] = asyncio.create_task(_feed_solana())


async def stop():
    for t in _tasks.values():
        t.cancel()
    _tasks.clear()
