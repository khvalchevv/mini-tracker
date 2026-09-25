"""Kyber DEX aggregator: quote + swap build + local signing + broadcast.

Public endpoints (no key):
  GET  https://aggregator-api.kyberswap.com/{chain}/api/v1/routes
  POST https://aggregator-api.kyberswap.com/{chain}/api/v1/route/build

Hot-wallet signing via eth-account. Broadcast via a public RPC (chain
metadata table below). Guard-rails: DEX_MAX_TX_USD, whitelist file
`dex_whitelist.json` — router + tokens only.

Env:
  DEX_PRIVATE_KEY   — 0x-prefixed hex, hot wallet
  DEX_MAX_TX_USD    — hard cap per swap (default 500)
  DEX_MAX_USD_PER_HOUR — cumulative cap (default 2000)

Chains supported here map 1:1 to KyberSwap's URL slug.
"""
import asyncio
import json
import logging
import os
import time
from decimal import Decimal

import aiohttp

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
WHITELIST_FILE = os.path.join(HERE, "dex_whitelist.json")

# our chain slug → Kyber path slug
KYBER_CHAIN = {
    "ethereum": "ethereum",
    "bsc": "bsc",
    "polygon": "polygon",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "avalanche": "avalanche",
    "fantom": "fantom",
    "linea": "linea",
    "scroll": "scroll",
    "blast": "blast",
    "zksync": "zksync",
    "mantle": "mantle",
    "cronos": "cronos",
    "polygon-zkevm": "polygon-zkevm",
}

# native gas token contract sentinel used by 1inch/kyber
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Canonical USDC per chain (6 decimals) — used as a stable quote leg
# for pricing tokens without touching more volatile stables.
USDC_BY_CHAIN = {
    "ethereum":  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "bsc":       "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "polygon":   "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
    "arbitrum":  "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "base":      "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "optimism":  "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
    "avalanche": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
    "linea":     "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
    "blast":     "0x4300000000000000000000000000000000000003",  # USDB actually — Blast's stable
}
USDC_DECIMALS = 6

# Alchemy chain-slug per our internal chain name (empty = no Alchemy support)
_ALCHEMY_SLUG = {
    "ethereum":  "eth-mainnet",
    "bsc":       "bnb-mainnet",
    "polygon":   "polygon-mainnet",
    "arbitrum":  "arb-mainnet",
    "optimism":  "opt-mainnet",
    "base":      "base-mainnet",
    "avalanche": "avax-mainnet",
    "linea":     "linea-mainnet",
    "scroll":    "scroll-mainnet",
    "blast":     "blast-mainnet",
    "zksync":    "zksync-mainnet",
}

# Fallback public RPCs (used only when ALCHEMY_KEY is unset)
DEFAULT_RPC = {
    "ethereum":  "https://ethereum-rpc.publicnode.com",
    "bsc":       "https://bsc-rpc.publicnode.com",
    "polygon":   "https://polygon-bor-rpc.publicnode.com",
    "arbitrum":  "https://arbitrum-one-rpc.publicnode.com",
    "optimism":  "https://optimism-rpc.publicnode.com",
    "base":      "https://base-rpc.publicnode.com",
    "avalanche": "https://avalanche-c-chain-rpc.publicnode.com",
    "fantom":    "https://rpc.ftm.tools",
    "linea":     "https://rpc.linea.build",
    "scroll":    "https://rpc.scroll.io",
    "blast":     "https://rpc.blast.io",
    "zksync":    "https://mainnet.era.zksync.io",
    "mantle":    "https://rpc.mantle.xyz",
    "cronos":    "https://evm.cronos.org",
}


_EXPLORER_URL = {
    "ethereum":  "https://etherscan.io/tx/",
    "bsc":       "https://bscscan.com/tx/",
    "polygon":   "https://polygonscan.com/tx/",
    "arbitrum":  "https://arbiscan.io/tx/",
    "optimism":  "https://optimistic.etherscan.io/tx/",
    "base":      "https://basescan.org/tx/",
    "avalanche": "https://snowtrace.io/tx/",
    "linea":     "https://lineascan.build/tx/",
    "scroll":    "https://scrollscan.com/tx/",
    "blast":     "https://blastscan.io/tx/",
    "zksync":    "https://era.zksync.network/tx/",
    "solana":    "https://solscan.io/tx/",
}


def tx_url(chain: str, tx_hash: str) -> str:
    base = _EXPLORER_URL.get(chain, "")
    if not tx_hash:
        return ""
    if tx_hash.startswith("0x") or chain != "solana":
        return base + tx_hash if base else tx_hash
    return base + tx_hash


def tx_link(chain: str, tx_hash: str, short: int = 12) -> str:
    """Return HTML link '<a href=...>0xabc...</a>'. Safe for TG parse_mode=HTML."""
    if not tx_hash:
        return ""
    url = tx_url(chain, tx_hash)
    disp = tx_hash if len(tx_hash) <= short else tx_hash[:short] + "…"
    if not url:
        return f"<code>{disp}</code>"
    return f'<a href="{url}"><code>{disp}</code></a>'


def _alchemy_key() -> str:
    return os.getenv("ALCHEMY_KEY", "").strip()


def _alchemy_rpc(chain: str) -> str | None:
    slug = _ALCHEMY_SLUG.get(chain)
    key = _alchemy_key()
    if slug and key:
        return f"https://{slug}.g.alchemy.com/v2/{key}"
    return None


def _load_whitelist() -> dict:
    """{ "routers": {chain: [addr,...]}, "tokens": {chain: [addr,...]} }"""
    try:
        with open(WHITELIST_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"routers": {}, "tokens": {}}
    except Exception as e:
        log.warning("dex_whitelist.json load err: %s", e)
        return {"routers": {}, "tokens": {}}


_wl = _load_whitelist()
_spend_hist: list[tuple[float, float]] = []           # [(ts, usd), ...]


def _spent_last_hour_usd() -> float:
    cutoff = time.time() - 3600
    return sum(u for t, u in _spend_hist if t >= cutoff)


def _under_hour_cap(usd: float) -> bool:
    cap = float(os.getenv("DEX_MAX_USD_PER_HOUR", "2000"))
    return _spent_last_hour_usd() + usd <= cap


def _under_tx_cap(usd: float) -> bool:
    # Default MUST match executor.py's pre-clamp default (1000). They
    # disagreed (500 here) — harmless only while .env pins the value.
    # Unset it and the clamp would produce $980, which this function
    # then refused at $500, stranding the position exactly the way the
    # FLOCK swap was stranded.
    cap = float(os.getenv("DEX_MAX_TX_USD", "1000"))
    return usd <= cap


def _record_spend(usd: float):
    _spend_hist.append((time.time(), usd))


_HUB_RPC_CHAINS = {
    "ethereum", "bsc", "polygon", "arbitrum", "optimism", "base", "avalanche",
    "linea", "scroll", "blast", "zksync",
}

# Minimum priority tip per chain. Module-level because BOTH senders need
# it: `send_token` applied these floors while the swap path in
# `sign_and_send` used a bare `w3.eth.gas_price`. On a quiet Ethereum
# `eth_gasPrice` returns roughly the base fee with a negligible tip, so
# swaps went out with ~zero priority and simply sat in the mempool —
# surfacing as "tx reverted or timed out" when the wait expired. A
# 35,629 ACX swap stalled exactly this way at 0.0555 gwei against a
# 0.0547 gwei base fee.
_MIN_TIP = {
    "ethereum":   800_000_000,      # 0.8 gwei
    "bsc":        500_000_000,
    "polygon": 30_000_000_000,
    "arbitrum":   100_000_000,
    "optimism":   100_000_000,
    "base":       100_000_000,
    "avalanche":  800_000_000,
}


def _floor_gas_price(w3, chain: str, gas_price: int) -> int:
    """Raise `gas_price` to base_fee*mult + the chain's minimum tip.

    Returns the input untouched when the node exposes no base fee
    (pre-1559 chains), so this can only ever raise the bid, never
    under-price a tx that would otherwise have gone through.
    """
    try:
        base_fee = (w3.eth.get_block("latest") or {}).get("baseFeePerGas")
    except Exception:
        base_fee = None
    if not base_fee:
        return gas_price
    tip = _MIN_TIP.get(chain, 500_000_000)
    mult = float(os.getenv("GAS_BASEFEE_MULT", "1.5"))
    return max(int(gas_price), int(base_fee * mult) + tip)


def _rpc_for(chain: str) -> str:
    """Priority order (with env flags):
    1) DEX_RPC_<CHAIN> override
    2) PublicNode (free/unlimited) if PREFER_PUBLIC_RPC=1
    3) arbitron hub if HUB_KEY set + USE_HUB_RPC=1
    4) Alchemy
    5) DEFAULT_RPC (publicnode) as last-resort
    """
    override = os.getenv(f"DEX_RPC_{chain.upper()}", "").strip()
    if override:
        return override
    prefer_public = os.getenv("PREFER_PUBLIC_RPC", "0") == "1"
    if prefer_public and chain in DEFAULT_RPC:
        return DEFAULT_RPC[chain]
    hub_key = os.getenv("HUB_KEY", "").strip()
    if hub_key and chain in _HUB_RPC_CHAINS \
            and os.getenv("USE_HUB_RPC", "0") == "1":
        base = os.getenv("HUB_BASE", "https://hub.arbitron.dev").rstrip("/")
        return f"{base}/rpc/{chain}"
    alc = _alchemy_rpc(chain)
    if alc:
        return alc
    return DEFAULT_RPC.get(chain, "")


_KYBER_PROXIES: list[str] = []


def _exec_proxies(proxies: list[str], exec_mode: bool) -> list[str]:
    """Split the proxy pool so execution never queues behind scanning.

    The hunter fires ~2,900 Kyber quotes per cycle (366 DEX tokens × 4
    ladder rungs × both sides) every ~20s. Sharing one pool meant a live
    trade competed with that firehose for the same rate-limited IPs: the
    PHA swap ground through 429s for seven minutes and by the time it
    landed a $48 edge had turned into a $5 loss.

    Execution gets an exclusive slice; scanning uses the rest and can
    afford to lose a quote.
    """
    if not proxies:
        return proxies
    share = float(os.getenv("KYBER_EXEC_PROXY_SHARE", "0.25"))
    cut = max(1, int(len(proxies) * share))
    return proxies[:cut] if exec_mode else (proxies[cut:] or proxies[:cut])


def _load_kyber_proxies() -> list[str]:
    global _KYBER_PROXIES
    if _KYBER_PROXIES:
        return _KYBER_PROXIES
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "proxies.txt")) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # format: host:port:user:pass  →  http://user:pass@host:port
                parts = line.split(":")
                if len(parts) == 4:
                    h, p, u, pw = parts
                    _KYBER_PROXIES.append(f"http://{u}:{pw}@{h}:{p}")
                elif len(parts) == 2:
                    _KYBER_PROXIES.append(f"http://{line}")
        log.info("dex: loaded %d Kyber proxies", len(_KYBER_PROXIES))
    except FileNotFoundError:
        pass
    return _KYBER_PROXIES


async def quote(chain: str, token_in: str, token_out: str,
                amount_in_wei: int, exec_mode: bool = False) -> dict | None:
    """Get best route + expected output. RACES direct + N proxies in
    parallel; first successful response wins, others cancelled. Beats
    sequential retry when a proxy is dead or rate-limits us."""
    k = KYBER_CHAIN.get(chain)
    if not k:
        return None
    url = (f"https://aggregator-api.kyberswap.com/{k}/api/v1/routes?"
           f"tokenIn={token_in}&tokenOut={token_out}&amountIn={amount_in_wei}"
           f"&gasInclude=true")
    proxies = _exec_proxies(_load_kyber_proxies(), exec_mode)
    import random as _rnd

    _last_status: dict = {}

    async def _one(proxy):
        tag = (proxy or "direct")[:30]
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, proxy=proxy,
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        _last_status[tag] = f"HTTP {r.status}"
                        return None
                    d = await r.json()
                    rs = (d.get("data") or {}).get("routeSummary")
                    if not rs:
                        _last_status[tag] = f"no routeSummary in body"
                    return rs
        except Exception as e:
            _last_status[tag] = f"{type(e).__name__}: {str(e)[:60]}"
            return None

    # Direct IP is persistently HTTP 403 (Cloudflare blocked us after
    # sustained load). Skip direct by default; env override to re-enable.
    # 6 proxy racers to compensate — at least one usually gets through
    # even when several flap TimeoutError.
    picks = _rnd.sample(proxies, min(6, len(proxies))) if proxies else []
    tasks = []
    if os.getenv("KYBER_TRY_DIRECT", "0") == "1" or not proxies:
        tasks.append(asyncio.create_task(_one(None)))
    for p in picks:
        tasks.append(asyncio.create_task(_one(p)))
    try:
        for coro in asyncio.as_completed(tasks, timeout=10):
            res = await coro
            if res:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return res
    except asyncio.TimeoutError:
        for t in tasks:
            if not t.done():
                t.cancel()
    # All racers returned None — surface WHY (helps debug transient issues)
    if _last_status:
        log.warning("kyber quote %s %s→%s ALL %d racers failed: %s",
                    chain, token_in[:8], token_out[:8], len(tasks),
                    " | ".join(f"{k}={v}"
                                 for k, v in list(_last_status.items())[:5]))
    return None


# cache token decimals (chain, contract) → int
_TOKEN_DECIMALS: dict[tuple[str, str], int] = {}


def _parse_decimals(route: dict) -> int | None:
    for k in ("tokenOut", "extraFee", "route"):
        v = route.get(k)
        if isinstance(v, dict) and v.get("decimals") is not None:
            try:
                return int(v["decimals"])
            except (TypeError, ValueError):
                pass
    return None


# (chain, contract) → [strike_count, muted_until_ts]. A token whose
# Kyber price is absurd vs the CEX reference is broken (wrong decimals,
# dust pool, honeypot) and stays broken. Re-quoting it every hunter
# cycle wasted ~1700 log lines/hour AND real Kyber quota, which fed the
# Cloudflare rate-limiting that broke healthy quotes.
_SANITY_REJECTS: dict[tuple[str, str], list] = {}
_SANITY_STRIKES = int(os.getenv("KYBER_SANITY_STRIKES", "3"))
_SANITY_MUTE_SEC = float(os.getenv("KYBER_SANITY_MUTE_SEC", "21600"))  # 6h


def _note_sanity_reject(chain: str, contract: str) -> int:
    """Record a sanity rejection; mute the pair once it hits the strike
    limit. Returns the running strike count."""
    k = (chain, contract.lower())
    rec = _SANITY_REJECTS.get(k)
    if rec is None:
        rec = [0, 0.0]
        _SANITY_REJECTS[k] = rec
    rec[0] += 1
    if rec[0] >= _SANITY_STRIKES:
        rec[1] = time.time() + _SANITY_MUTE_SEC
    return rec[0]


def _is_sanity_muted(chain: str, contract: str) -> bool:
    rec = _SANITY_REJECTS.get((chain, contract.lower()))
    if not rec:
        return False
    if rec[1] and time.time() < rec[1]:
        return True
    if rec[1]:                                   # mute expired — reset
        _SANITY_REJECTS.pop((chain, contract.lower()), None)
    return False


# chain → (usd_cost, fetched_ts). Live gas cost of one Kyber swap.
_GAS_COST_CACHE: dict[str, tuple[float, float]] = {}
_GAS_COST_TTL = float(os.getenv("GAS_COST_TTL_SEC", "120"))
# Typical Kyber aggregator swap: multi-hop, generous
_SWAP_GAS_UNITS = int(os.getenv("SWAP_GAS_UNITS", "300000"))
# A plain ERC-20 transfer (Direction B: alt from the hot wallet to the
# exchange deposit address).
_TRANSFER_GAS_UNITS = int(os.getenv("TRANSFER_GAS_UNITS", "65000"))
# FALLBACK ONLY — see `_native_usd`. This is the same stale-literal bug
# that broke gas refills: ETH 4000 against a market near 2400, POL 0.4
# against 0.097, AVAX 30 against 7.8. Used here it overstated the gas
# cost of every DEX ladder rung by 1.6–4×, and that fiction was
# subtracted from net before the profit floor.
_NATIVE_USD = {"ethereum": 4000, "base": 4000, "arbitrum": 4000,
               "optimism": 4000, "linea": 4000, "scroll": 4000,
               "blast": 4000, "zksync": 4000,
               "bsc": 520, "polygon": 0.4, "avalanche": 30}
_CHAIN_NATIVE = {"ethereum": "ETH", "base": "ETH", "arbitrum": "ETH",
                 "optimism": "ETH", "linea": "ETH", "scroll": "ETH",
                 "blast": "ETH", "zksync": "ETH",
                 "bsc": "BNB", "polygon": "POL", "avalanche": "AVAX"}
_NATIVE_LIVE: dict[str, tuple[float, float]] = {}      # asset → (usd, ts)
_NATIVE_LIVE_TTL = 600.0


def set_native_usd(asset: str, usd: float) -> None:
    """Feed a live native-asset price. The hunter already holds fresh
    Bitvavo prices for ETH/BNB/POL/AVAX every cycle — zero extra HTTP."""
    if usd and usd > 0:
        _NATIVE_LIVE[asset.upper()] = (float(usd), time.time())


def _native_usd(chain: str) -> float:
    asset = _CHAIN_NATIVE.get(chain)
    hit = _NATIVE_LIVE.get(asset) if asset else None
    if hit and time.time() - hit[1] < _NATIVE_LIVE_TTL:
        return hit[0]
    return float(_NATIVE_USD.get(chain, 4000))


def swap_gas_cost_usd(chain: str) -> float:
    """What one Kyber swap actually costs in gas on `chain`, right now.

    The alert sizing used a flat $5. On Ethereum at 0.09 gwei a swap
    costs ~$0.11, so every marginal opportunity was charged ~$4.90 of
    imaginary cost and dropped below the profit floor. On L2s the gap
    is larger still. Cached briefly — gas doesn't move that fast.
    """
    now = time.time()
    hit = _GAS_COST_CACHE.get(chain)
    if hit and now - hit[1] < _GAS_COST_TTL:
        return hit[0]
    usd = float(os.getenv("SWAP_GAS_FALLBACK_USD", "2.0"))
    try:
        w3 = _get_w3(chain)
        if w3:
            gp = w3.eth.gas_price                     # wei per gas unit
            usd = (_SWAP_GAS_UNITS * gp / 1e18) * _native_usd(chain)
    except Exception as e:
        log.debug("gas cost %s: %s", chain, e)
    # Clamp — a bad RPC read shouldn't make everything look free or
    # impossibly expensive.
    usd = max(0.05, min(usd, 60.0))
    _GAS_COST_CACHE[chain] = (usd, now)
    return usd


def transfer_gas_cost_usd(chain: str) -> float:
    """Gas for one ERC-20 transfer on `chain` — the Direction B leg that
    moves the alt from the hot wallet to the exchange. Scaled from the
    cached swap cost so it costs no extra RPC."""
    return swap_gas_cost_usd(chain) * (_TRANSFER_GAS_UNITS / max(_SWAP_GAS_UNITS, 1))


# (chain, contract) → True if code exists at that address, False if not.
# Contract addresses reach us from CoinGecko platform maps and exchange
# metadata, and both are sometimes wrong. ACX carried
# 0x44108f0223A3C3028feBb5f7fa73d84e6a53c8f8 — a plausible-looking
# address with ZERO bytes of code, 19 hex chars of shared prefix with
# the real token. Every quote for it returned Kyber's "token not found",
# every balance read returned 0, and the DEX cycle could never work.
_IS_CONTRACT: dict[tuple[str, str], bool] = {}


def is_contract(chain: str, address: str) -> bool | None:
    """True if `address` holds code on `chain`. None if we can't tell.

    Cached forever — code presence at an address does not change (a
    self-destruct would, but then quoting fails loudly anyway).
    """
    if not chain or not address:
        return False
    key = (chain, address.lower())
    hit = _IS_CONTRACT.get(key)
    if hit is not None:
        return hit
    try:
        from web3 import Web3 as _W3
        w3 = _get_w3(chain)
        if not w3:
            return None
        code = w3.eth.get_code(_W3.to_checksum_address(address))
        ok = len(code) > 0
        _IS_CONTRACT[key] = ok
        if not ok:
            log.error("NOT A CONTRACT: %s on %s has 0 bytes of code — "
                      "bad address from upstream metadata", address, chain)
        return ok
    except Exception as e:
        log.debug("is_contract %s/%s: %s", chain, address[:12], e)
        return None


async def usd_price(chain: str, token_contract: str,
                    usd_notional: float = 100.0,
                    reference_price_usd: float | None = None) -> dict | None:
    """Two-sided Kyber quote for the token — returns both:
      - price_buy_usd:  USD you PAY per 1 token if you buy on Kyber
                        (route USDC→token; higher = worse)
      - price_sell_usd: USD you GET per 1 token if you sell to Kyber
                        (route token→USDC; higher = better)

    `reference_price_usd` (optional) — anchor for sizing the SELL side.
    If given, we quote selling `usd_notional / reference_price` tokens
    (the amount you'd actually acquire on the OTHER leg for that USD
    budget at the market's price). Without it we fall back to the
    self-referential Kyber-buy amount, which under-samples liquidity for
    illiquid tokens.
    """
    usdc = USDC_BY_CHAIN.get(chain)
    if not usdc or not token_contract:
        return None
    # Skip tokens already proven broken — saves the Kyber round-trip
    # entirely (this is the quota that was starving healthy quotes).
    if _is_sanity_muted(chain, token_contract):
        return None
    # Refuse addresses that aren't contracts. Cheap (one cached eth_getCode)
    # and it kills a whole class of upstream-metadata errors before they
    # turn into "no Kyber route" mysteries and stranded positions.
    if is_contract(chain, token_contract) is False:
        return None

    buy_in_wei = int(usd_notional * (10 ** USDC_DECIMALS))
    cached_decimals = _TOKEN_DECIMALS.get((chain, token_contract.lower()))
    # Well-known stables — hardcode decimals so first quote works without RPC
    _KNOWN_DEC = {
        "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT ETH
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC ETH
    }
    if not cached_decimals:
        cached_decimals = _KNOWN_DEC.get(token_contract.lower())
    # Authoritative source: read `decimals()` from the ERC-20 contract via
    # web3. One RPC call per token, cached forever. Defaulting to 18 was
    # catastrophic for 6/8-dec tokens (price came out 10^12 too high).
    if not cached_decimals:
        try:
            w3 = _get_w3(chain)
            if w3:
                from web3 import Web3 as _W3
                _abi = [{"inputs": [], "name": "decimals",
                          "outputs": [{"name": "", "type": "uint8"}],
                          "stateMutability": "view", "type": "function"}]
                _c = w3.eth.contract(
                    address=_W3.to_checksum_address(token_contract),
                    abi=_abi)
                cached_decimals = int(_c.functions.decimals().call())
                _TOKEN_DECIMALS[(chain, token_contract.lower())] = cached_decimals
        except Exception as e:
            log.debug("decimals() RPC %s/%s: %s", chain, token_contract[:10], e)
    # If still None → REFUSE to quote (garbage-in-garbage-out is worse
    # than no quote at all; CROSS token showed $102B price bug otherwise).
    if not cached_decimals:
        return None

    # If we know decimals AND have a reference price → sell side can run
    # in parallel with buy side (both quotes independent, ~halves latency).
    if cached_decimals and reference_price_usd and reference_price_usd > 0:
        sell_in_tokens = usd_notional / reference_price_usd
        sell_in_wei = int(sell_in_tokens * (10 ** cached_decimals))
        buy_route, sell_route = await asyncio.gather(
            quote(chain, usdc, token_contract, buy_in_wei),
            quote(chain, token_contract, usdc, sell_in_wei),
        )
        decimals = cached_decimals
    else:
        buy_route = await quote(chain, usdc, token_contract, buy_in_wei)
        decimals = _parse_decimals(buy_route or {}) or cached_decimals
        # Kyber's routeSummary keeps tokenOut as ADDRESS STRING (not dict),
        # so _parse_decimals typically returns None for freshly seen tokens.
        # 99% of ERC-20 use 18; default to that so the quote isn't wasted.
        if decimals is None and buy_route:
            decimals = 18
        sell_route = None
        sell_in_tokens = None
        if decimals:
            if reference_price_usd and reference_price_usd > 0:
                sell_in_tokens = usd_notional / reference_price_usd
            elif buy_route:
                try:
                    tow = int(buy_route.get("amountOut") or "0")
                    if tow > 0:
                        sell_in_tokens = usd_notional / (usd_notional / (tow / (10 ** decimals)))
                except (TypeError, ValueError):
                    pass
            if sell_in_tokens and sell_in_tokens > 0:
                sell_in_wei = int(sell_in_tokens * (10 ** decimals))
                sell_route = await quote(chain, token_contract, usdc, sell_in_wei)

    price_buy = None
    if buy_route:
        try:
            token_out_wei = int(buy_route.get("amountOut") or "0")
            if token_out_wei > 0 and decimals:
                token_out = token_out_wei / (10 ** decimals)
                price_buy = usd_notional / token_out
        except (TypeError, ValueError):
            pass

    price_sell = None
    if sell_route and sell_in_tokens:
        try:
            usdc_out_wei = int(sell_route.get("amountOut") or "0")
            if usdc_out_wei > 0:
                usdc_out = usdc_out_wei / (10 ** USDC_DECIMALS)
                price_sell = usdc_out / sell_in_tokens
        except (TypeError, ValueError):
            pass

    if not price_buy and not price_sell:
        return None
    # SANITY: reject fantasy prices (wrong decimals, dust pool, honeypot).
    # If reference given and Kyber says >100x or <0.01x — decimals were
    # wrong OR pool is broken. Better to skip than propagate 10^12x arbs.
    if reference_price_usd and reference_price_usd > 0:
        for label, p in (("buy", price_buy), ("sell", price_sell)):
            if p and (p > reference_price_usd * 100 or p < reference_price_usd / 100):
                n = _note_sanity_reject(chain, token_contract)
                # Log the first few, then go quiet — 4 broken BSC
                # contracts produced 1700 identical WARNING lines in
                # one hour and kept burning Kyber quota every cycle.
                if n <= _SANITY_STRIKES:
                    log.warning("kyber sanity reject %s/%s %s: kyber=%.6g "
                                "ref=%.6g (%.1fx off) — likely wrong decimals"
                                "%s",
                                chain, token_contract[:12], label, p,
                                reference_price_usd, p / reference_price_usd,
                                (f" [strike {n}/{_SANITY_STRIKES} — muting "
                                 f"for {_SANITY_MUTE_SEC / 3600:.0f}h]"
                                 if n == _SANITY_STRIKES else ""))
                return None
    if decimals:
        _TOKEN_DECIMALS[(chain, token_contract.lower())] = decimals

    # mid (for identity comparisons only) — average of what we have
    mid = None
    if price_buy and price_sell:
        mid = (price_buy + price_sell) / 2
    else:
        mid = price_buy or price_sell

    return {
        "chain": chain,
        "price_usd": mid,                                        # legacy field
        "price_buy_usd": price_buy,
        "price_sell_usd": price_sell,
        "decimals": decimals,
        "url": f"https://kyberswap.com/swap/{KYBER_CHAIN.get(chain, chain)}"
               f"/{usdc}-to-{token_contract}",
    }


async def build_swap(chain: str, route_summary: dict, sender: str,
                     recipient: str, slippage_bps: int = 100,
                     exec_mode: bool = False) -> dict | None:
    """POST route_summary → returns {data (calldata), routerAddress, ...}.
    RACES direct + N proxies in parallel; logs real error body on failure
    so we can debug rate-limits/malformed payloads (was silently None)."""
    k = KYBER_CHAIN.get(chain)
    if not k:
        return None
    url = f"https://aggregator-api.kyberswap.com/{k}/api/v1/route/build"
    body = {
        "routeSummary": route_summary,
        "sender": sender,
        "recipient": recipient,
        "slippageTolerance": slippage_bps,
        "deadline": int(time.time()) + 600,
        "source": "mini-tracker",
    }
    # Kyber requires `x-client-id` on public endpoint in newer API versions;
    # missing header sometimes returns 400 with unhelpful body.
    headers = {"x-client-id": "mini-tracker"}

    async def _one(proxy):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=body, headers=headers, proxy=proxy,
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        txt = await r.text()
                        log.warning("kyber build %s status=%d via %s: %s",
                                    chain, r.status, (proxy or "direct")[:28],
                                    txt[:200])
                        return None
                    d = await r.json()
                    data = d.get("data")
                    if not data:
                        log.warning("kyber build %s empty data: %s",
                                    chain, str(d)[:200])
                    return data
        except Exception as e:
            log.debug("kyber build %s via %s: %s", chain,
                      (proxy or "direct")[:28], e)
            return None

    proxies = _exec_proxies(_load_kyber_proxies(), exec_mode)
    import random as _rnd
    # Skip direct (CF-blocked); 6 proxy racers. Same rationale as quote().
    picks = _rnd.sample(proxies, min(6, len(proxies))) if proxies else []
    tasks = []
    if os.getenv("KYBER_TRY_DIRECT", "0") == "1" or not proxies:
        tasks.append(asyncio.create_task(_one(None)))
    for p in picks:
        tasks.append(asyncio.create_task(_one(p)))
    try:
        for coro in asyncio.as_completed(tasks, timeout=12):
            res = await coro
            if res:
                for t in tasks:
                    if not t.done(): t.cancel()
                return res
    except asyncio.TimeoutError:
        for t in tasks:
            if not t.done(): t.cancel()
    return None


async def sign_and_send(chain: str, tx_data: dict) -> str | None:
    """Sign the built tx with DEX_PRIVATE_KEY and broadcast via public RPC.
    Returns tx hash on success, None on failure."""
    try:
        from eth_account import Account
        from web3 import Web3
    except ImportError:
        log.error("dex.sign_and_send needs `pip install eth-account web3`")
        return None
    pk = os.getenv("DEX_PRIVATE_KEY", "").strip()
    if not pk:
        log.error("DEX_PRIVATE_KEY not set")
        return None
    rpc = _rpc_for(chain)
    if not rpc:
        log.error("no RPC for %s", chain)
        return None

    acct = Account.from_key(pk)
    w3 = _get_w3(chain) or Web3(Web3.HTTPProvider(rpc))    # cached provider
    router = Web3.to_checksum_address(tx_data["routerAddress"])

    # whitelist enforce
    ok_routers = {a.lower() for a in _wl.get("routers", {}).get(chain, [])}
    if ok_routers and router.lower() not in ok_routers:
        log.error("router %s not in dex_whitelist for %s", router, chain)
        return None

    # Nonce: use "pending" so we account for our own unmined tx (approve
    # sent 1-2s earlier may still be in mempool from RPC's perspective).
    # Serialize per-chain — two concurrent swaps on same HW would race
    # the RPC and collide on the same nonce.
    async with _nonce_lock(chain):
        try:
            gas_price = w3.eth.gas_price
        except Exception:
            gas_price = 20_000_000_000
        # Floor it the same way send_token does, or the tx lands in the
        # mempool with no priority and never gets picked up.
        gas_price = _floor_gas_price(w3, chain, gas_price)
        # Retry on transient nonce/broadcast errors that resolve within
        # a few seconds (PublicNode state lag right after approve mined).
        _errors_transient = (
            "replacement transaction underpriced",
            "nonce too low",
            "already known",
            "known transaction",
            "ALREADY_EXISTS",
            "tx already in mempool",
        )
        last_err = None
        for attempt in range(3):
            try:
                nonce = w3.eth.get_transaction_count(acct.address, "pending")
            except Exception as e:
                last_err = f"nonce fetch: {e}"
                await asyncio.sleep(2)
                continue
            # Bump gas price on retry to beat any conflicting pending tx
            gp = int(gas_price * (1.0 + 0.3 * attempt))
            tx = {
                # `from` is MANDATORY for a meaningful estimate. Without
                # it the node simulates from 0x0 — no balance, no
                # allowance — so estimate_gas ALWAYS reverted, the
                # except-branch always fired, and every swap went out
                # with the blind 1.5M fallback. That killed the free
                # pre-flight check: all 25 observed TRANSFER_FROM_FAILED
                # reverts were detectable here before spending gas.
                "from": acct.address,
                "to": router,
                "value": (int(tx_data.get("amountIn", "0"))
                          if tx_data.get("tokenIn", "").lower() == NATIVE.lower()
                          else 0),
                "data": tx_data["data"],
                "nonce": nonce,
                "gasPrice": gp,
                "chainId": w3.eth.chain_id,
            }
            try:
                # Kyber router on Base/BSC uses multi-hop DELEGATECALL
                # chains that consume more gas than the estimate; keep a
                # 2× buffer and a floor.
                est = w3.eth.estimate_gas(tx)
                tx["gas"] = max(int(est * 2.0), 1_200_000)
            except Exception as e:
                # A real revert reason. Broadcasting now just burns gas
                # to reproduce it on-chain, so refuse — unless explicitly
                # overridden for a chain whose node estimates badly.
                reason = str(e)[:200]
                # A TRANSFER_FROM_FAILED here means our cached "already
                # approved" belief is wrong (dropped approve, reorg, or a
                # USDT-style token). Evict so the next attempt re-approves
                # instead of failing forever on a stale cache entry.
                if "TRANSFER_FROM_FAILED" in reason:
                    _tin = (tx_data.get("tokenIn") or "").lower()
                    for _k in [k for k in _ALLOWANCE_APPROVED
                               if len(k) >= 2 and k[0] == chain and k[1] == _tin]:
                        _ALLOWANCE_APPROVED.discard(_k)
                        log.warning("evicted stale allowance cache %s", _k)
                if os.getenv("SWAP_IGNORE_ESTIMATE_FAIL", "0") != "1":
                    log.warning("swap pre-flight REVERT %s→%s: %s "
                                "— not broadcasting",
                                tx_data.get("tokenIn", "")[:10],
                                tx_data.get("tokenOut", "")[:10], reason)
                    return None
                log.warning("swap pre-flight failed (%s) — broadcasting "
                            "anyway per SWAP_IGNORE_ESTIMATE_FAIL", reason)
                tx["gas"] = 1_500_000
            signed = acct.sign_transaction(tx)
            raw = (getattr(signed, "raw_transaction", None)
                   or getattr(signed, "rawTransaction", None))
            try:
                h = w3.eth.send_raw_transaction(raw)
                if attempt > 0:
                    log.warning("send_raw_transaction OK on retry #%d "
                                "nonce=%d gasPrice=%.2fgwei",
                                attempt + 1, nonce, gp / 1e9)
                return h.hex()
            except Exception as e:
                last_err = str(e)
                msg = last_err.lower()
                is_transient = any(t.lower() in msg for t in _errors_transient)
                log.warning("send_raw_transaction try %d/%d failed "
                            "(nonce=%d gp=%.2fgwei): %s%s",
                            attempt + 1, 3, nonce, gp / 1e9,
                            last_err[:200],
                            " → retry after 3s" if is_transient else "")
                if not is_transient:
                    return None
                await asyncio.sleep(3)                   # let RPC state settle
        log.warning("send_raw_transaction giving up after 3 tries: %s",
                    (last_err or "")[:200])
        return None


def hot_wallet_address() -> str | None:
    """Address derived from DEX_PRIVATE_KEY. Same wallet used for Kyber
    swaps AND as the intermediary receiver for exchange→wallet withdrawals."""
    pk = os.getenv("DEX_PRIVATE_KEY", "").strip()
    if not pk:
        return None
    try:
        from eth_account import Account
        return Account.from_key(pk).address
    except Exception:
        return None


HOT_WALLET_ADDRESS = hot_wallet_address()


async def wallet_token_balance(chain: str, token_contract: str) -> int | None:
    """Read the hot wallet's balance of `token_contract` on `chain`, in wei.
    Uses cached Web3 provider (was creating fresh HTTPProvider per call —
    wasteful + occasional connection churn under Alchemy 429)."""
    try:
        from web3 import Web3
    except ImportError:
        return None
    if not HOT_WALLET_ADDRESS:
        return None
    w3 = _get_w3(chain)                                    # cached provider
    if not w3:
        return None
    addr = Web3.to_checksum_address(HOT_WALLET_ADDRESS)
    try:
        if token_contract.lower() == NATIVE.lower():
            return w3.eth.get_balance(addr)
        erc20_abi = [{
            "name": "balanceOf", "type": "function", "stateMutability": "view",
            "inputs": [{"name": "owner", "type": "address"}],
            "outputs": [{"name": "", "type": "uint256"}],
        }]
        c = w3.eth.contract(address=Web3.to_checksum_address(token_contract),
                            abi=erc20_abi)
        return c.functions.balanceOf(addr).call()
    except Exception as e:
        log.debug("balance %s %s: %s", chain, token_contract, e)
        return None


# Per-chain nonce lock so concurrent sends never collide.
_NONCE_LOCKS: dict[str, asyncio.Lock] = {}


def _nonce_lock(chain: str) -> asyncio.Lock:
    lk = _NONCE_LOCKS.get(chain)
    if lk is None:
        lk = asyncio.Lock()
        _NONCE_LOCKS[chain] = lk
    return lk


async def send_token(chain: str, token_contract: str,
                     amount_wei: int, to_address: str) -> dict:
    """Sign+broadcast ERC20 transfer (or native send if token_contract==NATIVE)
    from HOT_WALLET_ADDRESS to `to_address` on `chain`.
    Returns {ok, tx_hash?, error?}. Serialized per-chain via nonce lock."""
    async with _nonce_lock(chain):
        return await _send_token_impl(chain, token_contract, amount_wei, to_address)


_CHAIN_ID_CACHE: dict[str, int] = {}                       # chain → chainId (constant)
_W3_CACHE: dict[str, "object"] = {}                         # chain → Web3 (reused)


def _get_w3(chain: str):
    from web3 import Web3
    if chain in _W3_CACHE:
        return _W3_CACHE[chain]
    rpc = _rpc_for(chain)
    if not rpc:
        return None
    # Inject X-Hub-Key header when routing through arbitron hub
    req_kwargs = {"timeout": 8}
    hub_key = os.getenv("HUB_KEY", "").strip()
    if hub_key and "hub.arbitron.dev" in rpc:
        req_kwargs["headers"] = {"X-Hub-Key": hub_key}
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs=req_kwargs))
    _W3_CACHE[chain] = w3
    return w3


async def _send_token_impl(chain: str, token_contract: str,
                            amount_wei: int, to_address: str) -> dict:
    import time as _time
    T0 = _time.time()
    try:
        from eth_account import Account
        from web3 import Web3
    except ImportError:
        return {"ok": False, "error": "install eth-account & web3"}
    pk = os.getenv("DEX_PRIVATE_KEY", "").strip()
    if not pk:
        return {"ok": False, "error": "DEX_PRIVATE_KEY not set"}
    w3 = _get_w3(chain)
    if not w3:
        return {"ok": False, "error": f"no RPC for {chain}"}
    acct = Account.from_key(pk)
    to = Web3.to_checksum_address(to_address)
    t1 = _time.time()
    # PARALLEL prep RPCs. `chainId` cached (constant per chain), fetched
    # once. nonce + gas fetched concurrently via run_in_executor because
    # web3.py sync calls block the event loop otherwise.
    loop = asyncio.get_event_loop()

    def _nonce():
        # `pending` state so back-to-back sends bump correctly
        return w3.eth.get_transaction_count(acct.address, "pending")

    def _chain_id():
        if chain in _CHAIN_ID_CACHE:
            return _CHAIN_ID_CACHE[chain]
        cid = w3.eth.chain_id
        _CHAIN_ID_CACHE[chain] = cid
        return cid

    def _pending_block():
        try:
            return w3.eth.get_block("pending").get("baseFeePerGas")
        except Exception:
            return None

    percentile = float(os.getenv("GAS_TIP_PCT", "50"))

    def _fee_hist():
        try:
            hist = w3.eth.fee_history(5, "latest", [percentile])
            tips = [int(r[0]) for r in (hist.get("reward") or []) if r]
            if tips:
                tips.sort()
                return tips[len(tips) // 2]                # median of 5 medians
        except Exception:
            pass
        return None

    nonce, chain_id, base_fee, tip_raw = await asyncio.gather(
        loop.run_in_executor(None, _nonce),
        loop.run_in_executor(None, _chain_id),
        loop.run_in_executor(None, _pending_block),
        loop.run_in_executor(None, _fee_hist),
    )
    t2 = _time.time()
    log.info("send_token %s prep(parallel): init=%.2fs nonce+chainId+gas=%.2fs",
             chain, t1-T0, t2-t1)
    # Smart gas: read fee_history over last 5 blocks, take 60th
    # percentile of priority tips. Enforces a per-chain min floor
    # (in case network is empty — RPC returns 0). Target: ~2-3 block
    # inclusion (~25s on ETH) — cheaper than "next block guaranteed"
    # but avoids stagnation.
    # Absolute minimum tip in wei per chain (safety floor when network
    # is quiet and RPC suggests 0).
    # Floors targeting NEXT-BLOCK inclusion. We read 90th-percentile
    # priority tip from last 5 blocks (top 10% of transactions land
    # next block) and take max with these safety floors.
    # Economy floors — median of recent blocks + these floors is enough
    # to land next-block 70-80% of the time. If tx sits > REPLACEMENT_
    # SEC unmined, we bump and resubmit (see replacement logic below).
    # Floors live at module scope (`_MIN_TIP`) so the swap path uses the
    # same numbers; a local copy here had already let the two drift.
    min_tip = _MIN_TIP.get(chain, 500_000_000)
    base_mult = float(os.getenv("GAS_BASEFEE_MULT", "1.5"))
    max_fee = None
    max_priority = None
    if base_fee:
        raw = tip_raw or 0
        max_priority = max(raw, min_tip)
        max_fee = int(base_fee * base_mult + max_priority)
    log.info("send_token %s: gas (base=%s tip_hist=%s tip_used=%s maxFee=%s)",
             chain, base_fee, tip_raw, max_priority, max_fee)
    if max_fee is None:
        try:
            gas_price = max(int(w3.eth.gas_price * 1.1), min_tip * 3)
        except Exception:
            gas_price = 20_000_000_000

    def _apply_gas(t: dict) -> dict:
        if max_fee is not None:
            t["maxFeePerGas"] = max_fee
            t["maxPriorityFeePerGas"] = max_priority
        else:
            t["gasPrice"] = gas_price
        return t

    if token_contract.lower() == NATIVE.lower():
        tx = _apply_gas({"to": to, "value": int(amount_wei), "nonce": nonce,
                         "chainId": chain_id, "gas": 21_000})
    else:
        erc20_abi = [{
            "name": "transfer", "type": "function",
            "inputs": [{"name": "to", "type": "address"},
                       {"name": "value", "type": "uint256"}],
            "outputs": [{"name": "", "type": "bool"}],
        }]
        c = w3.eth.contract(address=Web3.to_checksum_address(token_contract),
                            abi=erc20_abi)
        data = c.encode_abi("transfer", args=[to, int(amount_wei)])
        tx = _apply_gas({"to": Web3.to_checksum_address(token_contract),
                          "value": 0, "data": data, "nonce": nonce,
                          "chainId": chain_id})
        # estimate_gas in executor to keep the event loop free
        def _est_gas():
            try:
                probe = {k: v for k, v in tx.items()
                         if k not in ("maxFeePerGas", "maxPriorityFeePerGas")}
                probe["from"] = acct.address
                probe.setdefault("gasPrice", gas_price if max_fee is None else max_fee)
                return int(w3.eth.estimate_gas(probe) * 1.2)
            except Exception:
                return 120_000
        tx["gas"] = await loop.run_in_executor(None, _est_gas)
    t3 = _time.time()

    # Sign + broadcast off-loop (both are CPU-bound / blocking IO)
    def _sign_and_send():
        try:
            signed = acct.sign_transaction(tx)
            raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction", None)
            return w3.eth.send_raw_transaction(raw)
        except Exception as e:
            return e

    h_or_err = await loop.run_in_executor(None, _sign_and_send)
    if isinstance(h_or_err, Exception):
        return {"ok": False, "error": f"send: {h_or_err}"}
    h = h_or_err
    t4 = _time.time()
    log.info("send_token %s FINAL: build/sign+broadcast=%.2fs TOTAL=%.2fs → %s",
             chain, t4-t3, t4-T0, h.hex()[:20])
    return {"ok": True, "tx_hash": h.hex()}


# Native gas symbol per chain (for pricing/display)
NATIVE_SYMBOL = {
    "ethereum": "ETH",  "arbitrum": "ETH", "optimism": "ETH",
    "base": "ETH", "linea": "ETH", "scroll": "ETH", "blast": "ETH",
    "zksync": "ETH",
    "bsc": "BNB",
    "polygon": "POL",
    "avalanche": "AVAX",
    "fantom": "FTM",
    "mantle": "MNT",
    "cronos": "CRO",
}

# USDT contract per chain (USDC already in USDC_BY_CHAIN)
USDT_BY_CHAIN = {
    "ethereum":  "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "bsc":       "0x55d398326f99059ff775485246999027b3197955",
    "polygon":   "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
    "arbitrum":  "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    "optimism":  "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",
    "avalanche": "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7",
}

# BEP20 tokens on BSC follow the 18-decimal default; everywhere else
# USDC/USDT are 6-decimal.
_STABLE_DECIMALS = {
    ("ethereum",  "USDC"): 6,  ("ethereum",  "USDT"): 6,
    ("bsc",       "USDC"): 18, ("bsc",       "USDT"): 18,
    ("polygon",   "USDC"): 6,  ("polygon",   "USDT"): 6,
    ("arbitrum",  "USDC"): 6,  ("arbitrum",  "USDT"): 6,
    ("base",      "USDC"): 6,
    ("optimism",  "USDC"): 6,  ("optimism",  "USDT"): 6,
    ("avalanche", "USDC"): 6,  ("avalanche", "USDT"): 6,
}


_CG_CONTRACTS_CACHE: dict[str, dict[str, str]] = {}


def _cg_contracts_for_chain(chain: str) -> dict[str, str]:
    """{SYMBOL_UPPER: contract_lower} from CG coins cache for `chain`.
    Cached in memory. Used to verify tokens in the hot wallet aren't
    spam impostors of well-known symbols."""
    if chain in _CG_CONTRACTS_CACHE:
        return _CG_CONTRACTS_CACHE[chain]
    # CG platform naming vs our chain slugs
    cg_platform = {
        "ethereum": "ethereum",
        "bsc": "binance-smart-chain",
        "polygon": "polygon-pos",
        "arbitrum": "arbitrum-one",
        "optimism": "optimistic-ethereum",
        "base": "base",
        "avalanche": "avalanche",
        "linea": "linea",
        "scroll": "scroll",
        "blast": "blast",
        "zksync": "zksync",
    }.get(chain)
    if not cg_platform:
        _CG_CONTRACTS_CACHE[chain] = {}
        return {}
    try:
        cg_path = os.path.join(HERE, "cg_coins.json")
        with open(cg_path, encoding="utf-8") as f:
            data = json.load(f)
        out: dict[str, str] = {}
        for c in data.get("coins", []):
            if not isinstance(c, dict):
                continue
            sym = (c.get("symbol") or "").upper()
            addr = ((c.get("platforms") or {}).get(cg_platform) or "").lower()
            if sym and addr:
                # First-seen wins (CG list is sorted by market cap loosely)
                if sym not in out:
                    out[sym] = addr
        _CG_CONTRACTS_CACHE[chain] = out
        log.info("cg contracts for %s: %d symbols", chain, len(out))
        return out
    except Exception as e:
        log.warning("cg contracts load %s err: %s", chain, e)
        _CG_CONTRACTS_CACHE[chain] = {}
        return {}


async def _alchemy_snapshot_chain(session, chain: str, addr: str,
                                  price_map: dict[str, float],
                                  min_usd: float) -> list[tuple[str, float, float]]:
    """One EVM chain via Alchemy: native gas + auto-discover all ERC20
    tokens with non-zero balance. Returns list of (symbol, amount, usd),
    entries below `min_usd` filtered."""
    url = _alchemy_rpc(chain)
    if not url:
        return []
    entries: list[tuple[str, float, float]] = []
    # 1) native balance
    try:
        r = await session.post(url, json={"jsonrpc": "2.0", "method": "eth_getBalance",
                                          "params": [addr, "latest"], "id": 1})
        data = await r.json()
        wei = int(data.get("result", "0x0"), 16)
        sym = NATIVE_SYMBOL.get(chain, chain.upper())
        amt = wei / 1e18
        usd = amt * (price_map.get(sym) or 0.0)
        if usd >= min_usd:
            entries.append((sym, amt, usd))
    except Exception as e:
        log.debug("alchemy native %s err: %s", chain, e)
    # 2) alchemy_getTokenBalances → returns every ERC20 the address has touched
    try:
        r = await session.post(url, json={"jsonrpc": "2.0", "method": "alchemy_getTokenBalances",
                                          "params": [addr, "erc20"], "id": 2})
        data = await r.json()
        tok_list = (data.get("result") or {}).get("tokenBalances") or []
        # keep only non-zero balances
        held = [t for t in tok_list if t.get("tokenBalance")
                and int(t["tokenBalance"], 16) > 0]
        if not held:
            return entries
        # 3) fetch metadata (symbol+decimals) per contract
        batch = [{"jsonrpc": "2.0", "id": i,
                  "method": "alchemy_getTokenMetadata",
                  "params": [t["contractAddress"]]}
                 for i, t in enumerate(held)]
        r = await session.post(url, json=batch)
        meta_res = await r.json()
        if isinstance(meta_res, dict):
            meta_res = [meta_res]
        meta_by_id = {int(m.get("id")): (m.get("result") or {}) for m in meta_res if m}
        # Load CoinGecko platform → contract map so we can verify each
        # discovered token IS the real one for its symbol on this chain.
        # Optimism/BSC/etc are riddled with scam airdrops that spoof
        # popular tickers ("OP", "ARB", "USDT"…) — without verification
        # they'd inflate wallet totals by hundreds of $ of fake tokens.
        cg_contracts_by_sym = _cg_contracts_for_chain(chain)
        stables = {"USDC", "USDT", "DAI", "USDP", "FDUSD", "PYUSD", "BUSD"}
        for i, t in enumerate(held):
            m = meta_by_id.get(i) or {}
            dec = int(m.get("decimals") or 18)
            sym = (m.get("symbol") or "").upper() or t["contractAddress"][:8]
            contract = t["contractAddress"].lower()
            # Stables — accept only if contract matches our USDC/USDT map
            if sym in stables:
                usdc_addr = USDC_BY_CHAIN.get(chain, "").lower()
                usdt_addr = USDT_BY_CHAIN.get(chain, "").lower()
                if contract not in (usdc_addr, usdt_addr):
                    log.debug("spam-skip %s/%s: contract %s not in stable map",
                              chain, sym, contract)
                    continue
            else:
                # Verify against CG platforms — real contract for this
                # symbol on this chain. Skip if we know the truth and it
                # doesn't match (scam token). If CG unknown, keep it.
                cg_expected = cg_contracts_by_sym.get(sym)
                if cg_expected and cg_expected.lower() != contract:
                    log.debug("spam-skip %s/%s: contract %s != CG %s",
                              chain, sym, contract, cg_expected)
                    continue
            amt = int(t["tokenBalance"], 16) / (10 ** dec)
            if sym in stables:
                usd = amt
            else:
                usd = amt * (price_map.get(sym) or 0.0)
            if usd >= min_usd:
                entries.append((sym, amt, usd))
    except Exception as e:
        log.debug("alchemy tokens %s err: %s", chain, e)
    entries.sort(key=lambda e: -e[2])
    return entries


_SNAPSHOT_CACHE: dict = {"ts": 0, "min_usd": None, "data": None}
_SNAPSHOT_TTL = 60.0                                       # cache 60s


async def wallet_snapshot(price_map: dict[str, float] | None = None,
                          min_usd: float = 1.0,
                          force: bool = False) -> dict:
    """On-chain snapshot of the hot wallet across EVM chains (via
    Alchemy `alchemy_getTokenBalances` for auto-discovery) and Solana.
    Tokens worth < `min_usd` filtered.

    Cached for 60s per (min_usd) to avoid burning ~7k CU each time
    /balances or /rebalance polls. Pass force=True to bypass."""
    # 60s cache — /balances + /rebalance + auto-rebal watch used to fan
    # out fresh N-chain RPC bursts on every trigger.
    now = time.time()
    if (not force and _SNAPSHOT_CACHE["data"] is not None
            and _SNAPSHOT_CACHE["min_usd"] == min_usd
            and now - _SNAPSHOT_CACHE["ts"] < _SNAPSHOT_TTL):
        return _SNAPSHOT_CACHE["data"]
    price_map = {k.upper(): v for k, v in (price_map or {}).items()}
    out: dict = {"address": HOT_WALLET_ADDRESS, "chains": {},
                 "solana_address": None, "usd_estimate": 0.0}
    if not HOT_WALLET_ADDRESS or not _alchemy_key():
        return out
    chains = list(_ALCHEMY_SLUG.keys())
    total_usd = 0.0
    import asyncio as _aio
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        tasks = [_alchemy_snapshot_chain(sess, c, HOT_WALLET_ADDRESS, price_map, min_usd)
                 for c in chains]
        results = await _aio.gather(*tasks, return_exceptions=True)
    for chain, entries in zip(chains, results):
        if isinstance(entries, Exception) or not entries:
            continue
        out["chains"][chain] = entries
        total_usd += sum(e[2] for e in entries)

    # Solana (separate module, own RPC)
    try:
        import sol
        sol_entries = await sol.wallet_snapshot(price_map=price_map, min_usd=min_usd)
        if sol_entries:
            out["chains"]["solana"] = sol_entries
            out["solana_address"] = sol.HOT_WALLET_ADDRESS
            total_usd += sum(e[2] for e in sol_entries)
    except Exception as e:
        log.debug("sol snapshot err: %s", e)

    out["usd_estimate"] = total_usd
    # Populate 60s cache — subsequent calls within TTL reuse this snapshot
    _SNAPSHOT_CACHE["ts"] = time.time()
    _SNAPSHOT_CACHE["min_usd"] = min_usd
    _SNAPSHOT_CACHE["data"] = out
    return out


async def wait_receipt(chain: str, tx_hash: str, timeout_sec: int = 300) -> dict | None:
    """Poll for tx receipt. HARD timeout per RPC call — sync web3 can
    hang indefinitely on flaky RPC (PublicNode 403/no-response silently)
    so we wrap each get_transaction_receipt in a per-call wait_for."""
    w3 = _get_w3(chain)                                    # cached provider
    if not w3:
        return None
    loop = asyncio.get_event_loop()
    deadline = time.time() + timeout_sec

    def _get():
        try:
            return w3.eth.get_transaction_receipt(tx_hash)
        except Exception:
            return None

    while time.time() < deadline:
        try:
            r = await asyncio.wait_for(
                loop.run_in_executor(None, _get), timeout=8.0)
            if r:
                return dict(r)
        except (asyncio.TimeoutError, Exception):
            pass
        await asyncio.sleep(5)
    return None


# Cache: (chain, token, spender) → True once approved to MAX
_ALLOWANCE_APPROVED: set = set()

MAX_UINT256 = (1 << 256) - 1


async def _ensure_allowance(chain: str, token: str, spender: str,
                             owner: str, need_wei: int) -> bool:
    """Guarantee ERC-20 allowance(owner, spender) >= need_wei on chain.
    If short, sends approve(spender, MAX_UINT256) and waits for confirm.
    Cached per (chain, token, spender) — one approve serves forever."""
    key = (chain, token.lower(), spender.lower())
    if key in _ALLOWANCE_APPROVED:
        return True
    try:
        from web3 import Web3
    except ImportError:
        return False
    w3 = _get_w3(chain)
    if not w3:
        return False
    erc20_abi = [
        {"name": "allowance", "type": "function", "stateMutability": "view",
         "inputs": [{"name": "owner", "type": "address"},
                    {"name": "spender", "type": "address"}],
         "outputs": [{"name": "", "type": "uint256"}]},
        {"name": "approve", "type": "function", "stateMutability": "nonpayable",
         "inputs": [{"name": "spender", "type": "address"},
                    {"name": "value", "type": "uint256"}],
         "outputs": [{"name": "", "type": "bool"}]},
    ]
    c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=erc20_abi)
    try:
        current = c.functions.allowance(
            Web3.to_checksum_address(owner),
            Web3.to_checksum_address(spender),
        ).call()
    except Exception as e:
        log.warning("allowance read %s/%s: %s", chain, token[:10], e)
        return False
    if current >= need_wei:
        _ALLOWANCE_APPROVED.add(key)
        return True
    log.warning("APPROVE %s %s → spender %s (allowance %d < need %d)",
                chain, token[:10], spender[:10], current, need_wei)
    # Build approve tx to MAX_UINT256 (industry standard — one approve forever)
    from eth_account import Account
    pk = os.getenv("DEX_PRIVATE_KEY", "").strip()
    acct = Account.from_key(pk)
    # Serialize with every other send from this wallet. `sign_and_send`
    # and `_send_token_impl` both hold this lock; approve did not, so a
    # concurrent swap could claim the same nonce and one of the two got
    # dropped as "replacement underpriced".
    try:
        async with _nonce_lock(chain):
            nonce = w3.eth.get_transaction_count(acct.address, "pending")
            chain_id = _CHAIN_ID_CACHE.get(chain) or w3.eth.chain_id
            _CHAIN_ID_CACHE[chain] = chain_id
            tx = c.functions.approve(
                Web3.to_checksum_address(spender), MAX_UINT256
            ).build_transaction({
                "from": acct.address, "nonce": nonce, "chainId": chain_id,
                "gas": 80_000,       # approve ≈ 46-55k gas, buffer
                "gasPrice": w3.eth.gas_price,
            })
            signed = acct.sign_transaction(tx)
            raw = getattr(signed, "raw_transaction", None) \
                or getattr(signed, "rawTransaction", None)
            h = w3.eth.send_raw_transaction(raw)
            log.warning("approve tx sent %s: %s", chain, h.hex()[:20])
    except Exception as e:
        log.warning("approve broadcast %s/%s: %s", chain, token[:10], e)
        return False
    # Wait for confirmation (approve is fast, usually 1-2 blocks)
    receipt = await wait_receipt(chain, h.hex(), timeout_sec=90)
    if receipt and receipt.get("status") == 1:
        log.warning("approve CONFIRMED %s %s → spender %s",
                    chain, token[:10], spender[:10])
        _ALLOWANCE_APPROVED.add(key)
        return True
    log.warning("approve tx not confirmed %s: %s", chain, h.hex()[:20])
    return False


async def swap(chain: str, token_in: str, token_out: str,
               amount_in_wei: int, usd_estimate: float,
               slippage_bps: int = 100,
               preset_route: dict | None = None) -> dict:
    """End-to-end: quote → build → sign → broadcast → wait. Enforces
    tx + hourly caps and the token/router whitelist.
    Returns dict with {ok, tx_hash?, amount_out?, error?}."""
    if not _under_tx_cap(usd_estimate):
        return {"ok": False, "error": f"tx cap ${os.getenv('DEX_MAX_TX_USD', 500)} exceeded"}
    if not _under_hour_cap(usd_estimate):
        return {"ok": False, "error": f"hourly cap exceeded (${_spent_last_hour_usd():.0f} spent)"}

    # BALANCE PRE-CHECK — the router pulls tokens via transferFrom, which
    # reverts with "TransferHelper: TRANSFER_FROM_FAILED" when the wallet
    # is short. That revert costs real gas (~87k) and tells us nothing.
    # Observed: 15 straight reverts on EURC because HW held $188 USDC
    # while the ladder kept trying $1000 / $500 / $250. Check first.
    if token_in.lower() != NATIVE.lower():
        # `wallet_token_balance` returns None on ANY RPC error. The old
        # code skipped the guard in that case and broadcast blind — the
        # exact situation (flaky RPC) where a blind send is most likely
        # to revert. Treat an unreadable balance as fatal.
        try:
            have_wei = await wallet_token_balance(chain, token_in)
        except Exception as e:
            have_wei = None
            log.warning("swap balance pre-check %s/%s raised: %s",
                        chain, token_in[:10], e)
        if have_wei is None:
            log.warning("swap %s/%s: balance unreadable — refusing to "
                        "broadcast blind", chain, token_in[:10])
            return {"ok": False,
                    "error": "balance unreadable (RPC) — swap not attempted"}
        if have_wei < amount_in_wei:
            return {
                "ok": False,
                "error": (f"insufficient balance: have {have_wei} raw, "
                          f"need {amount_in_wei} raw "
                          f"({have_wei / amount_in_wei * 100:.1f}% of size)"),
                "have_wei": have_wei,
                "need_wei": amount_in_wei,
            }

    # token whitelist
    ok_tokens = {t.lower() for t in _wl.get("tokens", {}).get(chain, [])}
    for t in (token_in, token_out):
        if t.lower() == NATIVE.lower():
            continue
        if ok_tokens and t.lower() not in ok_tokens:
            return {"ok": False, "error": f"token {t[:10]}… not whitelisted for {chain}"}

    # Try to reuse a caller-supplied route (saves 1 full quote race —
    # ~500ms-8s at hot times, and eliminates the transient-failure gap
    # between caller's own quote and our internal one).
    # Require the amountIn to match EXACTLY: Kyber's route (pool choice,
    # splits) is amount-specific. Any drift ⇒ fall back to fresh quote.
    route = None
    if preset_route:
        try:
            if str(preset_route.get("amountIn")) == str(amount_in_wei):
                route = preset_route
                log.info("Kyber reuse preset_route %s→%s (amountIn=%s) — "
                         "skip internal quote",
                         token_in[:10], token_out[:10], amount_in_wei)
        except Exception:
            pass
    if not route:
        # Retry `quote` on transient Kyber failures (all racers None) —
        # aggregator/pool hiccups usually clear in 2-3s.
        for _q_try in range(3):
            route = await quote(chain, token_in, token_out, amount_in_wei,
                                exec_mode=True)
            if route: break
            log.warning("Kyber quote %s→%s attempt %d/3 returned None — "
                        "retry in 2s",
                        token_in[:10], token_out[:10], _q_try + 1)
            await asyncio.sleep(2)
        if not route:
            return {"ok": False, "error": "no Kyber route (3 attempts)"}

    try:
        from eth_account import Account
    except ImportError:
        return {"ok": False, "error": "install eth-account & web3"}
    pk = os.getenv("DEX_PRIVATE_KEY", "").strip()
    if not pk:
        return {"ok": False, "error": "DEX_PRIVATE_KEY not set"}
    sender = Account.from_key(pk).address

    # Retry `build_swap` on transient failures too (same reason as quote)
    built = None
    for _b_try in range(3):
        built = await build_swap(chain, route, sender, sender,
                                   slippage_bps=slippage_bps,
                                   exec_mode=True)
        if built: break
        log.warning("Kyber build %s→%s attempt %d/3 failed — retry in 2s",
                    token_in[:10], token_out[:10], _b_try + 1)
        await asyncio.sleep(2)
    if not built:
        return {"ok": False, "error": "Kyber build failed (3 attempts)"}
    # Kyber's build response carries only {data, routerAddress, amountIn,
    # amountOut} — no token fields. sign_and_send reads `tokenIn` to
    # decide whether to attach `value` (native swaps) and which allowance
    # cache key to evict on TRANSFER_FROM_FAILED; both were silently
    # dead because the key was always absent. Stamp them here.
    built["tokenIn"] = token_in
    built["tokenOut"] = token_out
    built.setdefault("amountIn", str(amount_in_wei))

    # ERC-20 APPROVE: Kyber router uses transferFrom to pull our tokens.
    # Without prior allowance, the swap reverts with
    # "TransferHelper: TRANSFER_FROM_FAILED". Check allowance and approve
    # to MAX once per (chain, token, router). Cheap: 30-60k gas one-off.
    if token_in.lower() != NATIVE.lower():
        try:
            router_addr = built.get("routerAddress")
            if router_addr:
                allow_ok = await _ensure_allowance(chain, token_in, router_addr,
                                                    sender, amount_in_wei)
                if not allow_ok:
                    return {"ok": False, "error": "approve failed / timeout"}
        except Exception as e:
            log.warning("ensure_allowance %s/%s: %s", chain, token_in[:10], e)
            return {"ok": False, "error": f"approve err: {e}"}

    tx_hash = await sign_and_send(chain, built)
    if not tx_hash:
        return {"ok": False, "error": "broadcast failed"}
    # DON'T record spend on broadcast — 3× slippage-retries in phase_buy
    # would triple-count $1000 each and exhaust $5k/hr cap in one session.
    # Record ONLY when tx actually succeeded on-chain.
    receipt = await wait_receipt(chain, tx_hash)
    if receipt and receipt.get("status") == 1:
        _record_spend(usd_estimate)
        return {"ok": True, "tx_hash": tx_hash,
                "amount_out_wei": int(route.get("amountOut", "0"))}
    return {"ok": False, "tx_hash": tx_hash, "error": "tx reverted or timed out"}
