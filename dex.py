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

# public RPC per chain (env override: DEX_RPC_<CHAIN>)
DEFAULT_RPC = {
    "ethereum":  "https://eth.llamarpc.com",
    "bsc":       "https://bsc-dataseed1.binance.org",
    "polygon":   "https://polygon-rpc.com",
    "arbitrum":  "https://arb1.arbitrum.io/rpc",
    "optimism":  "https://mainnet.optimism.io",
    "base":      "https://mainnet.base.org",
    "avalanche": "https://api.avax.network/ext/bc/C/rpc",
    "fantom":    "https://rpc.ftm.tools",
    "linea":     "https://rpc.linea.build",
    "scroll":    "https://rpc.scroll.io",
    "blast":     "https://rpc.blast.io",
    "zksync":    "https://mainnet.era.zksync.io",
    "mantle":    "https://rpc.mantle.xyz",
    "cronos":    "https://evm.cronos.org",
}


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
    cap = float(os.getenv("DEX_MAX_TX_USD", "500"))
    return usd <= cap


def _record_spend(usd: float):
    _spend_hist.append((time.time(), usd))


def _rpc_for(chain: str) -> str:
    return os.getenv(f"DEX_RPC_{chain.upper()}") or DEFAULT_RPC.get(chain, "")


async def quote(chain: str, token_in: str, token_out: str,
                amount_in_wei: int) -> dict | None:
    """Get best route + expected output. Returns raw Kyber routeSummary
    or None if no route."""
    k = KYBER_CHAIN.get(chain)
    if not k:
        return None
    url = (f"https://aggregator-api.kyberswap.com/{k}/api/v1/routes?"
           f"tokenIn={token_in}&tokenOut={token_out}&amountIn={amount_in_wei}"
           f"&gasInclude=true")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                d = await r.json()
                return (d.get("data") or {}).get("routeSummary")
    except Exception as e:
        log.debug("kyber quote %s: %s", chain, e)
        return None


async def build_swap(chain: str, route_summary: dict, sender: str,
                     recipient: str, slippage_bps: int = 100) -> dict | None:
    """POST route_summary → returns {data (calldata), routerAddress, ...}."""
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
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=body,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    log.debug("kyber build %d", r.status)
                    return None
                d = await r.json()
                return d.get("data")
    except Exception as e:
        log.debug("kyber build %s: %s", chain, e)
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
    w3 = Web3(Web3.HTTPProvider(rpc))
    router = Web3.to_checksum_address(tx_data["routerAddress"])

    # whitelist enforce
    ok_routers = {a.lower() for a in _wl.get("routers", {}).get(chain, [])}
    if ok_routers and router.lower() not in ok_routers:
        log.error("router %s not in dex_whitelist for %s", router, chain)
        return None

    nonce = w3.eth.get_transaction_count(acct.address)
    try:
        gas_price = w3.eth.gas_price
    except Exception:
        gas_price = 20_000_000_000
    tx = {
        "to": router,
        "value": int(tx_data.get("amountIn", "0")) if tx_data.get("tokenIn", "").lower() == NATIVE.lower() else 0,
        "data": tx_data["data"],
        "nonce": nonce,
        "gasPrice": gas_price,
        "chainId": w3.eth.chain_id,
    }
    try:
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.2)
    except Exception:
        tx["gas"] = 500_000
    signed = acct.sign_transaction(tx)
    try:
        h = w3.eth.send_raw_transaction(signed.rawTransaction)
    except Exception as e:
        log.warning("send_raw_transaction failed: %s", e)
        return None
    return h.hex()


async def wait_receipt(chain: str, tx_hash: str, timeout_sec: int = 300) -> dict | None:
    try:
        from web3 import Web3
    except ImportError:
        return None
    rpc = _rpc_for(chain)
    if not rpc:
        return None
    w3 = Web3(Web3.HTTPProvider(rpc))
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = w3.eth.get_transaction_receipt(tx_hash)
            if r:
                return dict(r)
        except Exception:
            pass
        await asyncio.sleep(5)
    return None


async def swap(chain: str, token_in: str, token_out: str,
               amount_in_wei: int, usd_estimate: float,
               slippage_bps: int = 100) -> dict:
    """End-to-end: quote → build → sign → broadcast → wait. Enforces
    tx + hourly caps and the token/router whitelist.
    Returns dict with {ok, tx_hash?, amount_out?, error?}."""
    if not _under_tx_cap(usd_estimate):
        return {"ok": False, "error": f"tx cap ${os.getenv('DEX_MAX_TX_USD', 500)} exceeded"}
    if not _under_hour_cap(usd_estimate):
        return {"ok": False, "error": f"hourly cap exceeded (${_spent_last_hour_usd():.0f} spent)"}

    # token whitelist
    ok_tokens = {t.lower() for t in _wl.get("tokens", {}).get(chain, [])}
    for t in (token_in, token_out):
        if t.lower() == NATIVE.lower():
            continue
        if ok_tokens and t.lower() not in ok_tokens:
            return {"ok": False, "error": f"token {t[:10]}… not whitelisted for {chain}"}

    route = await quote(chain, token_in, token_out, amount_in_wei)
    if not route:
        return {"ok": False, "error": "no Kyber route"}

    try:
        from eth_account import Account
    except ImportError:
        return {"ok": False, "error": "install eth-account & web3"}
    pk = os.getenv("DEX_PRIVATE_KEY", "").strip()
    if not pk:
        return {"ok": False, "error": "DEX_PRIVATE_KEY not set"}
    sender = Account.from_key(pk).address

    built = await build_swap(chain, route, sender, sender, slippage_bps)
    if not built:
        return {"ok": False, "error": "Kyber build failed"}

    tx_hash = await sign_and_send(chain, built)
    if not tx_hash:
        return {"ok": False, "error": "broadcast failed"}
    _record_spend(usd_estimate)

    receipt = await wait_receipt(chain, tx_hash)
    if receipt and receipt.get("status") == 1:
        return {"ok": True, "tx_hash": tx_hash,
                "amount_out_wei": int(route.get("amountOut", "0"))}
    return {"ok": False, "tx_hash": tx_hash, "error": "tx reverted or timed out"}
