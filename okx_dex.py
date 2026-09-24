"""OKX Web3 DEX prices — via api-hub proxy.

Uses `/priapi/v1/dx/market/v2/latest/info` which returns spot DEX price
in USD + volume + liquidity, directly from a token contract. No pool
picking, no candles, no rate-limit games — 20 req/s per key through hub.
"""
import asyncio
import logging
import os

import aiohttp

log = logging.getLogger(__name__)

HUB_BASE = os.getenv("HUB_BASE", "https://hub.arbitron.dev")
HUB_KEY = os.getenv("HUB_KEY", "")    # from .env — never hardcode a default
HUB_PATH = "/run/web3okx/priapi/v1/dx/market/v2/latest/info"

# our chain slug -> OKX chainIndex (EVM uses the EVM chainId).
CHAIN_INDEX = {
    "ethereum": "1", "bsc": "56", "arbitrum": "42161", "solana": "501",
    "polygon": "137", "base": "8453", "avalanche": "43114", "optimism": "10",
    "tron": "195", "sui": "784", "ton": "607", "aptos": "637", "zksync": "324",
    "linea": "59144", "scroll": "534352", "mantle": "5000", "blast": "81457",
    "sei": "1329", "cronos": "25", "fantom": "146", "gnosis": "100",
    "celo": "42220", "pulsechain": "369", "berachain": "80094",
    "ronin": "2020", "abstract": "2741",
}

# our chain slug -> OKX Web3 URL slug (nicer than chainId in URL).
# `https://web3.okx.com/en/token/<slug>/<contract>` — matches what
# OKX Web3 actually displays (e.g. `robinhood-chain` for RHC).
CHAIN_URL_SLUG = {
    "ethereum": "ethereum", "bsc": "bsc", "arbitrum": "arbitrum",
    "solana": "solana", "polygon": "polygon", "base": "base",
    "avalanche": "avalanche-c", "optimism": "optimism", "tron": "tron",
    "sui": "sui", "ton": "ton", "aptos": "aptos", "zksync": "zksync-era",
    "linea": "linea", "scroll": "scroll", "mantle": "mantle",
    "blast": "blast", "sei": "sei", "cronos": "cronos", "fantom": "sonic",
    "gnosis": "gnosis", "celo": "celo", "pulsechain": "pulsechain",
    "berachain": "berachain", "ronin": "ronin", "abstract": "abstract",
}


async def _fetch_one(session: aiohttp.ClientSession,
                       chain: str, contract: str) -> dict | None:
    ci = CHAIN_INDEX.get(chain)
    if not ci:
        return None
    url = f"{HUB_BASE}{HUB_PATH}?chainId={ci}&tokenContractAddress={contract}"
    try:
        async with session.get(url, headers={"X-Hub-Key": HUB_KEY},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            j = await r.json()
    except Exception as e:
        log.debug("okx_dex hub %s/%s err: %s", chain, contract[:12], e)
        return None
    data = ((j.get("data") or {}).get("data") or {})
    try:
        price = float(data.get("price") or 0)
    except (ValueError, TypeError):
        return None
    if price <= 0:
        return None
    return {
        "chain": chain,
        "price": price,
        "vol24h": float(data.get("volume") or 0),
        "liquidity": float(data.get("liquidity") or 0),
        "market_cap": float(data.get("marketCap") or 0),
        "change_5m": float(data.get("change5M") or 0),
        "change_1h": float(data.get("change1H") or 0),
        "change_24h": float(data.get("change") or 0),
        "trade_num": int(float(data.get("tradeNum") or 0)),
        "contract": contract.lower(),
        # Prefer readable slug (matches what OKX Web3 shows in the URL bar);
        # fall back to numeric chainId if we don't have a slug mapping.
        "url": (f"https://web3.okx.com/en/token/"
                f"{CHAIN_URL_SLUG.get(chain, ci)}/{contract}"),
    }


async def fetch_all(contracts: dict[str, str],
                     proxies: list[str] | None = None) -> dict[str, dict]:
    """contracts: {chain: contract_lower} -> {chain: {price, vol24h, liquidity, ...}}.
    `proxies` kept for signature compat but ignored — hub handles rate limits."""
    if not contracts:
        return {}
    async with aiohttp.ClientSession() as s:
        async def one(chain, contract):
            # 2 retries on transient hub 429/5xx
            for attempt in range(2):
                r = await _fetch_one(s, chain, contract)
                if r:
                    return chain, r
                await asyncio.sleep(0.3 * (attempt + 1))
            return chain, None

        results = await asyncio.gather(*(one(c, a) for c, a in contracts.items()))
    return {c: v for c, v in results if v}


async def fetch_price(chain: str, contract: str) -> float | None:
    """Convenience single-shot: USD price for one token contract."""
    async with aiohttp.ClientSession() as s:
        r = await _fetch_one(s, chain, contract)
    return r["price"] if r else None
