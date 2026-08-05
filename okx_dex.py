"""OKX Web3 DEX aggregator — public /api/v5/dex/market/candles endpoint.

Keyless. Per (chain, contract) we take the latest 1H close as price and
sum volUsd over the last 24 candles for 24h volume. Every request rotates
through the local proxy pool.
"""
import asyncio
import logging
import random

import aiohttp

log = logging.getLogger(__name__)

OKX_CANDLES = "https://web3.okx.com/api/v5/dex/market/candles"
OKX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://web3.okx.com/",
    "Origin": "https://web3.okx.com",
}

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


async def _fetch_one(session, proxy, chain: str, contract: str) -> dict | None:
    ci = CHAIN_INDEX.get(chain)
    if not ci:
        return None
    url = (f"{OKX_CANDLES}?chainIndex={ci}"
           f"&tokenContractAddress={contract}&bar=1H&limit=24")
    try:
        async with session.get(url, headers=OKX_HEADERS, proxy=proxy,
                               timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status != 200:
                return None
            d = await r.json()
    except Exception:
        return None
    data = d.get("data") or []
    if not data:
        return None
    try:
        price = float(data[0][4])                                # latest close
        vol = sum(float(c[6]) for c in data if len(c) > 6)       # ~24h volUsd
    except (ValueError, IndexError, TypeError):
        return None
    if price <= 0:
        return None
    return {
        "chain": chain,
        "price": price,
        "vol24h": vol,
        "contract": contract.lower(),
        "url": f"https://web3.okx.com/en/token/{ci}/{contract}",
    }


async def fetch_all(contracts: dict[str, str],
                    proxies: list[str] | None = None) -> dict[str, dict]:
    """contracts: {chain: contract_lower} -> {chain: {price,vol24h,url,contract}}."""
    if not contracts:
        return {}
    proxies = proxies or []
    async with aiohttp.ClientSession() as s:
        async def one(chain, contract):
            for _ in range(3):
                proxy = random.choice(proxies) if proxies else None
                r = await _fetch_one(s, proxy, chain, contract)
                if r:
                    return chain, r
            return chain, None

        results = await asyncio.gather(*(one(c, a) for c, a in contracts.items()))
    return {c: v for c, v in results if v}
