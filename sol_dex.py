"""Jupiter aggregator quotes for Solana SPL tokens — analogue of
`dex.usd_price` for EVM tokens via Kyber. Returns two-sided price
{buy, sell} in USD so the ladder-probe in bot.py can evaluate net.

Jupiter v6 quote API:
  GET https://quote-api.jup.ag/v6/quote
        ?inputMint=<mint>&outputMint=<mint>
        &amount=<lamports>&slippageBps=<bps>
Returns: {inAmount, outAmount, priceImpactPct, routePlan[], ...}
"""
import asyncio
import logging
import os
import aiohttp

log = logging.getLogger(__name__)

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDC_DECIMALS = 6

# cache mint → decimals (0.5s RPC call otherwise)
_MINT_DECIMALS: dict[str, int] = {}


def _proxy() -> str | None:
    """Reuse Kyber proxies list — same rotation strategy."""
    try:
        import dex as _dex
        proxies = _dex._load_kyber_proxies()
        if not proxies:
            return None
        import random
        return random.choice(proxies)
    except Exception:
        return None


async def _get_mint_decimals(mint: str) -> int | None:
    """Fetch SPL token decimals via Solana RPC. Cached forever."""
    if mint in _MINT_DECIMALS:
        return _MINT_DECIMALS[mint]
    if mint == USDC_MINT:
        _MINT_DECIMALS[mint] = USDC_DECIMALS
        return USDC_DECIMALS
    rpc = (os.getenv("SOLANA_RPC") or
           f"https://solana-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_KEY','')}")
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply",
                "params": [mint]}
    try:
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=6)) as s:
            async with s.post(rpc, json=payload) as r:
                d = await r.json()
                dec = ((d.get("result") or {}).get("value") or {}).get("decimals")
                if dec is not None:
                    _MINT_DECIMALS[mint] = int(dec)
                    return int(dec)
    except Exception as e:
        log.debug("sol decimals %s: %s", mint[:10], e)
    return None


async def _jup_quote(input_mint: str, output_mint: str,
                     amount_atoms: int) -> dict | None:
    """One Jupiter quote — RACE direct + N proxies in parallel, first
    successful response wins, rest cancelled. Way faster than sequential
    retry when the direct hop is 429-limited or a proxy is dead."""
    # New Jupiter endpoint (v1 replaced v6 in late 2025; old host dead)
    url = ("https://lite-api.jup.ag/swap/v1/quote"
           f"?inputMint={input_mint}&outputMint={output_mint}"
           f"&amount={amount_atoms}&slippageBps=100")

    async def _one(proxy: str | None):
        try:
            async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=6)) as s:
                async with s.get(url, proxy=proxy) as r:
                    if r.status != 200:
                        return None
                    return await r.json()
        except Exception as e:
            log.debug("jupiter %s: %s", (proxy or "direct")[:28], e)
            return None

    # Racers: direct + 3 different proxies (if available)
    proxies_all = []
    try:
        import dex as _dex
        proxies_all = _dex._load_kyber_proxies()
    except Exception:
        pass
    import random as _rnd
    picks = _rnd.sample(proxies_all, min(3, len(proxies_all)))
    tasks = [asyncio.create_task(_one(None))]
    for p in picks:
        tasks.append(asyncio.create_task(_one(p)))
    # Wait for the first non-None; cancel the rest
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
    return None


async def usd_price(mint: str, usd_notional: float = 100.0,
                     reference_price_usd: float | None = None) -> dict | None:
    """Two-sided Jupiter quote — same shape as dex.usd_price so bot.py
    can treat EVM and Solana uniformly.
    Returns {price_buy_usd, price_sell_usd, decimals, url, chain}."""
    if not mint:
        return None
    decimals = await _get_mint_decimals(mint)
    if decimals is None:
        return None
    buy_in_atoms = int(usd_notional * (10 ** USDC_DECIMALS))
    # Buy side: USDC → TOKEN. amountOut = token atoms. price = usd/token.
    # Sell side: TOKEN → USDC. Need to know how many tokens = usd_notional
    # at reference; if no reference, use buy result as pivot.
    buy_route, buy_price = None, None
    sell_route, sell_price = None, None

    if reference_price_usd and reference_price_usd > 0:
        sell_in_tokens = usd_notional / reference_price_usd
        sell_in_atoms = int(sell_in_tokens * (10 ** decimals))
        buy_route, sell_route = await asyncio.gather(
            _jup_quote(USDC_MINT, mint, buy_in_atoms),
            _jup_quote(mint, USDC_MINT, sell_in_atoms),
        )
    else:
        buy_route = await _jup_quote(USDC_MINT, mint, buy_in_atoms)
        sell_in_atoms = 0

    if buy_route:
        try:
            out_atoms = int(buy_route.get("outAmount") or 0)
            if out_atoms > 0:
                token_out = out_atoms / (10 ** decimals)
                buy_price = usd_notional / token_out
        except (TypeError, ValueError):
            pass

    if sell_route and sell_in_atoms > 0:
        try:
            usdc_out_atoms = int(sell_route.get("outAmount") or 0)
            if usdc_out_atoms > 0:
                usdc_out = usdc_out_atoms / (10 ** USDC_DECIMALS)
                sell_in_tokens = sell_in_atoms / (10 ** decimals)
                sell_price = usdc_out / sell_in_tokens
        except (TypeError, ValueError):
            pass

    if not buy_price and not sell_price:
        return None
    # Sanity: reject fantasy prices (wrong mint / dust pool / rug)
    if reference_price_usd and reference_price_usd > 0:
        for label, p in (("buy", buy_price), ("sell", sell_price)):
            if p and (p > reference_price_usd * 100 or
                       p < reference_price_usd / 100):
                log.warning("jupiter sanity reject %s %s: jup=%.6g ref=%.6g",
                            mint[:12], label, p, reference_price_usd)
                return None
    mid = (buy_price + sell_price) / 2 if buy_price and sell_price \
        else (buy_price or sell_price)
    return {
        "chain": "solana",
        "price_usd": mid,
        "price_buy_usd": buy_price,
        "price_sell_usd": sell_price,
        "decimals": decimals,
        "url": f"https://jup.ag/swap/USDC-{mint}",
    }
