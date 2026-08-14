"""Fee tracking — trading fees per exchange (cached 1h) and
withdraw fees per (exchange, chain) via the capital-feed data.

Fees are subtracted from executable profit BEFORE the min-profit filter.
Failures fall back to conservative defaults so we never over-count profit.
"""
import logging
import time

import cex

log = logging.getLogger(__name__)

_trading_cache: dict[str, dict] = {}                   # eid -> {sym: {maker, taker}}
_trading_ts: dict[str, float] = {}
TRADING_TTL = 3600.0

# Conservative fallback taker fees when we can't fetch
_DEFAULT_TAKER = {
    "binance": 0.001,
    "gate": 0.002,
    "bitget": 0.001,
    "bitvavo": 0.0025,
    "kraken": 0.0026,
    "coinbase": 0.006,
}


async def taker_fee(eid: str, symbol: str) -> float:
    now = time.time()
    if now - _trading_ts.get(eid, 0) > TRADING_TTL:
        try:
            inst = cex._get(eid)
            inst.aiohttp_proxy = cex._pick_proxy()
            fees = await inst.fetch_trading_fees()
            _trading_cache[eid] = fees or {}
            _trading_ts[eid] = now
        except Exception as e:
            log.debug("trading fees %s: %s", eid, e)
    fees = _trading_cache.get(eid) or {}
    f = fees.get(symbol) or {}
    tk = f.get("taker")
    if isinstance(tk, (int, float)) and tk > 0:
        return float(tk)
    # per-exchange top-level default from ccxt sometimes lives under fees['trading']['taker']
    trading = fees.get("trading") if isinstance(fees, dict) else None
    if isinstance(trading, dict):
        tk = trading.get("taker")
        if isinstance(tk, (int, float)) and tk > 0:
            return float(tk)
    return _DEFAULT_TAKER.get(eid, 0.002)


def withdraw_fee_native(eid: str, base: str, chain: str | None) -> float | None:
    """Look up per-network withdraw fee (in BASE units) from cached
    capital feed. Returns None if unknown."""
    from chains import canonical
    target_chain = canonical(chain) if chain else None
    for n in cex.network_info(eid, base):
        if target_chain and canonical(n["network"]) != target_chain:
            continue
        fee = n.get("fee")
        if fee is None:
            continue
        try:
            return float(fee)
        except (TypeError, ValueError):
            continue
    return None


def total_fees_usd(buy_eid: str, buy_sym: str, buy_notional_usd: float,
                   sell_eid: str, sell_sym: str, sell_notional_usd: float,
                   base: str, transfer_chain: str | None,
                   base_price_usd: float,
                   taker_buy: float, taker_sell: float) -> dict:
    """Bundle all fee components into one USD number.
    Returns dict with breakdown + total."""
    buy_fee = buy_notional_usd * taker_buy
    sell_fee = sell_notional_usd * taker_sell
    wd_fee_native = withdraw_fee_native(buy_eid, base, transfer_chain)
    wd_fee_usd = (wd_fee_native or 0) * base_price_usd
    total = buy_fee + sell_fee + wd_fee_usd
    return {
        "buy_taker_pct": taker_buy,
        "sell_taker_pct": taker_sell,
        "buy_fee_usd": buy_fee,
        "sell_fee_usd": sell_fee,
        "withdraw_fee_native": wd_fee_native,
        "withdraw_fee_usd": wd_fee_usd,
        "total_usd": total,
    }
