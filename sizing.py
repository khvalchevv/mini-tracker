"""Full two-sided arb sizing.

Given a buy leg (exchange + symbol) and a sell leg, walk the ask side
of the buy book and the bid side of the sell book LEVEL BY LEVEL,
matching quantities until the marginal buy price (in USD) meets the
marginal sell price. Everything is normalised to USD via
`cex.get_fx_rate(quote_ccy)` so an EUR-quoted Bitvavo book compares
apples-to-apples with a USDT-quoted Binance book.

Return payload aims to be human-readable:
  qty              — total BASE that can be moved profitably
  notional_usd     — total USD you SPEND on the buy leg
  profit_usd       — sell_notional − buy_notional (before exchange fees)
  eff_spread_pct   — profit / buy_notional × 100
  last_buy_native  — worst (highest) ASK price you'd hit, in buy quote
  last_sell_native — worst (lowest) BID price you'd accept, in sell quote
  buy_quote / sell_quote — the currencies those native prices are in

None if either book is empty / target price already crossed.
"""
import logging
import random

import cex

log = logging.getLogger(__name__)


async def _fetch_book(eid: str, symbol: str, limit: int = 100):
    """Fetch order book with proxy rotation; retry once on failure."""
    inst = cex._get(eid)
    proxies = cex._PROXIES
    for attempt in range(2):
        inst.aiohttp_proxy = random.choice(proxies) if proxies else None
        try:
            return await inst.fetch_order_book(symbol, limit=limit)
        except Exception as e:
            log.debug("book %s %s (attempt %d): %s", eid, symbol, attempt + 1, e)
    return None


async def cross_match(buy_eid: str, buy_sym: str,
                      sell_eid: str, sell_sym: str) -> dict | None:
    ob_buy = await _fetch_book(buy_eid, buy_sym)
    ob_sell = await _fetch_book(sell_eid, sell_sym)
    if not ob_buy or not ob_sell:
        return None

    buy_quote = buy_sym.split("/")[1]
    sell_quote = sell_sym.split("/")[1]
    fx_buy = cex.get_fx_rate(buy_quote) or 1.0
    fx_sell = cex.get_fx_rate(sell_quote) or 1.0

    asks = ob_buy.get("asks") or []                              # (price, qty) in buy_quote
    bids = ob_sell.get("bids") or []                             # in sell_quote
    if not asks or not bids:
        return None

    top_ask_usd = asks[0][0] * fx_buy
    top_bid_usd = bids[0][0] * fx_sell
    if top_bid_usd <= top_ask_usd:                               # no cross → no profit
        return {
            "crossed": False,
            "top_ask_native": asks[0][0],
            "top_bid_native": bids[0][0],
            "buy_quote": buy_quote,
            "sell_quote": sell_quote,
        }

    total_qty = 0.0
    buy_usd = 0.0
    sell_usd = 0.0
    last_buy_native = asks[0][0]
    last_sell_native = bids[0][0]

    j = 0
    remaining_bid = bids[j][1]

    for ask_price, ask_qty in asks:
        ask_usd = ask_price * fx_buy
        left_ask = ask_qty
        while left_ask > 0 and j < len(bids):
            bid_price, _ = bids[j]
            bid_usd = bid_price * fx_sell
            if bid_usd <= ask_usd:                               # spread closed
                left_ask = -1                                    # sentinel: stop outer loop
                break
            take = min(left_ask, remaining_bid)
            total_qty += take
            buy_usd += take * ask_usd
            sell_usd += take * bid_usd
            last_buy_native = ask_price
            last_sell_native = bid_price
            left_ask -= take
            remaining_bid -= take
            if remaining_bid <= 0:
                j += 1
                if j < len(bids):
                    remaining_bid = bids[j][1]
        if left_ask < 0 or j >= len(bids):
            break

    if total_qty <= 0:
        return None
    avg_buy_usd = buy_usd / total_qty
    avg_sell_usd = sell_usd / total_qty
    return {
        "crossed": True,
        "qty": total_qty,
        "notional_usd": buy_usd,
        "profit_usd": sell_usd - buy_usd,
        "eff_spread_pct": (sell_usd - buy_usd) / buy_usd * 100.0,
        "avg_buy_usd": avg_buy_usd,
        "avg_sell_usd": avg_sell_usd,
        "last_buy_native": last_buy_native,
        "last_sell_native": last_sell_native,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
    }
