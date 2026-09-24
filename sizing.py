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
import asyncio
import logging
import random

import cex

log = logging.getLogger(__name__)


async def _fetch_book(eid: str, symbol: str, limit: int = 50):
    """Fetch order book with proxy rotation. Up to 3 fresh proxies —
    ccxt timeout (5s) bails a stuck proxy fast.

    Returns (book_dict, None) on success, (None, "reason") on failure."""
    inst = cex._get(eid)
    if inst.symbols and symbol not in inst.symbols:
        return None, f"symbol {symbol} not listed on {eid} (delisted?)"
    last_err = None
    # Bumped 3→8 attempts because CF bot-fight rejects ~30-50% proxies;
    # each fresh proxy pick tries again. Exponential-ish backoff.
    import asyncio as _aio
    for attempt in range(8):
        proxy = cex._pick_proxy()
        inst.aiohttp_proxy = proxy
        try:
            r = await inst.fetch_order_book(symbol, limit=limit)
            cex.mark_proxy_ok(proxy)
            return r, None
        except Exception as e:
            last_err = e
            cex.mark_proxy_fail(proxy)
            log.debug("book %s %s (attempt %d): %s", eid, symbol, attempt + 1, e)
            if attempt < 7:
                await _aio.sleep(0.2 * (attempt + 1))
    return None, f"{eid} book fetch failed: {str(last_err)[:80]}"


async def book_side_notional(eid: str, symbol: str, side: str,
                             ref_price_usd: float, max_notional_usd: float,
                             tolerance_pct: float = 0.5) -> dict | None:
    """Walk one side of an exchange's book, accumulating notional until
    either we hit `max_notional_usd` OR the marginal price drifts more
    than `tolerance_pct` from ref_price_usd (i.e. the book gets thin).

    side="asks" — you'd BUY consuming asks. Marginal price ↑ from top.
    side="bids" — you'd SELL consuming bids. Marginal price ↓ from top.

    Returns {qty, notional_usd, avg_price_usd, last_price_native,
             quote} — or None if book unfetchable / empty."""
    ob, err = await _fetch_book(eid, symbol)
    if not ob:
        return None
    quote = symbol.split("/")[1]
    fx = cex.get_fx_rate(quote) or 1.0
    levels = ob.get(side) or []
    if not levels:
        return None
    tol = ref_price_usd * (tolerance_pct / 100.0)
    lo, hi = ref_price_usd - tol, ref_price_usd + tol
    qty = 0.0
    notional_usd = 0.0
    last_price_native = levels[0][0]
    for price, avail_qty in levels:
        px_usd = price * fx
        if side == "asks" and px_usd > hi:
            break                                                # asks too pricey
        if side == "bids" and px_usd < lo:
            break                                                # bids too weak
        want_notional = max_notional_usd - notional_usd
        if want_notional <= 0:
            break
        take_qty = min(avail_qty, want_notional / px_usd)
        qty += take_qty
        notional_usd += take_qty * px_usd
        last_price_native = price
        if notional_usd >= max_notional_usd:
            break
    if qty <= 0:
        return None
    return {"qty": qty, "notional_usd": notional_usd,
            "avg_price_usd": notional_usd / qty,
            "last_price_native": last_price_native, "quote": quote}


async def cross_match(buy_eid: str, buy_sym: str,
                      sell_eid: str, sell_sym: str,
                      max_notional_usd: float | None = None) -> dict | None:
    # Fetch both books in parallel — halves latency, cuts the "window
    # already closed" race window in half.
    # max_notional_usd caps the cumulative BUY-side spend; sizing stops
    # as soon as buy_usd would exceed it (so an arb with $10k depth
    # gets scaled down to the user-configured per-trade limit).
    (ob_buy, err_buy), (ob_sell, err_sell) = await asyncio.gather(
        _fetch_book(buy_eid, buy_sym),
        _fetch_book(sell_eid, sell_sym),
    )
    if not ob_buy or not ob_sell:
        return {"crossed": False, "error": err_buy or err_sell,
                "buy_quote": buy_sym.split("/")[1],
                "sell_quote": sell_sym.split("/")[1]}

    buy_quote = buy_sym.split("/")[1]
    sell_quote = sell_sym.split("/")[1]
    # Await the async version so a stale FX cache gets refreshed BEFORE
    # we compare bid/ask in USD. Sync get_fx_rate silently uses stale.
    fx_buy = await cex._quote_to_usd(buy_quote) or 1.0
    fx_sell = await cex._quote_to_usd(sell_quote) or 1.0

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

    capped = False
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
            # Respect per-trade cap: only take as much as fits under it
            if max_notional_usd is not None:
                room = max_notional_usd - buy_usd
                if room <= 0:
                    capped = True
                    left_ask = -1
                    break
                take_room = room / ask_usd
                if take_room < take:
                    take = take_room
                    capped = True
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
            if capped:
                break
        if left_ask < 0 or j >= len(bids) or capped:
            break

    if total_qty <= 0:
        return None
    avg_buy_usd = buy_usd / total_qty
    avg_sell_usd = sell_usd / total_qty
    return {
        "crossed": True,
        "capped": capped,
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
