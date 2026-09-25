"""WebSocket top-of-book feeders for Bitvavo, Binance, Gate.

Maintains an in-memory cache of best bid/ask (+ qty) per (exchange,
symbol) that's continuously updated by dedicated WS reader coroutines.

Hunter and sizing read via `get_book_top(eid, sym)` — no HTTP, sub-ms
latency, always as fresh as the market itself.

REST fetch_book_top is kept as fallback for pairs the WS hasn't hit
yet (post-connect warmup) and for tokens that don't have a live pair
on the exchange we're watching.
"""
import asyncio
import json
import logging
import os
import random
import time

import aiohttp

log = logging.getLogger(__name__)


# --- Shared cache: {eid: {symbol_ccxt_form: {bid, ask, bid_qty, ask_qty, ts}}} -----
_cache: dict[str, dict[str, dict]] = {"binance": {}, "bitvavo": {}, "gate": {}}
_tasks: dict[str, asyncio.Task] = {}
_want: dict[str, set[str]] = {"binance": set(), "bitvavo": set(), "gate": set()}


def _proxy() -> str | None:
    """Return the static PRIVATE_PROXY URL (or None)."""
    raw = os.getenv("PRIVATE_PROXY", "").strip()
    if not raw:
        return None
    if raw.startswith("http"):
        return raw
    parts = raw.split(":")
    if len(parts) == 4:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    if len(parts) == 2:
        return f"http://{raw}"
    return None


def get_book_top(eid: str, symbol: str) -> dict | None:
    """Return the freshest {bid, ask, bid_qty, ask_qty, ts} for
    (eid, symbol) — symbol in ccxt form like 'BTC/USDT' or 'BTC/EUR'.
    None if the pair isn't in the cache yet."""
    return (_cache.get(eid) or {}).get(symbol)


def all_book_tops(eid: str) -> dict[str, dict]:
    """Snapshot dict of all cached symbols for `eid`."""
    return dict(_cache.get(eid) or {})


def is_fresh(eid: str, symbol: str, max_age_sec: float = 30.0) -> bool:
    e = get_book_top(eid, symbol)
    return bool(e and (time.time() - e["ts"]) <= max_age_sec)


# ================================================================
# BINANCE — per-symbol `<sym>@bookTicker` streams on one connection.
# ================================================================
#
# This used to open `/ws/!bookTicker`, the all-market stream. Binance
# removed that stream in 2023; the socket still CONNECTS (23 "connected"
# lines in the logs) but never delivers a frame, so the Binance cache
# stayed empty for the whole life of this code and every cycle fell
# back to a 316-symbol REST fetch_tickers through Cloudflare proxies.
# Measured: `!bookTicker` → 0 msgs in 12 s (proxy or not);
# `btcusdt@bookTicker` → 1,753 msgs in 12 s.
#
# Binance allows up to 1024 streams per connection and a SUBSCRIBE
# method on the raw `/ws` endpoint, so we subscribe exactly the pairs
# the hunter wants. Frames arrive unwrapped ({"s","b","B","a","A"}).

_BINANCE_MAX_STREAMS = 1024
_BINANCE_SUB_BATCH = 200                      # ≤5 control msgs/s allowed


def _binance_stream_map() -> dict[str, str]:
    """{'BTCUSDT': 'BTC/USDT', ...} for every wanted ccxt symbol — any
    quote, not just USDT, so USDC-only listings ride the socket too."""
    out: dict[str, str] = {}
    for sym in _want["binance"]:
        base, _, quote = sym.partition("/")
        if base and quote:
            out[f"{base}{quote}".upper()] = sym
    return out


async def _binance_reader():
    url = "wss://stream.binance.com:9443/ws"
    proxy = _proxy()
    delay = 1.0
    while True:
        try:
            raw_to_sym = _binance_stream_map()
            if not raw_to_sym:
                await asyncio.sleep(5)
                continue
            streams = sorted(f"{raw.lower()}@bookTicker" for raw in raw_to_sym)
            if len(streams) > _BINANCE_MAX_STREAMS:
                log.warning("wsfeed: binance wants %d streams, capping at %d",
                            len(streams), _BINANCE_MAX_STREAMS)
                streams = streams[:_BINANCE_MAX_STREAMS]
            timeout = aiohttp.ClientTimeout(total=None, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(url, proxy=proxy, heartbeat=30,
                                           max_msg_size=8 * 1024 * 1024) as ws:
                    for i in range(0, len(streams), _BINANCE_SUB_BATCH):
                        await ws.send_json({
                            "method": "SUBSCRIBE",
                            "params": streams[i:i + _BINANCE_SUB_BATCH],
                            "id": i // _BINANCE_SUB_BATCH + 1,
                        })
                        await asyncio.sleep(0.25)
                    log.info("wsfeed: binance bookTicker subscribed to %d streams",
                             len(streams))
                    delay = 1.0
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                            s = d.get("s")
                            if not s:                        # SUBSCRIBE ack etc.
                                continue
                            sym = raw_to_sym.get(s)
                            if not sym:
                                continue
                            _cache["binance"][sym] = {
                                "bid": float(d["b"]),
                                "ask": float(d["a"]),
                                "bid_qty": float(d.get("B") or 0),
                                "ask_qty": float(d.get("A") or 0),
                                "ts": time.time(),
                            }
                        except Exception as e:
                            log.debug("binance ws parse: %s", e)
        except Exception as e:
            log.warning("binance ws err: %s (reconnect in %.1fs)", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


# ================================================================
# BITVAVO — ticker channel (bestBid/bestAsk pushed on change).
# ================================================================
#
# Bitvavo's ticker channel is INCREMENTAL: after the initial snapshot,
# each frame carries only the side that changed — bestBid or bestAsk,
# almost never both. Measured live: 921 frames in 25 s, 456 bid-only,
# 436 ask-only, 15 with both. The old reader required both fields and
# dropped everything else, so the cache froze right after subscribing
# and the "live" Bitvavo price was really the 30-second REST snapshot.
# Merge each half into the cached entry instead; publish only once both
# sides are known, because every consumer reads e["bid"] and e["ask"].

_bv_partial: dict[str, dict] = {}             # sym → half-built entry


def _bitvavo_apply(sym: str, d: dict) -> None:
    bid = d.get("bestBid"); ask = d.get("bestAsk")
    if not bid and not ask:
        return
    entry = _cache["bitvavo"].get(sym) or _bv_partial.get(sym) or {}
    if bid:
        entry["bid"] = float(bid)
        entry["bid_qty"] = float(d.get("bestBidSize") or 0)
    if ask:
        entry["ask"] = float(ask)
        entry["ask_qty"] = float(d.get("bestAskSize") or 0)
    # A frame arrives whenever either side moves, so the entry as a whole
    # reflects the current top of book — stamp it fresh on every half.
    entry["ts"] = time.time()
    if "bid" in entry and "ask" in entry:
        _cache["bitvavo"][sym] = entry
        _bv_partial.pop(sym, None)
    else:
        _bv_partial[sym] = entry


async def _bitvavo_reader():
    url = "wss://ws.bitvavo.com/v2/"
    proxy = _proxy()
    delay = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(url, proxy=proxy, heartbeat=30,
                                           max_msg_size=8 * 1024 * 1024) as ws:
                    markets = sorted({s.replace("/", "-") for s in _want["bitvavo"]})
                    if not markets:
                        await asyncio.sleep(5)
                        continue
                    # Subscribe in batches (Bitvavo caps ~250 markets/msg)
                    for i in range(0, len(markets), 200):
                        chunk = markets[i:i + 200]
                        await ws.send_json({
                            "action": "subscribe",
                            "channels": [{"name": "ticker", "markets": chunk}],
                        })
                    log.info("wsfeed: bitvavo ticker subscribed to %d markets", len(markets))
                    delay = 1.0
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                            if d.get("event") != "ticker":
                                continue
                            m = d.get("market")                # "BTC-EUR"
                            if not m:
                                continue
                            _bitvavo_apply(m.replace("-", "/"), d)
                        except Exception as e:
                            log.debug("bitvavo ws parse: %s", e)
        except Exception as e:
            log.warning("bitvavo ws err: %s (reconnect in %.1fs)", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


# ================================================================
# GATE — spot.book_ticker channel (best bid/ask on every change).
# ================================================================

async def _gate_reader():
    url = "wss://api.gateio.ws/ws/v4/"
    proxy = _proxy()
    delay = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(url, proxy=proxy, heartbeat=30,
                                           max_msg_size=8 * 1024 * 1024) as ws:
                    payload = sorted({s.replace("/", "_")
                                      for s in _want["gate"]})
                    if not payload:
                        await asyncio.sleep(5)
                        continue
                    # Gate subscribes in one message; caps ~300 items/sub, batch if needed
                    for i in range(0, len(payload), 250):
                        chunk = payload[i:i + 250]
                        await ws.send_json({
                            "time": int(time.time()),
                            "channel": "spot.book_ticker",
                            "event": "subscribe",
                            "payload": chunk,
                        })
                    log.info("wsfeed: gate spot.book_ticker subscribed to %d markets", len(payload))
                    delay = 1.0
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                            if d.get("event") != "update":
                                continue
                            r = d.get("result") or {}
                            s = r.get("s")                    # "BTC_USDT"
                            if not s:
                                continue
                            sym = s.replace("_", "/")
                            _cache["gate"][sym] = {
                                "bid": float(r["b"]),
                                "ask": float(r["a"]),
                                "bid_qty": float(r.get("B") or 0),
                                "ask_qty": float(r.get("A") or 0),
                                "ts": time.time(),
                            }
                        except Exception as e:
                            log.debug("gate ws parse: %s", e)
        except Exception as e:
            log.warning("gate ws err: %s (reconnect in %.1fs)", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


# ================================================================
# Lifecycle
# ================================================================

def set_symbols(eid: str, symbols: set[str]):
    """Update the set of symbols we want streamed for `eid`. Only takes
    effect after the reader reconnects (which happens on next drop);
    call before starting for immediate effect."""
    _want[eid] = set(symbols)


async def start(binance_symbols: set[str] | None = None,
                bitvavo_symbols: set[str] | None = None,
                gate_symbols: set[str] | None = None):
    """Start reader tasks for whichever exchanges have symbols."""
    if binance_symbols is not None:
        _want["binance"] = set(binance_symbols)
    if bitvavo_symbols is not None:
        _want["bitvavo"] = set(bitvavo_symbols)
    if gate_symbols is not None:
        _want["gate"] = set(gate_symbols)
    if "binance" not in _tasks:
        _tasks["binance"] = asyncio.create_task(_binance_reader())
    if "bitvavo" not in _tasks and _want["bitvavo"]:
        _tasks["bitvavo"] = asyncio.create_task(_bitvavo_reader())
    if "gate" not in _tasks and _want["gate"]:
        _tasks["gate"] = asyncio.create_task(_gate_reader())


async def resubscribe(eid: str) -> None:
    """Restart the reader for `eid` so a changed `_want` takes effect now,
    not at the next accidental reconnect. Used when Bitvavo lists a new
    market mid-run — the socket set was fixed at start, so a token listed
    at 09:00 had no live price until the next restart."""
    t = _tasks.pop(eid, None)
    if t:
        t.cancel()
        try:
            await t
        except BaseException:
            pass
    reader = {"binance": _binance_reader, "bitvavo": _bitvavo_reader,
              "gate": _gate_reader}.get(eid)
    if reader and _want.get(eid):
        _tasks[eid] = asyncio.create_task(reader())


async def stop():
    for t in _tasks.values():
        t.cancel()
    _tasks.clear()
