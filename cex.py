"""CEX price polling via ccxt.async_support.

One ccxt instance per exchange, cached; per cycle `fetch_tickers(symbols)`
returns all requested symbols in a single HTTP call (for exchanges that
support batch — most majors do). Falls back to per-symbol.
"""
import asyncio
import logging
import re

import ccxt.async_support as ccxt

log = logging.getLogger(__name__)

# Curated list — reliable public tickers, no auth needed.
SUPPORTED_EXCHANGES = [
    "binance", "bybit", "okx", "kucoin", "mexc",
    "gate", "bitget", "kraken", "coinbase", "bingx", "bitvavo",
]

EXCHANGE_PRETTY = {
    "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
    "kucoin": "KuCoin", "mexc": "MEXC", "gate": "Gate.io",
    "bitget": "Bitget", "htx": "HTX", "kraken": "Kraken",
    "coinbase": "Coinbase", "bingx": "BingX", "cryptocom": "Crypto.com",
    "bitvavo": "Bitvavo",
}

_instances: dict[str, ccxt.Exchange] = {}
_markets_loaded: set[str] = set()

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,15}[\-\/_]?[A-Z0-9]{2,15}$", re.IGNORECASE)


def pretty(eid: str) -> str:
    return EXCHANGE_PRETTY.get(eid, eid.upper())


def normalize_symbol(raw: str) -> str | None:
    """Accept 'ETH/USDT', 'ETH-USDT', 'eth_usdt', 'ETHUSDT' -> 'ETH/USDT'."""
    if not raw:
        return None
    s = raw.strip().upper().replace("_", "/").replace("-", "/")
    if "/" not in s:
        # try common quote splits
        for q in ("USDT", "USDC", "USD", "BTC", "ETH", "EUR"):
            if s.endswith(q) and len(s) > len(q):
                s = f"{s[:-len(q)]}/{q}"
                break
        else:
            return None
    parts = s.split("/")
    if len(parts) != 2 or not all(2 <= len(p) <= 15 for p in parts):
        return None
    return f"{parts[0]}/{parts[1]}"


def _get(eid: str) -> ccxt.Exchange:
    if eid not in _instances:
        cls = getattr(ccxt, eid)
        _instances[eid] = cls({"enableRateLimit": True, "timeout": 8000})
    return _instances[eid]


async def _ensure_markets(eid: str):
    if eid in _markets_loaded:
        return
    try:
        inst = _get(eid)
        await inst.load_markets()
        _markets_loaded.add(eid)
        log.info("cex: %s markets loaded (%d symbols)", eid, len(inst.symbols or []))
    except Exception as e:
        log.warning("cex: %s load_markets failed: %s", eid, e)


async def has_symbol(eid: str, symbol: str) -> bool:
    """True if the symbol is tradable on this exchange."""
    if eid not in SUPPORTED_EXCHANGES:
        return False
    await _ensure_markets(eid)
    inst = _get(eid)
    return symbol in (inst.symbols or [])


async def fetch_for(eid: str, symbols: list[str]) -> dict:
    """Return {symbol: price_usd_or_quote}. Uses batch fetch_tickers when possible."""
    if not symbols:
        return {}
    await _ensure_markets(eid)
    inst = _get(eid)
    out: dict = {}
    try:
        if inst.has.get("fetchTickers"):
            data = await inst.fetch_tickers(symbols)
            for sym, t in data.items():
                px = t.get("last") or t.get("close") or t.get("bid")
                if px:
                    out[sym] = float(px)
            return out
    except Exception as e:
        log.debug("cex: %s fetch_tickers failed (%s); per-symbol fallback", eid, e)

    # per-symbol fallback
    async def one(sym):
        try:
            t = await inst.fetch_ticker(sym)
            px = t.get("last") or t.get("close") or t.get("bid")
            if px:
                out[sym] = float(px)
        except Exception:
            pass

    await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)
    return out


async def close_all():
    for inst in _instances.values():
        try:
            await inst.close()
        except Exception:
            pass


def trading_url(eid: str, symbol: str) -> str:
    """Best-effort deep link to the exchange's trading page."""
    base, _, quote = symbol.partition("/")
    b, q = base.upper(), quote.upper()
    if eid == "binance":
        return f"https://www.binance.com/en/trade/{b}_{q}"
    if eid == "bybit":
        return f"https://www.bybit.com/trade/spot/{b}/{q}"
    if eid == "okx":
        return f"https://www.okx.com/trade-spot/{b.lower()}-{q.lower()}"
    if eid == "kucoin":
        return f"https://www.kucoin.com/trade/{b}-{q}"
    if eid == "mexc":
        return f"https://www.mexc.com/exchange/{b}_{q}"
    if eid == "gate":
        return f"https://www.gate.io/trade/{b}_{q}"
    if eid == "bitget":
        return f"https://www.bitget.com/spot/{b}{q}"
    if eid == "htx":
        return f"https://www.htx.com/trade/{b.lower()}_{q.lower()}"
    if eid == "kraken":
        return f"https://pro.kraken.com/app/trade/{b}-{q}"
    if eid == "coinbase":
        return f"https://www.coinbase.com/advanced-trade/spot/{b}-{q}"
    if eid == "bingx":
        return f"https://bingx.com/en/spot/{b}{q}"
    if eid == "bitvavo":
        return f"https://bitvavo.com/en/trade/{b}-{q}"
    if eid == "cryptocom":
        return f"https://crypto.com/exchange/trade/{b}_{q}"
    return "https://www.tradingview.com/symbols/" + b + q
