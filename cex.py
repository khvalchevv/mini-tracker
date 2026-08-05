"""CEX price polling via ccxt.async_support.

One ccxt instance per exchange, cached; per cycle `fetch_tickers(symbols)`
returns all requested symbols in a single HTTP call (for exchanges that
support batch — most majors do). Falls back to per-symbol.

Every request rotates through the local proxy pool (loaded from
proxies.txt) — useful for exchanges Cloudflare-blocks the caller's IP
(e.g. Bitvavo from EU-restricted IPs). Each call retries with a fresh
proxy up to 3 times.
"""
import asyncio
import json
import logging
import os
import random
import re
import time

import aiohttp
import ccxt.async_support as ccxt

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
PROXIES_FILE = os.path.join(HERE, "proxies.txt")


def _load_proxies() -> list[str]:
    out = []
    try:
        with open(PROXIES_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("http"):
                    out.append(line)
                    continue
                parts = line.split(":")
                if len(parts) == 4:
                    ip, port, user, pwd = parts
                    out.append(f"http://{user}:{pwd}@{ip}:{port}")
                elif len(parts) == 2:
                    out.append(f"http://{line}")
    except FileNotFoundError:
        pass
    return out


_PROXIES: list[str] = _load_proxies()
log.info("cex: %d proxies loaded", len(_PROXIES))


def _pick_proxy() -> str | None:
    return random.choice(_PROXIES) if _PROXIES else None


# --- FX: normalise every price to USD ------------------------------------

USD_LIKE = {"USD", "USDT", "USDC", "DAI", "BUSD", "USDP", "TUSD", "FDUSD", "PYUSD"}
_FX_TTL = 300.0                                                    # 5 min cache
_fx_cache: dict[str, tuple[float, float]] = {}                     # quote -> (rate_in_usd, ts)
_fx_lock = asyncio.Lock()


def get_fx_rate(quote: str) -> float:
    """Return the last-cached USD-per-1-unit rate for `quote`. 1.0 for USD-like."""
    q = (quote or "").upper()
    if q in USD_LIKE:
        return 1.0
    entry = _fx_cache.get(q)
    return entry[0] if entry else 1.0


async def _quote_to_usd(quote: str) -> float:
    """Return how many USD 1 unit of `quote` is worth. Cached 5 min."""
    q = (quote or "").upper()
    if q in USD_LIKE:
        return 1.0
    now = time.time()
    cached = _fx_cache.get(q)
    if cached and now - cached[1] < _FX_TTL:
        return cached[0]

    async with _fx_lock:
        # double-check under lock
        cached = _fx_cache.get(q)
        if cached and now - cached[1] < _FX_TTL:
            return cached[0]

        # Route: try Binance {q}/USDT (works for EUR, GBP, TRY, ...)
        try:
            await _ensure_markets("binance")
            inst = _get("binance")
            sym = f"{q}/USDT"
            if inst.symbols and sym in inst.symbols:
                inst.aiohttp_proxy = _pick_proxy()
                t = await inst.fetch_ticker(sym)
                px = t.get("last") or t.get("close") or t.get("bid")
                if px:
                    rate = float(px)
                    _fx_cache[q] = (rate, now)
                    log.info("fx: 1 %s = %.4f USD", q, rate)
                    return rate
            # inverted pair (USDT/{q}) — rare
            sym_inv = f"USDT/{q}"
            if inst.symbols and sym_inv in inst.symbols:
                inst.aiohttp_proxy = _pick_proxy()
                t = await inst.fetch_ticker(sym_inv)
                px = t.get("last") or t.get("close") or t.get("bid")
                if px:
                    rate = 1.0 / float(px)
                    _fx_cache[q] = (rate, now)
                    log.info("fx: 1 %s = %.4f USD (via USDT/%s)", q, rate, q)
                    return rate
        except Exception as e:
            log.warning("fx: %s->USD lookup failed: %s", q, e)

    # unknown: fall back to prior cached or 1.0 so the pair still shows
    if cached:
        return cached[0]
    log.warning("fx: no rate for %s, using 1.0", q)
    return 1.0

# Curated list — reliable public tickers, no auth needed.
SUPPORTED_EXCHANGES = [
    "binance", "gate", "bitget", "bitvavo",
]

EXCHANGE_PRETTY = {
    "binance": "Binance", "gate": "Gate.io", "bitget": "Bitget",
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
    inst = _get(eid)
    last_err = None
    for attempt in range(4):
        try:
            inst.aiohttp_proxy = _pick_proxy()
            await inst.load_markets(reload=True)
            _markets_loaded.add(eid)
            log.info("cex: %s markets loaded (%d symbols)%s",
                     eid, len(inst.symbols or []),
                     " via proxy" if inst.aiohttp_proxy else "")
            return
        except Exception as e:
            last_err = e
    log.warning("cex: %s load_markets failed after retries: %s", eid, last_err)


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

    if inst.has.get("fetchTickers"):
        for attempt in range(3):
            try:
                inst.aiohttp_proxy = _pick_proxy()
                data = await inst.fetch_tickers(symbols)
                for sym, t in data.items():
                    px = t.get("last") or t.get("close") or t.get("bid")
                    if px:
                        out[sym] = float(px)
                break
            except Exception as e:
                log.debug("cex: %s fetch_tickers attempt %d failed (%s)",
                          eid, attempt + 1, e)
    if not out:
        # per-symbol fallback (proxy per request)
        async def one(sym):
            for attempt in range(3):
                try:
                    inst.aiohttp_proxy = _pick_proxy()
                    t = await inst.fetch_ticker(sym)
                    px = t.get("last") or t.get("close") or t.get("bid")
                    if px:
                        out[sym] = float(px)
                    return
                except Exception:
                    continue
        await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)

    # normalise every price to USD via quote-currency FX rate
    for sym, px in list(out.items()):
        _, _, quote = sym.partition("/")
        rate = await _quote_to_usd(quote)
        out[sym] = px * rate
    return out


async def close_all():
    for inst in _instances.values():
        try:
            await inst.close()
        except Exception:
            pass


_NETWORK_FIELDS_CONTRACT = ("contract", "contractAddress", "tokenAddress",
                            "address", "asset", "assetContract")

# ---------- Public capital feeds (per CEX, ~6h cache) ------------------
CAPITAL_CACHE_TTL = 6 * 3600
_capital_feeds: dict[str, dict[str, list[dict]]] = {}   # eid -> BASE -> [network]

BINANCE_CAPITAL_URL = ("https://www.binance.com/bapi/capital/v1/public/"
                       "capital/getNetworkCoinAll")
BINANCE_CACHE_FILE = os.path.join(HERE, "binance_capital.json")
BINANCE_CACHE_TTL = 6 * 3600
_binance_networks: dict[str, list[dict]] = {}     # BASE -> [network_info, ...]


def _binance_load_cache() -> bool:
    try:
        with open(BINANCE_CACHE_FILE, encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        return False
    except Exception:
        return False
    if time.time() - d.get("ts", 0) > BINANCE_CACHE_TTL:
        return False
    _binance_networks.clear()
    _binance_networks.update(d.get("data", {}))
    log.info("cex: binance capital feed cached (%d coins, %.1fh old)",
             len(_binance_networks), (time.time() - d["ts"]) / 3600)
    return True


def _binance_save_cache():
    try:
        with open(BINANCE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "data": _binance_networks}, f)
    except Exception as e:
        log.warning("binance capital cache save err: %s", e)


async def load_binance_capital(proxies: list[str] | None = None) -> None:
    """Fetch Binance's public getNetworkCoinAll feed — every coin's networks
    with contract addresses + deposit/withdraw flags. Public, no key needed."""
    if _binance_load_cache():
        return
    proxies = proxies or []
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for _ in range(6):
        proxy = random.choice(proxies) if proxies else None
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(BINANCE_CAPITAL_URL, headers=headers, proxy=proxy,
                                 timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status != 200:
                        continue
                    js = await r.json()
            break
        except Exception:
            continue
    else:
        log.warning("cex: binance capital feed unavailable")
        return

    for coin in (js.get("data") or []):
        base = (coin.get("coin") or "").upper()
        if not base:
            continue
        nets = []
        for n in (coin.get("networkList") or []):
            nets.append({
                "network": n.get("network") or n.get("name") or "",
                "deposit": bool(n.get("depositEnable")),
                "withdraw": bool(n.get("withdrawEnable")),
                "contract": (n.get("contractAddress") or "").lower() or None,
                "fee": n.get("withdrawFee"),
            })
        if nets:
            _binance_networks[base] = nets
    log.info("cex: binance capital feed loaded (%d coins)", len(_binance_networks))
    _binance_save_cache()


# ---------- Generic public-feed helpers -------------------------------
def _capital_cache_path(eid: str) -> str:
    return os.path.join(HERE, f"capital_{eid}.json")


def _capital_load_cache(eid: str) -> bool:
    try:
        with open(_capital_cache_path(eid), encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        return False
    except Exception:
        return False
    if time.time() - d.get("ts", 0) > CAPITAL_CACHE_TTL:
        return False
    _capital_feeds[eid] = d.get("data", {})
    log.info("cex: %s capital feed cached (%d coins, %.1fh old)",
             eid, len(_capital_feeds[eid]), (time.time() - d["ts"]) / 3600)
    return True


def _capital_save_cache(eid: str):
    try:
        with open(_capital_cache_path(eid), "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "data": _capital_feeds.get(eid, {})}, f)
    except Exception as e:
        log.warning("%s capital cache save err: %s", eid, e)


async def _fetch_json(url: str, proxies: list[str]) -> dict | list | None:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for _ in range(4):
        proxy = random.choice(proxies) if proxies else None
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, proxy=proxy,
                                 timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        continue
                    return await r.json()
        except Exception:
            continue
    return None


async def load_kucoin_capital(proxies=None):
    eid = "kucoin"
    if _capital_load_cache(eid):
        return
    js = await _fetch_json("https://api.kucoin.com/api/v3/currencies", proxies or [])
    if not js or not js.get("data"):
        return
    out: dict[str, list[dict]] = {}
    for c in js["data"]:
        base = (c.get("currency") or "").upper()
        if not base:
            continue
        nets = []
        for ch in (c.get("chains") or []):
            nets.append({
                "network": ch.get("chainName") or ch.get("chain") or "",
                "deposit": bool(ch.get("isDepositEnabled")),
                "withdraw": bool(ch.get("isWithdrawEnabled")),
                "contract": (ch.get("contractAddress") or "").lower() or None,
                "fee": ch.get("withdrawalMinFee"),
            })
        if nets:
            out[base] = nets
    _capital_feeds[eid] = out
    log.info("cex: kucoin capital feed loaded (%d coins)", len(out))
    _capital_save_cache(eid)


async def load_gate_capital(proxies=None):
    eid = "gate"
    if _capital_load_cache(eid):
        return
    js = await _fetch_json("https://api.gateio.ws/api/v4/spot/currencies", proxies or [])
    if not js:
        return
    out: dict[str, list[dict]] = {}
    # gate returns one row per (currency, chain); group by currency
    from collections import defaultdict
    grouped = defaultdict(list)
    for c in js:
        base = (c.get("currency") or "").upper()
        if not base or c.get("delisted"):
            continue
        grouped[base].append(c)
    for base, rows in grouped.items():
        nets = []
        for r in rows:
            chain = r.get("chain") or ""
            if not chain:
                continue
            nets.append({
                "network": chain,
                "deposit": not r.get("deposit_disabled"),
                "withdraw": not r.get("withdraw_disabled"),
                "contract": None,           # gate doesn't expose contracts publicly
                "fee": None,
            })
        if nets:
            out[base] = nets
    _capital_feeds[eid] = out
    log.info("cex: gate capital feed loaded (%d coins)", len(out))
    _capital_save_cache(eid)


async def load_bitget_capital(proxies=None):
    eid = "bitget"
    if _capital_load_cache(eid):
        return
    js = await _fetch_json("https://api.bitget.com/api/v2/spot/public/coins", proxies or [])
    if not js or not js.get("data"):
        return
    out: dict[str, list[dict]] = {}
    for c in js["data"]:
        base = (c.get("coin") or "").upper()
        if not base:
            continue
        nets = []
        for ch in (c.get("chains") or []):
            nets.append({
                "network": ch.get("chain") or "",
                "deposit": ch.get("rechargeable") == "true",
                "withdraw": ch.get("withdrawable") == "true",
                "contract": (ch.get("contractAddress") or "").lower() or None,
                "fee": ch.get("withdrawFee"),
            })
        if nets:
            out[base] = nets
    _capital_feeds[eid] = out
    log.info("cex: bitget capital feed loaded (%d coins)", len(out))
    _capital_save_cache(eid)


async def load_coinbase_capital(proxies=None):
    eid = "coinbase"
    if _capital_load_cache(eid):
        return
    js = await _fetch_json("https://api.exchange.coinbase.com/currencies", proxies or [])
    if not js:
        return
    out: dict[str, list[dict]] = {}
    for c in js:
        base = (c.get("id") or "").upper()
        if not base or c.get("status") != "online":
            continue
        details = c.get("details") or {}
        pushed = []
        for net in (details.get("supported_networks") or []) or []:
            # coinbase currencies endpoint doesn't always give network list;
            # skip nets that we can't verify.
            pass
        # coinbase's `details` sometimes has `network_confirmations` etc but
        # not a networkList. Fall back to single native chain.
        chain = details.get("crypto_address_link", "").split("/")[2] if details.get("crypto_address_link") else ""
        pushed.append({
            "network": chain or base,
            "deposit": True,      # if listed and online, deposits usually enabled
            "withdraw": True,
            "contract": None,
            "fee": None,
        })
        out[base] = pushed
    _capital_feeds[eid] = out
    log.info("cex: coinbase capital feed loaded (%d coins)", len(out))
    _capital_save_cache(eid)


async def load_all_capital_feeds(proxies=None):
    """Load public capital feeds for every CEX that exposes them."""
    await asyncio.gather(
        load_binance_capital(proxies),
        load_kucoin_capital(proxies),
        load_gate_capital(proxies),
        load_bitget_capital(proxies),
        load_coinbase_capital(proxies),
        return_exceptions=True,
    )


def network_info(eid: str, base: str) -> list[dict]:
    """Extract per-network deposit/withdraw + contract info.
    - Binance: use the pre-loaded public getNetworkCoinAll feed (always full).
    - Others: read from ccxt.currencies (public-only; coverage patchy —
      KuCoin/Kraken/Gate/Bitvavo give a subset, most others return nothing)."""
    if eid == "binance":
        cached = _binance_networks.get(base.upper())
        if cached:
            return cached
    inst = _instances.get(eid)
    if not inst or not inst.currencies:
        return []
    info = inst.currencies.get(base) or {}
    out: list[dict] = []
    nets = info.get("networks") or {}
    if not nets:
        return []
    for net_name, nd in nets.items():
        raw = nd.get("info") or {}
        dep_top = nd.get("deposit")
        wd_top = nd.get("withdraw")
        dep = (dep_top if dep_top is not None
               else raw.get("depositEnable")
               if raw.get("depositEnable") is not None
               else (raw.get("depositStatus") == "OK") if raw.get("depositStatus") else None)
        wd = (wd_top if wd_top is not None
              else raw.get("withdrawEnable")
              if raw.get("withdrawEnable") is not None
              else (raw.get("withdrawalStatus") == "OK") if raw.get("withdrawalStatus") else None)
        contract = None
        for k in _NETWORK_FIELDS_CONTRACT:
            v = raw.get(k)
            if v and isinstance(v, str) and len(v) >= 20:
                contract = v.lower()
                break
        out.append({
            "network": net_name,
            "deposit": dep,
            "withdraw": wd,
            "contract": contract,
            "fee": raw.get("withdrawalFee") or raw.get("withdrawFee") or nd.get("fee"),
        })
    return out


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
