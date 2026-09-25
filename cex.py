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
import math as _math
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

# Health tracker per proxy: fail_count, last_fail_ts. Ban a proxy for
# BAN_SEC after FAIL_THRESH consecutive fails.
_PROXY_FAILS: dict[str, int] = {}
_PROXY_BANNED_UNTIL: dict[str, float] = {}
FAIL_THRESH = 2            # 2 fails in a row → ban (aggressive: CF-blocked proxies out fast)
BAN_SEC = 600              # 10 min quarantine


def _pick_proxy() -> str | None:
    if not _PROXIES:
        return None
    import time as _t
    now = _t.time()
    # Filter out currently-banned proxies
    live = [p for p in _PROXIES if _PROXY_BANNED_UNTIL.get(p, 0) < now]
    if not live:                                # everyone banned — fall back to full pool
        return random.choice(_PROXIES)
    return random.choice(live)


def mark_proxy_fail(proxy: str | None) -> None:
    """Call from except-handlers when a request via `proxy` fails.
    3 fails in a row → 5-min ban from the pool."""
    if not proxy:
        return
    _PROXY_FAILS[proxy] = _PROXY_FAILS.get(proxy, 0) + 1
    if _PROXY_FAILS[proxy] >= FAIL_THRESH:
        import time as _t
        _PROXY_BANNED_UNTIL[proxy] = _t.time() + BAN_SEC
        _PROXY_FAILS[proxy] = 0
        log.debug("proxy banned %ds: %s", BAN_SEC, proxy[:40] + "…")


def mark_proxy_ok(proxy: str | None) -> None:
    """Call on successful request — resets fail streak."""
    if proxy and _PROXY_FAILS.get(proxy):
        _PROXY_FAILS.pop(proxy, None)


def _private_proxy_for(eid: str) -> str | None:
    """Static proxy for authenticated calls. Two levels of config:
      PRIVATE_PROXY_<EID> — per-exchange override (recommended when
        each exchange has its own IP whitelist entry)
      PRIVATE_PROXY — fallback for anything without a specific override
    Format: host:port:user:pass  (or full http URL)."""
    raw = os.getenv(f"PRIVATE_PROXY_{eid.upper()}", "").strip()
    if not raw:
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


# --- FX: normalise every price to USD ------------------------------------

USD_LIKE = {"USD", "USDT", "USDC", "DAI", "BUSD", "USDP", "TUSD", "FDUSD", "PYUSD"}
_FX_TTL = float(os.getenv("FX_TTL_SEC", "60"))                     # 60 sec cache (tunable)
_fx_cache: dict[str, tuple[float, float]] = {}                     # quote -> (rate_in_usd, ts)
_fx_lock = asyncio.Lock()


# Exchanges that MUST rotate proxy for authenticated calls (Cloudflare
# blocks direct residential IPs). Empty now that Bitvavo API is
# IP-restricted to our static PRIVATE_PROXY (82.206.73.23), same IP as
# Gate/Binance — they all go through the whitelisted static exit.
_KEEP_PROXY: set[str] = set()

# Dedicated ccxt instances for authenticated calls. Separate from the
# public-price ones so the hunter's proxy rotation (on public reads)
# can't race with a private call in progress.
_private_instances: dict[str, ccxt.Exchange] = {}


def get_private(eid: str, keys_data: dict | None = None) -> ccxt.Exchange:
    """Return (or create) a dedicated authenticated ccxt instance.
    - Bitvavo: rotate proxy per call (Cloudflare bypass)
    - IP-whitelisted exchanges: use PRIVATE_PROXY env if set (single
      static proxy — whitelist that IP once and forget about your
      local IP changing), otherwise direct."""
    inst = _private_instances.get(eid)
    if inst is None:
        cls = getattr(ccxt, eid)
        conf = {"enableRateLimit": True, "timeout": 15000}
        if keys_data:
            conf["apiKey"] = keys_data.get("apiKey", "")
            conf["secret"] = keys_data.get("secret", "")
            if keys_data.get("password"):
                conf["password"] = keys_data["password"]
            if keys_data.get("uid"):
                conf["uid"] = keys_data["uid"]
        inst = cls(conf)
        # Exchanges known to be picky about clock drift → auto-adjust
        # each request timestamp by the offset ccxt learns via
        # load_time_difference. Kept per-instance so we don't re-sync
        # on every call.
        if eid in {"binance", "bybit", "mexc", "gate"}:
            inst.options["adjustForTimeDifference"] = True
            inst.options["recvWindow"] = 60000
        # Bitvavo uses ACCESS-WINDOW header (default 10000ms). Rotating
        # proxies can add lag → bump to 60s so slow proxies don't fail
        # with errorCode 304 "not received within acceptable window".
        if eid == "bitvavo":
            inst.options["adjustForTimeDifference"] = True
            inst.options["window"] = 60000
        # Binance: our API key has NO margin/futures perms → don't try
        # to load those market types (they 401 on `sapiGetMarginAllPairs`
        # and pollute startup logs).
        if eid == "binance":
            inst.options["defaultType"] = "spot"
            inst.options["fetchMarkets"] = ["spot"]
            inst.options["fetchMargins"] = False               # skip margin/allPairs call
            inst.options["fetchCurrencies"] = True             # keep spot currencies
        _private_instances[eid] = inst
    # Per-call proxy choice
    if eid in _KEEP_PROXY:
        inst.aiohttp_proxy = _pick_proxy()
    else:
        inst.aiohttp_proxy = _private_proxy_for(eid)             # None = direct
    return inst


async def withdraw_robust(inst, base: str, amount, address: str,
                          tag: str | None = None, network: str | None = None):
    """Call inst.withdraw with `network` in params first (Binance/Gate
    need it), fall back to no-network on Bitvavo's errorCode 204
    'network parameter is not supported' (single-network tokens).

    IMPORTANT: some exchanges (esp. Bitvavo) return an error response
    even when the withdraw request was actually accepted server-side.
    On any failure we do a post-hoc verify by scanning fetch_withdrawals
    for a matching new entry — if found, treat as success."""
    import time as _t
    started = _t.time()
    tried = []
    if network:
        tried.append({"network": network})
    tried.append({})                                       # no-param fallback
    last = None
    for p in tried:
        try:
            return await inst.withdraw(base, amount, address, tag, p)
        except Exception as e:
            last = e
            msg = str(e)
            # Binance rejects some withdrawals with "amount must be an
            # integer" (-4021) even though its own metadata advertises
            # withdrawIntegerMultiple=0.000001. The declared precision
            # lies, so react to the error: floor and retry once. Retrying
            # the same fractional amount would fail forever.
            if ("must be an integer" in msg or "-4021" in msg) \
                    and float(amount) >= 1:
                whole = float(int(float(amount)))
                if whole >= 1 and whole != float(amount):
                    log.warning("withdraw %s %s: exchange wants a whole "
                                "number — retrying with %g", base, amount,
                                whole)
                    try:
                        return await inst.withdraw(base, whole, address,
                                                     tag, p)
                    except Exception as e2:
                        last = e2
                        msg = str(e2)
            if "network parameter is not supported" not in msg and "204" not in msg:
                # Not the fall-through case — but still verify before raising
                found = await _verify_withdraw_after_error(
                    inst, base, amount, address, started)
                if found:
                    return found
                raise
    # All attempts failed — verify against exchange history
    found = await _verify_withdraw_after_error(inst, base, amount, address, started)
    if found:
        return found
    if last:
        raise last


async def _verify_withdraw_after_error(inst, base: str, amount, address: str,
                                        started_ts: float):
    """Verify a possibly-still-accepted withdraw by looking at what's
    fastest to observe. Preference order:
      1) fetch_withdrawals (exchange side records ~instantly on real accept)
      2) fetch_deposits on OUR side — not applicable here
    Wait only 30s total (short) — if it went through, exchange usually
    records it within a few sec. Longer waits are wasteful because the
    on-chain arrival is detected by the caller's own balance polling."""
    import time as _t
    deadline = _t.time() + 30
    started_ms = started_ts * 1000
    amt = float(amount)
    while _t.time() < deadline:
        try:
            wds = await inst.fetch_withdrawals(base, limit=5)
            for w in wds:
                if (w.get("timestamp") or 0) < started_ms - 5000:
                    continue
                wamt = float(w.get("amount") or 0)
                waddr = (w.get("address") or "").lower()
                target = address.lower()
                if abs(wamt - amt) / max(amt, 1) < 0.02 and waddr == target:
                    log.info("post-error verify: found matching wd id=%s amt=%s",
                             w.get("id"), wamt)
                    return w
        except Exception as e:
            log.debug("post-error verify fetch err: %s", e)
        await asyncio.sleep(3)
    return None


# In-memory deposit-address cache: (eid, base, network) → dict
# Addresses are permanent per-account — no reason to hit the API twice.
_DEPOSIT_ADDR_CACHE: dict = {}


async def fetch_wd_dep_live(eid: str, base: str, inst) -> list[dict] | None:
    """Targeted live check: fetch wd/dep status for ONE token (not a
    full currencies reload). Returns the same shape as `network_info`
    or None on failure. Falls back to nothing if the exchange has no
    per-coin endpoint — caller can decide.

    - Gate: /wallet/currency_chains?currency=AGI
    - Bitvavo: /v2/{coin}/assets   (part of currencies but keyed by coin)
    - Binance: /sapi/v1/capital/config/getall (still all-coins; filter locally)
    """
    from chains import canonical as _cnl
    base = base.upper()
    try:
        if eid == "gate":
            r = await inst.publicWalletGetCurrencyChains({"currency": base})
            out = []
            for c in (r or []):
                if not isinstance(c, dict):
                    continue
                out.append({
                    "network": c.get("chain") or c.get("name_en") or "",
                    "deposit":  c.get("is_deposit_disabled")  in ("0", 0, False, None),
                    "withdraw": c.get("is_withdraw_disabled") in ("0", 0, False, None),
                    "contract": (c.get("contract_address") or "").lower() or None,
                    "fee": c.get("withdraw_amount_min"),
                })
            return out
        if eid == "bitvavo":
            # Bitvavo publicGetAssets returns list; find the coin.
            r = await inst.publicGetAssets({"symbol": base})
            items = r if isinstance(r, list) else [r]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if (item.get("symbol") or "").upper() != base:
                    continue
                nets = []
                for n in (item.get("networks") or []):
                    if isinstance(n, str):
                        nets.append({"network": n,
                                     "deposit": item.get("depositStatus") == "OK",
                                     "withdraw": item.get("withdrawalStatus") == "OK",
                                     "contract": None, "fee": None})
                    elif isinstance(n, dict):
                        nets.append({
                            "network": n.get("name") or n.get("network"),
                            "deposit": (n.get("depositStatus") or item.get("depositStatus")) == "OK",
                            "withdraw": (n.get("withdrawalStatus") or item.get("withdrawalStatus")) == "OK",
                            "contract": None,
                            "fee": n.get("withdrawalFee") or item.get("withdrawalFee"),
                        })
                if not nets:
                    # Fallback — single-network representation
                    nets = [{
                        "network": "ERC20",
                        "deposit": item.get("depositStatus") == "OK",
                        "withdraw": item.get("withdrawalStatus") == "OK",
                        "contract": None,
                        "fee": item.get("withdrawalFee"),
                    }]
                return nets
            return []
        if eid == "binance":
            all_ = await inst.sapiGetCapitalConfigGetall()
            for c in (all_ or []):
                if (c.get("coin") or "").upper() != base:
                    continue
                out = []
                for n in (c.get("networkList") or []):
                    out.append({
                        "network": n.get("network"),
                        "deposit": bool(n.get("depositEnable")),
                        "withdraw": bool(n.get("withdrawEnable")),
                        "contract": (n.get("contractAddress") or "").lower() or None,
                        "fee": n.get("withdrawFee"),
                    })
                return out
            return []
    except Exception as e:
        log.debug("fetch_wd_dep_live %s/%s: %s", eid, base, e)
        return None


async def fetch_deposit_address_robust(inst, base: str, network: str | None = None):
    """Fetch a deposit address for `base`. Cached forever after first
    success (deposit addresses don't change). Tries with the `network`
    param first (needed for multi-network tokens on Binance/Gate), then
    without (Bitvavo single-network tokens like TAI reject `network`
    with errorCode 204). Returns the ccxt address dict, or raises the
    last error if all attempts fail."""
    eid = (getattr(inst, "id", "") or "").lower()
    ck = (eid, base.upper(), (network or "").upper())
    cached = _DEPOSIT_ADDR_CACHE.get(ck)
    if cached:
        return cached
    last_err = None
    tried_params = []
    if network:
        tried_params.append({"network": network})
    tried_params.append(None)                                # no-param fallback
    for p in tried_params:
        try:
            if p:
                r = await inst.fetch_deposit_address(base, params=p)
            else:
                r = await inst.fetch_deposit_address(base)
            if r and r.get("address"):
                _DEPOSIT_ADDR_CACHE[ck] = r
                return r
        except Exception as e:
            last_err = e
            log.debug("fetch_deposit_address %s params=%s: %s", base, p, e)
    if last_err:
        raise last_err
    return None


async def cancel_order_robust(inst, order_id: str, symbol: str,
                                params: dict | None = None) -> dict:
    """Cancel an order and PROVE it's gone.

    `place_order` already injects Bitvavo's mandatory `operatorId`, but
    nothing did the same for cancels — so every cancel raised
    errorCode 203 ("operatorId parameter is required"). The caller
    logged that at debug level, so a half-filled ACX limit order sat
    open on the book for an hour, quietly accumulating inventory that
    nothing was tracking.

    Returns {"ok": bool, "verified": bool, "error": str|None}.
    Never raises — the caller decides how loud to be.
    """
    p = dict(params or {})
    eid = (getattr(inst, "id", "") or "").lower()
    if eid == "bitvavo" and "operatorId" not in p:
        p["operatorId"] = int(time.time() * 1000)
    err = None
    for attempt in range(2):
        try:
            await inst.cancel_order(order_id, symbol, p)
            err = None
            break
        except Exception as e:
            err = str(e)
            msg = err.lower()
            # Already gone counts as success
            if ("order not found" in msg or "unknown order" in msg
                    or "already" in msg or "404" in msg):
                err = None
                break
            if attempt == 0:
                p["operatorId"] = int(time.time() * 1000) + 1
                await asyncio.sleep(1.0)
    # VERIFY — an accepted cancel can still leave the order working.
    verified = False
    try:
        await asyncio.sleep(0.8)
        oo = await inst.fetch_open_orders(symbol)
        verified = not any(str(o.get("id")) == str(order_id)
                           for o in (oo or []))
    except Exception as e:
        log.debug("cancel verify %s: %s", order_id, e)
    if not verified:
        log.error("cancel_order %s %s NOT verified gone (err=%s) — order "
                  "may still be live on the book",
                  symbol, str(order_id)[:24], err or "none")
    return {"ok": err is None, "verified": verified, "error": err}


async def place_order(inst, symbol: str, order_type: str, side: str,
                      amount, price=None, params: dict | None = None):
    """Wrapper around ccxt `create_order` that injects exchange-specific
    quirks:
      - Bitvavo: mandatory `operatorId` (unique int per call).
      - Bitvavo: some markets have no pricePrecision defined; a limit
        order there raises AssertionError inside ccxt's
        `price_to_precision`. Fall back to market for the same side."""
    p = dict(params or {})
    eid = (getattr(inst, "id", "") or "").lower()
    if eid == "bitvavo" and "operatorId" not in p:
        p["operatorId"] = int(time.time() * 1000)
    if eid == "bitvavo" and order_type == "limit":
        try:
            m = inst.markets.get(symbol) or {}
            info = m.get("info") or {}
            # Bitvavo enforces exact price multiples of tickSize (not just
            # X decimals). E.g. tickSize=0.0001 → 0.00382 is INVALID even
            # though it has 5 decimals; it must be 0.0038 or 0.0039.
            # Snap price down to the nearest tick before submitting.
            tick_str = info.get("tickSize")
            if price and tick_str:
                try:
                    tick = float(tick_str)
                    if tick > 0:
                        snapped = _math.floor(float(price) / tick) * tick \
                            if side == "sell" \
                            else _math.ceil(float(price) / tick) * tick
                        # Format to same #decimals as tickSize to avoid float noise
                        dec = max(0, -_math.floor(_math.log10(tick)))
                        price = round(snapped, dec)
                except Exception as _te:
                    log.debug("bitvavo tick-snap err: %s", _te)
            pp = (m.get("precision") or {}).get("price")
            if pp is None:
                log.info("bitvavo %s has no pricePrecision → market %s fallback",
                         symbol, side)
                # Bitvavo returns null pricePrecision AND null costPrecision
                # for many alt markets. ccxt's decimal_to_precision asserts
                # on None. Patch precision from the raw `notionalDecimals`
                # field so create_order can format correctly.
                info = m.get("info") or {}
                try:
                    notional_dec = int(info.get("notionalDecimals") or 2)
                except (TypeError, ValueError):
                    notional_dec = 2
                m.setdefault("precision", {})
                if m["precision"].get("cost") is None:
                    m["precision"]["cost"] = notional_dec
                if m["precision"].get("price") is None:
                    # tickSize like "0.00000100" → 8 decimals
                    tick = info.get("tickSize") or "0.01"
                    try:
                        dec_price = max(0, -_math.floor(_math.log10(float(tick))))
                    except Exception:
                        dec_price = 8
                    m["precision"]["price"] = dec_price
                if side == "buy":
                    cost = float(amount) * float(price) if price else None
                    if cost:
                        p["cost"] = cost
                        return await inst.create_order(symbol, "market", "buy",
                                                        None, None, p)
                return await inst.create_order(symbol, "market", side, amount, None, p)
        except Exception as _e:
            log.debug("bitvavo precision-patch fallback err: %s", _e)
    return await inst.create_order(symbol, order_type, side, amount, price, p)


async def clear_proxy(eid: str):
    """Legacy shim — retained for callers that still use the shared
    public instance. Prefer get_private() for authenticated calls."""
    inst = _instances.get(eid)
    if not inst:
        return
    if eid in _KEEP_PROXY:
        inst.aiohttp_proxy = _pick_proxy()
        return
    inst.aiohttp_proxy = None
    try:
        if getattr(inst, "session", None):
            await inst.session.close()
            inst.session = None
    except Exception:
        pass


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

        # Try Bitvavo first (its EUR/USDC is spot-on and the proxy we
        # already use for it is reliable), then Binance, then Bitfinex.
        # A stale FX cache silently kills real EUR-vs-USDT arbs since
        # cross_match converts both legs to USD with this rate.
        # PRIORITY ORDER for EUR→USD:
        # (1) Bitvavo USDC/EUR inverse — spot EUR/USD (~1.17)
        # (2) Kraken EUR/USDT — very tight, no USDT depeg
        # (3) Kraken EUR/USD direct pair
        # (4) frankfurter.app (ECB reference) — external HTTP API, free
        # Binance EUR/USDT is EXCLUDED (persistent 3-4% USDT depeg
        # premium on Binance = false arb signals on every Bitvavo token).
        sources = [
            ("bitvavo", f"{q}/USDC", 1.0),
            ("bitvavo", f"{q}/EUR",  1.0),
            ("kraken",  f"{q}/USDT", 1.0),
            ("kraken",  f"{q}/USD",  1.0),
        ]
        inverse_sources = [
            ("bitvavo", f"USDC/{q}"),
            ("bitvavo", f"USDT/{q}"),
            ("kraken",  f"USDT/{q}"),
            ("kraken",  f"USD/{q}"),
        ]
        fallback_sources = ([] if q == "EUR" else [
            ("binance", f"{q}/USDT", 1.0),
            ("bitfinex", f"{q}/USD", 1.0),
        ])
        fallback_inverse = ([] if q == "EUR" else [
            ("binance", f"USDT/{q}"),
        ])
        async def _try_direct(src_list):
            for eid, sym, _ in src_list:
                try:
                    await _ensure_markets(eid)
                    inst = _get(eid)
                    if not inst.symbols or sym not in inst.symbols:
                        continue
                    inst.aiohttp_proxy = _pick_proxy()
                    t = await inst.fetch_ticker(sym)
                    bid = t.get("bid"); ask = t.get("ask")
                    if bid and ask and float(bid) > 0 and float(ask) > 0:
                        px = (float(bid) + float(ask)) / 2
                    else:
                        px = t.get("last") or t.get("close") or bid or ask
                    if px and float(px) > 0:
                        rate = float(px)
                        _fx_cache[q] = (rate, now)
                        log.info("fx: 1 %s = %.4f USD (via %s %s)", q, rate, eid, sym)
                        return rate
                    else:
                        log.info("fx: %s %s empty ticker (bid=%s ask=%s last=%s)",
                                 eid, sym, bid, ask, t.get("last"))
                except Exception as e:
                    log.info("fx: %s %s failed: %s", eid, sym, e)
            return None

        async def _try_inverse(src_list):
            for eid, sym in src_list:
                try:
                    await _ensure_markets(eid)
                    inst = _get(eid)
                    if not inst.symbols or sym not in inst.symbols:
                        continue
                    inst.aiohttp_proxy = _pick_proxy()
                    t = await inst.fetch_ticker(sym)
                    bid = t.get("bid"); ask = t.get("ask")
                    if bid and ask:
                        px = (float(bid) + float(ask)) / 2
                    else:
                        px = t.get("last") or t.get("close") or bid or ask
                    if px and float(px) > 0:
                        rate = 1.0 / float(px)
                        _fx_cache[q] = (rate, now)
                        log.info("fx: 1 %s = %.4f USD (via inverse %s %s)",
                                 q, rate, eid, sym)
                        return rate
                    else:
                        log.info("fx: inverse %s %s empty ticker (bid=%s ask=%s last=%s)",
                                 eid, sym, bid, ask, t.get("last"))
                except Exception as e:
                    log.info("fx: inverse %s %s failed: %s", eid, sym, e)
            return None

        # PARALLEL: fire all direct + inverse sources at once, take
        # first success. Beats sequential (which stacks timeouts:
        # bitvavo 15s + kraken 15s + frankfurter 15s = 45s wait).
        async def _one_direct(eid, sym):
            try:
                await _ensure_markets(eid)
                inst = _get(eid)
                if not inst.symbols or sym not in inst.symbols:
                    return None
                inst.aiohttp_proxy = _pick_proxy()
                t = await asyncio.wait_for(inst.fetch_ticker(sym), timeout=8)
                bid = t.get("bid"); ask = t.get("ask")
                if bid and ask and float(bid) > 0 and float(ask) > 0:
                    px = (float(bid) + float(ask)) / 2
                else:
                    px = t.get("last") or t.get("close") or bid or ask
                if px and float(px) > 0:
                    return float(px), f"{eid} {sym}"
            except Exception as e:
                log.debug("fx par direct %s %s: %s", eid, sym, e)
            return None
        async def _one_inverse(eid, sym):
            try:
                await _ensure_markets(eid)
                inst = _get(eid)
                if not inst.symbols or sym not in inst.symbols:
                    return None
                inst.aiohttp_proxy = _pick_proxy()
                t = await asyncio.wait_for(inst.fetch_ticker(sym), timeout=8)
                bid = t.get("bid"); ask = t.get("ask")
                if bid and ask:
                    px = (float(bid) + float(ask)) / 2
                else:
                    px = t.get("last") or t.get("close") or bid or ask
                if px and float(px) > 0:
                    return 1.0 / float(px), f"inverse {eid} {sym}"
            except Exception as e:
                log.debug("fx par inv %s %s: %s", eid, sym, e)
            return None
        # Sequential with tight 5s per-source timeout. Total worst-case
        # bounded to len(sources+inverse+fallback) × 5s ≈ 40s but usually
        # first source (Bitvavo USDC/EUR) returns in <1s. No parallel
        # tasks = no cross-cancellation issues.
        chosen = None; via = None
        for eid, sym, _ in sources + fallback_sources:
            r = await _one_direct(eid, sym)
            if r and r[0] > 0:
                chosen, via = r
                break
        if chosen is None:
            for eid, sym in inverse_sources + fallback_inverse:
                r = await _one_inverse(eid, sym)
                if r and r[0] > 0:
                    chosen, via = r
                    break
        if chosen and chosen > 0:
            _fx_cache[q] = (chosen, now)
            log.info("fx: 1 %s = %.4f USD (via %s)", q, chosen, via)
            return chosen
        # Ultimate fallback: external ECB reference via frankfurter.app
        # (free, no auth). Only for EUR since it's forex only.
        if q == "EUR":
            try:
                import aiohttp as _aio
                async with _aio.ClientSession() as _s:
                    async with _s.get(
                            "https://api.frankfurter.app/latest?from=EUR&to=USD",
                            timeout=15) as _r:
                        _j = await _r.json()
                        rate = float((_j.get("rates") or {}).get("USD") or 0)
                        if rate > 0:
                            _fx_cache[q] = (rate, now)
                            log.info("fx: 1 EUR = %.4f USD (via frankfurter/ECB)",
                                     rate)
                            return rate
            except Exception as e:
                log.info("fx: frankfurter fallback failed: %s", e)
        # Ultimate: return stale cached value rather than 1.0.
        # 1.0 would trigger mass false arb alerts across every Bitvavo
        # token; stale is still close to real EUR/USD in the short term.
        if cached and cached[0] > 1.05:
            log.warning("fx: %s->USD ALL LIVE SOURCES FAILED — using stale "
                        "cache %.4f (age %.0fs)",
                        q, cached[0], now - cached[1])
            _fx_cache[q] = (cached[0], now)          # extend TTL to prevent thrash
            return cached[0]
        log.warning("fx: %s->USD lookup failed on all sources", q)

    # unknown: fall back to prior cached or 1.0 so the pair still shows
    if cached:
        return cached[0]
    log.warning("fx: no rate for %s, using 1.0", q)
    return 1.0

# Curated list — reliable public tickers, no auth needed.
SUPPORTED_EXCHANGES = [
    "gate", "bitvavo", "binance",
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
        # tight per-request timeout so a stuck proxy fails fast and we
        # rotate to a fresh one on retry rather than blocking the cycle
        _instances[eid] = cls({"enableRateLimit": True, "timeout": 5000})
    return _instances[eid]


async def _ensure_markets(eid: str):
    if eid in _markets_loaded:
        return
    inst = _get(eid)
    last_err = None
    for attempt in range(8):                                     # rotate up to 8 proxies
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
    """Return {symbol: price_usd} — legacy shape (mid, USD). Kept for
    call sites that don't need bid/ask. New code should prefer
    `fetch_book_top()` which returns bid+ask+mid."""
    ba = await fetch_book_top(eid, symbols)
    return {sym: v["mid"] for sym, v in ba.items() if v.get("mid")}


async def fetch_book_top(eid: str, symbols: list[str]) -> dict:
    """Return {symbol: {bid, ask, mid}} — all USD-normalised. Uses
    batch fetch_tickers where supported (single HTTP round-trip). The
    bid/ask come straight from the ticker's top-of-book snapshot so
    callers can detect real crossing without a follow-up book fetch."""
    if not symbols:
        return {}
    await _ensure_markets(eid)
    inst = _get(eid)
    raw: dict[str, tuple] = {}                                 # sym -> (bid_native, ask_native)

    def _ba(t: dict) -> tuple:
        bid = t.get("bid"); ask = t.get("ask")
        # fall back to last/close only if bid/ask absent
        if not (bid and ask and float(bid) > 0 and float(ask) > 0):
            px = t.get("last") or t.get("close")
            if px:
                return (float(px), float(px))
            return (None, None)
        return (float(bid), float(ask))

    if inst.has.get("fetchTickers"):
        # Sequential proxy rotation with short timeout — hedging via
        # throw-away ccxt instances added markets-load overhead that
        # made cycles slower than plain retry.
        for attempt in range(4):
            try:
                inst.aiohttp_proxy = _pick_proxy()
                data = await inst.fetch_tickers(symbols)
                for sym, t in data.items():
                    b, a = _ba(t)
                    if b and a:
                        raw[sym] = (b, a)
                break
            except Exception as e:
                log.debug("cex: %s fetch_tickers attempt %d failed (%s)",
                          eid, attempt + 1, e)
    if not raw:
        async def one(sym):
            for attempt in range(4):
                try:
                    inst.aiohttp_proxy = _pick_proxy()
                    t = await inst.fetch_ticker(sym)
                    b, a = _ba(t)
                    if b and a:
                        raw[sym] = (b, a)
                    return
                except Exception:
                    continue
        await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)

    # Normalise to USD once per unique quote currency (one FX call per q)
    quotes = {sym.partition("/")[2] for sym in raw}
    rates = {q: (await _quote_to_usd(q)) or 1.0 for q in quotes}
    out: dict = {}
    for sym, (b, a) in raw.items():
        r = rates.get(sym.partition("/")[2], 1.0)
        bid_usd = b * r
        ask_usd = a * r
        out[sym] = {"bid": bid_usd, "ask": ask_usd,
                    "mid": (bid_usd + ask_usd) / 2}
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


async def load_binance_capital(proxies: list[str] | None = None,
                               force: bool = False) -> None:
    """Fetch Binance's public getNetworkCoinAll feed — every coin's networks
    with contract addresses + deposit/withdraw flags. Public, no key needed."""
    if not force and _binance_load_cache():
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


async def load_kucoin_capital(proxies=None, force: bool = False):
    eid = "kucoin"
    if not force and _capital_load_cache(eid):
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


async def load_gate_capital(proxies=None, force: bool = False):
    eid = "gate"
    if not force and _capital_load_cache(eid):
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


async def load_bitget_capital(proxies=None, force: bool = False):
    eid = "bitget"
    if not force and _capital_load_cache(eid):
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


async def load_coinbase_capital(proxies=None, force: bool = False):
    eid = "coinbase"
    if not force and _capital_load_cache(eid):
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


async def load_all_capital_feeds(proxies=None, force: bool = False):
    """Load public capital feeds for every CEX that exposes them."""
    await asyncio.gather(
        load_binance_capital(proxies, force=force),
        load_kucoin_capital(proxies, force=force),
        load_gate_capital(proxies, force=force),
        load_bitget_capital(proxies, force=force),
        load_coinbase_capital(proxies, force=force),
        return_exceptions=True,
    )


# per-eid cache: last force-refresh timestamp; used to throttle
# per-alert refreshes so we don't hammer capital endpoints
_last_refresh_ts: dict[str, float] = {}
NETWORK_REFRESH_MIN_INTERVAL = 45.0                              # sec


# (eid, BASE) → last per-token network refresh; ("bitvavo", "*") is the
# global Bitvavo /assets reload, which serves every base at once.
_base_refresh_ts: dict[tuple[str, str], float] = {}


async def refresh_networks_for(eids: list[str], proxies=None, base: str | None = None):
    """Refresh dep/wd/network info per exchange, per-token where possible
    (cheap 1-coin endpoints for Bitget/Gate). Falls back to bulk feed
    for exchanges without a per-token endpoint (Binance).

    If `base` is given, refresh only that one token — always, no throttle.
    If `base` is None, do the historical bulk refresh with 45s throttle."""
    now = time.time()
    todo = []
    for eid in set(eids):
        if base:
            # Per-token fetch, THROTTLED. This runs from every alert
            # dispatch, and for Bitvavo it is a full fetch_currencies()
            # — the whole /assets payload through a Cloudflare proxy.
            # At ~60 dispatches a cycle that was 60 full reloads a cycle
            # for a dep/wd status that changes maybe once a day.
            per_base_ttl = float(os.getenv("NETWORK_REFRESH_BASE_SEC", "300"))
            k = (eid, base.upper())
            if now - _base_refresh_ts.get(k, 0) < per_base_ttl:
                continue
            _base_refresh_ts[k] = now
            if eid == "bitget":
                todo.append(_refresh_bitget_coin(base, proxies))
            elif eid == "gate":
                todo.append(_refresh_gate_coin(base, proxies))
            elif eid == "bitvavo":
                # One global endpoint serves every base — at most once a
                # minute regardless of how many bases ask.
                if now - _base_refresh_ts.get(("bitvavo", "*"), 0) < 60:
                    continue
                _base_refresh_ts[("bitvavo", "*")] = now
                todo.append(_refresh_bitvavo_currencies())
            elif eid == "binance":
                # Binance has no per-token endpoint; use cached feed with
                # its own 6h TTL. Force only if the cache is > 15 min old.
                if time.time() - _binance_cache_ts_get() > 900:
                    todo.append(load_binance_capital(proxies, force=True))
            continue
        # BULK path (throttled) — legacy behaviour
        if now - _last_refresh_ts.get(eid, 0) < NETWORK_REFRESH_MIN_INTERVAL:
            continue
        _last_refresh_ts[eid] = now
        if eid == "binance":
            todo.append(load_binance_capital(proxies, force=True))
        elif eid == "kucoin":
            todo.append(load_kucoin_capital(proxies, force=True))
        elif eid == "gate":
            todo.append(load_gate_capital(proxies, force=True))
        elif eid == "bitget":
            todo.append(load_bitget_capital(proxies, force=True))
        elif eid == "coinbase":
            todo.append(load_coinbase_capital(proxies, force=True))
        elif eid == "bitvavo":
            todo.append(_refresh_bitvavo_currencies())
    if todo:
        await asyncio.gather(*todo, return_exceptions=True)


def _binance_cache_ts_get() -> float:
    """Read the persisted Binance capital cache timestamp so we can decide
    whether it's stale enough to warrant a bulk refresh."""
    try:
        with open(BINANCE_CACHE_FILE, encoding="utf-8") as f:
            return float(json.load(f).get("ts", 0))
    except Exception:
        return 0.0


async def _refresh_bitget_coin(base: str, proxies=None):
    """Single-coin refresh for Bitget: /api/v2/spot/public/coins?coin=BASE."""
    url = f"https://api.bitget.com/api/v2/spot/public/coins?coin={base.upper()}"
    js = await _fetch_json(url, proxies or [])
    if not js or not js.get("data"):
        return
    coin = (js["data"] or [{}])[0]
    if (coin.get("coin") or "").upper() != base.upper():
        return
    nets = []
    for ch in (coin.get("chains") or []):
        nets.append({
            "network": ch.get("chain") or "",
            "deposit": ch.get("rechargeable") == "true",
            "withdraw": ch.get("withdrawable") == "true",
            "contract": (ch.get("contractAddress") or "").lower() or None,
            "fee": ch.get("withdrawFee"),
        })
    if nets:
        _capital_feeds.setdefault("bitget", {})[base.upper()] = nets


async def _refresh_gate_coin(base: str, proxies=None):
    """Single-currency refresh for Gate: /api/v4/spot/currencies/{currency}.
    Gate's endpoint returns per-chain rows for that currency."""
    url = f"https://api.gateio.ws/api/v4/spot/currencies/{base.upper()}"
    js = await _fetch_json(url, proxies or [])
    if not js:
        return
    # Gate may return either one dict (rare) or a list of chain rows.
    rows = js if isinstance(js, list) else [js]
    nets = []
    for r in rows:
        chain = r.get("chain") or ""
        if not chain or r.get("delisted"):
            continue
        nets.append({
            "network": chain,
            "deposit": not r.get("deposit_disabled"),
            "withdraw": not r.get("withdraw_disabled"),
            "contract": None,
            "fee": None,
        })
    if nets:
        _capital_feeds.setdefault("gate", {})[base.upper()] = nets


async def _refresh_bitvavo_currencies():
    """Bitvavo doesn't have a light per-token capital feed — the fastest
    fresh source is ccxt.fetch_currencies() which repopulates
    inst.currencies (used by network_info)."""
    try:
        inst = _get("bitvavo")
        inst.aiohttp_proxy = _pick_proxy()
        await inst.fetch_currencies()
    except Exception as e:
        log.debug("bitvavo currencies refresh err: %s", e)


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
        # Gate exposes contracts as `raw.chains[].addr` (per-currency
        # list, replicated on each network's `info`). Chain names in the
        # list use Gate's own labels ("ETH", "BSC") while the outer
        # network name is often the standard ("ERC20", "BEP20"). Match
        # via chains.canonical() so ERC20↔ETH == same chain.
        if not contract:
            try:
                from chains import canonical as _cnl_chain
            except Exception:
                _cnl_chain = lambda x: (x or "").lower()
            raw_chains = raw.get("chains") if isinstance(raw, dict) else None
            net_can = _cnl_chain(net_name) or net_name.lower()
            for c in (raw_chains or []):
                if not isinstance(c, dict):
                    continue
                cname_raw = c.get("name") or c.get("chain") or ""
                cname_can = _cnl_chain(cname_raw) or cname_raw.lower()
                if cname_can == net_can:
                    addr = c.get("addr") or c.get("contract") or c.get("address")
                    if addr and isinstance(addr, str) and len(addr) >= 20:
                        contract = addr.lower()
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
