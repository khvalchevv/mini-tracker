"""Pool-wide health for the rotating proxy pool (Webshare, proxies.txt).

Per-proxy bans (cex.mark_proxy_fail) handle individual dead IPs. They are
useless when the WHOLE pool fails at once: Webshare answers HTTP 402
"Payment Required" (X-Webshare-Reason: bandwidthlimit) from every one of
the 1000 IPs the moment the plan's monthly bandwidth is used up. On
2026-10-01 that silently killed Kyber quotes ("ALL 6 racers failed ... 402"),
Bitvavo REST, DexScreener and CoinGecko in both trackers while the
per-proxy ban logic kept rotating through 1000 equally dead proxies.

Measured fallback (2026-10-01): Kyber, DexScreener, CoinGecko, Gate,
Binance and Kraken all answer DIRECT from this IP (Kyber: 30 parallel
requests -> 30x200). Bitvavo REST is Cloudflare-blocked direct but works
through PRIVATE_PROXY (the static IP the WS feeds already use).

State machine
  alive  -> note_fail() sees DEAD_DISTINCT distinct proxies answer 402
            within DEAD_WINDOW_SEC, or probe() gets 402 from every pick
            -> dead
  dead   -> active() returns [] (callers go direct), pick()/cex._pick_proxy
            return the PRIVATE_PROXY fallback; monitor() re-probes every
            PROBE_SEC and flips back to alive on the first 200
  alive again -> suspect() stays True for SUSPECT_SEC so the Kyber racers
            keep direct + PRIVATE_PROXY alongside pool picks in case it flaps

ccxt swallows the proxy's status (bare "gate GET <url>" ExchangeError, no
headers kept), so ccxt callers cannot report a 402 directly. They call
suspicious_fail(); a burst of distinct-proxy failures with no pool success
in the same window triggers one debounced probe(), which is authoritative.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Callable, Optional

import aiohttp

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
PROXIES_FILE = os.path.join(HERE, "proxies.txt")

DEAD_DISTINCT = int(os.getenv("PROXY_POOL_DEAD_DISTINCT", "5"))
DEAD_WINDOW_SEC = float(os.getenv("PROXY_POOL_DEAD_WINDOW_SEC", "120"))
PROBE_SEC = float(os.getenv("PROXY_POOL_PROBE_SEC", "600"))
SUSPECT_SEC = float(os.getenv("PROXY_POOL_SUSPECT_SEC", "1800"))
SUSPICIOUS_DISTINCT = int(os.getenv("PROXY_POOL_SUSPICIOUS_DISTINCT", "8"))
SUSPICIOUS_WINDOW_SEC = 60.0
PROBE_URL = os.getenv("PROXY_POOL_PROBE_URL",
                      "https://api.gateio.ws/api/v4/spot/time")

# Optional notifier (e.g. Telegram). Called with a plain-text message on
# every dead/alive transition; may be sync or return an awaitable.
on_change: Optional[Callable[[str], object]] = None


def _parse(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("http"):
        return line
    parts = line.split(":")
    if len(parts) == 4:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    if len(parts) == 2:
        return f"http://{line}"
    return None


def _load() -> list[str]:
    out: list[str] = []
    try:
        with open(PROXIES_FILE, encoding="utf-8") as f:
            for line in f:
                p = _parse(line)
                if p:
                    out.append(p)
    except FileNotFoundError:
        pass
    return out


POOL: list[str] = _load()
_POOL_SET = set(POOL)

_dead = False
_dead_since = 0.0
_revived_at = 0.0
_dead_events = 0
_402_ts: dict[str, float] = {}          # proxy -> ts of its last 402
_susp_ts: dict[str, float] = {}         # proxy -> ts of its last opaque failure
_last_pool_ok = 0.0
_last_probe_kick = 0.0


def fallback() -> str | None:
    """Static PRIVATE_PROXY (host:port:user:pass or URL) or None = direct."""
    raw = os.getenv("PRIVATE_PROXY", "").strip()
    return _parse(raw) if raw else None


def dead() -> bool:
    return _dead


def suspect() -> bool:
    """Dead now, or revived less than SUSPECT_SEC ago."""
    return _dead or bool(_revived_at and time.time() - _revived_at < SUSPECT_SEC)


def active() -> list[str]:
    """The pool to rotate through -- EMPTY while the pool is dead so every
    `random.choice(proxies) if proxies else None` caller goes direct."""
    return [] if _dead else POOL


def pick() -> str | None:
    """One proxy for a request: random pool member, or the fallback
    (PRIVATE_PROXY / direct) while the pool is dead."""
    if _dead:
        return fallback()
    if not POOL:
        return None
    return random.choice(POOL)


def racers(n: int, pool: list[str] | None = None) -> list[str | None]:
    """Proxy set for a parallel race (Kyber). Healthy: n pool picks.
    Suspect: n-2 pool picks + direct + fallback. Dead/empty: direct + fallback."""
    pool = active() if pool is None else pool
    extras: list[str | None] = []
    if _dead or suspect() or not pool:
        extras.append(None)
        fb = fallback()
        if fb:
            extras.append(fb)
    if _dead or not pool:
        return extras
    k = max(1, n - len(extras))
    return random.sample(pool, min(k, len(pool))) + extras


def is_pay_error(exc: BaseException | None, headers=None) -> bool:
    """True when the PROXY (not the target) refused the request.
    aiohttp raises ClientHttpProxyError 402/407 on the CONNECT."""
    if isinstance(exc, aiohttp.ClientHttpProxyError) and exc.status in (402, 407):
        return True
    if headers:
        try:
            for k in headers.keys():
                if k.lower().startswith("x-webshare"):
                    return True
        except Exception:
            pass
    if exc is not None:
        s = str(exc)
        if "Payment Required" in s or "X-Webshare" in s:
            return True
    return False


def _emit(msg: str) -> None:
    log.warning("proxypool: %s", msg)
    cb = on_change
    if not cb:
        return
    try:
        r = cb(msg)
        if asyncio.iscoroutine(r):
            asyncio.ensure_future(r)
    except Exception as e:                  # notifier must never break the pool
        log.debug("proxypool: on_change failed: %s", e)


def _mark_dead(reason: str) -> None:
    global _dead, _dead_since, _dead_events
    if _dead:
        return
    _dead = True
    _dead_since = time.time()
    _dead_events += 1
    _402_ts.clear()
    _susp_ts.clear()
    fb = fallback()
    _emit(f"proxy pool DEAD -- {reason}. "
          f"Switching to {'PRIVATE_PROXY' if fb else 'direct'} for Kyber/REST, "
          f"direct for DexScreener/CoinGecko; re-probing the pool every {PROBE_SEC:.0f}s.")


def _mark_alive(how: str) -> None:
    global _dead, _revived_at, _dead_since
    if not _dead:
        return
    _dead = False
    _revived_at = time.time()
    down = _revived_at - _dead_since
    _dead_since = 0.0
    _emit(f"proxy pool ALIVE again after {down/60:.0f} min ({how}). "
          f"Rotating through {len(POOL)} proxies again.")


def note_fail(proxy: str | None, exc: BaseException | None = None,
              headers=None) -> bool:
    """Record a failed request made via `proxy`. Returns True when it was a
    pool-level pay error (402) -- callers should then NOT count it against
    the individual proxy."""
    if not proxy or proxy not in _POOL_SET:
        return False
    if not is_pay_error(exc, headers):
        return False
    now = time.time()
    _402_ts[proxy] = now
    if _dead:
        return True
    recent = sum(1 for t in _402_ts.values() if now - t < DEAD_WINDOW_SEC)
    if recent >= DEAD_DISTINCT:
        _mark_dead(f"{recent} distinct proxies answered 402 Payment Required "
                   f"within {DEAD_WINDOW_SEC:.0f}s (Webshare bandwidth cap)")
    return True


def suspicious_fail(proxy: str | None) -> None:
    """An opaque failure via a pool proxy (ccxt hides the status). Many
    distinct proxies failing with no pool success in the same minute
    kicks one debounced probe() to settle whether the pool is dead."""
    global _last_probe_kick
    if _dead or not proxy or proxy not in _POOL_SET:
        return
    now = time.time()
    _susp_ts[proxy] = now
    if now - _last_pool_ok < SUSPICIOUS_WINDOW_SEC:
        return
    recent = sum(1 for t in _susp_ts.values() if now - t < SUSPICIOUS_WINDOW_SEC)
    if recent >= SUSPICIOUS_DISTINCT and now - _last_probe_kick > SUSPICIOUS_WINDOW_SEC:
        _last_probe_kick = now
        _susp_ts.clear()
        try:
            asyncio.get_running_loop().create_task(probe())
            log.info("proxypool: %d distinct proxies failed in %.0fs with no pool "
                     "success -- probing", recent, SUSPICIOUS_WINDOW_SEC)
        except RuntimeError:
            pass                            # no running loop (sync context)


def note_ok(proxy: str | None) -> None:
    """A pool proxy answered normally."""
    global _last_pool_ok
    if proxy and proxy in _POOL_SET:
        _last_pool_ok = time.time()
        _402_ts.pop(proxy, None)
        _susp_ts.pop(proxy, None)
        if _dead:
            _mark_alive("a pool proxy answered")


async def _probe_one(session: aiohttp.ClientSession, proxy: str) -> str:
    try:
        async with session.get(PROBE_URL, proxy=proxy,
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            return "ok" if r.status < 500 else f"HTTP {r.status}"
    except aiohttp.ClientHttpProxyError as e:
        return "402" if e.status in (402, 407) else f"proxy {e.status}"
    except Exception as e:
        return type(e).__name__


async def probe(n: int = 5) -> bool:
    """Startup / periodic check of the pool itself. Returns True when the
    pool is usable. Flips state in both directions."""
    global _last_pool_ok
    if not POOL:
        return False
    picks = random.sample(POOL, min(n, len(POOL)))
    try:
        async with aiohttp.ClientSession() as s:
            res = await asyncio.gather(*[_probe_one(s, p) for p in picks])
    except Exception as e:
        log.debug("proxypool: probe err: %s", e)
        return not _dead
    oks = sum(1 for r in res if r == "ok")
    pays = sum(1 for r in res if r == "402")
    if oks:
        _last_pool_ok = time.time()
        _mark_alive(f"probe {oks}/{len(res)} ok")
        return True
    if pays and pays == len(res):
        _mark_dead(f"probe: {pays}/{len(res)} proxies answered 402 Payment Required "
                   f"(Webshare bandwidth cap)")
        return False
    log.info("proxypool: probe inconclusive %s", list(res))
    return not _dead


async def monitor() -> None:
    """Background loop: while dead, re-probe every PROBE_SEC; while alive,
    a light probe every 10xPROBE_SEC catches a pool that died quietly
    between the hunter's own requests."""
    while True:
        try:
            await asyncio.sleep(PROBE_SEC if _dead else PROBE_SEC * 10)
            await probe()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("proxypool: monitor err: %s", e)


def status() -> str:
    if _dead:
        since = time.strftime("%H:%M", time.localtime(_dead_since))
        fb = "private" if fallback() else "direct"
        return f"DEAD since {since} ({len(POOL)} proxies, fallback={fb})"
    return f"alive ({len(POOL)} proxies{', suspect' if suspect() else ''})"
