"""DexScreener pool polling + spread computation + alert dispatch.

Groups all tracked (chain, pair-address) tuples per cycle, hits the
DS batch endpoint `/latest/dex/pairs/{chain}/{a1,a2,...}` (up to 30
addresses per call), rotates through the local proxy pool, updates
each stored pair's last_price_* + last_spread, and fires alerts
via `send_alert_cb(pair, price_a, price_b, spread_pct)`.
"""
import asyncio
import logging
import os
import random
import time
from collections import defaultdict

import aiohttp

import cex
import storage

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
PROXIES_FILE = os.path.join(HERE, "proxies.txt")

DS_URL = "https://api.dexscreener.com/latest/dex/pairs/{chain}/{addrs}"
BATCH_SIZE = 30
_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def load_proxies() -> list[str]:
    try:
        with open(PROXIES_FILE, encoding="utf-8") as f:
            out = []
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "://" not in line:
                    line = "http://" + line
                out.append(line)
            return out
    except FileNotFoundError:
        return []


class Tracker:
    def __init__(self, send_alert_cb, poll_interval: float = 3.0,
                 cooldown: float = 60.0, timeout: float = 8.0):
        self.send_alert = send_alert_cb
        self.poll_interval = poll_interval
        self.cooldown = cooldown
        self.timeout = timeout
        self.proxies = load_proxies()
        log.info("tracker: %d proxies loaded", len(self.proxies))
        self._session: aiohttp.ClientSession | None = None
        self._stop = asyncio.Event()

    async def _fetch_chain_batch(self, chain: str, addrs: list[str]) -> dict:
        """Return {(chain, addr_lower): {price, liquidity, url, ts}}."""
        url = DS_URL.format(chain=chain, addrs=",".join(addrs))
        proxy = random.choice(self.proxies) if self.proxies else None
        out: dict = {}
        for attempt in range(3):
            try:
                async with self._session.get(
                    url, headers=_UA, proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as r:
                    if r.status != 200:
                        proxy = random.choice(self.proxies) if self.proxies else None
                        continue
                    js = await r.json()
                break
            except Exception:
                proxy = random.choice(self.proxies) if self.proxies else None
                if attempt == 2:
                    return out
        else:
            return out

        pairs = js.get("pairs") or []
        now = time.time()
        for p in pairs:
            addr = (p.get("pairAddress") or "").lower()
            price = p.get("priceUsd")
            if not addr or price is None:
                continue
            try:
                out[(chain, addr)] = {
                    "price": float(price),
                    "liq": float((p.get("liquidity") or {}).get("usd") or 0),
                    "url": p.get("url") or "",
                    "base": (p.get("baseToken") or {}).get("symbol") or "",
                    "quote": (p.get("quoteToken") or {}).get("symbol") or "",
                    "dex": p.get("dexId") or "",
                    "ts": now,
                }
            except (TypeError, ValueError):
                continue
        return out

    def _side_key(self, side: dict):
        if side.get("type") == "cex":
            return ("cex", side["exchange"], side["symbol"])
        return ("dex", side["chain"], side["addr"].lower())

    async def _cycle(self):
        pairs = storage.all_pairs()
        if not pairs:
            return

        by_chain: dict[str, set[str]] = defaultdict(set)
        by_ex: dict[str, set[str]] = defaultdict(set)
        for p in pairs:
            if p.get("paused"):
                continue
            for side in (p["a"], p["b"]):
                if side.get("type") == "cex":
                    by_ex[side["exchange"]].add(side["symbol"])
                else:
                    by_chain[side["chain"]].add(side["addr"].lower())

        tasks = []
        for chain, addr_set in by_chain.items():
            addrs = list(addr_set)
            for i in range(0, len(addrs), BATCH_SIZE):
                tasks.append(self._fetch_chain_batch(chain, addrs[i:i + BATCH_SIZE]))
        cex_task_index = len(tasks)
        cex_task_meta: list[str] = []
        for eid, symset in by_ex.items():
            cex_task_meta.append(eid)
            tasks.append(cex.fetch_for(eid, list(symset)))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        prices: dict = {}
        for r in results[:cex_task_index]:
            if isinstance(r, dict):
                prices.update(r)
        for eid, r in zip(cex_task_meta, results[cex_task_index:]):
            if isinstance(r, dict):
                for sym, px in r.items():
                    prices[("cex", eid, sym)] = {"price": px, "liq": 0.0,
                                                 "url": cex.trading_url(eid, sym)}

        now = time.time()
        for p in pairs:
            if p.get("paused"):
                continue
            key_a = self._side_key(p["a"])
            key_b = self._side_key(p["b"])
            # DEX keys stored as 2-tuple in fetch results, normalise for lookup
            pa = prices.get(key_a) if key_a[0] == "cex" else prices.get((key_a[1], key_a[2]))
            pb = prices.get(key_b) if key_b[0] == "cex" else prices.get((key_b[1], key_b[2]))
            if not pa or not pb:
                continue
            if pa["price"] <= 0 or pb["price"] <= 0:
                continue
            spread = abs(pa["price"] - pb["price"]) / min(pa["price"], pb["price"]) * 100.0
            storage.update(
                p["id"],
                last_price_a=pa["price"],
                last_price_b=pb["price"],
                last_spread=spread,
                last_ts=now,
            )
            if spread >= p["threshold_pct"] and (now - p.get("last_alert_ts", 0)) >= self.cooldown:
                storage.update(p["id"], last_alert_ts=now)
                try:
                    await self.send_alert(p, pa, pb, spread)
                except Exception as e:
                    log.warning("alert send failed: %s", e)

    async def run(self):
        self._session = aiohttp.ClientSession()
        try:
            while not self._stop.is_set():
                t0 = time.time()
                try:
                    await self._cycle()
                except Exception as e:
                    log.exception("cycle err: %s", e)
                dt = time.time() - t0
                sleep = max(0.1, self.poll_interval - dt)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep)
                except asyncio.TimeoutError:
                    pass
        finally:
            await self._session.close()

    def stop(self):
        self._stop.set()
