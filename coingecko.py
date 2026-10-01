"""CoinGecko-backed identity mapping.

Solves the ticker-collision problem — Bitvavo `LUNA` (Terra Classic) must
not compare against Binance `LUNA` (Terra 2.0). Bitvavo tells us the
token *name* and *network*; CG's coin list gives us the definitive
`coin_id` plus contract addresses on every chain.

Public endpoints only (no key needed):
  /api/v3/coins/list?include_platform=true   ~5MB, cached 24h

Load flow:
  1. cg.load()                                 # network, cached to disk
  2. cg.match_bitvavo(symbol, name)            # -> coin_id | None
  3. cg.contracts_for(coin_id)                 # -> {chain: [contract, ...]}
  4. cg.ambiguous_symbol(symbol)               # True if >1 CG coin uses it
"""
import json
import logging
import os
import random
import re
import time
from collections import defaultdict

import asyncio
import itertools

import aiohttp

import proxypool


def _pick(proxies):
    """Re-read the live pool on EVERY attempt: proxypool.active() is empty
    while the pool is dead (Webshare 402), so a pool that dies mid-loop
    sends the next attempt direct instead of burning the retry budget."""
    pool = proxypool.active() if proxies else []
    return random.choice(pool) if pool else None

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(HERE, "cg_coins.json")
EXCHANGES_CACHE_FILE = os.path.join(HERE, "cg_exchanges.json")
CACHE_TTL = 24 * 3600
CG_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
CG_TICKERS_URL = "https://api.coingecko.com/api/v3/exchanges/{eid}/tickers?page={page}&depth=false"

# Free CoinGecko "Demo" key (coingecko.com -> Developer Dashboard): 30 calls/min
# and 10k/month on the same public host. Without it the public API allows
# roughly 5 calls/min per IP -- enough only while the proxy pool spreads the
# load over 1000 IPs. Measured 2026-10-01 direct: 429 after every 2-3 pages.
CG_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()


def _headers() -> dict:
    h = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if CG_API_KEY:
        h["x-cg-demo-api-key"] = CG_API_KEY
    return h


# our CEX id -> CG exchange id
CEX_TO_CG = {
    "binance": "binance",
    "bybit": "bybit_spot",
    "okx": "okx",
    "kucoin": "kucoin",
    "mexc": "mxc",
    "gate": "gate",
    "bitget": "bitget",
    "kraken": "kraken",
    "coinbase": "gdax",                 # CG legacy id for Coinbase Exchange
    "bingx": "bingx",
    "bitvavo": "bitvavo",
}

# DexScreener chain-id → CG platform-key. CG uses different platform slugs.
# Bitvavo network label -> CG platform slug
_NETWORK_TO_CG_PLATFORM = {
    "ERC20": "ethereum", "ETH": "ethereum", "ETHEREUM": "ethereum",
    "BSC": "binance-smart-chain", "BEP20": "binance-smart-chain",
    "BASE": "base",
    "SOL": "solana", "SOLANA": "solana",
    "TON": "the-open-network",
    "ARB": "arbitrum-one", "ARBITRUM": "arbitrum-one",
    "MATIC": "polygon-pos", "POLYGON": "polygon-pos",
    "AVAXC": "avalanche", "AVAX": "avalanche", "AVALANCHE": "avalanche",
    "OP": "optimistic-ethereum", "OPTIMISM": "optimistic-ethereum",
    "TRX": "tron", "TRC20": "tron", "TRON": "tron",
    "SUI": "sui", "APTOS": "aptos", "APT": "aptos",
    "LINEA": "linea", "SCROLL": "scroll", "MANTLE": "mantle",
    "ZKSYNC": "zksync", "BLAST": "blast",
    "RON": "ronin", "RONIN": "ronin",
    "CRO": "cronos", "FTM": "fantom", "CELO": "celo",
}


CG_TO_DS_CHAIN = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bsc",
    "polygon-pos": "polygon",
    "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism",
    "base": "base",
    "avalanche": "avalanche",
    "fantom": "fantom",
    "solana": "solana",
    "sui": "sui",
    "the-open-network": "ton",
    "aptos": "aptos",
    "linea": "linea",
    "blast": "blast",
    "scroll": "scroll",
    "mantle": "mantle",
    "zksync": "zksync",
    "pulsechain": "pulsechain",
    "berachain": "berachain",
    "hyperliquid": "hyperliquid",
    "unichain": "unichain",
    "tron": "tron",
    "cronos": "cronos",
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


class CoinGecko:
    def __init__(self):
        self.coins: list[dict] = []                                          # raw entries
        self.by_symbol: dict[str, list[dict]] = defaultdict(list)            # symbol_lower -> [coin, ...]
        self.by_name_norm: dict[str, list[dict]] = defaultdict(list)         # norm(name) -> [coin, ...]
        self.by_id: dict[str, dict] = {}                                     # coin_id -> coin
        # (our_cex_id, base_symbol_upper) -> coin_id
        self.exchange_map: dict[tuple[str, str], str] = {}
        self._bg_task: asyncio.Task | None = None       # paced direct refresh (pool dead)
        self._bg_started = 0.0
        self.on_exchange_map_update = None               # hunter hook: rebuild identity maps
        self._best: dict[str, int] = {}                  # per exchange: bases seen in a complete walk

    def _load_cache(self) -> bool:
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                d = json.load(f)
        except FileNotFoundError:
            return False
        except Exception as e:
            log.warning("cg cache load err: %s", e)
            return False
        if time.time() - d.get("ts", 0) > CACHE_TTL:
            return False
        self.coins = d.get("coins", [])
        self._index()
        log.info("cg: loaded %d coins from cache (%.1fh old)",
                 len(self.coins), (time.time() - d["ts"]) / 3600)
        return True

    def _save_cache(self) -> None:
        try:
            tmp = CACHE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"ts": time.time(), "coins": self.coins}, f)
            os.replace(tmp, CACHE_FILE)
        except Exception as e:
            log.warning("cg cache save err: %s", e)

    def _index(self) -> None:
        self.by_symbol.clear()
        self.by_name_norm.clear()
        self.by_id.clear()
        for c in self.coins:
            sym = (c.get("symbol") or "").lower()
            if sym:
                self.by_symbol[sym].append(c)
            nm = _norm(c.get("name") or "")
            if nm:
                self.by_name_norm[nm].append(c)
            cid = c.get("id")
            if cid:
                self.by_id[cid] = c

    async def load(self, proxies: list[str] | None = None) -> None:
        if self._load_cache():
            return
        log.info("cg: fetching /coins/list (this is a one-time ~5MB download)")
        proxies = proxies or []
        headers = _headers()
        async with aiohttp.ClientSession() as s:
            for attempt in range(6):
                proxy = _pick(proxies)
                try:
                    async with s.get(CG_URL, headers=headers, proxy=proxy,
                                     timeout=aiohttp.ClientTimeout(total=45)) as r:
                        if r.status != 200:
                            log.warning("cg: got %d, retry", r.status)
                            continue
                        self.coins = await r.json()
                        break
                except Exception as e:
                    proxypool.note_fail(proxy, e)
                    log.warning("cg fetch attempt %d failed: %s", attempt + 1, e)
        if not self.coins:
            log.error("cg: failed to load; identity filtering will be disabled")
            return
        self._index()
        self._save_cache()
        log.info("cg: %d coins indexed", len(self.coins))

    def match_bitvavo_exact(self, symbol: str, name: str) -> str | None:
        """Only return a coin_id if there's exactly one CG coin with this
        (symbol, exact-normalized-name). Zero false positives → safe for
        overriding CG's authoritative mapping."""
        cands = self.by_symbol.get((symbol or "").lower(), [])
        target = _norm(name)
        if not target:
            return None
        exact = [c for c in cands if _norm(c.get("name") or "") == target]
        if len(exact) == 1:
            return exact[0]["id"]
        return None

    def match_bitvavo(self, symbol: str, name: str,
                      network_hint: str | None = None) -> str | None:
        """Bitvavo gave us symbol + name + (network). Pick the coin_id whose
        name matches best; if multiple pass the name check, prefer the one
        whose platforms include the Bitvavo network. Returns None otherwise."""
        cands = self.by_symbol.get((symbol or "").lower(), [])
        if not cands:
            return None

        target = _norm(name)
        # Score candidates by name match strength
        scored: list[tuple[int, dict]] = []
        for c in cands:
            cand_name = _norm(c.get("name") or "")
            if not cand_name:
                continue
            if target and cand_name == target:
                scored.append((10_000, c))
                continue
            if target and (cand_name in target or target in cand_name):
                scored.append((min(len(cand_name), len(target)), c))
        if not scored and len(cands) == 1:
            return cands[0]["id"]
        if not scored:
            return None
        scored.sort(key=lambda x: -x[0])
        top_score = scored[0][0]
        top_cands = [c for s, c in scored if s == top_score]
        if len(top_cands) == 1:
            return top_cands[0]["id"]

        # Tie-break with Bitvavo's network → CG platform
        wanted_platform = _NETWORK_TO_CG_PLATFORM.get((network_hint or "").upper())
        if wanted_platform:
            for c in top_cands:
                if wanted_platform in (c.get("platforms") or {}):
                    return c["id"]
        return top_cands[0]["id"]

    def contracts_for(self, coin_id: str) -> dict[str, str]:
        """Return {ds_chain_id: contract_lower} for this coin (only chains we support)."""
        c = self.by_id.get(coin_id)
        if not c:
            return {}
        out = {}
        for cg_chain, addr in (c.get("platforms") or {}).items():
            if not addr:
                continue
            ds_chain = CG_TO_DS_CHAIN.get(cg_chain)
            if ds_chain:
                out[ds_chain] = str(addr).lower()
        return out

    def ambiguous_symbol(self, symbol: str) -> bool:
        """True if more than one CG coin uses this ticker — CEX symbol match
        alone is unsafe (LUNA, RON, U, TOMO, ...)."""
        return len(self.by_symbol.get((symbol or "").lower(), [])) > 1

    async def load_exchange_tickers(self, our_cex_ids: list[str],
                                    proxies: list[str] | None = None,
                                    force: bool = False) -> None:
        """Fetch /exchanges/{eid}/tickers for each of our CEX ids (paginated).
        Populates self.exchange_map: (our_id, BASE) -> coin_id.
        Cached 24h on disk; `force=True` bypasses the cache so the hourly
        identity refresh can pick up listings added since.

        Pool dead (proxypool): CoinGecko allows only ~5 calls/min per IP
        without a key, so a full refresh takes minutes. With ANY baseline map
        (fresh-but-thin or stale cache) the fetch runs in the background --
        the hunter keeps tracking on the baseline and `on_exchange_map_update`
        fires when the map is complete. Only with no map at all do we wait."""
        ids = [e for e in our_cex_ids if e in CEX_TO_CG]
        fresh = False if force else self._load_exchange_cache()
        if not fresh and not self.exchange_map:
            # Stale cache as a BASELINE: a rate-limited fetch must never leave
            # the map empty (that is what blanked bitvavo/binance on 2026-10-01).
            self._load_exchange_cache(stale_ok=True)
        min_entries = int(os.getenv("CG_MIN_EXCHANGE_ENTRIES", "300"))
        # "thin" = fewer bases than the floor, or under 80% of the most this
        # exchange ever returned in a COMPLETE walk (remembered in the cache).
        thin = [e for e in ids
                if self._count(e) < max(min_entries, int(0.8 * self._best.get(e, 0)))]
        todo = thin if (fresh and not force) else ids
        if not todo:
            return
        if not proxypool.active() and self.exchange_map:
            now = time.time()
            if self._bg_task and not self._bg_task.done():
                return
            if now - self._bg_started < float(os.getenv("CG_BG_REFETCH_COOLDOWN_SEC", "1800")):
                return
            self._bg_started = now
            log.info("cg: pool dead -- refreshing %s in the background (paced); "
                     "tracking continues on %d cached entries", todo, len(self.exchange_map))
            self._bg_task = asyncio.create_task(
                self._fetch_exchange_tickers(todo, proxies, notify=True))
            return
        await self._fetch_exchange_tickers(todo, proxies, notify=False)

    def _count(self, eid: str) -> int:
        return sum(1 for k in self.exchange_map if k[0] == eid)

    async def _fetch_exchange_tickers(self, ids: list[str],
                                      proxies: list[str] | None,
                                      notify: bool) -> None:
        proxies = proxies or []
        # No pool (proxypool dead) -> requests leave from direct + PRIVATE_PROXY
        # only. CoinGecko's public limit is ~5 req/min per IP without a key, so
        # pace pages one at a time, alternate the two IPs, honour Retry-After.
        direct = not proxypool.active()
        gap = float(os.getenv("CG_DIRECT_PAGE_GAP_SEC", "2.1" if CG_API_KEY else "6.0"))
        routes: list[str | None] = [None]
        if direct and proxypool.fallback():
            routes.append(proxypool.fallback())
        rr = itertools.count()
        pace = asyncio.Lock()
        headers = _headers()
        log.info("cg: fetching tickers for %s (%s)", ids,
                 f"direct, paced over {len(routes)} IP(s)" if direct else "uses proxies")

        async def fetch_page(session, cg_eid: str, page: int) -> list[dict] | None:
            for _ in range(4):
                proxy = routes[next(rr) % len(routes)] if direct else _pick(proxies)
                try:
                    url = CG_TICKERS_URL.format(eid=cg_eid, page=page)
                    if direct:
                        await pace.acquire()
                    try:
                        async with session.get(url, headers=headers, proxy=proxy,
                                               timeout=aiohttp.ClientTimeout(total=15)) as r:
                            if r.status == 429:
                                ra = r.headers.get("Retry-After")
                                try:
                                    wait = min(60.0, float(ra)) if ra else 10.0
                                except ValueError:
                                    wait = 10.0
                                if direct:
                                    log.info("cg: 429 on %s p%d -- waiting %.0fs", cg_eid, page, wait)
                                    await asyncio.sleep(wait)
                                continue
                            if r.status != 200:
                                continue
                            d = await r.json()
                            return d.get("tickers") or []
                    finally:
                        if direct:
                            await asyncio.sleep(gap / len(routes))
                            pace.release()
                except Exception as e:
                    proxypool.note_fail(proxy, e)
                    continue
            return None

        def _index(our_eid: str, tickers: list[dict]) -> int:
            n = 0
            for t in tickers:
                base = (t.get("base") or "").upper()
                cid = t.get("coin_id") or ""
                if base and cid:
                    self.exchange_map[(our_eid, base)] = cid
                    n += 1
            return n

        async def fetch_all(cg_eid: str, our_eid: str) -> int:
            """Pool: pages 1..30 in parallel batches of 10 while any page has
            data. Direct: one page at a time, stop at the first empty page."""
            n = 0
            async with aiohttp.ClientSession() as s:
                if direct:
                    page = 1
                    while page <= 200:
                        res = await fetch_page(s, cg_eid, page)
                        if res is None:              # 4 attempts failed: NOT the end
                            log.warning("cg: %s p%d unreachable -- walk incomplete", cg_eid, page)
                            return n, False
                        if not res:
                            break
                        n += _index(our_eid, res)
                        page += 1
                    return n, True
                offset = 1
                while offset <= 200:
                    batch = list(range(offset, offset + 10))
                    results = await asyncio.gather(
                        *(fetch_page(s, cg_eid, p) for p in batch),
                        return_exceptions=True,
                    )
                    any_data = False
                    for res in results:
                        if not isinstance(res, list) or not res:
                            continue
                        any_data = True
                        n += _index(our_eid, res)
                    if not any_data:
                        # all None = every page failed, not the end of the list
                        return n, any(isinstance(r, list) for r in results)
                    offset += 10
            return n, True

        totals = await asyncio.gather(*(fetch_all(CEX_TO_CG[e], e) for e in ids))
        ok = True
        for eid, (cnt, complete) in zip(ids, totals):
            if cnt and complete:
                self._best[eid] = max(self._best.get(eid, 0), self._count(eid))
                log.info("cg: %s -> %d tickers indexed (%d bases)", eid, cnt, self._count(eid))
            else:
                ok = False
                log.warning("cg: %s -> %d tickers, walk %s -- keeping %d cached "
                            "entries, cache not rewritten", eid, cnt,
                            "complete" if complete else "INCOMPLETE", self._count(eid))
        if ok:
            self._save_exchange_cache()
        if notify and any(c for c, _ in totals) and self.on_exchange_map_update:
            try:
                r = self.on_exchange_map_update()
                if asyncio.iscoroutine(r):
                    await r
            except Exception as e:
                log.warning("cg: on_exchange_map_update failed: %s", e)

    def _load_exchange_cache(self, stale_ok: bool = False) -> bool:
        try:
            with open(EXCHANGES_CACHE_FILE, encoding="utf-8") as f:
                d = json.load(f)
        except FileNotFoundError:
            return False
        except Exception as e:
            log.warning("cg exchange cache err: %s", e)
            return False
        stale = time.time() - d.get("ts", 0) > CACHE_TTL
        if stale and not stale_ok:
            return False
        self.exchange_map = {tuple(k.split("|", 1)): v
                             for k, v in d.get("map", {}).items()}
        self._best = {k: int(v) for k, v in (d.get("best") or {}).items()}
        log.info("cg: exchange map loaded from cache (%d entries, %.1fh old%s)",
                 len(self.exchange_map), (time.time() - d["ts"]) / 3600,
                 ", stale baseline" if stale else "")
        return not stale

    def _save_exchange_cache(self) -> None:
        try:
            tmp = EXCHANGES_CACHE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"ts": time.time(), "best": self._best,
                           "map": {f"{k[0]}|{k[1]}": v
                                   for k, v in self.exchange_map.items()}}, f)
            os.replace(tmp, EXCHANGES_CACHE_FILE)
        except Exception as e:
            log.warning("cg exchange cache save err: %s", e)

    def coin_id_on(self, our_cex_id: str, base_symbol: str) -> str | None:
        return self.exchange_map.get((our_cex_id, base_symbol.upper()))
