"""Continuous Bitvavo-anchored spread hunter.

Every cycle:
  1. Fetch every Bitvavo ticker (EUR-quoted; auto-converted to USD by cex.py).
  2. For each other supported CEX, batch-fetch the same bases against
     USDT/USDC/USD if the exchange lists them.
  3. For each base, resolve the best DexScreener pool once per hour
     (search by symbol → pick highest USD-liquidity match), then batch-
     fetch its current price.
  4. Compute spread |bitvavo − other| / min · 100 for every (base, target).
  5. Alert every subscriber when the spread crosses the global threshold,
     with a per-target cooldown so the same lead doesn't spam.

Subscribers persist to hunter_subs.json so the setting survives restarts.
"""
import asyncio
import json
import logging
import os
import random
import time
from collections import defaultdict

import aiohttp

import blacklist
import cex
import dex
import okx_dex
from coingecko import CoinGecko, _norm as _norm_name
from tracker import load_proxies

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
SUBS_FILE = os.path.join(HERE, "hunter_subs.json")

DS_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search?q={q}"
DS_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/{chain}/{addrs}"

POOL_CACHE_TTL = 3600.0
STABLE_QUOTES = ("USDT", "USDC", "USD", "FDUSD", "DAI", "BUSD", "TUSD")

_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _load_subs() -> dict:
    try:
        with open(SUBS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"subs": [], "threshold_pct": 3.0}
    except Exception:
        return {"subs": [], "threshold_pct": 3.0}


def _save_subs(data: dict) -> None:
    tmp = SUBS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, SUBS_FILE)


class Hunter:
    def __init__(self, alert_cb, cycle_sec: float = 2.0,
                 cooldown_sec: float = 120.0, fetch_timeout: float = 1.8,
                 dex_enabled: bool = True, kyber_enabled: bool = False):
        self.alert_cb = alert_cb
        self.cycle_sec = cycle_sec
        self.cooldown = cooldown_sec
        self.fetch_timeout = fetch_timeout
        self.dex_enabled = dex_enabled
        self.kyber_enabled = kyber_enabled
        # stale-but-usable last snapshots — so a slow proxy in one cycle
        # doesn't blow the whole pipeline
        self.last_bitvavo_prices: dict[str, float] = {}
        self.last_other_prices: dict[tuple[str, str], dict] = {}
        self.last_dex_prices: dict[str, dict] = {}
        st = _load_subs()
        self.subs: set[int] = set(int(x) for x in st.get("subs", []))
        self.threshold: float = float(st.get("threshold_pct", 3.0))
        self.min_profit_usd: float = float(st.get("min_profit_usd", 0.0))
        self.bases: list[str] = []
        self.pool_cache: dict[str, dict | None] = {}
        self.pool_ts: dict[str, float] = {}
        self.last_alert: dict[tuple[str, str], float] = {}
        self.last_cycle_summary: str = ""
        self.cg = CoinGecko()
        self.base_to_coin_id: dict[str, str] = {}                  # bitvavo base -> CG coin_id
        self.base_to_contracts: dict[str, dict[str, str]] = {}     # base -> {ds_chain: contract_lower}
        self.ambiguous_bases: set[str] = set()                     # bases where symbol matches >1 CG coin
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def _persist(self):
        try:
            _save_subs({
                "subs": sorted(self.subs),
                "threshold_pct": self.threshold,
                "min_profit_usd": self.min_profit_usd,
            })
        except Exception as e:
            log.warning("hunter subs persist err: %s", e)

    def set_min_profit(self, usd: float) -> None:
        self.min_profit_usd = float(usd)
        self._persist()

    def subscribe(self, chat_id: int) -> None:
        self.subs.add(int(chat_id))
        self._persist()

    def unsubscribe(self, chat_id: int) -> None:
        self.subs.discard(int(chat_id))
        self._persist()

    def set_threshold(self, pct: float) -> None:
        self.threshold = float(pct)
        self._persist()

    async def _load_bitvavo_bases(self) -> None:
        await cex._ensure_markets("bitvavo")
        inst = cex._get("bitvavo")
        bases = set()
        for sym in (inst.symbols or []):
            base, _, _ = sym.partition("/")
            if base:
                bases.add(base.upper())
        self.bases = sorted(bases)
        log.info("hunter: %d unique Bitvavo bases", len(self.bases))

    async def _build_identity_maps(self) -> None:
        """Ground truth: CG's /exchanges/{eid}/tickers already knows what
        coin_id trades under each (exchange, symbol). We use CG's
        `bitvavo|BASE → coin_id` as authoritative for the Bitvavo side,
        then the same map for every target CEX to gate matches by coin_id
        equality. Contracts (for DS filtering) come from CG's platforms list."""
        proxies = load_proxies()
        await self.cg.load(proxies)
        if not self.cg.coins:
            log.warning("hunter: no CG data — identity filter disabled")
            return
        await self.cg.load_exchange_tickers(cex.SUPPORTED_EXCHANGES, proxies)
        await cex.load_binance_capital(proxies)
        log.info("hunter: CG exchange map — %d (exchange, base) → coin_id entries",
                 len(self.cg.exchange_map))
        # Bitvavo-side identity — hybrid strategy:
        #   1. Ask CG's authoritative /exchanges/bitvavo/tickers map.
        #   2. Ask our (symbol + name + network) fuzzy match.
        #   3. If they agree → use it. If CG returns something whose CG-name
        #      is unrelated to Bitvavo's declared name (e.g. CG mapped BIO→
        #      bionergy while Bitvavo says "Bio Protocol"), OVERRIDE with the
        #      name-based answer.
        #   4. If CG has none → use name-based; if name-based has none →
        #      keep CG's (better than nothing); if both empty → unmapped.
        inst = cex._get("bitvavo")
        currencies = inst.currencies or {}
        matched = 0
        overrides = 0
        for base in self.bases:
            info = currencies.get(base) or {}
            name = info.get("name") or (info.get("info") or {}).get("name") or ""
            nets = list((info.get("networks") or {}).keys())
            hint = nets[0] if nets else None

            cg_answer = self.cg.coin_id_on("bitvavo", base)
            coin_id = None
            if cg_answer:
                # Override only when name-based has EXACT name match — this
                # catches CG mis-mappings (BIO → bionergy) without corrupting
                # correct CG mappings (BNB → binancecoin) where Bitvavo uses
                # a longer name ("Binance Coin") than CG's shortname ("BNB").
                exact = self.cg.match_bitvavo_exact(base, name)
                if exact and exact != cg_answer:
                    coin_id = exact
                    overrides += 1
                else:
                    coin_id = cg_answer
            else:
                coin_id = self.cg.match_bitvavo(base, name, hint)

            if coin_id:
                self.base_to_coin_id[base] = coin_id
                self.base_to_contracts[base] = self.cg.contracts_for(coin_id)
                matched += 1
        log.info("hunter: bitvavo identity — %d/%d bases mapped (%d overrides)",
                 matched, len(self.bases), overrides)

    async def _find_ds_pool(self, base: str) -> dict | None:
        now = time.time()
        if base in self.pool_ts and now - self.pool_ts[base] < POOL_CACHE_TTL:
            return self.pool_cache.get(base)
        proxies = load_proxies()
        proxy = random.choice(proxies) if proxies else None
        url = DS_SEARCH_URL.format(q=base)
        js = None
        for _ in range(2):
            try:
                async with self._session.get(url, headers=_UA, proxy=proxy,
                                             timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        proxy = random.choice(proxies) if proxies else None
                        continue
                    js = await r.json()
                break
            except Exception:
                proxy = random.choice(proxies) if proxies else None
        self.pool_ts[base] = now
        if not js:
            self.pool_cache[base] = None
            return None
        pairs = js.get("pairs") or []
        known_contracts = self.base_to_contracts.get(base, {})  # {chain: contract}
        cands = []
        for p in pairs:
            bt = p.get("baseToken") or {}
            if bt.get("symbol", "").upper() != base:
                continue
            chain = p.get("chainId")
            addr = (bt.get("address") or "").lower()
            # IDENTITY GATE — only accept pool whose baseToken address
            # matches a known CG contract for this base. If we have no CG
            # data for this base (unmatched), fall back to symbol-only.
            if known_contracts:
                exp = known_contracts.get(chain)
                if not exp or exp != addr:
                    continue
            cands.append(p)
        if not cands:
            self.pool_cache[base] = None
            return None
        cands.sort(key=lambda p: -float((p.get("liquidity") or {}).get("usd") or 0))
        best = cands[0]
        best_liq = float((best.get("liquidity") or {}).get("usd") or 0)
        if best_liq < 10_000:
            self.pool_cache[base] = None
            return None
        result = {
            "chain": best.get("chainId"),
            "addr": best.get("pairAddress"),
            "url": best.get("url", ""),
            "dex_id": best.get("dexId", ""),
            "liq": best_liq,
        }
        self.pool_cache[base] = result
        return result

    async def _fetch_other_cex(self, eid: str, bases: set[str]) -> dict:
        """Return (base, eid) -> {price, symbol}."""
        await cex._ensure_markets(eid)
        inst = cex._get(eid)
        if not inst.symbols:
            return {}
        wanted, base_map = [], {}
        symset = set(inst.symbols)
        for base in bases:
            # IDENTITY GATE (strict)
            #   - If Bitvavo side has no known coin_id → skip (untrusted).
            #   - Else target exchange MUST list the same coin_id under this
            #     ticker; if unknown or different → skip (homonym).
            btc_id = self.base_to_coin_id.get(base)
            if not btc_id:
                continue
            tgt_id = self.cg.coin_id_on(eid, base)
            if tgt_id != btc_id:
                continue
            for q in STABLE_QUOTES:
                sym = f"{base}/{q}"
                if sym in symset:
                    wanted.append(sym)
                    base_map[sym] = base
                    break
        if not wanted:
            return {}
        try:
            prices = await cex.fetch_for(eid, wanted)
        except Exception as e:
            log.debug("hunter: %s fetch err: %s", eid, e)
            return {}
        return {(base_map[sym], eid): {"price": px, "symbol": sym}
                for sym, px in prices.items() if px > 0}

    async def _fetch_ds_prices(self, pools: dict[str, dict]) -> dict[str, dict]:
        """pools: base -> {chain, addr, ...}. Returns base -> {price, chain, url, liq, dex_id}."""
        by_chain: dict[str, list[str]] = defaultdict(list)
        lookup: dict[tuple[str, str], str] = {}
        for base, pool in pools.items():
            if not pool or not pool.get("addr"):
                continue
            addr = pool["addr"].lower()
            by_chain[pool["chain"]].append(addr)
            lookup[(pool["chain"], addr)] = base
        out: dict[str, dict] = {}
        proxies = load_proxies()

        async def one(chain: str, addrs: list[str]):
            for i in range(0, len(addrs), 30):
                batch = addrs[i:i + 30]
                url = DS_PAIRS_URL.format(chain=chain, addrs=",".join(batch))
                proxy = random.choice(proxies) if proxies else None
                for _ in range(2):
                    try:
                        async with self._session.get(url, headers=_UA, proxy=proxy,
                                                     timeout=aiohttp.ClientTimeout(total=8)) as r:
                            if r.status != 200:
                                proxy = random.choice(proxies) if proxies else None
                                continue
                            js = await r.json()
                        break
                    except Exception:
                        proxy = random.choice(proxies) if proxies else None
                else:
                    continue
                for p in (js.get("pairs") or []):
                    addr = (p.get("pairAddress") or "").lower()
                    base = lookup.get((chain, addr))
                    if base and p.get("priceUsd"):
                        out[base] = {
                            "price": float(p["priceUsd"]),
                            "chain": chain,
                            "url": p.get("url", ""),
                            "liq": float((p.get("liquidity") or {}).get("usd") or 0),
                            "dex_id": p.get("dexId", ""),
                        }

        await asyncio.gather(*(one(c, a) for c, a in by_chain.items()))
        return out

    async def _warmup_pools(self) -> None:
        """One-shot: resolve DS pool for every base upfront. ~1 min for 431 bases."""
        if not self.bases:
            return
        log.info("hunter: warming DS pool cache for %d bases", len(self.bases))
        sem = asyncio.Semaphore(30)
        done = 0
        found = 0

        async def one(base: str):
            nonlocal done, found
            async with sem:
                pool = await self._find_ds_pool(base)
                done += 1
                if pool:
                    found += 1
                if done % 100 == 0:
                    log.info("hunter: warmup %d/%d bases · %d pools found",
                             done, len(self.bases), found)

        await asyncio.gather(*(one(b) for b in self.bases))
        log.info("hunter: warmup done · %d/%d bases have DS pools", found, len(self.bases))

    async def _fetch_kyber_prices(self, bases) -> dict[str, dict]:
        """For each Bitvavo base with a known CG contract on a Kyber-supported
        chain, run dex.usd_price(). Returns {base: {price, chain, url, dex_id, liq}}.
        Concurrency-capped so we don't hammer the Kyber API each cycle."""
        wanted: list[tuple[str, str, str]] = []                    # (base, chain, contract)
        for base in bases:
            contracts = self.base_to_contracts.get(base) or {}
            for ds_chain, addr in contracts.items():
                if ds_chain in dex.KYBER_CHAIN:
                    wanted.append((base, ds_chain, addr))
                    break                                          # 1 chain per base for now
        if not wanted:
            return {}

        sem = asyncio.Semaphore(10)

        quote_size = float(os.getenv("KYBER_QUOTE_USD", "500"))

        async def one(base, chain, addr):
            async with sem:
                try:
                    r = await dex.usd_price(chain, addr, usd_notional=quote_size)
                except Exception:
                    return base, None
                if not r:
                    return base, None
                return base, {
                    "price": r["price_usd"],
                    "chain": chain,
                    "url": r["url"],
                    "liq": 0.0,                                    # Kyber doesn't return a liquidity number
                    "dex_id": "kyber",
                    "contract": addr,                              # needed by executor for DEX swap
                }

        results = await asyncio.gather(*(one(b, c, a) for b, c, a in wanted))
        return {b: p for b, p in results if p}

    async def _cycle(self) -> None:
        # 1) Bitvavo prices — ccxt has its own request timeout, no wait_for
        inst = cex._get("bitvavo")
        if not inst.symbols:
            return
        try:
            raw = await cex.fetch_for("bitvavo", list(inst.symbols))
        except Exception as e:
            log.warning("bitvavo fetch err: %s", e)
            raw = {}
        bitvavo_prices: dict[str, float] = dict(self.last_bitvavo_prices)
        for sym, px in raw.items():
            base = sym.split("/")[0].upper()
            if px > 0:
                bitvavo_prices[base] = px
        self.last_bitvavo_prices = bitvavo_prices
        if not bitvavo_prices:
            return

        # 2) Other CEX prices — parallel, per-call errors → fallback to stale
        other_exchanges = [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]
        bases_set = set(bitvavo_prices)

        async def _safe_fetch(e: str):
            try:
                return await self._fetch_other_cex(e, bases_set)
            except Exception:
                return {}

        other_results = await asyncio.gather(
            *(_safe_fetch(e) for e in other_exchanges),
        )
        other_prices: dict[tuple[str, str], dict] = dict(self.last_other_prices)
        for r in other_results:
            if isinstance(r, dict):
                other_prices.update(r)
        self.last_other_prices = other_prices

        # 3) DEX side — either DexScreener (existing pool-scan flow) or
        # Kyber-aggregator direct quotes. Kyber avoids the pool-discovery
        # step: for each base with a known CG contract, we quote
        # USDC→base on 1 chain and treat the result as the DEX price.
        dex_prices: dict = {}
        if self.dex_enabled:
            uncached = [b for b in bitvavo_prices
                        if b not in self.pool_ts
                        or time.time() - self.pool_ts[b] >= POOL_CACHE_TTL]
            random.shuffle(uncached)
            for base in uncached[:20]:
                await self._find_ds_pool(base)
            pools = {b: self.pool_cache.get(b) for b in bitvavo_prices}
            dex_prices = await self._fetch_ds_prices(pools)
        if self.kyber_enabled:
            kyber_prices = await self._fetch_kyber_prices(bitvavo_prices.keys())
            # Kyber wins if we have both — Kyber returns real executable
            # aggregator quotes.
            for b, p in kyber_prices.items():
                dex_prices[b] = p

        # 4) Group per base — one alert lists Bitvavo + all matched CEX (+ DEX)
        now = time.time()
        alerts: list[dict] = []
        for base, bpx in bitvavo_prices.items():
            if blacklist.is_base_banned(base):
                continue
            # Contract-identity gate — target CEX must either offer a
            # native-chain deposit (no contract needed) OR its exposed
            # contract must appear in CG's platform list for the mapped
            # coin_id. If it does neither, target is selling a homonym
            # token (e.g. Bitget "RON" as an Arbitrum contract vs Ronin
            # native).
            coin_id = self.base_to_coin_id.get(base)
            cg_platforms = (self.cg.by_id.get(coin_id) or {}).get("platforms") or {}
            cg_contracts = {str(a).lower() for a in cg_platforms.values() if a}
            entries: list[dict] = []
            max_spread = 0.0
            for eid in other_exchanges:
                if blacklist.is_pair_banned(base, eid):
                    continue
                info = other_prices.get((base, eid))
                if not info or info["price"] <= 0:
                    continue
                if coin_id:
                    tgt_nets = cex.network_info(eid, base)
                    if tgt_nets:                                     # else we can't verify → keep
                        has_native = any(not n.get("contract") for n in tgt_nets)
                        tgt_contracts = {n["contract"].lower()
                                         for n in tgt_nets if n.get("contract")}
                        if not has_native and tgt_contracts and not (tgt_contracts & cg_contracts):
                            continue                                 # different token
                sp = abs(bpx - info["price"]) / min(bpx, info["price"]) * 100.0
                entries.append({
                    "kind": "cex", "eid": eid, "symbol": info["symbol"],
                    "price": info["price"], "spread": sp,
                })
                if sp > max_spread:
                    max_spread = sp
            dex_ = dex_prices.get(base)
            if dex_ and dex_["price"] > 0:
                sp = abs(bpx - dex_["price"]) / min(bpx, dex_["price"]) * 100.0
                entries.append({
                    "kind": "dex", "chain": dex_["chain"], "dex_id": dex_["dex_id"],
                    "url": dex_["url"], "liq": dex_["liq"],
                    "price": dex_["price"], "spread": sp,
                    "contract": dex_.get("contract"),
                })
                if sp > max_spread:
                    max_spread = sp

            if not entries or max_spread < self.threshold:
                continue
            key = (base,)
            if now - self.last_alert.get(key, 0) < self.cooldown:
                continue
            entries.sort(key=lambda e: -e["spread"])
            alerts.append({
                "base": base,
                "bitvavo_price": bpx,
                "max_spread": max_spread,
                "entries": entries,
                "_key": key,                                       # for cooldown-on-send
            })

        alerts.sort(key=lambda a: -a["max_spread"])
        for a in alerts[:20]:
            try:
                sent = await self.alert_cb(a, list(self.subs))
                # only start cooldown if the alert actually went out
                if sent:
                    self.last_alert[a["_key"]] = time.time()
            except Exception as e:
                log.warning("alert dispatch err: %s", e)

        self.last_cycle_summary = (
            f"bases={len(bitvavo_prices)} "
            f"other_cex={len(other_prices)} "
            f"dex={len(dex_prices)} "
            f"alerts={len(alerts)}"
        )

    async def run(self):
        self._session = aiohttp.ClientSession()
        try:
            await self._load_bitvavo_bases()
            await self._build_identity_maps()
            if self.dex_enabled:
                await self._warmup_pools()
            else:
                log.info("hunter: DEX disabled (HUNT_DEX_ENABLED=false) — Bitvavo vs CEX only")
            while not self._stop.is_set():
                t0 = time.time()
                try:
                    await self._cycle()
                except Exception as e:
                    log.exception("hunter cycle err: %s", e)
                dt = time.time() - t0
                log.info("hunter cycle: %.1fs · %s · %d subs · %.2f%%",
                         dt, self.last_cycle_summary, len(self.subs), self.threshold)
                sleep = max(1.0, self.cycle_sec - dt)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep)
                except asyncio.TimeoutError:
                    pass
        finally:
            await self._session.close()

    def start_bg(self):
        loop = asyncio.get_event_loop()
        if self._task is None or self._task.done():
            self._task = loop.create_task(self.run())

    def stop(self):
        self._stop.set()

    def untracked(self) -> dict:
        """Bitvavo bases that don't produce any comparison, split by reason."""
        no_cg = [b for b in self.bases if b not in self.base_to_coin_id]
        others = [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]
        only_bv = []
        for b in self.bases:
            cid = self.base_to_coin_id.get(b)
            if not cid:
                continue
            if not any(self.cg.coin_id_on(e, b) == cid for e in others):
                only_bv.append(b)
        return {"no_coin_id": sorted(no_cg), "only_bitvavo": sorted(only_bv)}

    # ------------------------------------------------------------------
    # /c command backend — inspect one token across all sources
    # ------------------------------------------------------------------
    async def _resolve_query(self, q: str) -> tuple[str | None, str | None]:
        """query -> (coin_id, note). Accepts contract 0x/base58 or ticker."""
        q = q.strip()
        if not q:
            return None, "empty query"
        if q.startswith("0x") or (len(q) >= 32 and not q.isupper()):
            addr = q.lower()
            for c in self.cg.coins:
                for _, contract in (c.get("platforms") or {}).items():
                    if contract and str(contract).lower() == addr:
                        return c["id"], None
            return None, f"contract {q[:12]}... not in CoinGecko"
        # ticker
        cid = self.cg.coin_id_on("bitvavo", q)
        if cid:
            return cid, None
        cands = self.cg.by_symbol.get(q.lower(), [])
        if len(cands) == 1:
            return cands[0]["id"], None
        if not cands:
            return None, f"symbol {q.upper()} not in CoinGecko"
        return None, f"ambiguous ticker ({len(cands)} coins share {q.upper()})"

    async def check(self, query: str) -> dict:
        coin_id, note = await self._resolve_query(query)
        if not coin_id:
            return {"error": note or "not found"}
        coin = self.cg.by_id.get(coin_id, {}) or {}
        contracts = self.cg.contracts_for(coin_id)               # {ds_chain: contract}

        # 1) which of our exchanges list this coin_id, and under what symbol
        ex_bases: dict[str, str] = {}
        for (eid, base), cid in self.cg.exchange_map.items():
            if cid == coin_id and eid in cex.SUPPORTED_EXCHANGES:
                ex_bases[eid] = base

        # 2) fetch prices in parallel + extract per-CEX network/contract info
        async def fetch_one(eid: str, base: str):
            inst = cex._get(eid)
            await cex._ensure_markets(eid)
            for q in STABLE_QUOTES + ("EUR",):
                sym = f"{base}/{q}"
                if inst.symbols and sym in inst.symbols:
                    r = await cex.fetch_for(eid, [sym])
                    if r:
                        return eid, {
                            "symbol": sym,
                            "price": r[sym],
                            "networks": cex.network_info(eid, base),
                        }
            return eid, None

        cex_results = await asyncio.gather(
            *(fetch_one(e, b) for e, b in ex_bases.items()),
            return_exceptions=True,
        )
        cex_prices: dict[str, dict] = {}
        for r in cex_results:
            if isinstance(r, tuple) and r[1]:
                cex_prices[r[0]] = r[1]

        # 3) DEX prices via OKX Web3 aggregator (per contract-bearing chain)
        dex_prices = await okx_dex.fetch_all(contracts, load_proxies())

        # 4) Bitvavo deposit/withdraw per network for this base
        bitvavo_networks: dict[str, dict] = {}
        bitvavo_base = None
        for (e, b), cid in self.cg.exchange_map.items():
            if e == "bitvavo" and cid == coin_id:
                bitvavo_base = b
                break
        if bitvavo_base:
            inst_b = cex._get("bitvavo")
            binfo = (inst_b.currencies or {}).get(bitvavo_base) or {}
            for net_name, net_data in (binfo.get("networks") or {}).items():
                ni = net_data.get("info") or {}
                bitvavo_networks[net_name] = {
                    "deposit": ni.get("depositStatus") == "OK",
                    "withdraw": ni.get("withdrawalStatus") == "OK",
                    "withdrawal_fee": ni.get("withdrawalFee"),
                    "withdrawal_min": ni.get("withdrawalMinAmount"),
                }

        return {
            "coin_id": coin_id,
            "name": coin.get("name") or coin_id,
            "symbol": (coin.get("symbol") or "").upper(),
            "contracts": contracts,
            "cex_prices": cex_prices,
            "dex_prices": dex_prices,                              # {chain: {price,vol24h,url}}
            "bitvavo_networks": bitvavo_networks,
        }
