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
        # base_key → last alerted max_spread. Re-alert during cooldown
        # only when spread grows by SPREAD_GROW_PCT relative points.
        self.last_alert_spread: dict[tuple, float] = {}
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
        # First try WS cache — near-instant, no HTTP round-trip. Fall
        # back to REST for symbols the WS doesn't have (warmup or gaps).
        import wsfeed
        ws_cache = wsfeed.all_book_tops(eid)
        book: dict[str, dict] = {}
        missing = []
        for sym in wanted:
            e = ws_cache.get(sym)
            if e and e.get("bid") and e.get("ask"):
                bid_usd = float(e["bid"])                     # USDT/USDC ~= USD
                ask_usd = float(e["ask"])
                book[sym] = {"bid": bid_usd, "ask": ask_usd,
                             "mid": (bid_usd + ask_usd) / 2}
            else:
                missing.append(sym)
        if missing:
            try:
                rest = await cex.fetch_book_top(eid, missing)
                book.update(rest)
            except Exception as ex:
                log.debug("hunter: %s REST fetch err: %s", eid, ex)
        return {(base_map[sym], eid): {"price": v["mid"], "bid": v["bid"],
                                       "ask": v["ask"], "symbol": sym}
                for sym, v in book.items() if v.get("mid") and v["mid"] > 0}

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

    async def _fetch_okx_prices(self, bases: list[str]) -> dict[str, dict]:
        """OKX Web3 by contract — primary DEX price source.
        Resolves (chain, contract) with a 3-tier fallback so coverage is
        broad: (1) target-CEX contract (Binance/Gate), (2) CoinGecko
        platforms (self.base_to_contracts), (3) any chain OKX supports
        where a contract is known. Then one hub call per (chain, ct)."""
        from chains import canonical
        wanted: dict[str, tuple[str, str]] = {}         # base → (chain, contract)
        others = [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]
        # Bitvavo WD chain lookup (cached per cycle for perf)
        _bv_chains_cache: dict[str, set] = {}
        for base in bases:
            picked = None

            # (a) target-CEX contract on Bitvavo-withdrawable chain
            if base not in _bv_chains_cache:
                bv = set()
                for n in cex.network_info("bitvavo", base):
                    if n.get("withdraw"):
                        c = canonical(n["network"])
                        if c: bv.add(c)
                _bv_chains_cache[base] = bv
            bv_chains = _bv_chains_cache[base]
            if bv_chains:
                # chains OKX supports (its CHAIN_INDEX)
                candidates = [c for c in bv_chains if c in okx_dex.CHAIN_INDEX]
                for c in candidates:
                    for eid in others:
                        for n in cex.network_info(eid, base):
                            if canonical(n["network"]) != c:
                                continue
                            if n.get("contract"):
                                picked = (c, n["contract"].lower())
                                break
                        if picked: break
                    if picked: break

            # (b) CoinGecko platforms — {ds_chain_slug: contract}
            # STRICT: DEX chain MUST be one Bitvavo can WD to AND the
            # contract MUST match what at least one target CEX confirms
            # for that chain. This blocks the CFG-fake-arb case where
            # CG has an old/wrong contract that trades at $0.026 while
            # Bitvavo/Binance list a different contract at $0.10.
            if not picked and bv_chains:
                cg_contracts = getattr(self, "base_to_contracts", {}).get(base) or {}
                # Collect target-CEX contract set for this base per chain
                # (for cross-verification below)
                ce_contracts: dict[str, set] = {}
                for eid in others:
                    for n in cex.network_info(eid, base):
                        c = canonical(n["network"])
                        if c and n.get("contract"):
                            ce_contracts.setdefault(c, set()).add(n["contract"].lower())
                for ds_chain, contract in cg_contracts.items():
                    if not contract or ds_chain not in okx_dex.CHAIN_INDEX:
                        continue
                    if ds_chain not in bv_chains:
                        continue
                    ct_l = contract.lower()
                    # Cross-check: if we have target CEX contracts for
                    # this chain, only accept if they match CG's.
                    known = ce_contracts.get(ds_chain)
                    if known and ct_l not in known:
                        continue                            # WRONG contract per CEX
                    picked = (ds_chain, ct_l)
                    break

            if not picked:
                continue
            wanted[base] = picked

        if not wanted:
            return {}
        # Hub enforces client_concurrency=1 → 20-way parallel killed 95%
        # of requests. Incremental refresh: this cycle only touches
        # OKX_BATCH_PER_CYCLE bases (round-robin); rest reuse cached prices.
        # Full 340 → 30/cycle × 12 cycles ≈ 60s at 5s cycle interval.
        batch = int(os.getenv("OKX_BATCH_PER_CYCLE", "40"))
        if not hasattr(self, "_okx_rr_offset"):
            self._okx_rr_offset = 0
        # Prioritize STALE entries (never fetched OR >90s old); fill
        # remaining budget with round-robin fresh checks.
        _now = time.time()
        if not hasattr(self, "_okx_last_seen"):
            self._okx_last_seen = {}
        _stale = [b for b in wanted
                  if _now - self._okx_last_seen.get(b, 0) > 90]
        _rr_rest = [b for b in wanted if b not in _stale]
        # Rotate through non-stale to eventually refresh everything
        _rr = _rr_rest[self._okx_rr_offset:] + _rr_rest[:self._okx_rr_offset]
        _picked = (_stale + _rr)[:batch]
        self._okx_rr_offset = (self._okx_rr_offset + batch) % max(len(_rr_rest), 1)
        wanted = {b: wanted[b] for b in _picked}
        import aiohttp as _aio
        sem = asyncio.Semaphore(int(os.getenv("OKX_HUB_CONCURRENCY", "1")))
        async def _one(base, ch, ct):
            async with sem:
                for _retry in range(3):
                    try:
                        async with _aio.ClientSession() as s:
                            r = await okx_dex._fetch_one(s, ch, ct)
                        if r and r.get("price"):
                            return base, {
                                "price": r["price"],
                                "price_buy_usd": r["price"],
                                "price_sell_usd": r["price"],
                                "chain": ch,
                                "url": r.get("url", ""),
                                "liq": r.get("liquidity", 0),
                                "vol24h": r.get("vol24h", 0),
                                "dex_id": "okx",
                                "contract": ct,
                            }
                        # Empty response — likely 429 in _fetch_one → retry
                        await asyncio.sleep(0.5 * (_retry + 1))
                    except Exception as e:
                        log.debug("okx price %s/%s err: %s", ch, base, e)
                        await asyncio.sleep(0.3)
                # All retries failed
                return base, None
        log.info("hunter: OKX Web3 candidates: %d bases with resolved contract",
                 len(wanted))
        results = await asyncio.gather(*(_one(b, c, ct)
                                          for b, (c, ct) in wanted.items()))
        got = {b: p for b, p in results if p}
        # Mark timestamps of successful fetches — powers stale-priority
        # selection on subsequent cycles.
        for b in got:
            self._okx_last_seen[b] = time.time()
        log.info("hunter: OKX Web3 hub returned prices for %d/%d candidates "
                 "(batch %d of %d total)",
                 len(got), len(wanted), len(wanted),
                 len(self._okx_last_seen))
        return got


    async def _fetch_kyber_prices(self, bases, ref_prices: dict[str, float] | None = None) -> dict[str, dict]:
        """For each Bitvavo base, pick a chain that (a) Bitvavo actually
        supports for withdraw, (b) Kyber supports, then take the contract
        from a target CEX's capital feed (fallback to CG platforms).
        Ensures the DEX price reflects the same token/chain that could
        actually flow between Bitvavo ↔ hot wallet."""
        import chains as _chains
        from chains import canonical
        wanted: list[tuple[str, str, str]] = []                    # (base, chain, contract)
        others = [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]
        for base in bases:
            # (a) chains Bitvavo supports for WITHDRAW
            bv_chains: set[str] = set()
            for n in cex.network_info("bitvavo", base):
                if not n.get("withdraw"):
                    continue
                c = canonical(n["network"])
                if c:
                    bv_chains.add(c)
            if not bv_chains:
                continue
            # (b) chains Kyber supports
            candidates = [c for c in bv_chains if c in dex.KYBER_CHAIN]
            if not candidates:
                continue
            # (c) prefer chain where at least one target CEX also has the
            # coin + exposes a contract (that becomes the auth contract)
            picked_chain = None
            picked_contract = None
            for c in candidates:
                for eid in others:
                    for n in cex.network_info(eid, base):
                        if canonical(n["network"]) != c:
                            continue
                        if n.get("contract"):
                            picked_chain = c
                            picked_contract = n["contract"].lower()
                            break
                    if picked_chain:
                        break
                if picked_chain:
                    break
            # fallback: CG platform contract for one of the candidate chains
            if not picked_chain:
                cg_contracts = self.base_to_contracts.get(base) or {}
                for c in candidates:
                    if c in cg_contracts:
                        picked_chain, picked_contract = c, cg_contracts[c]
                        break
            if not picked_chain or not picked_contract:
                continue
            wanted.append((base, picked_chain, picked_contract))
        if not wanted:
            return {}

        sem = asyncio.Semaphore(int(os.getenv("KYBER_CONCURRENCY", "30")))

        quote_size = float(os.getenv("KYBER_QUOTE_USD", "500"))

        ref_prices = ref_prices or {}

        async def one(base, chain, addr):
            async with sem:
                ref = ref_prices.get(base)
                try:
                    r = await dex.usd_price(chain, addr, usd_notional=quote_size,
                                            reference_price_usd=ref)
                except Exception:
                    return base, None
                if not r:
                    return base, None
                return base, {
                    "price": r["price_usd"],                       # mid — for identity gate only
                    "price_buy_usd": r.get("price_buy_usd"),
                    "price_sell_usd": r.get("price_sell_usd"),
                    "chain": chain,
                    "url": r["url"],
                    "liq": 0.0,
                    "dex_id": "kyber",
                    "contract": addr,
                }

        results = await asyncio.gather(*(one(b, c, a) for b, c, a in wanted))
        return {b: p for b, p in results if p}

    async def _cycle(self) -> None:
        # 1) Bitvavo top-of-book — WS pushes only when a market trades,
        # so low-volume pairs may never appear via WS. Hybrid: use WS
        # for freshness, run a periodic REST snapshot to fill the tail.
        import wsfeed
        inst = cex._get("bitvavo")
        if not inst.symbols:
            return
        fx_eur = await cex._quote_to_usd("EUR")
        # Sanity: EUR/USD is never near 1.0. If FX lookup failed and we
        # got the 1.0 fallback, SKIP this cycle — otherwise we'd cache
        # Bitvavo EUR prices under the "USD" key and every downstream
        # spread/alert would be wrong (~15% off). Better silent skip
        # than a wave of false alerts.
        if fx_eur < 1.05:
            log.warning("hunter: fx_eur=%.4f looks like fallback → skip cycle",
                        fx_eur)
            return
        bitvavo_prices: dict[str, float] = dict(self.last_bitvavo_prices)
        bitvavo_ba: dict[str, dict] = dict(getattr(self, "last_bitvavo_ba", {}))
        bv_ws = wsfeed.all_book_tops("bitvavo")
        # Always apply WS entries (they're the freshest)
        for sym, e in bv_ws.items():
            base = sym.split("/")[0].upper()
            bid_usd = e["bid"] * fx_eur
            ask_usd = e["ask"] * fx_eur
            mid = (bid_usd + ask_usd) / 2
            if mid > 0:
                bitvavo_prices[base] = mid
                bitvavo_ba[base] = {"bid": bid_usd, "ask": ask_usd}
        # Periodic REST snapshot every REST_SNAPSHOT_SEC to fill the
        # low-volume tail that WS silence leaves stale.
        now_t = time.time()
        snap_interval = float(os.getenv("REST_SNAPSHOT_SEC", "30"))
        if now_t - getattr(self, "_last_rest_snap", 0) > snap_interval:
            self._last_rest_snap = now_t
            try:
                raw_ba = await cex.fetch_book_top("bitvavo", list(inst.symbols))
                for sym, v in raw_ba.items():
                    base = sym.split("/")[0].upper()
                    if v.get("mid") and v["mid"] > 0:
                        # WS entry (if any) wins on freshness only when
                        # updated within snap_interval; otherwise REST wins.
                        ws_entry = bv_ws.get(sym)
                        if ws_entry and (now_t - ws_entry["ts"]) < snap_interval:
                            continue
                        bitvavo_prices[base] = v["mid"]
                        bitvavo_ba[base] = {"bid": v["bid"], "ask": v["ask"]}
            except Exception as e:
                log.debug("bitvavo REST snapshot err: %s", e)
        self.last_bitvavo_prices = bitvavo_prices
        self.last_bitvavo_ba = bitvavo_ba
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

        # 3) DEX side runs in a separate background loop (see
        # _dex_loop) so its wall-clock (5-15s per pass) never blocks
        # this CEX cycle. We just read whatever's in the cache now.
        dex_prices: dict = dict(self.last_dex_prices)

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
                # REAL bid/ask cross — no mid-vs-mid illusions. Two
                # directions, take the one that actually crosses (if any).
                bv_ba = (getattr(self, "last_bitvavo_ba", {}) or {}).get(base) or {}
                bv_bid = bv_ba.get("bid"); bv_ask = bv_ba.get("ask")
                tg_bid = info.get("bid"); tg_ask = info.get("ask")
                # STALE-WS GUARD: if bitvavo mid and target mid disagree
                # by >5%, one side has stale WS cache (Bitvavo WS is silent
                # on low-vol pairs — old bid/ask lingers forever). This
                # gave fake "10% spread" alerts on TAIKO/AXS/AIOZ/PENDLE.
                tg_price = info.get("price") or 0
                if bpx > 0 and tg_price > 0:
                    _mid_ratio = max(bpx, tg_price) / min(bpx, tg_price)
                    if _mid_ratio > 1.05:                    # >5% mid-vs-mid
                        log.debug("hunter: skip %s %s — mids %.6g vs %.6g "
                                  "differ %.1f%% (likely stale WS cache)",
                                  base, eid, bpx, tg_price,
                                  (_mid_ratio - 1) * 100)
                        continue
                sp = 0.0
                # Direction 1: buy Bitvavo (ask), sell target (bid)
                if bv_ask and tg_bid and tg_bid > bv_ask:
                    sp = max(sp, (tg_bid - bv_ask) / bv_ask * 100.0)
                # Direction 2: buy target (ask), sell Bitvavo (bid)
                if tg_ask and bv_bid and bv_bid > tg_ask:
                    sp = max(sp, (bv_bid - tg_ask) / tg_ask * 100.0)
                if sp <= 0:
                    continue                                    # no real cross
                entries.append({
                    "kind": "cex", "eid": eid, "symbol": info["symbol"],
                    "price": info["price"], "spread": sp,
                })
                if sp > max_spread:
                    max_spread = sp
            dex_ = dex_prices.get(base)
            # SANITY: if DEX price is wildly off Bitvavo (>10x either way),
            # the contract is almost certainly a different token (CFG-like
            # bug where old/dead contract trades at 1/4 the real price).
            # Rejecting here prevents fake 3000%+ spread alerts.
            if dex_:
                _dex_p = dex_.get("price") or 0
                if _dex_p > 0 and bpx > 0:
                    _ratio = max(_dex_p, bpx) / min(_dex_p, bpx)
                    if _ratio > 10:
                        log.debug("hunter: skip DEX %s — price %.6g vs bpx %.6g "
                                  "(%.1fx off, wrong contract likely)",
                                  base, _dex_p, bpx, _ratio)
                        dex_ = None
            if dex_:
                # Pick the DEX side that matches the arb direction:
                #   Bitvavo cheap → we'd SELL on DEX → compare vs price_SELL
                #   Bitvavo expensive → we'd BUY on DEX → compare vs price_BUY
                pb = dex_.get("price_buy_usd") or 0
                ps = dex_.get("price_sell_usd") or 0
                fallback_price = dex_.get("price") or 0
                # Actionable arb price + direction detection
                if ps and bpx < ps:
                    dpx = ps
                    arb_ok = True
                elif pb and bpx > pb:
                    dpx = pb
                    arb_ok = True
                else:
                    # No directional arb — still show DEX price for
                    # visibility (user wants to compare CEX↔Bitvavo arb
                    # against DEX price + liq/vol even when DEX itself
                    # isn't a viable venue).
                    dpx = fallback_price or ps or pb
                    arb_ok = False
                if dpx > 0:
                    sp = abs(bpx - dpx) / min(bpx, dpx) * 100.0
                    entries.append({
                        "kind": "dex", "chain": dex_["chain"], "dex_id": dex_["dex_id"],
                        "url": dex_["url"],
                        "liq": dex_.get("liq") or 0,
                        "vol24h": dex_.get("vol24h") or 0,
                        "price": dpx, "spread": sp,
                        "contract": dex_.get("contract"),
                        "price_buy_usd": pb, "price_sell_usd": ps,
                        "arb_ok": arb_ok,               # False = display-only, no auto-exec
                    })
                    if arb_ok and sp > max_spread:
                        max_spread = sp

            if not entries or max_spread < self.threshold:
                continue
            key = (base,)
            # Cooldown gate — 30 min by default (HUNT_COOLDOWN_SEC).
            # BUT re-fire during cooldown if spread GREW meaningfully
            # since the last alert (user wants to know when a stale
            # opportunity got better).
            since_last = now - self.last_alert.get(key, 0)
            if since_last < self.cooldown:
                prev_sp = self.last_alert_spread.get(key, 0.0)
                grow_thresh = float(os.getenv("SPREAD_GROW_PCT", "0.3"))
                if max_spread - prev_sp < grow_thresh:
                    continue                                # still same or smaller
            entries.sort(key=lambda e: -e["spread"])
            alerts.append({
                "base": base,
                "bitvavo_price": bpx,
                "max_spread": max_spread,
                "entries": entries,
                "_key": key,                                       # for cooldown-on-send
            })

        alerts.sort(key=lambda a: -a["max_spread"])
        # Take a wider slice so tight legit arbs (0.1-0.5%) don't get
        # crowded out by ticker-collision fake spreads (which the
        # identity gate should filter but sometimes leak through).
        async def _dispatch(a):
            try:
                sent = await self.alert_cb(a, list(self.subs))
                if sent:
                    self.last_alert[a["_key"]] = time.time()
                    self.last_alert_spread[a["_key"]] = a["max_spread"]
            except Exception as e:
                log.warning("alert dispatch err: %s", e)
        # Fire all alerts concurrently — each one does its own sizing
        # (2 book fetches), so N alerts sequential = N*bookfetch wait.
        await asyncio.gather(*(_dispatch(a) for a in alerts[:60]),
                             return_exceptions=True)

        self.last_cycle_summary = (
            f"bases={len(bitvavo_prices)} "
            f"other_cex={len(other_prices)} "
            f"dex={len(dex_prices)} "
            f"alerts={len(alerts)}"
        )

    async def _dex_cycle(self) -> None:
        """One DEX refresh pass — runs in its own loop so CEX price
        polling never blocks on Kyber's 5-15s wall clock."""
        bitvavo_prices = dict(self.last_bitvavo_prices)
        other_prices = dict(self.last_other_prices)
        other_exchanges = [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]
        # Don't fire until main CEX cycle has both legs populated —
        # otherwise the had_peer check falls through and Kyber quotes
        # every single base ($1000 × 400+ requests = wasted budget).
        if not bitvavo_prices or not other_prices:
            return
        new_dex: dict = dict(self.last_dex_prices)
        # HYBRID: DS = primary (fast/wide coverage), OKX = precision layer
        # applied only to bases where hunter finds an arb candidate.
        #
        # DS gives per-pool price+liq for all 340 bases in ~5-10s.
        # OKX gives aggregate price for a targeted few bases in 5-10s each
        # via hub (throttled to ~1 req/5s), so we only spend that budget
        # on tokens where CEX-cross already suggests real arb potential.

        # === PHASE 1: DS refresh — all bases, fast ===
        okx_verified_prev: set[str] = {b for b, p in new_dex.items()
                                         if p and p.get("dex_id") == "okx"}
        ds_bases_count = 0
        if self.dex_enabled:
            uncached = [b for b in bitvavo_prices
                        if b not in self.pool_ts
                        or time.time() - self.pool_ts[b] >= POOL_CACHE_TTL]
            random.shuffle(uncached)
            for base in uncached[:20]:                    # 20 pool-discoveries/cycle
                await self._find_ds_pool(base)
            pools = {b: self.pool_cache.get(b) for b in bitvavo_prices}
            ds = await self._fetch_ds_prices(pools)
            for b, p in ds.items():
                # DS never overwrites a fresh OKX entry (that's more accurate)
                existing = new_dex.get(b)
                if existing and existing.get("dex_id") == "okx":
                    # Keep OKX price, but refresh liq/vol from DS if OKX had 0
                    if not existing.get("liq"):
                        existing["liq"] = p.get("liq", 0)
                    continue
                new_dex[b] = p
                ds_bases_count += 1
            if ds:
                log.info("hunter: DS refreshed %d bases (OKX-verified carry: %d)",
                         ds_bases_count, len(okx_verified_prev))

        # === PHASE 2: identify arb candidates (CEX cross ≥ threshold) ===
        # These are the ONLY bases we spend precious OKX/hub budget on.
        arb_candidates: list[str] = []
        for base, bpx in bitvavo_prices.items():
            for eid in other_exchanges:
                info = other_prices.get((base, eid))
                if not info or info["price"] <= 0: continue
                _sp = abs(bpx - info["price"]) / min(bpx, info["price"]) * 100.0
                if _sp >= self.threshold:
                    arb_candidates.append(base)
                    break
            # Also target if DS price shows a spread
            _dex_p = (new_dex.get(base) or {}).get("price") or 0
            if _dex_p > 0 and bpx > 0:
                _sp = abs(bpx - _dex_p) / min(bpx, _dex_p) * 100.0
                if _sp >= self.threshold and base not in arb_candidates:
                    arb_candidates.append(base)

        # === PHASE 3: OKX refresh — arb candidates only ===
        try:
            okx_prices = await self._fetch_okx_prices(arb_candidates or [])
            for b, p in okx_prices.items():
                new_dex[b] = p                             # OKX beats DS
            if okx_prices:
                log.info("hunter: OKX Web3 refreshed %d/%d arb candidates",
                         len(okx_prices), len(arb_candidates))
        except Exception as e:
            log.warning("okx precision fetch err: %s", e)
        # Kyber quotes — pre-filter to bases with a raw mid gap
        if self.kyber_enabled:
            kyber_bases: list[str] = []
            for base, bpx in bitvavo_prices.items():
                gap = 0.0
                had_peer = False
                for eid in other_exchanges:
                    info = other_prices.get((base, eid))
                    if not info or info["price"] <= 0:
                        continue
                    had_peer = True
                    sp = abs(bpx - info["price"]) / min(bpx, info["price"]) * 100.0
                    if sp > gap:
                        gap = sp
                if gap >= self.threshold or not had_peer:
                    kyber_bases.append(base)
            if kyber_bases:
                # Kyber is the VALIDATION layer at alert-time (bot.py
                # re-quote block runs a size ladder for the DEX top entry
                # right before firing the alert). Hunter's dex_prices
                # stays OKX/DS — those give the URL, liq, spot price
                # that populate the alert body. So we DO NOT overwrite
                # new_dex[b] with Kyber here — that would replace OKX's
                # web3.okx.com URL with kyberswap.com and drop the
                # OKX-sourced liquidity/vol figures.
                if kyber_bases:
                    log.info("hunter: kyber will validate %d bases at alert-time",
                             len(kyber_bases))
        self.last_dex_prices = new_dex

    async def _dex_loop(self):
        """Background DEX refresher — decoupled from CEX cycle rate."""
        # Give the CEX loop a head start so bitvavo_prices/other_prices exist.
        await asyncio.sleep(2.0)
        interval = float(os.getenv("HUNT_DEX_CYCLE_SEC", "15"))
        while not self._stop.is_set():
            t0 = time.time()
            try:
                await self._dex_cycle()
            except Exception as e:
                log.exception("dex cycle err: %s", e)
            dt = time.time() - t0
            sleep = max(1.0, interval - dt)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=sleep)
            except asyncio.TimeoutError:
                pass

    async def run(self):
        self._session = aiohttp.ClientSession()
        try:
            await self._load_bitvavo_bases()
            await self._build_identity_maps()
            if self.dex_enabled:
                await self._warmup_pools()
            else:
                log.info("hunter: DEX disabled (HUNT_DEX_ENABLED=false) — Bitvavo vs CEX only")

            # Start WebSocket feeders — top-of-book pushed live, so cycle
            # reads bid/ask from an in-memory cache with sub-second staleness
            # instead of hitting REST every tick.
            import wsfeed
            other_exchanges = [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]
            bv_syms = {f"{b}/EUR" for b in self.bases}
            oth_symsets: dict[str, set[str]] = {e: set() for e in other_exchanges}
            for eid in other_exchanges:
                await cex._ensure_markets(eid)
                inst = cex._get(eid)
                symset = set(inst.symbols or [])
                for base in self.bases:
                    for q in ("USDT", "USDC"):
                        sym = f"{base}/{q}"
                        if sym in symset:
                            oth_symsets[eid].add(sym)
                            break
            await wsfeed.start(
                binance_symbols=oth_symsets.get("binance") or set(),
                bitvavo_symbols=bv_syms,
                gate_symbols=oth_symsets.get("gate") or set(),
            )
            log.info("hunter: ws started — bitvavo=%d, binance=%d, gate=%d",
                     len(bv_syms), len(oth_symsets.get("binance") or set()),
                     len(oth_symsets.get("gate") or set()))

            # Spawn background DEX refresh so CEX cycle can stay tight
            dex_task = None
            if self.dex_enabled or self.kyber_enabled:
                dex_task = asyncio.create_task(self._dex_loop())
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
            if dex_task:
                dex_task.cancel()
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
