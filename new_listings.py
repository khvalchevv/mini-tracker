"""New-listing auto-detector + auto-linker.

Priority chain when a NEW Bitvavo base appears:
  (1) Read Bitvavo assets → get name + networks (e.g. "The Interfold", ["ETH"])
  (2) CoinGecko search by name+symbol → resolve contract per chain
  (3) Cross-check Gate / Binance for the same contract → auto-bind
      CEX↔Bitvavo pair (best case: normal arb via bot)
  (4) If neither Gate nor Binance has it → fall back to DexScreener,
      pick top-liquidity pool → bind CEX↔DEX arb (via hunter's Kyber path)
  (5) If DexScreener also has nothing → TG notification asking for manual

Fully automatic; no /pool command needed unless everything fails.
"""
import asyncio
import json
import logging
import os
import time
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT_FILE = os.path.join(HERE, "bitvavo_bases_snapshot.json")
POLL_SEC = 60                                # 1 min — catch new listings fast

MIN_LIQ_USD = 100_000                         # min DEX pool liquidity

EVM_CHAINS = {"ethereum", "bsc", "polygon", "arbitrum", "optimism",
              "base", "avalanche", "linea", "scroll", "blast", "zksync"}

# Bitvavo network label → our canonical chain
BV_NET_MAP = {
    "ETH": "ethereum", "ERC20": "ethereum",
    "BSC": "bsc", "BEP20": "bsc",
    "MATIC": "polygon", "POLYGON": "polygon",
    "ARB": "arbitrum", "ARBITRUM": "arbitrum",
    "OP": "optimism", "OPTIMISM": "optimism",
    "BASE": "base",
    "AVAX": "avalanche", "AVAXC": "avalanche",
    "SOL": "solana",
}

# CoinGecko platform key → our canonical chain
CG_PLATFORM_MAP = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bsc",
    "polygon-pos": "polygon",
    "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism",
    "base": "base",
    "avalanche": "avalanche",
    "linea": "linea",
    "scroll": "scroll",
    "blast": "blast",
    "zksync": "zksync",
    "solana": "solana",
}

# DexScreener chain id → our canonical chain
DS_CHAIN_MAP = {
    "ethereum": "ethereum", "bsc": "bsc", "polygon": "polygon",
    "arbitrum": "arbitrum", "optimism": "optimism", "base": "base",
    "avalanche": "avalanche", "linea": "linea", "scroll": "scroll",
    "blast": "blast", "zksync": "zksync", "solana": "solana",
}


# ─── Snapshot ─────────────────────────────────────────────────────────
def _load_snapshot() -> set[str]:
    try:
        with open(SNAPSHOT_FILE) as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()
    except Exception:
        return set()


def _save_snapshot(bases: set[str]) -> None:
    try:
        with open(SNAPSHOT_FILE, "w") as f:
            json.dump(sorted(bases), f)
    except Exception as e:
        log.warning("snapshot save err: %s", e)


# ─── Lookups ──────────────────────────────────────────────────────────
async def _http_json(url: str, timeout: int = 10) -> Any:
    try:
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout)) as s:
            async with s.get(url, headers={
                    "User-Agent": "arb-bot/1.0",
                    "Accept": "application/json"}) as r:
                if r.status != 200:
                    return None
                return await r.json()
    except Exception as e:
        log.debug("http %s err: %s", url, e)
        return None


async def _bitvavo_asset_info(inst, base: str) -> dict:
    """Fetch Bitvavo public asset info — name + networks + WD fee."""
    try:
        r = await inst.publicGetAssets({"symbol": base})
        if isinstance(r, list):
            r = r[0] if r else {}
        return {
            "name": r.get("name") or "",
            "networks": [n.upper() for n in (r.get("networks") or [])],
            "decimals": int(r.get("decimals") or 18),
            "withdrawal_fee": float(r.get("withdrawalFee") or 0),
            "deposit_status": r.get("depositStatus"),
            "withdrawal_status": r.get("withdrawalStatus"),
        }
    except Exception as e:
        log.debug("bv asset %s err: %s", base, e)
        return {}


async def _cg_lookup_contract(base: str, name: str,
                                bitvavo_chains: set[str]) -> dict[str, str]:
    """Search CoinGecko by ticker, filter by matching name to disambiguate
    homonyms. Returns {chain: contract} for chains Bitvavo supports."""
    # 1. Search by ticker — returns list of candidate coin_ids
    j = await _http_json(
        f"https://api.coingecko.com/api/v3/search?query={base}")
    if not j:
        return {}
    coins = j.get("coins") or []
    # Prefer coins whose symbol matches AND name contains our name (or v.v.)
    def _score(c):
        s = 0
        if (c.get("symbol") or "").upper() == base.upper(): s += 10
        cname = (c.get("name") or "").lower()
        bname = (name or "").lower()
        if bname and cname:
            if bname == cname: s += 30
            elif bname in cname or cname in bname: s += 15
            # word overlap
            bw = set(bname.split()); cw = set(cname.split())
            s += min(10, len(bw & cw) * 3)
        return -s
    coins.sort(key=_score)
    if not coins:
        return {}
    for coin in coins[:3]:                    # try top-3 matches
        coin_id = coin.get("id")
        if not coin_id: continue
        j2 = await _http_json(
            f"https://api.coingecko.com/api/v3/coins/{coin_id}"
            f"?localization=false&tickers=false&market_data=false"
            f"&community_data=false&developer_data=false&sparkline=false")
        if not j2: continue
        platforms = j2.get("platforms") or {}
        out: dict[str, str] = {}
        for cg_key, addr in platforms.items():
            if not addr: continue
            chain = CG_PLATFORM_MAP.get(cg_key)
            if not chain: continue
            if bitvavo_chains and chain not in bitvavo_chains: continue
            out[chain] = addr.lower()
        if out:
            log.info("cg matched %s → %s (id=%s, score=%.0f)",
                     base, out, coin_id, -_score(coin))
            return out
    return {}


async def _ds_search(base: str) -> list[dict]:
    j = await _http_json(
        f"https://api.dexscreener.com/latest/dex/search?q={base}")
    if not j: return []
    return j.get("pairs") or []


def _best_ds_pool(base: str, pairs: list[dict],
                    allowed_chains: set[str]) -> dict | None:
    """Highest-liquidity pool where base=BASE, chain in allowed."""
    candidates = []
    for p in pairs:
        base_sym = ((p.get("baseToken") or {}).get("symbol") or "").upper()
        if base_sym != base.upper(): continue
        chain_ds = (p.get("chainId") or "").lower()
        chain = DS_CHAIN_MAP.get(chain_ds)
        if not chain or (allowed_chains and chain not in allowed_chains):
            continue
        liq_usd = float((p.get("liquidity") or {}).get("usd") or 0)
        if liq_usd < MIN_LIQ_USD: continue
        candidates.append({
            "chain": chain,
            "contract": ((p.get("baseToken") or {}).get("address") or "").lower(),
            "liq_usd": liq_usd,
            "dex_id": p.get("dexId"),
            "url": p.get("url"),
        })
    candidates.sort(key=lambda c: -c["liq_usd"])
    return candidates[0] if candidates else None


def _cex_has_contract(cex_mod, eid: str, base: str,
                        expected_contract: str) -> str | None:
    """Return chain name if this CEX lists base with the matching contract."""
    try:
        for n in cex_mod.network_info(eid, base):
            c = (n.get("contract") or "").lower()
            if c and c == expected_contract.lower():
                # normalize network label to our canonical chain
                from chains import canonical as _cnl
                return _cnl(n.get("network")) or (n.get("network") or "").lower()
    except Exception as e:
        log.debug("_cex_has %s/%s err: %s", eid, base, e)
    return None


# ─── Main processor ───────────────────────────────────────────────────
async def _notify(app, subs: list[int], text: str) -> None:
    for cid in subs:
        try:
            await app.bot.send_message(cid, text, parse_mode="HTML",
                                         disable_web_page_preview=True)
        except Exception as e:
            log.debug("notify %s err: %s", cid, e)


async def _process_new(base: str, app, hunter, subs: list[int],
                        cex_mod, inst_bv) -> None:
    """Auto-link a newly-listed base — ALWAYS wire CEX-matches AND DEX pool
    (DEX is not fallback, both channels always active)."""
    # (1) Bitvavo metadata
    bv = await _bitvavo_asset_info(inst_bv, base)
    name = bv.get("name") or "?"
    bv_nets = {BV_NET_MAP.get(n) for n in (bv.get("networks") or [])
               if BV_NET_MAP.get(n)}
    header = (f"🆕 <b>{base}</b> — новий лістинг на Bitvavo\n"
              f"  <i>{name}</i>  ·  networks: {', '.join(sorted(bv_nets)) or '?'}  ·  "
              f"WD fee: {bv.get('withdrawal_fee'):.2f} {base}")

    # (2) CoinGecko contract lookup — disambiguated by project name
    cg_map = await _cg_lookup_contract(base, name, bv_nets)

    # (3) In PARALLEL: check every CEX for same contract + DexScreener
    cex_matches: list[tuple[str, str, str]] = []      # (eid, chain, contract)
    if cg_map:
        for chain, contract in cg_map.items():
            for eid in ("gate", "binance"):
                hit_chain = _cex_has_contract(cex_mod, eid, base, contract)
                if hit_chain:
                    cex_matches.append((eid, hit_chain, contract))

    ds_pairs = await _ds_search(base)
    # Use CG-known chains if any, else Bitvavo chains, else all EVM+SOL
    allowed = set(cg_map.keys()) if cg_map else (bv_nets or EVM_CHAINS | {"solana"})
    dex_pool = _best_ds_pool(base, ds_pairs, allowed)

    # (4) Wire whatever we found — CEX pairs AND DEX pool (both if both)
    wired_summary: list[str] = []
    for eid, chain, contract in cex_matches:
        _wire_hunter(hunter, base, chain, contract)
        wired_summary.append(f"✅ <b>{eid}</b> ({chain})  <code>{contract[:14]}…</code>")

    if dex_pool:
        _wire_hunter(hunter, base, dex_pool["chain"], dex_pool["contract"])
        wired_summary.append(
            f"🌊 <b>DEX</b> ({dex_pool['chain']}, {dex_pool.get('dex_id')}) "
            f"liq <b>${dex_pool['liq_usd']:,.0f}</b>  "
            f'<a href="{dex_pool.get("url") or ""}">пул</a>')

    # (5) Notify
    if not wired_summary:
        cg_desc = "; ".join(f"{c}:{a[:10]}…" for c, a in (cg_map or {}).items())
        cg_line = f"\n<i>CG: {cg_desc or 'нічого'}</i>"
        await _notify(app, subs, header + cg_line +
            f"\n❌ Ні на CEX (Gate/Binance), ні на DexScreener (&gt;${MIN_LIQ_USD:,.0f} liq) нема. "
            f"Тільки Bitvavo — арб неможливий.\n"
            f"Пошук: https://dexscreener.com/search?q={base}")
        return

    await _notify(app, subs, header +
        "\n<b>Прив'язано канали:</b>\n" + "\n".join("  " + s for s in wired_summary))


def _wire_hunter(hunter, base: str, chain: str, contract: str) -> None:
    """Insert base into hunter's identity maps so DEX/CEX quoting starts
    IMMEDIATELY (not after next cycle). Also subscribes WSfeed if needed."""
    try:
        base_u = base.upper()
        hunter.base_to_contracts.setdefault(base_u, {})[chain] = contract
        if base_u not in hunter.base_to_coin_id:
            hunter.base_to_coin_id[base_u] = f"auto_{base_u.lower()}"
        # Force DS pool refresh
        hunter.pool_cache.pop(base_u, None)
        hunter.pool_ts.pop(base_u, None)
        # Add to hunter's bases list so next cycle iterates over it
        if base_u not in hunter.bases:
            hunter.bases.append(base_u); hunter.bases.sort()
        # Try WSfeed resubscribe (Bitvavo/Gate/Binance) — dynamic add
        try:
            import wsfeed
            for eid in ("bitvavo", "gate", "binance"):
                add = getattr(wsfeed, "add_symbol", None)
                if callable(add):
                    quote = "EUR" if eid == "bitvavo" else "USDT"
                    add(eid, f"{base_u}/{quote}")
        except Exception as _e:
            log.debug("wsfeed add err: %s", _e)
    except Exception as e:
        log.warning("hunter wire %s err: %s", base, e)


# ─── Loop ─────────────────────────────────────────────────────────────
async def watch_loop(app, hunter_provider, subs_provider) -> None:
    """Long-running loop. `hunter_provider` / `subs_provider` = callables."""
    await asyncio.sleep(30)                        # let bot warm up
    import cex as _cex_mod
    inst_bv = _cex_mod._get("bitvavo")
    prev = _load_snapshot()
    while True:
        try:
            hunter = hunter_provider()
            if not hunter or not hunter.bases:
                await asyncio.sleep(POLL_SEC); continue
            cur = set(hunter.bases)
            if not prev:
                _save_snapshot(cur); prev = cur
                log.info("new_listings: baseline of %d bases saved", len(cur))
                await asyncio.sleep(POLL_SEC); continue
            new_bases = cur - prev
            if new_bases:
                subs = subs_provider() or []
                log.info("new_listings: %d new bases: %s",
                         len(new_bases), sorted(new_bases))
                for base in sorted(new_bases):
                    try:
                        await _process_new(base, app, hunter, subs,
                                            _cex_mod, inst_bv)
                    except Exception as e:
                        log.warning("process_new %s err: %s", base, e)
            _save_snapshot(cur); prev = cur
        except Exception as e:
            log.warning("new_listings loop err: %s", e)
        await asyncio.sleep(POLL_SEC)
