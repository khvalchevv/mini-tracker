"""Startup pre-cache of everything needed for zero-latency SEND.

Two persistent maps live in `token_cache.json`:

1. `deposit_addresses`  — per (exchange, canonical_chain) → address.
   EVM: one address per chain covers ALL ERC-20 tokens on that chain
   (deposit addresses are per-account per-chain, not per-token).
   Solana: one SPL-token deposit address per exchange.
   Fetched once via `fetch_deposit_address` and cached forever.

2. `tokens` — per base ticker → {chain, contract, per-exchange status}
   Contracts come only from Gate/Binance (Bitvavo doesn't expose them).
   Chain is picked as the fastest EVM/Solana common chain between the
   source and destination exchanges.

Refresh: 24h TTL; call `/precache refresh` to force early.

Non-EVM chains (BTC, TRX, XRP…) are skipped — we can't send them from
the hot wallet anyway.
"""
import asyncio
import json
import logging
import os
import time

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(HERE, "token_cache.json")
CACHE_TTL = 24 * 3600

# EVM chains we send from — must match dex.KYBER_CHAIN keys
EVM_CHAINS = {"ethereum", "bsc", "polygon", "arbitrum", "optimism",
              "base", "avalanche", "linea", "scroll", "blast", "zksync"}
SUPPORTED_CHAINS = EVM_CHAINS | {"solana"}


def _load() -> dict:
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            d = json.load(f)
        d.setdefault("deposit_addresses", {})
        d.setdefault("tokens", {})
        d.setdefault("generated_ts", 0)
        return d
    except FileNotFoundError:
        return {"deposit_addresses": {}, "tokens": {}, "generated_ts": 0}
    except Exception as e:
        log.warning("token_cache load err: %s", e)
        return {"deposit_addresses": {}, "tokens": {}, "generated_ts": 0}


def _save(d: dict) -> None:
    try:
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        log.warning("token_cache save err: %s", e)


# ─── Public read API ────────────────────────────────────────────────
_CACHE: dict = _load()


def get_deposit_address(eid: str, chain: str) -> str | None:
    return ((_CACHE.get("deposit_addresses") or {}).get(eid) or {}).get(chain)


def get_token(base: str) -> dict | None:
    """Return {chain, contract, gate/binance/bitvavo entries} or None."""
    return (_CACHE.get("tokens") or {}).get(base.upper())


def is_fresh() -> bool:
    return time.time() - _CACHE.get("generated_ts", 0) < CACHE_TTL


# ─── Build/refresh ──────────────────────────────────────────────────
async def _fetch_dep_addr(inst, chain: str, sample_base: str,
                            network_label: str) -> str | None:
    """Fetch one deposit address to represent the whole (exchange, chain).
    Validates that the returned address MATCHES the chain family (EVM
    format 0x… vs Solana base58) — some exchanges silently return the
    EVM fallback address when asked for an unsupported network."""
    import cex as _cex
    try:
        r = await _cex.fetch_deposit_address_robust(
            inst, sample_base, network_label)
        addr = (r or {}).get("address")
        if not addr:
            return None
        # Chain-format sanity
        if chain in EVM_CHAINS:
            if not addr.startswith("0x") or len(addr) != 42:
                log.info("dep_addr %s/%s/%s: not EVM-format (%s) — reject",
                         getattr(inst, "id", "?"), chain, sample_base, addr[:20])
                return None
        elif chain == "solana":
            # Solana = base58, no 0x prefix, length 32-44
            if addr.startswith("0x") or not (32 <= len(addr) <= 44):
                log.info("dep_addr %s/%s/%s: not SOL-format (%s) — reject",
                         getattr(inst, "id", "?"), chain, sample_base, addr[:20])
                return None
        return addr
    except Exception as e:
        log.debug("dep_addr fetch %s/%s/%s: %s",
                  getattr(inst, "id", "?"), chain, sample_base, e)
        return None


# Sample coin to use per chain when asking the exchange for its
# deposit address. Any widely-listed coin on that chain works; we
# only need the address, which is same for every ERC-20 on the chain.
# List of (coin, network_label) candidates to try per chain. Exchanges
# differ on which coins they list on which chains — Bitvavo has USDC on
# ETH but not USDT, Gate has both, Binance has USDT everywhere. Try in
# order; first success wins. The address returned is per (account,
# chain), NOT per token, so any coin on that chain gives the right one.
_SAMPLE_COIN_PER_CHAIN = {
    "ethereum":  [("USDC", "ETH"), ("USDT", "ETH"), ("ETH", "ETH")],
    "bsc":       [("USDT", "BSC"), ("USDC", "BSC"), ("BNB", "BSC")],
    "polygon":   [("USDT", "MATIC"), ("USDC", "MATIC"), ("POL", "MATIC")],
    "arbitrum":  [("USDT", "ARBITRUM"), ("USDC", "ARBITRUM"), ("ETH", "ARBITRUM")],
    "optimism":  [("USDT", "OPTIMISM"), ("USDC", "OPTIMISM"), ("ETH", "OPTIMISM")],
    "base":      [("USDC", "BASE"), ("ETH", "BASE")],
    "avalanche": [("USDT", "AVAXC"), ("USDC", "AVAXC"), ("AVAX", "AVAXC")],
    "linea":     [("USDC", "LINEA"), ("ETH", "LINEA")],
    "scroll":    [("USDC", "SCROLL"), ("ETH", "SCROLL")],
    "blast":     [("USDB", "BLAST"), ("ETH", "BLAST")],
    "zksync":    [("USDC", "ZKSYNC"), ("ETH", "ZKSYNC")],
    "solana":    [("SOL", "SOL"), ("USDC", "SOL")],
}


async def refresh(exchanges: list[str] | None = None,
                   force: bool = False) -> dict:
    """Rebuild the cache. `exchanges` defaults to cex.SUPPORTED_EXCHANGES."""
    global _CACHE
    if not force and is_fresh() and _CACHE.get("tokens"):
        return _CACHE
    import cex as _cex
    import keys as _keys
    from chains import canonical as _cnl
    if exchanges is None:
        exchanges = list(_cex.SUPPORTED_EXCHANGES)
    kd = _keys.load_keys()

    # Ensure markets loaded for every exchange in the list
    for eid in exchanges:
        try:
            await _cex._ensure_markets(eid)
        except Exception as e:
            log.warning("precache: %s markets load fail: %s", eid, e)

    # ─── 1) Deposit addresses per (exchange, chain) ─────────────────
    dep_addrs: dict[str, dict[str, str]] = {}
    for eid in exchanges:
        creds = kd.get(eid) or {}
        if not creds.get("apiKey"):
            continue
        try:
            inst = _cex.get_private(eid, creds)
        except Exception:
            continue
        if eid == "binance":
            try: await inst.load_time_difference()
            except Exception: pass
        dep_addrs[eid] = {}
        for chain, candidates in _SAMPLE_COIN_PER_CHAIN.items():
            for sample, net_label in candidates:
                addr = await _fetch_dep_addr(inst, chain, sample, net_label)
                if addr:
                    dep_addrs[eid][chain] = addr
                    log.info("precache %s/%s → %s (via %s)",
                             eid, chain, addr[:10] + "…", sample)
                    break
        log.info("precache: %s → %d chain addresses",
                 eid, len(dep_addrs[eid]))

    # ─── 2) Token map from cached currency data (no extra API calls) ─
    tokens: dict[str, dict] = {}
    bases: set[str] = set()
    for eid in exchanges:
        inst = _cex._instances.get(eid)
        if not inst or not inst.currencies:
            continue
        bases.update(inst.currencies.keys())

    for base in sorted(bases):
        entry: dict = {}
        # Contract preference: gate → binance (they expose it;
        # bitvavo doesn't). Choose the chain each exchange uses.
        for eid in ("gate", "binance"):
            if eid not in exchanges:
                continue
            for n in _cex.network_info(eid, base):
                chain = _cnl(n.get("network")) or (n.get("network") or "").lower()
                if chain not in SUPPORTED_CHAINS:
                    continue
                if not n.get("contract"):
                    continue
                entry.setdefault(chain, {})
                entry[chain].setdefault("contract", n["contract"])
                entry[chain].setdefault("networks", {})
                entry[chain]["networks"][eid] = {
                    "network": n.get("network"),
                    "wd": n.get("withdraw"),
                    "dep": n.get("deposit"),
                    "fee": n.get("fee"),
                }
        # Bitvavo: no contract, but record status so precheck can use it.
        if "bitvavo" in exchanges:
            for n in _cex.network_info("bitvavo", base):
                chain = _cnl(n.get("network")) or (n.get("network") or "").lower()
                if chain not in SUPPORTED_CHAINS:
                    continue
                entry.setdefault(chain, {})
                entry[chain].setdefault("networks", {})
                entry[chain]["networks"]["bitvavo"] = {
                    "network": n.get("network"),
                    "wd": n.get("withdraw"),
                    "dep": n.get("deposit"),
                    "fee": n.get("fee"),
                }
        if entry:
            tokens[base.upper()] = entry

    _CACHE = {
        "generated_ts": time.time(),
        "deposit_addresses": dep_addrs,
        "tokens": tokens,
    }
    _save(_CACHE)
    log.info("precache built: %d tokens, %d exchanges with addresses",
             len(tokens), len(dep_addrs))
    return _CACHE


async def refresh_periodic():
    """Background loop — rebuilds every CACHE_TTL seconds."""
    while True:
        try:
            await refresh()
        except Exception as e:
            log.warning("precache periodic err: %s", e)
        await asyncio.sleep(CACHE_TTL)
