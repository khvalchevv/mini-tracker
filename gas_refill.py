"""Auto-refill hot-wallet native gas from Gate spot.

Public entry point:
    await ensure_hot_gas(chain, min_usd=None)

Returns True if HW has enough gas AFTER this call, else False.
Rate-limits to 1 refill per (chain, hour) to avoid hammering Gate.
If Gate lacks native asset → auto-buys with USDT (spot IOC limit).
If USDT also insufficient → returns False; caller decides (skip/notify).

Both `_gas_refill_loop` (periodic) and any inline caller (before a
HW→dest send) should route through this function.
"""
import asyncio
import logging
import os
import time

log = logging.getLogger(__name__)

# chain → (native_asset, gate_network_label, trigger_usd, target_usd)
# trigger_usd = fire refill ONLY when HW < this threshold
# target_usd  = top up to this level when firing
# Rare/lazy refills — only when balance is truly low.
REFILL = {
    "ethereum":  ("ETH",  "ETH",       3,   30),  # trigger $3 → topup $30
    "bsc":       ("BNB",  "BSC",       1,   5),
    "arbitrum":  ("ETH",  "ARBITRUM",  0.5, 3),
    "base":      ("ETH",  "BASE",      0.5, 3),
    "optimism":  ("ETH",  "OPTIMISM",  0.5, 3),
    "polygon":   ("POL",  "MATIC",     0.3, 2),
    "avalanche": ("AVAX", "AVAXC",     0.5, 3),
}

# Fallback only — see `_native_price_usd`. These are stale by design:
# nobody updates a literal. Sizing orders against them is what broke
# gas refills (ETH 4000 vs ~2490, POL 0.4 vs ~0.097, AVAX 30 vs ~7.93),
# because a "$3.45" order was really $0.78-$2.49 and Gate rejects
# anything under $3 with INVALID_PARAM_VALUE. Every 5-minute cycle on
# optimism/polygon/avalanche logged the same failure for days.
NATIVE_PRICE = {"ETH": 4000, "BNB": 520, "POL": 0.4, "AVAX": 30}

_PRICE_CACHE: dict[str, tuple[float, float]] = {}    # asset → (usd, ts)
_PRICE_TTL_SEC = 300


async def _native_price_usd(asset: str) -> float:
    """Live USD price for a native gas asset, cached for 5 minutes.

    Falls back to `NATIVE_PRICE` when the ticker is unreachable, so a
    dead proxy degrades refill sizing instead of disabling gas entirely.
    """
    now = time.time()
    hit = _PRICE_CACHE.get(asset)
    if hit and now - hit[1] < _PRICE_TTL_SEC:
        return hit[0]
    try:
        import cex as _cex
        inst = _cex._get("gate")
        t = await inst.fetch_ticker(f"{asset}/USDT")
        px = float(t.get("last") or t.get("bid") or 0)
        if px > 0:
            _PRICE_CACHE[asset] = (px, now)
            return px
    except Exception as e:
        log.debug("gas-refill: live price %s failed (%s) — using fallback",
                  asset, e)
    return float(NATIVE_PRICE.get(asset, 0))

# Gate min WD/precision by native asset (rough, safe upper bound)
_GATE_MIN_WD = {"ETH": 0.001, "BNB": 0.005, "POL": 1.0, "AVAX": 0.01}

_last_refill: dict[str, float] = {}       # chain → ts of last refill
_RATE_LIMIT_SEC = 3600                    # 1 refill / chain / hour
# Chains whose Gate withdrawal is blocked by account config (address not
# whitelisted). Retrying can never succeed — park until restart.
_PERMANENT_WD_BLOCK: set[str] = set()


async def _hw_native_balance(chain: str) -> float | None:
    """Return HW native balance in whole units, or None on failure.
    Uses cached Web3 provider (dex._get_w3) — was making fresh HTTPProvider
    per call which churned connections under Alchemy 429."""
    try:
        from web3 import Web3
        import dex as _dex
        w3 = _dex._get_w3(chain)
        if not w3: return None
        wei = w3.eth.get_balance(Web3.to_checksum_address(
            _dex.HOT_WALLET_ADDRESS))
        return wei / 1e18
    except Exception as e:
        log.debug("hw balance %s err: %s", chain, e)
        return None


async def ensure_hot_gas(chain: str, min_usd: float | None = None,
                           bypass_rate_limit: bool = False) -> bool:
    """Ensure HW has >= threshold native gas for `chain`.

    If below threshold and not rate-limited, buys native on Gate (if
    Gate lacks native, buys it with USDT first) then withdraws to HW.

    Returns True if HW has enough gas after this call. False if:
    - Rate-limited (recent refill just happened; still may have gas)
    - Gate USDT insufficient
    - Buy/WD failed
    """
    if chain not in REFILL:
        return True                                # not our supported chain
    if chain in _PERMANENT_WD_BLOCK:
        return False                               # address not whitelisted
    asset, net, trigger_usd, target_usd = REFILL[chain]
    if min_usd is not None:
        trigger_usd = min_usd
        target_usd = max(target_usd, min_usd * 3)  # refill to at least 3× trigger

    native_h = await _hw_native_balance(chain)
    if native_h is None:
        return False
    # Live price, not the literal table: it drives BOTH the "do we need
    # gas?" test and the order size below, so a stale number both
    # misjudges the balance and builds an order the exchange rejects.
    price = await _native_price_usd(asset)
    if price <= 0:
        return False
    hw_usd = native_h * price
    # Fire refill ONLY when HW drops below the trigger; otherwise noop
    if hw_usd >= trigger_usd:
        return True

    # Compute deficit to reach TARGET (not just trigger — so we don't
    # ping-pong refilling $1 at a time)
    deficit_usd = target_usd - hw_usd
    topup = deficit_usd / price                    # native units to WD
    min_wd = _GATE_MIN_WD.get(asset, 0.001)
    if topup < min_wd:
        topup = min_wd                             # round up to Gate min

    # Rate-limit — but allow bypass for inline emergencies
    if not bypass_rate_limit:
        if time.time() - _last_refill.get(chain, 0) < _RATE_LIMIT_SEC:
            return False

    # Load Gate credentials + balance
    try:
        import keys as _k
        import cex as _cex
        kd = _k.load_keys()
        if "gate" not in kd:
            return False
        gate_inst = _cex.get_private("gate", kd["gate"])
        gate_bal = await gate_inst.fetch_balance()
    except Exception as e:
        log.warning("gas-refill %s: gate balance err: %s", chain, e)
        return False

    gfree = float((gate_bal.get(asset) or {}).get("free") or 0)

    # Buy missing native with USDT if Gate lacks it. Buy only what's
    # missing above what Gate already has — but no less than Gate's
    # min-precision lot (otherwise the order errors).
    if gfree < topup:
        need = max(topup - gfree, min_wd)
        est_cost_usd = need * price
        # Gate rejects spot orders below $3 notional. Our per-chain
        # targets are tiny ($2-3 of POL/AVAX), so `need` routinely came
        # out at $0.49 and every 5-min cycle logged the same
        # INVALID_PARAM_VALUE. Round the order up past the exchange
        # minimum instead — buying a few extra dollars of gas is fine.
        _min_order_usd = float(os.getenv("GATE_MIN_ORDER_USD", "3.0"))
        # Clear the floor with real headroom, not by a hair: ccxt rounds
        # the amount DOWN to the market's precision, so an order sized to
        # land exactly on $3.00 can arrive at $2.99 and get rejected.
        if est_cost_usd < _min_order_usd * 1.10:
            need = (_min_order_usd * 1.30) / price
            est_cost_usd = need * price
        gate_usdt = float((gate_bal.get("USDT") or {}).get("free") or 0)
        if gate_usdt < est_cost_usd * 1.02:
            log.warning("gas-refill %s: Gate lacks native (%.6f %s, need %f) "
                        "AND USDT (%.2f < needed %.2f)",
                        chain, gfree, asset, topup, gate_usdt, est_cost_usd * 1.02)
            return False
        try:
            sym = f"{asset}/USDT"
            ob = await gate_inst.fetch_order_book(sym, 5)
            best_ask = float(ob["asks"][0][0])
            px = best_ask * 1.01                   # +1% IOC headroom
            log.warning("gas-refill %s: BUY %.6f %s @ %.4f (~$%.2f) on Gate",
                        chain, need, asset, px, est_cost_usd)
            await _cex.place_order(gate_inst, sym, "limit", "buy",
                                    need, px, {"timeInForce": "IOC"})
            await asyncio.sleep(3)
            gate_bal = await gate_inst.fetch_balance()
            gfree = float((gate_bal.get(asset) or {}).get("free") or 0)
        except Exception as e:
            log.warning("gas-refill %s: buy err: %s", chain, e)
            return False

    # WD what we actually have (clipped to target so we don't drain
    # Gate if it happens to hold much more than we need).
    wd_amt = min(topup, gfree * 0.98)
    if wd_amt < min_wd:
        log.warning("gas-refill %s: after buy still under min_wd (%.6f < %f)",
                    chain, wd_amt, min_wd)
        return False

    try:
        import dex as _dex
        import cex as _cex
        log.warning("gas-refill %s: WD %f %s (~$%.2f) via %s → HW",
                    chain, wd_amt, asset, wd_amt * price, net)
        await _cex.withdraw_robust(gate_inst, asset, wd_amt,
                                    _dex.HOT_WALLET_ADDRESS, None, net)
        _last_refill[chain] = time.time()
        return True
    except Exception as e:
        msg = str(e)
        # Permanent config problems — retrying every 5 min forever just
        # spams the log. Gate's ADDRESS_NOT_USED means the HW address
        # isn't whitelisted for that network in the Gate account; only
        # the user can fix that, so park the chain until restart.
        if "ADDRESS_NOT_USED" in msg or "not allowed" in msg.lower():
            if chain not in _PERMANENT_WD_BLOCK:
                _PERMANENT_WD_BLOCK.add(chain)
                log.error("gas-refill %s: WD address NOT WHITELISTED on Gate "
                          "— park this chain until restart. Whitelist %s for "
                          "%s in Gate withdrawal settings. (%s)",
                          chain, _dex.HOT_WALLET_ADDRESS, net, msg[:160])
            return False
        log.warning("gas-refill %s: WD err: %s", chain, e)
        return False


async def periodic_loop() -> None:
    """Background loop — checks all supported chains every 5 min."""
    while True:
        await asyncio.sleep(300)
        for chain in REFILL:
            try:
                await ensure_hot_gas(chain)
            except Exception as e:
                log.debug("periodic gas-refill %s: %s", chain, e)
