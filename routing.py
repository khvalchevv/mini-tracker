"""Multi-hop USDC routing between the hot wallet and exchanges.

Bitvavo only accepts USDC on Ethereum. When the working capital sits on
Base — as $2736 did while Bitvavo ran down to 681 EUR — a direct send is
impossible and the rebalancer stalls: it can see the money but not move
it.

Binance is the way across; it takes USDC deposits on Base, Arbitrum,
Optimism, Polygon and more, and withdraws on any of them. So the route
becomes:

    HW(base) --deposit--> Binance --withdraw--> HW(ethereum) --> Bitvavo

Deliberately hopping back through our own wallet instead of withdrawing
straight to Bitvavo's deposit address: exchange-to-exchange transfers
draw compliance attention on both ends, and a failed leg is far easier
to recover from when the funds land somewhere we control.
"""
import asyncio
import logging
import os
import time

log = logging.getLogger(__name__)

# our chain slug → the network label each exchange uses for USDC
_NET_LABEL = {
    "binance": {
        "ethereum": "ERC20", "base": "BASE", "arbitrum": "ARBITRUM",
        "optimism": "OPTIMISM", "polygon": "MATIC", "bsc": "BEP20",
        "avalanche": "AVAXC", "solana": "SPL", "zksync": "ZKSYNCERA",
    },
    "gate": {"ethereum": "ERC20"},
    "bitvavo": {"ethereum": "ETH"},
}

# Exchanges usable as a bridge, best first. Binance has by far the
# widest USDC network coverage.
_BRIDGE_PREFERENCE = ["binance", "gate"]

# USDC parked on an exchange mid-route: {eid: amount}. The rebalancer's
# `_normalize` treats stray USDC on Gate/Binance as dead weight and sells
# it to USDT — which is correct for idle funds and catastrophic for a
# bridge hop. It ate 737 USDC three minutes after leg 1 landed, killing
# the route. Anything reserved here is invisible to the normalizer.
_IN_TRANSIT: dict[str, float] = {}


def reserve_transit(eid: str, amount: float) -> None:
    _IN_TRANSIT[eid] = _IN_TRANSIT.get(eid, 0.0) + float(amount)
    log.warning("transit reserve %s +%.2f (total %.2f)",
                eid, amount, _IN_TRANSIT[eid])


def release_transit(eid: str, amount: float | None = None) -> None:
    if amount is None:
        _IN_TRANSIT.pop(eid, None)
        return
    left = _IN_TRANSIT.get(eid, 0.0) - float(amount)
    if left <= 0.01:
        _IN_TRANSIT.pop(eid, None)
    else:
        _IN_TRANSIT[eid] = left


def transit_amount(eid: str) -> float:
    """How much USDC on `eid` belongs to an in-flight route."""
    return _IN_TRANSIT.get(eid, 0.0)


# Withdrawal errors worth waiting out. Everything else is a settled
# fact — a whitelist gap, a format rule, a disabled network — and
# retrying it just burns minutes. (The first version backed off
# 60/120/180/240s against "amount must be an integer", which was never
# going to succeed.)
_TRANSIENT_WD = (
    "insufficient balance",      # deposit still settling
    "insufficient funds",
    "try again",
    "timeout",
    "timed out",
    "too many requests",
    "429",
    "503",
    "system busy",
    "temporarily",
)


def _is_transient_wd_error(msg: str) -> bool:
    m = (msg or "").lower()
    return any(k in m for k in _TRANSIENT_WD)


# Withdrawal errors that cannot clear inside the lifetime of a run.
# `_TRANSIENT_WD` above answers "is it worth waiting out?"; this answers
# "is waiting pointless?". The two lists are deliberately separate: an
# unrecognised error stays retryable, because a trading leg must not
# abandon a filled position over a phrase nobody has classified yet.
#
# Bitvavo errorCode 434 is the rolling 24h withdrawal cap — a settled
# fact until the window rolls. The 10_000-retry loop in bot.py ground
# against it for 8 minutes while $1,625 of IOST sat on the exchange and
# the auto-rebalancer stayed blocked behind the live session.
_PERMANENT_WD = (
    "exceeds your allowed limit",
    "withdrawal limit",
    '"errorcode":434',
    "withdrawals are disabled",
    "withdrawal is disabled",
    "withdrawal suspended",
    "address_not_used",
    "address not used",
    "not whitelisted",
    "verification required",
)


def is_permanent_wd_error(msg: str) -> bool:
    """True when retrying this withdrawal cannot succeed in this run.

    Callers are expected to stop retrying and unwind the position rather
    than hold inventory they have no path out of.
    """
    m = (msg or "").lower()
    return any(k in m for k in _PERMANENT_WD)


# ── Withdrawal-block memory ────────────────────────────────────────────
# A blocked withdrawal is a property of the VENUE, not of the run that
# happened to discover it. Without this, each new run rediscovers the
# block the expensive way: buy the alt, get refused, sell it back at a
# loss. On the night of 2026-09-10 that loop ran 20 times in 43 minutes
# and cost EUR 426 on LAPTOP alone.
#
# This is a cooldown, NOT a blacklist. It only stops us OPENING new
# positions that would need this venue's withdrawal; a position already
# held always attempts its withdrawal regardless. When the cooldown
# lapses the next run probes the venue for real, and a withdrawal that
# succeeds clears the block immediately.
_WD_BLOCKED: dict[str, float] = {}                    # eid → blocked-until ts
_WD_BLOCK_DEFAULT_SEC = 3600.0


def note_wd_blocked(eid: str, reason: str = "") -> float:
    """Mark `eid` as unable to withdraw. Returns the cooldown seconds."""
    cool = float(os.getenv("WD_BLOCK_COOLDOWN_SEC", _WD_BLOCK_DEFAULT_SEC))
    _WD_BLOCKED[eid] = time.time() + cool
    log.warning("routing: %s withdrawals marked blocked for %.0f min (%s)",
                eid, cool / 60.0, (reason or "")[:120])
    return cool


def wd_block_remaining(eid: str) -> float:
    """Seconds until `eid` may be used for a withdrawal-dependent run."""
    until = _WD_BLOCKED.get(eid)
    if not until:
        return 0.0
    left = until - time.time()
    if left <= 0:
        _WD_BLOCKED.pop(eid, None)
        return 0.0
    return left


def clear_wd_block(eid: str) -> None:
    """A withdrawal went through — the venue is healthy again."""
    if _WD_BLOCKED.pop(eid, None) is not None:
        log.info("routing: %s withdrawal block cleared (withdrawal succeeded)",
                 eid)


def net_label(eid: str, chain: str) -> str | None:
    return (_NET_LABEL.get(eid) or {}).get(chain)


def accepts(eid: str, chain: str) -> bool:
    """Can `eid` take a USDC deposit on `chain`?"""
    return net_label(eid, chain) is not None


def chains_for(eid: str) -> list[str]:
    return list((_NET_LABEL.get(eid) or {}).keys())


def plan_route(target_eid: str, hw_balances: dict[str, float],
               need_usd: float, min_leg_usd: float = 20.0) -> dict | None:
    """Work out how to get `need_usd` of USDC from the hot wallet to
    `target_eid`.

    `hw_balances` is {chain: usd_on_that_chain}.

    Returns either
        {"kind": "direct", "chain": c, "amount": n}
    or
        {"kind": "bridge", "via": eid, "from_chain": c1, "to_chain": c2,
         "amount": n}
    or None when no path carries a worthwhile amount.
    """
    funded = sorted(((c, v) for c, v in hw_balances.items()
                     if v >= min_leg_usd),
                    key=lambda kv: -kv[1])
    if not funded:
        return None

    # 1) Direct — target accepts a chain we already hold
    direct = None
    for chain, have in funded:
        if accepts(target_eid, chain):
            direct = {"kind": "direct", "chain": chain,
                      "amount": min(have, need_usd)}
            break
    # A direct hop that covers the whole need always wins — one leg,
    # one fee, minutes instead of an hour.
    if direct and direct["amount"] >= need_usd * 0.95:
        return direct

    # 2) Bridge — move to a chain the target does accept. Worth doing
    # when the direct path can't carry enough: Bitvavo takes USDC only
    # on Ethereum, so with $266 on ETH and $2736 on Base, going direct
    # delivers $266 and leaves Bitvavo starving next to idle capital.
    target_chains = chains_for(target_eid)
    bridge = None
    for chain, have in funded:
        if direct and chain == direct["chain"]:
            continue                  # already counted as the direct leg
        for via in _BRIDGE_PREFERENCE:
            if via == target_eid or not accepts(via, chain):
                continue
            for tc in target_chains:
                if not accepts(via, tc):
                    continue          # bridge can't pay out on that chain
                cand = {"kind": "bridge", "via": via,
                        "from_chain": chain, "to_chain": tc,
                        "amount": min(have, need_usd)}
                if not bridge or cand["amount"] > bridge["amount"]:
                    bridge = cand
                break
            if bridge:
                break
    # A bridge costs two withdrawal fees (~$0.80) plus an hour of
    # waiting — not worth it for small amounts. Below the floor, take
    # whatever the direct path can carry instead.
    _min_bridge = float(os.getenv("ROUTE_MIN_BRIDGE_USD", "150"))
    if bridge and bridge["amount"] < _min_bridge:
        bridge = None
    # Take whichever actually delivers more; ties go to direct.
    if bridge and (not direct or bridge["amount"] > direct["amount"]):
        return bridge
    return direct


def describe(route: dict, target_eid: str) -> str:
    if not route:
        return "маршрут не знайдено"
    if route["kind"] == "direct":
        return (f"HW({route['chain']}) → {target_eid} · "
                f"${route['amount']:,.0f}")
    return (f"HW({route['from_chain']}) → {route['via']} → "
            f"HW({route['to_chain']}) → {target_eid} · "
            f"${route['amount']:,.0f}")


async def _wait_hw_usdc(chain: str, baseline: float, expect: float,
                        timeout_sec: float, step=None) -> float:
    """Poll HW USDC on `chain` until it grows by ~expect. Returns delta."""
    import dex
    deadline = time.time() + timeout_sec
    usdc = dex.USDC_BY_CHAIN.get(chain)
    while time.time() < deadline:
        await asyncio.sleep(20)
        try:
            now = (await dex.wallet_token_balance(chain, usdc) or 0) / 1e6
        except Exception:
            continue
        delta = now - baseline
        if delta >= expect * 0.85:
            return delta
        if step and int(time.time()) % 120 < 20:
            step(f"   ⏳ чекаю на HW({chain}): +{delta:,.2f} / "
                 f"{expect:,.2f} USDC")
    return 0.0


async def _wait_cex_usdc(inst, baseline: float, expect: float,
                         timeout_sec: float, step=None) -> float:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        await asyncio.sleep(20)
        try:
            b = await inst.fetch_balance()
            now = float((b.get("USDC") or {}).get("free") or 0)
        except Exception:
            continue
        delta = now - baseline
        if delta >= expect * 0.85:
            return delta
    return 0.0


async def move_hw_cross_chain(from_chain: str, to_chain: str,
                              amount_usd: float, via: str,
                              step=None) -> dict:
    """Move USDC between two chains on OUR OWN wallet, via `via`.

    `execute_route`'s bridge path ends at an exchange; this one ends back
    on the hot wallet. Needed because the wallet itself gets lopsided —
    Base drained to $0 by a run while $1,796 sat on Ethereum, so the next
    three attempts died on an empty wallet with the money one chain away.
    """
    import cex
    import dex
    import keys

    def _s(msg):
        log.warning("hw-xchain: %s", msg)
        if step:
            try:
                step(msg)
            except Exception:
                pass

    if not (accepts(via, from_chain) and accepts(via, to_chain)):
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"{via} does not serve both {from_chain} and {to_chain}"}
    kd = keys.load_keys()
    hop_timeout = float(os.getenv("ROUTE_HOP_TIMEOUT_SEC", "1800"))
    inst = cex.get_private(via, kd[via])
    await inst.load_markets()

    # leg 1: HW(from) → exchange
    label_in = net_label(via, from_chain)
    dep = await cex.fetch_deposit_address_robust(inst, "USDC", label_in)
    addr = (dep or {}).get("address")
    if not addr:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"no USDC/{label_in} deposit address on {via}"}
    b0 = await inst.fetch_balance()
    base0 = float((b0.get("USDC") or {}).get("free") or 0)
    usdc_from = dex.USDC_BY_CHAIN[from_chain]
    _s(f"HW({from_chain}) → {via}: {amount_usd:,.2f} USDC ({label_in})…")
    res = await dex.send_token(from_chain, usdc_from,
                                 int(amount_usd * 1e6), addr)
    if not res.get("ok"):
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"leg1 send: {res.get('error')}"}
    got = await _wait_cex_usdc(inst, base0, amount_usd, hop_timeout, step)
    if got <= 0:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"{via} never credited the deposit"}
    reserve_transit(via, got)

    # leg 2: exchange → HW(to)
    label_out = net_label(via, to_chain)
    usdc_to = dex.USDC_BY_CHAIN[to_chain]
    hw_before = (await dex.wallet_token_balance(to_chain, usdc_to) or 0) / 1e6
    wd = got * 0.999
    _s(f"{via} → HW({to_chain}): {wd:,.2f} USDC ({label_out})…")
    ok = False
    err = None
    for attempt in range(4):
        try:
            await cex.withdraw_robust(inst, "USDC", wd,
                                        dex.HOT_WALLET_ADDRESS, None,
                                        label_out)
            ok = True
            break
        except Exception as e:
            err = str(e)
            if not _is_transient_wd_error(err):
                _s(f"❌ {via} відмовив остаточно: {err[:120]}")
                break
            await asyncio.sleep(60 * (attempt + 1))
    if not ok:
        release_transit(via)
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"leg2 withdraw: {(err or '')[:140]}; "
                         f"${got:,.2f} USDC on {via}"}
    landed = await _wait_hw_usdc(to_chain, hw_before, wd, hop_timeout, step)
    release_transit(via)
    if landed <= 0:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"payout never landed on HW({to_chain})"}
    _s(f"✅ HW({to_chain}) отримав {landed:,.2f} USDC")
    return {"ok": True, "delivered_usd": landed, "error": None}


async def move_cex_to_cex(src_eid: str, dst_eid: str, amount_usd: float,
                          step=None) -> dict:
    """Move USDC from one exchange to another via the hot wallet.

    Needed because capital also gets stranded ON an exchange, not just
    on-chain: after a failed bridge leg, $736 sat on Binance (over its
    target) while Bitvavo ran $943 short — and the trigger-based
    rebalancer ignored both because neither had fallen below $1000.

    Goes through our own wallet rather than exchange-to-exchange, same
    reasoning as `execute_route`.
    """
    import cex
    import dex
    import keys

    def _s(msg):
        log.warning("cex-move: %s", msg)
        if step:
            try:
                step(msg)
            except Exception:
                pass

    kd = keys.load_keys()
    hop_timeout = float(os.getenv("ROUTE_HOP_TIMEOUT_SEC", "1800"))
    # Pick a chain both ends support
    chain = None
    for c in chains_for(src_eid):
        if accepts(dst_eid, c):
            chain = c
            break
    if not chain:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"no shared USDC network between {src_eid} "
                         f"and {dst_eid}"}
    label_src = net_label(src_eid, chain)
    inst_src = cex.get_private(src_eid, kd[src_eid])
    await inst_src.load_markets()

    # The source may hold USDT (the normalizer converts idle USDC), so
    # buy USDC first if needed.
    try:
        b = await inst_src.fetch_balance()
        have_usdc = float((b.get("USDC") or {}).get("free") or 0)
        if have_usdc < amount_usd:
            need = amount_usd - have_usdc
            _s(f"   · {src_eid}: купую {need:,.2f} USDC за USDT…")
            await cex.place_order(inst_src, "USDC/USDT", "limit", "buy",
                                    need, 1.005, {"timeInForce": "IOC"})
            await asyncio.sleep(4)
            b = await inst_src.fetch_balance()
            have_usdc = float((b.get("USDC") or {}).get("free") or 0)
    except Exception as e:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"{src_eid} USDT→USDC: {str(e)[:140]}"}
    send_amt = min(amount_usd, have_usdc * 0.999)
    if send_amt < 20:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"{src_eid} only has {have_usdc:.2f} USDC"}

    reserve_transit(src_eid, send_amt)
    usdc_c = dex.USDC_BY_CHAIN[chain]
    hw_before = (await dex.wallet_token_balance(chain, usdc_c) or 0) / 1e6
    _s(f"   · {src_eid} → HW({chain}): {send_amt:,.2f} USDC ({label_src})…")
    ok = False
    err = None
    for attempt in range(4):
        try:
            await cex.withdraw_robust(inst_src, "USDC", send_amt,
                                        dex.HOT_WALLET_ADDRESS, None,
                                        label_src)
            ok = True
            break
        except Exception as e:
            err = str(e)
            if not _is_transient_wd_error(err):
                _s(f"   ❌ {src_eid} відмовив остаточно: {err[:120]}")
                break                       # retrying won't change it
            wait = 60 * (attempt + 1)
            _s(f"   ⏳ {src_eid} не віддає ({str(e)[:80]}) — "
               f"повтор через {wait}s")
            await asyncio.sleep(wait)
    if not ok:
        release_transit(src_eid)
        log.error("cex-move %s→%s withdraw failed: %s", src_eid, dst_eid,
                  (err or "")[:200])
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"{src_eid} withdraw: {(err or '')[:140]}"}
    landed = await _wait_hw_usdc(chain, hw_before, send_amt,
                                  hop_timeout, step)
    release_transit(src_eid)
    if landed <= 0:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"{src_eid} payout never landed on HW({chain})"}
    # Second hop: HW → destination
    route = {"kind": "direct", "chain": chain, "amount": landed * 0.999}
    return await execute_route(route, dst_eid, step)


async def execute_route(route: dict, target_eid: str, step=None) -> dict:
    """Run a route produced by `plan_route`.

    `step` is an optional callable(str) for progress lines. Returns
    {"ok": bool, "delivered_usd": float, "error": str|None}.
    """
    import cex
    import dex
    import keys

    def _s(msg):
        log.warning("route: %s", msg)
        if step:
            try:
                step(msg)
            except Exception:
                pass

    kd = keys.load_keys()
    hop_timeout = float(os.getenv("ROUTE_HOP_TIMEOUT_SEC", "1800"))
    amount = float(route["amount"])

    async def _send_hw_to(eid: str, chain: str, amt_usd: float) -> float:
        """HW → exchange deposit. Returns credited amount (0 on failure)."""
        inst = cex.get_private(eid, kd[eid])
        await inst.load_markets()
        label = net_label(eid, chain)
        dep = await cex.fetch_deposit_address_robust(inst, "USDC", label)
        addr = (dep or {}).get("address")
        if not addr:
            _s(f"❌ немає адреси депозиту USDC/{label} на {eid}")
            return 0.0
        b0 = await inst.fetch_balance()
        base0 = float((b0.get("USDC") or {}).get("free") or 0)
        usdc = dex.USDC_BY_CHAIN[chain]
        wei = int(amt_usd * 1e6)
        _s(f"   · HW({chain}) → {eid}: {amt_usd:,.2f} USDC ({label})…")
        res = await dex.send_token(chain, usdc, wei, addr)
        if not res.get("ok"):
            _s(f"❌ send HW({chain})→{eid}: {res.get('error')}")
            return 0.0
        _s(f"   · tx {str(res.get('tx_hash'))[:18]}… чекаю зарахування")
        got = await _wait_cex_usdc(inst, base0, amt_usd, hop_timeout, step)
        # No inst.close(): cex.get_private returns a SHARED cached
        # instance (cex.py:142). Closing it here tears the session out
        # from under every other caller still using that exchange.
        if got <= 0:
            _s(f"❌ {eid} не зарахував USDC за {hop_timeout/60:.0f}хв")
        return got

    if route["kind"] == "direct":
        got = await _send_hw_to(target_eid, route["chain"], amount)
        return {"ok": got > 0, "delivered_usd": got,
                "error": None if got > 0 else "deposit not credited"}

    # ── bridge: HW(from) → via → HW(to) → target ──────────────────
    via = route["via"]
    from_chain, to_chain = route["from_chain"], route["to_chain"]
    _s(f"🌉 маршрут через {via}: HW({from_chain}) → {via} → "
       f"HW({to_chain}) → {target_eid}")

    # leg 1
    got = await _send_hw_to(via, from_chain, amount)
    if got <= 0:
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"leg1 HW({from_chain})→{via} failed"}
    # Claim it before anything else can decide it's idle cash.
    reserve_transit(via, got)

    # leg 2: via → HW(to_chain)
    inst_via = cex.get_private(via, kd[via])
    await inst_via.load_markets()
    label_out = net_label(via, to_chain)
    usdc_to = dex.USDC_BY_CHAIN[to_chain]
    hw_before = (await dex.wallet_token_balance(to_chain, usdc_to) or 0) / 1e6
    wd_amt = got * 0.999
    _s(f"   · {via} → HW({to_chain}): {wd_amt:,.2f} USDC ({label_out})…")
    # Freshly credited deposits are not immediately withdrawable —
    # Binance refused with "insufficient balance" 27s after crediting.
    # Retry with backoff instead of abandoning the route (and the funds).
    _wd_ok = False
    _wd_err = None
    for _try in range(4):
        try:
            await cex.withdraw_robust(inst_via, "USDC", wd_amt,
                                        dex.HOT_WALLET_ADDRESS, None,
                                        label_out)
            _wd_ok = True
            break
        except Exception as e:
            _wd_err = str(e)
            log.warning("route leg2 %s attempt %d/4: %s", via, _try + 1,
                        str(e)[:200])
            if not _is_transient_wd_error(_wd_err):
                _s(f"   ❌ {via} відмовив остаточно: {_wd_err[:120]}")
                break
            wait = 60 * (_try + 1)
            _s(f"   ⏳ {via} ще не віддає ({str(e)[:90]}) — "
               f"повтор через {wait}s")
            await asyncio.sleep(wait)
    if not _wd_ok:
        release_transit(via)
        log.error("route leg2 %s FAILED after 4 tries: %s — $%.2f USDC "
                  "sits on %s", via, (_wd_err or "")[:200], got, via)
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"leg2 {via} withdraw after 4 tries: "
                         f"{(_wd_err or '')[:140]}; ${got:,.2f} USDC on {via}"}
    # (no inst_via.close() — shared cached instance, see above)
    landed = await _wait_hw_usdc(to_chain, hw_before, wd_amt,
                                  hop_timeout, step)
    release_transit(via)            # it has left the bridge exchange
    if landed <= 0:
        log.error("route leg2 %s→HW(%s): withdrawal accepted but never "
                  "landed within %.0fmin", via, to_chain, hop_timeout / 60)
        return {"ok": False, "delivered_usd": 0.0,
                "error": f"leg2 payout to HW({to_chain}) never landed"}

    # leg 3: HW(to_chain) → target
    # From here the funds are USDC on our own wallet — not lost, but if
    # this leg fails nothing else will come back for them, so record the
    # position before attempting it.
    pid = None
    try:
        import ledger
        pid = ledger.open_position(
            "USDC", qty=landed, cost_usd=landed,
            location=ledger.LOC_HW, chain=to_chain,
            contract=dex.USDC_BY_CHAIN.get(to_chain),
            next_action=f"deposit_to_{target_eid}",
            note=f"route leg3 via {via}")
    except Exception as e:
        log.debug("route ledger open: %s", e)

    final = await _send_hw_to(target_eid, to_chain, landed * 0.999)
    if pid:
        try:
            import ledger
            if final > 0:
                ledger.close(pid, realized_usd=final,
                             note="route completed")
            else:
                ledger.mark_stuck(
                    pid, f"leg3 HW({to_chain})→{target_eid} not credited; "
                         f"${landed:,.2f} USDC sits on HW")
        except Exception:
            pass
    if final <= 0:
        _s(f"⚠️ ${landed:,.2f} USDC залишились на HW({to_chain}) — "
           f"депозит на {target_eid} не зарахувався. Дивись /positions")
    return {"ok": final > 0, "delivered_usd": final,
            "error": None if final > 0 else
            f"leg3 not credited; ${landed:,.2f} USDC left on HW({to_chain})"}
