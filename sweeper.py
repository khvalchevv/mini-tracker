"""Autonomous liquidation sweeper.

The bot only ever acted *inside* a session: buy → move → sell. Any asset
that fell out of a session (failed Kyber leg, withdrawal that landed
after the cycle gave up, manual transfer) sat untouched forever. Real
cases: 8604 FTT and 860 EURC idle on the hot wallet, 15 alt positions
rotting on Bitvavo.

This loop closes that gap. Every `SWEEP_INTERVAL_SEC` it scans:
  • hot wallet, every EVM chain → any non-USDC ERC-20 with value
    → Kyber swap into USDC
  • every CEX → any non-stable balance with value
    → market sell into the venue's quote currency (EUR on Bitvavo,
      USDT elsewhere)

Guards:
  • never touches a base that an active session is working on
  • ignores anything under SWEEP_MIN_USD (gas/fees would eat it)
  • per-run cap so one bad cycle can't churn the whole book
  • honours executor kill-switch
"""
import asyncio
import logging
import os
import time

log = logging.getLogger(__name__)

SWEEP_INTERVAL_SEC = float(os.getenv("SWEEP_INTERVAL_SEC", "600"))
SWEEP_MIN_USD = float(os.getenv("SWEEP_MIN_USD", "10"))
SWEEP_MAX_PER_RUN = int(os.getenv("SWEEP_MAX_PER_RUN", "5"))
SWEEP_ENABLED = os.getenv("SWEEP_ENABLED", "1") == "1"
SWEEP_CHAINS = [c.strip() for c in
                os.getenv("SWEEP_CHAINS", "ethereum,base,arbitrum,optimism,polygon,bsc").split(",")
                if c.strip()]

# Never auto-sell these — they ARE the working capital.
# EURC is deliberately NOT here: it's a tradeable asset for us, not a
# funding currency, and listing it meant the sweeper walked straight
# past 860 EURC (~$998) stranded on the hot wallet.
_STABLES = {"USDC", "USDT", "EUR", "DAI", "BUSD", "TUSD", "FDUSD"}


def _active_bases() -> set[str]:
    """Bases an in-flight session is currently working on — off limits."""
    try:
        import bot as _bot
        out = set()
        for entry in (getattr(_bot, "_SESSIONS", None) or {}).values():
            p = entry.get("plan")
            b = getattr(p, "base", None)
            if b:
                out.add(b.upper())
        return out
    except Exception:
        return set()


async def sweep_hot_wallet(notify=None) -> list[dict]:
    """Kyber-swap every stray ERC-20 on the hot wallet into USDC."""
    import dex
    results = []
    busy = _active_bases()
    for chain in SWEEP_CHAINS:
        usdc = dex.USDC_BY_CHAIN.get(chain)
        if not usdc:
            continue
        try:
            tokens = await _discover_hw_tokens(chain)
        except Exception as e:
            log.debug("sweep discover %s: %s", chain, e)
            continue
        for tok in tokens:
            if len(results) >= SWEEP_MAX_PER_RUN:
                return results
            sym = (tok.get("symbol") or "").upper()
            contract = tok["contract"]
            if contract.lower() == usdc.lower() or sym in _STABLES:
                continue
            if sym and sym in busy:
                log.info("sweep skip %s on %s — active session", sym, chain)
                continue
            raw = int(tok["raw"])
            dec = int(tok.get("decimals") or 18)
            qty = raw / (10 ** dec)
            # Price it through Kyber; this also proves a route exists
            try:
                q = await dex.usd_price(chain, contract, usd_notional=100.0)
            except Exception as e:
                log.debug("sweep quote %s/%s: %s", chain, contract[:10], e)
                continue
            px = (q or {}).get("price_sell_usd") or 0
            value = qty * px
            if value < SWEEP_MIN_USD:
                continue
            log.warning("sweep HW %s %s: %.4f (~$%.2f) → USDC",
                        chain, sym or contract[:10], qty, value)
            if notify:
                await notify(f"🧹 Sweep {sym or contract[:10]} on {chain}: "
                             f"{qty:.4f} (~${value:.2f}) → USDC…")
            # Raise the per-tx cap for this sweep only — stray positions
            # can legitimately exceed the arb-sized default.
            old_cap = os.getenv("DEX_MAX_TX_USD", "1000")
            try:
                if value > float(old_cap):
                    os.environ["DEX_MAX_TX_USD"] = f"{value * 1.1:.0f}"
                res = await dex.swap(chain, contract, usdc, raw,
                                       usd_estimate=value, slippage_bps=200)
            except Exception as e:
                res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            finally:
                os.environ["DEX_MAX_TX_USD"] = old_cap
            got = (int(res.get("amount_out_wei", 0)) / 1e6
                   if res.get("ok") else 0)
            results.append({"venue": f"hw:{chain}", "asset": sym or contract[:10],
                             "qty": qty, "usd": value, "ok": bool(res.get("ok")),
                             "got_usdc": got, "error": res.get("error")})
            if notify:
                if res.get("ok"):
                    await notify(f"✅ Sweep {sym}: → {got:.2f} USDC")
                else:
                    await notify(f"⚠️ Sweep {sym} fail: "
                                 f"{str(res.get('error'))[:120]}")
            # Clear any matching stuck record now that it's liquidated
            if res.get("ok"):
                try:
                    import stuck as _stuck
                    for it in _stuck.list_all():
                        if (it["chain"] == chain
                                and it["contract"].lower() == contract.lower()):
                            _stuck.remove(it["id"])
                except Exception:
                    pass
    return results


async def best_exit_venue(base: str, chain: str, contract: str,
                          qty: float, dex_usd: float) -> dict | None:
    """Is some exchange a better exit than Kyber for this position?

    The sweeper only ever dumped into USDC on the DEX. On a hand-checked
    MOVR position that route paid $5,896 while Bitvavo paid $7,510 — the
    "autonomous" path would have burned $1,614 versus doing it by hand.
    Liquidating well means comparing venues, not just having a venue.

    Returns {"eid", "proceeds_usd", "network"} when a CEX beats the DEX
    by a worthwhile margin, else None.
    """
    import cex
    import keys
    import routing
    try:
        from chains import canonical
    except Exception:
        canonical = lambda x: x                              # noqa: E731

    margin = float(os.getenv("SWEEP_CEX_MARGIN", "1.03"))    # 3% better
    kd = keys.load_keys()
    best = None
    for eid in cex.SUPPORTED_EXCHANGES:
        if not (kd.get(eid) or {}).get("apiKey"):
            continue
        try:
            inst = cex.get_private(eid, kd[eid])
            await inst.load_markets()
            quote = "EUR" if eid == "bitvavo" else "USDT"
            sym = f"{base.upper()}/{quote}"
            if sym not in (inst.symbols or []):
                continue
            # It must also accept a deposit on the chain we hold it on,
            # otherwise the position can't get there without a bridge we
            # don't have for arbitrary tokens.
            net = None
            for n in cex.network_info(eid, base.upper()):
                if canonical(n.get("network")) == chain and n.get("deposit"):
                    net = n.get("network")
                    break
            if not net:
                continue
            t = await inst.fetch_ticker(sym)
            bid = float(t.get("bid") or 0)
            if bid <= 0:
                continue
            fx = cex.get_fx_rate(quote) or 1.0
            # Walk the book so a thin bid doesn't flatter the estimate
            ob = await inst.fetch_order_book(sym, 30)
            filled = 0.0
            proceeds = 0.0
            for p, q in (ob.get("bids") or []):
                take = min(qty - filled, float(q))
                proceeds += take * float(p) * fx
                filled += take
                if filled >= qty:
                    break
            if filled < qty * 0.95:            # book can't absorb it
                continue
            proceeds *= 0.9975                 # taker fee
            if proceeds > dex_usd * margin and (not best
                                                or proceeds > best["proceeds_usd"]):
                best = {"eid": eid, "proceeds_usd": proceeds, "network": net}
        except Exception as e:
            log.debug("best_exit %s/%s: %s", eid, base, e)
    return best


async def _discover_hw_tokens(chain: str) -> list[dict]:
    """Every ERC-20 the hot wallet holds on `chain`, via Alchemy's
    token-balance index. Returns [{contract, raw, symbol, decimals}]."""
    import aiohttp
    import dex
    url = dex._alchemy_rpc(chain)
    addr = dex.HOT_WALLET_ADDRESS
    if not url or not addr:
        return []
    out = []
    async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)) as s:
        async with s.post(url, json={
                "jsonrpc": "2.0", "id": 1, "method": "alchemy_getTokenBalances",
                "params": [addr, "erc20"]}) as r:
            d = await r.json()
        balances = ((d.get("result") or {}).get("tokenBalances") or [])
        for b in balances:
            raw_hex = b.get("tokenBalance") or "0x0"
            try:
                raw = int(raw_hex, 16)
            except (TypeError, ValueError):
                continue
            if raw <= 0:
                continue
            contract = b.get("contractAddress")
            meta = {}
            try:
                async with s.post(url, json={
                        "jsonrpc": "2.0", "id": 2,
                        "method": "alchemy_getTokenMetadata",
                        "params": [contract]}) as r2:
                    meta = (await r2.json()).get("result") or {}
            except Exception:
                pass
            out.append({
                "contract": contract,
                "raw": raw,
                "symbol": meta.get("symbol") or "",
                "decimals": meta.get("decimals") or 18,
            })
    return out


async def sweep_exchanges(notify=None) -> list[dict]:
    """Market-sell stray alt balances on every CEX into its quote ccy."""
    import cex
    import keys
    results = []
    busy = _active_bases()
    kd = keys.load_keys()
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = kd.get(eid) or {}
        if not creds.get("apiKey"):
            continue
        quote = "EUR" if eid == "bitvavo" else "USDT"
        try:
            inst = cex.get_private(eid, creds)
            await inst.load_markets()
            bal = await inst.fetch_balance()
        except Exception as e:
            log.debug("sweep %s balance: %s", eid, e)
            continue
        fx = cex.get_fx_rate(quote) or 1.0
        for asset, v in list(bal.items()):
            if len(results) >= SWEEP_MAX_PER_RUN:
                return results
            if not isinstance(v, dict) or asset.upper() in _STABLES:
                continue
            if asset.upper() in busy:
                continue
            free = float(v.get("free") or 0)
            if free <= 0:
                continue
            sym = f"{asset}/{quote}"
            if sym not in (inst.symbols or []):
                continue
            try:
                t = await inst.fetch_ticker(sym)
                px = float(t.get("bid") or 0)
                value = free * px * fx
                if value < SWEEP_MIN_USD:
                    continue
                log.warning("sweep %s %s: %.6f (~$%.2f) → %s",
                            eid, asset, free, value, quote)
                if notify:
                    await notify(f"🧹 Sweep {asset} on {cex.pretty(eid)}: "
                                 f"{free:.4f} (~${value:.2f}) → {quote}…")
                params = ({"operatorId": int(time.time() * 1000)}
                          if eid == "bitvavo" else {})
                o = await inst.create_order(sym, "market", "sell",
                                              free, None, params)
                got = float(o.get("cost") or 0) * fx
                results.append({"venue": eid, "asset": asset, "qty": free,
                                 "usd": value, "ok": True, "got": got})
                if notify:
                    await notify(f"✅ Sweep {asset}: → ${got:.2f} {quote}")
            except Exception as e:
                log.debug("sweep %s %s: %s", eid, asset, e)
        # No inst.close(): cex.get_private hands back a SHARED cached
        # instance (cex.py:142), so closing it breaks every other caller.
    return results


async def reap_orphan_orders(notify=None) -> list[dict]:
    """Cancel resting orders that no live session owns.

    A limit order outlives the code path that placed it: the session can
    time out, crash, or lose the cancel (Bitvavo rejects cancels without
    operatorId). Whatever the cause, an unowned working order keeps
    filling — real case: ACX/EUR bought 12499 of 24810 and the rest sat
    open for an hour with nothing watching it.

    Only touches orders older than ORPHAN_ORDER_AGE_SEC whose base has
    no active session, so a running arb is never disturbed.
    """
    import cex
    import keys
    out = []
    busy = _active_bases()
    max_age = float(os.getenv("ORPHAN_ORDER_AGE_SEC", "900"))
    now_ms = time.time() * 1000
    kd = keys.load_keys()
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = kd.get(eid) or {}
        if not creds.get("apiKey"):
            continue
        try:
            inst = cex.get_private(eid, creds)
            await inst.load_markets()
            orders = await inst.fetch_open_orders()
        except Exception as e:
            log.debug("reap %s fetch: %s", eid, e)
            continue
        for o in (orders or []):
            sym = o.get("symbol") or ""
            base = sym.split("/")[0].upper() if "/" in sym else ""
            if base and base in busy:
                continue                       # a live session owns it
            age = (now_ms - float(o.get("timestamp") or now_ms)) / 1000
            if age < max_age:
                continue                       # still fresh, leave it
            oid = o.get("id")
            filled = float(o.get("filled") or 0)
            amount = float(o.get("amount") or 0)
            log.warning("reap orphan order %s %s %s filled=%.4f/%.4f "
                        "age=%.0fmin", eid, sym, str(oid)[:20],
                        filled, amount, age / 60)
            res = await cex.cancel_order_robust(inst, oid, sym)
            out.append({"venue": eid, "symbol": sym, "id": oid,
                         "filled": filled, "amount": amount,
                         "age_min": age / 60, **res})
            if notify:
                if res["verified"]:
                    await notify(
                        f"🧯 Скасовано осиротілий ордер "
                        f"<code>{sym}</code> на {cex.pretty(eid)} "
                        f"(залито {filled:.4f}/{amount:.4f}, "
                        f"висів {age / 60:.0f}хв)")
                else:
                    await notify(
                        f"🚨 НЕ вдалось скасувати <code>{sym}</code> на "
                        f"{cex.pretty(eid)}: {str(res.get('error'))[:120]}")
        # No inst.close(): cex.get_private hands back a SHARED cached
        # instance (cex.py:142), so closing it breaks every other caller.
    return out


async def run_once(notify=None) -> dict:
    """One full sweep pass. Returns {hw: [...], cex: [...]}"""
    try:
        import executor
        if executor.kill_active():
            log.info("sweep skipped — kill switch active")
            return {"hw": [], "cex": []}
    except Exception:
        pass
    # Orders first — cancelling a runaway limit stops the bleeding
    # before we bother liquidating what it already bought.
    orders = await reap_orphan_orders(notify)
    hw = await sweep_hot_wallet(notify)
    ex = await sweep_exchanges(notify)
    if hw or ex or orders:
        log.warning("sweep done: %d orphan orders, %d HW, %d CEX",
                    len(orders), len(hw), len(ex))
    return {"hw": hw, "cex": ex, "orders": orders}


async def periodic_loop(notify_factory=None):
    """Background loop. `notify_factory()` should return an async
    callable taking one string, or None to stay silent."""
    if not SWEEP_ENABLED:
        log.info("sweeper disabled (SWEEP_ENABLED=0)")
        return
    log.info("sweeper: every %.0fs, min $%.0f, max %d/run, chains=%s",
             SWEEP_INTERVAL_SEC, SWEEP_MIN_USD, SWEEP_MAX_PER_RUN,
             ",".join(SWEEP_CHAINS))
    while True:
        await asyncio.sleep(SWEEP_INTERVAL_SEC)
        try:
            notify = notify_factory() if notify_factory else None
            await run_once(notify)
        except Exception as e:
            log.warning("sweep loop err: %s", e, exc_info=True)
