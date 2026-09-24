"""Entry point: build TG application + start tracker in one asyncio loop."""
import asyncio
import io
import logging
import logging.handlers
import os
import sys

from dotenv import load_dotenv

load_dotenv(override=True)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Rotating log file so history survives restarts. 20 MB per file, keep 20.
_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "bot.log")
_file_handler = logging.handlers.RotatingFileHandler(
    _LOG_FILE, maxBytes=20 * 1024 * 1024, backupCount=20, encoding="utf-8",
)
_console_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[_file_handler, _console_handler],
)
log = logging.getLogger("main")

import keys
import storage
from bot import build_application, make_alert_sender, make_hunter_sender, register_hunter, notify_purged_zombies
from hunter import Hunter
from tracker import Tracker


def _parse_allowed(raw: str) -> set[int]:
    out = set()
    for x in (raw or "").split(","):
        x = x.strip()
        if x.isdigit():
            out.add(int(x))
    return out


async def _run():
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        raise SystemExit("TG_BOT_TOKEN not set in .env")

    allowed = _parse_allowed(os.getenv("TG_ALLOWED_USERS", ""))
    log.info("allowed users: %s", allowed or "(anyone)")

    storage.init()
    log.info("storage: %d pairs loaded", len(storage.all_pairs()))

    keys.wire_all()                                          # attach api_keys.json → ccxt
    # Handshake in background — logs each exchange's readiness
    async def _hs():
        try:
            res = await keys.handshake_all()
            log.info("keys handshake done: %s",
                     {e: r["status"] for e, r in res.items()})
        except Exception as e:
            log.warning("keys handshake err: %s", e)
    asyncio.create_task(_hs())

    app = build_application(token, allowed)
    tracker = Tracker(
        send_alert_cb=make_alert_sender(app),
        poll_interval=float(os.getenv("POLL_INTERVAL_SEC", "3")),
        cooldown=float(os.getenv("ALERT_COOLDOWN_SEC", "60")),
        timeout=float(os.getenv("DS_TIMEOUT", "8")),
    )
    hunter = Hunter(
        alert_cb=make_hunter_sender(app),
        cycle_sec=float(os.getenv("HUNT_CYCLE_SEC", "5")),
        cooldown_sec=float(os.getenv("HUNT_COOLDOWN_SEC", "120")),
        fetch_timeout=float(os.getenv("HUNT_FETCH_TIMEOUT", "4")),
        dex_enabled=os.getenv("HUNT_DEX_ENABLED", "true").lower() in ("1", "true", "yes"),
        kyber_enabled=os.getenv("KYBER_ENABLED", "false").lower() in ("1", "true", "yes"),
    )
    register_hunter(hunter)

    await app.initialize()
    await notify_purged_zombies(app)
    await app.start()
    await app.updater.start_polling()
    log.info("bot polling started")

    # Background: only prune truly zombie sessions (buy never landed).
    # Sessions with real funds run to completion via their own async task.
    from bot import _prune_zombie_sessions_loop
    asyncio.create_task(_prune_zombie_sessions_loop())

    # Resume any in-flight rebalance interrupted by restart
    from bot import _load_active_rebal, _rebalance_run
    _rb = _load_active_rebal()
    if _rb:
        log.info("resuming rebalance %s from action #%d/%d", _rb["rid"],
                 _rb["done_idx"], len(_rb["actions"]))
        asyncio.create_task(_rebalance_run(app, _rb["rid"], _rb["actions"],
                                             _rb["chat_ids"],
                                             start_idx=_rb["done_idx"]))

    # RESUME all in-flight auto-sessions (BUY/WD/SEND/RECEIVED/SELL
    # phases that got interrupted by restart). _load_sessions already
    # ran; _SESSIONS holds them. For each, spawn _run_auto_session in
    # resume=True mode — it reads session.state and jumps to the right
    # phase (skips completed ones).
    from bot import _SESSIONS, _run_auto_session, _persist_sessions
    import keys as _k
    import cex as _cex_m
    async def _resume_sessions_boot():
        await asyncio.sleep(10)                          # let handshakes finish
        kd = _k.load_keys()
        # SMART STALE CHECK — query exchange history to see if user has
        # already handled the session manually:
        #   (a) Any SELL of `base` on dest since session start → user sold
        #       manually, session already realized. Drop.
        #   (b) Dest balance = 0 AND no recent deposit history for base
        #       AND no recent trades → nothing to resume. Drop.
        # No age-based purge — we trust exchange data over stale timers.
        import time as _t
        drop_ok: list[tuple[str, str]] = []                  # (sid, reason)
        for _sid, _entry in list(_SESSIONS.items()):
            sess = _entry.get("session")
            plan = _entry.get("plan")
            if not sess or not plan: continue
            r = getattr(sess, "receipt", None)
            started_ts = getattr(r, "started_ts", None) if r else _t.time()
            since_ms = int((started_ts or _t.time() - 24 * 3600) * 1000)
            try:
                inst = _cex_m.get_private(plan.sell_eid, kd[plan.sell_eid])
                if plan.sell_eid == "binance":
                    try: await inst.load_time_difference()
                    except Exception: pass
                # (1) Look at my trade history — any SELL of this base
                # since session start = user sold manually.
                try:
                    trades = await inst.fetch_my_trades(plan.sell_sym,
                                                         since=since_ms)
                    sells = [t for t in (trades or [])
                             if (t.get("side") or "").lower() == "sell"
                             and (t.get("timestamp") or 0) >= since_ms]
                    if sells:
                        drop_ok.append((_sid,
                            f"user manually sold {sum(float(t.get('amount') or 0) for t in sells):.4f} "
                            f"{plan.base} on {plan.sell_eid}"))
                        continue
                except Exception as e:
                    log.debug("history trades %s err: %s", _sid, e)
                # (2) Balance check — 0 base + no deposit history since
                # forward_ts (SEND already fired but nothing arrived) →
                # stuck externally.
                b = await inst.fetch_balance()
                free = float((b.get(plan.base) or {}).get("free") or 0)
                fw_ts = getattr(sess, "forward_ts", None)
                if free <= 0 and getattr(sess, "deposit_credited", False):
                    drop_ok.append((_sid, "credited=True but 0 balance — sold"))
                    continue
                if free <= 0 and fw_ts and _t.time() - fw_ts > 3600:
                    # SEND > 1h ago, no balance, no sell record → likely
                    # not arriving or user emptied it.
                    drop_ok.append((_sid,
                        f"0 balance + SEND {int((_t.time()-fw_ts)/60)}min ago"))
                    continue
            except Exception as e:
                log.debug("stale check %s err: %s", _sid, e)
        # Dropping a session on boot used to be a bare log.info — no TG,
        # no stuck record, no ledger. If the heuristic was wrong (e.g.
        # the "manual sell" it detected was the session's OWN multi-pass
        # IOC sell, leaving an unsold remainder), the position vanished
        # from the bot's view entirely. Reconcile every drop against live
        # balances before letting it go.
        for _sid, _reason in drop_ok:
            _entry = _SESSIONS.get(_sid) or {}
            _plan = _entry.get("plan")
            log.warning("resume drop %s: %s — reconciling first",
                        _sid, _reason)
            try:
                import reconcile as _rec

                async def _n(text, _s=(_entry.get("subs") or [])):
                    for cid in _s:
                        try:
                            await app.bot.send_message(
                                cid, text, parse_mode="HTML")
                        except Exception:
                            pass
                if _plan is not None:
                    await _rec.reconcile_session(
                        _plan, session_id=_sid, succeeded=False, notify=_n)
            except Exception as e:
                log.warning("resume-drop reconcile %s: %s", _sid, e,
                            exc_info=True)
            _SESSIONS.pop(_sid, None)
        if drop_ok:
            _persist_sessions()
        for _sid, _entry in list(_SESSIONS.items()):
            try:
                _subs = _entry.get("subs") or []
                log.info("↻ resuming session %s (base=%s)",
                         _sid, _entry["plan"].base)
                asyncio.create_task(
                    _run_auto_session(app, _sid, _subs, resume=True))
            except Exception as e:
                log.warning("resume session %s err: %s", _sid, e)
    asyncio.create_task(_resume_sessions_boot())

    # Periodic dump — captures mid-flight state changes (filled_qty_wei,
    # forward_tx, deposit_credited) so a crash between phases still
    # leaves enough on disk for resume to pick up.
    async def _sessions_dump_loop():
        from bot import _persist_sessions
        while True:
            await asyncio.sleep(20)
            try:
                _persist_sessions()
            except Exception: pass
    asyncio.create_task(_sessions_dump_loop())

    # Alchemy WebSocket subscription for hot wallet — sub-second push
    # on any incoming ERC20 transfer.
    import walletfeed, dex as _dex_mod
    if _dex_mod.HOT_WALLET_ADDRESS:
        asyncio.create_task(walletfeed.start(_dex_mod.HOT_WALLET_ADDRESS))

    # Token precache — every EVM/Solana deposit address + contract map
    # so /testrun and autoexec don't burn API calls at trade time.
    import token_precache
    async def _precache_boot():
        await asyncio.sleep(20)          # let handshakes + markets finish
        try:
            await token_precache.refresh()
        except Exception as e:
            log.warning("precache initial build err: %s", e)
    asyncio.create_task(_precache_boot())
    asyncio.create_task(token_precache.refresh_periodic())

    # New-listing auto-detector: watches Bitvavo bases every 5 min,
    # auto-links via CG contract → Gate/Binance same-contract check →
    # DexScreener top pool. TG notifies each new base with wiring result.
    import new_listings as _nl
    def _hunter_provider():
        try: from bot import _HUNTER; return _HUNTER
        except Exception: return None
    def _subs_provider():
        try:
            from bot import _HUNTER
            return list(_HUNTER.subs) if _HUNTER else []
        except Exception: return []
    asyncio.create_task(_nl.watch_loop(app, _hunter_provider, _subs_provider))

    # Live wd/dep truth: reload currencies every 15 min so hunter's
    # per-alert wd/dep gate sees fresh status (exchanges disable coin
    # withdrawals mid-day; stale cache = false alerts).
    async def _currency_refresh_loop():
        import cex as _cex
        while True:
            await asyncio.sleep(900)                    # 15 min
            for eid in _cex.SUPPORTED_EXCHANGES:
                try:
                    inst = _cex._instances.get(eid)
                    if inst:
                        await inst.load_markets(reload=True)
                        log.info("currencies refreshed: %s (%d)",
                                 eid, len(inst.currencies or {}))
                except Exception as e:
                    log.debug("currency refresh %s err: %s", eid, e)
    asyncio.create_task(_currency_refresh_loop())

    # Auto-refill HW gas from Gate — logic in gas_refill.ensure_hot_gas
    # (also callable inline before any HW→dest send).
    import gas_refill as _gr
    asyncio.create_task(_gr.periodic_loop())

    # Ledger reconciliation — the backstop for positions whose session
    # never reached its `finally` (SIGKILL, power loss, an exception in
    # the exception handler). Reads live balances and settles or flags
    # every unresolved ledger row. Reporting only; it never trades.
    async def _reconcile_loop():
        import reconcile as _rec
        interval = float(os.getenv("RECONCILE_INTERVAL_SEC", "900"))
        await asyncio.sleep(90)                 # let boot settle first
        while True:
            try:
                import bot as _b
                subs = list(_b._HUNTER.subs) if getattr(_b, "_HUNTER", None) \
                    else []

                async def _n(text):
                    for cid in subs:
                        try:
                            await app.bot.send_message(cid, text,
                                                        parse_mode="HTML")
                        except Exception:
                            pass
                await _rec.reconcile_all(_n if subs else None)
            except Exception as e:
                log.warning("reconcile loop: %s", e, exc_info=True)
            await asyncio.sleep(interval)
    asyncio.create_task(_reconcile_loop())

    # Auto-rebalance watch — every 2 min check if any exchange dropped
    # below the trigger threshold ($1000). Only fires when no session is
    # in flight, so it never interferes with an active arb.
    async def _auto_rebal_watch():
        import bot as _bot_mod
        while True:
            await asyncio.sleep(120)
            try:
                # Skip if any session is active — user asked "після прогону".
                # BUT never let a stale session block rebalance forever: a
                # 20h-old zombie once starved HW USDC to $188 and every
                # swap reverted with TRANSFER_FROM_FAILED. Only sessions
                # younger than the guard actually hold the lock.
                import time as _time_m
                _sess = getattr(_bot_mod, "_SESSIONS", None) or {}
                if _sess:
                    _guard = float(os.getenv("REBAL_SESSION_GUARD_SEC", "1800"))
                    _now = _time_m.time()
                    _fresh = []
                    for _sid, _e in list(_sess.items()):
                        _p = _e.get("plan")
                        _ts = (getattr(_p, "alert_ts", None)
                               or _e.get("created_ts") or _now)
                        if _now - float(_ts) < _guard:
                            _fresh.append(_sid)
                    if _fresh:
                        # Log it: this silent `continue` made the
                        # rebalancer look dead for seven minutes while a
                        # 20-min-old zombie session held the gate.
                        log.info("auto-rebal skip: %d live session(s) %s",
                                 len(_fresh), _fresh[:4])
                        continue                    # a real run is in flight
                    log.warning("auto-rebal: ignoring %d stale session(s) %s "
                                "— older than %.0fmin guard",
                                len(_sess), list(_sess.keys())[:5], _guard / 60)
                subs = list(_bot_mod._HUNTER.subs) if _bot_mod._HUNTER else []
                if not subs:
                    continue
                await _bot_mod._maybe_auto_rebalance(app, subs, "watch")
            except Exception as e:
                log.debug("auto-rebal watch: %s", e)
    asyncio.create_task(_auto_rebal_watch())

    # Watchdog: alert if any WS goes silent >5 min (indicates broken feed)
    async def _ws_watchdog():
        alerted = set()
        recovered = set()
        while True:
            await asyncio.sleep(60)
            health = walletfeed.ws_health()
            for chain, silence_sec in health.items():
                if silence_sec > 300 and chain not in alerted:
                    alerted.add(chain)
                    recovered.discard(chain)
                    log.warning("WS %s silent for %.0fs — feed may be broken",
                                chain, silence_sec)
                    try:
                        from bot import _HUNTER
                        subs = list(_HUNTER.subs) if _HUNTER else []
                        for cid in subs:
                            await app.bot.send_message(
                                cid, f"⚠️ WS <b>{chain}</b> тихо {int(silence_sec / 60)}хв — "
                                     f"хот wallet detection може не працювати", parse_mode="HTML")
                    except Exception: pass
                elif silence_sec < 60 and chain in alerted:
                    alerted.discard(chain)
                    recovered.add(chain)
                    log.info("WS %s recovered", chain)
    asyncio.create_task(_ws_watchdog())

    async def _guard(name, task):
        while True:
            try:
                await task
                log.warning("%s exited cleanly — restarting in 3s", name)
            except asyncio.CancelledError:
                log.warning("%s cancelled — restarting in 3s", name)
            except BaseException:
                log.exception("%s crashed — restarting in 3s", name)
            await asyncio.sleep(3)
            task = asyncio.create_task(
                tracker.run() if name == "tracker" else hunter.run()
            )

    # Startup report → send full status to all hunter subs (autoexec state,
    # thresholds, exchange balances, HW native balances, WS chains, blacklist,
    # rebalance resume, gas refill config). Fire in background so it doesn't
    # block startup if any exchange is slow.
    async def _startup_report():
        try:
            await asyncio.sleep(15)                    # wait for handshakes + WS
            from bot import _HUNTER, _AUTOEXEC, _load_active_rebal
            import cex as _cex, dex as _dex_m, walletfeed as _wf, blacklist as _bl
            import keys as _k
            from web3 import Web3
            kd = _k.load_keys()
            lines = ["🤖 <b>Бот запущено</b>"]
            # Config
            au = "⚡ УВІМК" if _AUTOEXEC else "⏸ ВИМК"
            hp = _HUNTER
            lines.append(
                f"• Автоекзек: <b>{au}</b>  ·  мін профіт: "
                f"<b>${(hp.min_profit_usd if hp else 0):.2f}</b>  ·  "
                f"мін спред: <b>{(hp.threshold if hp else 0):.2f}%</b>")
            # Exchange balances
            for eid in _cex.SUPPORTED_EXCHANGES:
                if eid not in kd:
                    lines.append(f"• {_cex.pretty(eid)}: <i>no key</i>"); continue
                try:
                    inst = _cex.get_private(eid, kd[eid])
                    if eid == "binance":
                        try: await inst.load_time_difference()
                        except Exception: pass
                    # Bitvavo v2/assets Cloudflare rate-limits sometimes;
                    # retry once via a fresh proxy.
                    bal = None
                    for attempt in range(3):
                        try:
                            bal = await inst.fetch_balance()
                            break
                        except Exception as _e:
                            if attempt == 2: raise
                            if eid == "bitvavo":
                                inst.aiohttp_proxy = _cex._pick_proxy()
                            await asyncio.sleep(1.5)
                    parts = []
                    for a in ("USDT", "USDC", "EUR", "BTC", "ETH", "BNB"):
                        f = float((bal.get(a) or {}).get("free") or 0)
                        if f > 0.5 or (a in ("BTC","ETH","BNB") and f > 0.0005):
                            parts.append(f"{a} {f:,.2f}" if a not in ("BTC","ETH","BNB")
                                          else f"{a} {f:.4f}")
                    lines.append(f"• {_cex.pretty(eid)}: {', '.join(parts) or '—'}")
                except Exception as e:
                    lines.append(f"• {_cex.pretty(eid)}: err {e}")
            # HW native balances (a few key chains)
            hw = _dex_m.HOT_WALLET_ADDRESS
            if hw:
                lines.append(f"• HW: <code>{hw}</code>")
                natives = []
                for chain, sym in (("ethereum","ETH"),("bsc","BNB"),
                                    ("arbitrum","ETH"),("base","ETH"),
                                    ("polygon","POL"),("avalanche","AVAX")):
                    try:
                        rpc = _dex_m._rpc_for(chain)
                        if not rpc: continue
                        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 5}))
                        v = w3.eth.get_balance(Web3.to_checksum_address(hw)) / 1e18
                        if v > 0.0001:
                            natives.append(f"{chain}: {v:.4f} {sym}")
                    except Exception:
                        pass
                if natives:
                    lines.append("  " + " · ".join(natives))
            # WS chains
            wsh = _wf.ws_health()
            if wsh:
                healthy = [c for c,s in wsh.items() if s < 120]
                lines.append(f"• WS: {len(healthy)}/{len(wsh)} chains alive "
                             f"({', '.join(sorted(healthy)[:8])}"
                             f"{'…' if len(healthy)>8 else ''})")
            # Blacklist
            try:
                snap = _bl.snapshot()
                nb = len(snap.get("bases") or [])
                np_ = len(snap.get("pairs") or [])
                if nb or np_:
                    lines.append(f"• Blacklist: {nb} base(s), {np_} pair(s)")
            except Exception:
                pass
            # Resumed rebalance
            rb = _load_active_rebal()
            if rb:
                lines.append(f"• ⚙️ Ребаланс відновлено: {rb['done_idx']}/"
                             f"{len(rb['actions'])} кроків")
            # Gas refill config note
            lines.append("─" * 15)
            lines.append("<b>Активні механізми:</b>")
            lines.append("• авто-дозаправка газу (Gate → HW, 7 chains)")
            lines.append("• WS watchdog (алерт при 5-хв тиші)")
            lines.append("• nonce lock (серійні відправки в межах чейну)")
            lines.append("• ребаланс переживає рестарт")
            lines.append("• стан автоекзека переживає рестарт")
            text = "\n".join(lines)
            subs = list(hunter.subs) if hunter else []
            for cid in subs:
                try:
                    await app.bot.send_message(cid, text, parse_mode="HTML",
                                                disable_web_page_preview=True)
                except Exception as e:
                    log.warning("startup report to %s err: %s", cid, e)
        except Exception as e:
            log.warning("startup report err: %s", e)
    asyncio.create_task(_startup_report())

    tracker_task = asyncio.create_task(tracker.run())
    hunter_task = asyncio.create_task(hunter.run())
    guard_t = asyncio.create_task(_guard("tracker", tracker_task))
    guard_h = asyncio.create_task(_guard("hunter", hunter_task))
    try:
        await asyncio.gather(guard_t, guard_h)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except BaseException:
        log.exception("main crashed")
    finally:
        tracker.stop()
        hunter.stop()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
