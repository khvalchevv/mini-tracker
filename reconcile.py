"""Inventory reconciliation — the safety net under every session exit.

The bot's failure mode was never "it crashed". It was "it returned".
Roughly a dozen early-return paths across `bot.py` and `executor.py`
exit while holding a non-stable asset, and almost none of them recorded
anything. The position simply stopped being anyone's problem.

`reconcile_session()` is meant to be called from a `finally` block. It
asks the only question that matters — *where is the coin right now?* —
by querying the source venue, the hot wallet, and the destination venue
directly. Anything material that is still sitting somewhere gets a
ledger row, a `stuck` entry and a Telegram line, whatever the code path
that got us here believed.

Deliberately reads live balances rather than trusting session state:
session state is what was unreliable in the first place.
"""
import asyncio
import logging
import os

log = logging.getLogger(__name__)

MIN_REPORT_USD = float(os.getenv("RECONCILE_MIN_USD", "5"))

_STABLES = {"USDC", "USDT", "EUR", "DAI", "BUSD", "TUSD", "FDUSD"}


async def _hw_qty(chain: str, contract: str) -> tuple[float | None, int]:
    """(qty, decimals) on the hot wallet, or (None, _) if UNKNOWN.

    The distinction matters more than anything else in this module: a
    failed RPC read must never look like "there is no coin", because the
    caller closes ledger rows on an empty result. Returning 0.0 for both
    cases would let a transient 429 erase the record of a live position —
    precisely the bug this file exists to prevent.
    """
    import dex
    if not chain or not contract:
        return 0.0, 18                      # nothing to look up: truly none
    try:
        raw = await dex.wallet_token_balance(chain, contract)
    except Exception as e:
        log.warning("reconcile: HW balance %s/%s UNREADABLE: %s",
                    chain, contract[:12], e)
        return None, 18
    if raw is None:
        log.warning("reconcile: HW balance %s/%s returned None (RPC) — "
                    "treating as unknown", chain, contract[:12])
        return None, 18
    # Decimals MUST be right. The cache is in-memory and empty at boot,
    # while the reconcile loop starts 90s later — defaulting to 18 for a
    # 6-decimal token understates the balance by 1e12 and the position
    # gets closed as dust. Read the contract when the cache misses.
    dec = None
    try:
        dec = dex._TOKEN_DECIMALS.get((chain, contract.lower()))
    except Exception:
        pass
    if dec is None:
        try:
            from web3 import Web3 as _W3
            w3 = dex._get_w3(chain)
            abi = [{"inputs": [], "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "stateMutability": "view", "type": "function"}]
            c = w3.eth.contract(
                address=_W3.to_checksum_address(contract), abi=abi)
            dec = int(c.functions.decimals().call())
            dex._TOKEN_DECIMALS[(chain, contract.lower())] = dec
        except Exception as e:
            log.warning("reconcile: decimals() unreadable for %s/%s (%s) "
                        "— cannot size the balance, reporting UNKNOWN",
                        chain, contract[:12], e)
            return None, 18
    return raw / (10 ** dec), dec


async def _cex_qty(eid: str, base: str) -> float | None:
    """Free+used balance of `base` on `eid`, or None if UNKNOWN."""
    import cex
    import keys
    try:
        creds = keys.load_keys().get(eid) or {}
        if not creds.get("apiKey"):
            return 0.0                      # no account: truly none
        inst = cex.get_private(eid, creds)
        bal = await inst.fetch_balance()
        v = bal.get(base.upper()) or {}
        return float(v.get("free") or 0) + float(v.get("used") or 0)
    except Exception as e:
        log.warning("reconcile: %s balance for %s UNREADABLE: %s",
                    eid, base, e)
        return None


async def _usd_value(base: str, qty: float, chain: str | None,
                     contract: str | None,
                     venue: str | None) -> float | None:
    """USD valuation, or None when we genuinely could not price it.

    Same discipline as the balance reads: an unpriceable position is NOT
    a worthless one. Returning 0.0 here made the caller drop a confirmed
    holding out of `found`, which then looked like "inventory clear" and
    closed the ledger row — for coin we had just proved we hold.
    """
    if qty <= 0:
        return 0.0
    if chain and contract:
        try:
            import dex
            q = await dex.usd_price(chain, contract, usd_notional=100.0)
            px = (q or {}).get("price_sell_usd") or 0
            if px > 0:
                return qty * px
        except Exception:
            pass
    if venue:
        try:
            import cex
            import keys
            creds = keys.load_keys().get(venue) or {}
            inst = cex.get_private(venue, creds)
            quote = "EUR" if venue == "bitvavo" else "USDT"
            sym = f"{base.upper()}/{quote}"
            if sym in (inst.symbols or []):
                t = await inst.fetch_ticker(sym)
                px = float(t.get("bid") or 0)
                fx = cex.get_fx_rate(quote) or 1.0
                return qty * px * fx
        except Exception:
            pass
    log.warning("reconcile: could not price %s (%.6f) on %s — "
                "treating value as UNKNOWN, not zero",
                base, qty, chain or venue or "?")
    return None


async def reconcile_session(plan, *, session_id: str = "",
                            ledger_ids: list[str] | None = None,
                            succeeded: bool = False,
                            notify=None,
                            timeout_sec: float | None = None) -> list[dict]:
    """Find inventory of `plan.base` left anywhere and account for it.

    Call from a `finally`. Returns a list of what was found.

    `ledger_ids` are rows this session opened; when the coin is gone
    they are closed, when it is still around they are marked STUCK.

    Bounded by `timeout_sec` (default RECONCILE_TIMEOUT_SEC, 90s): this
    runs inside `finally`, which may execute during task cancellation,
    and an unbounded await there would hang the shutdown path forever.
    On timeout the ledger rows are left OPEN — the periodic
    `reconcile_all` will retry.
    """
    limit = timeout_sec if timeout_sec is not None else \
        float(os.getenv("RECONCILE_TIMEOUT_SEC", "90"))
    try:
        return await asyncio.wait_for(
            _reconcile_session_inner(
                plan, session_id=session_id, ledger_ids=ledger_ids,
                succeeded=succeeded, notify=notify),
            timeout=limit)
    except asyncio.TimeoutError:
        log.error("reconcile %s: TIMED OUT after %.0fs — ledger rows stay "
                  "open for the periodic pass",
                  getattr(plan, "base", "?"), limit)
        return []
    except asyncio.CancelledError:
        log.error("reconcile %s: cancelled mid-check — ledger rows stay open",
                  getattr(plan, "base", "?"))
        raise


async def _reconcile_session_inner(plan, *, session_id: str = "",
                                   ledger_ids: list[str] | None = None,
                                   succeeded: bool = False,
                                   notify=None) -> list[dict]:
    import stuck as stuck_mod
    try:
        import ledger
    except Exception:
        ledger = None

    base = getattr(plan, "base", None)
    if not base or base.upper() in _STABLES:
        return []
    chain = getattr(plan, "chain", None)
    contract = getattr(plan, "base_contract", None)
    venues = [v for v in (getattr(plan, "buy_eid", None),
                          getattr(plan, "sell_eid", None))
              if v and v != "dex"]

    found: list[dict] = []
    unknown: list[str] = []          # places we could NOT read

    # Hot wallet
    if chain and contract:
        hw_qty, _dec = await _hw_qty(chain, contract)
        if hw_qty is None:
            unknown.append(f"hw:{chain}")
        elif hw_qty > 0:
            usd = await _usd_value(base, hw_qty, chain, contract, None)
            # Report whenever we HOLD something. An unknown price (None)
            # must never filter a real holding out — that path ended in
            # "inventory clear" and closed the row.
            if usd is None or usd >= MIN_REPORT_USD:
                found.append({"where": f"hw:{chain}", "qty": hw_qty,
                              "usd": usd, "chain": chain,
                              "contract": contract,
                              "priced": usd is not None})

    # Exchanges
    for eid in dict.fromkeys(venues):
        q = await _cex_qty(eid, base)
        if q is None:
            unknown.append(f"cex:{eid}")
            continue
        if q <= 0:
            continue
        usd = await _usd_value(base, q, None, None, eid)
        if usd is None or usd >= MIN_REPORT_USD:
            found.append({"where": f"cex:{eid}", "qty": q, "usd": usd,
                          "venue": eid, "priced": usd is not None})

    # Nothing found. Only settle the ledger if we could actually SEE
    # everywhere — an unreadable venue means "unknown", not "empty", and
    # closing on unknown is how a live position gets erased.
    if not found:
        if unknown:
            log.error("reconcile %s: no inventory found BUT %s unreadable "
                      "— leaving ledger rows open for the next pass",
                      base, ", ".join(unknown))
            if ledger and ledger_ids:
                for pid in ledger_ids:
                    ledger.update(pid, note=f"reconcile inconclusive: "
                                             f"{','.join(unknown)} unreadable")
            if notify:
                try:
                    await notify(
                        f"⚠️ <b>{base}</b>: не вдалось перевірити "
                        f"{', '.join(unknown)} — позицію лишаю відкритою "
                        f"в /positions до наступної звірки")
                except Exception:
                    pass
            return []
        if ledger and ledger_ids:
            for pid in ledger_ids:
                row = ledger.get(pid)
                if row and row.get("state") in (ledger.OPEN, ledger.STUCK):
                    # Preserve a realized figure the trading path already
                    # wrote — passing None here blanked it and the row
                    # then read as a total loss of the cost basis.
                    ledger.close(pid,
                                 realized_usd=row.get("realized_usd"),
                                 note="reconciler: inventory clear")
        return []

    # Something is still held. Record it regardless of what the caller
    # thought happened — `succeeded` only changes the wording.
    for f in found:
        _usd_txt = ("$%.2f" % f["usd"]) if f.get("usd") is not None \
            else "ціна невідома"
        log.error("RECONCILE %s: %.6f %s (~%s) still at %s "
                  "(session %s, succeeded=%s)",
                  base, f["qty"], base, _usd_txt, f["where"],
                  session_id or "?", succeeded)
        try:
            # Record BOTH wallet- and exchange-held leftovers. The old
            # guard required chain+contract, so anything sitting on a
            # CEX was silently dropped — the user got "дивись /stuck"
            # and /stuck said there was nothing.
            stuck_mod.add(base, f.get("chain"), f.get("contract"),
                          qty=f["qty"], paid_usd=(f.get("usd") or 0),
                          venue=f.get("venue"),
                          err=f"reconciler: left at {f['where']}"
                              + ("" if f.get("priced") else " (unpriced)"))
        except Exception as e:
            log.warning("reconcile stuck.add %s: %s", base, e)

    if ledger and ledger_ids:
        _left_usd = sum((f.get("usd") or 0) for f in found)
        _left_qty = sum(f.get("qty") or 0 for f in found)
        for pid in ledger_ids:
            row = ledger.get(pid)
            if not row or row.get("state") != ledger.OPEN:
                continue
            # A 99% fill leaves a sliver behind. After a SUCCESSFUL run
            # that sliver is dust, not a stuck position — flagging it
            # marked a completed $1000 IQ cycle as STUCK over $7, and
            # left the row's qty at the full 1.27M so the dashboard
            # read $1187 of exposure that no longer existed.
            _dust_cap = float(os.getenv("RECONCILE_DUST_USD", "25"))
            _cost = float(row.get("cost_usd") or 0)
            _is_sliver = (_left_usd < _dust_cap
                          or (_cost > 0 and _left_usd < _cost * 0.05))
            if succeeded and _is_sliver:
                ledger.close(pid, note=f"done; ${_left_usd:.2f} dust left "
                                       f"at {found[0]['where']}")
                continue
            # Genuinely stuck: correct the quantity to what is REALLY
            # still held, otherwise the row overstates the exposure.
            ledger.update(pid, qty=_left_qty)
            ledger.mark_stuck(
                pid, f"reconciler: {_left_qty:,.4f} (~${_left_usd:,.2f}) "
                     f"still at {', '.join(f['where'] for f in found)}")

    if notify:
        lines = [f"📦 <b>{base}</b> залишився після сесії:"]
        for f in found:
            _v = (f"~${f['usd']:,.2f}" if f.get("usd") is not None
                  else "ціну не вдалось дізнатись")
            lines.append(f"   · {f['qty']:,.4f} на <code>{f['where']}</code> "
                         f"({_v})")
        lines.append("   Дивись /stuck — там кнопка допродажу.")
        try:
            await notify("\n".join(lines))
        except Exception:
            pass
    return found


async def reconcile_all(notify=None) -> list[dict]:
    """Sweep every unresolved ledger row against live balances.

    Run periodically and on boot: it catches positions whose session
    died without ever reaching its `finally` (kill -9, power loss).
    """
    try:
        import ledger
    except Exception:
        return []
    out = []
    # Rows sharing coordinates share ONE physical balance. Reconciling
    # them one at a time wrote that whole balance into each row: eleven
    # open LAPTOP rows every one claiming the 59.7165 actually held on
    # Bitvavo, so /positions reported $3,738 stranded against $108 real.
    # Look each pot up once, then divide it between its rows.
    groups: dict[tuple, list[dict]] = {}
    for row in ledger.open_rows():
        key = (row.get("base"), row.get("chain") or "",
               (row.get("contract") or "").lower(), row.get("venue") or "")
        groups.setdefault(key, []).append(row)

    for (base, _chain, _contract, _venue), rows in groups.items():
        chain = _chain or None
        contract = _contract or None
        venue = _venue or None
        # Check EVERY place these rows have coordinates for, not just the
        # one `location` claims. A row is only closed when all of them
        # read zero — `location` lags reality (it is updated by the
        # pipeline, which is exactly what fails), and trusting it alone
        # closed positions that had simply moved on to the next hop.
        qty = 0.0
        unreadable = []
        where_found = None
        if chain and contract:
            q, _ = await _hw_qty(chain, contract)
            if q is None:
                unreadable.append(f"hw:{chain}")
            elif q > 0:
                qty, where_found = q, f"hw:{chain}"
        if qty <= 0 and venue:
            q = await _cex_qty(venue, base)
            if q is None:
                unreadable.append(f"cex:{venue}")
            elif q > 0:
                qty, where_found = q, f"cex:{venue}"
        if qty <= 0 and unreadable:
            # Nothing seen, but we couldn't look everywhere.
            log.warning("reconcile_all: %s not found but %s unreadable "
                        "— leaving %d row(s) open",
                        base, ",".join(unreadable), len(rows))
            continue
        if qty <= 0:
            # Confirmed gone everywhere we can see — someone (sweeper,
            # manual sale, a later session) resolved it.
            for row in rows:
                ledger.close(row["id"], note="reconcile_all: inventory clear")
            continue
        usd_total = await _usd_value(base, qty, chain, contract, venue)
        if usd_total is None:
            log.warning("reconcile_all: %s %.6f at %s holds but is "
                        "unpriceable — leaving %d row(s) open",
                        base, qty, where_found, len(rows))
            continue
        # Divide the observed balance newest-first. Under FIFO the
        # earliest lots are the ones already sold, so whatever is still
        # on the venue belongs to the most recent rows; the older rows
        # are the ones that have actually been resolved.
        rows_sorted = sorted(rows, key=lambda r: float(r.get("created_ts") or 0),
                             reverse=True)
        remaining = qty
        alloc: list[tuple[dict, float]] = []
        for row in rows_sorted:
            claim = float(row.get("qty") or 0)
            take = min(claim, remaining) if claim > 0 else 0.0
            remaining -= take
            alloc.append((row, take))
        if remaining > 1e-12 and alloc:
            # Venue holds more than every row together claims — hand the
            # surplus to the newest row rather than losing sight of it.
            alloc[0] = (alloc[0][0], alloc[0][1] + remaining)
        unit_usd = usd_total / qty if qty else 0.0
        for row, take in alloc:
            if take <= 0:
                ledger.close(row["id"],
                             note="reconcile_all: superseded — balance "
                                  "accounted for by a newer position")
                continue
            usd = take * unit_usd
            if usd < MIN_REPORT_USD:
                ledger.close(row["id"], note=f"reconcile_all: dust ${usd:.2f}")
                continue
            # Record where it ACTUALLY is, and correct the row if the
            # pipeline never got round to updating `location`.
            out.append({"id": row["id"], "base": base, "qty": take, "usd": usd,
                        "where": where_found or
                                 f"{row.get('location')}:{chain or venue}"})
            try:
                _loc = (ledger.LOC_HW if (where_found or "").startswith("hw:")
                        else ledger.LOC_CEX)
                if (row.get("location") != _loc
                        or abs(float(row.get("qty") or 0) - take) > 1e-9):
                    ledger.update(row["id"], location=_loc, qty=take)
            except Exception:
                pass
    if out and notify:
        lines = ["📒 <b>Незакриті позиції</b>:"]
        for o in out:
            lines.append(f"   · {o['base']} {o['qty']:,.4f} на "
                         f"<code>{o['where']}</code> (~${o['usd']:,.2f})")
        try:
            await notify("\n".join(lines))
        except Exception:
            pass
    return out
