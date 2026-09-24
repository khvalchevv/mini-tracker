"""Position ledger — the record of what the bot currently owns.

Before this existed, the only trace of a live position was an in-memory
dict pickled to `sessions.pkl`, which four independent tasks were free to
delete without telling anyone. That is how 8604 FTT (~$2000) and 860 EURC
(~$998) ended up sitting on the hot wallet with nothing coming back for
them, and how a 20h zombie session silently blocked every rebalance.

The rule this module enforces:

    A position is opened BEFORE the money moves, and closed ONLY when the
    inventory is back in a stable asset.

`_SESSIONS` / `sessions.pkl` are a UI cache and may be pruned freely.
This file is the source of truth and is never pruned — entries move to
CLOSED, they don't disappear.

Storage: a single JSON file, rewritten atomically (tmp + os.replace).
Not a database; the working set is tens of rows, and being greppable by
a human at 3am is worth more here than write throughput.
"""
import json
import logging
import os
import threading
import time
import uuid

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(HERE, os.getenv("LEDGER_FILE", "positions.json"))

# Lifecycle
OPEN = "OPEN"            # we hold inventory; work is expected to continue
STUCK = "STUCK"          # we hold inventory and the pipeline gave up
CLOSED = "CLOSED"        # inventory is back in a stable asset

# Where the inventory physically is
LOC_HW = "hw"            # hot wallet, needs `chain`
LOC_CEX = "cex"          # an exchange, needs `venue`
LOC_TRANSIT = "transit"  # withdrawn/sent, not yet credited

_lock = threading.Lock()
_rows: list[dict] = []
_loaded = False


def _load() -> None:
    global _loaded, _rows
    if _loaded:
        return
    _loaded = True
    try:
        with open(FILE, encoding="utf-8") as f:
            _rows = json.load(f) or []
        n_open = sum(1 for r in _rows if r.get("state") in (OPEN, STUCK))
        if n_open:
            log.warning("ledger: %d unresolved position(s) on boot", n_open)
        else:
            log.info("ledger: %d rows, none open", len(_rows))
    except FileNotFoundError:
        _rows = []
    except Exception as e:
        # Never start with a blank ledger on a parse error — that is
        # exactly how positions get forgotten. Preserve the file so a
        # human can inspect it, and refuse to pretend it was empty.
        log.error("ledger: FAILED to read %s (%s) — preserving as .corrupt",
                  FILE, e)
        try:
            os.replace(FILE, FILE + f".corrupt.{int(time.time())}")
        except Exception:
            pass
        _rows = []


def _save_locked() -> None:
    """Caller must hold _lock."""
    tmp = FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_rows, f, ensure_ascii=False, indent=2)
    os.replace(tmp, FILE)


def open_position(base: str, qty: float, cost_usd: float,
                  location: str, *, chain: str | None = None,
                  venue: str | None = None, contract: str | None = None,
                  next_action: str = "", session_id: str = "",
                  note: str = "") -> str:
    """Record inventory we are about to acquire or move. Call this
    BEFORE the money moves — a crash between the call and the trade
    leaves a harmless phantom row, while the reverse leaves real money
    with no record."""
    _load()
    pid = uuid.uuid4().hex[:12]
    now = time.time()
    row = {
        "id": pid,
        "base": (base or "").upper(),
        "qty": float(qty or 0),
        "cost_usd": float(cost_usd or 0),
        "location": location,
        "chain": chain,
        "venue": venue,
        "contract": (contract or "").lower() or None,
        "state": OPEN,
        "next_action": next_action,
        "session_id": session_id,
        "note": note,
        "refs": {},
        "created_ts": now,
        "updated_ts": now,
        "closed_ts": None,
        "realized_usd": None,
    }
    with _lock:
        _rows.append(row)
        _save_locked()
    log.warning("ledger OPEN %s: %s %.6f @ ~$%.2f at %s%s → next=%s",
                pid, row["base"], row["qty"], row["cost_usd"], location,
                f"/{chain or venue}" if (chain or venue) else "",
                next_action or "?")
    return pid


def update(pid: str, **fields) -> bool:
    """Patch a row. `refs` merges rather than replaces so tx/order ids
    accumulate across the legs."""
    _load()
    with _lock:
        for r in _rows:
            if r["id"] != pid:
                continue
            refs = fields.pop("refs", None)
            if refs:
                r.setdefault("refs", {}).update(refs)
            r.update(fields)
            r["updated_ts"] = time.time()
            _save_locked()
            return True
    log.warning("ledger update: unknown id %s", pid)
    return False


def close(pid: str, realized_usd: float | None = None,
          note: str = "") -> bool:
    """Mark a position resolved. Only call once the inventory is back in
    a stable asset — not merely because the code path ended."""
    _load()
    with _lock:
        for r in _rows:
            if r["id"] != pid:
                continue
            r["state"] = CLOSED
            r["closed_ts"] = time.time()
            r["updated_ts"] = r["closed_ts"]
            if realized_usd is not None:
                r["realized_usd"] = float(realized_usd)
            if note:
                r["note"] = note
            _save_locked()
            # `realized_usd is None` means "resolved, proceeds unknown" —
            # not "sold for nothing". Printing it as $0.00 turned every
            # such close into a full-cost loss in the log: collapsing 17
            # duplicate LAPTOP rows read as -$3,400 that never happened.
            _real = r.get("realized_usd")
            _cost = r.get("cost_usd") or 0
            if _real is None:
                log.warning("ledger CLOSE %s: %s realized UNKNOWN "
                            "(cost $%.2f, pnl n/a) — %s",
                            pid, r["base"], _cost, (note or r.get("note") or "")[:80])
            else:
                log.warning("ledger CLOSE %s: %s realized $%.2f (cost $%.2f, "
                            "pnl $%+.2f)", pid, r["base"], float(_real),
                            _cost, float(_real) - _cost)
            return True
    log.warning("ledger close: unknown id %s", pid)
    return False


def mark_stuck(pid: str, reason: str = "") -> bool:
    """The pipeline gave up while still holding this. Stays visible."""
    _load()
    with _lock:
        for r in _rows:
            if r["id"] != pid:
                continue
            r["state"] = STUCK
            r["note"] = reason or r.get("note", "")
            r["updated_ts"] = time.time()
            _save_locked()
            log.error("ledger STUCK %s: %s %.6f at %s — %s", pid,
                      r["base"], r["qty"], r.get("chain") or r.get("venue"),
                      reason)
            return True
    return False


def open_rows() -> list[dict]:
    """Everything still holding inventory (OPEN or STUCK)."""
    _load()
    with _lock:
        return [dict(r) for r in _rows if r["state"] in (OPEN, STUCK)]


def get(pid: str) -> dict | None:
    _load()
    with _lock:
        for r in _rows:
            if r["id"] == pid:
                return dict(r)
    return None


def by_session(session_id: str) -> list[dict]:
    _load()
    with _lock:
        return [dict(r) for r in _rows
                if r.get("session_id") == session_id
                and r["state"] in (OPEN, STUCK)]


def by_base(base: str, only_open: bool = True) -> list[dict]:
    _load()
    b = (base or "").upper()
    with _lock:
        return [dict(r) for r in _rows
                if r["base"] == b
                and (not only_open or r["state"] in (OPEN, STUCK))]


def exposure_usd() -> float:
    """Total cost basis currently unresolved."""
    return sum(r.get("cost_usd") or 0 for r in open_rows())


def prune_closed(keep_days: float = 30.0) -> int:
    """Trim old CLOSED rows. OPEN/STUCK are never removed."""
    _load()
    cutoff = time.time() - keep_days * 86400
    with _lock:
        before = len(_rows)
        _rows[:] = [r for r in _rows
                    if r["state"] != CLOSED
                    or (r.get("closed_ts") or 0) > cutoff]
        removed = before - len(_rows)
        if removed:
            _save_locked()
    return removed
