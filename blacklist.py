"""Global alert blacklist — persisted to blacklist.json.

Three levels:
  - bases: {BASE, ...}                  full-mute a token permanently
  - pairs: {BASE: {eid, eid, ...}}      mute one target CEX for that base
  - timed: {BASE: {until_ts, reason}}   auto-populated cooldown after
                                         losing trade (default 1h)
"""
import json
import logging
import os
import threading
import time

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(HERE, "blacklist.json")

_lock = threading.Lock()
_data = {"bases": set(), "pairs": {}, "timed": {}}


def _load():
    global _data
    try:
        with open(FILE, encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        return
    except Exception:
        return
    _data["bases"] = set(d.get("bases", []))
    _data["pairs"] = {b: set(v) for b, v in (d.get("pairs") or {}).items()}
    # Timed entries: {base: {until_ts, reason}} — drop expired on load
    now = time.time()
    raw_timed = d.get("timed") or {}
    _data["timed"] = {
        b.upper(): {"until_ts": float(v.get("until_ts", 0)),
                     "reason": str(v.get("reason", ""))}
        for b, v in raw_timed.items()
        if isinstance(v, dict) and float(v.get("until_ts", 0)) > now
    }


def _save():
    tmp = FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({
            "bases": sorted(_data["bases"]),
            "pairs": {b: sorted(v) for b, v in _data["pairs"].items()},
            "timed": {b: {"until_ts": r["until_ts"], "reason": r["reason"]}
                       for b, r in _data["timed"].items()},
        }, f, indent=2)
    os.replace(tmp, FILE)


def _prune_timed_locked() -> bool:
    """Remove expired timed entries. Caller must hold _lock. Returns
    True if anything was removed."""
    now = time.time()
    stale = [b for b, r in _data["timed"].items() if r["until_ts"] <= now]
    for b in stale:
        _data["timed"].pop(b, None)
    return bool(stale)


_load()


def ban_base(base: str) -> None:
    with _lock:
        _data["bases"].add(base.upper())
        _data["pairs"].pop(base.upper(), None)          # base ban supersedes pair bans
        _save()


def unban_base(base: str) -> None:
    with _lock:
        _data["bases"].discard(base.upper())
        _save()


def ban_pair(base: str, eid: str) -> None:
    with _lock:
        _data["pairs"].setdefault(base.upper(), set()).add(eid)
        _save()


def unban_pair(base: str, eid: str) -> None:
    with _lock:
        s = _data["pairs"].get(base.upper())
        if s:
            s.discard(eid)
            if not s:
                _data["pairs"].pop(base.upper(), None)
            _save()


def ban_base_for(base: str, hours: float = 1.0, reason: str = "") -> float:
    """Time-based cooldown — block `base` for `hours`. If already blocked
    for longer, keep the longer block. Returns until_ts."""
    base = base.upper()
    until = time.time() + hours * 3600
    with _lock:
        existing = _data["timed"].get(base)
        if existing and existing["until_ts"] > until:
            return existing["until_ts"]
        _data["timed"][base] = {"until_ts": until, "reason": reason}
        _save()
    log.warning("blacklist TIMED %s for %.1fh (%s)", base, hours, reason)
    return until


def unban_timed(base: str) -> bool:
    """Remove `base` from timed cooldown. Returns True if it was there."""
    base = base.upper()
    with _lock:
        if base in _data["timed"]:
            _data["timed"].pop(base, None)
            _save()
            log.warning("blacklist UNTIMED %s", base)
            return True
    return False


def timed_info(base: str) -> tuple[bool, float, str]:
    """Return (blocked, remaining_sec, reason) for the timed cooldown."""
    base = base.upper()
    with _lock:
        rec = _data["timed"].get(base)
        if not rec:
            return False, 0.0, ""
        rem = rec["until_ts"] - time.time()
        if rem <= 0:
            _data["timed"].pop(base, None)
            _save()
            return False, 0.0, ""
        return True, rem, rec.get("reason", "")


def list_timed() -> list[tuple[str, float, str]]:
    """Return [(base, remaining_sec, reason), ...] sorted by remaining."""
    with _lock:
        if _prune_timed_locked():
            _save()
        now = time.time()
        out = [(b, r["until_ts"] - now, r.get("reason", ""))
               for b, r in _data["timed"].items()]
    out.sort(key=lambda x: x[1])
    return out


def is_base_banned(base: str) -> bool:
    """True if base is permanently banned OR in an active timed cooldown."""
    base = base.upper()
    if base in _data["bases"]:
        return True
    # Timed cooldown — honored by all callers via this same function
    with _lock:
        rec = _data["timed"].get(base)
        if not rec:
            return False
        if rec["until_ts"] > time.time():
            return True
        # Expired — clean up
        _data["timed"].pop(base, None)
        _save()
        return False


def is_pair_banned(base: str, eid: str) -> bool:
    if is_base_banned(base):
        return True
    return eid in _data["pairs"].get(base.upper(), set())


def snapshot() -> dict:
    with _lock:
        _prune_timed_locked()
        now = time.time()
        return {
            "bases": sorted(_data["bases"]),
            "pairs": {b: sorted(v) for b, v in _data["pairs"].items()},
            "timed": {b: {"remaining_sec": r["until_ts"] - now,
                           "reason": r.get("reason", "")}
                       for b, r in _data["timed"].items()},
        }
