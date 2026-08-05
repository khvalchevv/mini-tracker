"""Global alert blacklist — persisted to blacklist.json.

Two levels:
  - bases: {BASE, ...}                  full-mute a token everywhere
  - pairs: {BASE: {eid, eid, ...}}      mute one target CEX for that base
"""
import json
import os
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(HERE, "blacklist.json")

_lock = threading.Lock()
_data = {"bases": set(), "pairs": {}}   # pairs: base -> set[eid]


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


def _save():
    tmp = FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({
            "bases": sorted(_data["bases"]),
            "pairs": {b: sorted(v) for b, v in _data["pairs"].items()},
        }, f, indent=2)
    os.replace(tmp, FILE)


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


def is_base_banned(base: str) -> bool:
    return base.upper() in _data["bases"]


def is_pair_banned(base: str, eid: str) -> bool:
    if is_base_banned(base):
        return True
    return eid in _data["pairs"].get(base.upper(), set())


def snapshot() -> dict:
    with _lock:
        return {
            "bases": sorted(_data["bases"]),
            "pairs": {b: sorted(v) for b, v in _data["pairs"].items()},
        }
