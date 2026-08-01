"""JSON-backed pair storage.

Pair record:
    id: short unique slug
    owner: telegram user id
    a, b: side dicts, each either
        {"type": "dex", "chain": "ethereum", "addr": "0x..", "url": ".."}
        {"type": "cex", "exchange": "binance", "symbol": "ETH/USDT"}
    threshold_pct: float       # alert when |pa-pb|/min*100 >= this
    paused: bool
    last_alert_ts: float       # for cooldown
    last_spread: float | None  # last computed % (for /list display)
    last_price_a / last_price_b: float | None
"""
import json
import os
import secrets
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(HERE, "pairs.json")

_lock = threading.Lock()
_db: dict[str, dict] = {}      # pair_id -> record


def _load() -> None:
    global _db
    try:
        with open(DB_FILE, encoding="utf-8") as f:
            _db = json.load(f)
    except FileNotFoundError:
        _db = {}
    except Exception:
        _db = {}
    # migrate: side dicts without "type" are legacy DEX pools
    for rec in _db.values():
        for side in ("a", "b"):
            s = rec.get(side) or {}
            if "type" not in s:
                s["type"] = "dex"
                rec[side] = s


def _save() -> None:
    tmp = DB_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_FILE)


def init() -> None:
    _load()


def all_pairs() -> list[dict]:
    with _lock:
        return list(_db.values())


def by_owner(owner: int) -> list[dict]:
    with _lock:
        return [p for p in _db.values() if p["owner"] == owner]


def get(pair_id: str) -> dict | None:
    with _lock:
        return _db.get(pair_id)


def add(owner: int, a: dict, b: dict, threshold_pct: float) -> dict:
    pair_id = secrets.token_hex(4)
    rec = {
        "id": pair_id,
        "owner": owner,
        "a": a,
        "b": b,
        "threshold_pct": float(threshold_pct),
        "paused": False,
        "last_alert_ts": 0.0,
        "last_spread": None,
        "last_price_a": None,
        "last_price_b": None,
        "last_ts": 0.0,
        "created_ts": time.time(),
    }
    with _lock:
        _db[pair_id] = rec
        _save()
    return rec


def update(pair_id: str, **fields) -> dict | None:
    with _lock:
        rec = _db.get(pair_id)
        if not rec:
            return None
        rec.update(fields)
        _save()
        return rec


def delete(pair_id: str) -> bool:
    with _lock:
        if pair_id not in _db:
            return False
        del _db[pair_id]
        _save()
        return True
