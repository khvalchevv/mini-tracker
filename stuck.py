"""Track HW positions stuck after failed DEX cycles.

When a Bitvavo→DEX cycle fails on the Kyber-sell leg, the alt tokens
sit on HW un-liquidated. Without a record, the bot "forgets" them —
user has to manually notice via /balances and hand-craft a sell.

This module keeps a JSON file of (base, chain, contract, qty, ts) so
users can `/stuck` to list them and retry from a button.

Not automatic retries — Kyber failures are usually transient (route,
gas, CF), but blindly re-broadcasting large positions burns gas on
repeat failures. Explicit user action keeps risk visible.
"""
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger(__name__)

_STORE = Path(os.getenv("STUCK_STORE", "stuck_positions.json"))
_ITEMS: list[dict] = []
_LOADED = False


def _load() -> None:
    global _LOADED
    if _LOADED:
        return
    _LOADED = True
    try:
        if _STORE.exists():
            _ITEMS.extend(json.loads(_STORE.read_text(encoding="utf-8")))
            log.info("stuck: loaded %d positions", len(_ITEMS))
    except Exception as e:
        log.warning("stuck load err: %s", e)


def _save() -> None:
    try:
        _STORE.write_text(json.dumps(_ITEMS, indent=2), encoding="utf-8")
    except Exception as e:
        log.warning("stuck save err: %s", e)


def add(base: str, chain: str | None, contract: str | None, qty: float,
        paid_usd: float, err: str = "", venue: str | None = None) -> None:
    """Record a stuck HW position after a failed Kyber sell.

    Idempotent per (chain, contract): the reconciler runs from a session
    `finally` AND from the periodic sweep, so the same untouched balance
    was filed twice — the list showed ACX 6347 as two separate
    positions, which reads like $526 stranded instead of $263. Refresh
    the existing row instead of appending a duplicate.
    """
    _load()
    # A position can sit on an EXCHANGE (no chain/contract) just as
    # easily as on the wallet. The old signature required both, so the
    # reconciler skipped every CEX-held position — it told the user
    # "дивись /stuck" while /stuck answered "немає застряглих".
    key = (chain or f"cex:{venue}", (contract or venue or "").lower())
    for it in _ITEMS:
        if (it.get("chain") or f"cex:{it.get('venue')}",
                (it.get("contract") or it.get("venue") or "").lower()) == key:
            it["qty"] = float(qty)
            if paid_usd:
                it["paid_usd"] = float(paid_usd)
            it["err"] = err[:200]
            it["updated_ts"] = time.time()
            _save()
            log.info("stuck: refreshed %s %.4f on %s (already tracked)",
                     base, qty, chain)
            return
    _ITEMS.append({
        "id": f"{base}-{int(time.time())}",
        "base": base.upper(),
        "chain": chain,
        "venue": venue,
        "contract": (contract or "").lower() or None,
        "qty": float(qty),
        "paid_usd": float(paid_usd),
        "err": err[:200],
        "created_ts": time.time(),
    })
    _save()
    log.warning("stuck: recorded %s %.4f on %s (paid ~$%.2f, err: %s)",
                base, qty, chain or f"cex:{venue}", paid_usd, err[:80])


def remove(item_id: str) -> bool:
    """Remove a resolved stuck position. Returns True if it existed."""
    _load()
    before = len(_ITEMS)
    _ITEMS[:] = [i for i in _ITEMS if i.get("id") != item_id]
    if len(_ITEMS) != before:
        _save()
        log.info("stuck: removed %s", item_id)
        return True
    return False


def list_all() -> list[dict]:
    _load()
    return list(_ITEMS)


def get(item_id: str) -> dict | None:
    _load()
    for i in _ITEMS:
        if i.get("id") == item_id:
            return i
    return None
