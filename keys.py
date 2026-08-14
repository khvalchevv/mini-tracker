"""Load exchange API keys from api_keys.json and wire them into ccxt.

Format:
    {
      "binance": {"apiKey": "...", "secret": "..."},
      "gate":    {"apiKey": "...", "secret": "..."},
      "bitget":  {"apiKey": "...", "secret": "...", "password": "..."},
      "bitvavo": {"apiKey": "...", "secret": "..."}
    }

Chmod 600, git-ignored. IP-restrict each key at the exchange dashboard.
"""
import json
import logging
import os

import cex

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(HERE, "api_keys.json")


def load_keys() -> dict:
    try:
        with open(FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("api_keys.json load err: %s", e)
        return {}


def wire_all() -> dict[str, bool]:
    """Attach credentials to every ccxt instance. Returns eid -> keyed?"""
    keys = load_keys()
    status = {}
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = keys.get(eid)
        if not creds:
            status[eid] = False
            continue
        inst = cex._get(eid)
        inst.apiKey = creds.get("apiKey", "")
        inst.secret = creds.get("secret", "")
        if creds.get("password"):
            inst.password = creds["password"]
        if creds.get("uid"):
            inst.uid = creds["uid"]
        status[eid] = bool(inst.apiKey and inst.secret)
    keyed = [e for e, ok in status.items() if ok]
    log.info("keys: wired %s (unkeyed: %s)",
             keyed, [e for e, ok in status.items() if not ok])
    return status


def has_keys(eid: str) -> bool:
    inst = cex._instances.get(eid)
    return bool(inst and inst.apiKey and inst.secret)
