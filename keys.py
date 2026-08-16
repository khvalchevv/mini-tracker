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


async def handshake_all() -> dict[str, dict]:
    """For each supported exchange, verify the API key works by calling
    fetch_balance. Returns {eid: {"status": "ok"|"no_keys"|"err",
    "msg": str, "quotes": {ccy: free_amount}}}.
    Only 4 quote currencies (EUR/USDT/USDC/USD) are returned to keep
    the report compact — enough to know if there's tradeable inventory."""
    results: dict[str, dict] = {}
    interesting = ("EUR", "USDT", "USDC", "USD", "BTC", "ETH")
    for eid in cex.SUPPORTED_EXCHANGES:
        inst = cex._instances.get(eid) or cex._get(eid)
        if not (inst.apiKey and inst.secret):
            results[eid] = {"status": "no_keys", "msg": "no api key in api_keys.json"}
            continue
        try:
            inst.aiohttp_proxy = None                            # private endpoint → direct
            bal = await inst.fetch_balance()
        except Exception as e:
            results[eid] = {"status": "err", "msg": str(e)[:140]}
            log.warning("keys handshake %s FAIL: %s", eid, e)
            continue
        quotes = {}
        for ccy in interesting:
            free = float((bal.get(ccy) or {}).get("free") or 0)
            if free > 0:
                quotes[ccy] = free
        results[eid] = {"status": "ok", "msg": "balance ok", "quotes": quotes}
        log.info("keys handshake %s OK · %s", eid, quotes)
    return results


def format_report(results: dict[str, dict]) -> str:
    lines = ["🔑 <b>API keys handshake</b>"]
    for eid, r in results.items():
        status = r["status"]
        if status == "ok":
            q = r.get("quotes") or {}
            qs = ", ".join(f"{v:.4g} {k}" for k, v in q.items()) if q else "(no balance)"
            lines.append(f"  ✅ {cex.pretty(eid)} — {qs}")
        elif status == "no_keys":
            lines.append(f"  ⚫ {cex.pretty(eid)} — no keys")
        else:
            lines.append(f"  ❌ {cex.pretty(eid)} — {r['msg']}")
    return "\n".join(lines)


async def balances_all() -> dict[str, dict]:
    """Full balance snapshot per keyed exchange. Returns
    {eid: {"totals": {ccy: {free, used, total}}, "usd_estimate": float, "err": str?}}."""
    results: dict[str, dict] = {}
    for eid in cex.SUPPORTED_EXCHANGES:
        inst = cex._instances.get(eid) or cex._get(eid)
        if not (inst.apiKey and inst.secret):
            continue
        try:
            inst.aiohttp_proxy = None
            bal = await inst.fetch_balance()
        except Exception as e:
            results[eid] = {"err": str(e)[:120]}
            continue
        totals = {}
        # ccxt returns per-currency dict + top-level 'free', 'used', 'total' aggregates
        for ccy, info in bal.items():
            if not isinstance(info, dict):
                continue
            tot = info.get("total")
            if not isinstance(tot, (int, float)) or tot <= 0:
                continue
            totals[ccy] = {
                "free": float(info.get("free") or 0),
                "used": float(info.get("used") or 0),
                "total": float(tot),
            }
        # rough USD estimate: sum with 1:1 for USD-like, else use recent Bitvavo/Binance ticker
        usd_est = 0.0
        for ccy, t in totals.items():
            if ccy.upper() in cex.USD_LIKE:
                usd_est += t["total"]
            elif ccy.upper() == "EUR":
                usd_est += t["total"] * (cex.get_fx_rate("EUR") or 1.0)
        results[eid] = {"totals": totals, "usd_estimate": usd_est}
    return results


def format_balances(results: dict[str, dict]) -> str:
    lines = ["💰 <b>Balances</b>"]
    for eid, r in results.items():
        if r.get("err"):
            lines.append(f"\n<b>{cex.pretty(eid)}</b> — ❌ {r['err']}")
            continue
        totals = r.get("totals") or {}
        usd = r.get("usd_estimate") or 0
        if not totals:
            lines.append(f"\n<b>{cex.pretty(eid)}</b> — (empty)")
            continue
        rows = sorted(totals.items(), key=lambda kv: -kv[1]["total"])
        lines.append(f"\n<b>{cex.pretty(eid)}</b>  <i>(≈${usd:,.2f} in stables)</i>")
        for ccy, t in rows[:15]:
            free = t["free"]
            used = t["used"]
            tot = t["total"]
            if used > 0:
                lines.append(f"  · {ccy}: {tot:.6g} <i>({free:.6g} free, {used:.6g} locked)</i>")
            else:
                lines.append(f"  · {ccy}: {tot:.6g}")
        if len(rows) > 15:
            lines.append(f"  <i>… {len(rows)-15} more</i>")
    return "\n".join(lines)
