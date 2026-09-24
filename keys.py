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
    """Report which exchanges have credentials in api_keys.json.
    IMPORTANT: We DO NOT attach keys to the shared public instance
    (`cex._get(eid)`) — if we did, ccxt would auto-prefer signed
    private endpoints for load_markets etc., and those would go via
    the public proxy pool (random IPs) → 'Invalid API-key/IP'. Private
    calls use dedicated instances via `cex.get_private()`."""
    keys_data = load_keys()
    status = {eid: bool((keys_data.get(eid) or {}).get("apiKey")
                        and (keys_data.get(eid) or {}).get("secret"))
              for eid in cex.SUPPORTED_EXCHANGES}
    keyed = [e for e, ok in status.items() if ok]
    log.info("keys: keyed %s (unkeyed: %s)",
             keyed, [e for e, ok in status.items() if not ok])
    return status


def has_keys(eid: str) -> bool:
    """True if api_keys.json has both apiKey+secret for this exchange."""
    creds = load_keys().get(eid) or {}
    return bool(creds.get("apiKey") and creds.get("secret"))


async def handshake_all() -> dict[str, dict]:
    """Verify each keyed exchange responds to fetch_balance.
    Uses dedicated `get_private()` instances so there's no race with
    hunter's public-price proxy rotation."""
    results: dict[str, dict] = {}
    interesting = ("EUR", "USDT", "USDC", "USD", "BTC", "ETH")
    keys_data = load_keys()
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = keys_data.get(eid) or {}
        if not (creds.get("apiKey") and creds.get("secret")):
            results[eid] = {"status": "no_keys", "msg": "no api key in api_keys.json"}
            continue
        bal = None
        last_err = None
        for _ in range(4):                                       # rotate a few proxies on Cloudflare-picky bitvavo
            try:
                inst = cex.get_private(eid, creds)
                # Prime clock offset once for exchanges strict about
                # timestamp drift (Binance -1021 fires >1s ahead).
                if eid in {"binance", "bybit", "mexc"} and not inst.options.get("timeDifference"):
                    try:
                        await inst.load_time_difference()
                    except Exception:
                        pass
                bal = await inst.fetch_balance()
                break
            except Exception as e:
                last_err = e
        if bal is None:
            results[eid] = {"status": "err", "msg": str(last_err)[:140]}
            log.warning("keys handshake %s FAIL: %s", eid, last_err)
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


MIN_USD_DUST = 1.0                                                 # hide tokens worth < $1


def _usd_price(ccy: str, price_map: dict[str, float] | None) -> float | None:
    """Best-effort USD price for a currency symbol. None if unknown."""
    u = (ccy or "").upper()
    if u in cex.USD_LIKE:
        return 1.0
    if u == "EUR":
        return cex.get_fx_rate("EUR") or 1.0
    if price_map:
        return price_map.get(u)
    return None


async def balances_all(price_map: dict[str, float] | None = None) -> dict[str, dict]:
    """Full balance snapshot per keyed exchange. Tokens worth < $1 hidden.
    `price_map` maps BASE symbol (uppercase) → USD price — pass hunter's
    last_bitvavo_prices so we can value alt-coins."""
    results: dict[str, dict] = {}
    keys_data = load_keys()
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = keys_data.get(eid) or {}
        if not (creds.get("apiKey") and creds.get("secret")):
            continue
        # 3-attempt retry with fresh proxy for transient failures
        # (Bitvavo timestamp-window, Gate rate-limit, etc.)
        import asyncio as _aio
        bal = None
        last_err = None
        for attempt in range(3):
            try:
                inst = cex.get_private(eid, creds)
                if eid == "binance":
                    try: await inst.load_time_difference()
                    except Exception: pass
                bal = await inst.fetch_balance()
                break
            except Exception as e:
                last_err = e
                if eid == "bitvavo":
                    inst.aiohttp_proxy = cex._pick_proxy()
                await _aio.sleep(1.5)
        if bal is None:
            results[eid] = {"err": str(last_err)[:120]}
            continue
        totals: dict[str, dict] = {}
        usd_est = 0.0
        for ccy, info in bal.items():
            if not isinstance(info, dict):
                continue
            tot = info.get("total")
            if not isinstance(tot, (int, float)) or tot <= 0:
                continue
            px = _usd_price(ccy, price_map)
            usd = (px or 0) * float(tot)
            if usd < MIN_USD_DUST:                                # hide dust
                continue
            totals[ccy] = {
                "free": float(info.get("free") or 0),
                "used": float(info.get("used") or 0),
                "total": float(tot),
                "usd": usd,
            }
            usd_est += usd
        results[eid] = {"totals": totals, "usd_estimate": usd_est}
    return results


def format_balances(results: dict[str, dict],
                    wallet_snap: dict | None = None) -> str:
    lines = ["💰 <b>Balances</b>"]
    grand_usd = 0.0
    for eid, r in results.items():
        if r.get("err"):
            lines.append(f"\n<b>{cex.pretty(eid)}</b> — ❌ {r['err']}")
            continue
        totals = r.get("totals") or {}
        usd = r.get("usd_estimate") or 0
        grand_usd += usd
        if not totals:
            lines.append(f"\n<b>{cex.pretty(eid)}</b> — (пусто)")
            continue
        rows = sorted(totals.items(), key=lambda kv: -kv[1]["usd"])
        lines.append(f"\n<b>{cex.pretty(eid)}</b>  <i>≈${usd:,.2f}</i>")
        for ccy, t in rows[:15]:
            free = t["free"]; used = t["used"]; tot = t["total"]
            u = t["usd"]
            if used > 0:
                lines.append(f"  · {ccy}: {tot:.6g} <i>(${u:,.2f}; {free:.6g} free / {used:.6g} lock)</i>")
            else:
                lines.append(f"  · {ccy}: {tot:.6g} <i>(${u:,.2f})</i>")
        if len(rows) > 15:
            lines.append(f"  <i>… ще {len(rows)-15}</i>")

    # ---- Hot wallet section (on-chain) --------------------------------
    if wallet_snap:
        addr = wallet_snap.get("address", "")
        sol_addr = wallet_snap.get("solana_address")
        chains = wallet_snap.get("chains", {})
        wallet_usd = wallet_snap.get("usd_estimate") or 0.0
        grand_usd += wallet_usd
        short_evm = f"{addr[:6]}…{addr[-4:]}" if addr else ""
        header = f"\n<b>💳 Hot wallet</b>  <i>≈${wallet_usd:,.2f}</i>\n  <code>{short_evm}</code> (EVM)"
        if sol_addr:
            short_sol = f"{sol_addr[:6]}…{sol_addr[-4:]}"
            header += f"\n  <code>{short_sol}</code> (Solana)"
        lines.append(header)
        if not chains:
            lines.append("  <i>(нічого > $1)</i>")
        for chain, entries in chains.items():
            if not entries:
                continue
            row = ", ".join(f"{sym} {amt:.6g} <i>(${u:,.2f})</i>"
                            for sym, amt, u in entries)
            lines.append(f"  · <b>{chain}</b>: {row}")

    lines.append(f"\n<b>Разом ≈${grand_usd:,.2f}</b>")
    return "\n".join(lines)
