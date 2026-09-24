"""Persistent aggregate trade statistics. Complements trades.jsonl
(append-only receipts) with a compact snapshot users care about:
completed vs failed count, gross/net profit, biggest win/loss, etc."""
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
STATS_FILE = os.path.join(HERE, "trades_stats.json")

_DEFAULT = {
    "completed": 0,       # cycles that fully closed (SELL filled)
    "failed": 0,          # buy filled but never reached SELL
    "cancelled": 0,       # user stop / kill
    "sum_net_usd": 0.0,   # cumulative realized P&L
    "sum_gross_usd": 0.0,
    "sum_fees_usd": 0.0,
    "wins": 0,            # completed with net > 0
    "losses": 0,          # completed with net <= 0
    "biggest_win_usd": 0.0,
    "biggest_loss_usd": 0.0,
    "last_completed_ts": 0,
    "last_10": [],        # last 10 outcomes: {ts, base, outcome, net_usd, dur_sec}
}


def load() -> dict:
    try:
        with open(STATS_FILE, encoding="utf-8") as f:
            d = json.load(f)
        for k, v in _DEFAULT.items():
            d.setdefault(k, v)
        return d
    except FileNotFoundError:
        return dict(_DEFAULT)
    except Exception:
        return dict(_DEFAULT)


def _save(d: dict):
    try:
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
        os.replace(tmp, STATS_FILE)
    except Exception:
        pass


def record_rebalance_cost(cost_usd: float) -> None:
    """Attach the rebalance cost to the most recent trade in last_10 (and
    subtract from cumulative net). Called after the auto-rebalance that
    follows a successful trade completes."""
    d = load()
    d.setdefault("rebalance_cost_sum", 0.0)
    d["rebalance_cost_sum"] += cost_usd
    d["sum_net_usd"] -= cost_usd                      # combined net = trade − rebalance
    last = d.get("last_10") or []
    if last and last[0].get("outcome") == "completed":
        last[0]["rebalance_cost"] = round(cost_usd, 4)
        last[0]["combined_net"] = round(last[0].get("net_usd", 0) - cost_usd, 4)
    _save(d)


def record(outcome: str, base: str, net_usd: float = 0.0,
           gross_usd: float = 0.0, fees_usd: float = 0.0,
           dur_sec: float = 0.0) -> None:
    """Record a single closed session.
    outcome: 'completed' | 'failed' | 'cancelled'"""
    d = load()
    if outcome == "completed":
        d["completed"] += 1
        d["sum_net_usd"] += net_usd
        d["sum_gross_usd"] += gross_usd
        d["sum_fees_usd"] += fees_usd
        if net_usd > 0:
            d["wins"] += 1
            if net_usd > d["biggest_win_usd"]:
                d["biggest_win_usd"] = net_usd
        else:
            d["losses"] += 1
            if net_usd < d["biggest_loss_usd"]:
                d["biggest_loss_usd"] = net_usd
        d["last_completed_ts"] = time.time()
    elif outcome == "failed":
        d["failed"] += 1
    elif outcome == "cancelled":
        d["cancelled"] += 1
    entry = {"ts": time.time(), "base": base, "outcome": outcome,
             "net_usd": round(net_usd, 4), "dur_sec": int(dur_sec)}
    d["last_10"] = ([entry] + list(d.get("last_10") or []))[:10]
    _save(d)


def render_summary() -> str:
    d = load()
    total = d["completed"] + d["failed"] + d["cancelled"]
    winrate = (d["wins"] / d["completed"] * 100) if d["completed"] else 0.0
    lines = [
        "📊 <b>Статистика прогонів</b>",
        f"• Всього сесій: <b>{total}</b>  ·  "
        f"завершено: <b>{d['completed']}</b>  ·  "
        f"фейл: <b>{d['failed']}</b>  ·  скасовано: {d['cancelled']}",
        f"• Виграш/програш: <b>{d['wins']}</b>/<b>{d['losses']}</b>  ·  "
        f"win-rate: <b>{winrate:.0f}%</b>",
        f"• Сумарний чистий P&L: <b>${d['sum_net_usd']:+,.2f}</b>",
        f"• Комісії всього: ${d['sum_fees_usd']:,.2f}",
        f"• Найбільший +/−: <b>${d['biggest_win_usd']:+,.2f}</b> / "
        f"<b>${d['biggest_loss_usd']:+,.2f}</b>",
    ]
    last = d.get("last_10") or []
    if last:
        lines.append("─" * 15)
        lines.append("<b>Останні 10:</b>")
        for e in last:
            when = time.strftime("%m-%d %H:%M", time.localtime(e["ts"]))
            emoji = {"completed": "✅", "failed": "❌",
                     "cancelled": "🛑"}.get(e["outcome"], "?")
            net = f"${e['net_usd']:+,.2f}" if e["outcome"] == "completed" else "—"
            lines.append(f"{emoji} {when}  {e['base']:<6}  {net}  ({e['dur_sec']}s)")
    return "\n".join(lines)
