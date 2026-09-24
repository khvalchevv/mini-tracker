"""Ground-truth trade statistics.

Two data sources, cross-checked:
  1. Portfolio snapshots (before/after) — authoritative net PnL because
     it AUTOMATICALLY captures all fees (trade, WD, gas, deposit fees,
     slippage). No arithmetic to get wrong.
  2. Per-order fields from ccxt (buy fee, sell fee) + WD fee (from
     network_info) + gas (from tx receipt) — line-item breakdown for
     display + reconciliation check.

If snapshot delta and per-line sum disagree by >$0.50, we flag the
session with `reconciliation_gap` so the user knows to check.

Storage (append-only JSONL, cheap to grep, easy to backup):
  · sessions_stats.jsonl  — one line per session (BUY→SELL cycle)
  · rebalances_stats.jsonl — one line per rebalance run

Timezone: Kyiv (UTC+3) for day-grouping.
"""
from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
SESSIONS_FILE = os.path.join(HERE, "sessions_stats.jsonl")
REBALANCES_FILE = os.path.join(HERE, "rebalances_stats.jsonl")
KYIV_TZ = timezone(timedelta(hours=3))


def _append(path: str, rec: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("stats2 append %s err: %s", path, e)


def _read_all(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning("stats2 read %s err: %s", path, e)
        return []


# ─── Session records ─────────────────────────────────────────────────
def session_start(sid: str, plan, snapshot_usd: float,
                  snapshot_detail: dict) -> None:
    """Called right before BUY. `plan` is executor.TradePlan.
    snapshot_detail = {venue: {ccy: amount, ...}, ..., "fx_eur": rate}"""
    _append(SESSIONS_FILE, {
        "type": "session_start",
        "ts": time.time(),
        "sid": sid,
        "base": plan.base,
        "chain": plan.chain,
        "direction": f"{plan.buy_eid}->{plan.sell_eid}",
        "buy_eid": plan.buy_eid,
        "sell_eid": plan.sell_eid,
        "buy_sym": plan.buy_sym,
        "sell_sym": plan.sell_sym,
        "notional_planned_usd": plan.notional_usd,
        "expected_net_usd": plan.net_profit_usd,
        "snapshot_before_usd": snapshot_usd,
        "snapshot_before_detail": snapshot_detail,
    })


def session_end(sid: str, plan, outcome: str,
                snapshot_usd: float | None, snapshot_detail: dict | None,
                real_delta_usd: float | None,
                buy_order: dict | None, sell_orders: list[dict] | None,
                buy_fee_native: float, sell_fee_native: float,
                wd_fee_usd: float, gas_usd: float,
                fx_buy: float, fx_sell: float,
                exec_qty: float, elapsed_sec: float,
                error: str | None = None,
                other_sessions_in_window: bool = False) -> None:
    """Called after SELL (success/fail) or session abort. `sell_orders`
    is the full multi-pass list (or single last order in list form)."""
    # Per-line breakdown in USD
    buy_notional_native = 0.0
    buy_avg = 0.0
    if buy_order:
        buy_notional_native = float(buy_order.get("cost") or 0)
        if not buy_notional_native:
            f = float(buy_order.get("filled") or 0)
            a = float(buy_order.get("average") or 0)
            buy_notional_native = f * a
        buy_avg = float(buy_order.get("average") or 0)
    sell_notional_native = 0.0
    sell_avg_num = 0.0
    sell_avg_den = 0.0
    for o in (sell_orders or []):
        c = float(o.get("cost") or 0) or \
            (float(o.get("filled") or 0) * float(o.get("average") or 0))
        sell_notional_native += c
        f = float(o.get("filled") or 0)
        sell_avg_num += f * float(o.get("average") or 0)
        sell_avg_den += f
    sell_avg = (sell_avg_num / sell_avg_den) if sell_avg_den else 0.0

    buy_cost_usd = (buy_notional_native + buy_fee_native) * fx_buy
    sell_proceeds_usd = (sell_notional_native - sell_fee_native) * fx_sell
    computed_pnl = sell_proceeds_usd - buy_cost_usd - wd_fee_usd - gas_usd

    # Reconciliation: snapshot delta vs computed
    recon_gap = None
    if real_delta_usd is not None:
        recon_gap = real_delta_usd - computed_pnl

    _append(SESSIONS_FILE, {
        "type": "session_end",
        "ts": time.time(),
        "sid": sid,
        "base": plan.base,
        "chain": plan.chain,
        "direction": f"{plan.buy_eid}->{plan.sell_eid}",
        "buy_eid": plan.buy_eid,
        "sell_eid": plan.sell_eid,
        "outcome": outcome,
        "elapsed_sec": elapsed_sec,
        # Ground-truth
        "snapshot_after_usd": snapshot_usd,
        "snapshot_after_detail": snapshot_detail,
        "real_delta_usd": real_delta_usd,
        # Per-line breakdown
        "buy_notional_native": buy_notional_native,
        "buy_avg_native": buy_avg,
        "buy_fee_native": buy_fee_native,
        "buy_cost_usd": buy_cost_usd,
        "sell_notional_native": sell_notional_native,
        "sell_avg_native": sell_avg,
        "sell_fee_native": sell_fee_native,
        "sell_proceeds_usd": sell_proceeds_usd,
        "wd_fee_usd": wd_fee_usd,
        "gas_usd": gas_usd,
        "trade_fee_usd_total": (buy_fee_native * fx_buy +
                                sell_fee_native * fx_sell),
        "computed_pnl_usd": computed_pnl,
        "reconciliation_gap": recon_gap,
        "fx_eur_at_end": (fx_buy if plan.buy_eid == "bitvavo"
                           else fx_sell if plan.sell_eid == "bitvavo" else None),
        "exec_qty": exec_qty,
        "notional_planned_usd": plan.notional_usd,
        "expected_net_usd": plan.net_profit_usd,
        "error": (error or "")[:300],
        "other_sessions_in_window": other_sessions_in_window,
    })


# ─── Rebalance records ────────────────────────────────────────────────
def rebalance_start(rid: str, linked_sid: str | None,
                     snapshot_usd: float, snapshot_detail: dict) -> None:
    _append(REBALANCES_FILE, {
        "type": "rebalance_start",
        "ts": time.time(),
        "rid": rid,
        "linked_sid": linked_sid,
        "snapshot_before_usd": snapshot_usd,
        "snapshot_before_detail": snapshot_detail,
    })


def rebalance_end(rid: str, linked_sid: str | None,
                   snapshot_usd: float, snapshot_detail: dict,
                   real_cost_usd: float, actions: list[dict]) -> None:
    _append(REBALANCES_FILE, {
        "type": "rebalance_end",
        "ts": time.time(),
        "rid": rid,
        "linked_sid": linked_sid,
        "snapshot_after_usd": snapshot_usd,
        "snapshot_after_detail": snapshot_detail,
        "real_cost_usd": real_cost_usd,
        "actions": actions,
    })


# ─── Aggregation ──────────────────────────────────────────────────────
def _kyiv_day(ts: float) -> str:
    """YYYY-MM-DD in Kyiv time."""
    return datetime.fromtimestamp(ts, KYIV_TZ).strftime("%Y-%m-%d")


def _kyiv_now_day() -> str:
    return datetime.now(KYIV_TZ).strftime("%Y-%m-%d")


def _closed_sessions() -> list[dict]:
    """Return session_end records with linked rebalance cost merged."""
    sess_recs = _read_all(SESSIONS_FILE)
    reb_recs = _read_all(REBALANCES_FILE)
    reb_by_sid: dict[str, float] = {}
    for r in reb_recs:
        if r.get("type") == "rebalance_end" and r.get("linked_sid"):
            reb_by_sid[r["linked_sid"]] = reb_by_sid.get(r["linked_sid"], 0) + \
                float(r.get("real_cost_usd") or 0)
    closed = []
    for s in sess_recs:
        if s.get("type") != "session_end":
            continue
        s = dict(s)
        s["rebalance_cost_usd"] = reb_by_sid.get(s.get("sid"), 0.0)
        # Combined = real delta − rebalance cost (rebalance is a REQUIRED
        # cost to restore balances for next trade)
        rd = s.get("real_delta_usd")
        if rd is not None:
            s["combined_net_usd"] = rd - s["rebalance_cost_usd"]
        else:
            s["combined_net_usd"] = None
        closed.append(s)
    return closed


def render() -> str:
    closed = _closed_sessions()
    if not closed:
        return "📈 <b>Статистика</b>\n\n<i>Ще жодного завершеного трейду.</i>"

    today = _kyiv_now_day()
    week_ago = time.time() - 7 * 86400
    day_ago = time.time() - 86400

    def _net(s):
        # Prefer real snapshot delta; fall back to computed.
        v = s.get("real_delta_usd")
        if v is None:
            v = s.get("computed_pnl_usd")
        return float(v or 0)

    completed = [s for s in closed if s.get("outcome") == "completed"]
    failed_or_cancelled = [s for s in closed
                            if s.get("outcome") in ("failed", "cancelled")]

    def _totals(subset):
        n = len(subset)
        s = sum(_net(x) for x in subset)
        wins = sum(1 for x in subset if _net(x) > 0)
        losses = sum(1 for x in subset if _net(x) <= 0)
        combined = sum(x.get("combined_net_usd") or _net(x) for x in subset)
        return {"n": n, "sum": s, "wins": wins, "losses": losses,
                "combined": combined}

    all_tot = _totals(completed)
    today_c = [s for s in completed if _kyiv_day(s["ts"]) == today]
    today_tot = _totals(today_c)
    week_c = [s for s in completed if s["ts"] >= week_ago]
    week_tot = _totals(week_c)
    day_c = [s for s in completed if s["ts"] >= day_ago]
    day_tot = _totals(day_c)

    # Fees breakdown
    sum_trade = sum(float(s.get("trade_fee_usd_total") or 0) for s in completed)
    sum_wd = sum(float(s.get("wd_fee_usd") or 0) for s in completed)
    sum_gas = sum(float(s.get("gas_usd") or 0) for s in completed)
    sum_reb = sum(float(s.get("rebalance_cost_usd") or 0) for s in completed)
    sum_all_fees = sum_trade + sum_wd + sum_gas + sum_reb
    avg_fee = sum_all_fees / len(completed) if completed else 0.0

    # By direction
    by_dir: dict[str, dict] = defaultdict(lambda: {"n": 0, "sum": 0.0})
    for s in completed:
        d = s.get("direction", "?")
        by_dir[d]["n"] += 1
        by_dir[d]["sum"] += _net(s)

    # By base
    by_base: dict[str, dict] = defaultdict(lambda: {"n": 0, "sum": 0.0})
    for s in completed:
        b = s.get("base", "?")
        by_base[b]["n"] += 1
        by_base[b]["sum"] += _net(s)
    top_bases = sorted(by_base.items(), key=lambda kv: -kv[1]["sum"])[:5]

    # Best/worst
    best = max(completed, key=_net) if completed else None
    worst = min(completed, key=_net) if completed else None

    # Reconciliation gaps
    gap_sessions = [s for s in completed
                    if abs(float(s.get("reconciliation_gap") or 0)) > 0.5]

    lines = [
        "📈 <b>Статистика</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        f"💰 <b>Тотал: ${all_tot['sum']:+,.2f}</b>  "
        f"({all_tot['n']} сесій, {all_tot['wins']}W/{all_tot['losses']}L, "
        f"win-rate {all_tot['wins'] * 100 // max(all_tot['n'], 1)}%)",
        f"     після ребалансу: <b>${all_tot['combined']:+,.2f}</b>",
        f"📅 Сьогодні: <b>${today_tot['sum']:+,.2f}</b> ({today_tot['n']} сесій)  ·  "
        f"24г: ${day_tot['sum']:+,.2f}  ·  7д: ${week_tot['sum']:+,.2f}",
    ]
    if best and worst and best != worst:
        b_day = _kyiv_day(best["ts"])
        w_day = _kyiv_day(worst["ts"])
        lines.append(
            f"🏆 Best/Worst: ${_net(best):+.2f} ({best.get('base')}, {b_day})  /  "
            f"${_net(worst):+.2f} ({worst.get('base')}, {w_day})")
    lines.append("")
    lines.append(f"<b>Комісії (тотал ${sum_all_fees:,.2f}):</b>")
    lines.append(f"  · Trade fees:  ${sum_trade:,.2f}")
    lines.append(f"  · WD fees:     ${sum_wd:,.2f}")
    lines.append(f"  · Gas:         ${sum_gas:,.2f}")
    lines.append(f"  · Ребаланс:    ${sum_reb:,.2f}")
    lines.append(f"  · Avg/session: ${avg_fee:,.2f}")
    if by_dir:
        lines.append("")
        lines.append("<b>За напрямком:</b>")
        for d, v in sorted(by_dir.items(), key=lambda kv: -kv[1]["sum"]):
            avg = v["sum"] / v["n"] if v["n"] else 0
            lines.append(f"  · {d}: {v['n']}× · ${v['sum']:+.2f} (avg ${avg:+.2f})")
    if top_bases:
        lines.append("")
        lines.append("<b>Топ базові:</b>")
        for b, v in top_bases:
            lines.append(f"  · {b}: {v['n']}× · ${v['sum']:+.2f}")

    if failed_or_cancelled:
        fc_by = defaultdict(int)
        for s in failed_or_cancelled:
            fc_by[s.get("outcome")] += 1
        parts = "  ·  ".join(f"{k}: {v}" for k, v in fc_by.items())
        lines.append(f"\n<b>Не завершено:</b> {parts}")

    # Last 10
    lines.append("")
    lines.append("<b>Останні 10:</b>")
    for s in list(reversed(closed))[:10]:
        when = datetime.fromtimestamp(s["ts"], KYIV_TZ).strftime("%m-%d %H:%M")
        emoji = {"completed": "✅", "failed": "❌",
                 "cancelled": "🛑"}.get(s.get("outcome"), "?")
        net = f"${_net(s):+,.2f}"
        base = s.get("base", "?")
        direction = s.get("direction", "?").replace("bitvavo", "bv").replace(
            "binance", "bnb").replace("gate", "gate")
        dur = int(float(s.get("elapsed_sec") or 0))
        line = f"{emoji} {when}  {base:<6} {net:>10}  {direction}  ({dur}s)"
        if s.get("rebalance_cost_usd"):
            line += f"  reb −${s['rebalance_cost_usd']:.2f}"
        lines.append(line)

    if gap_sessions:
        lines.append(f"\n⚠️ <b>{len(gap_sessions)} сесій з reconciliation gap "
                     f"&gt;$0.50</b> — snapshot Δ і computed розходяться, "
                     f"деталі в лог")
    return "\n".join(lines)
