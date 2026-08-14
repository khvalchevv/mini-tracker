"""Trade executor — state machine per alert, dry-run by default.

Given an alert dict from hunter (with `base`, `bitvavo_price`, `entries`),
the executor plans and (optionally) executes the two-leg trade against
the top target CEX. Every state transition is posted live to the
subscribers via `status_cb`, and a final JSON receipt goes to trades.jsonl.

Modes:
    "dry"   — never touches exchange APIs; logs the intended orders.
    "live"  — actually places IOC orders + triggers withdraw/deposit
              polling + sells on the destination. Requires api_keys.json
              loaded into ccxt instances.

State machine:
    PLANNING → PLACING_BUY → BUY_FILLED → WITHDRAWING →
    CONFIRMING → DEPOSITED → PLACING_SELL → SELL_FILLED → DONE
    (any step can transition to FAILED with a reason)
"""
import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field

import cex

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(HERE, "trades.jsonl")

# state constants (strings for log/persist readability)
S_PLANNING     = "PLANNING"
S_PLACING_BUY  = "PLACING_BUY"
S_BUY_FILLED   = "BUY_FILLED"
S_WITHDRAWING  = "WITHDRAWING"
S_CONFIRMING   = "CONFIRMING"
S_DEPOSITED    = "DEPOSITED"
S_PLACING_SELL = "PLACING_SELL"
S_SELL_FILLED  = "SELL_FILLED"
S_DONE         = "DONE"
S_FAILED       = "FAILED"

_KILL = False                                          # global halt flag


def set_kill(v: bool):
    global _KILL
    _KILL = bool(v)


def kill_active() -> bool:
    return _KILL


@dataclass
class TradePlan:
    trade_id: str
    base: str
    buy_eid: str
    buy_sym: str
    sell_eid: str
    sell_sym: str
    bitvavo_side: str                                  # "buy" or "sell"
    target_price: float                                # sizing.last_buy or _sell in USD
    qty: float
    notional_usd: float
    expected_profit_usd: float
    chain: str | None = None                           # chain to withdraw over


@dataclass
class TradeReceipt:
    trade_id: str
    base: str
    buy_eid: str
    sell_eid: str
    state: str = S_PLANNING
    error: str | None = None
    planned: TradePlan | None = None
    buy_order: dict | None = None
    withdraw: dict | None = None
    sell_order: dict | None = None
    started_ts: float = field(default_factory=time.time)
    finished_ts: float | None = None
    net_pnl_usd: float | None = None
    steps: list = field(default_factory=list)          # [(ts, state, msg)]


def _persist(receipt: TradeReceipt):
    try:
        with open(TRADES_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "trade_id": receipt.trade_id,
                "base": receipt.base,
                "buy_eid": receipt.buy_eid,
                "sell_eid": receipt.sell_eid,
                "state": receipt.state,
                "error": receipt.error,
                "net_pnl_usd": receipt.net_pnl_usd,
                "started_ts": receipt.started_ts,
                "finished_ts": receipt.finished_ts,
                "steps": receipt.steps,
            }) + "\n")
    except Exception as e:
        log.warning("trades persist err: %s", e)


class Executor:
    def __init__(self, mode: str = "dry"):
        self.mode = mode                               # "dry" | "live"
        self.status_cb = None                          # set by wire_status()

    def wire_status(self, cb):
        """`cb(receipt, message: str)` — posts a live line to TG."""
        self.status_cb = cb

    async def _post(self, r: TradeReceipt, state: str, msg: str):
        r.state = state
        r.steps.append((time.time(), state, msg))
        log.info("[%s] %s → %s: %s", r.trade_id, r.base, state, msg)
        if self.status_cb:
            try:
                await self.status_cb(r, msg)
            except Exception as e:
                log.debug("status_cb err: %s", e)

    # -------------------------------------------------------- planning

    def plan(self, alert: dict, sizing_result: dict) -> TradePlan:
        """Build a TradePlan from an alert + its cross_match sizing result."""
        base = alert["base"]
        entries = alert["entries"]
        top = entries[0]
        if top["kind"] != "cex":
            raise ValueError("top target is not a CEX; DEX side not wired yet")

        bpx = alert["bitvavo_price"]
        tpx = top["price"]
        bv_sym = f"{base}/EUR"
        if bpx < tpx:
            buy_eid, buy_sym = "bitvavo", bv_sym
            sell_eid, sell_sym = top["eid"], top["symbol"]
            bv_side = "buy"
            target_price = sizing_result["last_sell_native"]
        else:
            buy_eid, buy_sym = top["eid"], top["symbol"]
            sell_eid, sell_sym = "bitvavo", bv_sym
            bv_side = "sell"
            target_price = sizing_result["last_buy_native"]

        return TradePlan(
            trade_id=uuid.uuid4().hex[:10],
            base=base,
            buy_eid=buy_eid,
            buy_sym=buy_sym,
            sell_eid=sell_eid,
            sell_sym=sell_sym,
            bitvavo_side=bv_side,
            target_price=target_price,
            qty=sizing_result["qty"],
            notional_usd=sizing_result["notional_usd"],
            expected_profit_usd=sizing_result["profit_usd"],
            chain=None,                                # picked in Phase 2
        )

    # ------------------------------------------------------ execution

    async def run(self, plan: TradePlan) -> TradeReceipt:
        r = TradeReceipt(
            trade_id=plan.trade_id, base=plan.base,
            buy_eid=plan.buy_eid, sell_eid=plan.sell_eid,
            planned=plan,
        )
        try:
            if _KILL:
                r.error = "killed"
                await self._post(r, S_FAILED, "🛑 /kill active — aborted before start")
                _persist(r)
                return r

            await self._post(
                r, S_PLANNING,
                f"⚡ <b>{plan.base}</b>  ·  <b>${plan.notional_usd:,.0f}</b> "
                f"(~{plan.qty:,.4f} {plan.base}) · exp profit "
                f"<b>${plan.expected_profit_usd:,.2f}</b>\n"
                f"   Buy {cex.pretty(plan.buy_eid)} <code>{plan.buy_sym}</code> "
                f"→ Sell {cex.pretty(plan.sell_eid)} <code>{plan.sell_sym}</code>",
            )

            # ---- PLACING_BUY ------------------------------------------------
            if self.mode == "dry":
                await self._post(
                    r, S_PLACING_BUY,
                    f"1/6 🧪 <i>dry-run</i> — WOULD place IOC BUY "
                    f"<b>{plan.qty:.6f} {plan.base}</b> on {cex.pretty(plan.buy_eid)} "
                    f"@ limit {plan.target_price:g}",
                )
                r.buy_order = {"dry_run": True, "qty": plan.qty, "price": plan.target_price}
            else:
                # TODO: real placement — create_order(sym, 'limit', 'buy', qty, price, {'timeInForce':'IOC'})
                await self._post(r, S_FAILED, "live mode not wired yet")
                r.error = "live mode not wired"
                _persist(r)
                return r

            await self._post(r, S_BUY_FILLED, f"2/6 ✅ <i>dry-run</i> — buy leg 'filled'")

            # ---- WITHDRAWING (Bitvavo → target OR target → Bitvavo) --------
            direction = "→" if plan.bitvavo_side == "buy" else "←"
            await self._post(
                r, S_WITHDRAWING,
                f"3/6 🧪 <i>dry-run</i> — WOULD withdraw {plan.qty:.6f} {plan.base} "
                f"({plan.buy_eid} {direction} {plan.sell_eid}) via <i>[chain TBD]</i>",
            )
            r.withdraw = {"dry_run": True, "qty": plan.qty}

            await self._post(r, S_CONFIRMING, "4/6 🧪 <i>dry-run</i> — skipping confirmations")
            await self._post(r, S_DEPOSITED,  "5/6 🧪 <i>dry-run</i> — deposit 'credited'")

            # ---- PLACING_SELL ----------------------------------------------
            await self._post(
                r, S_PLACING_SELL,
                f"6/6 🧪 <i>dry-run</i> — WOULD place IOC SELL "
                f"<b>{plan.qty:.6f} {plan.base}</b> on {cex.pretty(plan.sell_eid)} "
                f"@ limit {plan.target_price:g}",
            )
            r.sell_order = {"dry_run": True}

            r.net_pnl_usd = plan.expected_profit_usd
            r.finished_ts = time.time()
            await self._post(
                r, S_DONE,
                f"✅ <b>DONE (dry)</b> — exp net PnL <b>${r.net_pnl_usd:,.2f}</b>",
            )
            _persist(r)
            return r

        except Exception as e:
            r.error = str(e)
            log.exception("executor crashed for %s", plan.trade_id)
            await self._post(r, S_FAILED, f"❌ crashed: {str(e)[:120]}")
            _persist(r)
            return r


# module-level singleton
_EX: Executor | None = None


def instance() -> Executor:
    global _EX
    if _EX is None:
        _EX = Executor(mode=os.getenv("EXEC_MODE", "dry"))
    return _EX
