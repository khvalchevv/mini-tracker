"""Trade executor — dry-run OR live CEX execution + Kyber DEX swap.

Given an alert dict (with `base`, `bitvavo_price`, `entries`) + its
sizing result, the executor plans and runs the two-leg trade against
the top target CEX. Every state transition posts a live line to TG.

Modes (env EXEC_MODE):
    "dry"   (default) — no API calls; logs intended orders + persists.
    "live"  — real orders + withdraws + deposit polling + sells.

State machine:
    PLANNING → PLACING_BUY → BUY_FILLED → WITHDRAWING →
    CONFIRMING → DEPOSITED → PLACING_SELL → SELL_FILLED → DONE
    (any step can transition to FAILED with a reason)

Persists every attempt to trades.jsonl. Auto-pauses execution
after 3 consecutive failures.
"""
import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field

import cex
import chains
import fees
import keys

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(HERE, "trades.jsonl")
ADDRESSES_FILE = os.path.join(HERE, "deposit_addresses.json")

# state names
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

_KILL = False
_CONSECUTIVE_FAILS = 0
FAIL_LIMIT = int(os.getenv("EXEC_FAIL_LIMIT", "3"))
DEPOSIT_POLL_SEC = int(os.getenv("EXEC_DEPOSIT_POLL_SEC", "20"))
DEPOSIT_TIMEOUT_MIN = int(os.getenv("EXEC_DEPOSIT_TIMEOUT_MIN", "45"))


def set_kill(v: bool):
    global _KILL
    _KILL = bool(v)


def kill_active() -> bool:
    return _KILL


def _load_addresses() -> dict:
    """{exchange: {chain: {"address": "...", "tag": "..."}}}"""
    try:
        with open(ADDRESSES_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("deposit_addresses.json load err: %s", e)
        return {}


@dataclass
class TradePlan:
    trade_id: str
    base: str
    buy_eid: str
    buy_sym: str
    sell_eid: str
    sell_sym: str
    buy_limit: float                                   # native buy quote
    sell_limit: float                                  # native sell quote
    qty: float
    notional_usd: float
    expected_profit_usd: float
    net_profit_usd: float                              # after fees
    chain: str | None
    eta_min: float | None
    fees: dict | None


@dataclass
class TradeReceipt:
    trade_id: str
    base: str
    buy_eid: str
    sell_eid: str
    mode: str
    state: str = S_PLANNING
    error: str | None = None
    buy_order: dict | None = None
    withdraw: dict | None = None
    sell_order: dict | None = None
    net_pnl_usd: float | None = None
    started_ts: float = field(default_factory=time.time)
    finished_ts: float | None = None
    steps: list = field(default_factory=list)          # [(ts, state, msg)]


def _persist(r: TradeReceipt, plan: TradePlan | None = None):
    try:
        with open(TRADES_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "trade_id": r.trade_id,
                "base": r.base,
                "buy_eid": r.buy_eid,
                "sell_eid": r.sell_eid,
                "mode": r.mode,
                "state": r.state,
                "error": r.error,
                "net_pnl_usd": r.net_pnl_usd,
                "started_ts": r.started_ts,
                "finished_ts": r.finished_ts,
                "chain": (plan.chain if plan else None),
                "qty": (plan.qty if plan else None),
                "notional_usd": (plan.notional_usd if plan else None),
                "steps": [(t, s, m[:200]) for (t, s, m) in r.steps],
            }) + "\n")
    except Exception as e:
        log.warning("trades persist err: %s", e)


class Executor:
    def __init__(self, mode: str = "dry"):
        self.mode = mode
        self.status_cb = None
        self.addresses = _load_addresses()

    def wire_status(self, cb):
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

    # ─── planning ────────────────────────────────────────────────────

    async def plan(self, alert: dict, sizing_result: dict) -> TradePlan:
        base = alert["base"]
        entries = alert["entries"]
        top = entries[0]
        if top["kind"] != "cex":
            raise ValueError("DEX plan handled by dex.py — this executor is CEX-only")

        bpx = alert["bitvavo_price"]
        tpx = top["price"]
        bv_sym = f"{base}/EUR"
        if bpx < tpx:
            buy_eid, buy_sym = "bitvavo", bv_sym
            sell_eid, sell_sym = top["eid"], top["symbol"]
        else:
            buy_eid, buy_sym = top["eid"], top["symbol"]
            sell_eid, sell_sym = "bitvavo", bv_sym

        # Chain policy: pick fastest common network
        buy_nets = cex.network_info(buy_eid, base)
        sell_nets = cex.network_info(sell_eid, base)
        chain_pick = chains.pick_transfer_chain(buy_nets, sell_nets)
        chain = chain_pick["chain"] if chain_pick else None
        eta = chain_pick["eta_min"] if chain_pick else None

        # Fees
        tk_buy = await fees.taker_fee(buy_eid, buy_sym)
        tk_sell = await fees.taker_fee(sell_eid, sell_sym)
        fee_bundle = fees.total_fees_usd(
            buy_eid, buy_sym, sizing_result["notional_usd"],
            sell_eid, sell_sym, sizing_result["notional_usd"] + sizing_result["profit_usd"],
            base, chain, sizing_result["avg_buy_usd"],
            tk_buy, tk_sell,
        )
        net_profit = sizing_result["profit_usd"] - fee_bundle["total_usd"]

        return TradePlan(
            trade_id=uuid.uuid4().hex[:10],
            base=base,
            buy_eid=buy_eid, buy_sym=buy_sym,
            sell_eid=sell_eid, sell_sym=sell_sym,
            buy_limit=sizing_result["last_buy_native"],
            sell_limit=sizing_result["last_sell_native"],
            qty=sizing_result["qty"],
            notional_usd=sizing_result["notional_usd"],
            expected_profit_usd=sizing_result["profit_usd"],
            net_profit_usd=net_profit,
            chain=chain,
            eta_min=eta,
            fees=fee_bundle,
        )

    # ─── execution ───────────────────────────────────────────────────

    async def run(self, plan: TradePlan) -> TradeReceipt:
        global _CONSECUTIVE_FAILS
        r = TradeReceipt(
            trade_id=plan.trade_id, base=plan.base,
            buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=self.mode,
        )
        try:
            if _KILL:
                r.error = "killed"
                await self._post(r, S_FAILED, "🛑 /kill active — aborted")
                _persist(r, plan); return r
            if _CONSECUTIVE_FAILS >= FAIL_LIMIT:
                r.error = "auto-paused"
                await self._post(r, S_FAILED,
                                 f"⏸ Auto-paused after {FAIL_LIMIT} consecutive fails."
                                 " /kill off to resume.")
                _KILL_ = True
                _persist(r, plan); return r
            if plan.chain is None:
                r.error = "no common chain"
                await self._post(r, S_FAILED,
                                 f"❌ No common transfer chain between {cex.pretty(plan.buy_eid)}"
                                 f" and {cex.pretty(plan.sell_eid)} for {plan.base}")
                _CONSECUTIVE_FAILS += 1
                _persist(r, plan); return r

            fee = plan.fees or {}
            header = (
                f"⚡ <b>{plan.base}</b>  ·  <b>${plan.notional_usd:,.0f}</b> "
                f"(~{plan.qty:,.4f} {plan.base})\n"
                f"   Buy {cex.pretty(plan.buy_eid)} <code>{plan.buy_sym}</code> "
                f"→ Sell {cex.pretty(plan.sell_eid)} <code>{plan.sell_sym}</code>\n"
                f"   Chain: <b>{plan.chain}</b> · ETA ~{plan.eta_min:.1f} min · "
                f"gross ${plan.expected_profit_usd:,.2f} − fees ${fee.get('total_usd', 0):,.2f} = "
                f"net <b>${plan.net_profit_usd:,.2f}</b>"
            )
            if self.mode == "dry":
                header = "🧪 <i>[DRY-RUN]</i> " + header
            await self._post(r, S_PLANNING, header)

            # ─── BUY leg ─────────────────────────────────────────────
            if self.mode == "dry":
                await self._post(r, S_PLACING_BUY,
                                 f"1/6 🧪 WOULD place IOC BUY {plan.qty:.6f} on "
                                 f"{cex.pretty(plan.buy_eid)} @ {plan.buy_limit:g}")
                r.buy_order = {"dry": True, "qty": plan.qty, "price": plan.buy_limit}
            else:
                if not keys.has_keys(plan.buy_eid):
                    r.error = f"no api keys for {plan.buy_eid}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                inst_buy = cex._get(plan.buy_eid)
                try:
                    order = await inst_buy.create_order(
                        plan.buy_sym, "limit", "buy", plan.qty, plan.buy_limit,
                        {"timeInForce": "IOC"},
                    )
                except Exception as e:
                    r.error = f"buy order rejected: {e}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                r.buy_order = order
                filled_qty = float(order.get("filled") or 0)
                if filled_qty <= 0:
                    r.error = "buy order 0-fill"
                    await self._post(r, S_FAILED, f"❌ IOC BUY got no fill")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                plan.qty = filled_qty                                # honour partial
                await self._post(r, S_BUY_FILLED,
                                 f"2/6 ✅ filled {filled_qty:.6f} {plan.base}")

            if self.mode == "dry":
                await self._post(r, S_BUY_FILLED, f"2/6 🧪 dry-fill {plan.qty:.6f}")

            # ─── WITHDRAW ────────────────────────────────────────────
            dst_addrs = self.addresses.get(plan.sell_eid, {})
            dst = dst_addrs.get(plan.chain)
            if not dst and self.mode == "live":
                r.error = f"no deposit address for {plan.sell_eid}/{plan.chain}"
                await self._post(r, S_FAILED,
                                 f"❌ {r.error}. Add it to deposit_addresses.json")
                _CONSECUTIVE_FAILS += 1
                _persist(r, plan); return r

            if self.mode == "dry":
                await self._post(r, S_WITHDRAWING,
                                 f"3/6 🧪 WOULD withdraw {plan.qty:.6f} {plan.base} "
                                 f"→ {plan.sell_eid} on {plan.chain}")
                await self._post(r, S_CONFIRMING,
                                 f"4/6 🧪 skipping ~{plan.eta_min:.1f} min confirmations")
                await self._post(r, S_DEPOSITED, "5/6 🧪 dry-deposit credited")
            else:
                inst_buy = cex._get(plan.buy_eid)
                try:
                    wd = await inst_buy.withdraw(
                        plan.base, plan.qty, dst["address"],
                        tag=dst.get("tag"),
                        params={"network": plan.chain.upper()},
                    )
                except Exception as e:
                    r.error = f"withdraw rejected: {e}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                r.withdraw = wd
                tx = wd.get("id") or wd.get("txid") or ""
                await self._post(r, S_WITHDRAWING,
                                 f"3/6 🚀 withdraw sent · tx <code>{tx[:16]}…</code>")

                # ─── poll destination for deposit ────────────────────
                deadline = time.time() + DEPOSIT_TIMEOUT_MIN * 60
                inst_sell = cex._get(plan.sell_eid)
                credited = False
                while time.time() < deadline:
                    try:
                        deps = await inst_sell.fetch_deposits(plan.base, limit=10)
                    except Exception:
                        deps = []
                    for d in deps or []:
                        if d.get("status") in ("ok", "completed") and \
                                float(d.get("amount") or 0) >= plan.qty * 0.98:
                            credited = True
                            break
                    if credited:
                        break
                    remaining = int(deadline - time.time())
                    await self._post(r, S_CONFIRMING,
                                     f"4/6 ⏳ waiting deposit… ({remaining}s left)")
                    await asyncio.sleep(DEPOSIT_POLL_SEC)
                if not credited:
                    r.error = "deposit timeout"
                    await self._post(r, S_FAILED,
                                     f"❌ deposit didn't credit within "
                                     f"{DEPOSIT_TIMEOUT_MIN} min")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                await self._post(r, S_DEPOSITED, "5/6 ✅ deposit credited")

            # ─── SELL leg ────────────────────────────────────────────
            if self.mode == "dry":
                await self._post(r, S_PLACING_SELL,
                                 f"6/6 🧪 WOULD place IOC SELL {plan.qty:.6f} on "
                                 f"{cex.pretty(plan.sell_eid)} @ {plan.sell_limit:g}")
                r.sell_order = {"dry": True}
                r.net_pnl_usd = plan.net_profit_usd
            else:
                inst_sell = cex._get(plan.sell_eid)
                try:
                    sell = await inst_sell.create_order(
                        plan.sell_sym, "limit", "sell", plan.qty, plan.sell_limit,
                        {"timeInForce": "IOC"},
                    )
                except Exception as e:
                    r.error = f"sell rejected: {e}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                r.sell_order = sell
                # approximate realised PnL
                sell_avg = float(sell.get("average") or plan.sell_limit)
                buy_avg = float((r.buy_order or {}).get("average") or plan.buy_limit)
                realised = (sell_avg - buy_avg) * float(sell.get("filled") or plan.qty)
                r.net_pnl_usd = realised - (plan.fees or {}).get("total_usd", 0)
                await self._post(r, S_SELL_FILLED,
                                 f"6/6 ✅ sold · realised ${realised:,.2f}")

            r.finished_ts = time.time()
            await self._post(r, S_DONE,
                             f"✅ <b>DONE</b> — net PnL <b>${r.net_pnl_usd or 0:,.2f}</b>")
            _CONSECUTIVE_FAILS = 0                                   # reset on success
            _persist(r, plan)
            return r

        except Exception as e:
            r.error = str(e)
            log.exception("executor crashed for %s", plan.trade_id)
            await self._post(r, S_FAILED, f"❌ crashed: {str(e)[:120]}")
            _CONSECUTIVE_FAILS += 1
            _persist(r, plan)
            return r


_EX: Executor | None = None


def instance() -> Executor:
    global _EX
    if _EX is None:
        _EX = Executor(mode=os.getenv("EXEC_MODE", "dry"))
    return _EX
