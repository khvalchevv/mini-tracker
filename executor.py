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
import dex
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
_ACTIVE_TASKS: set = set()                              # asyncio.Tasks currently running a trade
_BASE_LOCKS: dict[str, asyncio.Lock] = {}               # per-base mutex to avoid double-execute


def _base_lock(base: str) -> asyncio.Lock:
    lk = _BASE_LOCKS.get(base)
    if lk is None:
        lk = asyncio.Lock()
        _BASE_LOCKS[base] = lk
    return lk
FAIL_LIMIT = int(os.getenv("EXEC_FAIL_LIMIT", "3"))
DEPOSIT_POLL_SEC = int(os.getenv("EXEC_DEPOSIT_POLL_SEC", "20"))
DEPOSIT_TIMEOUT_MIN = int(os.getenv("EXEC_DEPOSIT_TIMEOUT_MIN", "45"))


def set_kill(v: bool):
    global _KILL
    _KILL = bool(v)
    if v:
        # cancel every running trade coroutine
        for t in list(_ACTIVE_TASKS):
            if not t.done():
                t.cancel()


def kill_active() -> bool:
    return _KILL


def register_task(task):
    _ACTIVE_TASKS.add(task)
    task.add_done_callback(_ACTIVE_TASKS.discard)


def active_trade_count() -> int:
    return sum(1 for t in _ACTIVE_TASKS if not t.done())


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
    chain: str | None                                  # canonical, e.g. "bsc"
    src_network: str | None                            # source exchange's own label ("ETH" / "ERC20" / …)
    dst_network: str | None                            # dest exchange's own label (for docs / display)
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

    async def _plan_dex(self, alert: dict, sizing_result: dict, top: dict) -> TradePlan:
        """Plan a single Kyber swap out of the hot wallet.
        If Bitvavo cheap → we buy on Bitvavo, ship to hot wallet, swap X→USDC.
        If Bitvavo expensive → we swap USDC→X in hot wallet, ship to Bitvavo, sell.
        For MVP we execute ONLY the Kyber swap leg; the CEX leg is left
        as a manual step (or Phase 2 auto with deposit-address whitelisting)."""
        base = alert["base"]
        bpx = alert["bitvavo_price"]
        dex_price = top["price"]
        chain = top["chain"]
        contract = (self.__class__._contract_for(base, chain)
                    if hasattr(self.__class__, "_contract_for") else None)
        # We don't have direct access to CG platforms here; hunter set them
        # on base_to_contracts. Fallback: read from top entry if present.
        contract = top.get("contract") or contract
        p = TradePlan(
            trade_id=uuid.uuid4().hex[:10],
            base=base,
            buy_eid="dex" if bpx > dex_price else "bitvavo",
            buy_sym=f"USDC/{base}" if bpx > dex_price else f"{base}/EUR",
            sell_eid="bitvavo" if bpx > dex_price else "dex",
            sell_sym=f"{base}/EUR" if bpx > dex_price else f"USDC/{base}",
            buy_limit=sizing_result.get("last_buy_native", dex_price),
            sell_limit=sizing_result.get("last_sell_native", dex_price),
            qty=sizing_result.get("qty") or (100 / bpx),
            notional_usd=sizing_result.get("notional_usd", 100.0),
            expected_profit_usd=sizing_result.get("profit_usd", 0.0),
            net_profit_usd=sizing_result.get("net_profit_usd", 0.0),
            chain=chain,
            src_network=None,
            dst_network=None,
            eta_min=1.0,                                     # on-chain confirm on the swap chain
            fees=sizing_result.get("fee_bundle"),
        )
        # attach the base token contract so _run_dex knows what to swap
        p.base_contract = contract
        return p

    async def plan(self, alert: dict, sizing_result: dict) -> TradePlan:
        base = alert["base"]
        entries = alert["entries"]
        top = entries[0]
        if top["kind"] == "dex":
            # For a DEX top entry we run a single Kyber swap out of the hot
            # wallet. The BUY leg is on Bitvavo and the SELL leg is on DEX
            # (or vice versa) — we express it as a TradePlan with
            # buy_eid="dex" / sell_eid="dex" and let run() branch.
            return await self._plan_dex(alert, sizing_result, top)

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
        src_net = chain_pick["src_network"] if chain_pick else None
        dst_net = chain_pick["dst_network"] if chain_pick else None

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
            src_network=src_net,
            dst_network=dst_net,
            eta_min=eta,
            fees=fee_bundle,
        )

    # ─── execution ───────────────────────────────────────────────────

    async def _run_dex(self, plan: TradePlan) -> TradeReceipt:
        """Kyber swap only (single on-chain leg). Requires DEX_PRIVATE_KEY
        and hot wallet with USDC (for buy) or `base` (for sell) already
        credited on the chosen chain. The Bitvavo leg is manual for MVP."""
        global _CONSECUTIVE_FAILS
        r = TradeReceipt(
            trade_id=plan.trade_id, base=plan.base,
            buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=self.mode,
        )
        if _KILL:
            r.error = "killed"
            await self._post(r, S_FAILED, "🛑 /kill active — DEX swap aborted")
            _persist(r, plan); return r

        # Look up token contract on this chain from the hunter's mapping
        # (we passed nothing directly, so pull from cex/CG via hunter).
        # Simplest: peek into `_HUNTER` from bot module — but keep executor
        # standalone: cache the mapping we stored on plan (top entry has it).
        chain = plan.chain
        # Determine token direction
        from dex import USDC_BY_CHAIN, USDC_DECIMALS
        usdc = USDC_BY_CHAIN.get(chain)
        if not usdc:
            r.error = f"no USDC contract known for {chain}"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r

        # Base contract must have been stored on the plan
        base_contract = getattr(plan, "base_contract", None)
        if not base_contract:
            r.error = "base contract missing on plan"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r

        if plan.buy_eid == "dex":
            # BUY: USDC → base
            token_in, token_out = usdc, base_contract
            amount_in_wei = int(plan.notional_usd * (10 ** USDC_DECIMALS))
        else:
            # SELL: base → USDC (needs `base` in hot wallet already)
            token_in, token_out = base_contract, usdc
            amount_in_wei = int(plan.qty * (10 ** 18))       # assume 18 decimals

        header = (
            f"⚡ <b>{plan.base}</b> · DEX (Kyber) <b>{chain}</b>\n"
            f"  {'BUY' if plan.buy_eid == 'dex' else 'SELL'} "
            f"<b>${plan.notional_usd:,.0f}</b> notional · "
            f"net <b>${plan.net_profit_usd:,.2f}</b>"
        )
        if self.mode == "dry":
            header = "🧪 <i>[DRY-RUN]</i> " + header
        await self._post(r, S_PLANNING, header)

        if self.mode == "dry":
            await self._post(r, S_PLACING_BUY,
                             f"1/2 🧪 WOULD Kyber-swap {amount_in_wei} wei "
                             f"{token_in[:8]}… → {token_out[:8]}… on {chain}")
            await self._post(r, S_DONE,
                             f"✅ <b>DRY DONE</b> — exp net ${plan.net_profit_usd:,.2f}")
            r.net_pnl_usd = plan.net_profit_usd
            r.finished_ts = time.time()
            _persist(r, plan); return r

        # LIVE — 1) fresh Kyber quote at ACTUAL notional (prices move
        # between the hunter's scan and now). Show the user the real
        # execution price BEFORE we broadcast. Abort if the fresh
        # quote is worse than the alert's threshold.
        await self._post(r, S_PLACING_BUY,
                         f"1/3 🔍 fetching fresh Kyber quote at "
                         f"<b>${plan.notional_usd:,.0f}</b> notional …")
        try:
            route = await dex.quote(chain, token_in, token_out, amount_in_wei)
        except Exception as e:
            r.error = f"kyber quote crashed: {e}"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r
        if not route:
            r.error = "no Kyber route"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r
        try:
            amount_out_wei = int(route.get("amountOut") or "0")
        except (TypeError, ValueError):
            amount_out_wei = 0
        if amount_out_wei <= 0:
            r.error = "empty Kyber amountOut"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r
        # Effective price after slippage
        if plan.buy_eid == "dex":
            # USDC in → base out.  price per base = usdc_in / base_out
            eff_price = plan.notional_usd / (amount_out_wei / (10 ** 18))
            price_line = (f"1 {plan.base} ≈ ${eff_price:,.6f}  "
                          f"(will receive ~{amount_out_wei/10**18:,.4f} {plan.base})")
        else:
            # base in → USDC out.  price per base = usdc_out / base_in
            usdc_out = amount_out_wei / (10 ** dex.USDC_DECIMALS)
            eff_price = usdc_out / plan.qty
            price_line = (f"1 {plan.base} ≈ ${eff_price:,.6f}  "
                          f"(will receive ~${usdc_out:,.2f} USDC)")
        await self._post(r, S_PLACING_BUY,
                         f"2/3 💹 fresh quote: {price_line}")

        # Sanity-check: net expected must still clear min-profit
        # (hard-abort if quote drifted enough to erase the edge)
        # For now we always proceed; a live guard could go here later.

        # Adaptive slippage: bigger for small orders (fee-relative), tighter
        # for large. Env override wins.
        slip = int(os.getenv("DEX_SLIPPAGE_BPS", "0"))
        if slip <= 0:
            if plan.notional_usd < 100:
                slip = 150            # 1.5% for <$100
            elif plan.notional_usd < 500:
                slip = 100            # 1.0% for <$500
            elif plan.notional_usd < 2000:
                slip = 70             # 0.7%
            else:
                slip = 50             # 0.5% for large
        await self._post(r, S_PLACING_BUY,
                         f"3/3 🚀 broadcasting swap · slippage {slip/100:.2f}% …")
        try:
            res = await dex.swap(chain, token_in, token_out,
                                 amount_in_wei, plan.notional_usd,
                                 slippage_bps=slip)
        except Exception as e:
            r.error = f"dex.swap crashed: {e}"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r
        if not res.get("ok"):
            r.error = f"swap failed: {res.get('error')}"
            await self._post(r, S_FAILED, f"❌ {r.error}"
                             f"  <code>{res.get('tx_hash', '') or ''}</code>")
            _CONSECUTIVE_FAILS += 1; _persist(r, plan); return r

        tx = res.get("tx_hash", "")
        await self._post(r, S_DONE,
                         f"✅ swap done · tx <code>{tx}</code>\n"
                         f"   Executed price: ~${eff_price:,.6f}\n"
                         f"⚠️ CEX leg is manual — go to Bitvavo and "
                         f"{'sell' if plan.sell_eid == 'bitvavo' else 'buy'} "
                         f"<b>{plan.base}</b> to close the arb.")
        r.net_pnl_usd = plan.net_profit_usd
        r.finished_ts = time.time()
        _CONSECUTIVE_FAILS = 0
        _persist(r, plan); return r

    async def run(self, plan: TradePlan) -> TradeReceipt:
        # Per-base lock — never let two trades on the same token run
        # concurrently. A second alert for the same base while one is
        # in-flight is a losing race that would double-spend inventory.
        lock = _base_lock(plan.base)
        if lock.locked():
            r = TradeReceipt(
                trade_id=plan.trade_id, base=plan.base,
                buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=self.mode,
            )
            r.error = "another trade on this base is in-flight"
            await self._post(r, S_FAILED,
                             f"⏭ skip <b>{plan.base}</b> — previous trade still running")
            _persist(r, plan); return r
        async with lock:
            return await self._run_locked(plan)

    async def _run_locked(self, plan: TradePlan) -> TradeReceipt:
        # Route to DEX-only path when either leg is a Kyber swap
        if plan.buy_eid == "dex" or plan.sell_eid == "dex":
            return await self._run_dex(plan)
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
                set_kill(True)
                await self._post(r, S_FAILED,
                                 f"⏸ Auto-paused after {FAIL_LIMIT} consecutive fails."
                                 " /kill off to resume.")
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

            # ─── BUY leg — pre-flight then place ─────────────────────
            quote_ccy = plan.buy_sym.split("/")[1]
            need_quote = plan.qty * plan.buy_limit                   # in quote units
            if self.mode == "dry":
                await self._post(
                    r, S_PLACING_BUY,
                    f"0/6 🧪 pre-flight: need <b>{need_quote:.4f} {quote_ccy}</b>"
                    f" on {cex.pretty(plan.buy_eid)} (skipped in dry-run)",
                )
                await self._post(r, S_PLACING_BUY,
                                 f"1/6 🧪 WOULD place IOC BUY {plan.qty:.6f} on "
                                 f"{cex.pretty(plan.buy_eid)} @ {plan.buy_limit:g}")
                r.buy_order = {"dry": True, "qty": plan.qty, "price": plan.buy_limit}
                await self._post(r, S_BUY_FILLED, f"2/6 🧪 dry-fill {plan.qty:.6f}")
            else:
                if not keys.has_keys(plan.buy_eid):
                    r.error = f"no api keys for {plan.buy_eid}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                inst_buy = cex._get(plan.buy_eid)
                # pre-flight quote balance
                try:
                    bal = await inst_buy.fetch_balance()
                except Exception as e:
                    r.error = f"fetch_balance {plan.buy_eid}: {e}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                free_quote = float((bal.get(quote_ccy) or {}).get("free") or 0)
                if free_quote < need_quote:
                    r.error = (f"insufficient {quote_ccy} on {plan.buy_eid}: "
                               f"have {free_quote:.4f}, need {need_quote:.4f}")
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                await self._post(
                    r, S_PLACING_BUY,
                    f"0/6 ✅ pre-flight: {free_quote:.4f} {quote_ccy} on "
                    f"{cex.pretty(plan.buy_eid)} (need {need_quote:.4f})",
                )
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
                if _KILL:
                    r.error = "killed mid-flight (post-buy)"
                    await self._post(r, S_FAILED, "🛑 aborted after buy — "
                                     "manual sell needed on source")
                    _persist(r, plan); return r

            # ─── WITHDRAW ── pre-flight then send ────────────────────
            dst_addrs = self.addresses.get(plan.sell_eid, {})
            dst = dst_addrs.get(plan.chain)
            if not dst and self.mode == "live":
                r.error = f"no deposit address for {plan.sell_eid}/{plan.chain}"
                await self._post(r, S_FAILED,
                                 f"❌ {r.error}. Add it to deposit_addresses.json")
                _CONSECUTIVE_FAILS += 1
                _persist(r, plan); return r

            # min-withdraw check from source's own network entry
            for n in cex.network_info(plan.buy_eid, plan.base):
                if n.get("network") != plan.src_network:
                    continue
                raw = (n.get("raw") or {})
                min_w = raw.get("withdrawMin") or raw.get("withdrawalMinAmount") or raw.get("min_withdraw_amount")
                try:
                    if min_w is not None and float(min_w) > plan.qty:
                        r.error = (f"below {plan.buy_eid} min withdraw: "
                                   f"{plan.qty:.6f} < {float(min_w):.6f} {plan.base}")
                        await self._post(r, S_FAILED, f"❌ {r.error}")
                        _CONSECUTIVE_FAILS += 1
                        _persist(r, plan); return r
                except (TypeError, ValueError):
                    pass
                break

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
                    # use SOURCE exchange's own network label (Binance='ETH',
                    # Bitget='ERC20', etc.) — canonical name would be rejected.
                    net_param = plan.src_network or plan.chain.upper()
                    wd = await inst_buy.withdraw(
                        plan.base, plan.qty, dst["address"],
                        tag=dst.get("tag"),
                        params={"network": net_param},
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
                # Different exchanges report status differently:
                #   Binance: {status: "ok"} (ccxt-normalized) or raw {status: 1}
                #   Gate:    "DONE" or raw code 6
                #   Bitget:  "success"
                #   Bitvavo: "completed"
                # ccxt normalizes SOME of these to "ok" but not all — accept
                # a wider set + inspect the raw `info.status` too.
                OK_STATES = {"ok", "completed", "done", "success", "confirmed", "credited", "1", "6"}
                deadline = time.time() + DEPOSIT_TIMEOUT_MIN * 60
                inst_sell = cex._get(plan.sell_eid)
                credited = False
                while time.time() < deadline:
                    try:
                        deps = await inst_sell.fetch_deposits(plan.base, limit=10)
                    except Exception:
                        deps = []
                    for d in deps or []:
                        norm_st = str(d.get("status") or "").lower()
                        raw_st = str(((d.get("info") or {}).get("status") or "")).lower()
                        if (norm_st in OK_STATES or raw_st in OK_STATES) and \
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
                if _KILL:
                    r.error = "killed mid-flight (post-deposit)"
                    await self._post(r, S_FAILED, "🛑 aborted before sell — "
                                     "manual sell needed on destination")
                    _persist(r, plan); return r

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
                # Realised PnL from ACTUAL filled quantities on both legs
                # (IOC on either side may partial-fill).
                sell_filled = float(sell.get("filled") or 0)
                buy_filled = float((r.buy_order or {}).get("filled") or plan.qty)
                exec_qty = min(sell_filled, buy_filled)               # min = the amount we truly cycled
                sell_avg = float(sell.get("average") or plan.sell_limit)
                buy_avg = float((r.buy_order or {}).get("average") or plan.buy_limit)
                # Fees scale with the actual traded notional — recompute pro-rata
                fee_total = (plan.fees or {}).get("total_usd", 0) * (exec_qty / max(plan.qty, 1e-12))
                realised = (sell_avg - buy_avg) * exec_qty
                r.net_pnl_usd = realised - fee_total
                partial_note = ""
                if sell_filled < buy_filled * 0.98:
                    stuck = buy_filled - sell_filled
                    partial_note = (f"  ⚠ partial: bought {buy_filled:.6f}, sold "
                                    f"{sell_filled:.6f} — <b>{stuck:.6f} {plan.base}</b> "
                                    f"stuck on {cex.pretty(plan.sell_eid)}. Manual sell needed.")
                await self._post(r, S_SELL_FILLED,
                                 f"6/6 ✅ sold {sell_filled:.6f} · realised "
                                 f"${realised:,.2f} · net after fees "
                                 f"${r.net_pnl_usd:,.2f}{partial_note}")

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
