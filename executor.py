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

import blacklist
import cex
import chains
import dex
import fees
import keys

BLACKLIST_LOSS_HOURS = float(os.getenv("BLACKLIST_LOSS_HOURS", "1.0"))


def _maybe_blacklist_on_loss(r) -> None:
    """If the trade netted a loss, cool the base down for N hours so we
    don't spam alerts / auto-runs on the same losing setup. User can
    remove via TG button."""
    try:
        base = getattr(r, "base", None)
        pnl = getattr(r, "net_pnl_usd", None)
        if not base or pnl is None:
            return
        if pnl < 0:
            reason = f"loss ${pnl:.2f} on {time.strftime('%H:%M %d-%m')}"
            blacklist.ban_base_for(base, hours=BLACKLIST_LOSS_HOURS,
                                     reason=reason)
    except Exception as e:
        log.debug("_maybe_blacklist_on_loss err: %s", e)

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
DEPOSIT_POLL_SEC = int(os.getenv("EXEC_DEPOSIT_POLL_SEC", "5"))
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


def _final_report(plan, sess, receipt, sell_filled: float, exec_qty: float,
                  buy_avg: float, sell_avg: float, realised_usd: float,
                  fee_total_usd: float, stuck_note: str,
                  is_dry: bool = False) -> str:
    """Compact end-of-trade summary: what actually happened + expectation + time."""
    duration = 0.0
    if receipt.started_ts and receipt.finished_ts:
        duration = receipt.finished_ts - receipt.started_ts
    net = (receipt.net_pnl_usd if receipt.net_pnl_usd is not None
           else (realised_usd - fee_total_usd))
    exp_net = plan.net_profit_usd or 0.0
    delta = net - exp_net
    delta_str = f"  <i>(vs очік. ${exp_net:+,.2f}, різниця ${delta:+,.2f})</i>"
    tag = "🧪 <b>ТЕСТ · Підсумок</b>" if is_dry else "✅ <b>Підсумок прогону</b>"
    lines = [
        f"{tag}  ·  {plan.base}  ·  {int(duration)}с",
        f"чистий: <b>${net:+,.2f}</b>{delta_str}",
        f"комісії: ${fee_total_usd:,.2f}   ·   продано {sell_filled:.6g} {plan.base}",
    ]
    if stuck_note:
        lines.append(stuck_note)
    return "\n".join(lines)


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
class InteractiveSession:
    """State carrier for step-by-step user-approved trades. Each user
    approval advances one phase; bot never advances without a tap."""
    plan: "TradePlan"
    receipt: "TradeReceipt"
    phase: str = "AWAIT_APPROVE_BUY"                     # next action user must approve
    filled_qty: float = 0.0                              # after buy (in base units)
    filled_qty_wei: int = 0                              # after on-chain credit to hot wallet
    withdraw_tx: str | None = None
    forward_tx: str | None = None                        # HW → dest tx hash
    hw_arrived_ts: float | None = None                   # when HW got tokens
    forward_ts: float | None = None                      # when SEND broadcast
    deposit_credited: bool = False
    # Ground-truth USD snapshots for real PnL calc (balances before BUY
    # and after SELL). Includes all exchanges + HW. Real net = after - before.
    usd_snapshot_before: float | None = None
    usd_snapshot_after: float | None = None


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

    def _potential_line(self, plan: "TradePlan") -> str:
        """One-line header showing potential profit + spread%
        for the picked size — prepended to phase messages."""
        if not plan:
            return ""
        pct = 0.0
        if plan.buy_limit and plan.qty:
            gross_usd = plan.expected_profit_usd or 0
            if plan.notional_usd:
                pct = gross_usd / plan.notional_usd * 100
        return (f"<i>[{plan.base} · обʼєм ${plan.notional_usd:,.0f} · "
                f"потенціал ${plan.net_profit_usd:,.2f} чист. / "
                f"${plan.expected_profit_usd:,.2f} гросс · {pct:.2f}%]</i>\n")

    # ─── planning ────────────────────────────────────────────────────

    def precheck_plan(self, plan: TradePlan) -> list[str]:
        """Static feasibility checks that don't need any API call. Returns
        a list of blocker reasons — empty list means good to go (subject
        to runtime balance/quote checks during actual execution)."""
        reasons: list[str] = []
        # (1) Chain existence
        if plan.chain is None:
            reasons.append(f"немає спільного чейну між "
                           f"{cex.pretty(plan.buy_eid)} і "
                           f"{cex.pretty(plan.sell_eid)} для {plan.base}")
            return reasons                                       # rest doesn't apply
        # (1b) Hop-only policy — refuse chains where we have no wallet
        supported_hop_chains = set(dex.KYBER_CHAIN.keys()) | {"solana"}
        if plan.chain not in supported_hop_chains:
            reasons.append(f"hop-only: немає hot wallet на {plan.chain} "
                           f"(підтримуємо EVM + Solana)")
            return reasons
        # (2) Source withdraw open for the chosen chain
        src_ok = False
        for n in cex.network_info(plan.buy_eid, plan.base):
            from chains import canonical
            if canonical(n["network"]) == plan.chain:
                src_ok = bool(n.get("withdraw"))
                break
        if not src_ok:
            reasons.append(f"{cex.pretty(plan.buy_eid)} закритий ВИВІД "
                           f"{plan.base} на {plan.chain}")
        # (3) Destination deposit open
        dst_ok = False
        for n in cex.network_info(plan.sell_eid, plan.base):
            from chains import canonical
            if canonical(n["network"]) == plan.chain:
                dst_ok = bool(n.get("deposit"))
                break
        if not dst_ok:
            reasons.append(f"{cex.pretty(plan.sell_eid)} закритий ДЕПОЗИТ "
                           f"{plan.base} на {plan.chain}")
        # (4) Live mode needs API keys. Deposit address is NOT checked
        # here — runtime tries fetch_deposit_address via ccxt first, with
        # deposit_addresses.json only as a fallback. Failing here would
        # reject plans that the API can actually resolve at execute time.
        if self.mode == "live":
            if not keys.has_keys(plan.buy_eid):
                reasons.append(f"немає API-ключів {cex.pretty(plan.buy_eid)}")
            if not keys.has_keys(plan.sell_eid):
                reasons.append(f"немає API-ключів {cex.pretty(plan.sell_eid)}")
        # (5) Net profit still positive (fees might have eaten the spread)
        if plan.net_profit_usd <= 0:
            reasons.append(f"чистий профіт ${plan.net_profit_usd:,.2f} ≤ 0 після комісій")
        # (6) Qty above withdraw minimum
        for n in cex.network_info(plan.buy_eid, plan.base):
            if n.get("network") != plan.src_network:
                continue
            raw = n.get("raw") or {}
            min_w = raw.get("withdrawMin") or raw.get("withdrawalMinAmount")
            try:
                if min_w is not None and float(min_w) > plan.qty:
                    reasons.append(f"обʼєм {plan.qty:.6f} нижче мін-виводу "
                                   f"{cex.pretty(plan.buy_eid)} "
                                   f"{float(min_w):.6f} на {plan.src_network}")
            except (TypeError, ValueError):
                pass
            break
        return reasons

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

        # Chain policy: pick fastest common network. If empty for either
        # side we FIRST refresh via a live per-token WD/DEP probe (avoids
        # the "empty cached networks -> pick returns None" trap where
        # BUY fires but WD has nowhere to go). If still empty, abort —
        # DON'T create a plan that will BUY into a dead-end.
        buy_nets = cex.network_info(buy_eid, base)
        sell_nets = cex.network_info(sell_eid, base)
        if not buy_nets or not sell_nets:
            log.info("plan %s %s->%s: empty cached nets (buy=%d, sell=%d) — live refresh",
                     base, buy_eid, sell_eid, len(buy_nets or []), len(sell_nets or []))
            try:
                if not buy_nets:
                    buy_nets = await cex.fetch_wd_dep_live(buy_eid, base, None) or []
                if not sell_nets:
                    sell_nets = await cex.fetch_wd_dep_live(sell_eid, base, None) or []
            except Exception as _e:
                log.debug("live nets refresh: %s", _e)
        chain_pick = chains.pick_transfer_chain(buy_nets, sell_nets)
        if not chain_pick:
            raise RuntimeError(
                f"no common transfer chain for {base}: "
                f"{buy_eid} WD={[n.get('network') for n in (buy_nets or [])]} "
                f"vs {sell_eid} DEP={[n.get('network') for n in (sell_nets or [])]}")
        chain = chain_pick["chain"]
        eta = chain_pick["eta_min"]
        src_net = chain_pick["src_network"]
        dst_net = chain_pick["dst_network"]

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

        # Base decimals — read from contract (was hardcoded 18 which
        # under/over-sends by 10^12 for USDC/USDT (6) or 10^10 for
        # 8-dec tokens like WBTC).
        base_dec = 18
        try:
            from web3 import Web3 as _W3
            _w3 = dex._get_w3(chain)
            if _w3 and base_contract.lower() != dex.NATIVE.lower():
                _abi = [{"inputs": [], "name": "decimals",
                          "outputs": [{"name": "", "type": "uint8"}],
                          "stateMutability": "view", "type": "function"}]
                _c = _w3.eth.contract(
                    address=_W3.to_checksum_address(base_contract),
                    abi=_abi)
                base_dec = int(_c.functions.decimals().call())
        except Exception as _e:
            log.debug("_run_dex decimals %s: %s", base_contract[:12], _e)

        if plan.buy_eid == "dex":
            # BUY: USDC → base
            token_in, token_out = usdc, base_contract
            amount_in_wei = int(plan.notional_usd * (10 ** USDC_DECIMALS))
        else:
            # SELL: base → USDC (needs `base` in hot wallet already)
            token_in, token_out = base_contract, usdc
            amount_in_wei = int(plan.qty * (10 ** base_dec))

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
        # Effective price after slippage — use real base decimals
        if plan.buy_eid == "dex":
            # USDC in → base out.  price per base = usdc_in / base_out
            base_out_h = amount_out_wei / (10 ** base_dec)
            eff_price = plan.notional_usd / base_out_h if base_out_h else 0
            price_line = (f"1 {plan.base} ≈ ${eff_price:,.6f}  "
                          f"(will receive ~{base_out_h:,.4f} {plan.base})")
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
        # for large. Env override wins. RETRY LADDER: on revert (frontrun,
        # pool state moved) we retry with wider slippage 2 more times.
        base_slip = int(os.getenv("DEX_SLIPPAGE_BPS", "0"))
        if base_slip <= 0:
            if plan.notional_usd < 100:
                base_slip = 200            # 2.0% for <$100 (illiquid)
            elif plan.notional_usd < 500:
                base_slip = 150            # 1.5% for <$500
            elif plan.notional_usd < 2000:
                base_slip = 120            # 1.2% for <$2k (was 0.7 — too tight)
            else:
                base_slip = 80             # 0.8% for large

        res = None
        last_err = None
        # LADDER: size × slippage combos. On revert we scale DOWN size AND
        # widen slippage — smaller trades have less price impact, so a
        # 40% smaller trade at wider slippage often clears while the full
        # size at any slippage keeps reverting on thin pools.
        # (size_multiplier, slip_bps)
        ladder = [
            (1.0,  base_slip),                # attempt 1: full size, base slip
            (0.5,  int(base_slip * 1.5)),     # attempt 2: half size, +50% slip
            (0.25, int(base_slip * 2.5)),     # attempt 3: quarter size, +150% slip
        ]
        _orig_amount = amount_in_wei
        _orig_notional = plan.notional_usd
        # CLAMP TO ACTUAL WALLET BALANCE — the ladder used to fire
        # $1000/$500/$250 at a wallet holding $188, so every rung
        # reverted with TRANSFER_FROM_FAILED and burned ~87k gas each.
        # Cap the top rung at 98% of what we actually hold.
        if token_in.lower() != dex.NATIVE.lower():
            try:
                _have = await dex.wallet_token_balance(chain, token_in)
                # `if _have and ...` skipped BOTH None (RPC error) and 0
                # (empty wallet) — the two cases that most need clamping.
                if _have is None:
                    r.error = ("не можу прочитати баланс гаманця (RPC) — "
                               "свап скасовано, щоб не палити газ наосліп")
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                # Only clamp on a MATERIAL shortfall. plan amounts come
                # from float math, so `have` can be a few hundred wei
                # under `wanted` on an identical balance — clamping that
                # shaved a pointless 2% off every swap ($452 → $443).
                if _have < int(_orig_amount * 0.995):
                    _ratio = _have / _orig_amount if _orig_amount else 0
                    _capped = int(_have * 0.98)          # 2% headroom
                    log.warning(
                        "_run_dex %s CLAMP to wallet balance: "
                        "wanted %d raw, have %d (%.1f%%) → using %d",
                        plan.base, _orig_amount, _have, _ratio * 100, _capped)
                    await self._post(
                        r, S_PLACING_BUY,
                        f"⚠️ гаманець має лише {_ratio * 100:.0f}% потрібного "
                        f"обʼєму — зменшую до "
                        f"${_orig_notional * _ratio * 0.98:,.0f}")
                    _orig_amount = _capped
                    _orig_notional = _orig_notional * _ratio * 0.98
                    # Write the clamp back into the plan. Only the LADDER
                    # used to do this (and only when sz_mult < 1.0), so a
                    # wallet-clamped swap that succeeded on attempt 1 left
                    # plan.qty/notional at the planned figures. Everything
                    # downstream reads them: the sell leg sizes itself,
                    # the ledger books cost basis, stats compute PnL. On
                    # 2026-09-10 08:12 a $233 MOVR swap was booked as
                    # 1,662 MOVR at $1,242.63 because of this.
                    plan.notional_usd = _orig_notional
                    plan.qty = float(plan.qty or 0) * _ratio * 0.98
                    if plan.net_profit_usd:
                        plan.net_profit_usd *= _ratio * 0.98
                    if _orig_notional < 20:
                        r.error = (f"гаманець майже порожній: "
                                   f"${_orig_notional:.2f} < $20 мін — "
                                   f"потрібен ребаланс HW")
                        await self._post(r, S_FAILED, f"❌ {r.error}")
                        _CONSECUTIVE_FAILS += 1
                        _persist(r, plan); return r
            except Exception as _e:
                log.debug("_run_dex balance clamp %s: %s", plan.base, _e)
        # Pre-clamp to the per-tx cap so we never waste an attempt getting
        # refused by our own limit (that's how the FLOCK position got
        # stranded). Shrink amount and notional together.
        try:
            _tx_cap = float(os.getenv("DEX_MAX_TX_USD", "1000"))
            if _orig_notional > _tx_cap > 0:
                _shrink = (_tx_cap * 0.98) / _orig_notional
                log.warning("_run_dex %s: notional $%.0f over tx cap $%.0f "
                            "→ clamping to $%.0f",
                            plan.base, _orig_notional, _tx_cap,
                            _orig_notional * _shrink)
                await self._post(
                    r, S_PLACING_BUY,
                    f"⚠️ обʼєм ${_orig_notional:,.0f} > ліміт ${_tx_cap:,.0f} "
                    f"— зменшую до ${_orig_notional * _shrink:,.0f}")
                _orig_amount = int(_orig_amount * _shrink)
                _orig_notional *= _shrink
        except Exception as _e:
            log.debug("tx-cap pre-clamp %s: %s", plan.base, _e)
        for attempt, (sz_mult, slip) in enumerate(ladder, 1):
            _amt = int(_orig_amount * sz_mult)
            _nom = _orig_notional * sz_mult
            await self._post(r, S_PLACING_BUY,
                             f"3/3 🚀 broadcasting swap · attempt {attempt}/3 "
                             f"· size ${_nom:,.0f} · slip {slip/100:.2f}% …")
            try:
                # Attempt 1: pass the fresh route we just fetched to skip
                # dex.swap's internal re-quote (saves ~1-8s + closes the
                # transient-failure gap that killed AIOZ/SYRUP earlier).
                # Attempts 2-3: size scales down 0.5×/0.25×, so route's
                # amountIn no longer matches — swap must re-quote.
                _preset = route if attempt == 1 else None
                res = await dex.swap(chain, token_in, token_out,
                                     _amt, _nom, slippage_bps=slip,
                                     preset_route=_preset)
            except Exception as e:
                last_err = f"dex.swap crashed: {e}"
                await self._post(r, S_PLACING_BUY,
                                  f"⚠️ attempt {attempt} crashed: {str(e)[:80]}")
                continue
            if res and res.get("ok"):
                # Update executed size for downstream (SELL leg / receipt)
                if sz_mult < 1.0:
                    plan.notional_usd = _nom
                    plan.qty = plan.qty * sz_mult
                    if plan.net_profit_usd:
                        plan.net_profit_usd *= sz_mult
                    log.warning("dex swap %s: filled at %.0f%% size = $%.0f",
                                plan.base, sz_mult * 100, _nom)
                    # A reduced-size fill leaves the rest of the position
                    # on the wallet. Declaring DONE here is how 6347 ACX
                    # (~$263) got stranded. Keep selling the remainder
                    # until it's below dust.
                    res = await self._drain_remainder(
                        r, plan, chain, token_in, token_out, slip)
                break
            last_err = f"swap failed: {res.get('error') if res else '?'}"
            err_str = str(res.get("error", "")).lower() if res else ""
            # "tx cap exceeded" and "insufficient balance" ARE recoverable
            # by shrinking — that's exactly what the next rung does.
            # Treating them as fatal stranded 15668 FLOCK (~$914) on Base:
            # the plan came in over the $1000 cap, attempt 1 was refused,
            # and the ladder quit without ever trying $500 or $250.
            _retryable = ("reverted", "slippage", "timed out", "call failed",
                          "cap", "insufficient balance", "exceeded")
            if not any(k in err_str for k in _retryable):
                break                                    # non-recoverable
            await self._post(r, S_PLACING_BUY,
                              f"⚠️ attempt {attempt} reverted "
                              f"({(res.get('tx_hash') or '')[:12]}…) "
                              f"— retry smaller size + wider slip")
            await asyncio.sleep(3)                      # let mempool settle
        if not res or not res.get("ok"):
            r.error = last_err or "swap failed: ?"
            await self._post(r, S_FAILED, f"❌ {r.error}"
                             f"  <code>{(res or {}).get('tx_hash', '')}</code>")
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

    async def _drain_remainder(self, r, plan, chain, token_in, token_out,
                                 slip: int, max_rounds: int = 4) -> dict:
        """Keep swapping `token_in` until the wallet holds only dust.

        The size ladder exists to find a size the pool will accept, but
        a reduced-size success is a PARTIAL exit — the rest of the
        position stays on the wallet. Nothing used to come back for it
        (6347 ACX ≈ $263 sat stranded that way). A leg is only finished
        when the inventory is gone.

        Returns the last swap result so the caller's success/failure
        handling is unchanged.
        """
        last = {"ok": True}
        dust_usd = float(os.getenv("DRAIN_DUST_USD", "15"))
        for rnd in range(max_rounds):
            try:
                left_wei = await dex.wallet_token_balance(chain, token_in)
            except Exception as e:
                log.debug("drain balance %s: %s", plan.base, e)
                return last
            if not left_wei or left_wei <= 0:
                return last
            # Value what's left; stop once it's not worth the gas
            try:
                q = await dex.usd_price(chain, token_in, usd_notional=100.0)
                px = (q or {}).get("price_sell_usd") or 0
            except Exception:
                px = 0
            dec = 18
            try:
                dec = dex._TOKEN_DECIMALS.get((chain, token_in.lower())) or 18
            except Exception:
                pass
            left_qty = left_wei / (10 ** dec)
            left_usd = left_qty * px
            if left_usd < dust_usd:
                if left_usd > 0:
                    log.info("drain %s: %.4f left (~$%.2f) — below $%.0f "
                             "dust, stopping", plan.base, left_qty,
                             left_usd, dust_usd)
                return last
            log.warning("drain %s round %d: %.4f left (~$%.2f) — selling",
                        plan.base, rnd + 1, left_qty, left_usd)
            await self._post(
                r, S_PLACING_BUY,
                f"♻️ дозачистка {rnd + 1}: лишилось {left_qty:,.4f} "
                f"{plan.base} (~${left_usd:,.2f}) — продаю")
            _cap = os.getenv("DEX_MAX_TX_USD", "1000")
            try:
                if left_usd > float(_cap):
                    os.environ["DEX_MAX_TX_USD"] = f"{left_usd * 1.1:.0f}"
                res2 = await dex.swap(chain, token_in, token_out,
                                        int(left_wei), left_usd,
                                        slippage_bps=min(slip * 2, 500))
            except Exception as e:
                log.warning("drain swap %s: %s", plan.base, e)
                return last
            finally:
                os.environ["DEX_MAX_TX_USD"] = _cap
            if res2 and res2.get("ok"):
                last = res2
                plan.notional_usd = (plan.notional_usd or 0) + left_usd
                await self._post(r, S_PLACING_BUY,
                                  f"✅ дозачистка: продано ~${left_usd:,.2f}")
                continue
            # Couldn't clear it — record so /stuck surfaces it
            log.warning("drain %s failed: %s", plan.base,
                        (res2 or {}).get("error"))
            try:
                import stuck as _stuck
                _stuck.add(plan.base, chain, token_in, qty=left_qty,
                             paid_usd=left_usd,
                             err=f"drain failed: {(res2 or {}).get('error')}")
            except Exception:
                pass
            await self._post(
                r, S_PLACING_BUY,
                f"⚠️ залишок {left_qty:,.4f} {plan.base} "
                f"(~${left_usd:,.2f}) не продався — записано в /stuck")
            return last
        return last

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
            r = await self._run_locked(plan)
        # Auto-blacklist for N hours on any losing trade (net_pnl < 0).
        # User can remove via /blacklist or the button on the loss alert.
        _maybe_blacklist_on_loss(r)
        return r

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
                    order = await cex.place_order(
                        inst_buy, plan.buy_sym, "limit", "buy",
                        plan.qty, plan.buy_limit, {"timeInForce": "FOK"},
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
                # Deduct fee-in-base + cross-check free balance so the
                # withdraw amount matches what's actually available.
                fee_in_base = 0.0
                fe = order.get("fee") or {}
                if isinstance(fe, dict) and (fe.get("currency") or "").upper() == plan.base.upper():
                    try: fee_in_base = float(fe.get("cost") or 0)
                    except (TypeError, ValueError): pass
                for f2 in (order.get("fees") or []):
                    if isinstance(f2, dict) and (f2.get("currency") or "").upper() == plan.base.upper():
                        try: fee_in_base += float(f2.get("cost") or 0)
                        except (TypeError, ValueError): pass
                try:
                    bal = await inst_buy.fetch_balance()
                    free_base = float((bal.get(plan.base) or {}).get("free") or 0)
                except Exception:
                    free_base = 0.0
                est_available = filled_qty - fee_in_base if fee_in_base > 0 else filled_qty * 0.998
                filled_qty = min(est_available, free_base) if free_base > 0 else est_available
                plan.qty = filled_qty                                # honour partial + fee
                await self._post(r, S_BUY_FILLED,
                                 f"2/6 ✅ filled {filled_qty:.6f} {plan.base} (after fee)")
                if _KILL:
                    r.error = "killed mid-flight (post-buy)"
                    await self._post(r, S_FAILED, "🛑 aborted after buy — "
                                     "manual sell needed on source")
                    _persist(r, plan); return r

            # ─── WITHDRAW ── pre-flight then send ────────────────────
            dst_addrs = self.addresses.get(plan.sell_eid, {})
            dst = dst_addrs.get(plan.chain)
            if not dst and self.mode == "live":
                # No file entry — try live API before failing.
                try:
                    creds_dst = keys.load_keys().get(plan.sell_eid) or {}
                    inst_dst = cex.get_private(plan.sell_eid, creds_dst)
                    dep = await cex.fetch_deposit_address_robust(
                        inst_dst, plan.base, plan.dst_network)
                    if dep and dep.get("address"):
                        dst = {"address": dep["address"], "tag": dep.get("tag")}
                except Exception as e:
                    log.debug("fetch_deposit_address fallback err: %s", e)
            if not dst and self.mode == "live":
                r.error = f"no deposit address for {plan.sell_eid}/{plan.chain}"
                await self._post(r, S_FAILED,
                                 f"❌ {r.error} (API теж не віддав)")
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
                    net_param = plan.src_network or plan.chain.upper()
                    wd = await cex.withdraw_robust(
                        inst_buy, plan.base, plan.qty, dst["address"],
                        dst.get("tag"), net_param,
                    )
                except Exception as e:
                    r.error = f"withdraw rejected: {type(e).__name__}: {str(e)[:200]}"
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    _CONSECUTIVE_FAILS += 1
                    _persist(r, plan); return r
                r.withdraw = wd
                tx = wd.get("id") or wd.get("txid") or ""
                await self._post(r, S_WITHDRAWING,
                                 f"3/6 🚀 withdraw sent · tx <code>{tx[:16]}…</code>")

                # ─── poll destination for deposit via fetch_balance ──
                # free balance updates faster than the "status: ok" label.
                deadline = time.time() + DEPOSIT_TIMEOUT_MIN * 60
                inst_sell = cex.get_private(plan.sell_eid,
                                             keys.load_keys().get(plan.sell_eid) or {})
                try:
                    b0 = await inst_sell.fetch_balance()
                    base0 = float((b0.get(plan.base) or {}).get("free") or 0)
                except Exception:
                    base0 = 0.0
                need = plan.qty * 0.90
                credited = False
                while time.time() < deadline:
                    try:
                        b = await inst_sell.fetch_balance()
                        cur = float((b.get(plan.base) or {}).get("free") or 0)
                        if cur - base0 >= need:
                            plan.qty = cur                       # actual credited
                            credited = True
                            break
                    except Exception:
                        pass
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
                    sell = await cex.place_order(
                        inst_sell, plan.sell_sym, "limit", "sell",
                        plan.qty, plan.sell_limit, {"timeInForce": "IOC"},
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


    # ────────────────────────────────────────────────────────────────
    # Step-by-step (interactive) phase methods — used by bot session
    # controller. Each phase does ONE thing and returns; user approves
    # via a TG button to advance to the next phase.
    # ────────────────────────────────────────────────────────────────

    async def phase_buy(self, sess: InteractiveSession) -> bool:
        """Place BUY leg. Returns True on success (sets sess.filled_qty)."""
        plan, r = sess.plan, sess.receipt
        if _KILL:
            await self._post(r, S_FAILED, "🛑 вбито перед купівлею")
            return False
        potential = self._potential_line(plan)
        if self.mode == "dry":
            await self._post(r, S_PLACING_BUY,
                             potential + f"1/3 🧪 <i>КУПИВ БИ</i> {plan.qty:.6f} "
                             f"{plan.base} на {cex.pretty(plan.buy_eid)} "
                             f"@ {plan.buy_limit:g}")
            sess.filled_qty = plan.qty
            await self._post(r, S_BUY_FILLED,
                             potential + f"2/3 🧪 тест-fill {plan.qty:.6f}")
            return True
        # LIVE — use the dedicated private instance (no race with hunter)
        creds = keys.load_keys().get(plan.buy_eid) or {}
        if not (creds.get("apiKey") and creds.get("secret")):
            r.error = f"немає API-ключів для {plan.buy_eid}"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        inst = cex.get_private(plan.buy_eid, creds)
        quote_ccy = plan.buy_sym.split("/")[1]
        need = plan.qty * plan.buy_limit
        try:
            bal = await inst.fetch_balance()
        except Exception as e:
            r.error = f"balance fetch: {e}"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        free = float((bal.get(quote_ccy) or {}).get("free") or 0)
        if free < need:
            # Scale down to what balance actually allows — as long as
            # the reduced trade STILL passes the net-profit floor.
            # Reserve 0.5% headroom for slippage/fees on the buy quote.
            usable = free * 0.995
            # `usable` is already in quote currency (~USD for USDT, or
            # EUR that we FX-adjust). Convert to USD for the notional check.
            fx = cex.get_fx_rate(quote_ccy) or 1.0
            usable_usd = usable * fx
            min_notional_usd = float(os.getenv("AUTO_MIN_NOTIONAL_USD", "10"))
            if usable_usd < min_notional_usd:
                r.error = (f"не вистачає {quote_ccy}: є {free:.4f} "
                           f"(~${usable_usd:.2f}), треба ${min_notional_usd:.0f}")
                await self._post(r, S_FAILED, f"❌ {r.error}"); return False
            new_qty = usable / plan.buy_limit
            scale = new_qty / plan.qty
            new_notional = usable_usd
            # Scale expected profit and fees proportionally
            new_gross = (plan.expected_profit_usd or 0) * scale
            new_fees = (plan.fees or {}).get("total_usd", 0) * scale
            new_net = new_gross - new_fees
            min_p = float(os.getenv("AUTOEXEC_MIN_PROFIT_USD",
                                    os.getenv("MIN_PROFIT_USD", "1")))
            if getattr(plan, "is_testrun", False):
                min_p = 0.0                                # testrun bypasses
            if new_net < min_p:
                r.error = (f"scale-down: net ${new_net:.2f} < ${min_p:.2f} "
                           f"({new_qty:.4f} {plan.base} @ ${new_notional:,.2f})")
                await self._post(r, S_FAILED, f"❌ {r.error}"); return False
            await self._post(r, S_PLACING_BUY,
                             f"⚙️ scale-down: qty {plan.qty:.4f}→{new_qty:.4f} "
                             f"(${new_notional:,.0f}), net ${new_net:.2f}")
            plan.qty = new_qty
            plan.notional_usd = new_notional
            plan.net_profit_usd = new_net
            need = new_qty * plan.buy_limit
        # SPEED-FILL FOK: widen the limit price so mild book moves
        # between alert-time and broadcast still fill.
        buffer_bps_ladder = [
            int(os.getenv("BUY_FOK_BUFFER_BPS_1", "30")),   # 0.30% attempt 1
            int(os.getenv("BUY_FOK_BUFFER_BPS_2", "80")),   # 0.80% attempt 2
        ]
        # LATENCY TRACING + FRESH BOOK: rebase FOK limit off the LIVE
        # best_ask (in native quote currency, e.g. EUR for Bitvavo), NOT
        # plan.buy_limit which for DEX-cycle plans is set in USD from the
        # Kyber quote — Bitvavo interprets that number as EUR → wildly
        # wrong limit price (was FOK-failing with "стакан не дав повного
        # обʼєму" because limit was ~15% too high in EUR terms).
        _phase_t0 = time.time()
        _alert_ts = getattr(plan, "alert_ts", None) or getattr(sess, "alert_ts", None)
        _lag = (_phase_t0 - _alert_ts) if _alert_ts else None
        _fresh_base_ask = None
        try:
            _ob = await inst.fetch_order_book(plan.buy_sym, 20)
            _asks = _ob.get("asks") or []
            if _asks:
                _fresh_base_ask = float(_asks[0][0])
                _limit_pxs = [_fresh_base_ask * (1 + b/10000)
                              for b in buffer_bps_ladder]
                _fills_at = []
                for _lp in _limit_pxs:
                    _c = sum(float(a[1]) for a in _asks if float(a[0]) <= _lp)
                    _fills_at.append((_lp, _c))
                log.warning(
                    "phase_buy %s START lag=%.2fs plan.buy_limit=%.6g "
                    "fresh_ask=%.6g need_qty=%.4f | book@buf: %s",
                    plan.base, _lag if _lag is not None else -1,
                    plan.buy_limit, _fresh_base_ask, plan.qty,
                    " | ".join(f"buf{i+1} px={pxq[0]:.6g}→{pxq[1]:.4g}"
                                 for i, pxq in enumerate(_fills_at)),
                )
        except Exception as _e:
            log.debug("phase_buy pre-fok book fetch %s: %s", plan.base, _e)
        # Base price for FOK: prefer FRESH ask from live book; fall back
        # to plan.buy_limit only if book fetch failed.
        base_px = _fresh_base_ask if _fresh_base_ask else plan.buy_limit

        # Bitvavo path: LIMIT-and-poll (book fills gradually over time,
        # accept any profitable partial fill). Other CEXes keep FOK ladder.
        _use_limit_wait = (plan.buy_eid == "bitvavo")

        order = None
        widened_limit = None
        last_err = None

        if _use_limit_wait:
            # LIMIT + poll strategy — sit in book, take whatever fills
            _limit_wait_sec = int(os.getenv("BV_BUY_LIMIT_WAIT_SEC", "300"))
            _poll_int = int(os.getenv("BV_BUY_POLL_SEC", "15"))
            _min_fill_usd = float(os.getenv("BV_BUY_MIN_FILL_USD", "50"))
            _limit_buf_bps = int(os.getenv("BV_BUY_LIMIT_BUFFER_BPS", "10"))
            widened_limit = base_px * (1 + _limit_buf_bps / 10000)
            _t_send = time.time()
            try:
                order = await cex.place_order(
                    inst, plan.buy_sym, "limit", "buy",
                    plan.qty, widened_limit, {},  # no TIF → sits as GTC
                )
            except Exception as e:
                r.error = f"біржа відхилила купівлю: {type(e).__name__}: {str(e)[:200]}"
                await self._post(r, S_FAILED, f"❌ {r.error}")
                return False
            _oid = order.get("id")
            log.warning(
                "LIMIT-WAIT %s @ %.6g qty=%.4f (~$%.0f) id=%s — "
                "polling up to %ds for fills",
                plan.base, widened_limit, plan.qty, plan.notional_usd,
                _oid, _limit_wait_sec,
            )
            _deadline = time.time() + _limit_wait_sec
            _last_reported = 0.0
            while time.time() < _deadline:
                await asyncio.sleep(_poll_int)
                try:
                    order = await inst.fetch_order(_oid, plan.buy_sym)
                except Exception as e:
                    log.debug("fetch_order %s err: %s", _oid, e)
                    continue
                _fil = float(order.get("filled") or 0)
                if _fil > _last_reported * 1.05 or (_fil > 0 and _last_reported == 0):
                    log.warning(
                        "LIMIT-WAIT %s fill=%.4f (%.0f%% of %.4f), "
                        "~$%.2f, %ds elapsed",
                        plan.base, _fil, _fil / plan.qty * 100,
                        plan.qty, _fil * widened_limit,
                        int(time.time() - _t_send),
                    )
                    _last_reported = _fil
                if order.get("status") in ("closed", "canceled"):
                    break
                # If ≥95% filled — done, no point waiting for last dust
                if _fil >= plan.qty * 0.95:
                    log.warning("LIMIT-WAIT %s ~fully filled (%.0f%%), exit",
                                plan.base, _fil / plan.qty * 100)
                    break
            # Cancel any remainder still in book. This MUST succeed and be
            # verified — a limit order left working keeps buying inventory
            # nobody is tracking. Previously the failure (Bitvavo needs
            # operatorId on cancel too) was swallowed at debug level and a
            # half-filled ACX order sat open for an hour.
            if order and order.get("status") not in ("closed", "canceled"):
                log.warning("LIMIT-WAIT %s timeout %ds — cancelling remainder",
                            plan.base, _limit_wait_sec)
                _c = await cex.cancel_order_robust(inst, _oid, plan.buy_sym)
                if not _c["verified"]:
                    # Loud: the book still holds a live order for this base
                    log.error("LIMIT-WAIT %s: cancel NOT verified (%s) — "
                              "order %s may still be filling",
                              plan.base, _c.get("error"), _oid)
                    await self._post(
                        r, S_PLACING_BUY,
                        f"🚨 <b>{plan.base}</b>: не вдалось скасувати "
                        f"залишок ліміт-ордера <code>{str(_oid)[:18]}…</code>"
                        f" — перевір Bitvavo вручну!")
                try:
                    order = await inst.fetch_order(_oid, plan.buy_sym)
                except Exception as e:
                    log.debug("refetch after cancel %s: %s", _oid, e)

            r.buy_order = order
            filled = float(order.get("filled") or 0) if order else 0.0
            if filled <= 0:
                r.error = (f"LIMIT wait {_limit_wait_sec}s: {plan.buy_eid} "
                           f"стакан нічого не налив на {plan.base} "
                           f"@ {widened_limit:.6g} — skip")
                await self._post(r, S_FAILED, f"❌ {r.error}")
                return False
            # Scale plan to what actually filled. Re-quote Kyber at the
            # actual filled qty (smaller size = tighter slippage = usually
            # BETTER net than linear extrapolation from plan). Only proceed
            # when real net at real size ≥ min_fill AND ≥ $5 profit.
            if filled < plan.qty * 0.95:
                _scale = filled / plan.qty
                _fill_notional = filled * widened_limit
                _linear_net = (plan.net_profit_usd or 0) * _scale
                # Fresh Kyber quote at actual filled size — dexcycle only.
                # If it errors we fall back to linear estimate.
                _real_net = _linear_net
                try:
                    if (plan.chain and plan.base_contract
                            and plan.sell_eid == "dex"):
                        _kq = await dex.usd_price(
                            plan.chain, plan.base_contract,
                            usd_notional=_fill_notional,
                            reference_price_usd=widened_limit,
                        )
                        if _kq and _kq.get("price_sell_usd", 0) > 0:
                            _sell_px = _kq["price_sell_usd"]
                            _gross = filled * (_sell_px - widened_limit)
                            _fees = (_fill_notional * 0.0035
                                     + dex.swap_gas_cost_usd(plan.chain))
                            _real_net = _gross - _fees
                            log.warning(
                                "LIMIT-WAIT %s re-quote Kyber at $%.0f: "
                                "sell_px=%.6g gross=$%.2f fees=$%.2f → "
                                "real net $%.2f (linear was $%.2f)",
                                plan.base, _fill_notional, _sell_px,
                                _gross, _fees, _real_net, _linear_net,
                            )
                except Exception as _e:
                    log.debug("re-quote Kyber partial %s: %s",
                              plan.base, _e)
                _best_net = max(_real_net, _linear_net)
                if _fill_notional < _min_fill_usd or _best_net < 5:
                    r.error = (f"LIMIT partial: {plan.buy_eid} налив "
                               f"{filled:.4f} {plan.base} (~${_fill_notional:.2f}), "
                               f"real net ${_real_net:.2f} / linear "
                               f"${_linear_net:.2f} — недостатньо, "
                               f"токени залишились на біржі")
                    await self._post(r, S_FAILED, f"❌ {r.error}")
                    return False
                log.warning(
                    "LIMIT-WAIT %s PARTIAL PROCEED: qty %.4f → %.4f "
                    "(%.0f%%), net $%.2f (real)",
                    plan.base, plan.qty, filled, _scale * 100, _best_net,
                )
                plan.qty = filled
                plan.notional_usd = _fill_notional
                plan.net_profit_usd = _best_net

        else:
            # FOK ladder for non-Bitvavo CEXes (unchanged)
            # SCALE-DOWN to actual book depth right before FOK — if book at
            # widest buffer can only fill 40% of need_qty, either scale to
            # that amount (if scaled_net still >= min_p) or bail.
            if _fresh_base_ask and _fills_at:
                _max_widest = max(pxq[1] for pxq in _fills_at)
                if _max_widest < plan.qty * 0.9:
                    _min_p = float(os.getenv("AUTOEXEC_MIN_PROFIT_USD",
                                              os.getenv("MIN_PROFIT_USD", "20")))
                    if getattr(plan, "is_testrun", False):
                        _min_p = 0.0
                    _scale = _max_widest / plan.qty
                    _scaled_net = (plan.net_profit_usd or 0) * _scale
                    if _scaled_net >= _min_p:
                        log.warning("phase_buy %s SCALE-DOWN to book depth: "
                                    "%.4f → %.4f (%.0f%%), net $%+.2f",
                                    plan.base, plan.qty, _max_widest,
                                    _scale * 100, _scaled_net)
                        plan.qty = _max_widest * 0.98          # 2% buffer
                        plan.notional_usd *= _scale
                        plan.net_profit_usd = _scaled_net
                    else:
                        r.error = (f"book depth {_max_widest:.4f} {plan.base} "
                                   f"< 90% of {plan.qty:.4f} потрібного; "
                                   f"scaled net ${_scaled_net:.2f} < "
                                   f"${_min_p:.0f} min — skip")
                        await self._post(r, S_FAILED, f"❌ {r.error}")
                        return False

            for _attempt, _buf_bps in enumerate(buffer_bps_ladder, 1):
                widened_limit = base_px * (1 + _buf_bps / 10000)
                _t_send = time.time()
                try:
                    order = await cex.place_order(
                        inst, plan.buy_sym, "limit", "buy",
                        plan.qty, widened_limit, {"timeInForce": "FOK"},
                    )
                except Exception as e:
                    last_err = str(e)
                    log.warning("FOK #%d %s buf %.2f%% CRASH after %.2fs: %s",
                                _attempt, plan.base, _buf_bps / 100,
                                time.time() - _t_send, str(e)[:120])
                    continue
                _dt = time.time() - _t_send
                _fil = float(order.get("filled") or 0) if order else 0
                if order and (_fil > 0 or order.get("status") in ("closed",)):
                    log.warning("FOK #%d %s buf %.2f%% OK filled=%.4f in %.2fs "
                                "(total phase %.2fs since start%s)",
                                _attempt, plan.base, _buf_bps / 100, _fil, _dt,
                                time.time() - _phase_t0,
                                f", {_lag:.2f}s after alert" if _lag else "")
                    break
                log.warning("FOK #%d %s buf %.2f%% NOFILL in %.2fs → wider",
                            _attempt, plan.base, _buf_bps / 100, _dt)
            try:
                if order is None:
                    raise Exception(last_err or "FOK failed all attempts")
            except Exception as e:
                r.error = f"біржа відхилила купівлю: {type(e).__name__}: {str(e)[:200]}"
                await self._post(r, S_FAILED, f"❌ {r.error}")
                return False
            r.buy_order = order
            filled = float(order.get("filled") or 0)
            if filled <= 0:
                _bpx = base_px if base_px else plan.buy_limit
                _wide = widened_limit if widened_limit else plan.buy_limit
                _quote = plan.buy_sym.split("/")[1] if "/" in plan.buy_sym else "?"
                r.error = (f"FOK: {plan.buy_eid} стакан не покрив {plan.qty:.4f} "
                           f"{plan.base}: live ask ~{_bpx:.6g} {_quote}, "
                           f"tried limit {_wide:.6g}, "
                           f"але потрібної глибини в стакані нема")
                await self._post(r, S_FAILED, f"❌ {r.error}")
                return False
        # Subtract the taker fee if it's charged in the base token
        # (most exchanges do that on BUY orders — you get X coins, they
        # deduct fee from X, so `filled` from API overstates what's
        # actually on your balance). Try to read the exact fee from
        # order['fee']/['fees']; fall back to a conservative 0.2%.
        fee_in_base = 0.0
        fee_field = order.get("fee") or {}
        if isinstance(fee_field, dict) and (fee_field.get("currency") or "").upper() == plan.base.upper():
            try:
                fee_in_base = float(fee_field.get("cost") or 0)
            except (TypeError, ValueError):
                pass
        for fe in (order.get("fees") or []):
            if not isinstance(fe, dict):
                continue
            if (fe.get("currency") or "").upper() == plan.base.upper():
                try:
                    fee_in_base += float(fe.get("cost") or 0)
                except (TypeError, ValueError):
                    pass
        # Cross-check with actual free balance — beats any guess
        try:
            bal = await inst.fetch_balance()
            free_base = float((bal.get(plan.base) or {}).get("free") or 0)
        except Exception:
            free_base = 0.0
        # available = min(filled-fee, actual_free) — never overstate.
        # Round down to a safe precision for the withdraw step:
        # Bitvavo alt-coin API sometimes rejects amounts with >4 decimals
        # (silent 204/406 errors). Match currency decimals if known.
        est_available = filled - fee_in_base if fee_in_base > 0 else filled * 0.998
        available = min(est_available, free_base) if free_base > 0 else est_available
        # Determine safe decimals for the withdraw (Bitvavo often finicky)
        try:
            wd_dec = int(inst.currencies.get(plan.base, {}).get("precision") or 8)
        except Exception:
            wd_dec = 8
        # Cap at 8 decimals and floor to avoid rounding UP past free balance
        wd_dec = min(wd_dec, 8)
        import math as _m
        factor = 10 ** wd_dec
        available = _m.floor(available * factor) / factor
        sess.filled_qty = available
        await self._post(r, S_BUY_FILLED,
                         potential + f"2/3 ✅ куплено {filled:.6f} {plan.base}"
                         f" (доступно {available:.6f} після fee)"
                         f" @ ~{float(order.get('average') or plan.buy_limit):g}")
        return True

    async def _solana_hop_withdraw(self, sess: InteractiveSession,
                                    potential: str) -> bool:
        """Solana wallet-hop:
          1) source exchange withdraw → Solana hot wallet
          2) poll SPL balance until token arrives
          3) send from Solana hot wallet → destination Bitvavo/etc address
          4) poll destination exchange until credited
        Requires SOLANA_PRIVATE_KEY + ALCHEMY_KEY. Address must be
        whitelisted on the source exchange for SOL withdraw."""
        import sol
        plan, r = sess.plan, sess.receipt
        sol_hot = sol.HOT_WALLET_ADDRESS
        if not sol_hot:
            r.error = "SOLANA_PRIVATE_KEY не заданий — Solana hot wallet недоступний"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        if self.mode == "dry":
            await self._post(r, S_WITHDRAWING,
                             potential + f"🧪 SOL hop DRY: {sess.filled_qty:.6f} "
                             f"{plan.base} через {sol_hot[:8]}…")
            return True

        # 1) Withdraw source → Solana hot wallet
        creds_src = keys.load_keys().get(plan.buy_eid) or {}
        inst_src = cex.get_private(plan.buy_eid, creds_src)
        try:
            wd_params = {}
            wd = await cex.withdraw_robust(inst_src, plan.base, sess.filled_qty,
                                            sol_hot, None, plan.src_network)
        except Exception as e:
            r.error = f"{cex.pretty(plan.buy_eid)} withdraw fail: {type(e).__name__}: {str(e)[:200]}"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        wd_id = wd.get("id") or wd.get("txid") or "pending"
        await self._post(r, S_WITHDRAWING,
                         potential + f"1/4 {cex.pretty(plan.buy_eid)} → SOL hot wallet · "
                         f"id={str(wd_id)[:20]}")

        # 2) Poll SPL balance on-chain — need the token mint
        # Try Bitvavo assets → networks metadata for the SPL mint.
        # Fallback: fetch from dest exchange deposit info.
        mint = None
        # Look up mint from destination exchange currency info if possible
        try:
            creds_dst = keys.load_keys().get(plan.sell_eid) or {}
            inst_dst = cex.get_private(plan.sell_eid, creds_dst)
            for n in cex.network_info(plan.sell_eid, plan.base):
                from chains import canonical
                if canonical(n.get("network")) == "solana" and n.get("contract"):
                    mint = n["contract"]
                    break
        except Exception:
            pass
        if not mint:
            # try source side
            for n in cex.network_info(plan.buy_eid, plan.base):
                from chains import canonical
                if canonical(n.get("network")) == "solana" and n.get("contract"):
                    mint = n["contract"]
                    break
        if not mint:
            r.error = f"невідомий SPL mint для {plan.base} на Solana"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False

        # WS-first: race Alchemy Solana WS log push against polling.
        try:
            import walletfeed
        except ImportError:
            walletfeed = None
        deadline = time.time() + DEPOSIT_TIMEOUT_MIN * 60
        seen0 = await sol.spl_balance(mint) or 0
        arrived = 0
        while time.time() < deadline:
            if walletfeed:
                # WS pushes any tx mentioning our SOL wallet
                await walletfeed.wait_for_transfer("solana", "*",
                                                    timeout_sec=min(DEPOSIT_POLL_SEC, 10))
            else:
                await asyncio.sleep(DEPOSIT_POLL_SEC)
            cur = await sol.spl_balance(mint) or 0
            delta = cur - seen0
            if delta > 0:
                arrived = cur
                break
        if arrived <= 0:
            r.error = f"SPL {plan.base} не приземлився за {DEPOSIT_TIMEOUT_MIN} хв"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        await self._post(r, S_CONFIRMING,
                         potential + f"2/4 ✅ SOL hot wallet отримав {arrived / 1e6:.4f} {plan.base}")

        # 3) Fetch destination deposit address
        dest_addr = None
        try:
            dep = await cex.fetch_deposit_address_robust(
                inst_dst, plan.base, plan.dst_network)
            if dep:
                dest_addr = dep.get("address")
        except Exception as e:
            log.debug("dst deposit addr err: %s", e)
        if not dest_addr:
            r.error = f"немає deposit-адреси {cex.pretty(plan.sell_eid)}/{plan.base}/solana"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False

        # 4) Send from hot wallet → destination (keep small dust for future ATA rent)
        send_amt = arrived
        send_res = await sol.send_token(mint, send_amt, dest_addr)
        if not send_res.get("ok"):
            r.error = f"SOL send fail: {send_res.get('error')}"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        sig = send_res.get("signature", "")
        # S_DEPOSITING never existed — this raised NameError immediately
        # AFTER a successful SPL send, so the caller retried the whole hop
        # and sent the tokens a second time. Record the signature first so
        # any future failure here can't cost a duplicate transfer.
        sess.forward_tx = sig
        try:
            await self._post(r, S_DEPOSITED,
                             potential + f"3/4 SOL hot → "
                             f"{cex.pretty(plan.sell_eid)} · sig={sig[:20]}…")
        except Exception as _e:
            log.warning("post after SOL send (tokens ALREADY sent, sig=%s): %s",
                        sig[:20], _e)

        # 5) Poll destination for deposit credit
        deadline = time.time() + 600
        expected = send_amt / 1e6                              # human units
        while time.time() < deadline:
            await asyncio.sleep(15)
            try:
                bal = await inst_dst.fetch_balance()
                free = float((bal.get(plan.base) or {}).get("free") or 0)
                if free >= expected * 0.9:
                    sess.filled_qty = free
                    await self._post(r, S_DEPOSITED,
                                     potential + f"4/4 ✅ {cex.pretty(plan.sell_eid)} кредитовано {free:.4f} {plan.base}")
                    return True
            except Exception as e:
                log.debug("dst balance poll err: %s", e)
        r.error = f"{cex.pretty(plan.sell_eid)} не кредитував за 10хв"
        await self._post(r, S_FAILED, f"❌ {r.error}"); return False


    async def _direct_withdraw_and_wait(self, sess: InteractiveSession,
                                         potential: str) -> bool:
        """Direct exchange → exchange withdraw for non-EVM chains
        (SAGA/SOL/BTC/TRX/XLM etc.) where the hot EVM wallet can't hold
        the asset. Uses deposit_addresses.json (or ccxt fetch_deposit_address)
        for the destination address."""
        plan, r = sess.plan, sess.receipt
        creds2 = keys.load_keys().get(plan.sell_eid) or {}
        inst_to = cex.get_private(plan.sell_eid, creds2)
        # Prefer live fetch of destination deposit address, fallback to file
        dest_addr = None
        dest_tag = None
        try:
            dep = await cex.fetch_deposit_address_robust(
                inst_to, plan.base, plan.dst_network)
            if dep:
                dest_addr = dep.get("address")
                dest_tag = dep.get("tag")
        except Exception as e:
            log.debug("fetch_deposit_address (non-EVM) err: %s", e)
        if not dest_addr:
            fb = (self.addresses.get(plan.sell_eid) or {}).get(plan.chain)
            if fb:
                dest_addr, dest_tag = fb.get("address"), fb.get("tag")
        if not dest_addr:
            r.error = (f"немає depsit-адреси {cex.pretty(plan.sell_eid)}/"
                       f"{plan.chain} для {plan.base}")
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False

        if self.mode == "dry":
            await self._post(r, S_WITHDRAWING,
                             potential + f"🧪 <i>ВИВІВ БИ</i> напряму "
                             f"{sess.filled_qty:.6f} {plan.base} → "
                             f"{cex.pretty(plan.sell_eid)} ({plan.chain})")
            await self._post(r, S_DEPOSITED, potential + "🧪 тест-депозит зараховано")
            sess.deposit_credited = True
            return True

        creds = keys.load_keys().get(plan.buy_eid) or {}
        inst_from = cex.get_private(plan.buy_eid, creds)
        net_param = plan.src_network or plan.chain.upper()
        try:
            wd = await cex.withdraw_robust(
                inst_from, plan.base, sess.filled_qty, dest_addr,
                dest_tag, net_param,
            )
        except Exception as e:
            r.error = f"біржа відхилила вивід: {type(e).__name__}: {str(e)[:200]}"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        r.withdraw = wd
        sess.withdraw_tx = wd.get("id") or wd.get("txid") or ""
        await self._post(r, S_WITHDRAWING,
                         potential + f"🚀 напряму {cex.pretty(plan.buy_eid)} → "
                         f"{cex.pretty(plan.sell_eid)} tx "
                         f"<code>{sess.withdraw_tx[:20]}…</code>")
        # Poll destination free-balance directly (updates faster than status).
        deadline = time.time() + DEPOSIT_TIMEOUT_MIN * 60
        try:
            b0 = await inst_to.fetch_balance()
            base0 = float((b0.get(plan.base) or {}).get("free") or 0)
        except Exception:
            base0 = 0.0
        need = sess.filled_qty * 0.90
        while time.time() < deadline:
            if _KILL:
                await self._post(r, S_FAILED, "🛑 вбито під час очікування депозиту")
                return False
            try:
                b = await inst_to.fetch_balance()
                cur = float((b.get(plan.base) or {}).get("free") or 0)
                if cur - base0 >= need:
                    sess.filled_qty = cur
                    sess.deposit_credited = True
                    await self._post(r, S_DEPOSITED,
                                     potential + f"✅ зараховано {cur - base0:.4f} {plan.base}")
                    return True
            except Exception:
                pass
            remaining = int(deadline - time.time())
            await self._post(r, S_CONFIRMING,
                             potential + f"⏳ чекаю депозит… ({remaining}с)")
            await asyncio.sleep(DEPOSIT_POLL_SEC)
        r.error = "timeout депозиту"
        await self._post(r, S_FAILED,
                         f"❌ депозит не зарахували за {DEPOSIT_TIMEOUT_MIN} хв")
        return False

    async def phase_withdraw_and_wait(self, sess: InteractiveSession,
                                        from_hw: bool = False,
                                        wait_manual_wd: bool = False) -> bool:
        """4-step wallet-hop:
          1) exchange withdraw → hot wallet   (skipped if from_hw=True)
          2) poll on-chain until wallet has the token   (skipped if from_hw=True)
          3) hot wallet → destination exchange deposit address
          4) poll destination exchange until credited

        When from_hw=True the caller guarantees the base token is already
        on HW (e.g. resume flow) — we read the current HW balance as the
        starting point and go directly to step 3.
        """
        plan, r = sess.plan, sess.receipt
        if _KILL:
            await self._post(r, S_FAILED, "🛑 вбито перед виводом")
            return False
        potential = self._potential_line(plan)
        hot_addr = dex.HOT_WALLET_ADDRESS
        # ALL exchange↔exchange moves MUST hop through the hot wallet.
        # Direct source→destination is forbidden even when it "works",
        # because it forces us to whitelist every deposit address on
        # every exchange for every chain.
        #  - EVM (any KYBER_CHAIN)  → EVM hot wallet
        #  - Solana                 → Solana hot wallet
        #  - Other (BTC/TRX/XLM/…)  → refuse: we don't own a wallet there
        is_evm = plan.chain in dex.KYBER_CHAIN
        if plan.chain == "solana":
            return await self._solana_hop_withdraw(sess, potential)
        if not is_evm:
            r.error = (f"hop-only policy: не маємо hot wallet на {plan.chain}. "
                       "Пропоную завести гаманець або блеклист базу.")
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        if not hot_addr:
            r.error = "DEX_PRIVATE_KEY не заданий — hot wallet недоступний"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        if self.mode == "dry":
            await self._post(r, S_WITHDRAWING,
                             potential + f"🧪 <i>ВИВІВ БИ</i> {sess.filled_qty:.6f} "
                             f"{plan.base} з {cex.pretty(plan.buy_eid)} → hot wallet "
                             f"<code>{hot_addr[:10]}…</code> чейн {plan.chain}")
            await self._post(r, S_CONFIRMING, potential + "🧪 тест: чекання on-chain")
            await self._post(r, S_WITHDRAWING,
                             potential + f"🧪 hot wallet → {cex.pretty(plan.sell_eid)} deposit")
            await self._post(r, S_DEPOSITED, potential + "🧪 тест-депозит зараховано")
            sess.deposit_credited = True
            return True
        # Resolve contract — precache first (0 API calls), fallback to
        # live network_info on the source exchange, final fallback is
        # WS ticker match after WD broadcast.
        from chains import canonical as _cnl
        target_chain = _cnl(plan.chain) or plan.chain.lower()
        contract = None
        try:
            import token_precache as _tp
            tok = _tp.get_token(plan.base) or {}
            entry = tok.get(target_chain) or {}
            if entry.get("contract"):
                contract = entry["contract"]
                log.debug("contract from precache: %s/%s → %s",
                          plan.base, target_chain, contract)
        except Exception: pass
        if not contract:
            for eid in (plan.buy_eid, plan.sell_eid):
                for n in cex.network_info(eid, plan.base):
                    if _cnl(n.get("network")) == target_chain and n.get("contract"):
                        contract = n["contract"]
                        break
                if contract:
                    break

        # Baseline BEFORE we broadcast the withdraw — otherwise if the
        # tokens arrive faster than our first read (Alchemy quirks or
        # fast chains) baseline already includes them and delta == 0.
        # If contract unknown at this point, baseline is skipped — the
        # WS will tell us the real contract from the first fresh arrival
        # after `withdraw_started_ts`.
        import dex as dex_mod
        baseline = 0
        if contract:
            try:
                baseline = await dex_mod.wallet_token_balance(plan.chain, contract) or 0
            except Exception:
                baseline = 0
        withdraw_started_ts = time.time()

        # Step 1/4: exchange withdraw → hot wallet  (skipped when from_hw)
        if from_hw:
            # HW already has base tokens — read balance now, skip the poll.
            arrived_wei = 0
            if contract:
                try:
                    arrived_wei = await dex_mod.wallet_token_balance(
                        plan.chain, contract) or 0
                except Exception:
                    arrived_wei = 0
            if arrived_wei <= 0:
                # No known contract, or balance read failed. Take best-guess
                # from any recent HW arrival on this chain.
                try:
                    import walletfeed as _wf
                    any_a = _wf.find_any_since(plan.chain, 0)
                    if any_a:
                        contract = any_a.get("token") or contract
                        arrived_wei = any_a["amount"]
                except Exception: pass
            if arrived_wei <= 0:
                r.error = f"from_wallet: не знайшов {plan.base} на HW ({plan.chain})"
                await self._post(r, S_FAILED, f"❌ {r.error}"); return False
            sess.filled_qty_wei = arrived_wei
            # Real decimals from contract, not hardcoded 18
            _dec = 18
            if contract:
                try:
                    from web3 import Web3 as _W3
                    _w3 = dex_mod._get_w3(plan.chain)
                    if _w3:
                        _abi = [{"inputs": [], "name": "decimals",
                                  "outputs": [{"name": "", "type": "uint8"}],
                                  "stateMutability": "view", "type": "function"}]
                        _c = _w3.eth.contract(
                            address=_W3.to_checksum_address(contract),
                            abi=_abi)
                        _dec = int(_c.functions.decimals().call())
                except Exception:
                    pass
            sess.filled_qty = arrived_wei / (10 ** _dec)
            await self._post(r, S_CONFIRMING,
                             potential + f"✅ from_wallet: {sess.filled_qty:.6f} "
                             f"{plan.base} на HW")
        elif wait_manual_wd:
            # User did the withdraw manually via exchange UI — skip
            # broadcast, just wait for HW arrival then continue with
            # SEND + credit + SELL as usual.
            await self._post(r, S_WITHDRAWING,
                             potential + f"👋 1/4 РУЧНИЙ вивід від "
                             f"{cex.pretty(plan.buy_eid)} — чекаю на hot wallet…")
        elif sess.withdraw_tx:
            # IDEMPOTENCY: this phase was already invoked in a prior
            # retry — we already broadcast the exchange WD. DO NOT send
            # another one (was creating duplicate WDs on every retry!).
            # Just resume waiting for HW arrival.
            log.info("phase_withdraw retry: withdraw_tx already set (%s) — "
                      "skipping re-broadcast", sess.withdraw_tx[:20])
            await self._post(r, S_WITHDRAWING,
                             potential + f"♻️ WD вже broadcast раніше "
                             f"(tx <code>{sess.withdraw_tx[:16]}…</code>) — "
                             f"чекаю прихід на hot wallet")
        else:
            creds = keys.load_keys().get(plan.buy_eid) or {}
            inst_from = cex.get_private(plan.buy_eid, creds)
            try:
                net_param = plan.src_network or plan.chain.upper()
                wd = await cex.withdraw_robust(
                    inst_from, plan.base, sess.filled_qty, hot_addr,
                    None, net_param,
                )
            except Exception as e:
                r.error = f"біржа відхилила вивід: {type(e).__name__}: {str(e)[:200]}"
                await self._post(r, S_FAILED, f"❌ {r.error}"); return False
            r.withdraw = wd
            sess.withdraw_tx = wd.get("id") or wd.get("txid") or ""
            await self._post(r, S_WITHDRAWING,
                             potential + f"🚀 1/4 {cex.pretty(plan.buy_eid)} → hot wallet · "
                             f"tx <code>{sess.withdraw_tx[:20]}…</code>")
        # WS-first: race Alchemy WebSocket log push against periodic
        # balance polling. First to detect the arrival wins (WS
        # typically ~200ms after tx mined). Skipped when from_hw.
        deadline_wallet = time.time() + DEPOSIT_TIMEOUT_MIN * 60
        if not from_hw:
            arrived_wei = 0
        try:
            import walletfeed
        except ImportError:
            walletfeed = None
        # MEMPOOL WATCHER — parallel background task. As soon as the CEX
        # broadcasts our WD to the public mempool, alchemy pushes it (~1-3s).
        # We log the pending event with amount so the caller can pre-decide
        # the forward amount BEFORE the block is even mined.
        _mempool_seen_ts: dict = {"ts": None, "amount": 0, "tx": None}
        async def _watch_mempool():
            if not walletfeed or not contract or from_hw or plan.chain == "solana":
                return
            try:
                p = await walletfeed.wait_for_pending(
                    plan.chain, contract, timeout_sec=DEPOSIT_TIMEOUT_MIN * 60,
                    since_ts=withdraw_started_ts)
                if p:
                    _mempool_seen_ts["ts"] = p.get("ts")
                    _mempool_seen_ts["amount"] = p.get("amount", 0)
                    _mempool_seen_ts["tx"] = p.get("tx", "")
                    log.info("MEMPOOL DETECTED %s %s: %s → %d wei "
                             "(delta from WD_broadcast=%.1fs)",
                             plan.chain, plan.base, p.get("tx", "")[:20],
                             p.get("amount", 0),
                             p.get("ts", 0) - withdraw_started_ts)
            except Exception as e:
                log.debug("mempool watch %s %s err: %s",
                          plan.chain, plan.base, e)
        _mempool_task = asyncio.create_task(_watch_mempool())
        while not from_hw and time.time() < deadline_wallet:
            if _KILL:
                await self._post(r, S_FAILED, "🛑 вбито під час очікування ончейн-депозиту")
                return False
            # Primary: exchange-declared contract. Trusted when non-None.
            # Fallback: ticker-only match (chain + timing) — used ONLY when
            # neither source nor destination exchange exposed a contract.
            if walletfeed:
                if contract:
                    arrival = await walletfeed.wait_for_transfer(
                        plan.chain, contract,
                        timeout_sec=min(DEPOSIT_POLL_SEC, 10),
                        since_ts=withdraw_started_ts)
                    if arrival:
                        arrived_wei = arrival.get("amount", 0)
                        log.info("hot-wallet WS arrival: %s %s tx=%s amount=%d",
                                 plan.chain, plan.base,
                                 arrival.get("tx", "")[:20], arrived_wei)
                        break
                else:
                    # No exchange contract → ticker fallback: match by
                    # chain + amount (raw amount must land within ±10% of
                    # `sess.filled_qty` when converted with 18/6/8-dec).
                    # This rejects random dust transfers.
                    any_arrival = walletfeed.find_any_since(
                        plan.chain, withdraw_started_ts,
                        expected_qty=float(sess.filled_qty or 0))
                    if any_arrival:
                        contract = any_arrival.get("token") or contract
                        arrived_wei = any_arrival["amount"]
                        log.info("hot-wallet ticker-fallback arrival: %s %s "
                                 "tx=%s contract=%s amount=%d (exp qty=%.4f)",
                                 plan.chain, plan.base,
                                 any_arrival.get("tx", "")[:20],
                                 contract, arrived_wei, sess.filled_qty)
                        break
            # Fallback poll — for chains without WS or missed events
            try:
                cur = await dex_mod.wallet_token_balance(plan.chain, contract) or 0
            except Exception:
                cur = 0
            if cur > baseline:
                arrived_wei = cur - baseline
                break
            remaining = int(deadline_wallet - time.time())
            await self._post(r, S_CONFIRMING,
                             potential + f"⏳ 2/4 чекаю on-chain… ({remaining}с)")
        # If WS gave us contract-verified arrival but amount=0 (shouldn't
        # happen for real transfers), or balance polling detected
        # non-zero delta — trust and proceed.
        if arrived_wei <= 0:
            # Last-ditch balance read
            try:
                cur = await dex_mod.wallet_token_balance(plan.chain, contract) or 0
                if cur > baseline:
                    arrived_wei = cur - baseline
            except Exception:
                pass
        if arrived_wei <= 0:
            r.error = "timeout на гаманці"
            await self._post(r, S_FAILED,
                             f"❌ не дочекались перерахування на гаманець за "
                             f"{DEPOSIT_TIMEOUT_MIN} хв")
            return False
        sess.filled_qty_wei = arrived_wei
        sess.hw_arrived_ts = time.time()
        # CRITICAL: Gate's withdrawal fee eats a chunk of what we bought.
        # (e.g. bought 1248 AGI, arrived 988 on HW after ~260 AGI fee).
        # Update sess.filled_qty to what ACTUALLY arrived, so the
        # downstream Bitvavo credit poll uses the right expected amount
        # (was waiting for 1123 = 1248 × 0.9 which never came).
        # Read token decimals from the contract (1 RPC call, cached in
        # web3 provider). Assume 18 as a safe default.
        decimals = 18
        try:
            # Cached per (chain, contract) — decimals never changes.
            if not hasattr(self, "_decimals_cache"):
                self._decimals_cache = {}
            _dk = (plan.chain, contract.lower())
            cached = self._decimals_cache.get(_dk)
            if cached is not None:
                decimals = cached
            else:
                from web3 import Web3 as _W3
                _w3 = dex_mod._get_w3(plan.chain) if hasattr(dex_mod, "_get_w3") \
                    else _W3(_W3.HTTPProvider(dex_mod._rpc_for(plan.chain),
                                                request_kwargs={"timeout": 5}))
                _abi = [{"inputs": [], "name": "decimals",
                          "outputs": [{"name": "", "type": "uint8"}],
                          "stateMutability": "view", "type": "function"}]
                _c = _w3.eth.contract(address=_W3.to_checksum_address(contract),
                                        abi=_abi)
                decimals = int(_c.functions.decimals().call())
                self._decimals_cache[_dk] = decimals
        except Exception as _e:
            log.debug("decimals read err: %s", _e)
        actual_qty = arrived_wei / (10 ** decimals)
        log.info("hot-wallet post-arrival: qty was %.6f → now %.6f (%d dec)",
                  sess.filled_qty, actual_qty, decimals)
        sess.filled_qty = actual_qty
        await self._post(r, S_CONFIRMING,
                         potential + f"✅ 2/4 гаманець отримав {actual_qty:.4f} "
                         f"{plan.base} (WD fee зʼїв різницю)")

        # Step 3/4: hot wallet → destination exchange deposit address.
        creds2 = keys.load_keys().get(plan.sell_eid) or {}
        inst_to = cex.get_private(plan.sell_eid, creds2)
        # Preferred path: precache (0 API calls). Fall back to live fetch,
        # then the static deposit_addresses.json file.
        dest_addr = None
        try:
            import token_precache as _tp
            dest_addr = _tp.get_deposit_address(plan.sell_eid, plan.chain)
        except Exception: pass
        if not dest_addr:
            try:
                dep_info = await cex.fetch_deposit_address_robust(
                    inst_to, plan.base, plan.dst_network)
                if dep_info:
                    dest_addr = dep_info.get("address")
            except Exception as e:
                log.debug("fetch_deposit_address err: %s", e)
        if not dest_addr:
            fb = (self.addresses.get(plan.sell_eid) or {}).get(plan.chain)
            dest_addr = fb.get("address") if fb else None
        if not dest_addr:
            r.error = (f"немає depsit-адреси {cex.pretty(plan.sell_eid)}/{plan.chain} "
                       f"(ні через API, ні в файлі)")
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False

        # IDEMPOTENCY — if a prior retry already broadcast the HW send,
        # DON'T fire another (would spam extra ETH transactions). BUT:
        # verify the tx actually exists on-chain first; a stale
        # `forward_tx` from a half-run that crashed BEFORE broadcast
        # would otherwise permanently freeze the session at "already sent".
        skip_broadcast = False
        forward_tx = None
        if sess.forward_tx and plan.chain != "solana":
            try:
                receipt = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, lambda: dex.wait_receipt(plan.chain, sess.forward_tx, timeout_sec=3)),
                    timeout=5.0)
            except Exception:
                receipt = None
            if receipt:
                skip_broadcast = True
                forward_tx = sess.forward_tx
                log.info("phase_withdraw retry: forward_tx %s verified on-chain — skipping re-broadcast",
                         sess.forward_tx[:20])
                await self._post(r, S_WITHDRAWING,
                                 potential + f"♻️ SEND вже broadcast "
                                 f"({dex.tx_link(plan.chain, forward_tx)}) — "
                                 f"чекаю кредит")
            else:
                log.warning("phase_withdraw: stale forward_tx %s not on-chain — will re-broadcast",
                            sess.forward_tx[:20])
                sess.forward_tx = None
        if not skip_broadcast:
            # Gas insurance: FAST-PATH — check HW native balance directly
            # first; only invoke the full refill machinery (Gate BUY/WD +
            # potential 90s wait) if HW is genuinely under the trigger.
            if plan.chain != "solana":
                try:
                    import gas_refill as _gr
                    # Cheap direct check — 1 RPC call, no Gate roundtrip
                    _asset = _gr.REFILL.get(plan.chain, (None,))[0]
                    _price = _gr.NATIVE_PRICE.get(_asset, 0) if _asset else 0
                    _target = _gr.REFILL.get(plan.chain, (None, None, 5))[2] \
                        if plan.chain in _gr.REFILL else 5
                    _native = await _gr._hw_native_balance(plan.chain)
                    _usd = (_native or 0) * _price
                    if _native is not None and _usd >= _target * 0.5:
                        pass                                # enough gas, skip refill
                    else:
                        ok_gas = await _gr.ensure_hot_gas(plan.chain,
                                                           bypass_rate_limit=True)
                        if not ok_gas:
                            await self._post(r, S_WITHDRAWING,
                                              potential +
                                              f"⛽ HW {plan.chain} — газ низький, "
                                              f"чекаю поповнення з Gate…")
                            # Wait for native token arrival via WS instead
                            # of a blind sleep(90). walletfeed wakes us as
                            # soon as the deposit lands (up to 60s cap).
                            try:
                                import walletfeed as _wf
                                await asyncio.wait_for(
                                    _wf.wait_for_transfer(plan.chain, None,
                                                            timeout=60,
                                                            since_ts=time.time() - 30),
                                    timeout=60.0)
                            except Exception:
                                await asyncio.sleep(20)     # short fallback
                except Exception as e:
                    log.debug("inline gas refill %s: %s", plan.chain, e)
            send_res = await dex_mod.send_token(plan.chain, contract,
                                                sess.filled_qty_wei, dest_addr)
            if not send_res.get("ok"):
                r.error = f"hot wallet send: {send_res.get('error')}"
                await self._post(r, S_FAILED, f"❌ {r.error}"); return False
            forward_tx = send_res["tx_hash"]
            sess.forward_tx = forward_tx                    # for UI status_cb
            sess.forward_ts = time.time()
            await self._post(r, S_WITHDRAWING,
                             potential + f"🚀 3/4 hot wallet → {cex.pretty(plan.sell_eid)} "
                             f"({dest_addr[:8]}…) · {dex.tx_link(plan.chain, forward_tx)}")

        # Step 4/4: poll destination for credit — use fetch_balance
        # (free balance appears BEFORE the exchange marks the deposit
        # as "ok" in its withdrawal-list status, sometimes minutes earlier).
        deadline_dest = time.time() + DEPOSIT_TIMEOUT_MIN * 60
        # RETRY fetch_balance up to 3 times — if it fails on ALL retries
        # we must ABORT (base0=0 fallback caused the ACE incident where
        # pre-existing balance triggered instant false-positive credit).
        base0 = None
        for _try in range(3):
            try:
                b0 = await inst_to.fetch_balance()
                base0 = float((b0.get(plan.base) or {}).get("free") or 0)
                break
            except Exception as _e:
                log.warning("baseline fetch %s %s try %d: %s",
                            plan.sell_eid, plan.base, _try + 1, _e)
                await asyncio.sleep(1.5)
        if base0 is None:
            r.error = f"cannot read baseline {plan.base} on {plan.sell_eid} — refusing to poll (avoid false-positive)"
            await self._post(r, S_FAILED, f"❌ {r.error}")
            return False
        need = sess.filled_qty * 0.90
        while time.time() < deadline_dest:
            if _KILL:
                await self._post(r, S_FAILED, "🛑 вбито під час очікування депозиту на біржі")
                return False
            try:
                b = await inst_to.fetch_balance()
                cur = float((b.get(plan.base) or {}).get("free") or 0)
                if cur - base0 >= need:
                    sess.filled_qty = cur                       # actual credited amount
                    sess.deposit_credited = True
                    await self._post(r, S_DEPOSITED,
                                     potential + f"✅ 4/4 біржа зарахувала {cur - base0:.4f} {plan.base}")
                    return True
            except Exception:
                pass
            remaining = int(deadline_dest - time.time())
            await self._post(r, S_CONFIRMING,
                             potential + f"⏳ 4/4 чекаю кредит на біржі… ({remaining}с)")
            await asyncio.sleep(DEPOSIT_POLL_SEC)
        r.error = "timeout біржі"
        await self._post(r, S_FAILED,
                         f"❌ біржа не зарахувала за {DEPOSIT_TIMEOUT_MIN} хв "
                         f"(tx на ланцюзі — <code>{forward_tx}</code>)")
        return False

    async def phase_sell(self, sess: InteractiveSession) -> bool:
        """Place SELL leg on destination. Final phase."""
        plan, r = sess.plan, sess.receipt
        if _KILL:
            await self._post(r, S_FAILED, "🛑 вбито перед продажем")
            return False
        potential = self._potential_line(plan)
        if self.mode == "dry":
            await self._post(r, S_PLACING_SELL,
                             potential + f"3/3 🧪 <i>ПРОДАВ БИ</i> "
                             f"{sess.filled_qty:.6f} "
                             f"на {cex.pretty(plan.sell_eid)} @ {plan.sell_limit:g}")
            r.net_pnl_usd = plan.net_profit_usd
            r.finished_ts = time.time()
            await self._post(r, S_DONE,
                             _final_report(plan, sess, r, sess.filled_qty,
                                           sess.filled_qty, plan.buy_limit,
                                           plan.sell_limit,
                                           plan.expected_profit_usd,
                                           (plan.fees or {}).get("total_usd", 0),
                                           "", is_dry=True))
            return True
        # LIVE — always trust actual free balance on destination over
        # our internal sess.filled_qty. Deposit fees / rounding can shrink
        # the deposited amount slightly, and a stale sess.filled_qty would
        # make the sell IOC over-request → 0-fill and "forget to sell".
        creds = keys.load_keys().get(plan.sell_eid) or {}
        inst = cex.get_private(plan.sell_eid, creds)
        # Refresh actual free balance right before placing the sell.
        try:
            bal = await inst.fetch_balance()
            actual_free = float((bal.get(plan.base) or {}).get("free") or 0)
        except Exception:
            actual_free = sess.filled_qty
        if actual_free < sess.filled_qty * 0.98 and actual_free < sess.filled_qty:
            log.info("phase_sell %s: adjusting qty %f → %f (actual free)",
                     plan.base, sess.filled_qty, actual_free)
        sell_qty = min(sess.filled_qty, actual_free) if actual_free > 0 else sess.filled_qty
        if sell_qty <= 0:
            r.error = f"на {cex.pretty(plan.sell_eid)} нема {plan.base} для продажу"
            await self._post(r, S_FAILED, f"❌ {r.error}"); return False
        # Floor to a safe precision — many alts have low decimals.
        try:
            amt_dec = int(inst.currencies.get(plan.base, {}).get("precision") or 6)
        except Exception:
            amt_dec = 6
        import math as _m
        factor = 10 ** min(amt_dec, 8)
        sell_qty = _m.floor(sell_qty * factor) / factor
        # Multi-pass IOC: initial IOC often eats only top-of-book depth
        # on illiquid alts. Loop until 99%+ sold or MAX_PASSES hit,
        # each pass fetching fresh best bid and placing IOC on remaining.
        MAX_PASSES = 8
        total_qty = sell_qty
        remaining = sell_qty
        total_filled = 0.0
        total_sell_proceeds_native = 0.0     # sum of (filled_i * avg_i) in sell_quote
        last_sell = None
        for attempt in range(1, MAX_PASSES + 1):
            if remaining <= total_qty * 0.01:                      # <1% left → done
                break
            # Fetch fresh best bid for THIS pass (price shifts between passes)
            try:
                ob = await inst.fetch_order_book(plan.sell_sym, 5)
                best_bid = float(ob["bids"][0][0]) if ob.get("bids") else plan.sell_limit
            except Exception:
                best_bid = plan.sell_limit
            # Slightly BELOW best bid so we're a taker and swallow depth.
            price_this = best_bid * 0.999
            qty_this = _m.floor(remaining * factor) / factor
            if qty_this <= 0:
                break
            try:
                sell = await cex.place_order(
                    inst, plan.sell_sym, "limit", "sell",
                    qty_this, price_this, {"timeInForce": "IOC"},
                )
            except Exception as e:
                log.warning("sell pass %d err: %s", attempt, e)
                await asyncio.sleep(2)
                continue
            last_sell = sell
            f = float(sell.get("filled") or 0)
            a = float(sell.get("average") or price_this)
            # ccxt returns `cost` = filled × avg (gross, before fee).
            # Fee is separate in `fee` (rare) or `fees[]`.
            gross_native = float(sell.get("cost") or 0) or (f * a)
            fee_native = 0.0
            fee_field = sell.get("fee") or {}
            if isinstance(fee_field, dict) and fee_field.get("cost"):
                fee_native = float(fee_field["cost"])
            for fx_item in (sell.get("fees") or []):
                if isinstance(fx_item, dict) and fx_item.get("cost"):
                    fee_native += float(fx_item["cost"])
            # Track NET proceeds — what actually landed in the quote balance.
            net_native = gross_native - fee_native
            total_filled += f
            total_sell_proceeds_native += net_native
            remaining = max(0.0, total_qty - total_filled)
            log.info("sell pass %d/%d: qty=%g @ %g filled=%g avg=%g remaining=%g",
                     attempt, MAX_PASSES, qty_this, price_this, f, a, remaining)
            # Emit progress so the TG session card updates the SELL row
            # live: "60000 / 240207 (25%)".
            await self._post(r, S_PLACING_SELL,
                             f"SELL_PROGRESS: {total_filled:.4f}/{total_qty:.4f} "
                             f"({total_filled / total_qty * 100:.0f}%) "
                             f"pass {attempt}/{MAX_PASSES}")
            if f < qty_this * 0.05:                              # nothing filled → back off
                await asyncio.sleep(2)
        r.sell_order = last_sell
        sess.filled_qty = total_qty
        exec_qty = total_filled
        # Compute effective average sell price
        sell_avg = (total_sell_proceeds_native / exec_qty) if exec_qty > 0 else plan.sell_limit
        buy_avg = float((r.buy_order or {}).get("average") or plan.buy_limit)
        # ── P&L: (actual notional sold in USD) − (actual USD cost of sold portion)
        # Both sides converted via live FX to USD.
        def _quote_of(sym: str) -> str:
            return (sym.split("/", 1)[1] if "/" in sym else "USDT").upper()
        buy_q = _quote_of(plan.buy_sym)
        sell_q = _quote_of(plan.sell_sym)
        fx_buy = cex.get_fx_rate(buy_q) or 1.0
        fx_sell = cex.get_fx_rate(sell_q) or 1.0
        proceeds_usd = total_sell_proceeds_native * fx_sell   # already NET after sell fee
        # Cost: what we ACTUALLY paid on BUY. ccxt `buy_order.cost` is
        # gross (filled × avg pre-fee); actual paid = cost + buy_fee.
        buy_order = r.buy_order or {}
        buy_cost_native = float(buy_order.get("cost") or 0) or (buy_avg * plan.qty)
        buy_fee_native = 0.0
        bf = buy_order.get("fee") or {}
        if isinstance(bf, dict) and bf.get("cost"):
            buy_fee_native = float(bf["cost"])
        for x in (buy_order.get("fees") or []):
            if isinstance(x, dict) and x.get("cost"):
                buy_fee_native += float(x["cost"])
        # Buy fee is charged in quote OR base ccy. If in base, converting
        # to quote requires price; assume in quote (majority case) and
        # tolerate tiny error. Total cost (all in) = cost + fee, in quote.
        buy_paid_native = buy_cost_native + buy_fee_native
        # Prorate to what we ACTUALLY sold (some may still be stuck)
        cost_usd = buy_paid_native * fx_buy * (exec_qty / max(plan.qty, 1e-12))
        # WD + gas fees (on-chain / withdrawal — NOT trade fees, since
        # trade fees are already inside proceeds_net and cost_paid).
        wd_gas_fees = (plan.fees or {}).get("wd_fee_usd", 0) + \
                      (plan.fees or {}).get("gas_usd", 0)
        # Fallback — if plan.fees.total_usd exists but per-component missing,
        # use half of it as rough on-chain (rest is trade fees).
        if not wd_gas_fees and (plan.fees or {}).get("total_usd"):
            wd_gas_fees = (plan.fees or {}).get("total_usd", 0) * 0.6
        wd_gas_fees *= (exec_qty / max(plan.qty, 1e-12))
        realised = proceeds_usd - cost_usd
        r.net_pnl_usd = realised - wd_gas_fees
        # Backwards-compat for _final_report signature
        fee_total = wd_gas_fees
        log.info("PnL %s: bought %g (paid %.4f %s = $%.2f) → sold %g "
                 "(got net %.4f %s = $%.2f) · WD+gas=$%.2f net=$%.2f",
                 plan.base, plan.qty, buy_paid_native, buy_q, buy_paid_native * fx_buy,
                 exec_qty, total_sell_proceeds_native, sell_q, proceeds_usd,
                 fee_total, r.net_pnl_usd)
        r.finished_ts = time.time()
        stuck_qty = total_qty - exec_qty
        stuck_note = ""
        # STRICT: if we couldn't sell ≥99%, treat as FAILED not DONE.
        if stuck_qty > total_qty * 0.01:
            stuck_usd = stuck_qty * sell_avg * fx_sell
            stuck_note = (f"\n❌ <b>ЗАВИСЛО:</b> {stuck_qty:.4f} {plan.base} "
                          f"(~${stuck_usd:.2f}) на {cex.pretty(plan.sell_eid)} "
                          f"після {MAX_PASSES} проходів — глибина стакана вичерпалася")
            r.error = f"partial sell: only {exec_qty:.2f}/{total_qty:.2f} filled"
            await self._post(r, S_FAILED,
                             _final_report(plan, sess, r, exec_qty, exec_qty,
                                           buy_avg, sell_avg, realised, fee_total,
                                           stuck_note))
            return False
        await self._post(r, S_DONE,
                         _final_report(plan, sess, r, exec_qty, exec_qty,
                                       buy_avg, sell_avg, realised, fee_total,
                                       stuck_note))
        return True


_EX: Executor | None = None


def instance() -> Executor:
    global _EX
    if _EX is None:
        # LIVE by default now — real orders, real money. `EXEC_MODE=dry`
        # in .env still overrides it back to test mode if you ever need
        # to sanity-check the flow without touching APIs.
        _EX = Executor(mode=os.getenv("EXEC_MODE", "live"))
    return _EX
