# Bitvavo autotrade — architecture & risk plan

Goal: from spread signal to end-to-end automated fills across Bitvavo,
target CEX (Binance/Gate/Bitget), and DEX (via OKX Web3 aggregator).
DEX takes priority when its spread ≥ the best CEX spread.

This document is the design ground-truth. Nothing gets built until the
decisions in the "Open questions" section are locked.

---

## 1. What actually has to happen (end-to-end)

For every alert the bot currently emits, an autotrade run is a sequence
of one of these two shapes:

**A) Inventory arbitrage (no cross-exchange transfer)**
1. Bot detects spread, sizing says $X executable at Y% eff spread.
2. Bot places market/IOC on the cheap side (buy) AND market/IOC on the
   expensive side (sell from existing inventory) — simultaneously.
3. Fills reported back; PnL logged; inventory drifts (more crypto on
   cheap-side, more fiat/USDT on expensive-side).
4. Rebalancing is a separate, opportunistic job — batched into big
   moves when the market is quiet.

**B) Cross-exchange arbitrage (with transfer)**
1. Bot buys on cheap side.
2. Bot triggers withdraw of the exact bought quantity to the destination
   exchange's pre-whitelisted deposit address on the chosen chain.
3. Bot polls the destination for deposit-credited event (can take 5s
   for TRC20, 15 min for ETH, 1–2h for BTC-based).
4. Bot sells on the destination once credited.
5. Failure modes at every step must be handled without stranding funds.

The bot should default to (A) because (B) has fundamental price-risk
during transfer. (B) is a manual-only or one-off flow behind a
`--allow-transfer` guard.

---

## 2. DEX priority

Every alert already ranks entries by spread. Once DEX data is back
(OKX Web3 aggregator per contract-bearing chain), a DEX entry is just
another candidate. Priority rule proposed:

> If any DEX pool for the base has usable liquidity AND a spread ≥ the
> best CEX spread minus a small tolerance (e.g. 10 bps), prefer DEX.

Reason: DEX has zero withdrawal risk (single tx), zero KYC risk, but
higher gas and price-impact risk on small pools. The liquidity gate
must be enforced upstream — no picking a DEX pool with $2k liquidity
for a $20k trade.

**DEX aggregator = KyberSwap** (chosen for deeper cross-DEX routing
than OKX Web3 and per-token per-chain support out of the box):

- Price/quote: `GET https://aggregator-api.kyberswap.com/{chain}/api/v1/routes?tokenIn=…&tokenOut=…&amountIn=…`.
  Returns `routeSummary` with `amountOut`, per-hop route, gas estimate.
- Swap build: `POST https://aggregator-api.kyberswap.com/{chain}/api/v1/route/build`
  with the `routeSummary` → returns unsigned `data` + `routerAddress`.
- No key required for either call.

Execution path:
- **Phase 1**: read-only. Kyber `amountOut` is what we use for DEX price
  in every alert (already normalises to a real executable rate,
  slippage-inclusive).
- **Phase 2**: server builds the swap tx and hands calldata to
  WalletConnect. User signs on own wallet (Rabby / MetaMask). Bot
  never sees the private key.
- **Phase 3** (later, optional): local hot wallet with strict per-tx
  and per-day caps for one-tap execution.

---

## 3. Risks and how each is mitigated

| Risk | Cause | Mitigation |
|------|-------|------------|
| Withdraw-key theft | API keys with withdraw perms + no IP lock | Never use withdraw keys in Phase 1–2. When enabled, IP-whitelist the server, whitelist destination addresses only, cap per-withdraw and per-day. |
| Transfer price collapse | Spread disappears while ETH is in mempool | Default to inventory arbitrage. Only allow cross-exchange transfer for `top_spread ≥ 3× threshold` AND for coins that clear in <60s (TRC20, SOL). |
| Slippage vs sizing estimate | Books move between sizing calc and order placement | Place LIMIT orders at the boundary prices `last_buy_native` and `last_sell_native` returned by sizing, valid for 3s. Fills < 60% → abandon and re-evaluate. |
| Partial fill on one leg | Cheap leg fills, expensive leg doesn't (or reverse) | Both legs go in as **IOC**; if one leg fills materially more than the other, cancel unfilled portion, hedge the exposure on the SAME exchange the imbalance sits on (small market order in the correction direction). |
| Symbol collision on Bitget/etc | CG's exchange map ties `bitget|RON` to Ronin coin_id but Bitget actually lists a different Arbitrum token | Add a contract-identity gate: if target CEX exposes any contract for the ticker AND none of those contracts appear in CG's platform list for the mapped coin, reject the entry. Already scoped as a Phase 1 pre-req. |
| Bitvavo EUR quote drift | EUR/USD moves between alert and fill | Bitvavo leg is placed in EUR at the EUR price we saw, not the USD-normalized one. FX is only for display + spread math. |
| Nonce collisions on same exchange | Two alerts on overlapping bases fire together | Per-exchange asyncio.Lock around order placement. |
| Balance overshoot | Bot tries to sell more base than we hold | Pre-flight `fetch_balance`; cap the sell leg at `min(sized_qty, current_balance × 0.98)`. |
| Race with the sniper crowd | 30% spreads on tickers = probably a fake / stale price, not a real opportunity | Contract-identity gate + honor blacklist + orderbook cross-check already implemented drops these. Confirmed in probe. |
| Silent kill of the bot process | tracker/hunter coroutine dies unnoticed | main.py already has task guards. Add heartbeat write to a file; if age > 60s, systemd (or a wrapper script) restarts. |

---

## 4. Component changes

**cex.py**
- Extend to authenticated ccxt mode (per-exchange `apiKey`/`secret` from
  api_keys.json — already the file convention from dex-cex).
- Wrap `create_order` / `fetch_balance` / `cancel_order` behind small
  helpers that log every request/response and enforce the per-exchange
  lock.
- Keep proxy rotation on public reads; direct connection (or a small
  fixed-IP proxy pool) for private write endpoints (many exchanges
  ban rotating IPs on trading endpoints).

**hunter.py**
- After building `entries`, augment the top entry with a DEX candidate
  and pick the winner by (spread − tolerance).
- Re-emit the `_key` cooldown only after successful fill (extends the
  existing "cooldown-on-send" pattern).

**executor.py (new)**
- Given an alert dict, compute leg parameters, place both legs, watch
  fills, report result. State-machine with these states:
  `IDLE → PLACING → PARTIAL → HEDGING → DONE | FAILED`.
- Never blocks the hunter cycle; runs as its own asyncio task per alert.
- Persists every trade attempt to `trades.jsonl` (append-only, one JSON
  per line — cheap to audit).

**bot.py**
- New button `⚡ Execute` on every alert. Confirm sub-menu:
  `[✅ Yes, ${size}] [🚫 Cancel]`.
- New commands:
  - `/balances` — show inventory across all four exchanges.
  - `/trades N` — last N trade attempts with outcome and PnL.
  - `/kill` — global halt (sets a flag that executor checks before
    placing anything).
  - `/autoexec on|off` — when on, execute automatically without the
    button on alerts that clear all safety checks.

**config**
- New env: `MAX_ORDER_USD`, `MIN_PROFIT_USD_TO_AUTOEXEC`,
  `AUTOEXEC` (bool), `ALLOW_TRANSFER` (bool, default false).
- New file: `api_keys.json` — `{exchange: {apiKey, secret, password?}}`.
  Chmod 600, git-ignored, IP-restricted at each exchange dashboard.

---

## 5. Locked decisions

1. **Trade mode**: transfer-based only (buy → withdraw → deposit → sell).
   Inventory arb dropped.
2. **Kill switch**: `/kill` halts execution only; alerts keep flowing.
   Blacklist stays as the alert-side mute mechanism.
3. **Execution mode**: `⚡ Execute` button on every alert by default;
   `/autoexec on|off` toggles fully autonomous execution for alerts
   that pass every safety filter.
4. **API keys**: `api_keys.json`, chmod 600, git-ignored. Shape:
   `{"binance": {"apiKey": "...", "secret": "..."}, ...}`.
5. **Chain policy** — dynamic, not a static whitelist. Every cycle
   pulls `minConfirm` / `unLockConfirm` from each exchange's capital
   feed and joins with a static block-time table
   (`chains.py: BLOCK_SEC = {"ethereum": 12, "bsc": 3, "polygon": 2,
   "solana": 0.4, "tron": 3, "arbitrum": 0.25, "base": 2, ...}`)
   → `eta_minutes`. An alert is executable only if
   `top_spread_pct ≥ MIN_PROFIT_PCT_PER_MIN × eta_minutes` (default
   `MIN_PROFIT_PCT_PER_MIN=1.0` → SOL needs ~0.05%, ETH needs ~6%).
6. **DEX wallet**: hot wallet in `.env` (`DEX_PRIVATE_KEY`), Kyber
   swaps run fully automated. Guard-rails:
   - `DEX_MAX_TX_USD` (per-swap cap)
   - `dex_whitelist.json` (token contracts + Kyber router only;
     any other destination is rejected)
   - `DEX_MAX_USD_PER_HOUR` spend cap
7. **Reporting**: live per-step status updates in TG as each trade
   moves through the executor's state machine (PLACING → FILLED →
   WITHDRAWING → CONFIRMING(N/M) → DEPOSITED → SELLING → DONE) plus
   a final summary line with net PnL. `/trades N` lists the last N
   attempts. Auto-pause + TG alert after 3 consecutive failures.
8. **Fees**: pulled live per exchange —
   - `exchange.fetch_trading_fees()` cached 1h
   - withdraw fees from the capital feed (already cached 6h)
   - DEX gas via 1inch/blocknative gas oracle, eth_gasPrice fallback
   Subtracted from executable profit BEFORE the `min_profit_usd`
   comparison.

---

## 6. Suggested first sprint (2–3 days)

1. Contract-identity gate in hunter (kills the RON-collision class).
2. `executor.py` skeleton with PLACING/DONE/FAILED, dry-run mode
   (logs the orders it *would* place, no real API call).
3. `⚡ Execute` button wired to dry-run — end-to-end sanity check
   with no capital at risk.
4. Add API keys → flip dry-run off for **one** exchange pair
   (e.g. Bitvavo↔Binance BTC/ETH only, MAX_ORDER_USD=$50).
5. Watch trades for 3–5 days, tune params.

Then decide whether to add DEX, more pairs, or transfer path.
