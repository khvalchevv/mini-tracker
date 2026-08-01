"""Telegram interface for the DS pool spread tracker.

Commands:
  /start        welcome + main menu
  /add          conversational: URL A -> URL B -> threshold %
  /list         all your tracked pairs, one card each, with inline buttons
  /cancel       abort a running /add

Inline buttons per pair:
  Pause / Resume, Edit threshold, Delete, Refresh
"""
import html
import logging
import os
import re
import time
from urllib.parse import urlparse

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters,
)

import cex
import storage

log = logging.getLogger(__name__)

# Conversation states
ASK_A_TYPE, ASK_A_DEX, ASK_A_CEX_SYM, ASK_B_TYPE, ASK_B_DEX, ASK_B_CEX_SYM, ASK_PCT = range(7)

CHAIN_ALIASES = {
    "ethereum": "ethereum", "eth": "ethereum",
    "bsc": "bsc", "binance-smart-chain": "bsc", "bnb": "bsc",
    "polygon": "polygon", "matic": "polygon",
    "arbitrum": "arbitrum", "arb": "arbitrum",
    "optimism": "optimism", "op": "optimism",
    "base": "base",
    "avalanche": "avalanche", "avax": "avalanche",
    "fantom": "fantom", "ftm": "fantom",
    "solana": "solana", "sol": "solana",
    "cronos": "cronos", "linea": "linea", "blast": "blast",
    "scroll": "scroll", "mantle": "mantle", "zksync": "zksync",
    "sui": "sui", "ton": "ton", "tron": "tron",
    "pulsechain": "pulsechain", "pulse": "pulsechain",
    "hyperliquid": "hyperliquid", "hype": "hyperliquid",
    "berachain": "berachain", "bera": "berachain",
    "unichain": "unichain",
}

CHAIN_PRETTY = {
    "ethereum": "ETH", "bsc": "BSC", "polygon": "POLYGON",
    "arbitrum": "ARBITRUM", "optimism": "OPTIMISM", "base": "BASE",
    "avalanche": "AVALANCHE", "fantom": "FANTOM", "solana": "SOL",
    "tron": "TRON", "sui": "SUI", "ton": "TON",
}

# 0x-hex EVM or base58 solana-style
_ADDR_RE = re.compile(r"^(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})$")

ALLOWED: set[int] = set()


def _is_allowed(user_id: int) -> bool:
    return not ALLOWED or user_id in ALLOWED


def parse_ds_input(text: str) -> tuple[str, str] | None:
    """Return (chain, addr_lower) from a DS URL or `chain addr` shorthand."""
    text = text.strip()
    if not text:
        return None
    # URL form
    if "dexscreener.com" in text:
        parsed = urlparse(text if text.startswith("http") else "https://" + text)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) < 2:
            return None
        chain_raw, addr = parts[0].lower(), parts[1]
        chain = CHAIN_ALIASES.get(chain_raw, chain_raw)
        addr = addr.split("?")[0].split("#")[0]
        if not _ADDR_RE.match(addr):
            return None
        return chain, addr.lower() if addr.startswith("0x") else addr
    # `chain addr` shorthand
    parts = text.split()
    if len(parts) == 2:
        chain_raw, addr = parts[0].lower(), parts[1]
        chain = CHAIN_ALIASES.get(chain_raw, chain_raw)
        if _ADDR_RE.match(addr):
            return chain, addr.lower() if addr.startswith("0x") else addr
    return None


def _fmt_price(x: float | None) -> str:
    if x is None:
        return "—"
    if x >= 1:
        return f"${x:,.4f}"
    if x >= 0.01:
        return f"${x:.5f}"
    return f"${x:.8f}".rstrip("0").rstrip(".")


def _chain_pretty(c: str) -> str:
    return CHAIN_PRETTY.get(c, c.upper())


def _side_short(side: dict) -> str:
    if side.get("type") == "cex":
        return f"{cex.pretty(side['exchange'])} {side['symbol']}"
    return f"{_chain_pretty(side['chain'])}:{side['addr'][:6]}"


def _side_link(side: dict) -> str:
    if side.get("type") == "cex":
        url = cex.trading_url(side["exchange"], side["symbol"])
        label = f"{cex.pretty(side['exchange'])}"
        return f'<a href="{html.escape(url)}">{label}</a>'
    url = side.get("url") or ""
    label = _chain_pretty(side["chain"])
    return f'<a href="{html.escape(url)}">{label}</a>' if url else label


def _pair_label(p: dict) -> str:
    return f"{html.escape(_side_short(p['a']))}  ↔  {html.escape(_side_short(p['b']))}"


def _pair_card(p: dict) -> str:
    pa, pb = p.get("last_price_a"), p.get("last_price_b")
    sp = p.get("last_spread")
    sp_s = f"{sp:.2f}%" if sp is not None else "—"
    age = ""
    if p.get("last_ts"):
        secs = max(0, int(time.time() - p["last_ts"]))
        age = f"  <i>({secs}s ago)</i>"
    status = "⏸ paused" if p.get("paused") else "▶ live"
    return (
        f"<b>{_pair_label(p)}</b>  {status}\n"
        f"  A · {_side_link(p['a'])}: {_fmt_price(pa)}\n"
        f"  B · {_side_link(p['b'])}: {_fmt_price(pb)}\n"
        f"  spread: <b>{sp_s}</b>  ·  alert ≥ {p['threshold_pct']:.2f}%{age}"
    )


def _pair_keyboard(p: dict) -> InlineKeyboardMarkup:
    pid = p["id"]
    pause_lbl = "▶ Resume" if p.get("paused") else "⏸ Pause"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(pause_lbl, callback_data=f"pause:{pid}"),
            InlineKeyboardButton("✏ Edit %", callback_data=f"edit:{pid}"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{pid}"),
            InlineKeyboardButton("🗑 Delete", callback_data=f"del:{pid}"),
        ],
    ])


# ---------- handlers ----------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    text = (
        "<b>🎯 Spread Tracker</b>\n\n"
        "Track the spread between any two prices — DexScreener pools "
        "or major CEX pairs — and get pinged when it crosses your threshold.\n\n"
        "Commands:\n"
        "  /add — add a new pair (guided)\n"
        "  /list — your tracked pairs\n"
        "  /cancel — abort /add\n\n"
        "<i>Supported CEX: " + ", ".join(cex.pretty(e) for e in cex.SUPPORTED_EXCHANGES) + "</i>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


def _nav_row(back: str | None = None) -> list[InlineKeyboardButton]:
    row = []
    if back:
        row.append(InlineKeyboardButton("⬅ Back", callback_data=f"nav:{back}"))
    row.append(InlineKeyboardButton("✖ Cancel", callback_data="nav:cancel"))
    return row


def _side_type_kb(letter: str, back: str | None = None) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton("🟢 DEX pool", callback_data=f"side:{letter}:dex"),
        InlineKeyboardButton("🔴 CEX", callback_data=f"side:{letter}:cex"),
    ], _nav_row(back)]
    return InlineKeyboardMarkup(rows)


def _exchange_kb(letter: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for eid in cex.SUPPORTED_EXCHANGES:
        row.append(InlineKeyboardButton(cex.pretty(eid), callback_data=f"ex:{letter}:{eid}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append(_nav_row(f"typea" if letter == "a" else "typeb"))
    return InlineKeyboardMarkup(rows)


def _text_step_kb(back: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([_nav_row(back)])


async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return ConversationHandler.END
    ctx.user_data.clear()
    await update.message.reply_text(
        "🅰 Pick source for <b>side A</b>:",
        parse_mode=ParseMode.HTML,
        reply_markup=_side_type_kb("a"),   # no Back on the very first step
    )
    return ASK_A_TYPE


async def cb_side_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, letter, kind = q.data.split(":")
    ctx.user_data[f"{letter}_type"] = kind
    letter_up = letter.upper()
    if kind == "dex":
        await q.edit_message_text(
            f"{'🅰' if letter=='a' else '🅱'} Send <b>DexScreener URL</b> for side <b>{letter_up}</b>\n"
            "(e.g. <code>https://dexscreener.com/ethereum/0xabc...</code>)",
            parse_mode=ParseMode.HTML,
            reply_markup=_text_step_kb(f"type{letter}"),
        )
        return ASK_A_DEX if letter == "a" else ASK_B_DEX
    else:
        await q.edit_message_text(
            f"{'🅰' if letter=='a' else '🅱'} Pick <b>exchange</b> for side <b>{letter_up}</b>:",
            parse_mode=ParseMode.HTML,
            reply_markup=_exchange_kb(letter),
        )
        return ASK_A_TYPE if letter == "a" else ASK_B_TYPE


async def cb_exchange_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, letter, eid = q.data.split(":")
    ctx.user_data[f"{letter}_exchange"] = eid
    letter_up = letter.upper()
    await q.edit_message_text(
        f"{'🅰' if letter=='a' else '🅱'} <b>{letter_up}</b> · Exchange: <b>{cex.pretty(eid)}</b>\n\n"
        "Send trading symbol (e.g. <code>ETH/USDT</code>, also accepts <code>ETH-USDT</code>, <code>ETHUSDT</code>).",
        parse_mode=ParseMode.HTML,
        reply_markup=_text_step_kb(f"type{letter}"),
    )
    return ASK_A_CEX_SYM if letter == "a" else ASK_B_CEX_SYM


async def cb_nav(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """⬅ Back / ✖ Cancel navigation."""
    q = update.callback_query
    await q.answer()
    _, target = q.data.split(":", 1)
    if target == "cancel":
        ctx.user_data.clear()
        try:
            await q.edit_message_text("Cancelled.")
        except Exception:
            pass
        return ConversationHandler.END
    if target == "typea":
        # forget side A pick
        for k in ("a", "a_type", "a_exchange"):
            ctx.user_data.pop(k, None)
        await q.edit_message_text(
            "🅰 Pick source for <b>side A</b>:",
            parse_mode=ParseMode.HTML,
            reply_markup=_side_type_kb("a"),
        )
        return ASK_A_TYPE
    if target == "typeb":
        for k in ("b", "b_type", "b_exchange"):
            ctx.user_data.pop(k, None)
        await q.edit_message_text(
            "🅱 Pick source for <b>side B</b>:",
            parse_mode=ParseMode.HTML,
            reply_markup=_side_type_kb("b", back="typea"),
        )
        return ASK_B_TYPE
    if target == "pct":
        # back to side B type
        ctx.user_data.pop("b", None)
        await q.edit_message_text(
            "🅱 Pick source for <b>side B</b>:",
            parse_mode=ParseMode.HTML,
            reply_markup=_side_type_kb("b", back="typea"),
        )
        return ASK_B_TYPE
    return ConversationHandler.END


async def _accept_dex(update: Update, ctx: ContextTypes.DEFAULT_TYPE, letter: str) -> int | None:
    parsed = parse_ds_input(update.message.text or "")
    if not parsed:
        await update.message.reply_text(
            "❌ Can't parse. Send a DexScreener URL or `chain 0xaddress`."
        )
        return None
    chain, addr = parsed
    ctx.user_data[letter] = {"type": "dex", "chain": chain, "addr": addr,
                             "url": (update.message.text or "").strip()}
    return 1


async def _accept_cex(update: Update, ctx: ContextTypes.DEFAULT_TYPE, letter: str) -> int | None:
    sym = cex.normalize_symbol(update.message.text or "")
    if not sym:
        await update.message.reply_text("❌ Bad symbol. Send like `ETH/USDT`.")
        return None
    eid = ctx.user_data.get(f"{letter}_exchange")
    if not await cex.has_symbol(eid, sym):
        await update.message.reply_text(
            f"❌ {cex.pretty(eid)} doesn't list <code>{sym}</code>. Try another symbol.",
            parse_mode=ParseMode.HTML,
        )
        return None
    ctx.user_data[letter] = {"type": "cex", "exchange": eid, "symbol": sym}
    return 1


async def _after_a(update, ctx):
    a = ctx.user_data["a"]
    await update.message.reply_text(
        f"✅ A: <b>{html.escape(_side_short(a))}</b>\n\n🅱 Pick source for <b>side B</b>:",
        parse_mode=ParseMode.HTML,
        reply_markup=_side_type_kb("b", back="typea"),
    )
    return ASK_B_TYPE


async def _after_b(update, ctx):
    b = ctx.user_data["b"]
    await update.message.reply_text(
        f"✅ B: <b>{html.escape(_side_short(b))}</b>\n\n"
        "📊 Send <b>threshold %</b> (e.g. <code>2.5</code>).",
        parse_mode=ParseMode.HTML,
        reply_markup=_text_step_kb("typeb"),
    )
    return ASK_PCT


async def h_a_dex(update, ctx):
    if await _accept_dex(update, ctx, "a") is None:
        return ASK_A_DEX
    return await _after_a(update, ctx)


async def h_a_cex(update, ctx):
    if await _accept_cex(update, ctx, "a") is None:
        return ASK_A_CEX_SYM
    return await _after_a(update, ctx)


async def h_b_dex(update, ctx):
    if await _accept_dex(update, ctx, "b") is None:
        return ASK_B_DEX
    return await _after_b(update, ctx)


async def h_b_cex(update, ctx):
    if await _accept_cex(update, ctx, "b") is None:
        return ASK_B_CEX_SYM
    return await _after_b(update, ctx)


async def add_threshold(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").replace(",", ".").strip().rstrip("%")
    try:
        pct = float(raw)
        if not (0 < pct <= 1000):
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Send a number, e.g. `2.5`")
        return ASK_PCT

    a = ctx.user_data["a"]
    b = ctx.user_data["b"]
    rec = storage.add(update.effective_user.id, a, b, pct)
    await update.message.reply_text(
        "✅ Pair added:\n\n" + _pair_card(rec),
        parse_mode=ParseMode.HTML,
        reply_markup=_pair_keyboard(rec),
        disable_web_page_preview=True,
    )
    return ConversationHandler.END


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    pairs = storage.by_owner(update.effective_user.id)
    if not pairs:
        await update.message.reply_text("No pairs yet. Use /add.")
        return
    pairs.sort(key=lambda p: -(p.get("last_spread") or 0))
    for p in pairs:
        await update.message.reply_text(
            _pair_card(p),
            parse_mode=ParseMode.HTML,
            reply_markup=_pair_keyboard(p),
            disable_web_page_preview=True,
        )


# ---------- callbacks ----------

async def _refresh_message(query, p: dict):
    try:
        await query.edit_message_text(
            _pair_card(p),
            parse_mode=ParseMode.HTML,
            reply_markup=_pair_keyboard(p),
            disable_web_page_preview=True,
        )
    except Exception:
        pass


async def cb_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _is_allowed(q.from_user.id):
        return
    data = q.data or ""
    action, _, pid = data.partition(":")
    p = storage.get(pid)
    if not p or p["owner"] != q.from_user.id:
        await q.edit_message_text("(pair not found)")
        return

    if action == "pause":
        p = storage.update(pid, paused=not p.get("paused"))
        await _refresh_message(q, p)
    elif action == "refresh":
        await _refresh_message(q, p)
    elif action == "del":
        storage.delete(pid)
        try:
            await q.edit_message_text(f"🗑 Deleted: {_pair_label(p)}",
                                      parse_mode=ParseMode.HTML)
        except Exception:
            pass
    elif action == "edit":
        ctx.user_data["edit_pid"] = pid
        await q.message.reply_text(
            f"Send new threshold % for <b>{_pair_label(p)}</b>\n"
            f"(current: {p['threshold_pct']:.2f}%)",
            parse_mode=ParseMode.HTML,
        )
        # picked up by ASK_EDIT_PCT handler via /edit conversation trigger
        ctx.application.chat_data.setdefault(q.message.chat_id, {})["awaiting_edit"] = pid


async def on_free_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Fallback: handle threshold edits triggered by inline `edit` button."""
    if not _is_allowed(update.effective_user.id):
        return
    pid = ctx.application.chat_data.get(update.effective_chat.id, {}).get("awaiting_edit")
    if not pid:
        return
    raw = (update.message.text or "").replace(",", ".").strip().rstrip("%")
    try:
        pct = float(raw)
        if not (0 < pct <= 1000):
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Send a number, e.g. `2.5`")
        return
    ctx.application.chat_data[update.effective_chat.id].pop("awaiting_edit", None)
    p = storage.update(pid, threshold_pct=pct)
    if p:
        await update.message.reply_text(
            f"✅ Updated:\n\n{_pair_card(p)}",
            parse_mode=ParseMode.HTML,
            reply_markup=_pair_keyboard(p),
            disable_web_page_preview=True,
        )


# ---------- alert dispatcher (called by Tracker) ----------

def _side_line(side: dict, price: float, liq: float) -> str:
    link = _side_link(side)
    label = html.escape(_side_short(side))
    liq_part = f" · liq ${liq:,.0f}" if liq else ""
    return f"  {link} · <b>{label}</b>: {_fmt_price(price)}{liq_part}"


def make_alert_sender(app: Application):
    async def send(pair: dict, pa: dict, pb: dict, spread: float):
        direction = "A → B" if pa["price"] < pb["price"] else "B → A"
        buy_link = pa["url"] if pa["price"] < pb["price"] else pb["url"]
        sell_link = pb["url"] if pa["price"] < pb["price"] else pa["url"]
        buy_side = "A" if pa["price"] < pb["price"] else "B"
        sell_side = "B" if buy_side == "A" else "A"
        text = (
            f"🚨 <b>{_pair_label(pair)}</b>  <b>{spread:.2f}%</b>\n"
            f"Direction: <b>{direction}</b> (buy {buy_side} / sell {sell_side})\n\n"
            f"{_side_line(pair['a'], pa['price'], pa.get('liq', 0))}\n"
            f"{_side_line(pair['b'], pb['price'], pb.get('liq', 0))}\n\n"
            f"Threshold: {pair['threshold_pct']:.2f}%"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🟢 Buy {buy_side}", url=buy_link or "https://dexscreener.com"),
             InlineKeyboardButton(f"🔴 Sell {sell_side}", url=sell_link or "https://dexscreener.com")],
            [InlineKeyboardButton("⏸ Pause", callback_data=f"pause:{pair['id']}"),
             InlineKeyboardButton("🗑 Delete", callback_data=f"del:{pair['id']}")],
        ])
        try:
            await app.bot.send_message(
                chat_id=pair["owner"], text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb, disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("send alert to %s failed: %s", pair["owner"], e)
    return send


# ---------- wiring ----------

def build_application(token: str, allowed: set[int]) -> Application:
    global ALLOWED
    ALLOWED = allowed
    app = Application.builder().token(token).build()

    nav_cb = CallbackQueryHandler(cb_nav, pattern=r"^nav:")
    add_conv = ConversationHandler(
        entry_points=[CommandHandler("add", cmd_add)],
        states={
            ASK_A_TYPE: [
                CallbackQueryHandler(cb_side_type, pattern=r"^side:a:"),
                CallbackQueryHandler(cb_exchange_pick, pattern=r"^ex:a:"),
                nav_cb,
            ],
            ASK_A_DEX:     [nav_cb, MessageHandler(filters.TEXT & ~filters.COMMAND, h_a_dex)],
            ASK_A_CEX_SYM: [nav_cb, MessageHandler(filters.TEXT & ~filters.COMMAND, h_a_cex)],
            ASK_B_TYPE: [
                CallbackQueryHandler(cb_side_type, pattern=r"^side:b:"),
                CallbackQueryHandler(cb_exchange_pick, pattern=r"^ex:b:"),
                nav_cb,
            ],
            ASK_B_DEX:     [nav_cb, MessageHandler(filters.TEXT & ~filters.COMMAND, h_b_dex)],
            ASK_B_CEX_SYM: [nav_cb, MessageHandler(filters.TEXT & ~filters.COMMAND, h_b_cex)],
            ASK_PCT:       [nav_cb, MessageHandler(filters.TEXT & ~filters.COMMAND, add_threshold)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel), nav_cb],
        per_message=False,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(add_conv)
    # pair-action buttons (pause/refresh/del/edit) — not scoped to conversation
    app.add_handler(CallbackQueryHandler(cb_button, pattern=r"^(pause|refresh|del|edit):"))
    # fallback for free-text (edit threshold triggered from inline)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_free_text))
    return app
