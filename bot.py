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

import blacklist
import cex
import sizing
import storage
from hunter import Hunter

_HUNTER: Hunter | None = None

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


_SUBSCRIPT = str.maketrans("0123456789", "₀₁₂₃₄₅₆₇₈₉")


def _fmt_price(x: float | None) -> str:
    import math
    if x is None:
        return "—"
    if x <= 0:
        return "$0"
    if x >= 1:
        return f"${x:,.4f}"
    if x >= 0.001:
        return f"${x:.5f}".rstrip("0").rstrip(".")
    # very small — count leading zeros via log10, then subscript-compress
    mag = int(math.floor(math.log10(x)))                    # e.g. 0.0001234 -> -4
    zeros = -mag - 1                                        # zeros after "0."
    sig_val = x * (10 ** -mag)                              # 1.xxx
    sig = f"{sig_val:.4f}".replace(".", "").rstrip("0")[:5] or "0"
    if zeros < 4:
        return f"$0.{'0' * zeros}{sig}"
    return f"$0.0{str(zeros).translate(_SUBSCRIPT)}{sig}"


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
    # auto-register this chat for Bitvavo-hunter alerts
    if _HUNTER is not None:
        _HUNTER.subscribe(update.effective_chat.id)
    text = (
        "<b>🎯 Spread Tracker</b>\n\n"
        "<b>Manual pairs</b> — pick 2 sources (DS pool or CEX), set threshold:\n"
        "  /add — add a new pair (guided)\n"
        "  /list — your tracked pairs\n"
        "  /cancel — abort /add\n\n"
        "<b>Bitvavo hunter</b> — auto-scans every Bitvavo listing vs other CEX. "
        "You're already receiving alerts.\n"
        "  /hunt_pct N — spread % threshold\n"
        "  /hunt_profit N — min executable profit in $\n"
        "  /hunt_status — current state\n"
        "  /blacklist — inspect mute list; /unban to lift\n\n"
        "<b>Check one token</b>:\n"
        "  /c SYMBOL — e.g. <code>/c QUID</code>\n"
        "  /c 0xADDRESS — inspect one contract\n\n"
        "<i>Supported CEX: " + ", ".join(cex.pretty(e) for e in cex.SUPPORTED_EXCHANGES) + "</i>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_hunt_pct(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id) or _HUNTER is None:
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text(
            f"Current: <b>{_HUNTER.threshold:.2f}%</b>. Usage: /hunt_pct 3",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        pct = float(args[0].replace(",", ".").rstrip("%"))
        if not (0 < pct <= 100):
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Bad number. Use e.g. `/hunt_pct 3`")
        return
    _HUNTER.set_threshold(pct)
    await update.message.reply_text(
        f"✅ Threshold set to <b>{pct:.2f}%</b>", parse_mode=ParseMode.HTML,
    )


def _spread_line(price: float, ref: float) -> str:
    if not ref or ref <= 0 or not price:
        return ""
    sp = (price - ref) / ref * 100.0
    sign = "+" if sp > 0 else ""
    return f" ({sign}{sp:.2f}%)"


async def cmd_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/c <symbol|contract>  — inspect all sources for one token."""
    if not _is_allowed(update.effective_user.id) or _HUNTER is None:
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text(
            "Usage: <code>/c SYMBOL</code>  or  <code>/c 0xADDRESS</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    msg = await update.message.reply_text("⏳ looking it up…")
    try:
        r = await _HUNTER.check(args[0])
    except Exception as e:
        await msg.edit_text(f"error: {e}")
        return
    if "error" in r:
        await msg.edit_text(f"❌ {r['error']}")
        return

    name = html.escape(r["name"])
    sym = html.escape(r["symbol"])
    cex_prices: dict = r["cex_prices"] or {}
    dex_prices: dict = r.get("dex_prices") or {}
    contracts: dict = r["contracts"] or {}
    networks: dict = r.get("bitvavo_networks") or {}

    ref = None
    if "bitvavo" in cex_prices:
        ref = cex_prices["bitvavo"]["price"]
    elif cex_prices:
        sorted_px = sorted(p["price"] for p in cex_prices.values())
        ref = sorted_px[len(sorted_px) // 2]

    lines = [f"<b>{name}</b> · <code>{sym}</code>  <i>({html.escape(r['coin_id'])})</i>"]

    def _nets_inline(nets: list) -> str:
        if not nets:
            return ""
        chips = []
        for n in nets:
            dep = "✅" if n["deposit"] else ("❌" if n["deposit"] is False else "?")
            wd = "✅" if n["withdraw"] else ("❌" if n["withdraw"] is False else "?")
            chip = f"{html.escape(n['network'])} {dep}/{wd}"
            if n.get("contract"):
                chip += f' <code>{html.escape(n["contract"][:8])}...</code>'
            chips.append(chip)
        return "\n      " + " · ".join(chips)

    if cex_prices:
        lines.append("\n<b>CEX</b>")
        order = ([("bitvavo", cex_prices["bitvavo"])] if "bitvavo" in cex_prices else []) + \
                sorted(((e, p) for e, p in cex_prices.items() if e != "bitvavo"),
                       key=lambda kv: -abs((kv[1]["price"] - (ref or 0)) / (ref or 1) if ref else 0))
        for eid, p in order:
            url = cex.trading_url(eid, p["symbol"])
            spread = _spread_line(p["price"], ref) if eid != "bitvavo" and ref else ""
            eur_annot = ""
            if eid == "bitvavo":
                rate = cex.get_fx_rate("EUR")
                if rate:
                    eur_annot = f'  <i>(€{_fmt_price(p["price"] / rate).lstrip("$")})</i>'
            lines.append(f'  · <a href="{html.escape(url)}">{cex.pretty(eid)}</a> '
                         f'<code>{html.escape(p["symbol"])}</code>: '
                         f'{_fmt_price(p["price"])}{eur_annot}{spread}'
                         f'{_nets_inline(p.get("networks") or [])}')
    else:
        lines.append("\n<b>CEX</b>: (no listings among supported)")

    if dex_prices:
        lines.append("\n<b>DEX</b>  <i>(OKX Web3)</i>")
        for chain, d in sorted(dex_prices.items(), key=lambda kv: -kv[1].get("vol24h", 0)):
            spread = _spread_line(d["price"], ref) if ref else ""
            vol = d.get("vol24h", 0)
            lines.append(
                f'  · <a href="{html.escape(d["url"])}">{chain.upper()}</a>: '
                f'{_fmt_price(d["price"])}{spread}  ·  24h ${vol:,.0f}'
            )
    else:
        lines.append("\n<b>DEX</b>: (no OKX Web3 quote)")

    if networks:
        lines.append("\n<b>Bitvavo dep/wd</b>")
        for net, n in networks.items():
            dep = "✅" if n["deposit"] else "❌"
            wd = "✅" if n["withdraw"] else "❌"
            fee = n.get("withdrawal_fee")
            mn = n.get("withdrawal_min")
            extra = f"  (wd fee {fee}, min {mn})" if fee else ""
            lines.append(f"  · {net}: dep {dep}  wd {wd}{extra}")

    if contracts:
        lines.append("\n<b>Contracts</b>")
        for chain, addr in contracts.items():
            lines.append(f"  · {chain.upper()}: <code>{addr}</code>")

    await msg.edit_text("\n".join(lines), parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True)


async def cb_blacklist_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Called when user taps the 🚫 Blacklist button on an alert.
    Expands into a 2-option sub-menu: ban this exchange only, or ban the whole token."""
    q = update.callback_query
    await q.answer()
    if not _is_allowed(q.from_user.id):
        return
    _, base, eid = q.data.split(":", 2)
    rows = []
    if eid:
        rows.append([InlineKeyboardButton(
            f"🚫 Only {cex.pretty(eid)} (keep other CEX)",
            callback_data=f"blex:{base}:{eid}",
        )])
    rows.append([InlineKeyboardButton(
        f"🚫 Whole token {base} (all exchanges)",
        callback_data=f"blbase:{base}",
    )])
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"blback:{base}:{eid}")])
    try:
        await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows))
    except Exception:
        pass


async def cb_blacklist_apply(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _is_allowed(q.from_user.id):
        return
    action, _, rest = q.data.partition(":")
    if action == "blex":
        base, eid = rest.split(":", 1)
        blacklist.ban_pair(base, eid)
        note = f"🚫 <b>{base}</b> muted on <b>{cex.pretty(eid)}</b>."
    elif action == "blbase":
        base = rest.split(":", 1)[0]
        blacklist.ban_base(base)
        note = f"🚫 <b>{base}</b> fully muted (all exchanges)."
    elif action == "blback":
        base, eid = rest.split(":", 1)
        try:
            await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([
                # rebuild original 2-row kb — buy/sell URLs lost, keep only blacklist button
                [InlineKeyboardButton("🚫 Blacklist",
                                      callback_data=f"blm:{base}:{eid}")],
            ]))
        except Exception:
            pass
        return
    else:
        return
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    try:
        await q.message.reply_text(note, parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def cmd_blacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/blacklist — show current bans + hint how to unban."""
    if not _is_allowed(update.effective_user.id):
        return
    d = blacklist.snapshot()
    parts = ["<b>Blacklist</b>"]
    if d["bases"]:
        parts.append("\n<b>Whole tokens muted</b>: " +
                     ", ".join(f"<code>{b}</code>" for b in d["bases"]))
    if d["pairs"]:
        parts.append("\n<b>Per-exchange mutes</b>:")
        for base, eids in d["pairs"].items():
            parts.append(f"  · <code>{base}</code> — " +
                         ", ".join(cex.pretty(e) for e in eids))
    if not d["bases"] and not d["pairs"]:
        parts.append("\n(empty)")
    parts.append("\n\nUse /unban SYMBOL or /unban SYMBOL EXCHANGE to lift a mute.")
    await update.message.reply_text("\n".join(parts), parse_mode=ParseMode.HTML)


async def cmd_unban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /unban SYMBOL  or  /unban SYMBOL EXCHANGE")
        return
    base = args[0].upper()
    if len(args) >= 2:
        eid = args[1].lower()
        blacklist.unban_pair(base, eid)
        await update.message.reply_text(
            f"✅ Unmuted <b>{base}</b> on <b>{cex.pretty(eid)}</b>.",
            parse_mode=ParseMode.HTML,
        )
    else:
        blacklist.unban_base(base)
        await update.message.reply_text(
            f"✅ Unmuted <b>{base}</b> everywhere.", parse_mode=ParseMode.HTML,
        )


async def cmd_untracked(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id) or _HUNTER is None:
        return
    d = _HUNTER.untracked()
    total = len(_HUNTER.bases)
    parts = [f"<b>Untracked Bitvavo bases</b> ({len(d['no_coin_id']) + len(d['only_bitvavo'])}/{total})"]
    if d["no_coin_id"]:
        parts.append(f"\n<b>Not in CoinGecko</b> ({len(d['no_coin_id'])}):\n  " +
                     ", ".join(f"<code>{b}</code>" for b in d["no_coin_id"]))
    if d["only_bitvavo"]:
        parts.append(f"\n<b>Only on Bitvavo</b> ({len(d['only_bitvavo'])}):\n  " +
                     ", ".join(f"<code>{b}</code>" for b in d["only_bitvavo"]))
    if not d["no_coin_id"] and not d["only_bitvavo"]:
        parts.append("\n✅ every base has at least one comparison")
    await update.message.reply_text("\n".join(parts), parse_mode=ParseMode.HTML)


async def cmd_hunt_profit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id) or _HUNTER is None:
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text(
            f"Min profit filter: <b>${_HUNTER.min_profit_usd:,.2f}</b>\n"
            "Usage: <code>/hunt_profit 50</code>  (drops alerts whose executable"
            " profit is under $50)",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        v = float(args[0].replace(",", ".").lstrip("$"))
        if v < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Bad number. e.g. /hunt_profit 50")
        return
    _HUNTER.set_min_profit(v)
    await update.message.reply_text(
        f"✅ Min executable profit set to <b>${v:,.2f}</b>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_hunt_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id) or _HUNTER is None:
        return
    # ensure chat is registered
    _HUNTER.subscribe(update.effective_chat.id)
    text = (
        f"<b>Hunter status</b>\n"
        f"  Threshold: <b>{_HUNTER.threshold:.2f}%</b>\n"
        f"  Min profit: <b>${_HUNTER.min_profit_usd:,.2f}</b>\n"
        f"  Cycle: {_HUNTER.cycle_sec:.0f}s  ·  cooldown: {_HUNTER.cooldown:.0f}s\n"
        f"  Bitvavo bases: {len(_HUNTER.bases)}\n"
        f"  DS pools cached: {sum(1 for v in _HUNTER.pool_cache.values() if v)}"
        f" / {len(_HUNTER.pool_cache)}\n"
        f"  Alert recipients: {len(_HUNTER.subs)}\n"
        f"  Last cycle: {_HUNTER.last_cycle_summary or '(pending — warmup running)'}"
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


def _bitvavo_networks_lines(base: str) -> list[str]:
    inst = cex._get("bitvavo")
    info = (inst.currencies or {}).get(base) or {}
    out = []
    for net, nd in (info.get("networks") or {}).items():
        ni = nd.get("info") or {}
        dep = "✅" if ni.get("depositStatus") == "OK" else "❌"
        wd = "✅" if ni.get("withdrawalStatus") == "OK" else "❌"
        fee = ni.get("withdrawalFee")
        extra = f"  wd fee {fee}" if fee else ""
        out.append(f"  · {net}: dep {dep}  wd {wd}{extra}")
    return out


def make_hunter_sender(app: Application):
    """One alert per Bitvavo base — lists Bitvavo + every matched target
    (CEX and DEX) with prices and per-target spread. Buy/Sell buttons
    point to Bitvavo vs the top-spread target."""
    async def send(a: dict, subs: list[int]):
        base = a["base"]
        bpx = a["bitvavo_price"]
        entries = a["entries"]                                     # sorted, top first

        eur_rate = cex.get_fx_rate("EUR")
        eur_price = bpx / eur_rate if eur_rate else None
        eur_part = f"  <i>(€{_fmt_price(eur_price).lstrip('$')})</i>" if eur_price else ""

        # top target — for headline direction + buttons
        top = entries[0]
        top_price = top["price"]
        if top["kind"] == "cex":
            top_label = cex.pretty(top["eid"])
            top_url = cex.trading_url(top["eid"], top["symbol"])
        else:
            top_label = f"{top['chain'].upper()} {top['dex_id']}"
            top_url = top["url"] or "https://dexscreener.com"

        buy_bv = bpx < top_price
        direction = f"Buy Bitvavo → Sell {top_label}" if buy_bv \
            else f"Buy {top_label} → Sell Bitvavo"

        # Two-sided cross-match against the top CEX target.
        # If the books don't cross → arb window already closed → skip alert entirely.
        size_block = ""
        skip_send = False
        if top["kind"] == "cex":
            bv_sym = f"{base}/EUR"
            if buy_bv:
                buy_eid, buy_sym = "bitvavo", bv_sym
                sell_eid, sell_sym = top["eid"], top["symbol"]
                buy_label, sell_label = "Bitvavo", top_label
            else:
                buy_eid, buy_sym = top["eid"], top["symbol"]
                sell_eid, sell_sym = "bitvavo", bv_sym
                buy_label, sell_label = top_label, "Bitvavo"
            try:
                # retry once on transient book-fetch failures
                s = await sizing.cross_match(buy_eid, buy_sym, sell_eid, sell_sym)
                if s is None:
                    s = await sizing.cross_match(buy_eid, buy_sym, sell_eid, sell_sym)
                if s and s.get("crossed"):
                    min_p = _HUNTER.min_profit_usd if _HUNTER else 0.0
                    if s["profit_usd"] < min_p:
                        skip_send = True                             # profit too small
                    def _p(x):                                    # native-quote formatter
                        if x >= 1: return f"{x:,.4f}"
                        if x >= 0.001: return f"{x:.6f}".rstrip("0").rstrip(".")
                        return _fmt_price(x).lstrip("$")

                    def _annot(price_native: float, quote: str) -> tuple[str, str]:
                        """Return (sign, extra) where extra is USDT annotation for EUR."""
                        if quote == "EUR":
                            rate = cex.get_fx_rate("EUR")
                            usdt = price_native * rate if rate else None
                            extra = f"  <i>(≈${_p(usdt)} USDT)</i>" if usdt else ""
                            return "€", extra
                        if quote in ("USDT", "USDC", "USD"):
                            return "$", ""
                        return "", ""

                    bs, buy_extra = _annot(s["last_buy_native"], s["buy_quote"])
                    ss, sell_extra = _annot(s["last_sell_native"], s["sell_quote"])
                    size_block = (
                        f"\n\n💰 <b>Executable: ${s['notional_usd']:,.0f}</b>"
                        f" (~{s['qty']:,.4f} {base}) · profit"
                        f" <b>${s['profit_usd']:,.2f}</b>"
                        f" ({s['eff_spread_pct']:.2f}%)\n"
                        f"  Buy {html.escape(buy_label)} asks up to"
                        f" <b>{bs}{_p(s['last_buy_native'])}</b> {html.escape(s['buy_quote'])}"
                        f"{buy_extra}\n"
                        f"  Sell {html.escape(sell_label)} bids down to"
                        f" <b>{ss}{_p(s['last_sell_native'])}</b> {html.escape(s['sell_quote'])}"
                        f"{sell_extra}"
                    )
                elif s and not s.get("crossed"):
                    skip_send = True
                else:                                              # fetch failed both tries
                    size_block = (
                        f"\n\n⚠️ <i>Sizing unavailable — couldn't fetch order books"
                        f" ({buy_eid} or {sell_eid} timed out via proxy).</i>"
                    )
                    log.warning("sizing: no data for %s (%s/%s vs %s/%s)",
                                base, buy_eid, buy_sym, sell_eid, sell_sym)
            except Exception as ex:
                log.warning("sizing err for %s: %s", base, ex)
                size_block = f"\n\n⚠️ <i>Sizing failed: {html.escape(str(ex)[:80])}</i>"

        # Inline network chips (dep/wd + full contract) under each exchange line
        def _net_chips(eid: str) -> list[str]:
            out = []
            for n in cex.network_info(eid, base):
                dep = "✅" if n["deposit"] else ("❌" if n["deposit"] is False else "?")
                wd = "✅" if n["withdraw"] else ("❌" if n["withdraw"] is False else "?")
                chip = f"      · {html.escape(n['network'])}: dep {dep}  wd {wd}"
                if n.get("fee"):
                    chip += f"  fee {n['fee']}"
                if n.get("contract"):
                    chip += f'\n         <code>{html.escape(n["contract"])}</code>'
                out.append(chip)
            return out

        def _cex_line(e: dict) -> str:
            url = cex.trading_url(e["eid"], e["symbol"])
            sign = "+" if e["price"] > bpx else "−" if e["price"] < bpx else ""
            return (f'  <b><a href="{html.escape(url)}">{cex.pretty(e["eid"])}</a></b> '
                    f'<code>{html.escape(e["symbol"])}</code>: {_fmt_price(e["price"])}  '
                    f'({sign}{e["spread"]:.2f}%)')

        bitvavo_row = f"  <b>Bitvavo</b> <code>{base}/EUR</code>: {_fmt_price(bpx)}{eur_part}"
        bitvavo_nets = _net_chips("bitvavo")

        lines = [
            f"🎯 <b>{base}</b>  ·  <b>{a['max_spread']:.2f}%</b>",
            direction,
            "",
        ]
        # Order rows: BUY side first, SELL side second
        if top["kind"] == "cex":
            top_row = _cex_line(top)
            top_nets = _net_chips(top["eid"])
            if buy_bv:                                         # buy Bitvavo, sell target
                lines.append(bitvavo_row); lines.extend(bitvavo_nets)
                lines.append(top_row);      lines.extend(top_nets)
            else:                                              # buy target, sell Bitvavo
                lines.append(top_row);      lines.extend(top_nets)
                lines.append(bitvavo_row); lines.extend(bitvavo_nets)
        else:
            lines.append(bitvavo_row); lines.extend(bitvavo_nets)

        # Remaining exchanges (excluding the top one) → Other CEX
        other_cex_entries = [e for e in entries
                             if e["kind"] == "cex" and e is not top]
        dex_entries = [e for e in entries if e["kind"] == "dex"]

        if other_cex_entries:
            lines.append("\n<b>Other CEX</b>")
            for e in other_cex_entries:
                url = cex.trading_url(e["eid"], e["symbol"])
                sign = "+" if e["price"] > bpx else "−" if e["price"] < bpx else ""
                lines.append(
                    f'  · <a href="{html.escape(url)}">{cex.pretty(e["eid"])}</a> '
                    f'<code>{html.escape(e["symbol"])}</code>: {_fmt_price(e["price"])}  '
                    f'({sign}{e["spread"]:.2f}%)'
                )
                lines.extend(_net_chips(e["eid"]))
        if dex_entries:
            lines.append("\n<b>DEX</b>")
            for e in dex_entries:
                sign = "+" if e["price"] > bpx else "−"
                lines.append(
                    f'  · <a href="{html.escape(e["url"])}">{e["chain"].upper()} {html.escape(e["dex_id"])}</a>: '
                    f'{_fmt_price(e["price"])}  ({sign}{e["spread"]:.2f}%)  ·  liq ${e["liq"]:,.0f}'
                )

        if skip_send:
            return                                             # arb window closed → drop alert
        text = "\n".join(lines) + size_block
        bitvavo_url = cex.trading_url("bitvavo", f"{base}/EUR")
        if buy_bv:
            buy_url, sell_url = bitvavo_url, top_url
            buy_lbl, sell_lbl = "🟢 Buy Bitvavo", f"🔴 Sell {top_label}"
        else:
            buy_url, sell_url = top_url, bitvavo_url
            buy_lbl, sell_lbl = f"🟢 Buy {top_label}", "🔴 Sell Bitvavo"
        top_eid = top["eid"] if top["kind"] == "cex" else ""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(buy_lbl, url=buy_url),
             InlineKeyboardButton(sell_lbl, url=sell_url)],
            [InlineKeyboardButton("🚫 Blacklist",
                                  callback_data=f"blm:{base}:{top_eid}")],
        ])

        for chat_id in subs:
            try:
                await app.bot.send_message(
                    chat_id=chat_id, text=text, parse_mode=ParseMode.HTML,
                    reply_markup=kb, disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("hunter send to %s failed: %s", chat_id, e)
    return send


def register_hunter(hunter: Hunter):
    global _HUNTER
    _HUNTER = hunter


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
    app.add_handler(CommandHandler("hunt_pct", cmd_hunt_pct))
    app.add_handler(CommandHandler("hunt_profit", cmd_hunt_profit))
    app.add_handler(CommandHandler("hunt_status", cmd_hunt_status))
    app.add_handler(CommandHandler("untracked", cmd_untracked))
    app.add_handler(CommandHandler("blacklist", cmd_blacklist))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(CommandHandler("c", cmd_check))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CallbackQueryHandler(cb_blacklist_menu, pattern=r"^blm:"))
    app.add_handler(CallbackQueryHandler(cb_blacklist_apply,
                                         pattern=r"^(blex|blbase|blback):"))
    app.add_handler(add_conv)
    # pair-action buttons (pause/refresh/del/edit) — not scoped to conversation
    app.add_handler(CallbackQueryHandler(cb_button, pattern=r"^(pause|refresh|del|edit):"))
    # fallback for free-text (edit threshold triggered from inline)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_free_text))
    return app
