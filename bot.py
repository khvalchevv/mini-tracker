"""Telegram interface for the DS pool spread tracker.

Commands:
  /start        welcome + main menu
  /add          conversational: URL A -> URL B -> threshold %
  /list         all your tracked pairs, one card each, with inline buttons
  /cancel       abort a running /add

Inline buttons per pair:
  Pause / Resume, Edit threshold, Delete, Refresh
"""
import asyncio
import html
import logging
import os
import re
import time
from urllib.parse import urlparse

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton,
    ReplyKeyboardMarkup, Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters,
)

import blacklist
import stuck as stuck_mod
import cex
import chains
import dex as dex_mod
import executor
import fees as fees_mod
import keys as keys_mod
import ledger                    # module-level: the CEX-cycle path needs it
import sizing
import storage
from hunter import Hunter

# ⚡ Execute state: pending trade plans keyed by short id, awaiting user tap
_PENDING_PLANS: dict[str, dict] = {}
# Soft-suppress bases whose sizing repeatedly fails with "no book" —
# usually halted markets that spam skip logs every cycle. Each pair
# accumulates fail count; when it hits 3, we suppress for 1h.
_NOBOOK_FAILS: dict[tuple, int] = {}                       # (base, buy_eid, sell_eid) -> count
_NOBOOK_UNTIL: dict[tuple, float] = {}                     # (base, buy_eid, sell_eid) -> epoch until suppressed
_NOBOOK_MAX = 3
# Trade-fail auto-blacklist: 3+ consecutive session fails on same base → 4h ban
_TRADE_FAILS: dict[str, int] = {}                          # base -> consecutive fails
_TRADE_BAN_UNTIL: dict[str, float] = {}                    # base -> epoch until banned
_TRADE_BAN_MAX = 3
_TRADE_BAN_SEC = 4 * 3600
_NOBOOK_SUPPRESS_SEC = 3600.0
_AUTOEXEC_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "autoexec.state")


def _load_autoexec_state() -> bool:
    """Read persisted autoexec flag; default ON when file absent."""
    try:
        if os.path.exists(_AUTOEXEC_FILE):
            with open(_AUTOEXEC_FILE) as f:
                return f.read().strip() == "1"
    except Exception:
        pass
    return True


def _save_autoexec_state(v: bool):
    try:
        with open(_AUTOEXEC_FILE, "w") as f:
            f.write("1" if v else "0")
    except Exception as e:
        log.debug("autoexec persist: %s", e)


_AUTOEXEC = _load_autoexec_state()                    # survives restart

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

def _autoexec_button_label() -> str:
    """Dynamic label — reflects current autoexec state on the button itself
    so the user can see at a glance without pressing anything."""
    try:
        state = "⚡ УВІМК" if _AUTOEXEC else "⏸ ВИМК"
    except NameError:
        state = "?"
    return f"🔀 Автоекзек: {state}"


def build_main_menu() -> "ReplyKeyboardMarkup":
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("💰 Баланси"),          KeyboardButton("🔑 Ключі")],
            [KeyboardButton("📊 Стан"),             KeyboardButton("📜 Трейди")],
            [KeyboardButton("📈 Статистика")],
            [KeyboardButton("🛑 KILL"),             KeyboardButton("▶️ РЕЗЮМ")],
            [KeyboardButton(_autoexec_button_label())],
            [KeyboardButton("🔄 Ребаланс"),         KeyboardButton("🚫 Блеклист")],
            [KeyboardButton("🏷 Адреси"),           KeyboardButton("❔ Untracked")],
        ],
        resize_keyboard=True,
        input_field_placeholder="/c SYMBOL   або   /hunt_pct 3   /hunt_profit 20",
    )


# Placeholder — will be rebuilt via build_main_menu() where reply_markup used.
MAIN_MENU = None                                          # set after startup

# Мапа тексту кнопки → команда для диспатчу. Autoexec matches by prefix
# since its label carries the current state ("🔀 Автоекзек: ⚡ УВІМК").
_MENU_MAP = {
    "💰 Баланси":          "balances",
    "🔑 Ключі":            "keys",
    "📊 Стан":             "hunt_status",
    "📜 Трейди":           "trades",
    "📈 Статистика":       "stats",
    "🛑 KILL":             ("kill", []),
    "▶️ РЕЗЮМ":            ("kill", ["off"]),
    "🚫 Блеклист":         "blacklist",
    "❔ Untracked":        "untracked",
    "🔄 Ребаланс":         "rebalance",
    "🏷 Адреси":           "addresses",
}
_MENU_PREFIX_MAP = {                                       # prefix match
    "🔀 Автоекзек":        "autoexec",
}


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
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                    reply_markup=build_main_menu())


async def cb_menu_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handles taps on the persistent MAIN_MENU reply-keyboard."""
    if not update.message or not update.message.text:
        return
    label = update.message.text.strip()
    mapping = _MENU_MAP.get(label)
    if not mapping:
        for pfx, m in _MENU_PREFIX_MAP.items():
            if label.startswith(pfx):
                mapping = m; break
    if not mapping:
        return
    if isinstance(mapping, str):
        cmd_name, args = mapping, []
    else:
        cmd_name, args = mapping
    ctx.args = args
    dispatcher = {
        "balances":   cmd_balances,
        "keys":       cmd_keys,
        "hunt_status": cmd_hunt_status,
        "trades":     cmd_trades,
        "stats":      cmd_stats,
        "kill":       cmd_kill,
        "autoexec":   cmd_autoexec,
        "blacklist":  cmd_blacklist,
        "untracked":  cmd_untracked,
        "rebalance":  cmd_rebalance,
        "addresses":  cmd_addresses,
    }.get(cmd_name)
    if dispatcher:
        await dispatcher(update, ctx)


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
    elif action == "bl1h":
        # Manual 1h cooldown from an alert (or any button)
        base = rest.split(":", 1)[0]
        _hours = float(os.getenv("BLACKLIST_LOSS_HOURS", "1.0"))
        blacklist.ban_base_for(base, hours=_hours, reason="manual TG button")
        note = (f"🚫 <b>{base}</b> заблоковано на {_hours:g}г. "
                f"Прибрати: /blacklist")
    elif action == "blun":
        # Unblock timed cooldown for a base
        base = rest.split(":", 1)[0]
        was = blacklist.unban_timed(base)
        note = (f"✅ <b>{base}</b> знято з кулдауну." if was
                else f"ℹ️ <b>{base}</b> не був у кулдауні.")
    elif action == "blunperm":
        # Unblock permanent ban for a base
        base = rest.split(":", 1)[0]
        blacklist.unban_base(base)
        note = f"✅ <b>{base}</b> знято з перманентного бану."
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


def _fmt_dur_short(sec: float) -> str:
    sec = int(max(0, sec))
    h, s = divmod(sec, 3600)
    m, s = divmod(s, 60)
    if h: return f"{h}h{m:02d}m"
    if m: return f"{m}m{s:02d}s"
    return f"{s}s"


async def cmd_blacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/blacklist — show current bans + inline buttons to unblock."""
    if not _is_allowed(update.effective_user.id):
        return
    d = blacklist.snapshot()
    parts = ["<b>Blacklist</b>"]
    buttons: list[list[InlineKeyboardButton]] = []
    if d.get("timed"):
        parts.append("\n<b>⏳ Timed cooldowns</b>:")
        for base, rec in sorted(d["timed"].items(),
                                  key=lambda x: x[1]["remaining_sec"]):
            _rem = _fmt_dur_short(rec["remaining_sec"])
            _rsn = rec.get("reason") or ""
            parts.append(f"  · <code>{base}</code> — {_rem} left"
                         + (f" · <i>{_rsn}</i>" if _rsn else ""))
            buttons.append([InlineKeyboardButton(
                f"✅ Unblock {base} ({_rem})",
                callback_data=f"blun:{base}")])
    if d["bases"]:
        parts.append("\n<b>🔒 Permanently muted</b>:")
        for base in d["bases"]:
            parts.append(f"  · <code>{base}</code>")
            buttons.append([InlineKeyboardButton(
                f"✅ Unmute {base}",
                callback_data=f"blunperm:{base}")])
    if d["pairs"]:
        parts.append("\n<b>Per-exchange mutes</b>:")
        for base, eids in d["pairs"].items():
            parts.append(f"  · <code>{base}</code> — " +
                         ", ".join(cex.pretty(e) for e in eids))
    if not d.get("timed") and not d["bases"] and not d["pairs"]:
        parts.append("\n(порожньо)")
    parts.append("\n\nManual: <code>/mute SYMBOL [hours]</code> · "
                 "<code>/unban SYMBOL</code>")
    await update.message.reply_text(
        "\n".join(parts), parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
    )


async def cmd_mute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/mute SYMBOL [hours]  — manual timed cooldown on a token."""
    if not _is_allowed(update.effective_user.id):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text(
            "Usage: /mute SYMBOL [hours]  (default 1h)")
        return
    base = args[0].upper()
    try:
        hours = float(args[1]) if len(args) > 1 else 1.0
    except ValueError:
        hours = 1.0
    blacklist.ban_base_for(base, hours=hours, reason="manual /mute")
    await update.message.reply_text(
        f"🚫 <b>{base}</b> заблоковано на {hours:g}г. "
        f"Зняти: /unban {base} або /blacklist",
        parse_mode=ParseMode.HTML,
    )


async def cmd_positions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/positions — the ledger: everything the bot believes it still owns.

    Unlike /stuck (things a leg gave up on) this shows OPEN positions
    too — inventory mid-pipeline. A row lingering here after its session
    ended is the signal that something was dropped."""
    if not _is_allowed(update.effective_user.id):
        return
    import ledger
    rows = ledger.open_rows()
    if not rows:
        await update.message.reply_text(
            "✅ Ledger чистий — незакритих позицій немає.")
        return
    now = time.time()
    lines = [f"<b>Позиції в ledger</b> ({len(rows)}), "
             f"експозиція ~${ledger.exposure_usd():,.2f}:"]
    for r in sorted(rows, key=lambda x: x.get("created_ts") or 0):
        age = (now - (r.get("created_ts") or now)) / 60
        where = r.get("chain") or r.get("venue") or "?"
        mark = "🔴" if r["state"] == ledger.STUCK else "🟡"
        lines.append(
            f"{mark} <code>{r['base']}</code> {r['qty']:,.4f} "
            f"@ <b>{r['location']}:{where}</b> · ${r['cost_usd']:,.0f} · "
            f"{age:,.0f}хв")
        if r.get("next_action"):
            lines.append(f"     далі: {r['next_action']}")
        if r.get("note"):
            lines.append(f"     <i>{html.escape(str(r['note'])[:110])}</i>")
    lines.append("\n🟡 OPEN — у роботі · 🔴 STUCK — конвеєр здався")
    lines.append("Продати залишки: /stuck або /sweep")
    await update.message.reply_text("\n".join(lines),
                                      parse_mode=ParseMode.HTML)


async def cmd_sweep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/sweep — liquidate every stray asset (hot wallet → USDC via Kyber,
    CEX alts → EUR/USDT market). Skips bases with an active session and
    anything under SWEEP_MIN_USD. Use /sweep dry to only list them."""
    if not _is_allowed(update.effective_user.id):
        return
    args = [a.lower() for a in (ctx.args or [])]
    dry = "dry" in args
    import sweeper as _sw
    await update.message.reply_text(
        f"🧹 <b>Sweep</b> {'(dry-run)' if dry else ''} — "
        f"мін ${_sw.SWEEP_MIN_USD:.0f}, макс {_sw.SWEEP_MAX_PER_RUN} позицій…",
        parse_mode=ParseMode.HTML)
    chat_id = update.effective_chat.id

    async def _notify(text: str):
        try:
            await ctx.application.bot.send_message(chat_id, text,
                                                     parse_mode=ParseMode.HTML)
        except Exception:
            pass
    if dry:
        # Report only — no orders, no swaps.
        lines = ["<b>Знайдено для зачистки:</b>"]
        try:
            for chain in _sw.SWEEP_CHAINS:
                toks = await _sw._discover_hw_tokens(chain)
                usdc = (dex_mod.USDC_BY_CHAIN.get(chain) or "").lower()
                for t in toks:
                    if t["contract"].lower() == usdc:
                        continue
                    if (t.get("symbol") or "").upper() in _sw._STABLES:
                        continue
                    q = await dex_mod.usd_price(chain, t["contract"],
                                                  usd_notional=100.0)
                    px = (q or {}).get("price_sell_usd") or 0
                    qty = int(t["raw"]) / (10 ** int(t.get("decimals") or 18))
                    val = qty * px
                    if val >= _sw.SWEEP_MIN_USD:
                        lines.append(f"  · HW {chain}: "
                                     f"<code>{t.get('symbol') or t['contract'][:10]}</code> "
                                     f"{qty:.4f} ~${val:.2f}")
        except Exception as e:
            lines.append(f"  ⚠️ {type(e).__name__}: {str(e)[:100]}")
        if len(lines) == 1:
            lines.append("  (нічого вище порогу)")
        await update.message.reply_text("\n".join(lines),
                                          parse_mode=ParseMode.HTML)
        return
    try:
        res = await _sw.run_once(_notify)
    except Exception as e:
        log.warning("sweep cmd err: %s", e, exc_info=True)
        await update.message.reply_text(
            f"❌ sweep crash: {type(e).__name__}: {str(e)[:150]}")
        return
    n_hw, n_cex = len(res.get("hw") or []), len(res.get("cex") or [])
    got = sum(float(x.get("got_usdc") or x.get("got") or 0)
              for x in (res.get("hw") or []) + (res.get("cex") or []))
    await update.message.reply_text(
        f"🧹 <b>Sweep готово</b>: {n_hw} на HW, {n_cex} на біржах · "
        f"отримано ~${got:,.2f}",
        parse_mode=ParseMode.HTML)


async def cmd_stuck(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/stuck — list HW positions stuck after failed Kyber sells,
    with inline buttons to retry the swap."""
    if not _is_allowed(update.effective_user.id):
        return
    items = stuck_mod.list_all()
    if not items:
        await update.message.reply_text("✅ Немає застряглих позицій.")
        return
    lines = [f"<b>Застряглі позиції</b> ({len(items)}):"]
    buttons: list[list[InlineKeyboardButton]] = []
    now = time.time()
    for it in items[:20]:
        age_min = (now - it.get("created_ts", now)) / 60
        lines.append(
            f"  · <code>{it['base']}</code> {it['qty']:.4f} on "
            f"<b>{it.get('chain') or it.get('venue') or '?'}</b> · "
            f"~${it['paid_usd']:.2f} · {age_min:.0f}m ago"
        )
        buttons.append([
            InlineKeyboardButton(f"🔄 Retry sell {it['base']}",
                                  callback_data=f"stkr:{it['id']}"),
            InlineKeyboardButton(f"❌ Remove",
                                  callback_data=f"stkd:{it['id']}"),
        ])
    lines.append("\n<i>Retry — заново broadcast Kyber свапу на "
                 "поточну ціну. Remove — прибрати з списку "
                 "(токени залишаться на HW).</i>")
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
    )


async def cb_stuck_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _is_allowed(q.from_user.id):
        return
    action, _, item_id = q.data.partition(":")
    it = stuck_mod.get(item_id)
    if not it:
        await q.message.reply_text("ℹ️ Позиція вже знята з списку.")
        return
    if action == "stkd":
        stuck_mod.remove(item_id)
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception: pass
        await q.message.reply_text(
            f"✅ {it['base']} прибрано з /stuck.",
            parse_mode=ParseMode.HTML,
        )
        return
    if action != "stkr":
        return
    # Retry sell — Kyber swap current HW balance → USDC
    await q.message.reply_text(
        f"🔄 Retry {it['base']}: fetch balance + Kyber quote…",
        parse_mode=ParseMode.HTML,
    )
    try:
        import dex as _dex
        bal_wei = await _dex.wallet_token_balance(it["chain"], it["contract"])
        if not bal_wei or bal_wei < 100:
            await q.message.reply_text(
                f"⚠️ HW {it['chain']} балансу {it['base']} = 0 — вже продано? "
                f"/stuck щоб прибрати запис.")
            return
        # decimals lookup — cached in dex._TOKEN_DECIMALS
        dec = _dex._TOKEN_DECIMALS.get((it["chain"], it["contract"].lower())) or 18
        try:
            from web3 import Web3 as _W3
            _w3 = _dex._get_w3(it["chain"])
            _abi = [{"inputs": [], "name": "decimals",
                      "outputs": [{"name": "", "type": "uint8"}],
                      "stateMutability": "view", "type": "function"}]
            _c = _w3.eth.contract(
                address=_W3.to_checksum_address(it["contract"]), abi=_abi)
            dec = int(_c.functions.decimals().call())
            _dex._TOKEN_DECIMALS[(it["chain"], it["contract"].lower())] = dec
        except Exception: pass
        qty = bal_wei / (10 ** dec)
        # Fresh Kyber quote for sizing (also verifies route exists)
        quote = await _dex.usd_price(it["chain"], it["contract"],
                                       usd_notional=100.0)
        sell_px = (quote or {}).get("price_sell_usd", 0)
        if sell_px <= 0:
            await q.message.reply_text(
                f"❌ Kyber зараз не квотує {it['base']} — спробуй пізніше.")
            return
        est_usd = qty * sell_px
        usdc = _dex.USDC_BY_CHAIN.get(it["chain"])
        if not usdc:
            await q.message.reply_text(
                f"❌ USDC не заданий для {it['chain']} — не можу продати.")
            return
        await q.message.reply_text(
            f"🌊 Swapping {qty:.4f} {it['base']} → USDC "
            f"(~${est_usd:.2f} @ ${sell_px:.6g})…",
            parse_mode=ParseMode.HTML,
        )
        # Temporarily bump tx cap for the retry (position may exceed default)
        _old_cap = os.getenv("DEX_MAX_TX_USD", "1000")
        try:
            if est_usd > float(_old_cap):
                os.environ["DEX_MAX_TX_USD"] = f"{est_usd * 1.1:.0f}"
            res = await _dex.swap(it["chain"], it["contract"], usdc,
                                   int(bal_wei), usd_estimate=est_usd,
                                   slippage_bps=200)
        finally:
            os.environ["DEX_MAX_TX_USD"] = _old_cap
        if res.get("ok"):
            got = int(res.get("amount_out_wei", 0)) / 1e6  # USDC 6 dec
            realized_pnl = got - it["paid_usd"]
            stuck_mod.remove(item_id)
            await q.message.reply_text(
                f"✅ <b>{it['base']}</b> продано → {got:.2f} USDC · "
                f"realized <b>${realized_pnl:+.2f}</b> vs "
                f"basis ${it['paid_usd']:.2f}\n"
                f"tx: <code>{res.get('tx_hash', '?')[:20]}…</code>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await q.message.reply_text(
                f"❌ Retry fail: {res.get('error', '?')[:200]}\n"
                f"Позиція лишається в /stuck.")
    except Exception as e:
        log.warning("stuck retry err: %s", e, exc_info=True)
        await q.message.reply_text(f"❌ crash: {type(e).__name__}: {str(e)[:120]}")


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
        _timed_removed = blacklist.unban_timed(base)
        _note = (f"✅ Unmuted <b>{base}</b> everywhere"
                 + (" + знято кулдаун" if _timed_removed else "") + ".")
        await update.message.reply_text(_note, parse_mode=ParseMode.HTML)


async def _run_dex_bitvavo_cycle(app, plan, chat_ids: list[int]) -> None:
    """Full Bitvavo↔DEX auto-cycle. Composes existing CEX phases with
    Kyber swap on HW. Two directions:

    A. buy Bitvavo, sell DEX:
       1. BUY alt on Bitvavo (limit FOK)
       2. Withdraw alt Bitvavo → HW, wait on-chain arrival
       3. Kyber swap alt → USDC on HW (USDC = profit)

    B. buy DEX, sell Bitvavo:
       1. Kyber swap USDC → alt on HW (needs USDC baseline)
       2. Send alt HW → Bitvavo deposit address
       3. Wait Bitvavo credit
       4. SELL alt on Bitvavo (limit IOC multi-pass)
    """
    ex = executor.instance()
    import ledger
    import secrets
    run_id = secrets.token_hex(3).upper()
    started = time.time()
    # Ledger rows this cycle opens. The finally block settles them:
    # CLOSED when the coin is gone, STUCK when it isn't.
    _ledger_ids: list[str] = []
    receipt = executor.TradeReceipt(
        trade_id=plan.trade_id, base=plan.base,
        buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=ex.mode)
    sess = executor.InteractiveSession(plan=plan, receipt=receipt)
    # PERSIST — so a bot restart mid-cycle can resume via _SESSIONS
    # (was totally stateless, funds got stranded on HW with no path back).
    _SESSIONS[run_id] = {"plan": plan, "session": sess,
                          "chat_ids": list(chat_ids),
                          "kind": "dex_cycle"}
    try:
        _persist_sessions()
    except Exception as _e:
        log.debug("persist dex cycle sess: %s", _e)

    def _fmt_dur(sec): m = int(sec // 60); s = int(sec % 60); return f"{m}m{s:02d}s" if m else f"{s}s"

    header = (f"🌊 <b>DEX cycle #{run_id}</b> — {plan.base}\n"
              f"  {plan.buy_eid} → {plan.sell_eid} · chain {plan.chain} · "
              f"${plan.notional_usd:,.2f}\n"
              f"  очік чист: ${plan.net_profit_usd:+.2f}")
    msg_ids: dict[int, int] = {}
    for cid in chat_ids:
        try:
            m = await app.bot.send_message(cid, header, parse_mode=ParseMode.HTML)
            msg_ids[cid] = m.message_id
        except Exception: pass

    async def _post(text: str):
        for cid in chat_ids:
            try: await app.bot.send_message(cid, text, parse_mode=ParseMode.HTML,
                                            disable_web_page_preview=True,
                                            reply_to_message_id=msg_ids.get(cid),
                                            allow_sending_without_reply=True)
            except Exception: pass

    # Bitvavo instance (both directions need it)
    kd = keys_mod.load_keys()
    bv = cex.get_private("bitvavo", kd["bitvavo"])

    try:
        if plan.buy_eid == "bitvavo":
            # ─── Direction A: BUY Bitvavo → WD → HW → Kyber sell ──────────
            await _post("💵 <b>BUY</b> Bitvavo…")
            ok = await ex.phase_buy(sess)
            if not ok:
                await _post(f"❌ BUY fail: {sess.receipt.error or '?'}")
                return
            await _post(f"✅ BUY: {sess.filled_qty:.4f} {plan.base} @ {plan.buy_limit:.6g}")
            # We now own coin on Bitvavo. Record it BEFORE moving it —
            # everything from here to the Kyber sell has historically
            # been where positions went missing.
            try:
                _bv_fx0 = cex.get_fx_rate("EUR") or 1.16
                # Cost basis MUST come from the actual fill, not
                # plan.buy_limit: on a DEX-cycle plan that field holds
                # the Kyber price in USD, not the Bitvavo price in EUR.
                # Using it inflated IQ to $1186 for a ~$995 buy and ACX
                # to $256 for ~$10 — every PnL downstream was wrong.
                _fill_px = 0.0
                try:
                    _bo = getattr(sess.receipt, "buy_order", None) or {}
                    _fill_px = float(_bo.get("average")
                                     or _bo.get("price") or 0)
                except Exception:
                    pass
                if _fill_px > 0:
                    _cost0 = float(sess.filled_qty or 0) * _fill_px * _bv_fx0
                else:
                    # No fill price on the order — fall back to the live
                    # book rather than the USD-denominated buy_limit.
                    _cost0 = float(plan.notional_usd or 0)
                    log.warning("ledger: no fill price for %s, using "
                                "notional $%.2f as cost basis",
                                plan.base, _cost0)
                _lid = ledger.open_position(
                    plan.base, qty=float(sess.filled_qty or 0),
                    cost_usd=_cost0, location=ledger.LOC_CEX,
                    venue="bitvavo", chain=plan.chain,
                    contract=plan.base_contract,
                    next_action="withdraw_to_hw", session_id=run_id,
                    note="DEX cycle direction A")
                _ledger_ids.append(_lid)
            except Exception as _e:
                log.warning("ledger open (dirA buy): %s", _e)
            await _post("📤 <b>WITHDRAW</b> Bitvavo → HW…")
            # phase_withdraw_and_wait does WD + wait HW + send + wait dest;
            # for DEX we need only first half. Skip dest by pre-marking
            # deposit_credited=True inside a wrapper — easier: just call
            # withdraw + poll HW ourselves.
            hot_addr = dex_mod.HOT_WALLET_ADDRESS
            # WD
            try:
                wd = await cex.withdraw_robust(bv, plan.base, sess.filled_qty,
                                                 hot_addr, None,
                                                 plan.src_network or "ETH")
                # Got out — the venue is healthy, drop any stale block.
                try:
                    import routing as _rt_ok
                    _rt_ok.clear_wd_block("bitvavo")
                except Exception:
                    pass
            except Exception as _wd_e:
                import routing as _rt_wd
                if not _rt_wd.is_permanent_wd_error(str(_wd_e)):
                    raise
                # Bitvavo will not release the coin this run — daily cap,
                # frozen asset, address not whitelisted. The alt is on
                # the exchange with no route to the DEX leg, so sell it
                # straight back instead of parking it as a stuck row the
                # user has to spot and unwind by hand.
                _wd_msg = str(_wd_e)[:160]
                # Remember at the VENUE level so the next run does not
                # rediscover this by buying into it again.
                _rt_wd.note_wd_blocked("bitvavo", _wd_msg)
                log.warning("dex cycle %s: withdrawal permanently blocked "
                            "(%s) — unwinding on Bitvavo", plan.base, _wd_msg)
                await _post(f"⛔ вивід заблоковано: {html.escape(_wd_msg)}\n"
                            f"↩️ продаю {plan.base} назад на Bitvavo")
                _unwound_usd = 0.0
                try:
                    # Cancel BEFORE reading the balance: a resting order
                    # parks its size in `used`, so `free` reads zero and
                    # the unwind wrongly concludes there is nothing to
                    # sell. That is how 18,170 ACX stayed put at 03:26.
                    try:
                        _open = await bv.fetch_open_orders(plan.buy_sym)
                        for _o in _open:
                            await cex.cancel_order_robust(bv, _o["id"],
                                                           plan.buy_sym)
                        if _open:
                            log.warning("dex cycle %s: cancelled %d resting "
                                        "order(s) before unwind",
                                        plan.base, len(_open))
                            await asyncio.sleep(1)
                    except Exception as _ce:
                        log.warning("dex cycle %s: unwind cancel: %s",
                                    plan.base, _ce)
                    _bal = await bv.fetch_balance()
                    _free = float((_bal.get("free") or {}).get(plan.base) or 0)
                    _total = float((_bal.get("total") or {}).get(plan.base) or 0)
                    _q = _free
                    # Cap at what this run bought — the venue balance can
                    # hold residue from earlier runs we must not touch.
                    _want = float(sess.filled_qty or 0)
                    if _want > 0:
                        _q = min(_q, _want)
                    if _q > 0:
                        _so = await cex.place_order(bv, plan.buy_sym,
                                                     "market", "sell", _q)
                        try:
                            _so = await bv.fetch_order(_so["id"], plan.buy_sym)
                        except Exception:
                            pass
                        _unwound_usd = (float(_so.get("cost") or 0)
                                        * (cex.get_fx_rate("EUR") or 1.16))
                        await _post(f"✅ відкат: {_q:,.4f} {plan.base} → "
                                    f"${_unwound_usd:,.2f}")
                    else:
                        log.error("dex cycle %s: unwind found nothing "
                                  "sellable — free=%.6f total=%.6f wanted=%.6f",
                                  plan.base, _free, _total, _want)
                        await _post(f"⚠️ відкат: вільних {plan.base} на Bitvavo "
                                    f"нема (free={_free:,.4f} / total={_total:,.4f})")
                except Exception as _ue:
                    log.error("dex cycle %s: unwind FAILED: %s", plan.base, _ue)
                    await _post(f"❌ відкат не вдався: {html.escape(str(_ue)[:120])}")
                for _lid in _ledger_ids:
                    try:
                        if _unwound_usd > 0:
                            ledger.close(_lid, realized_usd=_unwound_usd,
                                         note=f"unwound on bitvavo: {_wd_msg[:80]}")
                        else:
                            ledger.mark_stuck(_lid, f"wd blocked: {_wd_msg[:80]}")
                    except Exception:
                        pass
                return
            # The coin has left Bitvavo. Move the ledger row with it —
            # a row still saying LOC_CEX makes reconcile_all read an
            # empty Bitvavo balance and close a position that is
            # actually mid-flight or already on HW. That is exactly how
            # the FTT position was lost.
            for _lid in _ledger_ids:
                try:
                    ledger.update(_lid, location=ledger.LOC_TRANSIT,
                                    next_action="await_hw_arrival",
                                    refs={"wd_id": str(wd.get("id")
                                                       or wd.get("txid") or "")})
                except Exception:
                    pass
            await _post(f"🚀 WD id={wd.get('id') or wd.get('txid') or '?'}")
            # Wait HW arrival via WS. Pass expected_qty to chain-any
            # fallback so it doesn't bind to a random MEV-dust/refund
            # transfer (empty expected_qty in prior version was the bug).
            import walletfeed, time as _t
            wd_ts = _t.time()
            expected = float(sess.filled_qty or plan.qty or 0)
            arrival = await walletfeed.wait_for_transfer(
                plan.chain, plan.base_contract or "", timeout_sec=45 * 60,
                since_ts=wd_ts) if getattr(plan, "base_contract", None) else \
                walletfeed.find_any_since(plan.chain, wd_ts,
                                            expected_qty=expected)
            # fallback: chain-any if contract-specific failed — with qty guard
            if not arrival:
                arrival = walletfeed.find_any_since(plan.chain, wd_ts,
                                                     expected_qty=expected)
            if not arrival:
                await _post("❌ HW не дочекались токенів за 45хв")
                return
            # Use decimals from the token contract, not hardcoded 18.
            arrival_dec = 18
            if getattr(plan, "base_contract", None):
                try:
                    from web3 import Web3 as _W3
                    _w3 = dex_mod._get_w3(plan.chain)
                    if _w3:
                        _abi = [{"inputs": [], "name": "decimals",
                                  "outputs": [{"name": "", "type": "uint8"}],
                                  "stateMutability": "view", "type": "function"}]
                        _c = _w3.eth.contract(
                            address=_W3.to_checksum_address(plan.base_contract),
                            abi=_abi)
                        arrival_dec = int(_c.functions.decimals().call())
                except Exception as _e:
                    log.debug("decimals lookup %s: %s", plan.base_contract, _e)
            arrived_qty = arrival["amount"] / (10 ** arrival_dec)
            # BUY-leg cost in USD (what we paid on Bitvavo). Prefer
            # actual order avg (native EUR) × fx; fall back to buy_limit
            # (USD-denominated for DEX-cycle plans per executor comment).
            _bv_fx = cex.get_fx_rate("EUR") or 1.17
            _bv_avg_eur = 0.0
            try:
                _bo = getattr(sess.receipt, "buy_order", None) or {}
                _bv_avg_eur = float(_bo.get("average")
                                     or _bo.get("price") or 0)
            except Exception: pass
            if _bv_avg_eur > 0:
                _buy_px_usd = _bv_avg_eur * _bv_fx
            else:
                # plan.buy_limit is USD for DEX-cycle plans (from Kyber)
                _buy_px_usd = float(plan.buy_limit or 0)
            _paid_usd = arrived_qty * _buy_px_usd
            for _lid in _ledger_ids:
                try:
                    ledger.update(_lid, location=ledger.LOC_HW,
                                    qty=arrived_qty,
                                    next_action="sell_kyber")
                except Exception:
                    pass
            await _post(f"✅ HW arrival: {arrived_qty:.4f} {plan.base} "
                        f"(BUY ~${_paid_usd:.2f} @ ${_buy_px_usd:.6g})")
            # Kyber swap alt → USDC
            plan.qty = arrived_qty
            sess.filled_qty = arrived_qty
            await _post(f"🌊 Kyber swap {arrived_qty:.4f} {plan.base} "
                        f"(~${_paid_usd:.2f} basis) → USDC…")
            r = await ex._run_dex(plan)
            state = r.state
            if state == "DONE":
                pnl = r.net_pnl_usd or 0.0
                success = True
                # Coin is back in USDC. The reconciler in `finally` will
                # verify that against the chain before this actually
                # sticks — this is the optimistic half.
                for _lid in _ledger_ids:
                    try:
                        ledger.update(_lid, location=ledger.LOC_HW,
                                        next_action="", realized_usd=pnl)
                    except Exception:
                        pass
                await _post(f"✅ DEX swap: DONE · net ${pnl:+.2f}")
            else:
                # Swap failed → tokens still on HW. REAL "cash loss" so far
                # is only gas we burned trying to swap; tokens themselves
                # keep their market value. Best proxy: re-quote Kyber at
                # arrived_qty → what we'd get NOW if we sold. Report:
                #   unrealized_loss = paid - current_sellable
                success = False
                err_short = (r.error or "?")[:120]
                _sellable_usd = None
                try:
                    _kq = await dex_mod.usd_price(
                        plan.chain, plan.base_contract,
                        usd_notional=_paid_usd,
                        reference_price_usd=_buy_px_usd,
                    )
                    _sell_px = (_kq or {}).get("price_sell_usd") or 0
                    if _sell_px > 0:
                        _sellable_usd = arrived_qty * _sell_px
                except Exception as _e:
                    log.debug("post-fail re-quote %s: %s", plan.base, _e)
                if _sellable_usd is not None:
                    # Unrealized loss = what we paid vs what Kyber offers now
                    pnl = _sellable_usd - _paid_usd
                    _reason = (f"paid ${_paid_usd:.2f}, зараз Kyber дає "
                               f"${_sellable_usd:.2f}")
                else:
                    # Fall back to gas-only estimate (~$1-2 typical)
                    pnl = -2.0
                    _reason = f"paid ${_paid_usd:.2f}, current price N/A"
                # Persist stuck position so bot doesn't "forget" — user
                # can list via /stuck and retry via button. Auto-remove
                # if we later liquidate via a manual sell.
                try:
                    stuck_mod.add(plan.base, plan.chain, plan.base_contract,
                                    qty=arrived_qty, paid_usd=_paid_usd,
                                    err=err_short)
                except Exception as _e:
                    log.debug("stuck record err: %s", _e)
                await _post(f"❌ DEX swap: {state} · токени на HW не продались "
                            f"({err_short})\n"
                            f"   💵 {_reason}\n"
                            f"   💰 нереалізований P&L: ${pnl:+.2f} "
                            f"(токени залишились, /stuck щоб побачити)")
        else:
            # ─── Direction B: Kyber buy → send → Bitvavo → SELL ───────────
            # 1. Kyber swap USDC → alt on HW
            await _post("🌊 <b>Kyber</b> USDC → " + plan.base + " on HW…")
            r = await ex._run_dex(plan)
            if r.state != "DONE":
                await _post(f"❌ Kyber swap fail: {r.error or '?'}")
                return
            # From here on we HOLD the alt on HW. Every downstream step
            # can `return` early (no deposit addr, send fail, credit
            # timeout, sell fail) — arm a guard so the finally block
            # records a stuck position instead of silently forgetting.
            # This is how 860 EURC (~$998) sat unnoticed on HW.
            _dirB_holding = {
                "base": plan.base,
                "chain": plan.chain,
                "contract": plan.base_contract,
                "paid_usd": float(plan.notional_usd or 0),
            }
            try:
                _lid = ledger.open_position(
                    plan.base, qty=float(plan.qty or 0),
                    cost_usd=float(plan.notional_usd or 0),
                    location=ledger.LOC_HW, chain=plan.chain,
                    contract=plan.base_contract,
                    next_action="send_to_bitvavo_and_sell",
                    session_id=run_id, note="DEX cycle direction B")
                _ledger_ids.append(_lid)
            except Exception as _e:
                log.warning("ledger open (dirB kyber buy): %s", _e)
            # After swap, HW has plan.qty of alt (approximately)
            # 2. Send alt to Bitvavo deposit
            dep_info = await cex.fetch_deposit_address_robust(
                bv, plan.base, "ETH")
            dep_addr = (dep_info or {}).get("address")
            if not dep_addr:
                await _post("❌ Bitvavo deposit address not found")
                return
            # Wait a moment for HW balance to reflect after swap
            await asyncio.sleep(3)
            hw_bal_wei = await dex_mod.wallet_token_balance(
                plan.chain, plan.base_contract) or 0
            if hw_bal_wei <= 0:
                await _post(f"❌ HW {plan.base} balance = 0 after Kyber")
                return
            # Fetch real decimals — hardcoding 18 gives 10^12x wrong size
            # for USDC/USDT (6) or 8-dec tokens (WBTC etc.)
            token_dec = 18
            try:
                from web3 import Web3 as _W3
                _w3 = dex_mod._get_w3(plan.chain)
                if _w3:
                    _abi = [{"inputs": [], "name": "decimals",
                              "outputs": [{"name": "", "type": "uint8"}],
                              "stateMutability": "view", "type": "function"}]
                    _c = _w3.eth.contract(
                        address=_W3.to_checksum_address(plan.base_contract),
                        abi=_abi)
                    token_dec = int(_c.functions.decimals().call())
            except Exception as _e:
                log.debug("dec lookup direction B %s: %s", plan.base_contract, _e)
            hw_bal_h = hw_bal_wei / (10 ** token_dec)
            await _post(f"🚀 Send {hw_bal_h:.4f} {plan.base} HW → Bitvavo…")
            send_res = await dex_mod.send_token(
                plan.chain, plan.base_contract, hw_bal_wei, dep_addr)
            if not send_res.get("ok"):
                await _post(f"❌ HW send: {send_res.get('error')}")
                return
            await _post(f"✅ tx: {dex_mod.tx_link(plan.chain, send_res['tx_hash'])}")
            # 3. Wait Bitvavo credit
            await _post("⏳ чекаю кредит на Bitvavo…")
            import time as _t
            deadline = _t.time() + 45 * 60
            base0 = float((await bv.fetch_balance()).get(plan.base, {}).get("free") or 0)
            credited_qty = 0
            while _t.time() < deadline:
                await asyncio.sleep(15)
                b = await bv.fetch_balance()
                cur = float((b.get(plan.base) or {}).get("free") or 0)
                if cur - base0 >= hw_bal_h * 0.85:
                    credited_qty = cur - base0
                    break
            if credited_qty <= 0:
                await _post("❌ Bitvavo credit timeout 45хв")
                return
            await _post(f"✅ Bitvavo credit: {credited_qty:.4f} {plan.base}")
            # 4. SELL on Bitvavo
            sess.filled_qty = credited_qty
            sess.deposit_credited = True
            await _post("💰 <b>SELL</b> on Bitvavo…")
            ok = await ex.phase_sell(sess)
            if not ok:
                await _post(f"❌ SELL fail: {sess.receipt.error or '?'}")
                return
            pnl = sess.receipt.net_pnl_usd or 0
            success = True
            # Record what we actually realised BEFORE the reconciler
            # closes the row. It closes with realized=None, which turned
            # a +$48.71 LAPTOP run into "realized $0.00, pnl -$1000.00"
            # — the ledger reported a loss on a winning trade.
            for _lid in _ledger_ids:
                try:
                    _row = ledger.get(_lid)
                    _cost = float((_row or {}).get("cost_usd") or 0)
                    ledger.update(_lid, realized_usd=_cost + float(pnl),
                                    next_action="")
                except Exception as _e:
                    log.debug("ledger realized (dirB): %s", _e)
            await _post(f"✅ SELL done · net ${pnl:+.2f}")
        elapsed = time.time() - started
        _ok = locals().get("success", False)
        if _ok:
            await _post(f"🏁 <b>DEX cycle DONE</b> · {_fmt_dur(elapsed)} · "
                        f"net ${(locals().get('pnl') or 0):+.2f}")
        else:
            _pnl_final = locals().get("pnl") or 0
            await _post(f"❌ <b>DEX cycle FAILED</b> · {_fmt_dur(elapsed)} · "
                        f"unrealized ${_pnl_final:+.2f} "
                        f"(токени залишились на HW)")
    except Exception as e:
        log.warning("dex cycle %s crashed: %s", plan.base, e, exc_info=True)
        await _post(f"💥 crash: {str(e)[:200]}")
    finally:
        # ── Inventory reconciliation — the backstop for EVERY exit ──
        # Direction A has three early returns and Direction B five, plus
        # crashes and cancellations. Rather than trusting each of them to
        # clean up, ask the chain and the exchanges what we actually hold
        # and settle the ledger against that.
        # NOTE: this runs AFTER the direction-B guard below. Ordering
        # matters — reconcile re-raises CancelledError (as it must), and
        # /kill cancels trade tasks, so putting it first meant the guard
        # and the blacklist block were skipped exactly when a cancelled
        # cycle was holding coin on the hot wallet.
        # Direction B guard: if we bought the alt on Kyber but never
        # completed the Bitvavo sell, the tokens are sitting on HW.
        # Every early `return` in that leg used to leave zero trace —
        # 860 EURC (~$998) went unnoticed for hours that way. Verify
        # against the live chain balance before recording, so a
        # completed cycle doesn't file a false stuck entry.
        try:
            _hold = locals().get("_dirB_holding")
            if _hold and not locals().get("success", False):
                _bal_wei = await dex_mod.wallet_token_balance(
                    _hold["chain"], _hold["contract"]) or 0
                if _bal_wei > 0:
                    _dec = 18
                    try:
                        from web3 import Web3 as _W3d
                        _w3d = dex_mod._get_w3(_hold["chain"])
                        _abid = [{"inputs": [], "name": "decimals",
                                   "outputs": [{"name": "", "type": "uint8"}],
                                   "stateMutability": "view",
                                   "type": "function"}]
                        _dec = int(_w3d.eth.contract(
                            address=_W3d.to_checksum_address(_hold["contract"]),
                            abi=_abid).functions.decimals().call())
                    except Exception:
                        pass
                    _qty = _bal_wei / (10 ** _dec)
                    stuck_mod.add(_hold["base"], _hold["chain"],
                                    _hold["contract"], qty=_qty,
                                    paid_usd=_hold["paid_usd"],
                                    err="Direction B: bought on Kyber, "
                                        "never reached Bitvavo sell")
                    for _cid in chat_ids:
                        try:
                            await app.bot.send_message(
                                _cid,
                                f"📦 <b>{_hold['base']}</b> {_qty:.4f} "
                                f"залишились на HW ({_hold['chain']}) — "
                                f"цикл не дійшов до продажу. "
                                f"Дивись /stuck",
                                parse_mode=ParseMode.HTML)
                        except Exception:
                            pass
        except Exception as _e:
            log.debug("dirB stuck guard err: %s", _e)
        # Auto-blacklist token for 1h if this cycle netted a loss.
        # `pnl` is set in both directions of the cycle after fills — if
        # the cycle crashed before any fill, `pnl` is undefined and we
        # skip (no realized loss to guard against). User can lift the
        # cooldown via the button on the loss notification or /blacklist.
        try:
            _final_pnl = locals().get("pnl", None)
            # Only blacklist on MATERIAL losses (below configurable
            # threshold). Transient Kyber routing errors, small gas
            # burns, or partially-recovered positions shouldn't lock
            # a token for 1h — tokens still on HW may be sellable
            # via a later re-quote or manually.
            _bl_min_loss = float(os.getenv("BLACKLIST_MIN_LOSS_USD", "-5.0"))
            if _final_pnl is not None and _final_pnl < _bl_min_loss:
                _hours = float(os.getenv("BLACKLIST_LOSS_HOURS", "1.0"))
                blacklist.ban_base_for(
                    plan.base, hours=_hours,
                    reason=f"DEX-cycle loss ${_final_pnl:+.2f} "
                           f"@ {time.strftime('%H:%M %d-%m')}",
                )
                _kb_unblock = InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        f"✅ Unblock {plan.base}",
                        callback_data=f"blun:{plan.base}"),
                ]])
                _msg = (f"🚫 <b>{plan.base}</b> заблоковано на {_hours:g}г "
                        f"(net ${_final_pnl:+.2f}). "
                        f"Кнопка нижче зніме кулдаун.")
                for _cid in chat_ids:
                    try:
                        await app.bot.send_message(
                            _cid, _msg, parse_mode=ParseMode.HTML,
                            reply_markup=_kb_unblock)
                    except Exception:
                        pass
        except Exception as _e:
            log.debug("dex-cycle blacklist-on-loss err: %s", _e)
        # ── Inventory reconciliation — LAST, so a CancelledError raised
        # here can't skip the guards above. Asks the chain and the
        # exchanges what we actually hold and settles the ledger rows
        # against that, whatever this code path believed happened.
        try:
            import reconcile as _rec
            await _rec.reconcile_session(
                plan, session_id=run_id, ledger_ids=_ledger_ids,
                succeeded=bool(locals().get("success", False)),
                notify=_post)
        except asyncio.CancelledError:
            log.error("dex-cycle %s: reconcile cancelled — ledger rows for "
                      "%s stay OPEN for the periodic pass",
                      run_id, plan.base)
        except Exception as _e:
            log.warning("dex-cycle reconcile: %s", _e, exc_info=True)
        # Cleanup persistence entry regardless of outcome
        _SESSIONS.pop(run_id, None)
        try: _persist_sessions()
        except Exception: pass


async def _run_auto_session(app: Application, plan_key: str, chat_ids: list[int],
                              resume: bool = False):
    """Full end-to-end automated pipeline for a single arb signal:
    BUY → WAIT for fill → WITHDRAW (with hot-wallet hop for EVM) →
    WAIT on-chain + dest deposit → SELL → single P&L summary.

    resume=True: `plan_key` is an EXISTING session_id in _SESSIONS
    (restored from disk after a restart). Plan + partial session state
    (filled_qty, filled_qty_wei, forward_tx, deposit_credited) are read
    from _SESSIONS and phases already completed are skipped."""
    import secrets, time as _time
    if resume:
        entry = _SESSIONS.get(plan_key)
        if not entry:
            return
        payload = {
            "alert": {"base": entry["plan"].base, "bitvavo_price": 0,
                       "max_spread": 0, "entries": [], "_key": (entry["plan"].base,)},
            "sizing": {"crossed": True, "notional_usd": entry["plan"].notional_usd,
                        "profit_usd": entry["plan"].expected_profit_usd or 0,
                        "avg_buy_usd": entry["plan"].buy_limit,
                        "last_buy_native": entry["plan"].buy_limit,
                        "last_sell_native": entry["plan"].sell_limit,
                        "qty": entry["plan"].qty},
        }
    else:
        payload = _PENDING_PLANS.pop(plan_key, None)
        if not payload:
            return
    ex = executor.instance()

    # Build plan (or reuse existing)
    if resume:
        plan = _SESSIONS[plan_key]["plan"]
    else:
        try:
            plan = await ex.plan(payload["alert"], payload["sizing"])
        except Exception as e:
            for cid in chat_ids:
                try: await app.bot.send_message(cid, f"❌ auto plan build failed: {e}")
                except Exception: pass
            return
        # Propagate alert timestamp — used by phase_buy latency tracing
        # to log "how long since alert detected"
        try: plan.alert_ts = payload.get("alert_ts") or time.time()
        except Exception: pass
        # Propagate testrun flag onto the plan — used to bypass post-buy
        # min-profit gate (a $3 test can never clear a $10 profit floor).
        if payload.get("is_testrun"):
            try: plan.is_testrun = True
            except Exception: pass
        # Don't open a position we already know we cannot move. This run
        # must withdraw the alt from `buy_eid`, and that venue refused a
        # withdrawal recently; buying now only repeats the discovery at
        # the cost of a round-trip spread — 20 times in 43 minutes on the
        # night of 2026-09-10. Direction B buys on the DEX and DEPOSITS
        # into the exchange, so it carries no block and keeps running.
        # This is a cooldown, not a blacklist: when it lapses the next
        # run probes the venue for real, and a successful withdrawal
        # clears it immediately.
        try:
            import routing as _rt_gate
            _left = _rt_gate.wd_block_remaining(plan.buy_eid)
            if _left > 0 and not getattr(plan, "is_testrun", False):
                log.warning("session skip %s: %s withdrawals blocked for "
                            "another %.0f min — not opening a position we "
                            "cannot move", plan.base, plan.buy_eid, _left / 60.0)
                for cid in chat_ids:
                    try:
                        await app.bot.send_message(
                            cid, f"⏸ {plan.base}: вивід з "
                                 f"{cex.pretty(plan.buy_eid)} заблокований — "
                                 f"чекаю ще {_left/60:.0f} хв, позицію не відкриваю")
                    except Exception:
                        pass
                return
        except Exception as _e:
            log.debug("wd-block gate: %s", _e)

    # DEX in play → full Bitvavo↔DEX cycle (composes CEX phases + Kyber)
    if not resume and (plan.buy_eid == "dex" or plan.sell_eid == "dex"):
        try:
            await _run_dex_bitvavo_cycle(app, plan, chat_ids)
        except Exception as e:
            log.warning("DEX cycle %s err: %s", plan.base, e, exc_info=True)
            for cid in chat_ids:
                try:
                    await app.bot.send_message(
                        cid, f"❌ DEX cycle {plan.base}: {str(e)[:200]}")
                except Exception: pass
        return

    # On resume: we've already committed funds — skip pre-flight checks
    # (wd/dep gate, precheck, active-base guard, trade-ban). Just pick
    # up where we left off.
    if resume:
        base_lock = executor._base_lock(plan.base)
        run_id = plan_key                                # reuse existing sid
        sess = _SESSIONS[plan_key]["session"]
        receipt = sess.receipt
    # LIVE wd/dep re-check for the SPECIFIC token — targeted fetch (no
    # full currencies reload). Exchanges disable coin withdrawals
    # mid-day; refuse to commit funds if either side is closed.
    if not resume and ex.mode != "dry" and plan.chain:
        try:
            from chains import canonical as _cnl
            target_chain = _cnl(plan.chain) or plan.chain.lower()
            kd = keys_mod.load_keys()
            block_reasons: list[str] = []
            for eid, want_wd, want_dep in (
                (plan.buy_eid, True, False),        # source must be able to WD
                (plan.sell_eid, False, True),        # dest must be able to DEPOSIT
            ):
                creds = kd.get(eid) or {}
                if not creds.get("apiKey"):
                    continue
                inst_live = cex.get_private(eid, creds)
                if eid == "binance":
                    try: await inst_live.load_time_difference()
                    except Exception: pass
                nets = await cex.fetch_wd_dep_live(eid, plan.base, inst_live)
                if not nets:
                    continue                          # can't verify → trust cache
                # Find our chain in the returned list
                ok = False
                for n in nets:
                    net_raw = n.get("network")
                    if not net_raw:
                        continue
                    if _cnl(net_raw) != target_chain:
                        continue
                    if want_wd and not n.get("withdraw"):
                        block_reasons.append(
                            f"{cex.pretty(eid)} закрив ВИВІД {plan.base}/{plan.chain}")
                    elif want_dep and not n.get("deposit"):
                        block_reasons.append(
                            f"{cex.pretty(eid)} закрив ДЕПОЗИТ {plan.base}/{plan.chain}")
                    else:
                        ok = True
                    break
                if not ok and not block_reasons:
                    block_reasons.append(
                        f"{cex.pretty(eid)} не має {plan.base} на {plan.chain} live")
            if block_reasons:
                log.info("auto-abort %s: %s", plan.base, "; ".join(block_reasons))
                for cid in chat_ids:
                    try:
                        await app.bot.send_message(
                            cid, f"⏭ АВТО-СКІП <b>{plan.base}</b> — " +
                                 "; ".join(block_reasons), parse_mode=ParseMode.HTML)
                    except Exception: pass
                return
        except Exception as e:
            log.warning("live wd/dep check err (%s): %s", plan.base, e)

    # Skip precheck / guards on resume (funds already committed)
    if resume:
        blockers = []
    else:
        blockers = ex.precheck_plan(plan)
    if blockers:
        # Non-EVM/Solana chains → silent skip. Spread alert already went
        # out; we just don't auto-execute. User asked: "автопрогін
        # ігноруємо на нон евм але спред нехай пікає".
        hop_only = all(("hop-only" in b or "немає hot wallet" in b or
                        "немає спільного чейну" in b) for b in blockers)
        if hop_only:
            log.info("auto-skip %s: non-EVM chain (silent) — %s",
                     plan.base, "; ".join(blockers))
            return
        for cid in chat_ids:
            try:
                await app.bot.send_message(
                    cid, f"⏭ АВТО-СКІП <b>{plan.base}</b> — " + "; ".join(blockers),
                    parse_mode=ParseMode.HTML,
                )
            except Exception: pass
        return

    if not resume:
        # Per-base in-flight guard: never fire a 2nd auto-run for the same
        # token while ANY prior session (active or paused-for-retry) exists.
        # SILENT skip — don't spam TG with "auto-skip" notes.
        active_bases = {s["plan"].base for s in _SESSIONS.values()
                        if s.get("plan")}
        if plan.base in active_bases:
            log.info("auto-skip %s: already in-flight", plan.base)
            return
        # Trade-ban gate — recently-failed base is muted for 4h
        if _TRADE_BAN_UNTIL.get(plan.base, 0) > time.time():
            remaining = int((_TRADE_BAN_UNTIL[plan.base] - time.time()) / 60)
            log.info("auto-skip %s: trade-banned %dm more (3+ consecutive fails)",
                     plan.base, remaining)
            return
        base_lock = executor._base_lock(plan.base)
        run_id = secrets.token_hex(3).upper()
        receipt = executor.TradeReceipt(
            trade_id=plan.trade_id, base=plan.base,
            buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=ex.mode,
        )
        sess = executor.InteractiveSession(plan=plan, receipt=receipt)
    started = _time.time()

    # Per-session skip flags — set by "Пропустити крок" button.
    # When a phase's name is in this set, its retry loop bails early.
    _SESSION_SKIP.setdefault(run_id, set())
    current_phase = [""]                                    # holder for skip-button target
    # ─── Clean stateful UI ───────────────────────────────────────────
    # 4 fixed-slot rows: BUY / WITHDRAW / SEND / SELL. Each has a status
    # (⏸ pending, ⏳ running, ✅ done, ❌ fail), an inline detail, and
    # a duration once done. NO free-form log spam — just the table.
    step_state: dict[str, dict] = {
        "BUY":      {"status": "⏸", "detail": "", "duration": None, "start_ts": None},
        "WITHDRAW": {"status": "⏸", "detail": "", "duration": None, "start_ts": None},
        "SEND":     {"status": "⏸", "detail": "", "duration": None, "start_ts": None},
        "RECEIVED": {"status": "⏸", "detail": "", "duration": None, "start_ts": None},
        "SELL":     {"status": "⏸", "detail": "", "duration": None, "start_ts": None},
    }
    last_note = ""                                        # single-line rolling note
    _note_holder = [last_note]

    def _fmt_dur(sec: float) -> str:
        m = int(sec // 60); s = int(sec % 60)
        return f"{m}m{s:02d}s" if m else f"{s}s"

    def _render() -> str:
        lines_out = [
            f"🔥 <b>АВТО-ПРОГІН #{run_id}</b> — <b>{plan.base}</b>",
            f"{cex.pretty(plan.buy_eid)} → {cex.pretty(plan.sell_eid)}  ·  "
            f"{plan.chain}  ·  ${plan.notional_usd:,.0f}",
            f"Очік чист: <b>${plan.net_profit_usd:,.2f}</b>  ·  🚀 "
            f"{_time.strftime('%H:%M:%S', _time.localtime(started))}",
            "─" * 20,
        ]
        label_map = {"BUY":      "💵 BUY      ",
                      "WITHDRAW": "📤 WITHDRAW ",
                      "SEND":     "🔀 SEND     ",
                      "RECEIVED": "📥 RECEIVED ",
                      "SELL":     "💰 SELL     "}
        for name in ("BUY", "WITHDRAW", "SEND", "RECEIVED", "SELL"):
            st = step_state[name]
            dur = f" ({_fmt_dur(st['duration'])})" if st["duration"] else (
                f" ({_fmt_dur(_time.time() - st['start_ts'])})"
                if st["start_ts"] and st["status"] == "⏳" else "")
            detail = f"  <i>{st['detail']}</i>" if st["detail"] else ""
            lines_out.append(f"{st['status']}  {label_map[name]}{detail}{dur}")
        if _note_holder[0]:
            lines_out.append(f"<i>{_note_holder[0]}</i>")
        return "\n".join(lines_out)

    def _kb():
        """Buttons pinned to the session message so user can control it."""
        rows = [
            [InlineKeyboardButton("⏭ Пропустити крок", callback_data=f"sskp:{run_id}"),
             InlineKeyboardButton("🚫 Стоп", callback_data=f"pcx:{run_id}")],
        ]
        return InlineKeyboardMarkup(rows)

    # Post initial pinned message
    msg_ids: dict[int, int] = {}
    for cid in chat_ids:
        try:
            m = await app.bot.send_message(cid, _render(),
                                           parse_mode=ParseMode.HTML,
                                           disable_web_page_preview=True,
                                           reply_markup=_kb())
            msg_ids[cid] = m.message_id
        except Exception as e:
            log.warning("auto session post to %s: %s", cid, e)

    async def _push():
        """Fire-and-forget master-card edits so executor NEVER blocks on
        TG round-trip (was adding 1-3s per status_cb call → 10s+ UI lag).
        Each edit_message_text launched as its own task."""
        text = _render()
        for cid, mid in msg_ids.items():
            async def _do(cid=cid, mid=mid, text=text):
                try:
                    await app.bot.edit_message_text(
                        text=text, chat_id=cid, message_id=mid,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=_kb(),
                    )
                except Exception as e:
                    log.debug("auto session edit: %s", e)
            asyncio.create_task(_do())

    def _step_start(name: str, detail: str = ""):
        step_state[name]["status"] = "⏳"
        step_state[name]["start_ts"] = _time.time()
        step_state[name]["detail"] = detail

    # Message chain: every phase completion sends a short NEW message
    # replying to the previous one (BUY → WD → SEND → RECEIVED → SELL).
    # Provides scrollback visibility so the summary card doesn't
    # get lost in busy chats.
    _last_msg_ids: dict[int, int] = {}     # chat_id → last phase message
    async def _phase_msg(chat_ids_l, text: str):
        for cid in chat_ids_l:
            reply_to = _last_msg_ids.get(cid) or msg_ids.get(cid)
            try:
                m = await app.bot.send_message(
                    cid, text, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_to_message_id=reply_to, allow_sending_without_reply=True)
                _last_msg_ids[cid] = m.message_id
            except Exception as e:
                log.debug("phase-msg reply err: %s", e)

    _announced = set()                     # {(name, status)} — dedup reply
    def _step_done(name: str, status: str = "✅", detail: str | None = None,
                    silent: bool = False):
        st = step_state[name]
        was_status = st["status"]
        if st["start_ts"]:
            st["duration"] = _time.time() - st["start_ts"]
        st["status"] = status
        if detail is not None:
            st["detail"] = detail
        # Reply-chain — only for REAL phase completions, not for resume
        # pre-marks (silent=True) that just backfill UI on restart.
        if silent:
            return
        # Dedup: fire ONE reply per (phase, final-status). Status callbacks
        # from executor emit multiple "2/4" posts during confirm polling,
        # so this method may be called many times per real completion.
        key = (name, status)
        if key in _announced:
            return
        if status not in ("✅", "❌", "⏭"):
            return
        _announced.add(key)
        emoji = {"BUY": "💵", "WITHDRAW": "📤", "SEND": "🔀",
                 "RECEIVED": "📥", "SELL": "💰"}.get(name, "•")
        dur = f" · {_fmt_dur(st['duration'])}" if st["duration"] else ""
        d = f" · <i>{st['detail']}</i>" if st.get("detail") else ""
        asyncio.create_task(_phase_msg(chat_ids,
            f"{emoji} <b>{name}</b> {status}{dur}{d}"))

    def _note(s: str):
        _note_holder[0] = s

    # Silence noisy executor status callbacks — we render our own UI.
    async def status_cb(_rcpt, msg):
        # Parse minimal signals to update step state without spam
        low = str(msg).lower()
        if "куплено" in low and step_state["BUY"]["status"] != "✅":
            _step_done("BUY", "✅",
                        msg.split("куплено", 1)[-1][:60] if "куплено" in msg else "")
        elif "1/4" in msg:
            _step_start("WITHDRAW", "→ hot wallet")
        elif "2/4" in msg:
            _step_done("WITHDRAW", "✅", "on hot wallet")
        elif "3/4" in msg:
            # SEND = broadcast done. Include hyperlink to explorer tx.
            tx_h = getattr(sess, "forward_tx", None) or ""
            hw_lag = ""
            if sess.hw_arrived_ts and sess.forward_ts:
                hw_lag = f" ({int(sess.forward_ts - sess.hw_arrived_ts)}s після HW)"
            tx_html = (f'<a href="{dex.tx_url(plan.chain, tx_h)}">tx</a>'
                       if tx_h else "tx")
            # Backfill start_ts so the duration column renders — SEND
            # actually spans HW-arrival → broadcast (short, <5s usually).
            if step_state["SEND"]["start_ts"] is None:
                step_state["SEND"]["start_ts"] = sess.hw_arrived_ts or _time.time()
            _step_done("SEND", "✅",
                       f"{tx_html} → {cex.pretty(plan.sell_eid)}{hw_lag}")
            _step_start("RECEIVED", f"чекаю кредит на {cex.pretty(plan.sell_eid)}")
        elif "4/4" in msg and ("✅" in msg or "зарахув" in low or "кредитовано" in low):
            _step_done("RECEIVED", "✅",
                       f"зараховано на {cex.pretty(plan.sell_eid)}")
        elif "SELL_PROGRESS" in msg:
            # "SELL_PROGRESS: 60000.0000/240207.4256 (25%) pass 2/8"
            try:
                after = msg.split("SELL_PROGRESS:", 1)[1].strip()
                # Grab the "X/Y (Z%)" and pass info
                step_state["SELL"]["detail"] = after
            except Exception:
                pass
        elif "sold" in low or "продано" in low:
            pass                                            # sell handled in flow
        else:
            # Route non-critical detail to note line if useful
            if any(x in low for x in ("scale", "warning", "⚠️", "err")):
                _note(msg[:80])
        try:
            await _push()
        except Exception: pass
    ex.wire_status(status_cb)

    task = asyncio.current_task()
    if task:
        executor.register_task(task)

    async def _finish(status_emoji: str, verdict: str):
        elapsed = _time.time() - started
        r = sess.receipt
        pnl = getattr(r, "net_pnl_usd", None) or 0.0
        tail_lines = ["─" * 20,
                      f"{status_emoji} <b>{verdict}</b>  ·  ⏱ Total {_fmt_dur(elapsed)}"]
        # Detailed line-items when completed
        is_done = verdict == "DONE"
        gross = 0.0
        fees = 0.0
        try:
            fees = float((plan.fees or {}).get("total_usd") or 0)
        except Exception: pass
        # GROUND-TRUTH portfolio delta — this is REAL PnL, no per-order math.
        real_delta = None
        try:
            if sess.usd_snapshot_before is not None:
                sess.usd_snapshot_after = await _snapshot_all_usd()
                real_delta = sess.usd_snapshot_after - sess.usd_snapshot_before
        except Exception as e:
            log.debug("snapshot after err: %s", e)
        # Show BOTH: real (portfolio delta) and computed (from orders).
        if real_delta is not None:
            polluted = (len({sid for sid in _SESSIONS if sid != run_id} |
                             _sessions_at_start) > 0)
            note = ("  <i>⚠️ у вікні була ще сесія — Δ може бути змішаною</i>"
                    if polluted else "")
            tail_lines.append(
                f"💰 <b>Δ портфеля: ${real_delta:+,.2f}</b>  "
                f"(${sess.usd_snapshot_before:,.2f} → "
                f"${sess.usd_snapshot_after:,.2f}){note}")
        if is_done:
            gross = pnl + fees
            tail_lines.append(
                f"    <i>розрахунково: чист ${pnl:+,.2f} · гросс ${gross:+,.2f} · "
                f"fees ${fees:.2f}</i>")
            exp = getattr(plan, "net_profit_usd", None) or 0.0
            if exp:
                delta = pnl - exp
                tail_lines.append(
                    f"    <i>очік. було: ${exp:+,.2f} · відхилення: ${delta:+,.2f}</i>")
        else:
            if pnl:
                tail_lines.append(f"💰 Realized: <b>${pnl:+,.2f}</b>")
            else:
                exp = getattr(plan, "net_profit_usd", None)
                if exp:
                    tail_lines.append(f"💰 Expected: <b>${exp:+,.2f}</b>")
        if getattr(r, "error", None):
            tail_lines.append(f"⚠️ {r.error}")
        final = _render() + "\n" + "\n".join(tail_lines)
        for cid, mid in msg_ids.items():
            try:
                await app.bot.edit_message_text(
                    text=final, chat_id=cid, message_id=mid,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                )
            except Exception: pass
        try: executor._persist(r, plan)
        except Exception: pass
        # Persistent aggregate stats (legacy + new)
        outcome = ("completed" if is_done else
                   "cancelled" if "CANCELLED" in verdict else "failed")
        try:
            import stats as _stats
            net_for_stats = real_delta if real_delta is not None else pnl
            _stats.record(outcome, plan.base, net_usd=net_for_stats,
                          gross_usd=gross, fees_usd=fees, dur_sec=elapsed)
        except Exception as e:
            log.debug("stats legacy record err: %s", e)
        # New rich stats2 with ground-truth + per-line reconciliation
        try:
            import stats2 as _st2
            # Buy/sell fees from ccxt order responses (native ccy).
            buy_ord = r.buy_order or {}
            buy_fee_native = 0.0
            bf = buy_ord.get("fee") or {}
            if isinstance(bf, dict) and bf.get("cost"):
                buy_fee_native = float(bf["cost"])
            for x in (buy_ord.get("fees") or []):
                if isinstance(x, dict) and x.get("cost"):
                    buy_fee_native += float(x["cost"])
            sell_ord = r.sell_order or {}
            sell_fee_native = 0.0
            sf = sell_ord.get("fee") or {}
            if isinstance(sf, dict) and sf.get("cost"):
                sell_fee_native = float(sf["cost"])
            for x in (sell_ord.get("fees") or []):
                if isinstance(x, dict) and x.get("cost"):
                    sell_fee_native += float(x["cost"])
            # FX rates for both quote ccys
            def _quote_of(sym):
                return (sym.split("/", 1)[1] if "/" in sym else "USDT").upper()
            fx_buy = cex.get_fx_rate(_quote_of(plan.buy_sym)) or 1.0
            fx_sell = cex.get_fx_rate(_quote_of(plan.sell_sym)) or 1.0
            wd_fee_usd = float((plan.fees or {}).get("wd_fee_usd") or 0)
            gas_usd = float((plan.fees or {}).get("gas_usd") or 0)
            # Fallbacks if per-item missing
            if not wd_fee_usd and not gas_usd and (plan.fees or {}).get("total_usd"):
                # Rough split: 60% on-chain fees, 40% trade fees
                wd_fee_usd = float((plan.fees or {}).get("total_usd") or 0) * 0.5
                gas_usd = float((plan.fees or {}).get("total_usd") or 0) * 0.1
            _st2.session_end(
                run_id, plan, outcome,
                snapshot_usd=sess.usd_snapshot_after,
                snapshot_detail={"total_usd": sess.usd_snapshot_after or 0},
                real_delta_usd=real_delta,
                buy_order=buy_ord, sell_orders=[sell_ord] if sell_ord else [],
                buy_fee_native=buy_fee_native, sell_fee_native=sell_fee_native,
                wd_fee_usd=wd_fee_usd, gas_usd=gas_usd,
                fx_buy=fx_buy, fx_sell=fx_sell,
                exec_qty=float(sess.filled_qty or 0),
                elapsed_sec=elapsed,
                error=getattr(r, "error", None),
                other_sessions_in_window=(
                    len({sid for sid in _SESSIONS if sid != run_id} |
                        _sessions_at_start) > 0),
            )
        except Exception as e:
            log.debug("stats2 session_end err: %s", e, exc_info=True)

    # Register into _SESSIONS so the existing pbuy/pwd/psl/pcx handlers
    # can pick up the session for manual retry on failure.
    _SESSIONS[run_id] = {"plan": plan, "session": sess, "subs": chat_ids}
    _persist_sessions()

    async def _prompt_retry(phase_label: str, err_text: str):
        """Post interactive buttons — the session stays alive so the user
        can retry, skip (if they did it manually), or cancel."""
        rows = []
        if phase_label == "BUY":
            rows.append([InlineKeyboardButton("🔁 Повторити BUY",
                                              callback_data=f"pbuy:{run_id}")])
        elif phase_label == "WITHDRAW":
            rows.append([InlineKeyboardButton("🔁 Повторити ВИВІД",
                                              callback_data=f"pwd:{run_id}")])
            rows.append([InlineKeyboardButton("✋ Зробив вручну → SELL",
                                              callback_data=f"psl:{run_id}")])
        elif phase_label == "SELL":
            rows.append([InlineKeyboardButton("🔁 Повторити SELL",
                                              callback_data=f"psl:{run_id}")])
        rows.append([InlineKeyboardButton("🚫 Стоп", callback_data=f"pcx:{run_id}")])
        for cid in chat_ids:
            try:
                await app.bot.send_message(
                    cid,
                    f"⚠️ <b>{plan.base} · {phase_label} FAIL</b>\n{err_text}\n\n"
                    f"Сесія жива (sid <code>{run_id}</code>). Обери що робити:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(rows),
                )
            except Exception as e:
                log.debug("prompt retry send: %s", e)

    # Phase name → error text, for phases that failed in a way retrying
    # cannot fix. Distinguishes "gave up, position needs unwinding" from
    # the ordinary False that means "killed / user cancelled".
    _hard_fail: dict[str, str] = {}
    # Ledger rows this session owns. This path used to write none at
    # all, which is why an abandoned position left no trace anywhere.
    _ledger_ids: list[str] = []

    async def _run_phase_retry(name: str, coro_fn, max_retries: int = 999,
                                 backoff_start: int = 15,
                                 stop_on=None) -> bool:
        """Keep re-running the phase (or until _KILL / user Stop / Skip).
        Skip cancels the currently-running phase task — no need to wait
        for it to finish naturally.

        `stop_on(err) -> bool` marks an error as unretryable: the loop
        exits immediately and records the reason in `_hard_fail[name]`
        so the caller can unwind instead of holding the position.
        """
        _SESSION_CURRENT_PHASE[run_id] = name
        attempt = 0
        while attempt < max_retries:
            if name in _SESSION_SKIP.get(run_id, set()):
                _SESSION_SKIP[run_id].discard(name)
                log.info("session %s: %s SKIPPED by user (pre-attempt)", run_id, name)
                _step_done(name, "⏭", "пропущено вручну")
                await _push()
                return True
            attempt += 1
            # Race coro_fn vs skip-watcher: if skip fires mid-phase,
            # cancel the phase task and return "skipped".
            phase_task = asyncio.create_task(coro_fn())
            async def _skip_watch():
                while True:
                    if name in _SESSION_SKIP.get(run_id, set()):
                        return "skip"
                    if run_id not in _SESSIONS or executor.kill_active():
                        return "kill"
                    await asyncio.sleep(1)
            watch_task = asyncio.create_task(_skip_watch())
            done, pending = await asyncio.wait(
                [phase_task, watch_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if watch_task in done:
                signal = watch_task.result()
                phase_task.cancel()
                try: await phase_task
                except BaseException: pass                  # CancelledError is BaseException
                if signal == "skip":
                    _SESSION_SKIP[run_id].discard(name)
                    log.info("session %s: %s SKIPPED by user (mid-phase)", run_id, name)
                    _step_done(name, "⏭", "пропущено вручну")
                    await _push()
                    return True
                # kill — clarify reason for logs
                reason = ("/kill" if executor.kill_active() else "session-gone")
                log.info("session %s: %s aborted (reason=%s)", run_id, name, reason)
                return False
            # Phase task completed first
            watch_task.cancel()
            try: await watch_task
            except BaseException: pass                    # CancelledError is BaseException
            try:
                ok = phase_task.result()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                sess.receipt.error = f"{type(e).__name__}: {str(e)[:200]}"
                ok = False
            if ok:
                return True
            if executor.kill_active():
                return False
            if run_id not in _SESSIONS:                    # user cancelled
                return False
            if name in _SESSION_SKIP.get(run_id, set()):    # skip mid-flight
                _SESSION_SKIP[run_id].discard(name)
                _step_done(name, "⏭", "пропущено вручну")
                await _push()
                return True
            err = sess.receipt.error or "?"
            if stop_on is not None and stop_on(err):
                _hard_fail[name] = err
                log.warning("session %s: %s unretryable, stopping after "
                            "attempt %d: %s", run_id, name, attempt, err[:200])
                _step_done(name, "⛔", f"незворотна помилка · {err[:60]}")
                await _push()
                return False
            _step_done(name, "⏳", f"retry #{attempt} · {err[:60]}")
            # IOC не заповнилась — це нормальний ретрай, не тривога.
            if "IOC" in err or "не заповнилась" in err:
                log.info("%s IOC retry #%d: %s", name, attempt, err[:120])
            else:
                _note(f"⚠️ {name} fail (attempt {attempt}): {err[:120]}")
            await _push()
            delay = min(backoff_start * (1.5 ** min(attempt - 1, 8)), 300)
            log.info("session %s: %s retry #%d in %.0fs", run_id, name, attempt, delay)
            # Interruptible sleep — checks skip flag every 2s
            waited = 0
            while waited < delay:
                await asyncio.sleep(min(2, delay - waited))
                waited += 2
                if name in _SESSION_SKIP.get(run_id, set()):
                    _SESSION_SKIP[run_id].discard(name)
                    _step_done(name, "⏭", "пропущено вручну")
                    await _push()
                    return True
                if run_id not in _SESSIONS or executor.kill_active():
                    return False
            _step_start(name, f"retry #{attempt + 1}")
            await _push()
        return False                                       # exhausted (only reachable for BUY)

    async def _unwind_on_buy_venue(reason: str) -> bool:
        """Sell the filled position back where we bought it.

        Reached when the coin is stuck on the buy exchange with no way
        out — a withdrawal cap, a delisted network, a frozen asset. The
        spread is already lost at that point; the only remaining choice
        is between eating it now and holding an unhedged alt position
        for however long the block lasts. Selling back caps the damage
        at the spread, which is what a human does by hand anyway.
        """
        try:
            kd = keys_mod.load_keys()
            inst = cex.get_private(plan.buy_eid, kd[plan.buy_eid])
            try:
                if not inst.markets:
                    await inst.load_markets()
            except Exception:
                pass
            base = plan.base
            # Cancel FIRST, read the balance second. A resting order
            # holds its size in `used`, not `free`, so checking `free`
            # before cancelling reports zero and the unwind concludes
            # there is nothing to sell — which is how 18,170 ACX stayed
            # on Bitvavo on 2026-09-10 03:26 while the log claimed the
            # unwind had run.
            try:
                _open = await inst.fetch_open_orders(plan.buy_sym)
                for o in _open:
                    await cex.cancel_order_robust(inst, o["id"], plan.buy_sym)
                if _open:
                    log.warning("session %s: unwind cancelled %d resting "
                                "order(s) on %s before selling",
                                run_id, len(_open), plan.buy_sym)
                    await asyncio.sleep(1)          # let the release settle
            except Exception as _e:
                log.warning("session %s: unwind cancel open orders: %s",
                            run_id, _e)
            bal = await inst.fetch_balance()
            free_q = float((bal.get("free") or {}).get(base) or 0)
            total_q = float((bal.get("total") or {}).get(base) or 0)
            qty = free_q
            # Never sell more than this run bought: the venue balance can
            # include residue from earlier runs the user may want kept.
            want = float(sess.filled_qty or 0)
            if want > 0:
                qty = min(qty, want)
            if qty <= 0:
                # Loud, and in the LOG — the old version only posted to
                # Telegram, so this branch was invisible in the logs and
                # had to be diagnosed from exchange history instead.
                log.error("session %s: unwind found nothing sellable — "
                          "%s on %s: free=%.6f total=%.6f wanted=%.6f",
                          run_id, base, plan.buy_eid, free_q, total_q, want)
                _note(f"⚠️ відкат: вільних {base} на {cex.pretty(plan.buy_eid)} "
                      f"нема (free={free_q:,.4f} / total={total_q:,.4f})")
                await _push()
                for _lid in _ledger_ids:
                    try:
                        ledger.mark_stuck(_lid, f"unwind found no free {base}")
                    except Exception:
                        pass
                return False
            _note(f"↩️ відкат: продаю {qty:,.4f} {base} назад на "
                  f"{cex.pretty(plan.buy_eid)} — {reason[:80]}")
            await _push()
            o = await cex.place_order(inst, plan.buy_sym, "market", "sell", qty)
            got = 0.0
            try:
                o2 = await inst.fetch_order(o["id"], plan.buy_sym)
                got = float(o2.get("cost") or 0)
            except Exception:
                got = float(o.get("cost") or 0)
            quote = plan.buy_sym.split("/", 1)[1] if "/" in plan.buy_sym else ""
            got_usd = got * (cex.get_fx_rate(quote) or 1.0)
            log.warning("session %s: unwound %s %.6f on %s for %.2f %s "
                        "(~$%.2f)", run_id, base, qty, plan.buy_eid, got,
                        quote, got_usd)
            _note(f"✅ відкат виконано: {qty:,.4f} {base} → {got:,.2f} {quote}")
            await _push()
            for _lid in _ledger_ids:
                try:
                    if got_usd > 0:
                        ledger.close(_lid, realized_usd=got_usd,
                                     note=f"unwound on {plan.buy_eid}: {reason[:70]}")
                    else:
                        # Sold, but the exchange gave us no cost back.
                        # Closing with a fabricated zero would understate
                        # the books, so flag it for a human instead.
                        ledger.mark_stuck(_lid, "unwound, proceeds unknown")
                except Exception:
                    pass
            return True
        except Exception as e:
            log.error("session %s: unwind FAILED: %s", run_id, e)
            _note(f"❌ відкат не вдався: {str(e)[:120]} — {plan.base} лишились "
                  f"на {cex.pretty(plan.buy_eid)}")
            await _push()
            for _lid in _ledger_ids:
                try:
                    ledger.mark_stuck(_lid, f"unwind failed: {str(e)[:80]}")
                except Exception:
                    pass
            return False

    skip_buy = bool(payload.get("skip_buy"))
    from_wallet = bool(payload.get("from_wallet"))
    only_sell = bool(payload.get("only_sell"))
    # RESUME: derive which phases are already done from session state.
    resume_skip_buy = False
    resume_skip_wd = False
    resume_from_send = False              # tokens on HW, need SEND
    resume_from_received = False          # SEND broadcast, waiting credit
    resume_from_sell = False              # credited, need SELL
    if resume:
        # Read persistent flags to figure out where we died
        if sess.deposit_credited:
            resume_from_sell = True; resume_skip_buy = True; resume_skip_wd = True
        elif getattr(sess, "forward_tx", None):
            resume_from_received = True; resume_skip_buy = True; resume_skip_wd = True
        elif sess.filled_qty_wei > 0:
            resume_from_send = True; resume_skip_buy = True; resume_skip_wd = True
        elif sess.filled_qty > 0:
            resume_skip_buy = True
        # Pre-mark UI rows that are already done
        for _st, done in (("BUY", resume_skip_buy),
                           ("WITHDRAW", resume_skip_wd),
                           ("SEND", resume_from_received or resume_from_sell),
                           ("RECEIVED", resume_from_sell)):
            if done:
                _step_done(_st, "✅", "resumed after restart", silent=True)
        _note(f"↻ ресюм після рестарту "
              f"({'SELL' if resume_from_sell else 'RECEIVED' if resume_from_received else 'SEND' if resume_from_send else 'WITHDRAW'})")
        await _push()
    try:
        async with base_lock:
            # GROUND-TRUTH balance snapshot BEFORE any trading. After SELL
            # we take another snapshot and delta = REAL PnL (includes all
            # trade + WD + gas fees automatically, no per-order math).
            if not resume and sess.usd_snapshot_before is None:
                try:
                    sess.usd_snapshot_before = await _snapshot_all_usd()
                    log.info("session %s: portfolio BEFORE = $%.2f",
                             run_id, sess.usd_snapshot_before)
                    # Persist to stats2 (session_start)
                    try:
                        import stats2 as _st2
                        _st2.session_start(run_id, plan,
                                            sess.usd_snapshot_before,
                                            {"total_usd": sess.usd_snapshot_before})
                    except Exception as e:
                        log.debug("stats2 session_start err: %s", e)
                except Exception as e:
                    log.debug("snapshot before err: %s", e)
            # Track any OTHER active sessions during our window — used to
            # flag polluted deltas in the final report.
            _sessions_at_start = {sid for sid in _SESSIONS if sid != run_id}
            # RESUME shortcuts — jump straight to remaining phase.
            if resume_from_sell:
                # Already credited on dest → just SELL.
                only_sell = True
            elif resume_from_received:
                # SEND broadcast, waiting for dest credit → poll balance
                # then SELL.
                try:
                    kd = keys_mod.load_keys()
                    inst_dst = cex.get_private(plan.sell_eid, kd[plan.sell_eid])
                    if plan.sell_eid == "binance":
                        try: await inst_dst.load_time_difference()
                        except Exception: pass
                    _step_start("RECEIVED",
                                 f"поновлено — чекаю кредит на {cex.pretty(plan.sell_eid)}")
                    await _push()
                    import time as _t
                    dead = _t.time() + 45 * 60
                    while _t.time() < dead:
                        b = await inst_dst.fetch_balance()
                        free = float((b.get(plan.base) or {}).get("free") or 0)
                        if free >= plan.qty * 0.90:
                            sess.filled_qty = free
                            sess.deposit_credited = True
                            _step_done("RECEIVED", "✅",
                                       f"кредит {free:.4f} {plan.base}")
                            await _push()
                            break
                        await asyncio.sleep(15)
                    else:
                        _step_done("RECEIVED", "❌", "45хв timeout")
                        await _push()
                        await _prompt_retry("WITHDRAW", "deposit timeout after resume")
                        return
                    only_sell = True
                except Exception as e:
                    _step_done("RECEIVED", "❌", f"resume err: {e}")
                    await _push(); return
            elif resume_from_send:
                from_wallet = True                        # skip BUY+WD, use HW balance
                skip_buy = True
            if only_sell:
                # Only-sell: base is already on the destination exchange.
                # Skip BUY / WITHDRAW / SEND / RECEIVED — go straight to SELL.
                try:
                    kd = keys_mod.load_keys()
                    inst_dst = cex.get_private(plan.sell_eid, kd[plan.sell_eid])
                    if plan.sell_eid == "binance":
                        try: await inst_dst.load_time_difference()
                        except Exception: pass
                    bal = await inst_dst.fetch_balance()
                    free_base = float((bal.get(plan.base) or {}).get("free") or 0)
                except Exception as e:
                    _step_done("BUY", "❌", f"only_sell err: {e}"); await _push(); return
                if free_base <= 0:
                    _step_done("BUY", "❌",
                               f"only_sell: 0 {plan.base} on {plan.sell_eid}")
                    await _push()
                    # Stale resume — nothing to sell. Clean up.
                    _SESSIONS.pop(run_id, None); _persist_sessions()
                    return
                sess.filled_qty = free_base
                plan.qty = free_base
                sess.deposit_credited = True
                for st in ("BUY", "WITHDRAW", "SEND", "RECEIVED"):
                    _step_done(st, "⏭", "skip (only-sell)")
                await _push()
                buy_ok = True
                # Jump directly to SELL — skip WITHDRAW retry below.
                _step_start("SELL", f"on {cex.pretty(plan.sell_eid)}  "
                                     f"({free_base:.4f} {plan.base})")
                await _push()
                sell_ok = await _run_phase_retry(
                    "SELL", lambda: ex.phase_sell(sess),
                    max_retries=10_000, backoff_start=15)
                if not sell_ok:
                    _step_done("SELL", "❌", (sess.receipt.error or "?")[:60])
                    await _push()
                    await _prompt_retry("SELL", sess.receipt.error or "sell stuck")
                    return
                try:
                    _so = sess.receipt.sell_order or {}
                    _q = float(_so.get("filled") or sess.filled_qty or 0)
                    _a = float(_so.get("average") or plan.sell_limit or 0)
                    _quo = (plan.sell_sym.split("/", 1)[1]
                            if "/" in plan.sell_sym else "USDT").upper()
                    _fx = cex.get_fx_rate(_quo) or 1.0
                    _step_done("SELL", "✅",
                               f"{_q:.4f} {plan.base} @ {_a:.6g} "
                               f"= <b>${_q * _a * _fx:,.2f}</b>")
                except Exception:
                    _step_done("SELL", "✅")
                await _finish("✅", "DONE")
                _SESSIONS.pop(run_id, None); _persist_sessions()
                try:
                    await _maybe_auto_rebalance(app, chat_ids,
                                                  f"після {plan.base} (only-sell)")
                except Exception as e:
                    log.warning("only-sell auto-rebalance err: %s", e)
                return
            if skip_buy:
                # Resume mode: BUY was done in a prior (cancelled) run.
                # Use current free balance of `base` on source as filled_qty.
                try:
                    kd = keys_mod.load_keys()
                    inst_src = cex.get_private(plan.buy_eid, kd[plan.buy_eid])
                    if plan.buy_eid == "binance":
                        try: await inst_src.load_time_difference()
                        except Exception: pass
                    bal = await inst_src.fetch_balance()
                    free_base = float((bal.get(plan.base) or {}).get("free") or 0)
                except Exception as e:
                    _step_done("BUY", "❌", f"resume err: {e}"); await _push(); return
                if free_base <= 0:
                    _step_done("BUY", "❌", f"resume: 0 {plan.base} free on {plan.buy_eid}")
                    await _push(); return
                sess.filled_qty = free_base
                sess.avg_buy_price = plan.buy_limit
                plan.qty = free_base
                _step_done("BUY", "⏭",
                           f"resume: {free_base:.4f} {plan.base} (skip buy)")
                await _push()
                buy_ok = True
            else:
                _step_start("BUY", f"on {cex.pretty(plan.buy_eid)}")
                await _push()
                buy_ok = await _run_phase_retry("BUY",
                                                 lambda: ex.phase_buy(sess),
                                                 max_retries=5, backoff_start=10)
            if not buy_ok:
                _step_done("BUY", "❌", (sess.receipt.error or "?")[:60])
                await _push()
                _TRADE_FAILS[plan.base] = _TRADE_FAILS.get(plan.base, 0) + 1
                if _TRADE_FAILS[plan.base] >= _TRADE_BAN_MAX:
                    _TRADE_BAN_UNTIL[plan.base] = time.time() + _TRADE_BAN_SEC
                    _TRADE_FAILS[plan.base] = 0
                    log.info("auto-blacklist %s for %dh — %d consecutive BUY fails",
                             plan.base, _TRADE_BAN_SEC // 3600, _TRADE_BAN_MAX)
                await _prompt_retry("BUY", sess.receipt.error or "buy exhausted")
                return
            buy_avg = None
            try:
                buy_avg = float((sess.receipt.buy_order or {}).get("average")
                                 or plan.buy_limit)
            except Exception:
                buy_avg = plan.buy_limit
            # Show ACTUAL USD spent — not the planned $1k (which is
             # notional_planned, the target). filled_qty × buy_avg × fx = real.
            try:
                _quote = (plan.buy_sym.split("/", 1)[1]
                          if "/" in plan.buy_sym else "USDT").upper()
                _fxbuy = cex.get_fx_rate(_quote) or 1.0
                _spent_usd = float(sess.filled_qty) * float(buy_avg) * _fxbuy
            except Exception:
                _spent_usd = 0
            _step_done("BUY", "✅",
                       f"{sess.filled_qty:.4f} {plan.base} @ {buy_avg:.6g} "
                       f"= <b>${_spent_usd:,.2f}</b>")
            # Record inventory the moment it exists, from the ACTUAL fill
            # rather than the plan. Every exit below — unwind, blocked
            # withdrawal, crash, restart — then leaves a trace that
            # /positions and the reconciler can find.
            try:
                _lid = ledger.open_position(
                    plan.base, qty=float(sess.filled_qty or 0),
                    cost_usd=float(_spent_usd or 0),
                    location=ledger.LOC_CEX, venue=plan.buy_eid,
                    chain=getattr(plan, "chain", None),
                    contract=getattr(plan, "base_contract", None),
                    next_action="withdraw_to_hw", session_id=run_id,
                    note=f"cex cycle {plan.buy_eid}->{plan.sell_eid}")
                _ledger_ids.append(_lid)
            except Exception as _e:
                log.warning("ledger open (cex cycle buy): %s", _e)
            # POST-BUY viability. The min-profit floor is an ENTRY gate:
            # it decides whether to spend money. Once the buy has filled
            # the money is spent, and the floor no longer describes any
            # available choice — there are only two, finish the run or
            # unwind, and "leave it on the exchange" is neither.
            #
            # The old code returned here holding the coin whenever the
            # projection fell under the floor. On 2026-09-10 01:49 that
            # abandoned 1,046,765 IOST (EUR 1,697.46) because a 99.19%
            # fill scaled a $20.04 plan down to $19.88 — twelve cents
            # under a $20 floor. Nothing recorded it in the ledger or in
            # stuck; the position then fell 41% over six hours (-$806).
            #
            # Finishing a run that nets less than the floor still beats
            # unwinding, which pays the round-trip spread for certain.
            # So unwind only when the projection is actually negative.
            try:
                is_testrun = bool(getattr(plan, "is_testrun", False))
                min_p = float(os.getenv("AUTOEXEC_MIN_PROFIT_USD",
                              os.getenv("MIN_PROFIT_USD", "10")))
                if _HUNTER and _HUNTER.min_profit_usd:
                    min_p = max(min_p, _HUNTER.min_profit_usd)
                # scale planned net profit by fraction filled
                filled_frac = float(sess.filled_qty) / max(plan.qty, 1e-9)
                proj_net = (plan.net_profit_usd or 0) * filled_frac
                if not is_testrun and proj_net <= 0:
                    _step_done("BUY", "↩️",
                               f"проект ${proj_net:.2f} &lt;= 0 — розвертаю "
                               f"(filled {filled_frac*100:.0f}%)")
                    _note(f"↩️ IOC зʼїв ${_spent_usd:,.2f} з ${plan.notional_usd:,.0f}, "
                          f"проект ${proj_net:.2f} ≤ 0 — продаю назад на "
                          f"{cex.pretty(plan.buy_eid)}.")
                    await _push()
                    await _unwind_on_buy_venue(f"post-buy проект ${proj_net:.2f}")
                    await _finish("↩️", f"UNWOUND (проект ${proj_net:.2f})")
                    _SESSIONS.pop(run_id, None); _persist_sessions()
                    return
                if not is_testrun and proj_net < min_p:
                    log.warning("session %s: %s post-buy net $%.2f < floor "
                                "$%.2f but positive — completing the run "
                                "rather than stranding the position",
                                run_id, plan.base, proj_net, min_p)
                    _note(f"⚠️ проект ${proj_net:.2f} &lt; min ${min_p:.2f}, але &gt; 0 "
                          f"— довожу прогін до кінця (кидати позицію не можна)")
                    await _push()
            except Exception as _e:
                # Falling through means completing the run, which is the
                # safe direction: the alternative is silently holding.
                log.warning("post-buy viability err (completing run): %s", _e)
            _step_start("WITHDRAW", f"→ hot wallet ({plan.chain})")
            await _push()
            # WITHDRAW + SELL retry indefinitely — funds MUST land + sell,
            # EXCEPT when the exchange has told us the withdrawal can
            # never go through (cap hit, asset frozen, address not
            # whitelisted). Grinding those holds the position hostage.
            import routing as _rt_wd
            await _run_phase_retry("WITHDRAW",
                                    lambda: ex.phase_withdraw_and_wait(sess,
                                                                         from_hw=from_wallet),
                                    max_retries=10_000, backoff_start=30,
                                    stop_on=_rt_wd.is_permanent_wd_error)
            if sess.deposit_credited:
                # Withdrawal cleared — the venue works, drop stale blocks.
                _rt_wd.clear_wd_block(plan.buy_eid)
            if _hard_fail.get("WITHDRAW"):
                # We own the alt and there is no route off the exchange
                # this run. Unwind now: holding it also blocks the
                # auto-rebalancer, which skips while any session is live.
                _wd_reason = _hard_fail["WITHDRAW"]
                _rt_wd.note_wd_blocked(plan.buy_eid, _wd_reason)
                await _unwind_on_buy_venue(_wd_reason)
                await _finish("↩️", f"UNWOUND (вивід заблоковано: "
                                    f"{html.escape(_wd_reason[:70])})")
                _SESSIONS.pop(run_id, None); _persist_sessions()
                return
            # If user tapped "Пропустити" for WITHDRAW = "я вивів руками".
            # Re-run the phase in wait_manual_wd mode — skip the broadcast
            # step but wait for HW arrival then continue SEND/RECEIVED normally.
            if run_id in _SESSION_MANUAL_WD and not sess.deposit_credited:
                _SESSION_MANUAL_WD.discard(run_id)
                _step_start("WITHDRAW", "👋 РУЧНИЙ — чекаю прихід на HW…")
                await _push()
                await _run_phase_retry("WITHDRAW",
                    lambda: ex.phase_withdraw_and_wait(sess, wait_manual_wd=True),
                    max_retries=10_000, backoff_start=30)
            if not sess.deposit_credited:
                # Session must stay alive — user intervention needed
                _step_done("WITHDRAW", "❌", (sess.receipt.error or "?")[:60])
                await _push()
                await _prompt_retry("WITHDRAW", sess.receipt.error or "withdraw stuck")
                return
            _step_done("WITHDRAW", "✅")
            _step_start("SELL", f"0/{sess.filled_qty:.4f} на {cex.pretty(plan.sell_eid)}")
            await _push()
            sell_ok = await _run_phase_retry("SELL",
                                              lambda: ex.phase_sell(sess),
                                              max_retries=10_000, backoff_start=15)
            if not sell_ok:
                _step_done("SELL", "❌", (sess.receipt.error or "?")[:60])
                await _push()
                await _prompt_retry("SELL", sess.receipt.error or "sell stuck")
                return
            # SELL done — compute REAL USD proceeds from sess.receipt fields.
            _got_usd = None                 # None = proceeds unknown, not $0
            try:
                _sell_ord = sess.receipt.sell_order or {}
                _sell_qty = float(_sell_ord.get("filled") or sess.filled_qty or 0)
                _sell_avg = float(_sell_ord.get("average")
                                   or plan.sell_limit or 0)
                _quote_s = (plan.sell_sym.split("/", 1)[1]
                            if "/" in plan.sell_sym else "USDT").upper()
                _fx_s = cex.get_fx_rate(_quote_s) or 1.0
                _got_native = _sell_qty * _sell_avg
                _got_usd = _got_native * _fx_s
                _step_done("SELL", "✅",
                           f"{_sell_qty:.4f} {plan.base} @ {_sell_avg:.6g} "
                           f"= <b>${_got_usd:,.2f}</b>")
            except Exception:
                _step_done("SELL", "✅")
            # Close exactly once, whether or not the PnL block above
            # threw: the sell already happened, and a row left OPEN sends
            # the reconciler chasing inventory that no longer exists.
            # `realized_usd=None` means "sold, amount unknown" — better
            # than booking a fabricated $0.
            for _lid in _ledger_ids:
                try:
                    ledger.close(_lid, realized_usd=_got_usd,
                                 note=f"cex cycle sold on {plan.sell_eid}")
                except Exception:
                    pass
            await _finish("✅", "DONE")
            _SESSIONS.pop(run_id, None); _persist_sessions()
            _TRADE_FAILS.pop(plan.base, None)               # success → reset counter
            # After a completed session — auto-check exchange balances.
            # If any exchange fell below its target (default $1000) —
            # trigger rebalance automatically. No button, no ask; the
            # session is over so there's no confusion about which action
            # moved what.
            try:
                await _maybe_auto_rebalance(app, chat_ids, f"після {plan.base}")
            except Exception as e:
                log.warning("post-trade auto-rebalance err: %s", e)
    except asyncio.CancelledError:
        # With BaseException swallow in _run_phase_retry this branch fires
        # only on real task cancellation (bot shutdown, external cancel).
        reason = "/kill" if executor.kill_active() else "task-cancel"
        log.warning("session %s CancelledError propagated to outer (%s)",
                    run_id, reason)
        await _finish("🛑", f"CANCELLED ({reason})")
        _SESSIONS.pop(run_id, None); _persist_sessions()
        raise
    except Exception as e:
        sess.receipt.error = str(e)[:200]
        await _prompt_retry("CRASH", f"{type(e).__name__}: {e}")


_AUTO_REBAL_LOCK = asyncio.Lock()
_AUTO_REBAL_LAST_TS: float = 0.0
_AUTO_REBAL_MIN_GAP_SEC = 60                             # cool-down 60s
AUTO_REBAL_TRIGGER_USD = float(os.getenv("AUTO_REBAL_TRIGGER_USD", "1000"))
AUTO_REBAL_TARGET_USD = float(os.getenv("AUTO_REBAL_TARGET_USD", "2000"))


async def _maybe_auto_rebalance(app: Application, chat_ids: list[int],
                                  reason: str = "") -> None:
    """If any exchange dropped below AUTO_REBAL_TRIGGER_USD ($1000) —
    trigger the same auto-rebalance the button starts, targeting
    AUTO_REBAL_TARGET_USD ($2000) on each exchange. Only one at a time,
    with a 60s cooldown."""
    global _AUTO_REBAL_LAST_TS
    import time as _t
    now = _t.time()
    if now - _AUTO_REBAL_LAST_TS < _AUTO_REBAL_MIN_GAP_SEC:
        log.info("auto-rebalance skip (%s): cooling down %.0fs",
                 reason, _AUTO_REBAL_MIN_GAP_SEC - (now - _AUTO_REBAL_LAST_TS))
        return
    if _AUTO_REBAL_LOCK.locked():
        log.info("auto-rebalance skip (%s): another rebalance in flight", reason)
        return
    async with _AUTO_REBAL_LOCK:
        _AUTO_REBAL_LAST_TS = now
        kd = keys_mod.load_keys()

        # PHASE 0 — COMBAT-READINESS NORMALIZE. Bitvavo trades EUR-pairs,
        # Gate/Binance trade USDT-pairs. Stray USDC on any of them is
        # dead weight for BUYs (phase_buy uses quote currency directly).
        # Normalize stragglers before the trigger check so USDC → EUR/USDT
        # happens as soon as it's spotted, not only during rebalance runs.
        async def _normalize(eid: str, inst) -> None:
            """Convert stray stables to the working currency:
              - Bitvavo BUYs use EUR → USDC and EURC both → EUR
              - Gate/Binance BUYs use USDT → USDC → USDT
            Silently handles what's below dust threshold.
            """
            try:
                b = await inst.fetch_balance()
            except Exception: return
            # Determine what to swap into what
            if eid == "bitvavo":
                target = "EUR"
                stables = [("USDC", "USDC/EUR"), ("EURC", "EURC/EUR")]
            else:
                target = "USDT"
                stables = [("USDC", "USDC/USDT")]
            # Funds parked here by an in-flight bridge route are NOT
            # stray — selling them breaks the route mid-hop. This is
            # exactly what killed a $736 transfer: leg 1 landed USDC on
            # Binance at 16:57 and the normalizer sold it at 17:01.
            try:
                import routing as _rt
                _reserved = _rt.transit_amount(eid)
            except Exception:
                _reserved = 0.0
            for asset, pair in stables:
                amt = float((b.get(asset) or {}).get("free") or 0)
                if asset == "USDC" and _reserved > 0:
                    amt = max(0.0, amt - _reserved)
                    if amt < 5:
                        log.info("normalize %s: skipping USDC — $%.2f is "
                                 "reserved for a bridge route", eid, _reserved)
                if amt < 5:                            # dust — skip
                    continue
                try:
                    if pair not in getattr(inst, "symbols", []) or []:
                        continue                        # market not listed
                    if eid == "bitvavo":
                        await inst.create_order(
                            pair, "market", "sell", amt, None,
                            {"operatorId": int(_t.time() * 1000)})
                    else:
                        await cex.place_order(
                            inst, pair, "market", "sell", amt, None)
                    log.info("normalize: %s swap %.2f %s → %s",
                             eid, amt, asset, target)
                    for cid in chat_ids:
                        try:
                            await app.bot.send_message(
                                cid,
                                f"🔁 normalize: {cex.pretty(eid)} "
                                f"{amt:.2f} {asset} → {target}",
                                parse_mode=ParseMode.HTML)
                        except Exception: pass
                except Exception as e:
                    log.debug("normalize %s %s: %s", eid, asset, e)

        # Uniform target on every exchange; trigger below $1000.
        below: list[tuple[str, float]] = []             # (eid, have_usd)
        for eid in cex.SUPPORTED_EXCHANGES:
            creds = kd.get(eid) or {}
            if not creds.get("apiKey"):
                continue
            try:
                inst = cex.get_private(eid, creds)
                await _normalize(eid, inst)              # swap USDC → EUR/USDT
                b = await inst.fetch_balance()          # re-read after swap
            except Exception as e:
                log.debug("auto-rebalance balance %s: %s", eid, e)
                continue
            if eid == "bitvavo":
                # Count only EUR (post-normalize USDC should be ~0).
                # BUYs on Bitvavo use EUR — USDC is dead weight.
                eur = float((b.get("EUR") or {}).get("free") or 0)
                fx = cex.get_fx_rate("EUR") or 1.17
                have = eur * fx
            else:
                # Gate/Binance BUYs use USDT — count only USDT.
                have = float((b.get("USDT") or {}).get("free") or 0)
            if have < AUTO_REBAL_TRIGGER_USD:
                below.append((eid, have))

        # HW USDC on ETH + Base — target so DEX-cycle (dex→bitvavo direction)
        # always has USDC on hand for Kyber USDC→base swap without needing
        # to wait for a CEX→HW hop first.
        _hw_eth_target = float(os.getenv("AUTO_REBAL_HW_ETH_USDC_TARGET", "1000"))
        _hw_eth_trigger = float(os.getenv("AUTO_REBAL_HW_ETH_USDC_TRIGGER", "500"))
        _hw_base_target = float(os.getenv("AUTO_REBAL_HW_BASE_USDC_TARGET", "1000"))
        _hw_base_trigger = float(os.getenv("AUTO_REBAL_HW_BASE_USDC_TRIGGER", "500"))
        # Read both chains first, then judge on the TOTAL. Judging each
        # chain in isolation made the bot spam a top-up every 2 min for
        # Ethereum ($0) while Base sat on $1585 — plenty of working
        # capital, just on the other chain. Only pull fresh money from a
        # CEX when the COMBINED HW stack is short.
        _hw_bal: dict[str, float] = {}
        for _hw_chain in ("ethereum", "base"):
            try:
                _wei = await dex_mod.wallet_token_balance(
                    _hw_chain, dex_mod.USDC_BY_CHAIN[_hw_chain]) or 0
                _hw_bal[_hw_chain] = _wei / 1e6           # USDC 6 decimals
            except Exception as _e:
                log.debug("hw usdc %s check: %s", _hw_chain, _e)
                _hw_bal[_hw_chain] = 0.0
        _hw_total = sum(_hw_bal.values())
        _hw_total_trigger = _hw_eth_trigger + _hw_base_trigger
        if _hw_total < _hw_total_trigger:
            # Genuinely short overall — flag whichever chain is emptier
            _worst = min(_hw_bal, key=_hw_bal.get)
            below.append((f"hw_{_worst}_usdc", _hw_bal[_worst]))
            log.info("auto-rebalance: HW total USDC $%.2f < trigger $%.0f "
                     "(eth $%.2f / base $%.2f)", _hw_total, _hw_total_trigger,
                     _hw_bal.get("ethereum", 0), _hw_bal.get("base", 0))
        else:
            log.info("auto-rebalance: HW total USDC $%.2f OK "
                     "(eth $%.2f / base $%.2f) — no CEX pull needed",
                     _hw_total, _hw_bal.get("ethereum", 0),
                     _hw_bal.get("base", 0))

        if not below:
            # Nobody is in emergency territory — but that is not the same
            # as capital being well placed. The trigger ($1000) sits far
            # under the target ($2000), so a venue can idle at $1057 and
            # look "fine" while the hot wallet holds thousands doing
            # nothing. Bitvavo is the anchor of every arb: if it is short
            # of target and HW has spare, push the spare out — bridging
            # across chains when the venue can't take our chain directly.
            try:
                await _deploy_idle_hw_capital(app, chat_ids, kd,
                                                _hw_bal, reason)
            except Exception as e:
                log.warning("idle-capital deploy: %s", e, exc_info=True)
            log.info("auto-rebalance skip (%s): every exchange + HW >= trigger",
                     reason)
            return
        # Announce + fire — override targets to uniform AUTO_REBAL_TARGET_USD
        summary = " · ".join(f"{cex.pretty(e)} ${h:.0f}" for e, h in below)
        for cid in chat_ids:
            try:
                await app.bot.send_message(
                    cid,
                    f"🔄 <b>Авто-ребаланс</b> ({reason})\n"
                    f"нижче ${AUTO_REBAL_TRIGGER_USD:.0f}: {summary}\n"
                    f"ціль скрізь ${AUTO_REBAL_TARGET_USD:.0f}",
                    parse_mode=ParseMode.HTML)
            except Exception: pass
        # Uniform targets — override env vars for this run
        os.environ["TARGET_GATE_USDT"] = str(AUTO_REBAL_TARGET_USD)
        os.environ["TARGET_BINANCE_USDT"] = str(AUTO_REBAL_TARGET_USD)
        os.environ["TARGET_BITVAVO_USDC"] = str(AUTO_REBAL_TARGET_USD)
        try:
            await _auto_rebalance_post_trade(app, chat_ids)
        except Exception as e:
            log.warning("_auto_rebalance_post_trade err: %s", e, exc_info=True)


async def _deploy_idle_hw_capital(app: Application, chat_ids: list[int],
                                    kd: dict, hw_bal: dict[str, float],
                                    reason: str) -> None:
    """Push spare hot-wallet USDC out to venues that are under target.

    The emergency path only fires when a venue drops below the TRIGGER
    ($1000). Between trigger and TARGET ($2000) nothing happened — so
    Bitvavo could sit at $1057, "fine" by that rule, while $2736 idled
    on Base. Bitvavo is where every arb is bought or sold; starving it
    while capital sleeps on-chain costs opportunities silently.

    Keeps the hot wallet's own reserve intact (it funds DEX cycles) and
    only deploys what is above it. Routes through `routing`, so a venue
    that can't accept our chain still gets funded via the Binance hop.
    """
    import routing as _routing
    hw_reserve = (float(os.getenv("AUTO_REBAL_HW_ETH_USDC_TARGET", "1000"))
                  + float(os.getenv("AUTO_REBAL_HW_BASE_USDC_TARGET", "1000")))
    hw_total = sum(hw_bal.values())
    spare = hw_total - hw_reserve
    min_deploy = float(os.getenv("DEPLOY_MIN_USD", "150"))

    # Read every venue once: who is short, and who is holding excess.
    # Capital strands on exchanges too, not only on-chain — after a
    # failed bridge leg $736 sat on Binance (over target) while Bitvavo
    # ran $943 short, and the trigger-based path ignored both.
    target = float(os.getenv("AUTO_REBAL_TARGET_USD", "2000"))
    gaps: list[tuple[str, float]] = []
    surplus: list[tuple[str, float]] = []
    for eid in cex.SUPPORTED_EXCHANGES:
        if not (kd.get(eid) or {}).get("apiKey"):
            continue
        try:
            inst = cex.get_private(eid, kd[eid])
            b = await inst.fetch_balance()
            if eid == "bitvavo":
                fx = cex.get_fx_rate("EUR") or 1.16
                have = float((b.get("EUR") or {}).get("free") or 0) * fx
            else:
                have = (float((b.get("USDT") or {}).get("free") or 0)
                        + float((b.get("USDC") or {}).get("free") or 0))
        except Exception as e:
            log.debug("idle-deploy balance %s: %s", eid, e)
            continue
        if target - have > min_deploy:
            gaps.append((eid, target - have))
        elif have - target > min_deploy:
            surplus.append((eid, have - target))
    if not gaps:
        log.info("idle-deploy: every venue at target (HW spare $%.0f)",
                 max(spare, 0))
        return
    if spare < min_deploy and not surplus:
        log.info("idle-deploy: %s short but no source — HW $%.0f = reserve, "
                 "no venue over target",
                 ",".join(e for e, _ in gaps), hw_total)
        return
    # Anchor exchange first, then the largest gap.
    gaps.sort(key=lambda g: (g[0] != "bitvavo", -g[1]))

    async def _say(text: str):
        for cid in chat_ids:
            try:
                await app.bot.send_message(cid, text,
                                            parse_mode=ParseMode.HTML)
            except Exception:
                pass

    def _step(line: str):
        log.info("idle-deploy: %s", line)
        asyncio.create_task(_say(line))

    remaining = max(spare, 0.0)
    for eid, gap in gaps:
        amount = min(gap, remaining) if remaining >= min_deploy else 0.0
        route = None
        if amount >= min_deploy:
            route = _routing.plan_route(eid, hw_bal, amount,
                                          min_leg_usd=min_deploy)
        if route:
            await _say(f"💸 <b>Розгін простою</b> ({reason})\n"
                       f"HW має ${hw_total:,.0f}, резерв ${hw_reserve:,.0f} → "
                       f"вільно ${spare:,.0f}\n"
                       f"{cex.pretty(eid)} нижче цілі на ${gap:,.0f}\n"
                       f"Маршрут: {_routing.describe(route, eid)}")
            try:
                res = await _routing.execute_route(route, eid, _step)
            except Exception as e:
                log.warning("idle-deploy route %s: %s", eid, e, exc_info=True)
                await _say(f"⚠️ Маршрут на {cex.pretty(eid)} впав: "
                           f"{type(e).__name__}: {str(e)[:120]}")
                continue
        elif surplus:
            # No spare on-chain, but another venue is over target.
            src_eid, src_amt = surplus[0]
            amount = min(gap, src_amt)
            if amount < min_deploy:
                continue
            await _say(f"💸 <b>Перерозподіл</b> ({reason})\n"
                       f"{cex.pretty(src_eid)} має ${src_amt:,.0f} понад ціль, "
                       f"{cex.pretty(eid)} не вистачає ${gap:,.0f}\n"
                       f"Маршрут: {src_eid} → HW → {eid} · ${amount:,.0f}")
            try:
                res = await _routing.move_cex_to_cex(src_eid, eid, amount,
                                                       _step)
            except Exception as e:
                log.warning("idle-deploy cex-move %s→%s: %s", src_eid, eid,
                            e, exc_info=True)
                await _say(f"⚠️ Перерозподіл {cex.pretty(src_eid)}→"
                           f"{cex.pretty(eid)} впав: {str(e)[:120]}")
                continue
            if res.get("ok"):
                surplus[0] = (src_eid, src_amt - res["delivered_usd"])
        else:
            log.info("idle-deploy: no route HW→%s for $%.0f", eid, amount)
            continue
        if res.get("ok"):
            delivered = res["delivered_usd"]
            if route:
                remaining -= delivered
                src = route.get("from_chain") or route.get("chain")
                hw_bal[src] = max(0.0, hw_bal.get(src, 0) - delivered)
            await _say(f"✅ {cex.pretty(eid)} отримав "
                       f"${delivered:,.2f} USDC")
        else:
            # log.error too, not just Telegram — the first failure of
            # this route was invisible in bot.log because `_say` only
            # talks to TG, which made the post-mortem far harder.
            log.error("idle-deploy: route HW→%s FAILED: %s",
                      eid, res.get("error"))
            await _say(f"⚠️ Не доставили на {cex.pretty(eid)}: "
                       f"{str(res.get('error'))[:150]}")
            break            # a broken hop will break for the next one too

    # ── HW cross-chain LAST ────────────────────────────────────────
    # Runs AFTER the venues are funded. Doing it first meant the bot
    # shuffled $204 between its own chains to hit a per-chain target
    # while Bitvavo — the anchor every buy goes through — sat at $38.
    # Exchanges that can't trade cost more than a lopsided wallet.
    try:
        import routing as _rt
        _per_chain_target = {
            "ethereum": float(os.getenv("AUTO_REBAL_HW_ETH_USDC_TARGET", "1000")),
            "base": float(os.getenv("AUTO_REBAL_HW_BASE_USDC_TARGET", "1000")),
        }
        _starved = [(c, _per_chain_target[c] - hw_bal.get(c, 0))
                    for c in _per_chain_target
                    if _per_chain_target[c] - hw_bal.get(c, 0) > min_deploy]
        _flush = [(c, hw_bal.get(c, 0) - _per_chain_target[c] * 0.5)
                  for c in _per_chain_target
                  if hw_bal.get(c, 0) - _per_chain_target[c] * 0.5 > min_deploy]
        for _dst_chain, _need in _starved:
            if not _flush:
                break
            _src_chain, _avail = _flush[0]
            if _src_chain == _dst_chain:
                continue
            _amt = min(_need, _avail)
            if _amt < min_deploy:
                continue
            _bridge = next((b for b in ("binance", "gate")
                            if _rt.accepts(b, _src_chain)
                            and _rt.accepts(b, _dst_chain)), None)
            if not _bridge:
                log.info("HW cross-chain %s→%s: no bridge exchange serves "
                         "both", _src_chain, _dst_chain)
                continue
            log.warning("HW cross-chain: %s has $%.0f, %s needs $%.0f "
                        "→ moving $%.0f via %s",
                        _src_chain, _avail, _dst_chain, _need, _amt, _bridge)
            for cid in chat_ids:
                try:
                    await app.bot.send_message(
                        cid,
                        f"🔀 <b>HW перекос</b>: {_src_chain} ${_avail:,.0f} · "
                        f"{_dst_chain} порожній\n"
                        f"Переганяю ${_amt:,.0f} через {_bridge}",
                        parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            def _st(line: str):
                log.info("hw-xchain: %s", line)
            # Ends on the WALLET, not on the exchange — execute_route's
            # bridge path would have shipped it onward to `via`.
            try:
                _res = await _rt.move_hw_cross_chain(
                    _src_chain, _dst_chain, _amt, _bridge, _st)
                if _res.get("ok"):
                    hw_bal[_src_chain] = hw_bal.get(_src_chain, 0) - _amt
                    hw_bal[_dst_chain] = hw_bal.get(_dst_chain, 0) + _amt
                    hw_total = sum(hw_bal.values())
                    spare = hw_total - hw_reserve
            except Exception as _e:
                log.warning("HW cross-chain move: %s", _e, exc_info=True)
    except Exception as _e:
        log.debug("HW cross-chain block: %s", _e)



async def _auto_rebalance_post_trade(app: Application, chat_ids: list[int]):
    """Auto-run rebalance after a completed trade. Route:
      source (excess) → hot wallet (USDC ETH) → dest (deficit)
      • On Gate/Binance: swap USDT→USDC before wd, then USDC→USDT after credit
      • On Bitvavo: USDC direct (Bitvavo is USDC-native)
    One concise TG summary at the end. Skips if delta < $20 anywhere."""
    kd = keys_mod.load_keys()
    t_gate = float(os.getenv("TARGET_GATE_USDT", "1000"))
    t_binance = float(os.getenv("TARGET_BINANCE_USDT", "1000"))
    t_bitvavo = float(os.getenv("TARGET_BITVAVO_USDC", "2000"))
    # Snapshot balances
    balances: dict = {}
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = kd.get(eid) or {}
        if not creds.get("apiKey"):
            continue
        try:
            inst = cex.get_private(eid, creds)
            b = await inst.fetch_balance()
        except Exception as e:
            log.debug("balance fetch %s: %s", eid, e)
            continue
        if eid == "bitvavo":
            eur = float((b.get("EUR") or {}).get("free") or 0)
            usdc = float((b.get("USDC") or {}).get("free") or 0)
            fx = cex.get_fx_rate("EUR") or 1.16
            balances[eid] = {"usdc": usdc, "eur": eur,
                              "total": usdc + eur * fx,
                              "target": t_bitvavo}
        else:
            usdt = float((b.get("USDT") or {}).get("free") or 0)
            usdc = float((b.get("USDC") or {}).get("free") or 0)
            balances[eid] = {"usdt": usdt, "usdc": usdc,
                              "total": usdt + usdc,
                              "target": t_gate if eid == "gate" else t_binance}
    if not balances:
        return
    # Compute deltas — positive = deficit, negative = excess
    deltas = {eid: bal["target"] - bal["total"] for eid, bal in balances.items()}
    total_excess = sum(-d for d in deltas.values() if d < -20)
    total_deficit = sum(d for d in deltas.values() if d > 20)
    if total_excess < 20 and total_deficit < 20:
        return                                              # already balanced, no-op silent
    HOT = dex_mod.HOT_WALLET_ADDRESS
    # Snapshot hot wallet USDC on ETH — shown as reserve buffer
    try:
        hot_usdc_eth = (await dex_mod.wallet_token_balance(
            "ethereum", dex_mod.USDC_BY_CHAIN["ethereum"]) or 0) / 1e6
    except Exception:
        hot_usdc_eth = 0.0
    summary_lines = [f"🔄 <b>Авто-ребаланс</b> (після трейду)"]
    summary_lines.append(f"Δ excess {total_excess:.0f} · deficit {total_deficit:.0f} · "
                          f"hot USDC ETH {hot_usdc_eth:.0f}")

    # Live-status: send the initial message once per chat, then edit it
    # after every appended step so the user watches progress remotely.
    msg_ids: dict[int, int] = {}
    for cid in chat_ids:
        try:
            m = await app.bot.send_message(cid, "\n".join(summary_lines),
                                             parse_mode=ParseMode.HTML)
            msg_ids[cid] = m.message_id
        except Exception as e:
            log.debug("auto-reb initial send %s: %s", cid, e)
    async def _push():
        text = "\n".join(summary_lines)
        for cid, mid in msg_ids.items():
            try:
                await app.bot.edit_message_text(text=text, chat_id=cid,
                                                 message_id=mid,
                                                 parse_mode=ParseMode.HTML)
            except Exception: pass
    def _step(line: str):
        summary_lines.append(line)
        log.info("auto-rebal step: %s", line)              # also to bot.log
        # Best-effort — don't block phase logic on edit failure
        asyncio.create_task(_push())

    # Route choice: Bitvavo in play → USDC ETH; Gate↔Binance only → USDT BSC.
    bitvavo_in_play = "bitvavo" in deltas and abs(deltas["bitvavo"]) > 20
    use_usdc_eth = bitvavo_in_play

    # ─── Phase A: excess → hot wallet ────────────────────────────────
    _step("<b>Фаза A</b>: біржа → hot wallet")
    hot_deltas: dict[str, float] = {}                       # eid → amount moved out
    for eid, d in deltas.items():
        if d >= -20:
            continue
        excess = -d
        try:
            inst = cex.get_private(eid, kd[eid])
            if eid == "bitvavo":
                # Bitvavo only supports USDC ETH — always this path
                bv = balances["bitvavo"]
                need_buy = max(0.0, excess - bv["usdc"])
                if need_buy > 5:
                    _step(f"  · Bitvavo: докупляю {need_buy:.2f} USDC з EUR")
                    await inst.create_order("USDC/EUR", "market", "buy",
                                             need_buy, None,
                                             {"operatorId": int(time.time() * 1000)})
                    for _ in range(10):
                        b2 = await inst.fetch_balance()
                        if float((b2.get("USDC") or {}).get("free") or 0) >= excess * 0.98:
                            break
                        await asyncio.sleep(2)
                b2 = await inst.fetch_balance()
                avail = float((b2.get("USDC") or {}).get("free") or 0)
                wd_amt = min(excess, avail)
                if wd_amt >= 1.2:
                    _step(f"  · Bitvavo: WD {wd_amt:.2f} USDC (ETH) → HW…")
                    # via withdraw_robust: handles Bitvavo errorCode 204
                    # ("network parameter is not supported" on single-net
                    # coins) and post-hoc verifies against fetch_withdrawals
                    # when the API errors on an accepted request.
                    await cex.withdraw_robust(inst, "USDC", wd_amt,
                                                HOT, None, None)
                    hot_deltas[eid] = wd_amt
                    _step(f"  ✅ Bitvavo WD {wd_amt:.0f} USDC broadcast")
            elif use_usdc_eth:
                # Gate/Binance → swap USDT→USDC via IOC limit (Gate rejects
                # market buy without price; USDC/USDT is tight so 1.005 caps
                # slippage at 0.5%).
                _step(f"  · {cex.pretty(eid)}: swap {excess:.2f} USDT→USDC")
                await cex.place_order(inst, "USDC/USDT", "limit", "buy",
                                       excess, 1.005,
                                       {"timeInForce": "IOC"})
                await asyncio.sleep(3)
                b2 = await inst.fetch_balance()
                avail_usdc = float((b2.get("USDC") or {}).get("free") or 0)
                wd_amt = min(excess, avail_usdc)
                if wd_amt >= 1.2:
                    _step(f"  · {cex.pretty(eid)}: WD {wd_amt:.2f} USDC (ETH) → HW…")
                    await cex.withdraw_robust(inst, "USDC", wd_amt, HOT, None, "ETH")
                    hot_deltas[eid] = wd_amt
                    _step(f"  ✅ {cex.pretty(eid)} WD {wd_amt:.0f} USDC broadcast")
            else:
                # Gate ↔ Binance only — direct USDT on BSC (cheaper)
                wd_amt = excess
                _step(f"  · {cex.pretty(eid)}: WD {wd_amt:.2f} USDT (BSC) → HW…")
                await cex.withdraw_robust(inst, "USDT", wd_amt, HOT, None, "BSC")
                hot_deltas[eid] = wd_amt
                _step(f"  ✅ {cex.pretty(eid)} WD {wd_amt:.0f} USDT broadcast")
        except Exception as e:
            log.warning("auto-rebal Phase A %s excess flow err: %s", eid, e, exc_info=True)
            _step(f"  ⚠️ {cex.pretty(eid)} excess flow: {type(e).__name__}: {str(e)[:120]}")

    # ─── Phase B: wait for hot wallet USDC to arrive, then send to deficits
    if total_deficit > 20:
        if use_usdc_eth:
            transit_contract = dex_mod.USDC_BY_CHAIN["ethereum"]
            transit_chain = "ethereum"; transit_sym = "USDC"; transit_dec = 6
        else:
            transit_contract = dex_mod.USDT_BY_CHAIN["bsc"]
            transit_chain = "bsc"; transit_sym = "USDT"; transit_dec = 18
        # Phase B only ever looks at the TRANSIT chain. When the wallet's
        # money is on a different one it reported "baseline hot (0 USDC),
        # pool 0 < deficit 1962" and gave up — with $3,310 sitting on Base
        # and Bitvavo starving at $38. Pull the funds across first.
        if use_usdc_eth:
            try:
                import routing as _rt2
                _have_transit = ((await dex_mod.wallet_token_balance(
                    transit_chain, transit_contract) or 0) / (10 ** transit_dec))
                _short = total_deficit - _have_transit
                if _short > 20:
                    _donor = None
                    for _c in ("base", "arbitrum", "optimism", "polygon"):
                        if _c == transit_chain:
                            continue
                        _u = dex_mod.USDC_BY_CHAIN.get(_c)
                        if not _u:
                            continue
                        _av = (await dex_mod.wallet_token_balance(_c, _u) or 0) / 1e6
                        if _av > _short * 1.01:
                            _donor = (_c, _av)
                            break
                    if _donor:
                        _c, _av = _donor
                        _via = next((b for b in ("binance", "gate")
                                     if _rt2.accepts(b, _c)
                                     and _rt2.accepts(b, transit_chain)), None)
                        if _via:
                            _step(f"  · транзит порожній (${_have_transit:,.0f}), "
                                  f"тягну ${_short:,.0f} з {_c} через {_via}…")
                            _r = await _rt2.move_hw_cross_chain(
                                _c, transit_chain, _short, _via, _step)
                            if _r.get("ok"):
                                _step(f"  ✅ на {transit_chain} прийшло "
                                      f"${_r['delivered_usd']:,.2f}")
                            else:
                                _step(f"  ⚠️ не перекинув: "
                                      f"{str(_r.get('error'))[:110]}")
                        else:
                            _step(f"  ⚠️ ${_av:,.0f} на {_c}, але немає біржі "
                                  f"що мостить {_c}→{transit_chain}")
            except Exception as _e:
                log.warning("Phase B transit top-up: %s", _e, exc_info=True)
        baseline = await dex_mod.wallet_token_balance(transit_chain, transit_contract) or 0
        baseline_h = baseline / (10 ** transit_dec)
        arrived_h = 0.0
        if hot_deltas:
            expected_delta = sum(hot_deltas.values())
            _step(f"<b>Фаза B</b>: чекаю {expected_delta:.0f} {transit_sym} "
                  f"на HW ({transit_chain})…")
            deadline = time.time() + 2700  # 45 min for slow ETH mainnet
            while time.time() < deadline:
                # Poll every 20s (was 5s). ETH block time 12s, so at most
                # 1-2 block delay in detection vs 4× fewer RPC calls.
                await asyncio.sleep(20)
                cur = await dex_mod.wallet_token_balance(transit_chain, transit_contract) or 0
                delta = (cur - baseline) / (10 ** transit_dec)
                if delta >= expected_delta * 0.9:
                    arrived_h = delta
                    _step(f"  ✅ прийшло {arrived_h:.0f} {transit_sym} на HW")
                    break
            if arrived_h < 5:
                _step(f"  ⚠️ {transit_sym} не дочекались на hot ({expected_delta:.0f})")
        else:
            _step(f"<b>Фаза B</b>: використовую baseline hot ({baseline_h:.0f} {transit_sym})")
        pool_h = arrived_h + baseline_h
        if pool_h < total_deficit:
            _step(f"  ⚠️ pool {pool_h:.0f} < deficit {total_deficit:.0f} — часткове заповнення")
        if pool_h >= 5:
            # ─── B1: broadcast ALL sends sequentially (nonce order from
            # a single HW), then in B2 wait credits + swap in parallel.
            # This is the big speedup — sequential wait for each dest
            # credit turns 2×5min into ~5min total.
            pending: list[dict] = []                        # per-dest work
            net = "ETH" if transit_chain == "ethereum" else "BSC"
            for eid, d in sorted(deltas.items(), key=lambda kv: -kv[1]):
                if d <= 20:
                    continue
                need = d
                give = min(need, pool_h)
                if give < 5:
                    continue
                try:
                    inst = cex.get_private(eid, kd[eid])
                    dep = await cex.fetch_deposit_address_robust(inst, transit_sym, net)
                    dst = (dep or {}).get("address") if dep else None
                    if not dst:
                        _step(f"  ⚠️ {cex.pretty(eid)} no {transit_sym} deposit addr")
                        continue
                    _step(f"  · hot → {cex.pretty(eid)}: send {give:.0f} {transit_sym} ({net})…")
                    r_send = await dex_mod.send_token(transit_chain,
                                                       transit_contract,
                                                       int(give * (10 ** transit_dec)), dst)
                    if not r_send.get("ok"):
                        _step(f"  ⚠️ hot→{cex.pretty(eid)}: {r_send.get('error','?')[:60]}")
                        continue
                    _step(f"  ✅ hot → {cex.pretty(eid)} +{give:.0f} {transit_sym} broadcast "
                          f"({r_send.get('tx_hash','')[:14]}…)")
                    pool_h -= give
                    pending.append({"eid": eid, "inst": inst, "give": give,
                                     "tx": r_send.get("tx_hash", "")})
                except Exception as e:
                    log.warning("auto-rebal Phase B1 %s err: %s", eid, e, exc_info=True)
                    _step(f"  ⚠️ {cex.pretty(eid)} broadcast: {type(e).__name__}: {str(e)[:100]}")

            # ─── B2: wait credit + swap for each pending dest in PARALLEL
            async def _finalize(pd: dict):
                eid = pd["eid"]; inst = pd["inst"]; give = pd["give"]
                try:
                    b0 = await inst.fetch_balance()
                    base0 = float((b0.get("USDC") or {}).get("free") or 0)
                    _step(f"    · {cex.pretty(eid)}: чекаю кредит {give:.0f} USDC…")
                    deadline2 = time.time() + 900
                    credited = False
                    while time.time() < deadline2:
                        await asyncio.sleep(5)
                        b1 = await inst.fetch_balance()
                        cur = float((b1.get("USDC") or {}).get("free") or 0)
                        if cur - base0 >= give * 0.9:
                            credited = True; break
                    if not credited:
                        _step(f"    ⚠️ {cex.pretty(eid)} USDC не зайшло за 15хв")
                        return
                    avail = float((await inst.fetch_balance()).get("USDC", {}).get("free") or 0)
                    if eid == "bitvavo":
                        try:
                            so = await inst.create_order("USDC/EUR", "market", "sell",
                                                          avail, None,
                                                          {"operatorId": int(time.time() * 1000)})
                            got = float(so.get("cost") or 0)
                            _step(f"    🔁 Bitvavo swap {avail:.0f} USDC → {got:.0f} EUR")
                        except Exception as e:
                            log.warning("auto-rebal bitvavo swap: %s", e, exc_info=True)
                            _step(f"    ⚠️ Bitvavo swap USDC/EUR: {str(e)[:100]}")
                    else:                                    # gate / binance → USDT
                        try:
                            so = await cex.place_order(inst, "USDC/USDT",
                                                        "market", "sell",
                                                        avail, None)
                            got = float(so.get("cost") or 0)
                            _step(f"    🔁 {cex.pretty(eid)} swap {avail:.0f} USDC → {got:.0f} USDT")
                        except Exception as e:
                            log.warning("auto-rebal %s swap: %s", eid, e, exc_info=True)
                            _step(f"    ⚠️ {cex.pretty(eid)} swap USDC/USDT: {str(e)[:100]}")
                except Exception as e:
                    log.warning("auto-rebal Phase B2 %s err: %s", eid, e, exc_info=True)
                    _step(f"  ⚠️ {cex.pretty(eid)} credit/swap: {type(e).__name__}: {str(e)[:100]}")

            if pending:
                _step(f"  ⏳ паралельно чекаю кредити на "
                      f"{', '.join(cex.pretty(p['eid']) for p in pending)}…")
                await asyncio.gather(*(_finalize(p) for p in pending),
                                       return_exceptions=True)

    # ─── Phase B-bridge: destinations unreachable on the transit chain ──
    # Phase B can only send along one chain. Bitvavo takes USDC on
    # Ethereum only, so while the working capital sat on Base it was
    # simply unreachable — the rebalancer watched Bitvavo drain to 681
    # EUR next to $2736 it could not move. Route those through an
    # exchange that bridges: HW(base) → Binance → HW(eth) → Bitvavo.
    try:
        import routing as _routing
        # transit_chain only exists when Phase B ran (total_deficit > 20).
        # When it didn't, nothing was sent, so every short destination
        # still needs a route.
        _transit = locals().get("transit_chain")
        # `deltas` is the PRE-Phase-B picture. Using it directly double-
        # funded any destination Phase B had already served: Gate's
        # USDC is ERC20-only, so with a BSC transit chain it looked
        # unserved here even though B1 had just sent it the full deficit
        # in USDT/BSC — and a second full deficit went out.
        # Subtract what B1 actually broadcast, then re-read balances.
        _sent_b1: dict[str, float] = {}
        for _p in (locals().get("pending") or []):
            _sent_b1[_p["eid"]] = _sent_b1.get(_p["eid"], 0) + _p.get("give", 0)
        _still_short: list[tuple[str, float]] = []
        for eid, d in deltas.items():
            remaining = d - _sent_b1.get(eid, 0.0)
            if remaining <= 20:
                continue                      # covered (or nearly) by B1
            if _transit and _routing.accepts(eid, _transit):
                continue                      # Phase B's chain reaches it
            _still_short.append((eid, remaining))
        # Verify against live balances before moving more money — B2's
        # credits may have landed while we were computing.
        if _still_short:
            _verified: list[tuple[str, float]] = []
            for eid, need in _still_short:
                try:
                    _i = cex.get_private(eid, kd[eid])
                    _b = await _i.fetch_balance()
                    _have = float((_b.get("USDT") or {}).get("free") or 0) \
                        + float((_b.get("USDC") or {}).get("free") or 0)
                    if eid == "bitvavo":
                        _have += (float((_b.get("EUR") or {}).get("free") or 0)
                                  * (cex.get_fx_rate("EUR") or 1.16))
                    _tgt = float((balances.get(eid) or {}).get("target") or 0)
                    _gap = _tgt - _have
                    if _gap > 20:
                        _verified.append((eid, min(need, _gap)))
                    else:
                        _step(f"  · {cex.pretty(eid)} вже покритий "
                              f"(${_have:,.0f}/${_tgt:,.0f}) — міст не потрібен")
                except Exception as _e:
                    log.warning("bridge re-check %s: %s", eid, _e)
            _still_short = _verified
        if _still_short:
            _hw_now2 = {}
            for _c in ("ethereum", "base", "arbitrum", "optimism", "polygon"):
                _u = dex_mod.USDC_BY_CHAIN.get(_c)
                if not _u:
                    continue
                try:
                    _hw_now2[_c] = (await dex_mod.wallet_token_balance(
                        _c, _u) or 0) / 1e6
                except Exception:
                    pass
            for eid, need in _still_short:
                route = _routing.plan_route(eid, _hw_now2, need)
                if not route:
                    _step(f"  ⚠️ {cex.pretty(eid)} треба ${need:,.0f}, "
                          f"але маршруту з HW немає")
                    continue
                _step(f"<b>Фаза B-міст</b> ({cex.pretty(eid)}): "
                      f"{_routing.describe(route, eid)}")
                res = await _routing.execute_route(route, eid, _step)
                if res.get("ok"):
                    _step(f"  ✅ {cex.pretty(eid)} отримав "
                          f"${res['delivered_usd']:,.2f} USDC")
                    # spend it down from our local view so a second
                    # destination doesn't try to use the same funds
                    _hw_now2[route.get("from_chain") or route.get("chain")] = \
                        max(0.0, _hw_now2.get(
                            route.get("from_chain") or route.get("chain"), 0)
                            - route["amount"])
                else:
                    _step(f"  ⚠️ {cex.pretty(eid)} маршрут не пройшов: "
                          f"{str(res.get('error'))[:120]}")
    except Exception as _e:
        log.warning("Phase B-bridge err: %s", _e, exc_info=True)

    # ─── Phase C: top-up HW USDC on ETH + Base to target ─────────────
    # DEX-cycle (dex→bitvavo direction) starts with a Kyber USDC→base
    # swap on HW. If HW is dry, first arb of the day waits for a WD hop.
    # Maintain a rolling USDC cache on HW on BOTH ETH and Base chains.
    _hw_topup_plan = [
        ("ethereum", "ETH",
         float(os.getenv("AUTO_REBAL_HW_ETH_USDC_TARGET",  "1000"))),
        ("base",     "BASE",
         float(os.getenv("AUTO_REBAL_HW_BASE_USDC_TARGET", "1000"))),
    ]
    # Gate on the COMBINED stack first — pulling $1000 from a CEX to
    # Ethereum while Base already holds $1585 just moves idle money
    # around and spams the chat every 2 min. Only top up when the whole
    # HW stack is short of the combined target.
    try:
        _hw_now = {}
        for _c, _n, _t in _hw_topup_plan:
            _hw_now[_c] = (await dex_mod.wallet_token_balance(
                _c, dex_mod.USDC_BY_CHAIN[_c]) or 0) / 1e6
        _hw_sum = sum(_hw_now.values())
        _hw_target_sum = sum(t for _, _, t in _hw_topup_plan)
        if _hw_sum >= _hw_target_sum * 0.9:
            log.info("Phase C skip: HW total USDC $%.2f >= 90%% of combined "
                     "target $%.0f (%s)", _hw_sum, _hw_target_sum,
                     " / ".join(f"{c}=${v:.0f}" for c, v in _hw_now.items()))
            _step(f"<b>Фаза C</b>: HW разом ${_hw_sum:,.0f} "
                  f"({' / '.join(f'{c}=${v:,.0f}' for c, v in _hw_now.items())})"
                  f" — достатньо, CEX не чіпаю")
            _hw_topup_plan = []
    except Exception as _e:
        log.debug("Phase C combined check: %s", _e)
    for _hw_chain, _hw_net, _hw_target in _hw_topup_plan:
        try:
            _hw_bal = (await dex_mod.wallet_token_balance(
                _hw_chain, dex_mod.USDC_BY_CHAIN[_hw_chain]) or 0) / 1e6
            _need = _hw_target - _hw_bal
            if _need <= 20:
                log.info("hw %s USDC $%.2f already at/near $%.0f target",
                         _hw_chain.upper(), _hw_bal, _hw_target)
                continue
            _step(f"<b>Фаза C ({_hw_chain.upper()})</b>: HW USDC $%.2f → "
                  f"ціль $%.0f (треба +$%.2f)"
                  % (_hw_bal, _hw_target, _need))
            # Find donor CEX with excess USDT above its target
            _donor = None
            _donor_usdt = 0.0
            for eid, bal in balances.items():
                if eid == "bitvavo": continue          # bitvavo uses EUR
                _excess_usdt = (float(bal.get("usdt", 0))
                                - float(bal.get("target", 0)))
                if _excess_usdt > _need * 1.02 and _excess_usdt > _donor_usdt:
                    _donor = eid
                    _donor_usdt = _excess_usdt
            if not _donor:
                _step(f"  ⚠️ жоден CEX не має надлишку USDT для +$%.2f "
                      f"({_hw_net}) — skip" % _need)
                continue
            _step(f"  · {cex.pretty(_donor)}: swap {_need:.2f} USDT→USDC, "
                  f"WD to HW ({_hw_net})…")
            try:
                inst_d = cex.get_private(_donor, kd[_donor])
                await cex.place_order(inst_d, "USDC/USDT", "limit", "buy",
                                        _need, 1.005, {"timeInForce": "IOC"})
                await asyncio.sleep(3)
                b2 = await inst_d.fetch_balance()
                avail = float((b2.get("USDC") or {}).get("free") or 0)
                _wd = min(_need, avail)
                if _wd >= 5:
                    await cex.withdraw_robust(inst_d, "USDC", _wd,
                                                HOT, None, _hw_net)
                    _step(f"  ✅ {cex.pretty(_donor)} WD {_wd:.2f} USDC → "
                          f"HW ({_hw_net})")
                else:
                    _step(f"  ⚠️ {cex.pretty(_donor)} avail USDC "
                          f"{avail:.2f} < potrebne")
            except Exception as e:
                log.warning("hw %s top-up %s err: %s",
                            _hw_chain, _donor, e, exc_info=True)
                _step(f"  ⚠️ HW {_hw_net} top-up err: "
                      f"{type(e).__name__}: {str(e)[:80]}")
        except Exception as e:
            log.warning("HW %s top-up phase err: %s",
                        _hw_chain, e, exc_info=True)

    _step("<b>Готово</b>.")
    await _push()                                            # ensure final state is in TG


async def _run_executor(app: Application, plan_key: str, chat_ids: list[int]):
    """Build TradePlan from stashed alert/sizing and run the executor,
    streaming per-step status to chat_ids. Registers the task so /kill
    can cancel it mid-flight."""
    payload = _PENDING_PLANS.pop(plan_key, None)
    if not payload:
        return
    ex = executor.instance()

    async def status_cb(receipt, msg):
        for cid in chat_ids:
            try:
                await app.bot.send_message(
                    chat_id=cid, text=msg, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.debug("status cb send: %s", e)
    ex.wire_status(status_cb)
    try:
        plan = await ex.plan(payload["alert"], payload["sizing"])
    except Exception as e:
        for cid in chat_ids:
            try:
                await app.bot.send_message(cid, f"❌ plan build failed: {e}")
            except Exception:
                pass
        return
    task = asyncio.current_task()
    if task:
        executor.register_task(task)
    try:
        await ex.run(plan)
    except asyncio.CancelledError:
        for cid in chat_ids:
            try:
                await app.bot.send_message(
                    cid, f"🛑 <b>{plan.base}</b> cancelled mid-flight by /kill",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        raise


async def cb_execute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Executing…")
    if not _is_allowed(q.from_user.id):
        return
    if executor.kill_active():
        await q.message.reply_text("🛑 /kill active — execution disabled.")
        return
    _, plan_key = q.data.split(":", 1)
    subs = list(_HUNTER.subs) if _HUNTER else [q.message.chat_id]
    asyncio.create_task(_run_executor(ctx.application, plan_key, subs))


# ─── Interactive step-by-step: Check route → pick size → approve each phase ───

_SESSIONS: dict[str, dict] = {}                            # sid -> {plan, session, subs}
# Offers awaiting a tap. Deliberately separate from _SESSIONS: nothing
# here holds inventory, so it must never block a per-base auto-run, the
# rebalance watch, or attract the zombie pruner. Promoted into _SESSIONS
# only when the user actually approves the buy.
_PENDING_BUTTONS: dict[str, dict] = {}
# Set once at startup so background loops can reach Telegram without
# threading the Application through every call site.
_APP: Application | None = None
_SESSION_SKIP: dict[str, set[str]] = {}                 # sid → {phase_names_to_skip}
_SESSION_CURRENT_PHASE: dict[str, str] = {}             # sid → current phase name
_SESSIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "sessions.pkl")


def _persist_sessions():
    """Snapshot _SESSIONS to disk so a bot restart mid-flight doesn't
    kill retry/manual-continuation buttons."""
    try:
        import pickle
        with open(_SESSIONS_FILE, "wb") as f:
            pickle.dump(_SESSIONS, f)
    except Exception as e:
        log.debug("sessions persist: %s", e)


_PURGED_ZOMBIES: list[dict] = []


def _load_sessions() -> None:
    """Read snapshot on startup — restores in-flight InteractiveSessions
    with their plans, receipts, subs. Auto-drops zombie sessions where
    the buy never landed any funds (filled_qty=0) — they'd otherwise
    block future auto-runs on the same base forever.

    Purged dex_cycle entries queue into _PURGED_ZOMBIES for
    notify_purged_zombies() to report once the bot can actually send
    messages (this runs before Application.initialize()). Without this,
    a cycle interrupted mid-BUY by a restart leaves "BUY Bitvavo…" as the
    last message forever — it looks stuck/unfinished, but nothing was
    ever going to follow up: the task that would have posted the result
    died with the old process.
    """
    try:
        import pickle
        if not os.path.exists(_SESSIONS_FILE):
            return
        with open(_SESSIONS_FILE, "rb") as f:
            data = pickle.load(f)
        if not isinstance(data, dict):
            return
        purged = 0
        kept = {}
        for sid, entry in data.items():
            sess = entry.get("session")
            fq = getattr(sess, "filled_qty", 0) if sess else 0
            if fq > 0:
                kept[sid] = entry
            else:
                purged += 1
                if entry.get("kind") == "dex_cycle":
                    plan = entry.get("plan")
                    _PURGED_ZOMBIES.append({
                        "run_id": sid,
                        "base": getattr(plan, "base", None) or "?",
                        "chat_ids": entry.get("chat_ids") or [],
                    })
        _SESSIONS.update(kept)
        if purged:
            log.info("sessions: purged %d zombies (filled=0), restored %d",
                     purged, len(kept))
            _persist_sessions()
        else:
            log.info("sessions: restored %d in-flight sessions", len(kept))
    except Exception as e:
        log.warning("sessions load: %s", e)


async def notify_purged_zombies(app) -> None:
    """Report DEX cycles still waiting on a BUY fill when the bot last
    restarted, so a chat that last saw "BUY Bitvavo…" gets a definite
    answer instead of permanent silence."""
    for z in _PURGED_ZOMBIES:
        text = (f"⚠️ <b>DEX cycle #{z['run_id']}</b> ({z['base']}) перервано "
                f"перезапуском — купівля не встигла заповнитись, нічого не куплено.")
        for cid in z["chat_ids"]:
            try:
                await app.bot.send_message(cid, text, parse_mode=ParseMode.HTML)
            except Exception as e:
                log.warning("notify purged zombie %s -> %s failed: %s",
                            z["run_id"], cid, e)
    _PURGED_ZOMBIES.clear()


async def _orphan_tokens_loop_DISABLED():
    """(disabled per user feedback — all logic stays inside sessions)"""
    return
    import time as _t
    seen_orphans: dict[str, float] = {}
    while True:
        try:
            await asyncio.sleep(30)
            # 1) Get on-chain balances (auto-discover) — use dex snapshot
            snap = await dex_mod.wallet_snapshot(price_map=None, min_usd=1.0)
            active_bases = {(s.get("plan").base if s.get("plan") else "")
                             for s in _SESSIONS.values()}
            # Iterate all chains + symbols
            for chain, entries in (snap.get("chains") or {}).items():
                for sym, amt, usd in entries:
                    if sym in ("USDC", "USDT", "ETH", "BNB", "POL", "SOL", "AVAX"):
                        continue                            # stables/gas
                    if sym in active_bases:
                        continue                            # session handling it
                    if usd < 5:
                        continue
                    # Find a recent trade receipt for this base
                    recent = _find_recent_trade(sym, max_age_sec=7200)
                    if not recent:
                        # No plan reference — just log
                        if sym not in seen_orphans:
                            log.warning("orphan token on hot wallet: %s %f (~$%.2f) on %s — no recent trade to match",
                                        sym, amt, usd, chain)
                            seen_orphans[sym] = _t.time()
                        continue
                    # Match! Forward to destination
                    log.info("orphan-forward: %s %f → %s per receipt %s",
                             sym, amt, recent["sell_eid"], recent["trade_id"])
                    asyncio.create_task(_orphan_forward_and_sell(
                        sym, chain, recent["sell_eid"], recent["sell_sym"]))
        except Exception as e:
            log.debug("orphan loop: %s", e)


def _find_recent_trade(base: str, max_age_sec: float = 7200) -> dict | None:
    """Look up trade receipts JSONL for the latest trade with this base."""
    import time as _t
    trade_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "trades.jsonl")
    if not os.path.exists(trade_path):
        return None
    try:
        with open(trade_path, encoding="utf-8") as f:
            lines = f.readlines()
        cutoff = _t.time() - max_age_sec
        for line in reversed(lines):
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("base") != base.upper():
                continue
            ts = d.get("finished_ts") or d.get("started_ts") or 0
            if ts < cutoff:
                return None
            return d
    except Exception:
        pass
    return None


async def _orphan_forward_and_sell(base: str, chain: str,
                                    dest_eid: str, dest_sym: str):
    """Send orphan token from hot wallet → destination exchange, wait
    for credit, then market-sell."""
    kd = keys_mod.load_keys()
    creds = kd.get(dest_eid) or {}
    if not creds.get("apiKey"):
        log.warning("orphan-forward %s: no keys for %s", base, dest_eid)
        return
    inst = cex.get_private(dest_eid, creds)
    try:
        # Contract lookup via CG (multi-chain aware)
        cg_map = dex_mod._cg_contracts_for_chain(chain)
        contract = cg_map.get(base.upper())
        if not contract:
            log.warning("orphan-forward %s: no CG contract on %s", base, chain)
            return
        bal = await dex_mod.wallet_token_balance(chain, contract) or 0
        if bal <= 0:
            return
        # Fetch decimals via ERC20 call — assume 18 default
        # (we could query contract, but wallet_snapshot already handled it)
        # For SPL/BSC BEP-tokens, may need chain-specific decimals
        # Get dest network label (ETH/BSC/etc)
        net_map = {"ethereum": "ETH", "bsc": "BSC", "polygon": "MATIC",
                   "arbitrum": "ARBITRUM", "base": "BASE",
                   "optimism": "OPTIMISM", "avalanche": "AVAXC"}
        net = net_map.get(chain, chain.upper())
        dep = await cex.fetch_deposit_address_robust(inst, base, net)
        dst = (dep or {}).get("address")
        if not dst:
            log.warning("orphan-forward %s: no deposit addr on %s", base, dest_eid)
            return
        r = await dex_mod.send_token(chain, contract, bal, dst)
        if not r.get("ok"):
            log.warning("orphan-forward %s send err: %s", base, r.get("error"))
            return
        log.info("orphan-forward %s: sent %s → %s tx=%s",
                 base, bal, dest_eid, r["tx_hash"][:20])
        # Wait for destination credit and sell
        import time as _t
        b0 = await inst.fetch_balance()
        base0 = float((b0.get(base) or {}).get("free") or 0)
        deadline = _t.time() + 2700
        while _t.time() < deadline:
            await asyncio.sleep(5)
            b = await inst.fetch_balance()
            cur = float((b.get(base) or {}).get("free") or 0)
            if cur - base0 >= 1:
                amt = int(cur)
                try:
                    if dest_eid == "bitvavo":
                        order = await cex.place_order(inst, dest_sym, "market", "sell",
                                                       amt, None)
                    else:
                        order = await inst.create_order(dest_sym, "market", "sell",
                                                        amt, None)
                    got = float(order.get("cost") or 0)
                    log.info("orphan-sell %s: %s → %.2f %s", base, amt, got,
                             dest_sym.split("/")[1])
                except Exception as e:
                    log.warning("orphan-sell %s err: %s", base, e)
                return
    finally:
        try: await inst.close()
        except: pass


async def _recover_paused_sessions_loop():
    """Every 3 min, scan _SESSIONS for sessions that have real funds
    moved (filled_qty > 0) but are stuck in a paused state (no active
    task advancing them). Re-fire the appropriate phase — if the
    underlying bug is now fixed (e.g. contract lookup) the session
    naturally completes.

    This means: funds that arrived on hot wallet from a stuck withdraw
    step get auto-forwarded to destination + sold, without the user
    having to tap 'Retry' every time."""
    import time as _t
    from executor import instance as _ex
    _in_progress: set[str] = set()
    while True:
        try:
            await asyncio.sleep(180)
            ex = _ex()
            for sid, entry in list(_SESSIONS.items()):
                if sid in _in_progress:
                    continue
                sess = entry.get("session")
                plan = entry.get("plan")
                if not (sess and plan):
                    continue
                fq = getattr(sess, "filled_qty", 0)
                if fq <= 0:
                    continue                                # zombie handled by prune loop
                phase = getattr(sess, "phase", "")
                # Only auto-recover sessions past BUY (funds real).
                # Determine which phase to re-fire.
                if phase in ("AWAIT_APPROVE_WITHDRAW", "AWAIT_APPROVE_BUY"):
                    # Actually phase_buy is done (filled_qty>0), retry withdraw
                    async def _run_wd(_sid, _sess, _plan):
                        _in_progress.add(_sid)
                        try:
                            log.info("auto-recover: %s → phase_withdraw_and_wait", _sid)
                            ok = await ex.phase_withdraw_and_wait(_sess)
                            if ok:
                                ok2 = await ex.phase_sell(_sess)
                                if ok2:
                                    log.info("auto-recover: %s completed", _sid)
                                    _SESSIONS.pop(_sid, None); _persist_sessions()
                        except Exception as e:
                            log.warning("auto-recover %s err: %s", _sid, e)
                        finally:
                            _in_progress.discard(_sid)
                    asyncio.create_task(_run_wd(sid, sess, plan))
                elif phase in ("AWAIT_APPROVE_SELL",):
                    # Withdraw done, just retry sell
                    async def _run_sell(_sid, _sess):
                        _in_progress.add(_sid)
                        try:
                            log.info("auto-recover: %s → phase_sell", _sid)
                            ok = await ex.phase_sell(_sess)
                            if ok:
                                log.info("auto-recover: %s completed", _sid)
                                _SESSIONS.pop(_sid, None); _persist_sessions()
                        except Exception as e:
                            log.warning("auto-recover %s sell err: %s", _sid, e)
                        finally:
                            _in_progress.discard(_sid)
                    asyncio.create_task(_run_sell(sid, sess))
        except Exception as e:
            log.debug("recover loop: %s", e)


async def _prune_zombie_sessions_loop():
    """Every 5 min, drop sessions that:
      • have filled_qty=0 (buy never landed) AND
      • older than 15 min (user isn't going to tap retry now)
    Keeps _SESSIONS clean so per-base auto-runs aren't blocked."""
    import time as _t
    # Age MUST come from persisted data, not a local dict — the old
    # version reset "first seen" on every restart, so a zombie session
    # survived 20h across ~10 restarts and silently blocked the
    # auto-rebalance watch (which skips while _SESSIONS is non-empty),
    # starving HW USDC down to $188 and reverting every swap.
    _HARD_MAX_AGE = float(os.getenv("SESSION_HARD_MAX_AGE_SEC", "3600"))
    while True:
        try:
            await asyncio.sleep(300)
            now = _t.time()
            drop = []
            stale_held: list[tuple] = []
            for sid, entry in list(_SESSIONS.items()):
                plan = entry.get("plan")
                # filled_qty lives FLAT in the persisted entry; the
                # in-memory session object is only present pre-restart.
                sess = entry.get("session")
                fq = entry.get("filled_qty")
                if fq is None and sess is not None:
                    fq = getattr(sess, "filled_qty", 0)
                fq = float(fq or 0)
                # Real age from the plan's alert timestamp (persisted)
                started = (getattr(plan, "alert_ts", None)
                           or entry.get("created_ts") or now)
                age = now - float(started)
                base = getattr(plan, "base", "?")
                # ONLY unfilled sessions may be dropped silently — those
                # hold nothing. The old `age > _HARD_MAX_AGE` branch also
                # deleted FILLED sessions, i.e. ones holding real coin,
                # and `_run_phase_retry` reads a missing sid as "user
                # cancelled" — so the pruner was aborting live withdraw
                # and sell retries out from under the position.
                if fq <= 0 and age > 900:
                    drop.append((sid, base, age, "no fill 15m+"))
                elif fq > 0 and age > _HARD_MAX_AGE:
                    # Holding inventory and stale: reconcile, never
                    # silently delete.
                    stale_held.append((sid, base, age, entry))
            for sid, base, age, why in drop:
                _SESSIONS.pop(sid, None)
                log.warning("session %s (%s) auto-dropped: %s (age %.0fmin)",
                            sid, base, why, age / 60)
            for sid, base, age, entry in stale_held:
                log.error("session %s (%s) stale %.0fmin WITH inventory — "
                          "reconciling instead of dropping",
                          sid, base, age / 60)
                try:
                    import reconcile as _rec
                    _plan = entry.get("plan")
                    # dex-cycle entries store "chat_ids", CEX ones "subs"
                    # — reading only one meant these alerts reached nobody.
                    _subs = (entry.get("subs")
                             or entry.get("chat_ids") or [])

                    async def _n(text, _s=_subs):
                        for cid in _s:
                            try:
                                await _APP.bot.send_message(
                                    cid, text, parse_mode=ParseMode.HTML)
                            except Exception:
                                pass
                    if _plan is not None:
                        left = await _rec.reconcile_session(
                            _plan, session_id=sid, succeeded=False,
                            notify=_n if _APP else None)
                        # Only release the slot once the coin is
                        # accounted for (stuck/ledger now own it).
                        _SESSIONS.pop(sid, None)
                        log.warning("session %s released after reconcile "
                                    "(%d position(s) recorded)", sid,
                                    len(left))
                except Exception as e:
                    log.warning("stale-held reconcile %s: %s", sid, e,
                                exc_info=True)
            if drop or stale_held:
                _persist_sessions()
        except Exception as e:
            log.warning("prune loop: %s", e, exc_info=True)


def _new_sid() -> str:
    import secrets
    return secrets.token_hex(4)


def _status_cb_for(app: Application, subs: list[int]):
    async def cb(receipt, msg):
        for cid in subs:
            try:
                await app.bot.send_message(
                    chat_id=cid, text=msg, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.debug("status send: %s", e)
    return cb


async def cb_check_route(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Tap on '🚀 Побудувати план' — enumerate all routes and post ranked list."""
    q = update.callback_query
    await q.answer("Будую план…")
    if not _is_allowed(q.from_user.id):
        return
    _, plan_key = q.data.split(":", 1)
    if plan_key not in _PENDING_PLANS:
        await q.message.reply_text("⌛ Алерт застарів — почекай новий.")
        return
    subs = list(_HUNTER.subs) if _HUNTER else [q.message.chat_id]
    asyncio.create_task(_auto_plan_and_notify(ctx.application, plan_key, subs))


async def cb_pick_size(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Step 2: user picked size. Build plan, show feasibility + Approve BUY button."""
    q = update.callback_query
    await q.answer()
    if not _is_allowed(q.from_user.id):
        return
    _, plan_key, size_str = q.data.split(":", 2)
    payload = _PENDING_PLANS.get(plan_key)
    if not payload:
        await q.message.reply_text("⌛ Plan expired.")
        return
    try:
        pick_usd = float(size_str)
    except ValueError:
        return
    # Adjust plan to picked size
    orig_sizing = dict(payload["sizing"])
    orig_max = orig_sizing["notional_usd"]
    if pick_usd < orig_max:
        scale = pick_usd / orig_max
        orig_sizing["notional_usd"] = pick_usd
        orig_sizing["qty"] = orig_sizing["qty"] * scale
        orig_sizing["profit_usd"] = orig_sizing["profit_usd"] * scale
        if "net_profit_usd" in orig_sizing:
            orig_sizing["net_profit_usd"] = orig_sizing["net_profit_usd"] * scale
    ex = executor.instance()
    try:
        plan = await ex.plan(payload["alert"], orig_sizing)
    except Exception as e:
        await q.message.reply_text(f"❌ plan build failed: {e}")
        return
    sid = _new_sid()
    subs = list(_HUNTER.subs) if _HUNTER else [q.message.chat_id]
    receipt = executor.TradeReceipt(
        trade_id=plan.trade_id, base=plan.base,
        buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=ex.mode,
    )
    sess = executor.InteractiveSession(plan=plan, receipt=receipt)
    _SESSIONS[sid] = {"plan": plan, "session": sess, "subs": subs}; _persist_sessions()
    ex.wire_status(_status_cb_for(ctx.application, subs))

    fee = plan.fees or {}
    exec_mode_tag = "🧪 ТЕСТ" if ex.mode == "dry" else "🔥 БОЙОВИЙ"
    prompt = (
        f"📋 <b>{plan.base}</b> план  ({exec_mode_tag}) · sid <code>{sid}</code>\n\n"
        f"<b>КУПІВЛЯ</b> на {cex.pretty(plan.buy_eid)} <code>{plan.buy_sym}</code>\n"
        f"  {plan.qty:.6f} @ ліміт {plan.buy_limit:g}  (~${plan.notional_usd:,.2f})\n\n"
        f"<b>ПЕРЕКАЗ</b> {plan.chain} через {plan.src_network} → "
        f"{cex.pretty(plan.sell_eid)} ({plan.dst_network}) · "
        f"ETA ~{plan.eta_min or 0:.1f} хв\n\n"
        f"<b>ПРОДАЖ</b> на {cex.pretty(plan.sell_eid)} <code>{plan.sell_sym}</code>\n"
        f"  @ ліміт {plan.sell_limit:g}\n\n"
        f"<b>Комісії</b>: ${fee.get('total_usd', 0):,.2f}   "
        f"<b>Очік. чистий</b>: <b>${plan.net_profit_usd:,.2f}</b>"
    )
    rows = [
        [InlineKeyboardButton("✅ Схвалити КУПІВЛЮ", callback_data=f"pbuy:{sid}")],
        [InlineKeyboardButton("🚫 Скасувати", callback_data=f"pcx:{sid}")],
    ]
    await q.message.reply_text(prompt, parse_mode=ParseMode.HTML,
                               reply_markup=InlineKeyboardMarkup(rows))


async def cb_phase_buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Купую…")
    if not _is_allowed(q.from_user.id): return
    _, sid = q.data.split(":", 1)
    entry = _SESSIONS.get(sid)
    if not entry:
        # Untapped offer — promote it to a real session now that the
        # user is committing. Until this moment it lived in
        # _PENDING_BUTTONS so it couldn't block anything.
        entry = _PENDING_BUTTONS.pop(sid, None)
        if entry:
            _SESSIONS[sid] = {"plan": entry["plan"],
                               "session": entry["session"],
                               "subs": entry["subs"]}
            _persist_sessions()
            entry = _SESSIONS[sid]
    if not entry:
        await q.message.reply_text("⌛ Сесія протухла."); return
    if executor.kill_active():
        await q.message.reply_text("🛑 /kill активний."); return
    ex = executor.instance()
    ex.wire_status(_status_cb_for(ctx.application, entry["subs"]))
    ok = await ex.phase_buy(entry["session"])
    if not ok:
        # Keep the session alive so user can retry manually (or /stop)
        # instead of losing all context to "session expired".
        rows = [
            [InlineKeyboardButton("🔁 Повторити BUY", callback_data=f"pbuy:{sid}")],
            [InlineKeyboardButton("🚫 Стоп", callback_data=f"pcx:{sid}")],
        ]
        await q.message.reply_text("Купівля не пройшла. Спробувати ще?",
                                    reply_markup=InlineKeyboardMarkup(rows))
        return
    entry["session"].phase = "AWAIT_APPROVE_WITHDRAW"
    rows = [
        [InlineKeyboardButton("✅ Схвалити ВИВІД", callback_data=f"pwd:{sid}")],
        [InlineKeyboardButton("🚫 Стоп", callback_data=f"pcx:{sid}")],
    ]
    plan = entry["plan"]
    await q.message.reply_text(
        f"<b>Далі</b>: ВИВОДЖУ {entry['session'].filled_qty:.6f} {plan.base} "
        f"→ {cex.pretty(plan.sell_eid)} на {plan.chain} "
        f"({plan.src_network})\nETA ~{plan.eta_min or 0:.1f} хв",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows),
    )


async def cb_phase_withdraw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Виводжу…")
    if not _is_allowed(q.from_user.id): return
    _, sid = q.data.split(":", 1)
    entry = _SESSIONS.get(sid)
    if not entry:
        await q.message.reply_text("⌛ Сесія протухла."); return
    if executor.kill_active():
        await q.message.reply_text("🛑 /kill активний."); return
    ex = executor.instance()
    ex.wire_status(_status_cb_for(ctx.application, entry["subs"]))
    ok = await ex.phase_withdraw_and_wait(entry["session"])
    if not ok:
        # Keep session — user may want to retry after fixing balance,
        # topping up gas, or restarting the withdraw manually.
        rows = [
            [InlineKeyboardButton("🔁 Повторити ВИВІД", callback_data=f"pwd:{sid}")],
            [InlineKeyboardButton("➡️ Пропустити (перейти на ПРОДАЖ)",
                                  callback_data=f"psl:{sid}")],
            [InlineKeyboardButton("🚫 Стоп", callback_data=f"pcx:{sid}")],
        ]
        await q.message.reply_text("Вивід не пройшов. Що робимо?",
                                    reply_markup=InlineKeyboardMarkup(rows))
        return
    entry["session"].phase = "AWAIT_APPROVE_SELL"
    rows = [
        [InlineKeyboardButton("✅ Схвалити ПРОДАЖ", callback_data=f"psl:{sid}")],
        [InlineKeyboardButton("🚫 Стоп", callback_data=f"pcx:{sid}")],
    ]
    plan = entry["plan"]
    await q.message.reply_text(
        f"<b>Далі</b>: ПРОДАЮ {entry['session'].filled_qty:.6f} {plan.base} "
        f"на {cex.pretty(plan.sell_eid)} @ ліміт {plan.sell_limit:g}",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows),
    )


async def cb_phase_sell(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Продаю…")
    if not _is_allowed(q.from_user.id): return
    _, sid = q.data.split(":", 1)
    entry = _SESSIONS.get(sid)
    if not entry:
        await q.message.reply_text("⌛ Сесія протухла."); return
    if executor.kill_active():
        await q.message.reply_text("🛑 /kill активний."); return
    ex = executor.instance()
    ex.wire_status(_status_cb_for(ctx.application, entry["subs"]))
    ok = await ex.phase_sell(entry["session"])
    executor._persist(entry["session"].receipt, entry["plan"])
    # The return value used to be discarded and the session dropped
    # regardless — a failed sell erased the only record of the coin.
    # Check what is actually left before letting the record go.
    try:
        import reconcile as _rec

        async def _n(text):
            for cid in entry.get("subs") or []:
                try:
                    await ctx.application.bot.send_message(
                        cid, text, parse_mode=ParseMode.HTML)
                except Exception:
                    pass
        left = await _rec.reconcile_session(
            entry["plan"], session_id=sid, succeeded=bool(ok), notify=_n)
    except Exception as e:
        log.warning("cb_phase_sell reconcile: %s", e, exc_info=True)
        left = []
    if not ok:
        await q.message.reply_text(
            f"❌ Продаж не пройшов: "
            f"{(entry['session'].receipt.error or '?')[:150]}"
            + ("\n📦 Залишок зафіксовано — /stuck" if left else ""))
    _SESSIONS.pop(sid, None); _persist_sessions()


async def cb_session_skip_step(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User tapped 'Пропустити крок' on an auto-session.

    For WITHDRAW phase this has a special meaning: user did the withdraw
    manually via the exchange UI — bot should NOT retry broadcasting,
    it should just wait for the tokens to arrive on HW and continue
    with SEND → RECEIVED → SELL as usual."""
    q = update.callback_query
    if not _is_allowed(q.from_user.id):
        await q.answer(); return
    _, sid = q.data.split(":", 1)
    phase = _SESSION_CURRENT_PHASE.get(sid)
    if not phase:
        await q.answer(); return
    if phase == "WITHDRAW":
        _SESSION_MANUAL_WD.add(sid)
        await q.answer("Ок — жду на HW, потім SEND")
        log.info("session %s: WITHDRAW skip → wait_manual_wd mode", sid)
    else:
        await q.answer("Пропущено крок")
        log.info("session %s: user requested SKIP for %s", sid, phase)
    _SESSION_SKIP.setdefault(sid, set()).add(phase)


_SESSION_MANUAL_WD: set[str] = set()


async def cb_session_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Скасовано.")
    if not _is_allowed(q.from_user.id): return
    _, sid = q.data.split(":", 1)
    who = q.from_user.username or q.from_user.first_name or str(q.from_user.id)
    if sid in _SESSIONS:
        _entry = _SESSIONS[sid]
        _plan = _entry.get("plan")
        base = getattr(_plan, "base", "?")
        log.info("session %s STOP pressed by %s (base=%s)", sid, who, base)
        # Stopping the pipeline does not make the coin disappear. Check
        # what we still hold before discarding the record, otherwise a
        # cancel mid-flight silently orphans the position.
        try:
            import reconcile as _rec

            async def _n(text):
                for cid in _entry.get("subs") or []:
                    try:
                        await ctx.application.bot.send_message(
                            cid, text, parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
            if _plan is not None:
                await _rec.reconcile_session(_plan, session_id=sid,
                                               succeeded=False, notify=_n)
        except Exception as e:
            log.warning("cb_session_cancel reconcile: %s", e, exc_info=True)
    _SESSIONS.pop(sid, None); _persist_sessions()
    _PENDING_PLANS.pop(sid, None)
    # Also drop the un-tapped offer. Without this, 🚫 replied "Скасовано"
    # while the ✅ button on the SAME message still resolved through
    # _PENDING_BUTTONS and executed a real buy.
    _PENDING_BUTTONS.pop(sid, None)
    try:
        await q.edit_message_text("🚫 <b>Скасовано</b>", parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def cmd_kill(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    args = ctx.args or []
    if args and args[0].lower() in ("off", "0", "no"):
        executor.set_kill(False)
        await update.message.reply_text("✅ Виконання увімкнено.")
    else:
        active = executor.active_trade_count()
        executor.set_kill(True)
        cancel_note = f"\n🛑 Скасовано {active} активних трейдів." if active else ""
        await update.message.reply_text(
            f"🛑 <b>KILL</b> — виконання зупинено. Алерти продовжують йти.{cancel_note}\n"
            "Знову увімкнути: /kill off",
            parse_mode=ParseMode.HTML,
        )


_REBAL_AWAIT_TARGETS: dict[int, dict] = {}                 # user_id → {balances, savedtargets}


async def cb_rebalance_targets_reply(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Second step of rebalance: parse target amounts from user reply
    and build the plan. Only fires for users who just clicked 🔄 Ребаланс
    (whose id is in _REBAL_AWAIT_TARGETS)."""
    uid = update.effective_user.id
    ctx_data = _REBAL_AWAIT_TARGETS.get(uid)
    if not ctx_data:
        return                                              # not in rebalance flow
    txt = (update.message.text or "").strip().lower()
    _REBAL_AWAIT_TARGETS.pop(uid, None)
    saved = ctx_data["saved"]
    balances = ctx_data["balances"]
    if txt == "same":
        targets = saved
    else:
        # parse pairs: "gate 1000 binance 1000 bitvavo 2000"
        parts = txt.split()
        targets = dict(saved)                               # start from saved as fallback
        i = 0
        while i < len(parts) - 1:
            key = parts[i]
            try:
                val = float(parts[i + 1])
            except ValueError:
                await update.message.reply_text(
                    f"❌ не змогло парснути '{parts[i + 1]}'. Формат: "
                    f"<code>gate 1000 binance 1000 bitvavo 2000</code>",
                    parse_mode=ParseMode.HTML)
                return
            if key in ("gate", "binance", "bitvavo"):
                targets[key] = val
            i += 2
    # persist targets to env / .env so future runs remember
    env_map = {"gate": "TARGET_GATE_USDT",
               "binance": "TARGET_BINANCE_USDT",
               "bitvavo": "TARGET_BITVAVO_USDC"}
    for k, v in targets.items():
        os.environ[env_map[k]] = str(v)
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        lines = []
        if os.path.exists(env_path):
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    if not line.endswith("\n"):
                        line = line + "\n"                  # ensure trailing NL
                    ek = line.split("=", 1)[0].strip() if "=" in line else ""
                    if ek in env_map.values():
                        continue
                    lines.append(line)
        for k, v in targets.items():
            lines.append(f"{env_map[k]}={v}\n")
        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        log.warning("targets persist err: %s", e)
    await _rebalance_build_plan(update, ctx, targets, balances)


async def cmd_rebalance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Step 1 of rebalance: show current per-exchange balances and
    saved targets, then wait for the user to send new targets as one
    message. Step 2 (message handler) builds the plan and asks to
    confirm. Excess above target → withdrawn to hot wallet."""
    if not _is_allowed(update.effective_user.id): return
    kd = keys_mod.load_keys()
    lines = ["💰 <b>Поточні балаyness</b>", ""]
    bal_snapshot: dict = {}
    for eid in ("gate", "binance", "bitvavo"):
        if not (kd.get(eid) or {}).get("apiKey"):
            lines.append(f"⚫ {cex.pretty(eid)} — no keys"); continue
        try:
            inst = cex.get_private(eid, kd[eid])
            b = None
            last = None
            for _ in range(3):
                try:
                    if eid == "binance":
                        try: await inst.load_time_difference()
                        except Exception: pass
                    b = await inst.fetch_balance()
                    break
                except Exception as e:
                    last = e
                    if eid == "bitvavo":
                        inst.aiohttp_proxy = cex._pick_proxy()
                    await asyncio.sleep(1.5)
            if b is None:
                raise last
        except Exception as e:
            lines.append(f"❌ {cex.pretty(eid)} — {str(e)[:80]}"); continue
        if eid == "bitvavo":
            eur = float((b.get("EUR") or {}).get("free") or 0)
            usdc = float((b.get("USDC") or {}).get("free") or 0)
            # Force live FX fetch (get_fx_rate sync may return 1.0 fallback
            # when cache empty; await _quote_to_usd resolves properly).
            fx = await cex._quote_to_usd("EUR") or 1.17
            if fx < 1.05: fx = 1.17                          # sanity floor
            total_usdc_val = usdc + eur * fx
            bal_snapshot["bitvavo"] = {"eur": eur, "usdc": usdc,
                                        "total_usdc": total_usdc_val}
            lines.append(f"• <b>Bitvavo</b>: {eur:.2f} EUR + {usdc:.2f} USDC "
                         f"(<i>~{total_usdc_val:.2f} USDC value</i>)")
        else:
            usdt = float((b.get("USDT") or {}).get("free") or 0)
            usdc = float((b.get("USDC") or {}).get("free") or 0)
            total = usdt + usdc
            # Target for Gate/Binance = required USDT amount. USDC counts
            # as "extra capital" — will either be swapped to USDT (deficit)
            # or sent out (excess, preferring USDC to avoid swap fees).
            bal_snapshot[eid] = {"usdt": usdt, "usdc": usdc, "total": total}
            parts = [f"{usdt:.2f} USDT"]
            if usdc > 0.5:
                parts.append(f"{usdc:.2f} USDC")
            lines.append(f"• <b>{cex.pretty(eid)}</b>: {' + '.join(parts)} "
                         f"(<i>всього {total:.2f}</i>)")
    # Hot wallet — stables on cheapest chains (used as reserve buffer
    # if excess < deficit after redistribution).
    hot_stables: dict[tuple, float] = {}
    try:
        snap = await dex_mod.wallet_snapshot(price_map=None, min_usd=1.0)
        for chain, entries in (snap.get("chains") or {}).items():
            for sym, amt, usd in entries:
                if sym in ("USDC", "USDT"):
                    hot_stables[(chain, sym)] = amt
        total_hot = sum(hot_stables.values())
        chain_summary = ", ".join(f"{c.upper()}:{amt:.0f} {s}"
                                   for (c, s), amt in hot_stables.items() if amt > 5) or "(нема)"
        lines.append(f"• <b>💳 Hot wallet</b>: ~{total_hot:.2f} стейблів ({chain_summary})")
        bal_snapshot["hot_wallet"] = hot_stables
    except Exception as e:
        log.debug("hot snap: %s", e)
        bal_snapshot["hot_wallet"] = {}
    # saved targets — only for currently-supported exchanges
    all_targets = {
        "gate":    float(os.getenv("TARGET_GATE_USDT", "1000")),
        "binance": float(os.getenv("TARGET_BINANCE_USDT", "1000")),
        "bitvavo": float(os.getenv("TARGET_BITVAVO_USDC", "2000")),
    }
    saved = {e: all_targets[e] for e in cex.SUPPORTED_EXCHANGES if e in all_targets}
    lines.append("")
    def _unit(e): return "USDC-value" if e == "bitvavo" else "USDT"
    tgt_parts = [f"{e} {v:.0f} {_unit(e)}" for e, v in saved.items()]
    lines.append("🎯 <b>Збережені цілі</b>: " + " · ".join(tgt_parts))
    lines.append("")
    lines.append("<b>Введи нові цілі одним повідомленням</b>, напр:")
    example = " ".join(f"{e} {v:.0f}" for e, v in saved.items())
    lines.append(f"<code>{example}</code>")
    lines.append("(або <code>same</code> щоб використати збережені)")
    _REBAL_AWAIT_TARGETS[update.effective_user.id] = {
        "balances": bal_snapshot,
        "saved": saved,
    }
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def _rebalance_build_plan(update: Update, ctx, targets: dict, balances: dict) -> None:
    """Build a two-phase plan:
      Phase A — withdraw excess from every over-target exchange into
                hot wallet (hot wallet is the ROUTER, not source).
      Phase B — after excess credits, redistribute to deficit exchanges.
    Hot wallet's baseline stables are the trading profit and are NOT
    tapped. If total excess < total deficit, partial fills — note it."""
    actions: list[dict] = []
    lines = ["🔄 <b>План ребалансу</b>", ""]
    def _unit(e): return "USDC-value" if e == "bitvavo" else "USDT"
    parts = [f"{e} {v:.0f} {_unit(e)}" for e, v in targets.items()]
    lines.append("🎯 Цілі: " + " · ".join(parts))
    lines.append("")
    # Compute delta per exchange (positive = deficit, negative = excess).
    deltas: dict[str, float] = {}
    for eid in [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]:
        if eid in balances:
            # Delta = target − TOTAL stables (USDT + USDC together).
            # >0 = deficit (need to top-up USDT).
            # <0 = excess (send it OUT — USDC preferred to avoid swap).
            total = balances[eid].get("total") or balances[eid].get("usdt", 0)
            deltas[eid] = targets[eid] - total
    if "bitvavo" in balances:
        deltas["bitvavo"] = targets["bitvavo"] - balances["bitvavo"]["total_usdc"]
    total_excess = sum(-d for d in deltas.values() if d < -5)
    total_deficit = sum(d for d in deltas.values() if d > 5)
    lines.append(f"📊 <b>Загалом</b>: надлишок {total_excess:.2f} USDC-value · "
                 f"дефіцит {total_deficit:.2f} USDC-value")
    if total_deficit > total_excess + 20:
        lines.append(f"⚠️ дефіцит &gt; надлишку на "
                     f"<b>{total_deficit - total_excess:.2f}</b> — заповнимо частково "
                     f"(hot wallet baseline НЕ чіпаємо — це profit)")
    lines.append("")

    # ─── Phase A: withdraw excess ────────────────────────────────────
    lines.append("<b>Фаза A</b>: біржа → hot wallet")
    # Bitvavo — if excess exists, we withdraw USDC. If not enough USDC
    # on hand, first BUY USDC/EUR to top-up (spend EUR to get USDC),
    # then withdraw. End state: target USDC-value stays mostly in EUR.
    if deltas.get("bitvavo", 0) < -20:
        bv = balances["bitvavo"]
        excess = -deltas["bitvavo"]                         # USDC-value to move out
        need_buy = max(0.0, excess - bv["usdc"])
        if need_buy > 5:
            actions.append({"eid": "bitvavo", "kind": "buy_usdc_eur",
                            "amount": need_buy,
                            "desc": f"Bitvavo: BUY {need_buy:.2f} USDC/EUR (spend ~{need_buy * 0.9:.2f} EUR)"})
            lines.append(f"  • Bitvavo: докупити <b>{need_buy:.2f} USDC</b> з EUR")
        if excess > 1.2:                                    # Bitvavo min withdraw ~1.2 USDC
            actions.append({"eid": "bitvavo", "kind": "withdraw_usdc",
                            "amount": excess,
                            "wait_for_usdc": True,           # wait for buy to fill first
                            "desc": f"Bitvavo: WITHDRAW {excess:.2f} USDC → hot wallet (ETH)"})
            lines.append(f"  • Bitvavo excess: <b>{excess:.2f} USDC</b> → hot wallet")
    # Bitvavo deficit case — if EUR-value is BELOW target, we top-up
    # from hot wallet (handled in Phase B like other exchanges).
    elif deltas.get("bitvavo", 0) < 5 and balances.get("bitvavo", {}).get("usdc", 0) > 5:
        # Even if on-target overall, sell stray USDC → EUR to normalize
        bv = balances["bitvavo"]
        actions.append({"eid": "bitvavo", "kind": "sell_usdc_eur",
                        "amount": bv["usdc"],
                        "desc": f"Bitvavo: SELL {bv['usdc']:.2f} USDC → EUR (normalize)"})
        lines.append(f"  • Bitvavo: конверт залишку {bv['usdc']:.2f} USDC → EUR")
    # Route choice: if Bitvavo participates (excess or deficit) →
    # every hop uses USDC on ETH (Bitvavo requires it). Otherwise
    # (only Gate↔Binance) → USDT on BSC (cheaper gas).
    bitvavo_in_play = ("bitvavo" in deltas
                       and abs(deltas["bitvavo"]) > 5)
    use_usdc_eth = bitvavo_in_play
    for eid in [e for e in cex.SUPPORTED_EXCHANGES if e != "bitvavo"]:
        if deltas.get(eid, 0) < -5:
            excess_usdt = -deltas[eid]
            # BONUS: if this CEX ALREADY has USDC (from prior swaps),
            # use it DIRECTLY — saves 0.1-0.2% swap fee per $ moved.
            usdc_on_hand = float(balances[eid].get("usdc", 0))
            if use_usdc_eth:
                # Case A: enough existing USDC to cover the whole excess
                if usdc_on_hand >= excess_usdt - 0.5:
                    actions.append({"eid": eid, "kind": "withdraw_usdc_eth",
                                    "amount": excess_usdt,
                                    "desc": f"{cex.pretty(eid)}: WD {excess_usdt:.2f} USDC (already on-hand) → HW (ETH)"})
                    lines.append(f"  • {cex.pretty(eid)}: WD <b>{excess_usdt:.2f} USDC</b> напряму (без swap, зекономлено ~${excess_usdt*0.001:.2f})")
                # Case B: partial USDC coverage
                elif usdc_on_hand > 5:
                    to_swap = excess_usdt - usdc_on_hand
                    actions.append({"eid": eid, "kind": "swap_usdt_to_usdc",
                                    "amount": to_swap,
                                    "desc": f"{cex.pretty(eid)}: swap {to_swap:.2f} USDT → USDC (доповнити наявні {usdc_on_hand:.2f})"})
                    lines.append(f"  • {cex.pretty(eid)}: swap лише <b>{to_swap:.2f} USDT → USDC</b> (є вже {usdc_on_hand:.2f})")
                    actions.append({"eid": eid, "kind": "withdraw_usdc_eth",
                                    "amount": excess_usdt,
                                    "desc": f"{cex.pretty(eid)}: WD {excess_usdt:.2f} USDC → HW (ETH)"})
                    lines.append(f"  • {cex.pretty(eid)} excess: <b>{excess_usdt:.2f} USDC</b> → HW (ETH)")
                # Case C: no USDC → full swap
                else:
                    actions.append({"eid": eid, "kind": "swap_usdt_to_usdc",
                                    "amount": excess_usdt,
                                    "desc": f"{cex.pretty(eid)}: SWAP {excess_usdt:.2f} USDT → USDC"})
                    lines.append(f"  • {cex.pretty(eid)}: swap {excess_usdt:.2f} USDT → USDC")
                    actions.append({"eid": eid, "kind": "withdraw_usdc_eth",
                                    "amount": excess_usdt,
                                    "desc": f"{cex.pretty(eid)}: WD {excess_usdt:.2f} USDC → HW (ETH)"})
                    lines.append(f"  • {cex.pretty(eid)} excess: <b>{excess_usdt:.2f} USDC</b> → HW (ETH)")
            else:
                actions.append({"eid": eid, "kind": "withdraw_usdt",
                                "amount": excess_usdt,
                                "desc": f"{cex.pretty(eid)}: WITHDRAW {excess_usdt:.2f} USDT → hot wallet (BSC)"})
                lines.append(f"  • {cex.pretty(eid)} excess: <b>{excess_usdt:.2f} USDT</b> → hot wallet (BSC)")

    # ─── Phase B: distribute to deficits ─────────────────────────────
    # Fund order:
    #   1. Use the incoming pool from Phase-A withdrawals first
    #   2. If a deficit still remains, tap the hot wallet baseline stables
    #      (last resort — that's trading profit).
    hot_stables_snap = balances.get("hot_wallet") or {}
    hot_pool_usdc_eth = float(hot_stables_snap.get(("ethereum", "USDC"), 0))
    lines.append("")
    lines.append("<b>Фаза B</b>: hot wallet → біржа (після надходжень)")
    pool = total_excess
    for eid, d in sorted(deltas.items(), key=lambda kv: -kv[1]):   # largest deficit first
        if d <= 5:
            continue
        need = d
        give = min(need, pool)
        # From incoming pool first
        if give >= 5:
            # Same rule: Bitvavo in play → USDC ETH; else USDT BSC
            if eid == "bitvavo":
                chain = "ethereum"; sym = "USDC"; swap_after = False
            elif use_usdc_eth:
                chain = "ethereum"; sym = "USDC"; swap_after = True
            else:
                chain = "bsc"; sym = "USDT"; swap_after = False
            actions.append({"eid": eid, "kind": "topup_from_hot_pooled",
                            "amount": give, "chain": chain, "sym": sym,
                            "wait_for_pool": True,
                            "swap_after": swap_after,
                            "desc": f"Hot → {cex.pretty(eid)}: send {give:.2f} {sym} ({chain.upper()})"
                                    + (" → swap USDC/USDT" if swap_after else "")})
            line = f"  • Hot wallet → {cex.pretty(eid)}: <b>{give:.2f} {sym}</b> ({chain.upper()}, з надходжень)"
            if swap_after:
                line += " → swap USDC→USDT"
            elif eid == "bitvavo" and sym == "USDC":
                # executor auto-swaps USDC→EUR after credit on Bitvavo
                line += " → swap USDC→EUR"
            lines.append(line)
            pool -= give
            need -= give
        # If still deficit → tap hot wallet baseline. Use same currency
        # choice as pool (USDC ETH if Bitvavo in play, else USDT BSC).
        if need > 5:
            if eid == "bitvavo":
                base_chain, base_sym = "ethereum", "USDC"; base_swap = False
            elif use_usdc_eth:
                base_chain, base_sym = "ethereum", "USDC"; base_swap = True
            else:
                base_chain, base_sym = "bsc", "USDT"; base_swap = False
            avail = float(hot_stables_snap.get((base_chain, base_sym), 0))
            take = min(need, avail)
            if take > 5:
                actions.append({"eid": eid, "kind": "topup_from_hot_baseline",
                                "amount": take, "chain": base_chain, "sym": base_sym,
                                "swap_after": base_swap,
                                "desc": f"Hot baseline → {cex.pretty(eid)}: {take:.2f} {base_sym}"})
                line = (f"  • Hot wallet baseline → {cex.pretty(eid)}: "
                        f"<b>{take:.2f} {base_sym}</b> ({base_chain.upper()}, з profit-резерву)")
                if base_swap:
                    line += " → swap USDC→USDT"
                elif eid == "bitvavo" and base_sym == "USDC":
                    line += " → swap USDC→EUR"
                lines.append(line)
                need -= take
        if need > 5:
            lines.append(f"  ⚠️ {cex.pretty(eid)}: дефіцит {need:.2f} — недостатньо ресурсів")
    if not actions:
        lines.append("\n<i>Все на цілях — нічого робити</i>")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return
    import secrets as _sec
    rid = _sec.token_hex(4)
    _REBAL_PLANS[rid] = actions
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Виконати", callback_data=f"rbx:{rid}")],
        [InlineKeyboardButton("🚫 Скасувати", callback_data=f"rbc:{rid}")],
    ])
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML,
                                     reply_markup=kb)


async def cmd_rebalance_LEGACY(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Legacy one-shot rebalance kept for reference; not wired."""
    if not _is_allowed(update.effective_user.id): return
    kd = keys_mod.load_keys()
    t_gate = float(os.getenv("TARGET_GATE_USDT", "1000"))
    t_binance = float(os.getenv("TARGET_BINANCE_USDT", "1000"))
    t_bitvavo = float(os.getenv("TARGET_BITVAVO_USDC", "2000"))
    targets = {"gate": ("USDT", t_gate),
               "binance": ("USDT", t_binance),
               "bitvavo": ("EUR-USDC", t_bitvavo)}
    lines = ["🔄 <b>План ребалансу</b>", ""]
    actions: list[dict] = []
    for eid, (target_ccy, target_amt) in targets.items():
        if not (kd.get(eid) or {}).get("apiKey"):
            lines.append(f"⚫ {cex.pretty(eid)} — no keys"); continue
        try:
            inst = cex.get_private(eid, kd[eid])
            bal = await inst.fetch_balance()
        except Exception as e:
            lines.append(f"❌ {cex.pretty(eid)} — balance fetch: {str(e)[:80]}")
            continue
        if eid == "bitvavo":
            free_eur = float((bal.get("EUR") or {}).get("free") or 0)
            free_usdc = float((bal.get("USDC") or {}).get("free") or 0)
            fx = cex.get_fx_rate("EUR") or 1.16
            total_usdc_value = free_usdc + free_eur * fx
            lines.append(f"• <b>Bitvavo</b>: {free_eur:.2f} EUR + {free_usdc:.2f} USDC "
                         f"(~{total_usdc_value:.2f} USDC value)")
            # Action 1: sell USDC → EUR (any USDC balance)
            if free_usdc > 5:
                actions.append({"eid": eid, "kind": "sell_usdc_eur",
                                "amount": free_usdc,
                                "desc": f"Bitvavo: SELL {free_usdc:.2f} USDC/EUR → EUR"})
            # Action 2: if total > 2000, withdraw excess as USDC to hot wallet
            excess = total_usdc_value - t_bitvavo
            if excess > 20:
                actions.append({"eid": eid, "kind": "withdraw_usdc",
                                "amount": excess,
                                "desc": f"Bitvavo: WITHDRAW ~{excess:.2f} USDC → hot wallet"})
        else:
            free = float((bal.get(target_ccy) or {}).get("free") or 0)
            lines.append(f"• <b>{cex.pretty(eid)}</b>: {free:.2f} {target_ccy}"
                         f" (target {target_amt:.0f})")
            excess = free - target_amt
            if excess > 5:
                actions.append({"eid": eid, "kind": "withdraw_usdt",
                                "amount": excess,
                                "desc": f"{cex.pretty(eid)}: WITHDRAW {excess:.2f} {target_ccy} → hot wallet"})
    if not actions:
        lines.append("\n<i>Все на цілях — нічого робити</i>")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return
    lines.append("\n<b>Дії:</b>")
    for a in actions:
        lines.append(f"  · {a['desc']}")
    # Stash plan for confirm
    import secrets as _sec
    rid = _sec.token_hex(4)
    _REBAL_PLANS[rid] = actions
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Виконати", callback_data=f"rbx:{rid}")],
        [InlineKeyboardButton("🚫 Скасувати", callback_data=f"rbc:{rid}")],
    ])
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML,
                                    reply_markup=kb)


_REBAL_PLANS: dict[str, list[dict]] = {}
_REBAL_ACTIVE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "rebal_active.json")


def _persist_active_rebal(rid: str, chat_ids: list[int], done_idx: int,
                            actions: list[dict]):
    """Snapshot in-flight rebalance so a restart mid-flight can resume."""
    try:
        state = {"rid": rid, "chat_ids": chat_ids, "done_idx": done_idx,
                 "actions": actions}
        with open(_REBAL_ACTIVE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        log.debug("rebal persist: %s", e)


def _clear_active_rebal():
    try:
        if os.path.exists(_REBAL_ACTIVE_FILE):
            os.remove(_REBAL_ACTIVE_FILE)
    except Exception: pass


def _load_active_rebal():
    try:
        if not os.path.exists(_REBAL_ACTIVE_FILE):
            return None
        with open(_REBAL_ACTIVE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.debug("rebal load: %s", e)
        return None


async def cmd_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show or set per-exchange rebalance targets.
    Usage:
      /target                     — show all
      /target gate 1500           — set Gate USDT target to 1500
      /target binance 800         — set Binance USDT target
      /target bitvavo 2500        — set Bitvavo total USDC-value target (held as EUR)
    Values persist for the process lifetime (writes to .env via update)."""
    if not _is_allowed(update.effective_user.id): return
    args = ctx.args or []
    if not args:
        t_gate = float(os.getenv("TARGET_GATE_USDT", "1000"))
        t_binance = float(os.getenv("TARGET_BINANCE_USDT", "1000"))
        t_bitvavo = float(os.getenv("TARGET_BITVAVO_USDC", "2000"))
        await update.message.reply_text(
            f"🎯 <b>Ребаланс-цілі</b>\n"
            f"  Gate: {t_gate:.0f} USDT\n"
            f"  Binance: {t_binance:.0f} USDT\n"
            f"  Bitvavo: {t_bitvavo:.0f} USDC-value (як EUR)\n\n"
            f"<i>/target gate|binance|bitvavo &lt;число&gt;</i>",
            parse_mode=ParseMode.HTML)
        return
    if len(args) != 2:
        await update.message.reply_text("Формат: /target &lt;біржа&gt; &lt;число&gt;")
        return
    eid = args[0].lower()
    try:
        val = float(args[1])
    except ValueError:
        await update.message.reply_text("Треба число"); return
    env_map = {"gate": "TARGET_GATE_USDT",
               "binance": "TARGET_BINANCE_USDT",
               "bitvavo": "TARGET_BITVAVO_USDC"}
    key = env_map.get(eid)
    if not key:
        await update.message.reply_text("біржа має бути gate|binance|bitvavo"); return
    os.environ[key] = str(val)
    # persist to .env so it survives restart
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        lines = []
        found = False
        if os.path.exists(env_path):
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith(f"{key}="):
                        lines.append(f"{key}={val}\n"); found = True
                    else:
                        lines.append(line)
        if not found:
            lines.append(f"{key}={val}\n")
        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        log.warning("target persist err: %s", e)
    await update.message.reply_text(f"✅ {eid} target = <b>{val:g}</b>",
                                     parse_mode=ParseMode.HTML)


async def cb_rebalance_execute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Виконую…")
    if not _is_allowed(q.from_user.id): return
    _, rid = q.data.split(":", 1)
    actions = _REBAL_PLANS.pop(rid, None)
    if not actions:
        await q.message.reply_text("⌛ План застарів."); return
    await _rebalance_run(ctx.application, rid, actions,
                          [q.message.chat_id], start_idx=0)


async def _rebalance_run(app, rid: str, actions: list[dict],
                          chat_ids: list[int], start_idx: int = 0):
    """Executes an approved rebalance plan. Persists progress to
    rebal_active.json so a restart mid-flight can pick up where left off."""
    kd = keys_mod.load_keys()
    HOT = dex_mod.HOT_WALLET_ADDRESS
    lines = ["🔄 <b>Ребаланс виконується</b>", ""]
    msg_id_map: dict[int, int] = {}
    for cid in chat_ids:
        try:
            m = await app.bot.send_message(cid, "\n".join(lines), parse_mode=ParseMode.HTML)
            msg_id_map[cid] = m.message_id
        except Exception: pass
    async def _edit():
        text = "\n".join(lines)
        for cid, mid in msg_id_map.items():
            try:
                await app.bot.edit_message_text(text=text, chat_id=cid,
                                                 message_id=mid, parse_mode=ParseMode.HTML)
            except Exception: pass

    def _fee_estimate(a) -> str:
        """Precise fee breakdown per action, from ccxt market metadata
        and exchange currency info. Returns exact numbers, not %."""
        eid = a.get("eid", "")
        k = a.get("kind", "")
        amt = float(a.get("amount", 0) or 0)

        def _taker_fee(sym: str) -> float | None:
            """Read taker fee rate from ccxt market metadata."""
            try:
                inst_l = cex._instances.get(eid)
                m = (inst_l.markets or {}).get(sym) if inst_l else None
                if m:
                    return float(m.get("taker") or m.get("maker") or 0)
            except Exception: pass
            return None

        # Exchange trade fees (Bitvavo EUR pair, Gate USDT pair)
        if k in ("sell_usdc_eur", "buy_usdc_eur"):
            rate = _taker_fee("USDC/EUR") or 0.0025
            fee_eur = amt * rate                      # amt is base USDC
            fx = cex.get_fx_rate("EUR") or 1.17
            return f"{fee_eur:.4f} EUR trade fee (${fee_eur * fx:.3f}, taker {rate*100:.2f}%)"
        if k in ("swap_usdt_to_usdc", "swap_usdc_to_usdt"):
            rate = _taker_fee("USDC/USDT") or 0.001
            fee = amt * rate
            return f"{fee:.4f} USDC trade fee (~${fee:.3f}, taker {rate*100:.2f}%)"
        # Withdraw fees — from network_info
        if k.startswith("withdraw"):
            base = "USDC" if "usdc" in k else "USDT"
            chain = "ETH" if "eth" in k or "usdc" in k and eid == "bitvavo" else \
                    ("BSC" if "bsc" in k else "ETH")
            fee_val = None
            try:
                for n in cex.network_info(eid, base):
                    net_up = (n.get("network") or "").upper()
                    if net_up in (chain, "ERC20"):
                        fv = n.get("fee")
                        if fv:
                            fee_val = float(fv); break
            except Exception: pass
            if fee_val is not None:
                return f"{fee_val:.2f} {base} WD fee (~${fee_val:.2f})"
            return f"~2 {base} WD fee (fallback, network_info missing)"
        # HW send: precise gas after broadcast (unknown pre). Show range.
        if k.startswith("send_hw"):
            chain = (a.get("chain") or "ethereum").lower()
            typical = {"ethereum": "$0.30-2.00 gas",
                        "bsc": "$0.01 gas",
                        "polygon": "$0.005 gas",
                        "arbitrum": "$0.02 gas",
                        "base": "$0.01 gas"}.get(chain, "$0.10 gas")
            return typical + f" ({chain})"
        return ""

    def _describe(a):
        eid = a.get("eid", "?")
        k = a.get("kind", "?")
        amt = a.get("amount", 0)
        pretty = cex.pretty(eid)
        fee_str = _fee_estimate(a)
        fee_tail = f"  <i>[{fee_str}]</i>" if fee_str else ""
        if k == "sell_usdc_eur":  return f"{pretty}: SELL {amt:.2f} USDC → EUR{fee_tail}"
        if k == "buy_usdc_eur":   return f"{pretty}: BUY {amt:.2f} USDC за EUR{fee_tail}"
        if k == "swap_usdt_to_usdc": return f"{pretty}: swap {amt:.2f} USDT → USDC{fee_tail}"
        if k == "swap_usdc_to_usdt": return f"{pretty}: swap {amt:.2f} USDC → USDT{fee_tail}"
        if k == "withdraw_usdc_eth": return f"{pretty}: WD {amt:.2f} USDC → HW (ETH){fee_tail}"
        if k == "withdraw_usdc": return f"{pretty}: WD {amt:.2f} USDC → HW{fee_tail}"
        if k == "withdraw_usdt": return f"{pretty}: WD {amt:.2f} USDT → HW{fee_tail}"
        if k == "send_hw_to_exchange": return f"HW → {pretty}: {amt:.2f} USDC (ETH){fee_tail}"
        return f"{pretty}: {k} {amt}{fee_tail}"

    # Snapshot totals BEFORE run — used for actual-cost calculation after.
    async def _snapshot_total_usd() -> float:
        tot = 0.0
        for eid in cex.SUPPORTED_EXCHANGES:
            creds = kd.get(eid) or {}
            if not creds.get("apiKey"): continue
            try:
                inst_s = cex.get_private(eid, creds)
                if eid == "binance":
                    try: await inst_s.load_time_difference()
                    except Exception: pass
                b_s = await inst_s.fetch_balance()
            except Exception: continue
            if eid == "bitvavo":
                fx = cex.get_fx_rate("EUR") or 1.17
                tot += float((b_s.get("EUR") or {}).get("free") or 0) * fx
                tot += float((b_s.get("USDC") or {}).get("free") or 0)
            else:
                tot += float((b_s.get("USDT") or {}).get("free") or 0)
                tot += float((b_s.get("USDC") or {}).get("free") or 0)
        # Hot wallet USDC ETH
        try:
            tot += (await dex_mod.wallet_token_balance(
                "ethereum", dex_mod.USDC_BY_CHAIN["ethereum"]) or 0) / 1e6
        except Exception: pass
        return tot
    total_before = await _snapshot_total_usd()
    # Persist stats2 rebalance_start. `linked_sid` = None here since this
    # runs from manual /rebalance or from post-DONE propose (no strict
    # linkage at this level; we attach later via most-recent-completed-session).
    try:
        import stats2 as _st2
        _linked_sid = None
        try:
            recent = [s for s in _st2._read_all(_st2.SESSIONS_FILE)
                       if s.get("type") == "session_end"
                       and s.get("outcome") == "completed"
                       and time.time() - s["ts"] < 900]        # <15min
            if recent:
                _linked_sid = sorted(recent, key=lambda s: s["ts"])[-1]["sid"]
        except Exception: pass
        _st2.rebalance_start(rid, _linked_sid, total_before,
                              {"total_usd": total_before})
    except Exception: pass

    for idx, a in enumerate(actions):
        if idx < start_idx:
            continue
        _persist_active_rebal(rid, chat_ids, idx, actions)
        eid = a["eid"]
        # Show what's about to happen — appends "⏳ …", edits after result.
        lines.append(f"⏳ [{idx + 1}/{len(actions)}] {_describe(a)}")
        pending_idx = len(lines) - 1
        await _edit()
        try:
            inst = cex.get_private(eid, kd[eid])
            if a["kind"] == "sell_usdc_eur":
                order = await inst.create_order("USDC/EUR", "market", "sell",
                                                a["amount"], None,
                                                {"operatorId": int(time.time() * 1000)})
                got = float(order.get("cost") or 0)
                lines[pending_idx] = f"✅ [{idx + 1}/{len(actions)}] Bitvavo: SELL {a['amount']:.2f} USDC → {got:.2f} EUR"
            elif a["kind"] == "buy_usdc_eur":
                # Market BUY USDC/EUR — request exact BASE amount.
                # Bitvavo consumes asks until we have `amount` USDC,
                # then charges us whatever EUR that cost.
                order = await inst.create_order("USDC/EUR", "market", "buy",
                                                a["amount"], None,
                                                {"operatorId": int(time.time() * 1000)})
                filled = float(order.get("filled") or 0)
                spent = float(order.get("cost") or 0)
                lines[pending_idx] = f"✅ [{idx + 1}/{len(actions)}] Bitvavo: BUY {filled:.4f} USDC (заплачено {spent:.2f} EUR)"
            elif a["kind"] == "withdraw_usdt":
                # DEPRECATED — kept for backward-compat.  New plans use
                # swap_usdt_to_usdc + withdraw_usdc_eth pair instead.
                net = "BSC"
                params = {"network": net}
                wd = await inst.withdraw("USDT", a["amount"], HOT, None, params)
                txid = wd.get("txid") or ""
                link = dex_mod.tx_link("bsc", txid) if txid else "<i>pending</i>"
                lines[pending_idx] = f"✅ [{idx + 1}/{len(actions)}] {cex.pretty(eid)}: WD {a['amount']:.2f} USDT ({net}) · {link}"
            elif a["kind"] == "swap_usdt_to_usdc":
                # USDC/USDT is a super-tight pair (typical spread <0.01%).
                # Gate rejects "market buy" without price — use IOC limit at
                # 1.005 (guaranteed fill, ≤0.5% slippage cap).
                order = await cex.place_order(inst, "USDC/USDT", "limit",
                                              "buy", a["amount"], 1.005,
                                              {"timeInForce": "IOC"})
                got = float(order.get("filled") or 0)
                if got < a["amount"] * 0.95:
                    lines[pending_idx] = (f"⚠️ [{idx + 1}/{len(actions)}] "
                                           f"{cex.pretty(eid)}: swap частково "
                                           f"{got:.2f}/{a['amount']:.2f} USDC")
                else:
                    lines[pending_idx] = (f"✅ [{idx + 1}/{len(actions)}] "
                                           f"{cex.pretty(eid)}: swap → {got:.2f} USDC")
            elif a["kind"] == "withdraw_usdc_eth":
                # Wait until USDC balance actually reaches target (post-swap settle)
                import time as _t
                deadline = _t.time() + 60
                avail = 0.0
                while _t.time() < deadline:
                    b = await inst.fetch_balance()
                    avail = float((b.get("USDC") or {}).get("free") or 0)
                    if avail >= a["amount"] * 0.98:
                        break
                    await asyncio.sleep(2)
                # Gate deducts the withdrawal fee from the SAME wallet
                # balance. Reserve it or the API returns "not enough balance".
                try:
                    fee_est = 0.0
                    for n in cex.network_info(eid, "USDC"):
                        if (n.get("network") or "").upper() in ("ETH", "ERC20"):
                            fee_est = float(n.get("fee") or 0)
                            break
                    if not fee_est:
                        fee_est = 2.0                          # safe fallback
                except Exception:
                    fee_est = 2.0
                # Reserve fee + safety buffer (2 USDC) — Binance often
                # holds a few $ internally on freshly-swapped USDC, real
                # free can be less than displayed for a minute → -4026.
                real_amt = min(a["amount"], avail - fee_est - 2.0)
                # Binance requires USDC WD amount aligned to 0.000001
                # (6 decimals). Floor to 6 places to satisfy code -4021.
                import math as _m
                real_amt = _m.floor(real_amt * 1_000_000) / 1_000_000
                if real_amt < 5:
                    lines[pending_idx] = (f"❌ [{idx + 1}/{len(actions)}] "
                                           f"{cex.pretty(eid)}: USDC WD skip — "
                                           f"free {avail:.4f} − fee {fee_est:.2f}")
                    continue
                wd = await cex.withdraw_robust(inst, "USDC", real_amt, HOT, None, "ETH")
                txid = wd.get("txid") or ""
                link = dex_mod.tx_link("ethereum", txid) if txid else "<i>pending</i>"
                lines[pending_idx] = (f"✅ [{idx + 1}/{len(actions)}] "
                                       f"{cex.pretty(eid)}: WD {real_amt:.2f} USDC "
                                       f"(ETH, fee {fee_est:.2f}) · {link}")
            elif a["kind"] == "withdraw_usdc":
                # Wait for the prior BUY to settle enough USDC to cover
                # the target amount (up to 60s). Since we now BUY by
                # base amount, filled should match ~exactly.
                import time as _t
                deadline = _t.time() + 60
                avail = 0.0
                target = a["amount"]
                while _t.time() < deadline:
                    b = await inst.fetch_balance()
                    avail = float((b.get("USDC") or {}).get("free") or 0)
                    if avail >= target * 0.98:
                        break
                    await asyncio.sleep(2)
                real_amt = min(target, avail)
                if real_amt < 1.2:
                    lines[pending_idx] = (f"❌ [{idx + 1}/{len(actions)}] "
                                           f"Bitvavo: WD skip — free USDC "
                                           f"{avail:.4f} < 1.2")
                    continue
                wd = await cex.withdraw_robust(inst, "USDC", real_amt,
                                                 HOT, None, None)
                txid = (wd or {}).get("txid") or ""
                link = dex_mod.tx_link("ethereum", txid) if txid else "<i>pending</i>"
                lines[pending_idx] = (f"✅ [{idx + 1}/{len(actions)}] "
                                       f"Bitvavo: WD {real_amt:.2f} USDC "
                                       f"(ETH, fee ~2.90) · {link}")
            elif a["kind"] in ("topup_from_hot", "topup_from_hot_pooled",
                                "topup_from_hot_baseline"):
                # Send from hot wallet → this exchange's deposit address.
                # For pooled variant: WAIT until the hot wallet has enough
                # of the requested stable on the requested chain (arrival
                # from Phase-A withdrawals). Do NOT tap baseline until it
                # actually arrives.
                chain = a["chain"]; sym = a["sym"]; amt = a["amount"]
                contract_map = dex_mod.USDC_BY_CHAIN if sym == "USDC" else dex_mod.USDT_BY_CHAIN
                contract = contract_map.get(chain)
                if not contract:
                    lines.append(f"❌ Hot wallet: немає {sym} контракту для {chain}")
                    continue
                decimals = dex_mod._STABLE_DECIMALS.get((chain, sym), 6)
                if a.get("wait_for_pool"):
                    # Poll on-chain balance up to 15 min for Phase-A
                    # deposits. When something meaningful (>=20 USDC)
                    # arrives, use ONLY that delta — never touch the
                    # baseline (that's trading profit).
                    baseline_raw = await dex_mod.wallet_token_balance(chain, contract) or 0
                    baseline_h = baseline_raw / (10 ** decimals)
                    need_h = amt
                    lines.append(f"⏳ чекаю приходу {sym} на hot wallet ({chain}, baseline {baseline_h:.2f}, "
                                 f"треба до {need_h:.2f})…")
                    try:
                        await _edit()
                    except Exception:
                        pass
                    import time as _time
                    deadline = _time.time() + 2700  # 45 min for slow ETH mainnet
                    arrived = 0.0
                    while _time.time() < deadline:
                        await asyncio.sleep(20)          # was 5s (4× less RPC)
                        cur = await dex_mod.wallet_token_balance(chain, contract) or 0
                        delta = (cur - baseline_raw) / (10 ** decimals)
                        if delta >= need_h * 0.95:
                            arrived = delta
                            break
                        # If we got at least 20 USDC delta and it hasn't
                        # grown in the last 3 min, stop waiting and send
                        # what came.
                        if delta >= 20 and _time.time() - deadline < -300:
                            # keep polling — could still grow
                            pass
                    # After timeout or full arrival, use actual delta
                    if arrived == 0:
                        cur = await dex_mod.wallet_token_balance(chain, contract) or 0
                        delta = (cur - baseline_raw) / (10 ** decimals)
                        arrived = max(0.0, delta)
                    if arrived < 5:
                        lines.append(f"❌ {sym} на hot wallet не прийшло (delta {arrived:.2f})")
                        continue
                    if arrived < need_h:
                        lines.append(f"⚠️ прийшло {arrived:.2f} з {need_h:.2f} — шлемо що є")
                    amt = min(arrived, need_h)                # never spend baseline
                # Fetch destination deposit address on that chain
                net_map = {"bsc": "BSC", "ethereum": "ETH", "polygon": "MATIC",
                           "arbitrum": "ARBITRUM", "base": "BASE",
                           "optimism": "OPTIMISM", "avalanche": "AVAXC"}
                net = net_map.get(chain, chain.upper())
                try:
                    dep = await cex.fetch_deposit_address_robust(inst, sym, net)
                    dst_addr = (dep or {}).get("address") if dep else None
                except Exception as e:
                    dst_addr = None
                    lines.append(f"❌ {cex.pretty(eid)} deposit-addr {sym}/{net}: {str(e)[:80]}")
                    continue
                if not dst_addr:
                    lines.append(f"❌ {cex.pretty(eid)}: не отримано deposit-адреси {sym}/{net}")
                    continue
                amt_wei = int(amt * (10 ** decimals))
                # Gas insurance: make sure HW has native for this send.
                # If HW lacks native, ensure_hot_gas buys+withdraws from Gate.
                try:
                    import gas_refill as _gr
                    if chain != "solana":
                        ok_gas = await _gr.ensure_hot_gas(chain, bypass_rate_limit=True)
                        if not ok_gas:
                            lines.append(f"⛽ HW {chain}: газ низький — чекаю поповнення з Gate…")
                            try: await _edit()
                            except Exception: pass
                            await asyncio.sleep(90)
                except Exception as _e:
                    log.debug("rebalance gas refill %s: %s", chain, _e)
                r_send = await dex_mod.send_token(chain, contract, amt_wei, dst_addr)
                # Retry once if it's a gas-shortage error — refill may
                # still be in flight; ensure_hot_gas re-checks live.
                if (not r_send.get("ok")
                    and "insufficient funds for gas" in str(r_send.get("error", ""))
                    and chain != "solana"):
                    lines.append(f"⛽ {chain}: send впав через газ → чекаю поповнення…")
                    try: await _edit()
                    except Exception: pass
                    try:
                        import gas_refill as _gr
                        await _gr.ensure_hot_gas(chain, bypass_rate_limit=True)
                    except Exception: pass
                    await asyncio.sleep(120)
                    r_send = await dex_mod.send_token(chain, contract, amt_wei, dst_addr)
                if r_send.get("ok"):
                    txh = r_send["tx_hash"]
                    link = dex_mod.tx_link(chain, txh)
                    lines.append(f"✅ Hot → {cex.pretty(eid)}: {amt:.2f} {sym} ({net}) · {link}")
                else:
                    lines.append(f"❌ Hot → {cex.pretty(eid)}: {r_send.get('error','?')[:100]}")
                    try: await _edit()
                    except Exception: pass
                    continue
                # Phase C: after Bitvavo credit → sell USDC → EUR (baseline is EUR)
                if eid == "bitvavo" and sym == "USDC":
                    import time as _t
                    lines.append(f"⏳ чекаю кредит {amt:.2f} USDC на Bitvavo…")
                    try: await _edit()
                    except Exception: pass
                    b0 = await inst.fetch_balance()
                    base0 = float((b0.get("USDC") or {}).get("free") or 0)
                    deadline = _t.time() + 900
                    credited = False
                    while _t.time() < deadline:
                        await asyncio.sleep(5)
                        b1 = await inst.fetch_balance()
                        cur = float((b1.get("USDC") or {}).get("free") or 0)
                        if cur - base0 >= amt * 0.9:
                            credited = True; break
                    if credited:
                        avail = float((await inst.fetch_balance()).get("USDC", {}).get("free") or 0)
                        try:
                            so = await inst.create_order("USDC/EUR", "market", "sell",
                                                          avail, None,
                                                          {"operatorId": int(time.time() * 1000)})
                            got = float(so.get("cost") or 0)
                            lines.append(f"🔁 Bitvavo swap {avail:.2f} USDC → {got:.2f} EUR")
                        except Exception as e:
                            lines.append(f"❌ Bitvavo swap USDC/EUR: {str(e)[:100]}")
                    else:
                        lines.append(f"⚠️ Bitvavo: USDC не зайшло за 15хв — конверт скіп")
                    continue
                # Optional Phase C: wait for USDC credit + swap USDC → USDT
                if a.get("swap_after") and sym == "USDC":
                    lines.append(f"⏳ чекаю кредит {amt:.2f} USDC на {cex.pretty(eid)}…")
                    try: await _edit()
                    except Exception: pass
                    import time as _time
                    deadline = _time.time() + 2700  # 45 min for slow ETH mainnet
                    baseline = 0.0
                    try:
                        b0 = await inst.fetch_balance()
                        baseline = float((b0.get("USDC") or {}).get("free") or 0)
                    except Exception: pass
                    credited = False
                    while _time.time() < deadline:
                        await asyncio.sleep(5)
                        try:
                            b = await inst.fetch_balance()
                            cur = float((b.get("USDC") or {}).get("free") or 0)
                            if cur - baseline >= amt * 0.9:
                                credited = True; break
                        except Exception:
                            pass
                    if not credited:
                        lines.append(f"⚠️ {cex.pretty(eid)}: USDC не зайшло за 15хв — swap скіп")
                        continue
                    # Market sell USDC/USDT
                    try:
                        symbol = "USDC/USDT"
                        b = await inst.fetch_balance()
                        avail = float((b.get("USDC") or {}).get("free") or 0)
                        order = await cex.place_order(inst, symbol, "market", "sell",
                                                       avail, None)
                        got_usdt = float(order.get("cost") or 0)
                        lines.append(f"✅ {cex.pretty(eid)} swap {avail:.2f} USDC → {got_usdt:.2f} USDT")
                    except Exception as e:
                        lines.append(f"❌ {cex.pretty(eid)} USDC/USDT swap: {str(e)[:100]}")
        except Exception as e:
            lines.append(f"❌ {cex.pretty(eid)} {a['kind']}: {str(e)[:100]}")
        try: await _edit()
        except Exception: pass
    # Real cost = totals BEFORE − totals AFTER (in USD).
    total_after = await _snapshot_total_usd()
    real_cost = total_before - total_after
    try:
        import stats2 as _st2
        _st2.rebalance_end(rid, _linked_sid, total_after,
                            {"total_usd": total_after},
                            real_cost, actions)
    except Exception: pass
    lines.append("")
    lines.append(f"<b>Готово.</b>")
    lines.append(f"💵 Загальний портфель: <b>${total_before:.2f} → "
                 f"${total_after:.2f}</b>")
    if real_cost > 0:
        lines.append(f"💸 <b>Реальна вартість ребалансу: ${real_cost:.2f}</b> "
                     f"<i>(різниця балансів до/після)</i>")
        # Attach to last trade's stats — reveal combined net (trade − rebalance)
        try:
            import stats as _stats
            _stats.record_rebalance_cost(real_cost)
            d = _stats.load()
            last = (d.get("last_10") or [])
            if last and "combined_net" in last[0]:
                lines.append(f"📊 Останній трейд: net <b>${last[0]['net_usd']:+.2f}</b> "
                             f"− ребаланс ${real_cost:.2f} = "
                             f"<b>${last[0]['combined_net']:+.2f}</b> combined")
        except Exception as e:
            log.debug("stats rebalance attach err: %s", e)
    elif real_cost < -1:
        lines.append(f"📈 Портфель <b>+${-real_cost:.2f}</b> "
                     f"<i>(swap/FX виграш)</i>")
    try: await _edit()
    except Exception: pass
    _clear_active_rebal()


async def cb_rebalance_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Скасовано.")
    _, rid = q.data.split(":", 1)
    _REBAL_PLANS.pop(rid, None)
    try: await q.edit_message_reply_markup(reply_markup=None)
    except Exception: pass


async def cmd_addresses(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show EVM + Solana deposit addresses for every exchange + HW."""
    if not _is_allowed(update.effective_user.id):
        return
    lines = ["🏷 <b>Депозитні адреси</b>", ""]
    try:
        import token_precache as _tp
        cache = _tp._CACHE
    except Exception:
        cache = {}
    dep = (cache.get("deposit_addresses") or {})
    for eid in cex.SUPPORTED_EXCHANGES:
        pretty = cex.pretty(eid)
        chains = dep.get(eid) or {}
        evm = chains.get("ethereum") or "—"
        sol = chains.get("solana") or "—"
        lines.append(f"<b>{pretty}</b>")
        lines.append(f"  EVM (всі чейни):  <code>{evm}</code>")
        lines.append(f"  Solana:            <code>{sol}</code>")
        lines.append("")
    # Hot wallet
    hw = dex_mod.HOT_WALLET_ADDRESS or "—"
    hw_sol = "—"
    try:
        import sol as _sol
        hw_sol = _sol.HOT_WALLET_ADDRESS or "—"
    except Exception: pass
    lines.append(f"<b>Hot Wallet</b>")
    lines.append(f"  EVM (всі чейни):  <code>{hw}</code>")
    lines.append(f"  Solana:            <code>{hw_sol}</code>")
    lines.append("")
    lines.append("<i>EVM-адреса одна на всі чейни біржі (ERC20/BSC/ARB/BASE/OP/POL/AVAX/...)</i>")
    await update.message.reply_text("\n".join(lines),
                                     parse_mode=ParseMode.HTML,
                                     disable_web_page_preview=True)


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Comprehensive stats — all info in one message."""
    if not _is_allowed(update.effective_user.id):
        return
    try:
        import stats2 as _st2
        text = _st2.render()
    except Exception as e:
        text = f"stats2 err: {e}"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                     disable_web_page_preview=True)


async def _snapshot_all_usd() -> float:
    """Total USD value across every keyed exchange + HW (USDC ETH baseline).
    Values EUR at live FX. Used as ground truth for real PnL — before/after
    balance delta bypasses any per-order fee-math errors."""
    try: await cex._quote_to_usd("EUR")
    except Exception: pass
    kd = keys_mod.load_keys()
    total = 0.0
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = kd.get(eid) or {}
        if not creds.get("apiKey"): continue
        try:
            inst = cex.get_private(eid, creds)
            if eid == "binance":
                try: await inst.load_time_difference()
                except Exception: pass
            b = await inst.fetch_balance()
        except Exception: continue
        if eid == "bitvavo":
            fx = cex.get_fx_rate("EUR") or 1.17
            total += float((b.get("EUR")  or {}).get("total") or 0) * fx
            total += float((b.get("USDC") or {}).get("total") or 0)
        else:
            total += float((b.get("USDT") or {}).get("total") or 0)
            total += float((b.get("USDC") or {}).get("total") or 0)
    try:
        total += (await dex_mod.wallet_token_balance(
            "ethereum", dex_mod.USDC_BY_CHAIN["ethereum"]) or 0) / 1e6
    except Exception: pass
    return total


async def _reb_preview_text() -> str:
    """Fetch balances + estimate the fees the auto-rebalance would burn.
    Used in the post-DONE 'Рекомендую ребаланс' propose message so the
    user sees UPFRONT cost, not just after the fact."""
    kd = keys_mod.load_keys()
    t_gate = float(os.getenv("TARGET_GATE_USDT", "1000"))
    t_binance = float(os.getenv("TARGET_BINANCE_USDT", "1000"))
    t_bitvavo = float(os.getenv("TARGET_BITVAVO_USDC", "2000"))
    lines = ["<b>Поточні баланси:</b>"]
    balances = {}
    for eid in cex.SUPPORTED_EXCHANGES:
        creds = kd.get(eid) or {}
        if not creds.get("apiKey"): continue
        try:
            inst = cex.get_private(eid, creds)
            if eid == "binance":
                try: await inst.load_time_difference()
                except Exception: pass
            b = await inst.fetch_balance()
        except Exception as e:
            lines.append(f"  ⚫ {cex.pretty(eid)}: fetch err {e}")
            continue
        if eid == "bitvavo":
            eur = float((b.get("EUR") or {}).get("free") or 0)
            usdc = float((b.get("USDC") or {}).get("free") or 0)
            fx = cex.get_fx_rate("EUR") or 1.17
            total = usdc + eur * fx
            balances[eid] = {"total": total, "target": t_bitvavo,
                              "eur": eur, "usdc": usdc}
            lines.append(f"  • Bitvavo: <b>€{eur:.2f}</b> + {usdc:.2f} USDC "
                         f"(=${total:.2f}) · ціль ${t_bitvavo:.0f}")
        else:
            usdt = float((b.get("USDT") or {}).get("free") or 0)
            usdc = float((b.get("USDC") or {}).get("free") or 0)
            target = t_gate if eid == "gate" else t_binance
            balances[eid] = {"total": usdt + usdc, "target": target,
                              "usdt": usdt, "usdc": usdc}
            lines.append(f"  • {cex.pretty(eid)}: <b>{usdt:.2f} USDT</b> "
                         f"+ {usdc:.2f} USDC · ціль ${target:.0f}")
    # HW baseline (USDC on ETH)
    try:
        hw_usdc = (await dex_mod.wallet_token_balance(
            "ethereum", dex_mod.USDC_BY_CHAIN["ethereum"]) or 0) / 1e6
    except Exception:
        hw_usdc = 0.0
    lines.append(f"  • HW: <b>{hw_usdc:.2f} USDC</b> (ETH)")
    # Deltas + PRECISE per-step fee list (not %). Simulates what auto-
    # rebalance will do — WD fees per exchange, swap fees at real
    # taker rates, gas per HW send.
    lines.append("\n<b>Дельти:</b>")
    excess_eids: list[str] = []
    deficit_eids: list[str] = []
    total_move_usd = 0.0
    for eid, bal in balances.items():
        delta = bal["target"] - bal["total"]
        if abs(delta) < 20: continue
        sign = "+" if delta > 0 else "−"
        lines.append(f"  • {cex.pretty(eid)}: {sign}${abs(delta):.0f} "
                     f"({'долити' if delta > 0 else 'зайве'})")
        total_move_usd += abs(delta) if delta < 0 else 0
        if delta < 0: excess_eids.append(eid)
        else: deficit_eids.append(eid)
    # Build precise fee list
    fee_items: list[tuple[str, float]] = []           # (desc, usd)
    def _taker(eid, sym):
        try:
            m = (cex._instances.get(eid).markets or {}).get(sym)
            return float(m.get("taker") or 0)
        except Exception: return 0
    def _wd_fee(eid, base, chain):
        try:
            for n in cex.network_info(eid, base):
                nu = (n.get("network") or "").upper()
                if nu in (chain, "ERC20"):
                    return float(n.get("fee") or 0)
        except Exception: pass
        return 0
    for eid in excess_eids:
        excess = -(balances[eid]["target"] - balances[eid]["total"])
        if eid == "bitvavo":
            # Bitvavo path: sell USDC/EUR (if EUR side) → WD USDC ETH
            wd = _wd_fee("bitvavo", "USDC", "ETH") or 2.9
            fee_items.append((f"Bitvavo WD {excess:.0f} USDC ETH", wd))
        else:
            # Gate path: swap USDT→USDC + WD USDC ETH
            t = _taker(eid, "USDC/USDT") or 0.001
            swap_fee = excess * t
            wd = _wd_fee(eid, "USDC", "ETH") or 1.6
            fee_items.append((f"{cex.pretty(eid)} swap USDT→USDC (taker {t*100:.2f}%)",
                              swap_fee))
            fee_items.append((f"{cex.pretty(eid)} WD {excess:.0f} USDC ETH", wd))
    for eid in deficit_eids:
        deficit = balances[eid]["target"] - balances[eid]["total"]
        # HW → this exchange: 1 ETH tx
        fee_items.append((f"HW → {cex.pretty(eid)} gas (ETH)", 0.30))
        if eid != "bitvavo":
            # After credit: swap USDC → USDT (Gate/Binance want USDT)
            t = _taker(eid, "USDC/USDT") or 0.001
            fee_items.append((f"{cex.pretty(eid)} swap USDC→USDT (taker {t*100:.2f}%)",
                              deficit * t))
    if fee_items:
        lines.append("\n<b>Витрати на це:</b>")
        total_fee = 0.0
        for desc, usd in fee_items:
            lines.append(f"  · {desc}: <b>${usd:.2f}</b>")
            total_fee += usd
        lines.append(f"\n💸 <b>Разом: ${total_fee:.2f}</b>")
    else:
        lines.append("\n<i>Все на цілях — ребаланс не потрібен.</i>")
    return "\n".join(lines)


async def cb_rebalance_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handler for '🔄 Запустити ребаланс' offered after each DONE."""
    q = update.callback_query
    await q.answer("Запускаю…")
    if not _is_allowed(q.from_user.id): return
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception: pass
    try:
        # Kick off the same auto-rebalance flow the trade would have run.
        subs = list(_HUNTER.subs) if _HUNTER else [q.message.chat_id]
        asyncio.create_task(_auto_rebalance_post_trade(ctx.application, subs))
    except Exception as e:
        await q.message.reply_text(f"❌ rebalance err: {e}")


async def cb_rebalance_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Ok.")
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception: pass


async def cmd_testrun(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Manually inject a fake signal so the bot runs a full auto-cycle
    with real orders/withdrawals for a chosen (BASE, USD) pair.

    Usage: /testrun AGI 5              — buy Gate, sell Bitvavo, $5
           /testrun AGI 5 reverse       — buy Bitvavo, sell Gate, $5"""
    if not _is_allowed(update.effective_user.id):
        return
    args = ctx.args or []
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: <code>/testrun BASE [USD] [reverse]</code>\n"
            "default: buy Gate, sell Bitvavo, $5",
            parse_mode=ParseMode.HTML)
        return
    if executor.kill_active():
        await update.message.reply_text(
            "🛑 /kill активний — спочатку зроби /kill off"); return
    base = args[0].upper()
    usd = float(args[1]) if len(args) > 1 else 5.0
    flags = [a.lower() for a in args[2:]]
    reverse = any(f.startswith("rev") for f in flags)
    resume = any(f == "resume" or f == "cont" for f in flags)
    from_wallet = any(f == "wallet" or f == "hw" for f in flags)
    only_sell = any(f == "sell" or f == "sellonly" for f in flags)
    try:
        # Use authed (private) instances — public shared instance is
        # rate-limited by the hunter's continuous scan. Auth key raises
        # limits and avoids "429 / order_book err".
        kd = keys_mod.load_keys()
        gate = cex.get_private("gate", kd["gate"])
        bv = cex.get_private("bitvavo", kd["bitvavo"])
        g_sym = f"{base}/USDT"; b_sym = f"{base}/EUR"
        # Retry small burst — Gate sometimes replies with transient errors
        async def _try(inst, sym):
            last = None
            for _ in range(3):
                try:
                    return await inst.fetch_order_book(sym, 5)
                except Exception as e:
                    last = e
                    await asyncio.sleep(0.6)
            raise last
        g_ob = await _try(gate, g_sym)
        b_ob = await _try(bv, b_sym)
        g_ask = float(g_ob["asks"][0][0]); g_bid = float(g_ob["bids"][0][0])
        b_ask = float(b_ob["asks"][0][0]); b_bid = float(b_ob["bids"][0][0])
    except Exception as e:
        await update.message.reply_text(f"❌ price fetch err: {type(e).__name__}: {str(e)[:200]}"); return
    # For testruns we want IOC to succeed even if the top-of-book
    # moves 1-2 ticks between fetch and place — add a 1% headroom on
    # the buy limit and a 1% haircut on the sell limit.
    BUY_BUFFER = 1.01
    SELL_BUFFER = 0.99
    if reverse:
        # buy Bitvavo, sell Gate → alert direction: bitvavo_price < top.price
        fake_bv = b_ask
        top_price = fake_bv * 1.10                     # force top > bpx
        entry = {"kind": "cex", "eid": "gate", "symbol": g_sym,
                 "price": top_price, "spread": 10.0}
        qty = usd / (b_ask * BUY_BUFFER)
        last_buy_nat = b_ask * BUY_BUFFER
        last_sell_nat = g_bid * SELL_BUFFER
    else:
        # buy Gate, sell Bitvavo → alert direction: bitvavo_price > top.price
        fake_bv = g_ask * 1.10
        entry = {"kind": "cex", "eid": "gate", "symbol": g_sym,
                 "price": g_ask, "spread": 10.0}
        qty = usd / (g_ask * BUY_BUFFER)
        last_buy_nat = g_ask * BUY_BUFFER
        last_sell_nat = b_bid * SELL_BUFFER
    alert = {"base": base, "bitvavo_price": fake_bv, "max_spread": 10.0,
             "entries": [entry], "_key": (base,)}
    sizing = {
        "crossed": True, "notional_usd": usd, "profit_usd": 0.05,
        "avg_buy_usd": last_buy_nat, "last_buy_native": last_buy_nat,
        "last_sell_native": last_sell_nat, "qty": qty,
    }
    import secrets as _s
    plan_key = _s.token_hex(4)
    _PENDING_PLANS[plan_key] = {"alert": alert, "sizing": sizing,
                                 "skip_buy": resume or from_wallet or only_sell,
                                 "from_wallet": from_wallet,
                                 "only_sell": only_sell,
                                 "is_testrun": True}
    subs = list(_HUNTER.subs) if _HUNTER else [update.effective_chat.id]
    dir_txt = "Bitvavo→Gate" if reverse else "Gate→Bitvavo"
    mode_tag = " [resume: skip BUY]" if resume else ""
    await update.message.reply_text(
        f"🧪 <b>TESTRUN</b> {base} · ${usd} · {dir_txt}{mode_tag}\n"
        f"real gate ask={g_ask} bv bid={b_bid} → starting auto-session…",
        parse_mode=ParseMode.HTML)
    asyncio.create_task(_run_auto_session(ctx.application, plan_key, subs))


async def cmd_autoexec(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global _AUTOEXEC
    if not _is_allowed(update.effective_user.id):
        return
    args = ctx.args or []
    if not args:
        v = not _AUTOEXEC                              # toggle
    else:
        v = args[0].lower() in ("on", "1", "yes", "true")
    _AUTOEXEC = v
    _save_autoexec_state(v)                           # persist across restarts
    badge = "⚡ УВІМКНЕНО" if v else "⏸ ВИМКНЕНО"
    who = update.effective_user.username or update.effective_user.first_name or "?"
    text = (f"━━━━━━━━━━━━━━\n"
            f"🔀 <b>АВТОЕКЗЕК</b>\n"
            f"      <b>{badge}</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"<i>перемкнув: {who}</i>")
    # Broadcast to every hunter subscriber (both users see change AND get
    # the refreshed keyboard). Sender always gets it too.
    targets = set()
    if _HUNTER:
        targets.update(_HUNTER.subs)
    targets.add(update.effective_chat.id)
    kb = build_main_menu()
    for cid in targets:
        try:
            await ctx.application.bot.send_message(
                cid, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as e:
            log.warning("autoexec broadcast to %s: %s", cid, e)


async def cmd_trades(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    args = ctx.args or []
    try:
        n = int(args[0]) if args else 10
    except ValueError:
        n = 10
    import os as _os
    from executor import TRADES_FILE
    if not _os.path.exists(TRADES_FILE):
        await update.message.reply_text("No trades yet.")
        return
    lines = []
    try:
        with open(TRADES_FILE, encoding="utf-8") as f:
            all_lines = f.readlines()[-n:]
    except Exception as e:
        await update.message.reply_text(f"read err: {e}")
        return
    out = [f"<b>Last {len(all_lines)} trades</b>"]
    import json as _json
    for line in all_lines:
        try:
            d = _json.loads(line)
        except Exception:
            continue
        pnl = d.get("net_pnl_usd")
        pnl_str = f"${pnl:+,.2f}" if pnl is not None else "—"
        out.append(
            f"  <code>{d['trade_id']}</code>  {d['base']:<6}  "
            f"{d['buy_eid']}→{d['sell_eid']}  {d['state']}  {pnl_str}"
        )
    await update.message.reply_text("\n".join(out), parse_mode=ParseMode.HTML)


async def cmd_balances(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show full balances across every keyed exchange + hot wallet."""
    if not _is_allowed(update.effective_user.id):
        return
    msg = await update.message.reply_text("💰 fetching balances…")
    # Force-populate FX cache so EUR isn't shown 1:1 vs USD. Balances
    # runs standalone (no hunter cycle bumps FX), so we do it here.
    try: await cex._quote_to_usd("EUR")
    except Exception: pass
    # Reuse hunter's cached USD-normalized bitvavo prices as a token → USD map.
    price_map: dict[str, float] = {}
    if _HUNTER and getattr(_HUNTER, "last_bitvavo_prices", None):
        price_map = dict(_HUNTER.last_bitvavo_prices)
    try:
        res, wallet_snap = await asyncio.gather(
            keys_mod.balances_all(price_map=price_map),
            dex_mod.wallet_snapshot(price_map=price_map, min_usd=1.0),
        )
    except Exception as e:
        await msg.edit_text(f"balances err: {e}")
        return
    if not res and not (wallet_snap and wallet_snap.get("chains")):
        await msg.edit_text("No exchanges keyed. Add credentials to <code>api_keys.json</code>",
                            parse_mode=ParseMode.HTML)
        return
    await msg.edit_text(keys_mod.format_balances(res, wallet_snap=wallet_snap),
                        parse_mode=ParseMode.HTML)


async def cmd_keys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Verify each api_keys.json entry against its exchange with fetch_balance."""
    if not _is_allowed(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔑 Testing keys… (fetch_balance per exchange)")
    try:
        res = await keys_mod.handshake_all()
    except Exception as e:
        await msg.edit_text(f"handshake err: {e}")
        return
    await msg.edit_text(keys_mod.format_report(res), parse_mode=ParseMode.HTML)


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
            f"Min NET profit filter: <b>${_HUNTER.min_profit_usd:,.2f}</b>  "
            "<i>(after trading + withdraw fees)</i>\n"
            "Usage: <code>/hunt_profit 50</code>  (drops alerts whose executable"
            " net profit is under $50 after fees)",
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
    exec_mode = executor.instance().mode
    mode_badge = "🧪 ТЕСТ" if exec_mode == "dry" else "🔥 БОЙОВИЙ (live)"
    kill_badge = " · 🛑 KILL" if executor.kill_active() else ""
    autoexec_badge = ("⚡ УВІМК" if _AUTOEXEC else "⏸ ВИМК")
    text = (
        f"<b>Стан hunter'а</b>\n"
        f"  Режим торгів: <b>{mode_badge}</b>{kill_badge}\n"
        f"  Автоекзек: <b>{autoexec_badge}</b>\n"
        f"  Мін спред: <b>{_HUNTER.threshold:.2f}%</b>\n"
        f"  Мін профіт: <b>${_HUNTER.min_profit_usd:,.2f}</b>\n"
        f"  Цикл: {_HUNTER.cycle_sec:.0f}с  ·  кулдаун: {_HUNTER.cooldown:.0f}с\n"
        f"  Bitvavo баз: {len(_HUNTER.bases)}\n"
        f"  DS-пули: {sum(1 for v in _HUNTER.pool_cache.values() if v)}"
        f" / {len(_HUNTER.pool_cache)}\n"
        f"  Отримують алерти: {len(_HUNTER.subs)}\n"
        f"  Останній цикл: {_HUNTER.last_cycle_summary or '(старт — прогрів)'}"
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
        direction = f"Купуємо Bitvavo → Продаємо {top_label}" if buy_bv \
            else f"Купуємо {top_label} → Продаємо Bitvavo"

        # Two-sided cross-match against the top CEX target.
        # If the books don't cross → arb window already closed → skip alert entirely.
        size_block = ""
        skip_send = False
        s: dict | None = None                                    # sizing result (None if top is DEX)
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
            # No-book suppression: skip the sizing pass entirely if the
            # same (base, buy, sell) triple keeps failing with empty
            # books — usually a halted market that would spam the log.
            nb_key = (base, buy_eid, sell_eid)
            if _NOBOOK_UNTIL.get(nb_key, 0) > time.time():
                skip_send = True
                s = None
            try:
                if not skip_send:
                    cap = float(os.getenv("DEX_MAX_TX_USD", "1000"))
                    s = await sizing.cross_match(buy_eid, buy_sym, sell_eid, sell_sym,
                                                 max_notional_usd=cap)
                    if s is None:
                        s = await sizing.cross_match(buy_eid, buy_sym, sell_eid, sell_sym,
                                                     max_notional_usd=cap)
                if s and s.get("crossed"):
                    # ─── Fee-aware net profit ───────────────────────────
                    # Pick the fastest common transfer chain to know the
                    # withdraw fee. Then live taker fees for both legs.
                    buy_nets = cex.network_info(buy_eid, base)
                    sell_nets = cex.network_info(sell_eid, base)
                    chain_pick = chains.pick_transfer_chain(buy_nets, sell_nets)
                    chain = chain_pick["chain"] if chain_pick else None
                    tk_buy = await fees_mod.taker_fee(buy_eid, buy_sym)
                    tk_sell = await fees_mod.taker_fee(sell_eid, sell_sym)
                    fee_bundle = fees_mod.total_fees_usd(
                        buy_eid, buy_sym, s["notional_usd"],
                        sell_eid, sell_sym, s["notional_usd"] + s["profit_usd"],
                        base, chain, s["avg_buy_usd"],
                        tk_buy, tk_sell,
                    )
                    net_profit = s["profit_usd"] - fee_bundle["total_usd"]
                    s["net_profit_usd"] = net_profit                    # for downstream / executor
                    s["fee_bundle"] = fee_bundle
                    s["chain"] = chain

                    min_p = _HUNTER.min_profit_usd if _HUNTER else 0.0
                    if net_profit < min_p:
                        skip_send = True                                # net too small
                        log.info("skip alert %s: net $%.2f < min $%.2f "
                                 "(gross $%.2f, fees $%.2f, %s vs %s)",
                                 base, net_profit, min_p,
                                 s["profit_usd"], fee_bundle["total_usd"],
                                 buy_eid, sell_eid)
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
                    chain_line = (
                        f"\n  <i>чейн: {chain} · комісії: ${fee_bundle['total_usd']:,.2f} "
                        f"({fee_bundle['buy_taker_pct']*100:.2f}%+{fee_bundle['sell_taker_pct']*100:.2f}% "
                        f"trade + ${fee_bundle['withdraw_fee_usd']:,.2f} wd)</i>"
                        if chain else
                        "\n  <i>⚠ немає спільного чейну для переказу</i>"
                    )
                    size_block = (
                        f"\n\n💰 <b>Обʼєм: ${s['notional_usd']:,.0f}</b>"
                        f" (~{s['qty']:,.4f} {base})\n"
                        f"  до фі <b>${s['profit_usd']:,.2f}</b>"
                        f" ({s['eff_spread_pct']:.2f}%) · "
                        f"<b>чистий ${net_profit:,.2f}</b>{chain_line}\n"
                        f"  Купити {html.escape(buy_label)} до ціни"
                        f" <b>{bs}{_p(s['last_buy_native'])}</b> {html.escape(s['buy_quote'])}"
                        f"{buy_extra}\n"
                        f"  Продати {html.escape(sell_label)} до ціни"
                        f" <b>{ss}{_p(s['last_sell_native'])}</b> {html.escape(s['sell_quote'])}"
                        f"{sell_extra}"
                    )
                elif not skip_send:
                    # Skip in any non-crossed case — delisted, book empty,
                    # book unfetchable, or top-of-book already crossed.
                    if not s:
                        reason = "no book at all"
                    elif s.get("error"):
                        reason = s["error"]
                    elif "top_ask_native" in s:
                        reason = (f"вікно закрилось: buy@ask "
                                  f"{s['top_ask_native']:.6g} {s.get('buy_quote','')}"
                                  f" >= sell@bid {s['top_bid_native']:.6g} {s.get('sell_quote','')}")
                    else:
                        reason = "no cross"
                    log.info("skip alert %s: %s (%s/%s vs %s/%s)",
                             base, reason, buy_eid, buy_sym, sell_eid, sell_sym)
                    # Track no-book failures — auto-suppress after N in a row
                    if reason == "no book at all" or "book fetch failed" in reason:
                        _NOBOOK_FAILS[nb_key] = _NOBOOK_FAILS.get(nb_key, 0) + 1
                        if _NOBOOK_FAILS[nb_key] >= _NOBOOK_MAX:
                            _NOBOOK_UNTIL[nb_key] = time.time() + _NOBOOK_SUPPRESS_SEC
                            _NOBOOK_FAILS[nb_key] = 0
                            log.info("auto-suppress %s (%s vs %s) for %.0f min — %d nobook fails",
                                     base, buy_eid, sell_eid,
                                     _NOBOOK_SUPPRESS_SEC / 60, _NOBOOK_MAX)
                    else:
                        _NOBOOK_FAILS[nb_key] = 0                # reset streak on any success/cross info
                    skip_send = True
            except Exception as ex:
                log.warning("sizing err for %s: %s — dropping alert", base, ex)
                skip_send = True

        # Re-quote each DEX entry at the RIGHT notional so the price in
        # the alert reflects reality at execution size:
        #   1. If CEX-CEX sizing crossed → use that notional (best signal
        #      of what's actually tradable right now).
        #   2. Else walk Bitvavo's book on the arb-direction side up to
        #      DEX_MAX_TX_USD to find how much Bitvavo alone can absorb.
        #      Cap the Kyber quote to min(bitvavo_side, DEX_MAX_TX_USD).
        #   3. Fall back to KYBER_QUOTE_USD if book unavailable.
        dex_entries_in_alert = [e for e in entries if e["kind"] == "dex"]
        if dex_entries_in_alert:
            dex_cap = float(os.getenv("DEX_MAX_TX_USD", "1000"))
            fallback = float(os.getenv("KYBER_QUOTE_USD", "1000"))
            # Must be defined on EVERY path — the `crossed` branch below
            # skips the Bitvavo book walk, and the ladder/exec-cap code
            # further down reads it unconditionally. Missing init here
            # raised NameError and killed 58 alert dispatches.
            _tight_absorb = 0.0
            if s and s.get("crossed"):
                quote_size = min(s["notional_usd"], dex_cap)
            else:
                # Walk Bitvavo's book for the direction we'd actually take.
                bv_sym_eur = f"{base}/EUR"
                # If Bitvavo cheap vs DEX → we BUY on Bitvavo → consume asks.
                # If Bitvavo expensive vs DEX → we SELL on Bitvavo → consume bids.
                top_dex = dex_entries_in_alert[0]
                side = "asks" if bpx < top_dex["price"] else "bids"
                # Two-tolerance sizing:
                #   tight (BV_BOOK_TOL_TIGHT ~= LIMIT buffer 0.10%) —
                #     what will realistically fill on FIRST attempt;
                #   loose (BV_BOOK_TOL_PCT ~= LIMIT-wait 5min headroom) —
                #     what could fill given the sitting-limit refill.
                # We use LOOSE for quote_size (Kyber ladder up-bound) but
                # STORE both so the auto-run gate can prefer TIGHT-safe rungs.
                _bv_tol_tight = float(os.getenv("BV_BOOK_TOL_TIGHT", "0.15"))
                _bv_tol = float(os.getenv("BV_BOOK_TOL_PCT", "0.5"))
                bv_side, bv_side_tight = await asyncio.gather(
                    sizing.book_side_notional(
                        "bitvavo", bv_sym_eur, side,
                        ref_price_usd=bpx, max_notional_usd=dex_cap,
                        tolerance_pct=_bv_tol),
                    sizing.book_side_notional(
                        "bitvavo", bv_sym_eur, side,
                        ref_price_usd=bpx, max_notional_usd=dex_cap,
                        tolerance_pct=_bv_tol_tight),
                )
                if bv_side:
                    quote_size = min(bv_side["notional_usd"], dex_cap)
                    _tight_absorb = (bv_side_tight or {}).get("notional_usd", 0)
                    log.info("dex sizing %s: bitvavo %s absorbs $%.0f loose "
                             "/ $%.0f tight (cap $%.0f)",
                             base, side, bv_side["notional_usd"],
                             _tight_absorb, dex_cap)
                else:
                    quote_size = min(fallback, dex_cap)
                    _tight_absorb = 0
            # MULTI-SIZE Kyber probe — quotes BOTH buy and sell sides
            # in one call (dex.usd_price returns both). We ladder sizes
            # and evaluate net per size using the DIRECTIONAL price:
            #   Bitvavo cheap (bpx < dex) → we SELL on DEX → use price_sell
            #   Bitvavo expensive (bpx > dex) → we BUY on DEX → use price_buy
            # Alert threshold uses min_profit_usd floor. Actual EXEC size
            # is separately capped by AUTOEXEC_DEX_MAX_USD (test: $100).
            min_p_floor = float(os.getenv("AUTOEXEC_MIN_PROFIT_USD",
                                            os.getenv("MIN_PROFIT_USD", "15")))
            if _HUNTER and _HUNTER.min_profit_usd:
                min_p_floor = max(min_p_floor, _HUNTER.min_profit_usd)
            exec_cap = float(os.getenv("AUTOEXEC_DEX_MAX_USD", "100"))
            # Coarser ladder — was 10 rungs which caused 377×10×5 = ~19k
            # Kyber HTTP req per hunter cycle → Cloudflare throttled us
            # into 40-50s cycles and event loop starvation. 4 rungs still
            # covers small/mid/large size discovery.
            # Rungs must reach the per-tx cap, otherwise a 13-30%
            # spread still gets traded at $1k while the book and the
            # balance would carry far more. The ladder picks the best
            # net among these, so a thin book still sizes itself down.
            _full_rungs = (100, 500, 1000, 2500, 5000)
            ladder = [s_ for s_ in _full_rungs if s_ <= quote_size]
            if quote_size not in ladder:
                ladder.append(quote_size)
            # Ensure the TIGHT-absorb rung is in ladder too — it's the
            # size we can realistically fill fast on Bitvavo (this is the
            # rung the auto-exec cap is based on, so it MUST be probed)
            if _tight_absorb and _tight_absorb >= 20 \
                    and _tight_absorb not in ladder \
                    and _tight_absorb <= quote_size:
                ladder.append(_tight_absorb)
                ladder.sort()
            # Import both aggregators
            try:
                import sol_dex as _sol
            except Exception:
                _sol = None
            for de in dex_entries_in_alert:
                if not de.get("contract"):
                    continue
                de["_okx_spot"] = de.get("price", 0)          # snapshot pre-Kyber
                # Route to correct aggregator: Kyber for EVM, Jupiter for Solana
                is_solana = de["chain"] == "solana"
                if is_solana and not _sol:
                    de["kyber_quote"] = {
                        "size_usd": 0, "price_usd": 0, "slippage_pct": 0,
                        "gross_usd": 0, "fees_usd": 0, "net_usd": 0,
                        "profitable": False, "exec_ok": False,
                        "exec_size_usd": 0, "exec_net_usd": 0,
                        "error": "Jupiter module unavailable",
                        "min_p": min_p_floor,
                    }
                    continue
                if not is_solana and de["chain"] not in dex_mod.KYBER_CHAIN:
                    de["kyber_quote"] = {
                        "size_usd": 0, "price_usd": 0, "slippage_pct": 0,
                        "gross_usd": 0, "fees_usd": 0, "net_usd": 0,
                        "profitable": False, "exec_ok": False,
                        "exec_size_usd": 0, "exec_net_usd": 0,
                        "error": f"Kyber N/A on {de['chain']} (EVM only)",
                        "min_p": min_p_floor,
                    }
                    continue

                async def _quote_at(size, _is_sol=is_solana):
                    try:
                        if _is_sol:
                            r = await _sol.usd_price(de["contract"],
                                                       usd_notional=size,
                                                       reference_price_usd=bpx)
                        else:
                            r = await dex_mod.usd_price(de["chain"], de["contract"],
                                                         usd_notional=size,
                                                         reference_price_usd=bpx)
                        return size, r
                    except Exception:
                        return size, None
                results = await asyncio.gather(*(_quote_at(s_) for s_ in ladder))

                probes = []
                for sz, r in results:
                    if not r:
                        probes.append({"size_usd": sz, "price_usd": 0,
                                        "gross_usd": 0, "fees_usd": 0,
                                        "net_usd": -9999, "ok": False,
                                        "side": None})
                        continue
                    # Direction: which Kyber side do we actually use?
                    okx_ref = de.get("_okx_spot", 0)
                    if bpx < okx_ref:                       # sell to DEX
                        kpx = r.get("price_sell_usd") or 0
                        side = "sell"
                        if kpx > 0:
                            gross_pct = (kpx - bpx) / bpx
                        else:
                            gross_pct = 0
                    else:                                    # buy from DEX
                        kpx = r.get("price_buy_usd") or 0
                        side = "buy"
                        if kpx > 0:
                            gross_pct = (bpx - kpx) / kpx
                        else:
                            gross_pct = 0
                    if kpx <= 0:
                        probes.append({"size_usd": sz, "price_usd": 0,
                                        "gross_usd": 0, "fees_usd": 0,
                                        "net_usd": -9999, "ok": False,
                                        "side": side})
                        continue
                    gross = sz * gross_pct
                    # Bitvavo taker + REAL Kyber gas for this chain.
                    # Was a flat $5, which on Ethereum at 0.09 gwei
                    # (~$0.11 actual) charged ~$4.90 of fiction and
                    # buried every marginal opportunity under the floor.
                    _gas_usd = dex_mod.swap_gas_cost_usd(de["chain"])
                    fees = sz * 0.0035 + _gas_usd
                    net = gross - fees
                    probes.append({"size_usd": sz, "price_usd": kpx,
                                    "gross_usd": gross, "fees_usd": fees,
                                    "net_usd": net, "ok": True,
                                    "side": side})
                # Pick winner — max net. For AUTO-RUN we further cap to
                # BOTH exec_cap AND the TIGHT Bitvavo book absorb (safer
                # size that'll actually fill on first LIMIT attempt).
                best = max(probes, key=lambda p: p["net_usd"])
                # Exec-cap winner: best probe with size <= exec_cap AND
                # (if we have tight-absorb reading) size <= tight_absorb.
                # This is what user wants: don't auto-run at a fantasy
                # $1000 rung when Bitvavo book only realistically holds
                # $389 near best_ask. Better to auto-run $300 profitably
                # than to plan $1000 and fail.
                _exec_hard_cap = exec_cap
                if _tight_absorb and _tight_absorb > 0:
                    _exec_hard_cap = min(_exec_hard_cap, _tight_absorb * 1.05)
                exec_probes = [p for p in probes
                                if p["ok"] and p["size_usd"] <= _exec_hard_cap]
                exec_best = max(exec_probes, key=lambda p: p["net_usd"]) \
                    if exec_probes else None
                if best["ok"]:
                    de["price"] = best["price_usd"]
                    de["spread"] = abs(bpx - best["price_usd"]) / \
                        min(bpx, best["price_usd"]) * 100.0
                    de["quote_notional"] = best["size_usd"]
                    prev_okx = de.get("_okx_spot", de["price"])
                    slip_pct = (best["price_usd"] - prev_okx) / prev_okx * 100.0 \
                        if prev_okx > 0 else 0
                    de["kyber_quote"] = {
                        **best,
                        "slippage_pct": slip_pct,
                        "min_p": min_p_floor,
                        # Alert fires if BEST net at any size hits min_p
                        "profitable": best["net_usd"] >= min_p_floor,
                        # Auto-run uses smaller exec_best; may differ from best
                        "exec_size_usd": exec_best["size_usd"] if exec_best else 0,
                        "exec_net_usd": exec_best["net_usd"] if exec_best else 0,
                        "exec_price_usd": exec_best["price_usd"] if exec_best else 0,
                        "exec_ok": bool(exec_best and exec_best["net_usd"] > 0),
                        "ladder": [(p["size_usd"], p["net_usd"]) for p in probes],
                    }
                else:
                    de["kyber_quote"] = {
                        "size_usd": ladder[-1], "price_usd": 0,
                        "slippage_pct": 0, "gross_usd": 0, "fees_usd": 0,
                        "net_usd": 0, "profitable": False,
                        "exec_ok": False, "exec_size_usd": 0, "exec_net_usd": 0,
                        "error": "no Kyber route across ladder",
                        "min_p": min_p_floor,
                    }
            # re-sort so DEX entry may become top if its refreshed spread wins
            entries.sort(key=lambda e: -e["spread"])
            top = entries[0]                                       # refresh headline
            top_price = top["price"]
            if top["kind"] == "cex":
                top_label = cex.pretty(top["eid"])
                top_url = cex.trading_url(top["eid"], top["symbol"])
            else:
                top_label = f"{top['chain'].upper()} {top['dex_id']}"
                top_url = top["url"] or "https://kyberswap.com"
            buy_bv = bpx < top_price
            direction = f"Buy Bitvavo → Sell {top_label}" if buy_bv \
                else f"Buy {top_label} → Sell Bitvavo"

            # GATE: DEX-top alerts fire ONLY when Kyber-quoted net at some
            # size >= min_profit floor. Silent drop otherwise (no alert).
            if top["kind"] == "dex":
                top_kq = top.get("kyber_quote") or {}
                top_net = top_kq.get("net_usd", -9999)
                if top_kq.get("error") or top_net < min_p_floor:
                    log.info("skip DEX alert %s: kyber best net $%.2f < min $%.0f (%s)",
                             base, top_net, min_p_floor,
                             top_kq.get("error", "insufficient net"))
                    skip_send = True

        # Refresh network status per-token for every involved exchange
        # BEFORE rendering — always fresh dep/wd/networks in the alert.
        # Uses per-coin endpoints on Bitget/Gate/Bitvavo; Binance stays
        # on its bulk cache since it has no per-token public endpoint.
        involved_eids = ["bitvavo"] + [e["eid"] for e in entries if e["kind"] == "cex"]
        try:
            await cex.refresh_networks_for(involved_eids, cex._PROXIES, base=base)
        except Exception as ex:
            log.debug("network refresh err: %s", ex)

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

        def _dex_line(e: dict) -> str:
            sign = "+" if e["price"] > bpx else "−" if e["price"] < bpx else ""
            return (f'  <b><a href="{html.escape(e["url"])}">'
                    f'{e["chain"].upper()} {html.escape(e["dex_id"])}</a></b> '
                    f'<code>{html.escape(base)}</code>: {_fmt_price(e["price"])}  '
                    f'({sign}{e["spread"]:.2f}%)')

        def _dex_chips(e: dict) -> list[str]:
            """Depth + optional Kyber-quote line — parallel to _net_chips."""
            out = []
            liq = e.get("liq", 0) or 0
            vol = e.get("vol24h", 0) or 0
            chip = f"      · {e['chain'].upper()}"
            if liq >= 1:
                chip += f": liq ${liq:,.0f}"
            elif vol >= 1:
                chip += f": vol24h ${vol:,.0f}"
            if e.get("contract"):
                chip += f'\n         <code>{html.escape(e["contract"])}</code>'
            out.append(chip)
            # Kyber executable quote (post re-quote — filled by re-quote block)
            kq = e.get("kyber_quote")
            if kq:
                if kq.get("error"):
                    out.append(f"      · Kyber: ❌ {kq['error']}")
                else:
                    prof_mark = "✅" if kq.get("profitable") else "❌"
                    min_p = kq.get("min_p", 15)
                    side_tag = kq.get("side", "?").upper()
                    out.append(
                        f"      · Kyber {side_tag} best @ <b>${kq['size_usd']:,.0f}</b>: "
                        f"px {_fmt_price(kq['price_usd'])}  "
                        f"slip {kq['slippage_pct']:+.2f}%  "
                        f"→ net <b>${kq['net_usd']:+,.2f}</b> "
                        f"{prof_mark} (min ${min_p:.0f})"
                    )
                    # Auto-run executes at capped size — show separately
                    exec_sz = kq.get("exec_size_usd", 0)
                    exec_net = kq.get("exec_net_usd", 0)
                    if exec_sz > 0 and exec_sz != kq["size_usd"]:
                        exec_mark = "✅" if kq.get("exec_ok") else "❌"
                        out.append(
                            f"      · 🚀 Auto-run @ <b>${exec_sz:,.0f}</b>: "
                            f"net <b>${exec_net:+,.2f}</b> {exec_mark}"
                        )
                    # Full ladder — see where depth breaks
                    ladder = kq.get("ladder") or []
                    if ladder and len(ladder) > 1:
                        parts = []
                        for sz, net in ladder:
                            marker = "▶" if sz == kq["size_usd"] else " "
                            parts.append(f"{marker}${sz}:${net:+.1f}")
                        out.append("         " + "  ".join(parts))
            return out

        bitvavo_row = f"  <b>Bitvavo</b> <code>{base}/EUR</code>: {_fmt_price(bpx)}{eur_part}"
        bitvavo_nets = _net_chips("bitvavo")

        # Compact 1-line summary shown right at top so user sees net/notional
        # without scrolling past all the price/network chips.
        # HARD-GATE: if the best available net < AUTOEXEC_MIN_PROFIT_USD,
        # drop the alert entirely — no reason to spam alerts we won't run.
        tldr = ""
        _min_p_alert = float(os.getenv("AUTOEXEC_MIN_PROFIT_USD",
                                         os.getenv("MIN_PROFIT_USD", "20")))
        _best_net = None
        # ✅ green mark ONLY when data is OKX-verified. DS-only entries
        # (fallback pool price) get ℹ️ — signal that auto-run is not
        # backed by a precision quote.
        _dex_top_verified = (top["kind"] == "dex"
                              and top.get("dex_id") == "okx")
        if s and s.get("crossed"):
            _best_net = s.get("net_profit_usd")
            _sz = s.get("notional_usd", 0)
            if _best_net is not None:
                if _best_net >= _min_p_alert:
                    _prof = "✅"
                else:
                    _prof = "⚠️"
                tldr = (f"💰 <b>${_sz:,.0f}</b> · net <b>${_best_net:+,.2f}</b> "
                        f"{_prof}\n")
        if top["kind"] == "dex":
            top_kq = top.get("kyber_quote") or {}
            _dex_net = top_kq.get("net_usd")
            if _dex_net is not None:
                _best_net = max(_best_net or -9999, _dex_net)
            _sz = top_kq.get("size_usd", 0)
            if _dex_net is not None and _sz:
                if top_kq.get("profitable") and _dex_top_verified:
                    _prof = "✅"
                elif top_kq.get("profitable"):
                    _prof = "ℹ️"                          # profitable but DS-only
                else:
                    _prof = "⚠️"
                if not tldr:
                    tldr = (f"💰 <b>${_sz:,.0f}</b> (Kyber) · "
                            f"net <b>${_dex_net:+,.2f}</b> {_prof}\n")
        # HARD-GATE: alert MUST have a computable net profit >= min_p.
        # If _best_net is None (no CEX cross AND no DEX Kyber quote), we
        # have no evidence the arb is profitable → drop. If it's below
        # min_p → drop. Zero silent noise.
        if _best_net is None or _best_net < _min_p_alert:
            log.info("skip alert %s: best net %s < min $%.0f (hard-gate)",
                     base,
                     f"${_best_net:.2f}" if _best_net is not None else "N/A",
                     _min_p_alert)
            return False

        lines = [
            f"🎯 <b>{base}</b>  ·  <b>{a['max_spread']:.2f}%</b>",
            direction,
            tldr if tldr else "",
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
            # DEX top — render DEX row with same detail as CEX top
            top_row = _dex_line(top)
            top_nets = _dex_chips(top)
            if buy_bv:                                         # buy Bitvavo, sell DEX
                lines.append(bitvavo_row); lines.extend(bitvavo_nets)
                lines.append(top_row);      lines.extend(top_nets)
            else:                                              # buy DEX, sell Bitvavo
                lines.append(top_row);      lines.extend(top_nets)
                lines.append(bitvavo_row); lines.extend(bitvavo_nets)

        # Remaining exchanges (excluding the top one) → Other CEX
        other_cex_entries = [e for e in entries
                             if e["kind"] == "cex" and e is not top]
        dex_entries = [e for e in entries if e["kind"] == "dex"]

        if other_cex_entries:
            lines.append("\n<b>Інші CEX</b>")
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
                sign = "+" if e["price"] > bpx else "−" if e["price"] < bpx else ""
                # Depth: prefer liq (DexScreener); fall back to 24h vol
                # (OKX Web3 exposes vol but not liq for many tokens).
                liq = e.get("liq", 0) or 0
                vol = e.get("vol24h", 0) or 0
                if liq >= 1:
                    depth = f"liq ${liq:,.0f}"
                elif vol >= 1:
                    depth = f"vol24h ${vol:,.0f}"
                else:
                    depth = "—"
                # Display-only marker (no directional arb vs Bitvavo)
                tag = "" if e.get("arb_ok", True) else "  ℹ️"
                lines.append(
                    f'  · <a href="{html.escape(e["url"])}">{e["chain"].upper()} {html.escape(e["dex_id"])}</a>: '
                    f'{_fmt_price(e["price"])}  ({sign}{e["spread"]:.2f}%)  ·  {depth}{tag}'
                )

        if skip_send:
            return False                                       # arb closed / profit below floor
        text = "\n".join(lines) + size_block
        bitvavo_url = cex.trading_url("bitvavo", f"{base}/EUR")
        if buy_bv:
            buy_url, sell_url = bitvavo_url, top_url
            buy_lbl, sell_lbl = "🟢 Купити Bitvavo", f"🔴 Продати {top_label}"
        else:
            buy_url, sell_url = top_url, bitvavo_url
            buy_lbl, sell_lbl = f"🟢 Купити {top_label}", "🔴 Продати Bitvavo"
        top_eid = top["eid"] if top["kind"] == "cex" else ""

        # Stash the plan so the Execute callback can retrieve everything
        # without re-doing the sizing pass. Key by short random id.
        # DEX-top alerts are ALSO stored (for auto-run) but ONLY if size
        # ≤ AUTOEXEC_DEX_MAX_USD — safe low-size test bed.
        plan_key = None
        _autoexec_dex_cap = float(os.getenv("AUTOEXEC_DEX_MAX_USD", "50"))
        # DEX auto-run REQUIRES a fresh Kyber quote at the actual execution
        # size that returns positive net (spread minus fees minus gas).
        # A Kyber-only slippage of 3-5% typical for small pools eats the arb.
        _dex_kq = (top.get("kyber_quote") or {}) if top["kind"] == "dex" else {}
        # Auto-run for DEX top REQUIRES OKX-verified price (dex_id=="okx")
        # AND Kyber execution net > 0. DS-only entries (pool price) do NOT
        # auto-run — they're display-only until an OKX refresh confirms.
        _dex_ok = (top["kind"] == "dex"
                   and top.get("dex_id") == "okx"
                   and _dex_kq.get("exec_ok")
                   and _dex_kq.get("exec_size_usd", 0) > 0)
        _cex_ok = top["kind"] == "cex" and s and s.get("crossed")
        # ALWAYS stash a plan-key when there's at least one entry, even if
        # auto-exec gates rejected. The button lets user tap to see the
        # ranked routes — otherwise a "0.5% spread but no button" alert is
        # a UX dead-end (see TAIKO/AVNT/PENDLE user report).
        if _cex_ok or _dex_ok or entries:
            import secrets
            plan_key = secrets.token_hex(4)                   # unique per alert
            # For DEX-top: override sizing so notional matches the size
            # Kyber says is optimal (not what Bitvavo book alone would
            # absorb). Otherwise plan.qty = huge → deep slippage kills PnL.
            sizing_for_plan = s
            if top["kind"] == "dex" and _dex_kq.get("exec_size_usd"):
                # Auto-run uses the EXEC-cap sub-winner (test: $100 max)
                dex_size = float(_dex_kq["exec_size_usd"])
                exec_px = _dex_kq.get("exec_price_usd") or _dex_kq["price_usd"]
                sizing_for_plan = {
                    "crossed": True,
                    "notional_usd": dex_size,
                    "profit_usd": _dex_kq.get("gross_usd", 0),
                    "net_profit_usd": _dex_kq.get("exec_net_usd", 0),
                    "avg_buy_usd": min(bpx, exec_px),
                    "avg_sell_usd": max(bpx, exec_px),
                    "last_buy_native": exec_px,
                    "last_sell_native": exec_px,
                    "qty": dex_size / max(bpx, 1e-12),
                    "buy_quote": "USDC", "sell_quote": "EUR",
                }
            _PENDING_PLANS[plan_key] = {"alert": a, "sizing": sizing_for_plan,
                                          "alert_ts": time.time()}
            # LRU cap — evict oldest when exceeding 60
            if len(_PENDING_PLANS) > 60:
                oldest = next(iter(_PENDING_PLANS))
                _PENDING_PLANS.pop(oldest, None)

        # Кнопки: 🚀 План (побудувати маршрути) + 🚫 Блеклист.
        # План будується ТІЛЬКИ на тап — не спамимо коли ти не за телефоном.
        rows = []
        if plan_key:
            rows.append([InlineKeyboardButton(
                "🚀 Побудувати план",
                callback_data=f"chk:{plan_key}",
            )])
        rows.append([
            InlineKeyboardButton("🚫 Блеклист",
                                 callback_data=f"blm:{base}:{top_eid}"),
            InlineKeyboardButton("⏳ Mute 1h",
                                 callback_data=f"bl1h:{base}"),
        ])
        kb = InlineKeyboardMarkup(rows)

        # Autoexec fires ONLY when both plan_key AND (_cex_ok or _dex_ok) —
        # plan_key alone doesn't mean the trade is viable (the button-only
        # branch stashes plan for user tap without auto-run intent).
        autoexec_ok = plan_key and (_cex_ok or _dex_ok)
        if autoexec_ok and _AUTOEXEC and not executor.kill_active():
            asyncio.create_task(_run_auto_session(app, plan_key, subs))

        any_sent = False
        for chat_id in subs:
            try:
                await app.bot.send_message(
                    chat_id=chat_id, text=text, parse_mode=ParseMode.HTML,
                    reply_markup=kb, disable_web_page_preview=True,
                )
                any_sent = True
            except Exception as e:
                log.warning("hunter send to %s failed: %s", chat_id, e)
        return any_sent
    return send


async def _auto_plan_and_notify(app: Application, plan_key: str, subs: list[int]):
    """After an alert, enumerate EVERY route (CEX×CEX + DEX) and post a
    ranked list of viable options. User picks one → session starts."""
    payload = _PENDING_PLANS.get(plan_key)
    if not payload:
        return
    alert = payload["alert"]
    base = alert["base"]
    bpx = alert["bitvavo_price"]
    top_sizing = payload["sizing"]                                # from hunter's top route
    max_usd = top_sizing.get("notional_usd", 100.0)

    ex = executor.instance()
    mode_tag = "🔥 БОЙОВИЙ" if ex.mode == "live" else "🧪 ТЕСТ"

    # Enumerate one plan per entry, keep the routes sorted by net profit
    routes: list[dict] = []                                       # {plan, reasons, entry}
    bv_sym = f"{base}/EUR"
    for entry in alert["entries"]:
        try:
            if entry["kind"] == "cex":
                # Freshly cross-match this route's books
                if bpx < entry["price"]:
                    buy_eid, buy_sym = "bitvavo", bv_sym
                    sell_eid, sell_sym = entry["eid"], entry["symbol"]
                else:
                    buy_eid, buy_sym = entry["eid"], entry["symbol"]
                    sell_eid, sell_sym = "bitvavo", bv_sym
                s = await sizing.cross_match(buy_eid, buy_sym, sell_eid, sell_sym)
                if not s or not s.get("crossed"):
                    routes.append({"entry": entry, "plan": None,
                                   "reasons": ["стакани не перетинаються"]})
                    continue
                plan = await ex.plan(alert, s)
            else:                                                 # DEX
                # DEX plan built from hunter's Kyber quote
                dex_sizing = {
                    "notional_usd": max_usd, "qty": max_usd / bpx,
                    "profit_usd": 0.0, "net_profit_usd": 0.0,
                    "last_buy_native": entry["price"],
                    "last_sell_native": entry["price"],
                    "avg_buy_usd": min(bpx, entry["price"]),
                    "avg_sell_usd": max(bpx, entry["price"]),
                    "buy_quote": "USDC", "sell_quote": "EUR",
                    "crossed": True,
                }
                plan = await ex.plan(alert, dex_sizing)
            reasons = ex.precheck_plan(plan)
            routes.append({"entry": entry, "plan": plan, "reasons": reasons})
        except Exception as e:
            routes.append({"entry": entry, "plan": None, "reasons": [f"плану не побудовано: {e}"]})

    # Sort: feasible by net profit desc, then blocked by fewest reasons
    def _rank(r):
        if r["plan"] and not r["reasons"]:
            return (0, -(r["plan"].net_profit_usd or 0))
        return (1, len(r["reasons"]))
    routes.sort(key=_rank)

    # Build message + buttons
    lines = [f"📋 <b>{base}</b> · маршрути ({mode_tag})"]
    rows: list[list] = []
    for i, r in enumerate(routes, start=1):
        entry = r["entry"]
        if entry["kind"] == "cex":
            target = cex.pretty(entry["eid"])
        else:
            target = f"Kyber {entry['chain'].upper()}"
        plan = r["plan"]
        if plan and not r["reasons"]:
            fee = plan.fees or {}
            eta = plan.eta_min or 0
            lines.append(
                f"\n<b>{i}. {target}</b>  ·  net "
                f"<b>${plan.net_profit_usd:,.2f}</b>"
                f"  ({entry['spread']:.2f}%)"
                f"\n   чейн <code>{plan.chain}</code> · ETA {eta:.1f} хв · "
                f"обʼєм ${plan.notional_usd:,.0f} · комісії ${fee.get('total_usd', 0):,.2f}"
            )
            # Stash for the approve callback.
            # This goes in _PENDING_BUTTONS, NOT _SESSIONS. Every alert
            # candidate used to insert a full session row here purely to
            # back a button — those rows then blocked per-base auto-runs
            # and the rebalance watch (which skips while any session is
            # live), and the zombie pruner had to clean up after them.
            # Nothing here holds inventory: it is an offer, not a trade.
            sid = _new_sid()
            receipt = executor.TradeReceipt(
                trade_id=plan.trade_id, base=plan.base,
                buy_eid=plan.buy_eid, sell_eid=plan.sell_eid, mode=ex.mode,
            )
            sess = executor.InteractiveSession(plan=plan, receipt=receipt)
            _PENDING_BUTTONS[sid] = {"plan": plan, "session": sess,
                                      "subs": subs, "ts": time.time()}
            # Evict by AGE, not insertion order: a burst of alerts used
            # to drop 20 entries at once, including offers the user was
            # still looking at. An hour-old offer is stale anyway.
            _cut = time.time() - float(os.getenv("OFFER_TTL_SEC", "3600"))
            for _old in [k for k, v in _PENDING_BUTTONS.items()
                         if (v.get("ts") or 0) < _cut]:
                _PENDING_BUTTONS.pop(_old, None)
            while len(_PENDING_BUTTONS) > 200:      # hard ceiling
                _PENDING_BUTTONS.pop(next(iter(_PENDING_BUTTONS)), None)
            btn = InlineKeyboardButton(
                f"✅ {i}. {target}  →  ${plan.net_profit_usd:,.0f}",
                callback_data=f"pbuy:{sid}",
            )
            rows.append([btn])
        else:
            lines.append(
                f"\n<b>{i}. {target}</b>  ❌  " + "; ".join(r["reasons"])
            )
    rows.append([InlineKeyboardButton("🚫 Пропустити цей алерт",
                                      callback_data=f"skpall:{plan_key}")])

    for cid in subs:
        try:
            await app.bot.send_message(
                cid, "\n".join(lines), parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(rows),
            )
        except Exception:
            pass


async def cb_skip_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Пропущено.")
    try:
        await q.edit_message_text("🚫 <b>Пропущено</b>", parse_mode=ParseMode.HTML)
    except Exception:
        pass


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
    global ALLOWED, _APP
    ALLOWED = allowed
    _load_sessions()
    app = Application.builder().token(token).build()
    _APP = app                      # background loops reach TG through this

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
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("stuck", cmd_stuck))
    app.add_handler(CommandHandler("sweep", cmd_sweep))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CallbackQueryHandler(cb_stuck_action,
                                         pattern=r"^(stkr|stkd):"))
    app.add_handler(CommandHandler("kill", cmd_kill))
    app.add_handler(CommandHandler("autoexec", cmd_autoexec))
    app.add_handler(CommandHandler("testrun", cmd_testrun))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("addresses", cmd_addresses))
    app.add_handler(CommandHandler("trades", cmd_trades))
    app.add_handler(CommandHandler("keys", cmd_keys))
    app.add_handler(CommandHandler("balances", cmd_balances))
    app.add_handler(CommandHandler("rebalance", cmd_rebalance))
    app.add_handler(CommandHandler("target", cmd_target))
    app.add_handler(CallbackQueryHandler(cb_rebalance_execute, pattern=r"^rbx:"))
    app.add_handler(CallbackQueryHandler(cb_rebalance_cancel, pattern=r"^rbc:"))
    app.add_handler(CallbackQueryHandler(cb_rebalance_start, pattern=r"^rbstart:"))
    app.add_handler(CallbackQueryHandler(cb_rebalance_skip, pattern=r"^rbskip$"))
    app.add_handler(CommandHandler("c", cmd_check))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CallbackQueryHandler(cb_execute, pattern=r"^ex:"))
    # menu-button dispatcher — must run BEFORE the free-text catch-all
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND
        & filters.Regex(r"^(💰|🔑|📊|📜|🛑|▶️|⚡|⏸|🚫|❔|🔄|🔀|📈|🏷)"),
        cb_menu_button,
    ), group=-1)
    # rebalance target intake — user replies with "gate 1000 binance 1000 bitvavo 2000"
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, cb_rebalance_targets_reply,
    ), group=-2)
    app.add_handler(CallbackQueryHandler(cb_check_route, pattern=r"^chk:"))
    app.add_handler(CallbackQueryHandler(cb_pick_size, pattern=r"^sz:"))
    app.add_handler(CallbackQueryHandler(cb_phase_buy, pattern=r"^pbuy:"))
    app.add_handler(CallbackQueryHandler(cb_phase_withdraw, pattern=r"^pwd:"))
    app.add_handler(CallbackQueryHandler(cb_phase_sell, pattern=r"^psl:"))
    app.add_handler(CallbackQueryHandler(cb_session_cancel, pattern=r"^(pcx|sscx):"))
    app.add_handler(CallbackQueryHandler(cb_session_skip_step, pattern=r"^sskp:"))
    app.add_handler(CallbackQueryHandler(cb_skip_all, pattern=r"^skpall:"))
    app.add_handler(CallbackQueryHandler(cb_blacklist_menu, pattern=r"^blm:"))
    app.add_handler(CallbackQueryHandler(cb_blacklist_apply,
                                         pattern=r"^(blex|blbase|blback|bl1h|blun|blunperm):"))
    app.add_handler(add_conv)
    # pair-action buttons (pause/refresh/del/edit) — not scoped to conversation
    app.add_handler(CallbackQueryHandler(cb_button, pattern=r"^(pause|refresh|del|edit):"))
    # fallback for free-text (edit threshold triggered from inline)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_free_text))
    return app
