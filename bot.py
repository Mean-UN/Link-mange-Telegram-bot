import asyncio
import io
import logging
import os
import re
import shutil
import sqlite3
import socket
import time
from pathlib import Path
from urllib.parse import urlparse
import urllib.error
import urllib.request
from functools import wraps
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Conflict, NetworkError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import ADMIN_IDS, BOT_TOKEN, DB_PATH
from db import Database

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("linkbot")


db = Database(DB_PATH)

AUTO_DELETE_SECONDS = 300
EP_PAGE_SIZE = 30
TITLE_PAGE_SIZE = 20
ADMIN_AUTO_DELETE_KEY = "admin_auto_delete"
EP_PREFIX = "\u1797\u17B6\u1782"
LABEL_TITLES = "\u1794\u1789\u17D2\u1785\u17B8\u179A\u17BF\u1784\u17D6\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
LABEL_ALL_EPS = "\u1797\u17B6\u1782\u1791\u17B6\u17C6\u1784\u17A2\u179F\u17CB"
DONATE_IMAGE_PATH = "donate_qr.png"
CAMBODIA_UTC_OFFSET_HOURS = 7
STARTUP_RETRY_SECONDS = 10
DEADLINK_DEFAULT_LIMIT = 50
DEADLINK_MAX_LIMIT = 1000
DEADLINK_TIMEOUT = 6
DEADLINK_CONCURRENCY = 20
DEADLINK_SKIP_HTTP = {"HTTP 401", "HTTP 403", "HTTP 429", "HTTP 999"}
AUDITLOG_DEFAULT_LIMIT = 20
AUDITLOG_MAX_LIMIT = 200
PLACEHOLDER_LINK_PATTERNS = ("no.link", "nolink", "no-link", "no_link", "emptylink")
DAILY_TOP_LIMIT = 10
TOPMANGA_DEFAULT_LIMIT = 10
TOPMANGA_MAX_LIMIT = 50
SUPPORT_GROUP = os.getenv("SUPPORT_GROUP", "@YourGroup").strip()
DEVELOPER_NAME = os.getenv("DEVELOPER_NAME", "Mean Un").strip()
DEVELOPER_TAG = os.getenv("DEVELOPER_TAG", "@Mean_Un").strip()
BTN_PREV = "ភាគមុន"
BTN_NEXT = "ភាគបន្ទាប់"
BTN_TITLES_PREV = "ត្រឡប់វិញ"
BTN_TITLES_NEXT = "រឿងបន្ទាប់"


def _log_admin_action(actor_id: int | None, action: str, details: str) -> None:
    if actor_id is None:
        return
    try:
        db.add_audit_log(int(actor_id), action, details)
    except Exception:
        logger.exception("Failed to save audit log: %s", action)


def _track_command_usage(update: Update, command_name: str) -> None:
    if command_name != "mangalink":
        return
    user = update.effective_user
    if not user:
        return
    try:
        db.add_usage_log(int(user.id), command_name)
    except Exception:
        logger.exception("Failed to save usage log: %s", command_name)


def _tracked_command(command_name: str, callback):
    @wraps(callback)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        _track_command_usage(update, command_name)
        await callback(update, context)
    return wrapped


def _is_super_admin(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in ADMIN_IDS)


def _is_admin(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    if user.id in ADMIN_IDS:
        return True
    return user.id in set(db.get_admin_ids())

def _reset_pending(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("pending_action", None)
    context.user_data.pop("pending_title_id", None)
    context.user_data.pop("pending_ep_name", None)
    context.user_data.pop("pending_episode_id", None)


def _set_admin_auto_delete(context: ContextTypes.DEFAULT_TYPE, enabled: bool) -> None:
    if enabled:
        context.user_data[ADMIN_AUTO_DELETE_KEY] = True
    else:
        context.user_data.pop(ADMIN_AUTO_DELETE_KEY, None)


def _admin_auto_delete_enabled(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.user_data.get(ADMIN_AUTO_DELETE_KEY))


def _valid_url(url: str) -> bool:
    if not url:
        return False
    return bool(re.match(r"^https?://", url.strip(), re.IGNORECASE))


def _normalize_url(url: str) -> str:
    u = url.strip()
    u = re.sub(r"^https?://(?:m|web|mobile)\.facebook\.com/", "https://www.facebook.com/", u, flags=re.IGNORECASE)
    return u


def _normalize_ep_name(name: str) -> str:
    n = name.strip()
    if n.startswith(EP_PREFIX):
        return n
    return f"{EP_PREFIX}{n}"


def _display_ep_name(name: str) -> str:
    n = name.strip()
    if n.startswith("???"):
        n = f"{EP_PREFIX}{n[3:]}"
    return n

async def _delete_message_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data if context.job else {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        return


def _schedule_delete(message, context: ContextTypes.DEFAULT_TYPE, force: bool = False) -> None:
    if not message:
        return
    if not force and not _admin_auto_delete_enabled(context):
        return
    if not getattr(context, "job_queue", None):
        return
    try:
        context.job_queue.run_once(
            _delete_message_job,
            when=AUTO_DELETE_SECONDS,
            data={"chat_id": message.chat_id, "message_id": message.message_id},
        )
    except Exception:
        return


async def _reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    msg = await update.message.reply_text(text, **kwargs)
    _schedule_delete(msg, context)
    return msg


def _format_report(title: str, lines: list[str]) -> str:
    return "\n".join([title, "━━━━━━━━━━━━━━━━━━", *lines]).strip()


def _developer_display() -> str:
    if DEVELOPER_NAME and DEVELOPER_TAG:
        return f"{DEVELOPER_NAME} ({DEVELOPER_TAG})"
    return DEVELOPER_TAG or DEVELOPER_NAME or "Unknown"


def _group_display() -> str:
    return SUPPORT_GROUP or "Unknown"


def _help_menu_text() -> str:
    lines = [
        "🗿There Are So Much Features In This Bot!",
        "🫀Use The Buttons Below To Check Classified Features!",
        "",
        "━━━━━━━━━━━━━━━━━━",
        f"Develop by {_developer_display()}",
    ]
    return _format_report("🤖 Link Manga Bot", lines)


def _paginate(items: list, page: int, per_page: int) -> tuple[list, int, int]:
    if page < 0:
        page = 0
    total = len(items)
    pages = max(1, (total + per_page - 1) // per_page)
    if page >= pages:
        page = pages - 1
    start = page * per_page
    end = start + per_page
    return items[start:end], page, pages


async def _send_long_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, chunk_size: int = 3500):
    if len(text) <= chunk_size:
        await _reply_text(update, context, text)
        return
    parts = []
    current = []
    length = 0
    for line in text.split("\n"):
        add_len = len(line) + 1
        if length + add_len > chunk_size and current:
            parts.append("\n".join(current))
            current = [line]
            length = len(line) + 1
        else:
            current.append(line)
            length += add_len
    if current:
        parts.append("\n".join(current))
    for part in parts:
        await _reply_text(update, context, part)


async def _send_long_text_from_query(query, context: ContextTypes.DEFAULT_TYPE, text: str, chunk_size: int = 3500):
    if len(text) <= chunk_size:
        await _reply_to_query(query, context, text)
        return
    parts = []
    current = []
    length = 0
    for line in text.split("\n"):
        add_len = len(line) + 1
        if length + add_len > chunk_size and current:
            parts.append("\n".join(current))
            current = [line]
            length = len(line) + 1
        else:
            current.append(line)
            length += add_len
    if current:
        parts.append("\n".join(current))
    for part in parts:
        await _reply_to_query(query, context, part)


async def _edit_text(query, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    msg = await query.edit_message_text(text, **kwargs)
    _schedule_delete(msg, context)
    return msg


async def _reply_to_query(query, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    msg = await query.message.reply_text(text, **kwargs)
    _schedule_delete(msg, context)
    return msg


async def _send_donate_qr(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not message:
        return
    path = Path(DONATE_IMAGE_PATH)
    if not path.exists():
        msg = await message.reply_text("Donation QR not found.")
        _schedule_delete(msg, context)
        return
    with path.open("rb") as photo:
        msg = await message.reply_photo(photo=photo, caption="💖 Donation QR")
    _schedule_delete(msg, context)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)
    text = (
        "📚 𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝘁𝗼 𝗟𝗶𝗻𝗸 𝗕𝗼𝘁\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Store manga, episodes, and links in one place.\n\n"
        "🚀 Quick Start\n"
        "• /mangalink - browse manga and open links\n"
        "• /listmanga - view all manga titles\n"
        "• /search <keyword> - find manga fast\n"
        "• /mangaupdated [n] - see recent updates\n"
        "• /lastupdate <manga title> - latest update time\n\n"
        "🧰 Useful Tools\n"
        "• /listep 1-10 - generate episode labels\n"
        "• /getuserid - get your ID or replied user's ID\n"
        "\n"
        "🔐 Admin: /mangaadmin\n"
        "💖 Support: /donateadmin\n"
        "👨‍💻 Developed by @Mean_Un"
    )
    await _reply_text(update, context, text)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)
    keyboard = [
        [
            InlineKeyboardButton("👤 User", callback_data="help:user"),
            InlineKeyboardButton("🛠️ Admin", callback_data="help:admin"),
            InlineKeyboardButton("🧰 Tools", callback_data="help:tools"),
        ],
    ]
    await _reply_text(
        update,
        context,
        _help_menu_text(),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    context.user_data.pop("bulk_buffer", None)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context, force=True)
    msg = await _reply_text(update, context, "Cancelled.")
    _schedule_delete(msg, context, force=True)


async def mangalink_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)
    titles = db.get_titles()
    if not titles:
        await _reply_text(update, context, "No manga yet.")
        return

    page_titles, page, pages = _paginate(titles, 0, TITLE_PAGE_SIZE)
    keyboard = [
        [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
        for t in page_titles
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(BTN_TITLES_PREV, callback_data=f"user:titles:{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(BTN_TITLES_NEXT, callback_data=f"user:titles:{page+1}"))
    if nav:
        keyboard.append(nav)
    await _reply_text(update, context, LABEL_TITLES, reply_markup=InlineKeyboardMarkup(keyboard))


async def list_manga_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)

    titles = db.get_titles()
    if not titles:
        await _reply_text(update, context, "No manga yet.")
        return

    lines: list[str] = []
    for idx, title in enumerate(titles, start=1):
        lines.append(f"{idx}. {title['name']}")
    await _send_long_text(update, context, _format_report("📚 𝗠𝗮𝗻𝗴𝗮 𝗟𝗶𝘀𝘁", lines))


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)

    query = " ".join(context.args).strip()
    if not query:
        await _reply_text(update, context, "Usage: /search <keyword>")
        return

    titles = db.get_titles()
    if not titles:
        await _reply_text(update, context, "No manga yet.")
        return

    q = query.casefold()
    matched = [t for t in titles if q in str(t["name"]).casefold()]
    if not matched:
        await _reply_text(update, context, _format_report("🔎 𝗦𝗲𝗮𝗿𝗰𝗵 𝗥𝗲𝘀𝘂𝗹𝘁", [f"❌ No manga found for: {query}"]))
        return

    shown = matched[:TITLE_PAGE_SIZE]
    keyboard = [
        [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
        for t in shown
    ]
    text_lines = [
        "🔎 𝗦𝗲𝗮𝗿𝗰𝗵 𝗥𝗲𝘀𝘂𝗹𝘁",
        "━━━━━━━━━━━━━━━━━━",
        f"🔤 Keyword: {query}",
        f"📚 Found: {len(matched)}",
    ]
    if len(matched) > TITLE_PAGE_SIZE:
        text_lines.append(f"ℹ️ Showing first {TITLE_PAGE_SIZE}. Refine keyword to narrow results.")
    text = "\n".join(text_lines)
    await _reply_text(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard))


async def search_by_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    query = " ".join(context.args).strip()
    if not query:
        await _reply_text(update, context, "Usage: /searchbyadmin <keyword>")
        return

    user = update.effective_user
    if not user:
        await _reply_text(update, context, "User not found.")
        return

    titles = db.get_titles()
    if not titles:
        await _reply_text(update, context, "No manga yet.")
        return

    q = query.casefold()
    matched = [
        t for t in titles
        if q in str(t["name"]).casefold() and _can_manage_title(user.id, int(t["id"]), t["created_by"])
    ]
    if not matched:
        await _reply_text(update, context, f"No manageable manga found for: {query}")
        return

    shown = matched[:TITLE_PAGE_SIZE]
    keyboard = [
        [InlineKeyboardButton(t["name"], callback_data=f"admin:title:{t['id']}")]
        for t in shown
    ]
    keyboard.append([InlineKeyboardButton("Back to admin panel", callback_data="admin:back")])
    text = f"Manageable results for '{query}' ({len(matched)} found):"
    if len(matched) > TITLE_PAGE_SIZE:
        text += f"\nShowing first {TITLE_PAGE_SIZE}. Refine your keyword for fewer results."
    await _reply_text(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard))


async def manga_updated_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)

    days_back = 0
    if context.args:
        if len(context.args) > 1:
            await _reply_text(update, context, "Usage: /mangaupdated [n]\nExample: /mangaupdated 1")
            return
        try:
            days_back = int(context.args[0])
        except ValueError:
            await _reply_text(update, context, "n must be a number. Example: /mangaupdated 2")
            return
        if days_back < 0:
            await _reply_text(update, context, "n must be 0 or higher.")
            return

    now_utc = datetime.utcnow()
    now_kh = now_utc + timedelta(hours=CAMBODIA_UTC_OFFSET_HOURS)
    today_kh = now_kh.date()
    start_date = today_kh - timedelta(days=days_back)
    start_kh_dt = datetime.combine(start_date, datetime.min.time())
    start_utc_dt = start_kh_dt - timedelta(hours=CAMBODIA_UTC_OFFSET_HOURS)
    start_iso = start_utc_dt.isoformat(timespec="seconds")
    rows = db.get_manga_update_counts_since(start_iso)

    title = "🕒 𝗠𝗮𝗻𝗴𝗮 𝗨𝗽𝗱𝗮𝘁𝗲"
    if days_back == 0:
        header_lines = [
            f"🗓️ Date: {today_kh.isoformat()}",
            "📆 Today",
        ]
    else:
        header_lines = [
            f"🗓️ Range: {start_date.isoformat()} to {today_kh.isoformat()}",
            f"📆 {days_back + 1} day(s)",
        ]

    if not rows:
        await _reply_text(
            update,
            context,
            _format_report(
                title,
                [
                    *header_lines,
                    "📚 Manga updated: 0",
                    "🔗 Links updated: 0",
                    "✅ No updates in this period.",
                ],
            ),
        )
        return

    total_added_episodes = sum(int(row["added_episodes"]) for row in rows)
    lines = [
        *header_lines,
        f"📚 Manga updated: {len(rows)}",
        f"🔗 Links updated: {total_added_episodes}",
    ]
    for idx, row in enumerate(rows, start=1):
        lines.append(f"{idx}. {row['title_name']}")
        lines.append(f"   🔗 Added {row['added_episodes']} Links")
    await _send_long_text(update, context, _format_report(title, lines))


async def last_update_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)

    raw = " ".join(context.args).strip()
    if not raw:
        await _reply_text(update, context, "Usage: /lastupdate <manga title>")
        return

    matches = db.search_titles_by_keyword(raw)
    if not matches:
        await _reply_text(update, context, f"Manga not found: {raw}")
        return

    picked = next((t for t in matches if str(t["name"]).casefold() == raw.casefold()), None)
    if not picked and len(matches) == 1:
        picked = matches[0]

    if not picked:
        names = "\n".join(f"- {t['name']}" for t in matches[:10])
        suffix = "\n..." if len(matches) > 10 else ""
        await _reply_text(
            update,
            context,
            f"Multiple manga matched '{raw}'. Please use full title:\n{names}{suffix}",
        )
        return

    stat = db.get_last_update_for_title(int(picked["id"]))
    if not stat or not stat["last_update_at"]:
        await _reply_text(
            update,
            context,
            "🕒 𝗠𝗮𝗻𝗴𝗮 𝗟𝗮𝘀𝘁 𝗨𝗽𝗱𝗮𝘁𝗲\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"📚 Title: {picked['name']}\n"
            "🕐 Last update: No links yet\n"
            "🔗 Total links: 0",
        )
        return

    last_update_utc = datetime.fromisoformat(str(stat["last_update_at"]))
    last_update_kh = last_update_utc + timedelta(hours=CAMBODIA_UTC_OFFSET_HOURS)
    now_kh = datetime.utcnow() + timedelta(hours=CAMBODIA_UTC_OFFSET_HOURS)
    days_ago = (now_kh.date() - last_update_kh.date()).days
    await _reply_text(
        update,
        context,
        "🕒 𝗠𝗮𝗻𝗴𝗮 𝗟𝗮𝘀𝘁 𝗨𝗽𝗱𝗮𝘁𝗲\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📚 Title: {stat['title_name']}\n"
        f"🕐 Last update: {last_update_kh.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"📆 Count day ago: {days_ago} day(s)\n"
        f"🔗 Total links: {stat['total_links']}",
    )


async def auto_delete_join_leave_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if not (message.new_chat_members or message.left_chat_member):
        return

    try:
        await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
    except Exception:
        return


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)
    if not ADMIN_IDS:
        await _reply_text(update, context, "Admin list is empty. Set ADMIN_IDS in .env")
        return
    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    titles_count = db.count_titles()
    eps_count = db.count_episodes()
    keyboard = [
        [InlineKeyboardButton("Add manga", callback_data="admin:add_title")],
        [InlineKeyboardButton("Manage manga", callback_data="admin:manage")],
    ]
    await _reply_text(
        update,
        context,
        _format_report(
            "🛠️ 𝗔𝗱𝗺𝗶𝗻 𝗣𝗮𝗻𝗲𝗹",
            [f"📚 Manga: {titles_count}", f"🎬 Episodes: {eps_count}"],
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _can_manage_title(user_id: int, title_id: int, created_by: int | None = None) -> bool:
    if user_id in ADMIN_IDS:
        return True
    if created_by is None:
        title = db.get_title(title_id)
        if not title:
            return False
        created_by = title["created_by"]
    if created_by is None:
        return False
    if user_id == created_by:
        return True
    return db.has_manga_admin(title_id, user_id)


async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)
    if not _is_super_admin(update):
        await _reply_text(update, context, "Only main admins can add admins.")
        return
    if not context.args:
        await _reply_text(update, context, "Usage: /addadmin <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await _reply_text(update, context, "User ID must be a number.")
        return
    if user_id in ADMIN_IDS:
        await _reply_text(update, context, "That user is already a main admin.")
        return
    added = db.add_admin(user_id)
    if added:
        _log_admin_action(update.effective_user.id if update.effective_user else None, "add_admin", f"user_id={user_id}")
        await _reply_text(update, context, f"Admin added: {user_id}")
    else:
        await _reply_text(update, context, "Admin already exists.")


async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)
    if not _is_super_admin(update):
        await _reply_text(update, context, "Only main admins can remove admins.")
        return
    if not context.args:
        await _reply_text(update, context, "Usage: /removeadmin <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await _reply_text(update, context, "User ID must be a number.")
        return
    if user_id in ADMIN_IDS:
        await _reply_text(update, context, "You cannot remove a main admin from .env.")
        return
    removed = db.remove_admin(user_id)
    if removed:
        _log_admin_action(update.effective_user.id if update.effective_user else None, "remove_admin", f"user_id={user_id}")
        await _reply_text(update, context, f"Admin removed: {user_id}")
    else:
        await _reply_text(update, context, "Admin not found.")


def _parse_manga_admin_args(args: list[str]) -> tuple[str, str] | None:
    if not args:
        return None
    raw = " ".join(args).strip()
    if "|" in raw:
        title_name, user_arg = raw.rsplit("|", 1)
        title_name = title_name.strip()
        user_arg = user_arg.strip()
        if title_name and user_arg:
            return title_name, user_arg
        return None
    if len(args) < 2:
        return None
    title_name = " ".join(args[:-1]).strip()
    user_arg = args[-1].strip()
    if not title_name or not user_arg:
        return None
    return title_name, user_arg


async def _resolve_user_id(context: ContextTypes.DEFAULT_TYPE, user_arg: str) -> int | None:
    raw = user_arg.strip()
    try:
        return int(raw)
    except ValueError:
        pass
    if raw.startswith("@"):
        try:
            chat = await context.bot.get_chat(raw)
            return int(chat.id)
        except Exception:
            return None
    return None


async def add_manga_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)
    if not _is_super_admin(update):
        await _reply_text(update, context, "Only main admins can add manga admins.")
        return
    parsed = _parse_manga_admin_args(context.args)
    if not parsed:
        await _reply_text(
            update,
            context,
            "Usage: /addmangaadmin <manga title> | <user_id or @username>\n"
            "Example: /addmangaadmin One Piece | 123456789",
        )
        return
    title_name, user_arg = parsed
    title = db.get_title_by_name(title_name)
    if not title:
        await _reply_text(update, context, f"Manga not found: {title_name}")
        return
    user_id = await _resolve_user_id(context, user_arg)
    if user_id is None:
        await _reply_text(update, context, "Invalid user. Use numeric user ID or @username.")
        return
    if user_id in ADMIN_IDS:
        await _reply_text(update, context, "That user is a main admin and already has full access.")
        return
    if user_id not in set(db.get_admin_ids()):
        await _reply_text(update, context, "That user is not an added admin. Use /addadmin first.")
        return
    added = db.add_manga_admin(int(title["id"]), user_id)
    if added:
        _log_admin_action(
            update.effective_user.id if update.effective_user else None,
            "add_manga_admin",
            f"title_id={title['id']}, user_id={user_id}",
        )
        await _reply_text(update, context, f"Added manga admin {user_id} for '{title['name']}'.")
    else:
        await _reply_text(update, context, f"User {user_id} already manages '{title['name']}'.")


async def remove_manga_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)
    if not _is_super_admin(update):
        await _reply_text(update, context, "Only main admins can remove manga admins.")
        return
    parsed = _parse_manga_admin_args(context.args)
    if not parsed:
        await _reply_text(
            update,
            context,
            "Usage: /removemangaadmin <manga title> | <user_id or @username>\n"
            "Example: /removemangaadmin One Piece | 123456789",
        )
        return
    title_name, user_arg = parsed
    title = db.get_title_by_name(title_name)
    if not title:
        await _reply_text(update, context, f"Manga not found: {title_name}")
        return
    user_id = await _resolve_user_id(context, user_arg)
    if user_id is None:
        await _reply_text(update, context, "Invalid user. Use numeric user ID or @username.")
        return
    removed = db.remove_manga_admin(int(title["id"]), user_id)
    if removed:
        _log_admin_action(
            update.effective_user.id if update.effective_user else None,
            "remove_manga_admin",
            f"title_id={title['id']}, user_id={user_id}",
        )
        await _reply_text(update, context, f"Removed manga admin {user_id} from '{title['name']}'.")
    else:
        await _reply_text(update, context, f"User {user_id} was not assigned to '{title['name']}'.")


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context, force=True)
    if context.user_data.get("pending_action") != "bulk_add":
        msg = await _reply_text(update, context, "Nothing to finish.")
        _schedule_delete(msg, context, force=True)
        return
    buffer = context.user_data.get("bulk_buffer", "")
    if not buffer.strip():
        _reset_pending(context)
        context.user_data.pop("bulk_buffer", None)
        msg = await _reply_text(update, context, "No bulk data received.")
        _schedule_delete(msg, context, force=True)
        return
    # Reuse bulk add parsing by sending through handler
    await _process_bulk_add(update, context, buffer)


async def _process_bulk_add(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    title_id = context.user_data.get("pending_title_id")
    if not title_id:
        _reset_pending(context)
        context.user_data.pop("bulk_buffer", None)
        await _reply_text(update, context, "Missing state. Start again from /admin.")
        return
    raw = text.replace("\u200b", "").strip()
    # Merge lines that start with URL query fragments onto the previous line
    merged_lines = []
    for line in raw.splitlines():
        part = line.strip()
        if not part:
            continue
        if merged_lines and (part.startswith("?") or part.startswith("&") or part.startswith("story_fbid=") or part.startswith("fbid=")):
            merged_lines[-1] = merged_lines[-1] + part
        else:
            merged_lines.append(part)
    raw = "\n".join(merged_lines)
    url_re = re.compile(r"https?://\S+", re.IGNORECASE)
    matches = list(url_re.finditer(raw))
    if not matches:
        await _reply_text(update, context, "Please include at least one http/https link.")
        return
    added = 0
    skipped = 0
    prev_end = 0
    for m in matches:
        name = raw[prev_end:m.start()].strip()
        url = m.group(0).strip()
        prev_end = m.end()
        if not name:
            skipped += 1
            continue
        name = _normalize_ep_name(name)
        url = _normalize_url(url)
        if not _valid_url(url):
            skipped += 1
            continue
        if _is_placeholder_link(url):
            skipped += 1
            continue
        db.add_episode(int(title_id), name, url, update.effective_user.id)
        added += 1
    _reset_pending(context)
    context.user_data.pop("bulk_buffer", None)
    keyboard = [
        [InlineKeyboardButton("List episodes", callback_data=f"admin:eps:{title_id}:0")],
        [InlineKeyboardButton("Back", callback_data="admin:manage")],
    ]
    await _reply_text(
        update,
        context,
        f"Bulk add complete. Added {added}, skipped {skipped}.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    if added > 0:
        _log_admin_action(
            update.effective_user.id if update.effective_user else None,
            "bulk_add_episodes",
            f"title_id={title_id}, added={added}, skipped={skipped}",
        )

async def get_user_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)
    target = update.message.reply_to_message.from_user if update.message and update.message.reply_to_message else update.effective_user
    if not target:
        await _reply_text(update, context, "User not found.")
        return
    await _reply_text(update, context, f"User ID: {target.id}")


async def list_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)
    if not _is_super_admin(update):
        await _reply_text(update, context, "Only main admins can list admins.")
        return
    db_admins = db.get_admin_ids()
    main_admins = sorted(list(ADMIN_IDS))
    lines = [
        "📚 Main admins:",
    ]
    for uid in main_admins:
        try:
            chat = await context.bot.get_chat(uid)
            full_name = (chat.full_name or "").strip()
            username = (chat.username or "").strip()
        except Exception:
            full_name = ""
            username = ""
        if full_name and username:
            display = f"{full_name} (@{username})"
        elif full_name:
            display = full_name
        elif username:
            display = f"@{username}"
        else:
            display = "Unknown"
        lines.append(f"• {display} ({uid})")
    lines.append("")
    lines.append("📚 Assigned admins:")
    for uid in db_admins:
        try:
            chat = await context.bot.get_chat(uid)
            full_name = (chat.full_name or "").strip()
            username = (chat.username or "").strip()
        except Exception:
            full_name = ""
            username = ""
        if full_name and username:
            display = f"{full_name} (@{username})"
        elif full_name:
            display = full_name
        elif username:
            display = f"@{username}"
        else:
            display = "Unknown"
        lines.append(f"• {display} ({uid})")
    text = _format_report("📋 𝗔𝗱𝗺𝗶𝗻 𝗟𝗶𝘀𝘁", lines)
    await _reply_text(update, context, text)


def _to_khmer_digits(value: int, width: int = 2) -> str:
    khmer = str(value).zfill(width).translate(str.maketrans("0123456789", "០១២៣៤៥៦៧៨៩"))
    return khmer


async def list_ep_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)
    if not context.args:
        await _reply_text(update, context, "Usage: /listep 1-10")
        return
    raw = " ".join(context.args)
    if "-" in raw:
        parts = raw.split("-", 1)
        if len(parts) != 2:
            await _reply_text(update, context, "Usage: /listep 1-10")
            return
        start_s, end_s = parts[0].strip(), parts[1].strip()
    else:
        if len(context.args) < 2:
            await _reply_text(update, context, "Usage: /listep 1-10")
            return
        start_s, end_s = context.args[0].strip(), context.args[1].strip()
    try:
        start = int(start_s)
        end = int(end_s)
    except ValueError:
        await _reply_text(update, context, "Usage: /listep 1-10")
        return
    if start <= 0 or end <= 0 or end < start:
        await _reply_text(update, context, "Usage: /listep 1-10")
        return
    lines = [f"{EP_PREFIX}{_to_khmer_digits(i)}" for i in range(start, end + 1)]
    text = "\n\n".join(lines)
    await _send_long_text(update, context, text)


async def donate_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, False)
    _schedule_delete(update.message, context)
    if not update.message:
        return
    await _send_donate_qr(update.message, context)


async def find_duplicate_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    rows = db.get_duplicate_link_usages()
    if not rows:
        await _reply_text(update, context, "No duplicate links found.")
        return

    groups: dict[str, list] = {}
    counts: dict[str, int] = {}
    for row in rows:
        url = str(row["url"])
        groups.setdefault(url, []).append(row)
        counts[url] = int(row["duplicate_count"])

    lines = [f"🔗 Duplicate links found: {len(groups)}", ""]
    for idx, (url, usages) in enumerate(groups.items(), start=1):
        lines.append(f"{idx}. 🔗 {url}")
        lines.append(f"   Used: {counts[url]} time(s)")
        for usage in usages:
            ep_name = _display_ep_name(str(usage["episode_name"]))
            lines.append(f"   - {usage['title_name']} | {ep_name}")
        lines.append("")

    await _send_long_text(update, context, _format_report("🔎 𝗗𝘂𝗽𝗹𝗶𝗰𝗮𝘁𝗲 𝗟𝗶𝗻𝗸 𝗥𝗲𝗽𝗼𝗿𝘁", lines))


def _probe_url_once(url: str, method: str, timeout: int = DEADLINK_TIMEOUT) -> tuple[bool, str]:
    req = urllib.request.Request(url=url, method=method, headers={"User-Agent": "LinkBot/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", 200))
            if code >= 400:
                return False, f"HTTP {code}"
            return True, f"HTTP {code}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except (TimeoutError, socket.timeout):
        return False, "TimeoutError"
    except Exception as exc:
        return False, exc.__class__.__name__


def _probe_url(url: str) -> tuple[bool, str]:
    # Retry strategy: HEAD first (fast), then GET fallback for timeout/restricted hosts.
    for _ in range(2):
        ok, detail = _probe_url_once(url, "HEAD")
        if ok:
            return ok, detail
        if detail in {"HTTP 405", "HTTP 403", "TimeoutError"}:
            ok_get, detail_get = _probe_url_once(url, "GET")
            if ok_get:
                return ok_get, detail_get
            detail = detail_get
        if detail != "TimeoutError":
            return False, detail
    return False, "TimeoutError"


def _is_placeholder_link(url: str) -> bool:
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    target = f"{parsed.netloc}{parsed.path}".lower()
    return any(pattern in target for pattern in PLACEHOLDER_LINK_PATTERNS)


async def dead_links_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    limit = min(db.count_episodes(), DEADLINK_MAX_LIMIT)
    scope_text = "all"
    if context.args:
        if len(context.args) > 1:
            await _reply_text(update, context, "Usage: /deadlinks [n|all]")
            return
        arg = (context.args[0] or "").strip().lower()
        if arg == "all":
            limit = min(db.count_episodes(), DEADLINK_MAX_LIMIT)
            scope_text = "all"
        else:
            try:
                limit = int(arg)
            except ValueError:
                await _reply_text(update, context, "n must be a number or 'all'.")
                return
            if limit <= 0:
                await _reply_text(update, context, "n must be greater than 0.")
                return
    limit = min(limit, DEADLINK_MAX_LIMIT)

    rows = db.get_recent_episode_links(limit)
    if not rows:
        await _reply_text(update, context, "No episodes found.")
        return

    header = [
        f"🔎 Scanning: {len(rows)} {scope_text} link(s)",
        "⏳ Dead links will be sent as they are found.",
    ]
    await _reply_text(update, context, _format_report("🔍 𝗗𝗲𝗮𝗱 𝗟𝗶𝗻𝗸𝘀", header))

    semaphore = asyncio.Semaphore(DEADLINK_CONCURRENCY)
    counter = 0

    async def check_row(row) -> tuple[object, bool, str]:
        async with semaphore:
            raw_url = str(row["url"])
            if _is_placeholder_link(raw_url):
                return row, False, "Placeholder link"
            ok, detail = await asyncio.to_thread(_probe_url, raw_url)
            return row, ok, detail

    results = await asyncio.gather(*(check_row(row) for row in rows))

    grouped: dict[str, list[tuple[str, str, str]]] = {}
    for row, ok, detail in results:
        if ok:
            continue
        if detail in DEADLINK_SKIP_HTTP:
            continue
        counter += 1
        ep_name = _display_ep_name(str(row["episode_name"]))
        grouped.setdefault(str(row["title_name"]), []).append((ep_name, str(row["url"]), detail))

    if counter == 0:
        await _reply_text(update, context, _format_report("✅ 𝗗𝗲𝗮𝗱 𝗟𝗶𝗻𝗸𝘀", ["No dead links found."]))
        return

    for title_name, items in grouped.items():
        lines = [f"📚 Title: {title_name}", f"❌ Dead links: {len(items)}", ""]
        for idx, (ep_name, url, detail) in enumerate(items, start=1):
            lines.append(f"{idx}. {ep_name}")
            lines.append(f"   Reason: {detail}")
            lines.append(f"   URL: {url}")
        await _send_long_text(update, context, _format_report("❌ 𝗗𝗲𝗮𝗱 𝗟𝗶𝗻𝗸𝘀", lines))

    await _reply_text(update, context, _format_report("✅ 𝗗𝗲𝗮𝗱 𝗟𝗶𝗻𝗸𝘀", [f"Finished. Total dead links: {counter}."]))


async def check_title_links_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    raw = " ".join(context.args).strip()
    if not raw:
        await _reply_text(update, context, "Usage: /checktitlelinks <manga title>")
        return

    matches = db.search_titles_by_keyword(raw)
    if not matches:
        await _reply_text(update, context, f"Manga not found: {raw}")
        return
    picked = next((t for t in matches if str(t["name"]).casefold() == raw.casefold()), None)
    if not picked and len(matches) == 1:
        picked = matches[0]
    if not picked:
        names = "\n".join(f"- {t['name']}" for t in matches[:10])
        suffix = "\n..." if len(matches) > 10 else ""
        await _reply_text(update, context, f"Multiple manga matched '{raw}'. Use full title:\n{names}{suffix}")
        return

    if not _can_manage_title(update.effective_user.id, int(picked["id"]), picked["created_by"]):
        await _reply_text(update, context, "You cannot check links for this manga.")
        return

    episodes = db.get_episodes(int(picked["id"]))
    if not episodes:
        await _reply_text(update, context, f"{picked['name']} - No episodes yet.")
        return

    semaphore = asyncio.Semaphore(10)

    async def check_ep(ep) -> tuple[object, bool, str]:
        async with semaphore:
            raw_url = str(ep["url"])
            if _is_placeholder_link(raw_url):
                return ep, False, "Placeholder link"
            ok, detail = await asyncio.to_thread(_probe_url, raw_url)
            return ep, ok, detail

    results = await asyncio.gather(*(check_ep(ep) for ep in episodes))
    bad = [(ep, detail) for ep, ok, detail in results if not ok]

    header = [
        f"📚 Title: {picked['name']}",
        f"🔗 Checked: {len(episodes)} link(s)",
        f"❌ Broken/timeout: {len(bad)}",
        "",
    ]
    if not bad:
        await _reply_text(update, context, _format_report("🔗 𝗧𝗶𝘁𝗹𝗲 𝗟𝗶𝗻𝗸 𝗖𝗵𝗲𝗰𝗸", header + ["✅ No dead links found."]))
        return

    lines = header
    for idx, (ep, detail) in enumerate(bad, start=1):
        ep_name = _display_ep_name(str(ep["name"]))
        lines.append(f"{idx}. {ep_name}")
        lines.append(f"   Reason: {detail}")
        lines.append(f"   URL: {ep['url']}")
    await _send_long_text(update, context, "\n".join(lines))


async def audit_log_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    limit = AUDITLOG_DEFAULT_LIMIT
    if context.args:
        if len(context.args) > 1:
            await _reply_text(update, context, "Usage: /auditlog [n]")
            return
        try:
            limit = int(context.args[0])
        except ValueError:
            await _reply_text(update, context, "n must be a number.")
            return
        if limit <= 0:
            await _reply_text(update, context, "n must be greater than 0.")
            return
    limit = min(limit, AUDITLOG_MAX_LIMIT)

    logs = db.get_audit_logs(limit)
    if not logs:
        await _reply_text(update, context, "No audit logs yet.")
        return

    lines = [f"📄 Showing latest {len(logs)} item(s)", ""]
    for item in logs:
        lines.append(f"[{item['created_at']}] {item['action']} by {item['actor_id']}")
        lines.append(f"  {item['details']}")
    await _send_long_text(update, context, _format_report("🧾 𝗔𝘂𝗱𝗶𝘁 𝗟𝗼𝗴", lines))


async def daily_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    kh_now = datetime.utcnow() + timedelta(hours=CAMBODIA_UTC_OFFSET_HOURS)
    month = kh_now.strftime("%Y-%m")
    if context.args:
        if len(context.args) > 1:
            await _reply_text(update, context, "Usage: /daily [YYYY-MM]")
            return
        candidate = context.args[0].strip()
        if not re.match(r"^\d{4}-\d{2}$", candidate):
            await _reply_text(update, context, "Month format must be YYYY-MM.")
            return
        month = candidate

    rows = db.get_top_users_for_month(month, "mangalink", DAILY_TOP_LIMIT)
    if not rows:
        await _reply_text(update, context, f"No /mangalink usage data for {month}.")
        return

    lines = [f"📅 Month: {month}", "🔍 Command: /mangalink", ""]
    for idx, row in enumerate(rows, start=1):
        user_id = int(row["user_id"])
        usage_count = int(row["usage_count"])
        display_name = f"User {user_id}"
        try:
            chat = await context.bot.get_chat(user_id)
            full_name = (chat.full_name or "").strip()
            username = (chat.username or "").strip()
            if full_name and username:
                display_name = f"{full_name} (@{username})"
            elif full_name:
                display_name = full_name
            elif username:
                display_name = f"@{username}"
        except Exception:
            pass
        lines.append(f"{idx}. {display_name} - {usage_count} use(s)")
    await _send_long_text(update, context, _format_report("📊 𝗠𝗼𝗻𝘁𝗵𝗹𝘆 𝗧𝗼𝗽 𝗨𝘀𝗲𝗿𝘀", lines))


async def top_manga_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return

    limit = TOPMANGA_DEFAULT_LIMIT
    if context.args:
        if len(context.args) > 1:
            await _reply_text(update, context, "Usage: /topmanga [n]")
            return
        try:
            limit = int(context.args[0])
        except ValueError:
            await _reply_text(update, context, "n must be a number.")
            return
        if limit <= 0:
            await _reply_text(update, context, "n must be greater than 0.")
            return
    limit = min(limit, TOPMANGA_MAX_LIMIT)

    rows = db.get_top_manga(limit)
    if not rows:
        await _reply_text(update, context, "No manga view data yet.")
        return

    lines = [f"📚 Showing top {len(rows)} manga by opens", ""]
    for idx, row in enumerate(rows, start=1):
        lines.append(f"{idx}. {row['title_name']} - {row['view_count']} open(s)")
    await _send_long_text(update, context, _format_report("📈 𝗧𝗼𝗽 𝗠𝗮𝗻𝗴𝗮", lines))


async def backup_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    if not _is_admin(update):
        await _reply_text(update, context, "You are not an admin.")
        return
    if not update.message:
        return

    keep: int | None = None
    if context.args:
        if len(context.args) > 1:
            await _reply_text(update, context, "Usage: /backupdb [keep]")
            return
        try:
            keep = int(context.args[0])
        except ValueError:
            await _reply_text(update, context, "keep must be a number.")
            return
        if keep <= 0:
            await _reply_text(update, context, "keep must be greater than 0.")
            return

    source = Path(DB_PATH)
    if not source.is_absolute():
        source = Path.cwd() / source
    if not source.exists():
        await _reply_text(update, context, f"Database file not found: {source}")
        return

    backup_dir = Path.cwd() / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"linkbot_backup_{stamp}.db"

    try:
        with sqlite3.connect(str(source)) as src_conn:
            with sqlite3.connect(str(backup_file)) as dst_conn:
                src_conn.backup(dst_conn)
    except Exception:
        shutil.copy2(source, backup_file)

    deleted = 0
    if keep is not None:
        files = sorted(
            backup_dir.glob("linkbot_backup_*.db"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old in files[keep:]:
            try:
                old.unlink()
                deleted += 1
            except OSError:
                continue

    with open(backup_file, "rb") as f:
        msg = await update.message.reply_document(
            document=f,
            filename=backup_file.name,
            caption=f"DB backup created: {backup_file.name}",
        )
    _schedule_delete(msg, context)

    detail = f"file={backup_file.name}"
    if keep is not None:
        detail += f", keep={keep}, deleted_old={deleted}"
    _log_admin_action(
        update.effective_user.id if update.effective_user else None,
        "backup_db",
        detail,
    )

    if keep is not None:
        await _reply_text(update, context, f"Backup complete. Kept latest {keep}, deleted {deleted} old file(s).")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    exc = context.error
    if isinstance(exc, (TimedOut, NetworkError)):
        logger.warning("Telegram network timeout: %s", exc)
        return
    if isinstance(exc, Conflict):
        logger.warning("Telegram conflict: another bot instance is running with the same token.")
        return
    if isinstance(exc, BadRequest) and "Query is too old" in str(exc):
        logger.info("Ignored expired callback query.")
        return
    logger.exception("Unhandled exception while processing update", exc_info=exc)


async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    try:
        await query.answer()
    except BadRequest as exc:
        # Happens when button press is too old; safe to ignore.
        if "Query is too old" in str(exc) or "query id is invalid" in str(exc):
            return
        raise
    except (TimedOut, NetworkError):
        return
    if data.startswith("help:"):
        section = data.split(":", 1)[1]
        keyboard = [
            [
                InlineKeyboardButton("👤 User", callback_data="help:user"),
                InlineKeyboardButton("🛠️ Admin", callback_data="help:admin"),
                InlineKeyboardButton("🧰 Tools", callback_data="help:tools"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="help:back")],
        ]
        if section == "back":
            await _edit_text(
                query,
                context,
                _help_menu_text(),
                reply_markup=InlineKeyboardMarkup([keyboard[0], keyboard[1]]),
            )
            return
        if section == "user":
            lines = [
                "• /start - welcome message",
                "• /mangalink - browse manga",
                "• /listmanga - list all manga",
                "• /search <keyword> - search manga title",
                "• /mangaupdated [n] - manga/link updates by day range",
                "• /lastupdate <manga title> - show latest update of one manga",
                "• /donateadmin - donation QR",
            ]
            title = "👤 𝗨𝘀𝗲𝗿 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀"
        elif section == "admin":
            lines = [
                "• /mangaadmin - admin panel",
                "• /searchbyadmin <keyword> - search manageable manga",
                "• /findduplicatelink - find same links used in episodes",
                "• /checktitlelinks <manga title> - check links for one manga",
                "• /topmanga [n] - top manga by open count",
                "• /deadlinks [n|all] - check non-working links",
                "• /daily [YYYY-MM] - top users by /mangalink per month",
                "• /backupdb [keep] - export DB backup",
                "• /auditlog [n] - show recent admin activity logs",
                "• /addadmin <user_id> - add admin (main admins only)",
                "• /removeadmin <user_id> - remove admin (main admins only)",
                "• /addmangaadmin <title> | <user_id/@username>",
                "• /removemangaadmin <title> | <user_id/@username>",
                "• /listadmin - list admins (main admins only)",
                "• /cancel - cancel current admin input",
                "• /done - finish bulk add input",
            ]
            title = "🛠️ 𝗔𝗱𝗺𝗶𝗻 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀"
        else:
            lines = [
                "• /listep 1-10 - generate episode labels",
                "• /getuserid - get user ID",
            ]
            title = "🧰 𝗧𝗼𝗼𝗹𝘀"
        await _edit_text(
            query,
            context,
            _format_report(title, lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    if data.startswith("admin:"):
        _set_admin_auto_delete(context, True)
    elif data.startswith("user:"):
        _set_admin_auto_delete(context, False)

    # Any button click should clear typed-input pending state to avoid stale actions
    # after navigation (especially when pressing Back).
    _reset_pending(context)
    context.user_data.pop("bulk_buffer", None)

    if data.startswith("user:title:"):
        title_id = int(data.split(":", 2)[2])
        user = update.effective_user
        if user:
            try:
                db.add_manga_view(title_id, int(user.id))
            except Exception:
                logger.exception("Failed to save manga view: title_id=%s user_id=%s", title_id, user.id)
        title = db.get_title(title_id)
        if not title:
            await _edit_text(query, context, "Manga not found.")
            return
        episodes = db.get_episodes(title_id)
        if not episodes:
            await _edit_text(
                query,
                context,
                _format_report("📚 𝗟𝗶𝗻𝗸 𝗠𝗮𝗻𝗴𝗮", [f"📚 Title: {title['name']}", "❌ No episodes yet."]),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data="user:back")]]
                ),
            )
            return
        page_episodes, page, pages = _paginate(episodes, 0, EP_PAGE_SIZE)
        keyboard: list[list[InlineKeyboardButton]] = []
        row: list[InlineKeyboardButton] = []
        for ep in page_episodes:
            row.append(InlineKeyboardButton(_display_ep_name(ep["name"]), url=ep["url"]))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(BTN_PREV, callback_data=f"user:eps:{title_id}:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(BTN_NEXT, callback_data=f"user:eps:{title_id}:{page+1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("Back", callback_data="user:back")])
        await _edit_text(
            query,
            context,
            f"📚 𝗟𝗶𝗻𝗸 𝗠𝗮𝗻𝗴𝗮\n━━━━━━━━━━━━━━━━━━\n{title['name']}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("user:eps:"):
        parts = data.split(":")
        if len(parts) < 4:
            return
        title_id = int(parts[2])
        page = int(parts[3])
        title = db.get_title(title_id)
        if not title:
            await _edit_text(query, context, "Manga not found.")
            return
        episodes = db.get_episodes(title_id)
        if not episodes:
            await _edit_text(
                query,
                context,
                _format_report("📚 𝗟𝗶𝗻𝗸 𝗠𝗮𝗻𝗴𝗮", [f"📚 Title: {title['name']}", "❌ No episodes yet."]),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data="user:back")]]
                ),
            )
            return
        page_episodes, page, pages = _paginate(episodes, page, EP_PAGE_SIZE)
        keyboard: list[list[InlineKeyboardButton]] = []
        row: list[InlineKeyboardButton] = []
        for ep in page_episodes:
            row.append(InlineKeyboardButton(_display_ep_name(ep["name"]), url=ep["url"]))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(BTN_PREV, callback_data=f"user:eps:{title_id}:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(BTN_NEXT, callback_data=f"user:eps:{title_id}:{page+1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("Back", callback_data="user:back")])
        await _edit_text(
            query,
            context,
            f"📚 𝗟𝗶𝗻𝗸 𝗠𝗮𝗻𝗴𝗮\n━━━━━━━━━━━━━━━━━━\n{title['name']}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("user:titles:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        page = int(parts[2])
        titles = db.get_titles()
        if not titles:
            await _edit_text(query, context, "No manga yet.")
            return
        page_titles, page, pages = _paginate(titles, page, TITLE_PAGE_SIZE)
        keyboard = [
            [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
            for t in page_titles
        ]
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(BTN_TITLES_PREV, callback_data=f"user:titles:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(BTN_TITLES_NEXT, callback_data=f"user:titles:{page+1}"))
        if nav:
            keyboard.append(nav)
        await _edit_text(query, context, LABEL_TITLES, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "user:back":
        titles = db.get_titles()
        if not titles:
            await _edit_text(query, context, "No manga yet.")
            return
        page_titles, page, pages = _paginate(titles, 0, TITLE_PAGE_SIZE)
        keyboard = [
            [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
            for t in page_titles
        ]
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(BTN_TITLES_PREV, callback_data=f"user:titles:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(BTN_TITLES_NEXT, callback_data=f"user:titles:{page+1}"))
        if nav:
            keyboard.append(nav)
        await _edit_text(query, context, LABEL_TITLES, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("admin:"):
        if not _is_admin(update):
            await _edit_text(query, context, "You are not an admin.")
            return

        action = data.split(":", 1)[1]

        if action == "add_title":
            _reset_pending(context)
            context.user_data["pending_action"] = "add_title"
            await _edit_text(
                query,
                context,
                _format_report("✍️ 𝗔𝗱𝗱 𝗠𝗮𝗻𝗴𝗮", ["Send the manga name:"]),
            )
            return

        if action == "manage":
            titles = db.get_titles()
            if not titles:
                await _edit_text(query, context, "No manga yet.")
                return
            page_titles, page, pages = _paginate(titles, 0, TITLE_PAGE_SIZE)
            keyboard = [
                [InlineKeyboardButton(t["name"], callback_data=f"admin:title:{t['id']}")]
                for t in page_titles
            ]
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Prev", callback_data=f"admin:titles:{page-1}"))
            if page < pages - 1:
                nav.append(InlineKeyboardButton("Next", callback_data=f"admin:titles:{page+1}"))
            if nav:
                keyboard.append(nav)
            keyboard.append([InlineKeyboardButton("Back", callback_data="admin:back")])
            await _edit_text(
                query,
                context,
                "Select a manga:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("use_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return
            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(
                    query,
                    context,
                    "You cannot manage this manga.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                    ),
                )
                return
            keyboard = [
                [InlineKeyboardButton("Add episode", callback_data=f"admin:addep:{title_id}")],
                [InlineKeyboardButton("Bulk add episodes", callback_data=f"admin:bulk_add:{title_id}")],
                [InlineKeyboardButton("List episodes", callback_data=f"admin:eps:{title_id}:0")],
                [InlineKeyboardButton("Copy all episodes", callback_data=f"admin:copy_eps:{title_id}")],
                [InlineKeyboardButton("Edit manga", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete manga", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _edit_text(
                query,
                context,
                _format_report("🛠️ 𝗠𝗮𝗻𝗮𝗴𝗲 𝗠𝗮𝗻𝗴𝗮", [f"📚 Title: {title['name']}", "Choose an action:"]),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("titles:"):
            parts = action.split(":")
            if len(parts) < 2:
                return
            page = int(parts[1])
            titles = db.get_titles()
            if not titles:
                await _edit_text(query, context, "No manga yet.")
                return
            page_titles, page, pages = _paginate(titles, page, TITLE_PAGE_SIZE)
            keyboard = [
                [InlineKeyboardButton(t["name"], callback_data=f"admin:title:{t['id']}")]
                for t in page_titles
            ]
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Prev", callback_data=f"admin:titles:{page-1}"))
            if page < pages - 1:
                nav.append(InlineKeyboardButton("Next", callback_data=f"admin:titles:{page+1}"))
            if nav:
                keyboard.append(nav)
            keyboard.append([InlineKeyboardButton("Back", callback_data="admin:back")])
            await _edit_text(
                query,
                context,
                "Select a manga:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action == "back":
            titles_count = db.count_titles()
            eps_count = db.count_episodes()
            keyboard = [
                [InlineKeyboardButton("Add manga", callback_data="admin:add_title")],
                [InlineKeyboardButton("Manage manga", callback_data="admin:manage")],
            ]
            await _edit_text(
                query,
                context,
                _format_report(
                    "🛠️ 𝗔𝗱𝗺𝗶𝗻 𝗣𝗮𝗻𝗲𝗹",
                    [f"📚 Manga: {titles_count}", f"🎬 Episodes: {eps_count}"],
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(
                    query,
                    context,
                    "You cannot manage this manga.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                    ),
                )
                return
            keyboard = [
                [InlineKeyboardButton("Add episode", callback_data=f"admin:addep:{title_id}")],
                [InlineKeyboardButton("Bulk add episodes", callback_data=f"admin:bulk_add:{title_id}")],
                [InlineKeyboardButton("List episodes", callback_data=f"admin:eps:{title_id}:0")],
                [InlineKeyboardButton("Copy all episodes", callback_data=f"admin:copy_eps:{title_id}")],
                [InlineKeyboardButton("Edit manga", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete manga", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _edit_text(
                query,
                context,
                _format_report("🛠️ 𝗠𝗮𝗻𝗮𝗴𝗲 𝗠𝗮𝗻𝗴𝗮", [f"📚 Title: {title['name']}", "Choose an action:"]),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("addep:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot add episodes to this manga.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "add_ep_name"
            context.user_data["pending_title_id"] = title_id
            await _edit_text(
                query,
                context,
                _format_report("✍️ 𝗔𝗱𝗱 𝗘𝗽𝗶𝘀𝗼𝗱𝗲", [f"📚 Title: {title['name']}", "Send episode name:"]),
            )
            return

        if action.startswith("copy_eps:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot access episodes from this manga.")
                return
            episodes = db.get_episodes(title_id)
            if not episodes:
                await _edit_text(
                    query,
                    context,
                    f"{title['name']} - No episodes yet.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data=f"admin:title:{title_id}")]]
                    ),
                )
                return
            pairs: list[str] = []
            for ep in episodes:
                name = _display_ep_name(ep["name"]).strip().replace("\n", " ")
                url = ep["url"].strip().replace("\n", "")
                pairs.append(f"{name}\n{url}")
            # Keep visual "#Link..." text but prevent Telegram hashtag parsing.
            text_out = f"#\u200bLinkរឿង៖\n{title['name']}\n" + "\n".join(pairs)
            if len(text_out) <= 3500:
                await _reply_to_query(query, context, text_out)
            else:
                data = text_out.encode("utf-8")
                bio = io.BytesIO(data)
                bio.name = f"{title['name']}_episodes.txt"
                msg = await query.message.reply_document(bio, caption="All episodes")
                _schedule_delete(msg, context)
            return

        if action.startswith("bulk_add:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot add episodes to this manga.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "bulk_add"
            context.user_data["pending_title_id"] = title_id
            await _edit_text(
                query,
                context,
                f"{title['name']}\nPlease input the link:\nExample:\nភាគ១\nhttps://m.facebook.com/...\nភាគ២\nhttps://m.facebook.com/...",
            )
            return


        if action.startswith("eps:"):
            parts = action.split(":")
            if len(parts) < 3:
                return
            title_id = int(parts[1])
            page = int(parts[2])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot access episodes from this manga.")
                return
            episodes = db.get_episodes(title_id)
            if not episodes:
                await _edit_text(
                    query,
                    context,
                    f"{title['name']} - No episodes yet.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data=f"admin:title:{title_id}")]]
                    ),
                )
                return
            page_episodes, page, pages = _paginate(episodes, page, EP_PAGE_SIZE)
            keyboard = []
            row = []
            for ep in page_episodes:
                row.append(InlineKeyboardButton(_display_ep_name(ep["name"]), callback_data=f"admin:ep:{ep['id']}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Prev", callback_data=f"admin:eps:{title_id}:{page-1}"))
            if page < pages - 1:
                nav.append(InlineKeyboardButton("Next", callback_data=f"admin:eps:{title_id}:{page+1}"))
            if nav:
                keyboard.append(nav)
            keyboard.append([InlineKeyboardButton("Back", callback_data=f"admin:title:{title_id}")])
            await _edit_text(
                query,
                context,
                f"{title['name']} - Select an episode:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("ep:"):
            episode_id = int(action.split(":", 1)[1])
            ep = db.get_episode(episode_id)
            if not ep:
                await _edit_text(query, context, "Episode not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(ep["title_id"])):
                await _edit_text(query, context, "You cannot manage this episode.")
                return
            prev_id = db.get_prev_episode_id(ep["title_id"], episode_id)
            next_id = db.get_next_episode_id(ep["title_id"], episode_id)
            keyboard = [
                [InlineKeyboardButton("Edit name", callback_data=f"admin:edit_ep_name:{episode_id}")],
                [InlineKeyboardButton("Edit link", callback_data=f"admin:edit_ep_url:{episode_id}")],
                [InlineKeyboardButton("Delete episode", callback_data=f"admin:del_ep:{episode_id}")],
                [InlineKeyboardButton("Back", callback_data=f"admin:eps:{ep['title_id']}:0")],
            ]
            nav = []
            if prev_id:
                nav.append(InlineKeyboardButton("Prev", callback_data=f"admin:ep:{prev_id}"))
            if next_id:
                nav.append(InlineKeyboardButton("Next", callback_data=f"admin:ep:{next_id}"))
            if nav:
                keyboard.insert(0, nav)
            await _edit_text(
                query,
                context,
                _format_report("🛠️ 𝗠𝗮𝗻𝗮𝗴𝗲 𝗘𝗽𝗶𝘀𝗼𝗱𝗲", [f"🎬 Episode: {_display_ep_name(ep['name'])}", "Choose an action:"]),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("edit_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot edit this manga.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "edit_title"
            context.user_data["pending_title_id"] = title_id
            await _edit_text(
                query,
                context,
                f"{title['name']} - Send the new manga name:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Cancel", callback_data=f"admin:title:{title_id}")]]
                ),
            )
            return

        if action.startswith("edit_ep_name:"):
            episode_id = int(action.split(":", 1)[1])
            ep = db.get_episode(episode_id)
            if not ep:
                await _edit_text(query, context, "Episode not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(ep["title_id"])):
                await _edit_text(query, context, "You cannot edit this episode.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "edit_ep_name"
            context.user_data["pending_episode_id"] = episode_id
            await _edit_text(query, context, 
                f"{_display_ep_name(ep['name'])}\nSend the new episode name:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Cancel", callback_data=f"admin:ep:{episode_id}")]]
                ),
            )
            return

        if action.startswith("edit_ep_url:"):
            episode_id = int(action.split(":", 1)[1])
            ep = db.get_episode(episode_id)
            if not ep:
                await _edit_text(query, context, "Episode not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(ep["title_id"])):
                await _edit_text(query, context, "You cannot edit this episode.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "edit_ep_url"
            context.user_data["pending_episode_id"] = episode_id
            await _edit_text(
                query,
                context,
                _format_report(
                    "✍️ 𝗘𝗱𝗶𝘁 𝗘𝗽𝗶𝘀𝗼𝗱𝗲 𝗟𝗶𝗻𝗸",
                    [f"🎬 Episode: {_display_ep_name(ep['name'])}", "Send the new episode link (http/https):"],
                ),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Cancel", callback_data=f"admin:ep:{episode_id}")]]
                ),
            )
            return

        if action.startswith("del_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot delete this manga.")
                return
            keyboard = [
                [InlineKeyboardButton("Yes, delete", callback_data=f"admin:confirm_del_title:{title_id}")],
                [InlineKeyboardButton("Cancel", callback_data=f"admin:title:{title_id}")],
            ]
            await _edit_text(query, context, 
                f"Delete manga '{title['name']}' and all episodes?",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return


        if action.startswith("confirm_del_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Manga not found.")
                return
            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(query, context, "You cannot delete this manga.")
                return
            deleted = db.delete_title(title_id)
            if deleted:
                _log_admin_action(
                    update.effective_user.id if update.effective_user else None,
                    "delete_title",
                    f"title_id={title_id}, name={title['name']}",
                )
                await _edit_text(
                    query,
                    context,
                    "Manga deleted.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                    ),
                )
            else:
                await _edit_text(query, context, "Manga not found.")
            return

        if action.startswith("del_ep:"):
            episode_id = int(action.split(":", 1)[1])
            ep = db.get_episode(episode_id)
            if not ep:
                await _edit_text(query, context, "Episode not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(ep["title_id"])):
                await _edit_text(query, context, "You cannot delete this episode.")
                return
            keyboard = [
                [InlineKeyboardButton("Yes, delete", callback_data=f"admin:confirm_del_ep:{episode_id}")],
                [InlineKeyboardButton("Cancel", callback_data=f"admin:ep:{episode_id}")],
            ]
            await _edit_text(query, context, 
                f"Delete episode '{_display_ep_name(ep['name'])}'?",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("confirm_del_ep:"):
            episode_id = int(action.split(":", 1)[1])
            ep = db.get_episode(episode_id)
            if not ep:
                await _edit_text(query, context, "Episode not found.")
                return

            if not _can_manage_title(update.effective_user.id, int(ep["title_id"])):
                await _edit_text(query, context, "You cannot delete this episode.")
                return
            title_id = ep["title_id"]
            deleted = db.delete_episode(episode_id)
            if deleted:
                _log_admin_action(
                    update.effective_user.id if update.effective_user else None,
                    "delete_episode",
                    f"episode_id={episode_id}, title_id={title_id}",
                )
                await _edit_text(query, context, 
                    "Episode deleted.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back to episodes", callback_data=f"admin:eps:{title_id}:0")]]
                    ),
                )
            else:
                await _edit_text(query, context, "Episode not found.")
            return


async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return

    pending = context.user_data.get("pending_action")
    if not pending:
        return
    _set_admin_auto_delete(context, True)
    _schedule_delete(update.message, context)

    text = (update.message.text or "").strip()
    if not text:
        await _reply_text(update, context, "Please send text.")
        return

    if pending == "add_title":
        existing = db.get_title_by_name(text)
        if existing:
            keyboard = [
                [InlineKeyboardButton("Use existing", callback_data=f"admin:use_title:{existing['id']}")],
                [InlineKeyboardButton("Cancel", callback_data="admin:manage")],
            ]
            _reset_pending(context)
            await _reply_text(
                update,
                context,
                "Manga already exists. Use existing manga?",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        
            if not _can_manage_title(update.effective_user.id, int(title["id"]), title["created_by"]):
                await _edit_text(
                    query,
                    context,
                    "You cannot manage this manga.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                    ),
                )
                return
            keyboard = [
                [InlineKeyboardButton("Add episode", callback_data=f"admin:addep:{title_id}")],
                [InlineKeyboardButton("Bulk add episodes", callback_data=f"admin:bulk_add:{title_id}")],
                [InlineKeyboardButton("List episodes", callback_data=f"admin:eps:{title_id}:0")],
                [InlineKeyboardButton("Copy all episodes", callback_data=f"admin:copy_eps:{title_id}")],
                [InlineKeyboardButton("Edit manga", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete manga", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _edit_text(
                query,
                context,
                f"{title['name']} - Choose an action:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return
        title_id = db.add_title(text, update.effective_user.id)
        _reset_pending(context)
        if title_id is None:
            await _reply_text(update, context, "Manga already exists.")
        else:
            _log_admin_action(
                update.effective_user.id if update.effective_user else None,
                "add_title",
                f"title_id={title_id}, name={text}",
            )
            keyboard = [
                [InlineKeyboardButton("Add episode", callback_data=f"admin:addep:{title_id}")],
                [InlineKeyboardButton("Bulk add episodes", callback_data=f"admin:bulk_add:{title_id}")],
                [InlineKeyboardButton("Edit manga", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete manga", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _reply_text(
                update,
                context,
                _format_report("🛠️ 𝗠𝗮𝗻𝗮𝗴𝗲 𝗠𝗮𝗻𝗴𝗮", [f"📚 Title: {text}", "Choose an action:"]),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        return

    if pending == "add_ep_name":
        context.user_data["pending_ep_name"] = _normalize_ep_name(text)
        context.user_data["pending_action"] = "add_ep_url"
        await _reply_text(
            update,
            context,
            _format_report("✍️ 𝗔𝗱𝗱 𝗘𝗽𝗶𝘀𝗼𝗱𝗲 𝗟𝗶𝗻𝗸", ["Send episode link (http/https):"]),
        )
        return

    if pending == "add_ep_url":
        raw_text = (text or "").strip().lower()
        if raw_text in {"nolink", "no link", "no-link", "no_link"}:
            url = "https://www.facebook.com/nolink"
        else:
            url = _normalize_url(text)
        if not _valid_url(url):
            await _reply_text(update, context, "Invalid URL. Please send the link again (http/https):")
            return
        if _is_placeholder_link(url) and raw_text not in {"nolink", "no link", "no-link", "no_link"}:
            await _reply_text(update, context, "Invalid link: placeholder link is not allowed. Send a real link.")
            return
        title_id = context.user_data.get("pending_title_id")
        ep_name = context.user_data.get("pending_ep_name")
        if not title_id or not ep_name:
            _reset_pending(context)
            await _reply_text(update, context, "Missing state. Start again from /admin.")
            return
        db.add_episode(int(title_id), ep_name, url, update.effective_user.id)
        _log_admin_action(
            update.effective_user.id if update.effective_user else None,
            "add_episode",
            f"title_id={title_id}, episode_name={ep_name}",
        )
        context.user_data.pop("pending_ep_name", None)
        context.user_data["pending_action"] = "add_ep_name"
        await _reply_text(update, context, "Episode added. Send next episode name or /done.")
        return

    if pending == "edit_title":
        title_id = context.user_data.get("pending_title_id")
        if not title_id:
            _reset_pending(context)
            await _reply_text(update, context, "Missing state. Start again from /admin.")
            return
        updated = db.update_title(int(title_id), text)
        _reset_pending(context)
        if updated:
            _log_admin_action(
                update.effective_user.id if update.effective_user else None,
                "edit_title",
                f"title_id={title_id}, new_name={text}",
            )
            await _reply_text(
                update,
                context,
                "Manga updated.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                ),
            )
        else:
            await _reply_text(update, context, "Manga not found.")
        return

    if pending == "edit_ep_name":
        episode_id = context.user_data.get("pending_episode_id")
        if not episode_id:
            _reset_pending(context)
            await _reply_text(update, context, "Missing state. Start again from /admin.")
            return
        ep = db.get_episode(int(episode_id))
        if not ep:
            _reset_pending(context)
            await _reply_text(update, context, "Episode not found.")
            return
        updated = db.update_episode(int(episode_id), _normalize_ep_name(text), ep["url"])
        _reset_pending(context)
        if updated:
            _log_admin_action(
                update.effective_user.id if update.effective_user else None,
                "edit_episode_name",
                f"episode_id={episode_id}, new_name={_normalize_ep_name(text)}",
            )
            await _reply_text(
                update,
                context,
                "Episode name updated.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data=f"admin:ep:{episode_id}")]]
                ),
            )
        else:
            await _reply_text(update, context, "Episode not found.")
        return

    if pending == "edit_ep_url":
        raw_text = (text or "").strip().lower()
        if raw_text in {"nolink", "no link", "no-link", "no_link"}:
            url = "https://www.facebook.com/nolink"
        else:
            url = _normalize_url(text)
        if not _valid_url(url):
            await _reply_text(update, context, "Invalid URL. Please send the link again (http/https):")
            return
        if _is_placeholder_link(url) and raw_text not in {"nolink", "no link", "no-link", "no_link"}:
            await _reply_text(update, context, "Invalid link: placeholder link is not allowed. Send a real link.")
            return
        episode_id = context.user_data.get("pending_episode_id")
        if not episode_id:
            _reset_pending(context)
            await _reply_text(update, context, "Missing state. Start again from /admin.")
            return
        ep = db.get_episode(int(episode_id))
        if not ep:
            _reset_pending(context)
            await _reply_text(update, context, "Episode not found.")
            return
        updated = db.update_episode(int(episode_id), ep["name"], url)
        _reset_pending(context)
        if updated:
            _log_admin_action(
                update.effective_user.id if update.effective_user else None,
                "edit_episode_url",
                f"episode_id={episode_id}",
            )
            await _reply_text(
                update,
                context,
                "Episode link updated.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data=f"admin:ep:{episode_id}")]]
                ),
            )
        else:
            await _reply_text(update, context, "Episode not found.")
        return

    if pending == "bulk_add":
        buffer = context.user_data.get("bulk_buffer", "")
        buffer = (buffer + "\n" + text).strip()
        context.user_data["bulk_buffer"] = buffer
        await _reply_text(update, context, "Added to bulk input. Send more or /done to finish.")
        return


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing. Set it in your environment or .env")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .get_updates_connect_timeout(20)
        .get_updates_read_timeout(30)
        .get_updates_write_timeout(30)
        .get_updates_pool_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("start", _tracked_command("start", start)))
    app.add_handler(CommandHandler("help", _tracked_command("help", help_command)))
    app.add_handler(CommandHandler("cancel", _tracked_command("cancel", cancel)))
    app.add_handler(CommandHandler("mangalink", _tracked_command("mangalink", mangalink_command)))
    app.add_handler(CommandHandler("listmanga", _tracked_command("listmanga", list_manga_command)))
    app.add_handler(CommandHandler("search", _tracked_command("search", search_command)))
    app.add_handler(CommandHandler("mangaupdated", _tracked_command("mangaupdated", manga_updated_command)))
    app.add_handler(CommandHandler("lastupdate", _tracked_command("lastupdate", last_update_command)))
    app.add_handler(CommandHandler("searchbyadmin", _tracked_command("searchbyadmin", search_by_admin_command)))
    app.add_handler(CommandHandler("findduplicatelink", _tracked_command("findduplicatelink", find_duplicate_link_command)))
    app.add_handler(CommandHandler("checktitlelinks", _tracked_command("checktitlelinks", check_title_links_command)))
    app.add_handler(CommandHandler("deadlinks", _tracked_command("deadlinks", dead_links_command)))
    app.add_handler(CommandHandler("topmanga", _tracked_command("topmanga", top_manga_command)))
    app.add_handler(CommandHandler("daily", _tracked_command("daily", daily_command)))
    app.add_handler(CommandHandler("backupdb", _tracked_command("backupdb", backup_db_command)))
    app.add_handler(CommandHandler("auditlog", _tracked_command("auditlog", audit_log_command)))
    app.add_handler(CommandHandler("mangaadmin", _tracked_command("mangaadmin", admin_command)))
    app.add_handler(CommandHandler("addadmin", _tracked_command("addadmin", add_admin_command)))
    app.add_handler(CommandHandler("removeadmin", _tracked_command("removeadmin", remove_admin_command)))
    app.add_handler(CommandHandler("addmangaadmin", _tracked_command("addmangaadmin", add_manga_admin_command)))
    app.add_handler(CommandHandler("removemangaadmin", _tracked_command("removemangaadmin", remove_manga_admin_command)))
    app.add_handler(CommandHandler("getuserid", _tracked_command("getuserid", get_user_id_command)))
    app.add_handler(CommandHandler("listadmin", _tracked_command("listadmin", list_admin_command)))
    app.add_handler(CommandHandler("listep", _tracked_command("listep", list_ep_command)))
    app.add_handler(CommandHandler("donateadmin", _tracked_command("donateadmin", donate_admin_command)))
    app.add_handler(CommandHandler("done", _tracked_command("done", done_command)))
    app.add_handler(CallbackQueryHandler(handle_callbacks))
    app.add_handler(
        MessageHandler(
            filters.StatusUpdate.NEW_CHAT_MEMBERS | filters.StatusUpdate.LEFT_CHAT_MEMBER,
            auto_delete_join_leave_message,
        )
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text))
    app.add_error_handler(error_handler)

    while True:
        try:
            logger.info("Link bot is running")
            app.run_polling(
                bootstrap_retries=-1,
            )
            break
        except (TimedOut, NetworkError) as exc:
            logger.warning(
                "Network timeout while connecting to Telegram API (%s). Retrying in %s seconds...",
                exc.__class__.__name__,
                STARTUP_RETRY_SECONDS,
            )
            time.sleep(STARTUP_RETRY_SECONDS)


if __name__ == "__main__":
    main()
