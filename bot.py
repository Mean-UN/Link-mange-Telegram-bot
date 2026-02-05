import io
import logging
import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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

AUTO_DELETE_SECONDS = 120
EP_PAGE_SIZE = 30
TITLE_PAGE_SIZE = 20
EP_PREFIX = "\u1797\u17B6\u1782"
LABEL_TITLES = "\u1794\u1789\u17D2\u1785\u17B8\u179A\u17BF\u1784\u17D6"
LABEL_ALL_EPS = "\u1797\u17B6\u1782\u1791\u17B6\u17C6\u1784\u17A2\u179F\u17CB"
DONATE_IMAGE_PATH = "donate_qr.png"


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


def _schedule_delete(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not message:
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _schedule_delete(update.message, context)
    text = (
        "Welcome! This bot stores titles, episodes, and links.\n"
        "How to use:\n"
        "- Use /linkmanga to browse titles and open episode links.\n"
        "- Use /listep 1-10 to generate episode labels (optional).\n"
        "- Use /getuserid to get a user ID (reply to a message to get that user).\n"
        "- Use /donateadmin to show the donation QR code.\n"
        "Admins can use /admin to manage titles and episodes."
        "\nDeveloped by @Mean_Un"
    )
    await _reply_text(update, context, text)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _schedule_delete(update.message, context)
    await _reply_text(
        update,
        context,
        "User commands:\n"
        "/start - welcome & how to use\n"
        "/linkmanga - show titles\n"
        "/listep 1-10 - generate episode labels\n"
        "/getuserid - get user ID (reply to a message)\n"
        "/donateadmin - show donation QR code\n"
        "\n"
        "Admin commands:\n"
        "/admin - admin menu\n"
        "/addadmin <user_id> - add admin (main admins only)\n"
        "/removeadmin <user_id> - remove admin (main admins only)\n"
        "/listadmin - list admins (main admins only)\n"
        "/cancel - cancel current admin input\n"
        "/done - finish bulk add input\n"
        "\n"
        "Admin rules:\n"
        "- Main admins can manage all data.\n"
        "- Added admins can only manage titles/episodes they created.\n"
        "- Added admins cannot add/remove other admins.\n"
        "Developed by @Mean_Un"
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    context.user_data.pop("bulk_buffer", None)
    _schedule_delete(update.message, context)
    await _reply_text(update, context, "Cancelled.")


async def linkmanga_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _schedule_delete(update.message, context)
    titles = db.get_titles()
    if not titles:
        await _reply_text(update, context, "No titles yet.")
        return

    page_titles, page, pages = _paginate(titles, 0, TITLE_PAGE_SIZE)
    keyboard = [
        [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
        for t in page_titles
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("Prev", callback_data=f"user:titles:{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next", callback_data=f"user:titles:{page+1}"))
    if nav:
        keyboard.append(nav)
    await _reply_text(update, context, LABEL_TITLES, reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
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
        [InlineKeyboardButton("Add title", callback_data="admin:add_title")],
        [InlineKeyboardButton("Manage titles", callback_data="admin:manage")],
    ]
    await _reply_text(update, context, 
        f"Admin panel\nTitles: {titles_count} | Episodes: {eps_count}",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _can_manage(user_id: int, created_by: int | None) -> bool:
    if user_id in ADMIN_IDS:
        return True
    if created_by is None:
        return False
    return user_id == created_by


async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
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
        await _reply_text(update, context, f"Admin added: {user_id}")
    else:
        await _reply_text(update, context, "Admin already exists.")


async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
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
        await _reply_text(update, context, f"Admin removed: {user_id}")
    else:
        await _reply_text(update, context, "Admin not found.")


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _schedule_delete(update.message, context)
    if context.user_data.get("pending_action") != "bulk_add":
        await _reply_text(update, context, "Nothing to finish.")
        return
    buffer = context.user_data.get("bulk_buffer", "")
    if not buffer.strip():
        _reset_pending(context)
        context.user_data.pop("bulk_buffer", None)
        await _reply_text(update, context, "No bulk data received.")
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

async def get_user_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _schedule_delete(update.message, context)
    target = update.message.reply_to_message.from_user if update.message and update.message.reply_to_message else update.effective_user
    if not target:
        await _reply_text(update, context, "User not found.")
        return
    await _reply_text(update, context, f"User ID: {target.id}")


async def list_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
    _schedule_delete(update.message, context)
    if not _is_super_admin(update):
        await _reply_text(update, context, "Only main admins can list admins.")
        return
    db_admins = db.get_admin_ids()
    main_admins = sorted(list(ADMIN_IDS))
    lines = ["Main admins:"]
    for uid in main_admins:
        try:
            chat = await context.bot.get_chat(uid)
            name = (chat.full_name or "").strip() or str(uid)
        except Exception:
            name = str(uid)
        lines.append(f"{name} - {uid}")
    lines.append("")
    lines.append("Added admins:")
    for uid in db_admins:
        try:
            chat = await context.bot.get_chat(uid)
            name = (chat.full_name or "").strip() or str(uid)
        except Exception:
            name = str(uid)
        lines.append(f"{name} - {uid}")
    text = "\n".join(lines).strip()
    await _reply_text(update, context, text)


def _to_khmer_digits(value: int, width: int = 2) -> str:
    khmer = str(value).zfill(width).translate(str.maketrans("0123456789", "០១២៣៤៥៦៧៨៩"))
    return khmer


async def list_ep_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_pending(context)
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
    _schedule_delete(update.message, context)
    if not update.message:
        return
    try:
        with open(DONATE_IMAGE_PATH, "rb") as f:
            msg = await update.message.reply_photo(
                photo=f,
                caption="Donate via QR code\nDeveloped by @Mean_Un",
            )
        _schedule_delete(msg, context)
    except FileNotFoundError:
        await _reply_text(update, context, f"Donation QR image not found: {DONATE_IMAGE_PATH}")


async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    await query.answer()

    if data.startswith("user:title:"):
        title_id = int(data.split(":", 2)[2])
        title = db.get_title(title_id)
        if not title:
            await _edit_text(query, context, "Title not found.")
            return
        episodes = db.get_episodes(title_id)
        if not episodes:
            await _edit_text(
                query,
                context,
                f"{title['name']} - No episodes yet.",
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
            nav.append(InlineKeyboardButton("Prev", callback_data=f"user:eps:{title_id}:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("Next", callback_data=f"user:eps:{title_id}:{page+1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("Back", callback_data="user:back")])
        await _edit_text(
            query,
            context,
            f"{title['name']} {LABEL_ALL_EPS}",
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
            await _edit_text(query, context, "Title not found.")
            return
        episodes = db.get_episodes(title_id)
        if not episodes:
            await _edit_text(
                query,
                context,
                f"{title['name']} - No episodes yet.",
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
            nav.append(InlineKeyboardButton("Prev", callback_data=f"user:eps:{title_id}:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("Next", callback_data=f"user:eps:{title_id}:{page+1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("Back", callback_data="user:back")])
        await _edit_text(
            query,
            context,
            f"{title['name']} {LABEL_ALL_EPS}",
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
            await _edit_text(query, context, "No titles yet.")
            return
        page_titles, page, pages = _paginate(titles, page, TITLE_PAGE_SIZE)
        keyboard = [
            [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
            for t in page_titles
        ]
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("Prev", callback_data=f"user:titles:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("Next", callback_data=f"user:titles:{page+1}"))
        if nav:
            keyboard.append(nav)
        await _edit_text(query, context, LABEL_TITLES, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "user:back":
        titles = db.get_titles()
        if not titles:
            await _edit_text(query, context, "No titles yet.")
            return
        page_titles, page, pages = _paginate(titles, 0, TITLE_PAGE_SIZE)
        keyboard = [
            [InlineKeyboardButton(t["name"], callback_data=f"user:title:{t['id']}")]
            for t in page_titles
        ]
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("Prev", callback_data=f"user:titles:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("Next", callback_data=f"user:titles:{page+1}"))
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
            await _edit_text(query, context, "Send the title name:")
            return

        if action == "manage":
            titles = db.get_titles()
            if not titles:
                await _edit_text(query, context, "No titles yet.")
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
                "Select a title:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("use_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return
            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(
                    query,
                    context,
                    "You cannot manage this title.",
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
                [InlineKeyboardButton("Edit title", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete title", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _edit_text(
                query,
                context,
                f"{title['name']} - Choose an action:",
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
                await _edit_text(query, context, "No titles yet.")
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
                "Select a title:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action == "back":
            titles_count = db.count_titles()
            eps_count = db.count_episodes()
            keyboard = [
                [InlineKeyboardButton("Add title", callback_data="admin:add_title")],
                [InlineKeyboardButton("Manage titles", callback_data="admin:manage")],
            ]
            await _edit_text(query, context, 
                f"Admin panel\nTitles: {titles_count} | Episodes: {eps_count}",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(
                    query,
                    context,
                    "You cannot manage this title.",
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
                [InlineKeyboardButton("Edit title", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete title", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _edit_text(
                query,
                context,
                f"{title['name']} - Choose an action:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("addep:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot add episodes to this title.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "add_ep_name"
            context.user_data["pending_title_id"] = title_id
            await _edit_text(
                query,
                context,
                f"{title['name']} - Send episode name:",
            )
            return

        if action.startswith("copy_eps:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot access episodes from this title.")
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
            text_out = "\n".join(pairs)
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
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot add episodes to this title.")
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
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot access episodes from this title.")
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

            if not _can_manage(update.effective_user.id, ep["created_by"]):
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
            await _edit_text(query, context, 
                f"{_display_ep_name(ep['name'])}\nChoose an action:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if action.startswith("edit_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot edit this title.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "edit_title"
            context.user_data["pending_title_id"] = title_id
            await _edit_text(
                query,
                context,
                f"{title['name']} - Send the new title name:",
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

            if not _can_manage(update.effective_user.id, ep["created_by"]):
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

            if not _can_manage(update.effective_user.id, ep["created_by"]):
                await _edit_text(query, context, "You cannot edit this episode.")
                return
            _reset_pending(context)
            context.user_data["pending_action"] = "edit_ep_url"
            context.user_data["pending_episode_id"] = episode_id
            await _edit_text(query, context, 
                f"{_display_ep_name(ep['name'])}\nSend the new episode link (http/https):",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Cancel", callback_data=f"admin:ep:{episode_id}")]]
                ),
            )
            return

        if action.startswith("del_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return

            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot delete this title.")
                return
            keyboard = [
                [InlineKeyboardButton("Yes, delete", callback_data=f"admin:confirm_del_title:{title_id}")],
                [InlineKeyboardButton("Cancel", callback_data=f"admin:title:{title_id}")],
            ]
            await _edit_text(query, context, 
                f"Delete title '{title['name']}' and all episodes?",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return


        if action.startswith("confirm_del_title:"):
            title_id = int(action.split(":", 1)[1])
            title = db.get_title(title_id)
            if not title:
                await _edit_text(query, context, "Title not found.")
                return
            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(query, context, "You cannot delete this title.")
                return
            deleted = db.delete_title(title_id)
            if deleted:
                await _edit_text(
                    query,
                    context,
                    "Title deleted.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                    ),
                )
            else:
                await _edit_text(query, context, "Title not found.")
            return

        if action.startswith("del_ep:"):
            episode_id = int(action.split(":", 1)[1])
            ep = db.get_episode(episode_id)
            if not ep:
                await _edit_text(query, context, "Episode not found.")
                return

            if not _can_manage(update.effective_user.id, ep["created_by"]):
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

            if not _can_manage(update.effective_user.id, ep["created_by"]):
                await _edit_text(query, context, "You cannot delete this episode.")
                return
            title_id = ep["title_id"]
            deleted = db.delete_episode(episode_id)
            if deleted:
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

    _schedule_delete(update.message, context)

    pending = context.user_data.get("pending_action")
    if not pending:
        return

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
                "Title already exists. Use existing title?",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        
            if not _can_manage(update.effective_user.id, title["created_by"]):
                await _edit_text(
                    query,
                    context,
                    "You cannot manage this title.",
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
                [InlineKeyboardButton("Edit title", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete title", callback_data=f"admin:del_title:{title_id}")],
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
            await _reply_text(update, context, "Title already exists.")
        else:
            keyboard = [
                [InlineKeyboardButton("Add episode", callback_data=f"admin:addep:{title_id}")],
                [InlineKeyboardButton("Bulk add episodes", callback_data=f"admin:bulk_add:{title_id}")],
                [InlineKeyboardButton("Edit title", callback_data=f"admin:edit_title:{title_id}")],
                [InlineKeyboardButton("Delete title", callback_data=f"admin:del_title:{title_id}")],
                [InlineKeyboardButton("Back", callback_data="admin:manage")],
            ]
            await _reply_text(
                update,
                context,
                f"{text} - Choose an action:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        return

    if pending == "add_ep_name":
        context.user_data["pending_ep_name"] = _normalize_ep_name(text)
        context.user_data["pending_action"] = "add_ep_url"
        await _reply_text(update, context, "Send episode link (http/https):")
        return

    if pending == "add_ep_url":
        url = _normalize_url(text)
        if not _valid_url(url):
            await _reply_text(update, context, "Invalid URL. Please send the link again (http/https):")
            return
        title_id = context.user_data.get("pending_title_id")
        ep_name = context.user_data.get("pending_ep_name")
        if not title_id or not ep_name:
            _reset_pending(context)
            await _reply_text(update, context, "Missing state. Start again from /admin.")
            return
        db.add_episode(int(title_id), ep_name, url, update.effective_user.id)
        context.user_data.pop("pending_ep_name", None)
        context.user_data["pending_action"] = "add_ep_name"
        await _reply_text(update, context, "Episode added. Send next episode name or /cancel.")
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
            await _reply_text(
                update,
                context,
                "Title updated.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data="admin:manage")]]
                ),
            )
        else:
            await _reply_text(update, context, "Title not found.")
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
        url = _normalize_url(text)
        if not _valid_url(url):
            await _reply_text(update, context, "Invalid URL. Please send the link again (http/https):")
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

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("linkmanga", linkmanga_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("addadmin", add_admin_command))
    app.add_handler(CommandHandler("removeadmin", remove_admin_command))
    app.add_handler(CommandHandler("getuserid", get_user_id_command))
    app.add_handler(CommandHandler("listadmin", list_admin_command))
    app.add_handler(CommandHandler("listep", list_ep_command))
    app.add_handler(CommandHandler("donateadmin", donate_admin_command))
    app.add_handler(CommandHandler("done", done_command))
    app.add_handler(CallbackQueryHandler(handle_callbacks))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text))

    logger.info("Link bot is running")
    app.run_polling()


if __name__ == "__main__":
    main()
