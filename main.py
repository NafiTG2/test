from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    filters,
)
import os
import html
import hashlib
import secrets
import sqlite3
import threading
import json
from io import BytesIO
from datetime import datetime
import time

# ================= TIMEZONE (BST: UTC+6) =================
def get_bst_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Dhaka")).strftime("%Y-%m-%d %H:%M:%S")
    except ImportError:
        import pytz
        tz = pytz.timezone('Asia/Dhaka')
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

# ================= ENV =================
TOKEN    = os.environ.get("BOT_TOKEN")
GROUP_ID = int(os.environ.get("GROUP_ID"))

# ================= DATABASE =================
DB_PATH  = "blockveil.db"
_db_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with _db_lock, get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT    DEFAULT '',
                first_name TEXT    DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS tickets (
                ticket_id  TEXT    PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                username   TEXT    DEFAULT '',
                status     TEXT    DEFAULT 'Pending',
                created_at TEXT    NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id  TEXT    NOT NULL,
                sender     TEXT    NOT NULL,
                content    TEXT    NOT NULL,
                timestamp  TEXT    NOT NULL,
                FOREIGN KEY(ticket_id) REFERENCES tickets(ticket_id)
            );

            CREATE TABLE IF NOT EXISTS group_message_map (
                tg_message_id INTEGER PRIMARY KEY,
                ticket_id     TEXT    NOT NULL,
                FOREIGN KEY(ticket_id) REFERENCES tickets(ticket_id)
            );
        """)

# ---- users ----
def db_upsert_user(user_id: int, username: str, first_name: str = ""):
    with _db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO users(user_id, username, first_name)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username   = excluded.username,
                first_name = excluded.first_name
        """, (user_id, username or "", first_name or ""))

def db_get_all_users():
    with get_conn() as conn:
        return conn.execute("SELECT user_id, username FROM users").fetchall()

def db_get_user(user_id: int):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()

def db_find_user_by_username(username: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (username,)
        ).fetchone()
        return row["user_id"] if row else None

# ---- tickets ----
def db_create_ticket(ticket_id: str, user_id: int, username: str, created_at: str):
    with _db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO tickets(ticket_id, user_id, username, status, created_at)
            VALUES(?, ?, ?, 'Pending', ?)
        """, (ticket_id, user_id, username or "", created_at))

def db_get_ticket(ticket_id: str):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()

def db_ticket_exists(ticket_id: str) -> bool:
    with get_conn() as conn:
        return conn.execute(
            "SELECT 1 FROM tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone() is not None

def db_update_ticket_status(ticket_id: str, status: str):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE tickets SET status = ? WHERE ticket_id = ?", (status, ticket_id)
        )

def db_get_active_ticket(user_id: int):
    with get_conn() as conn:
        row = conn.execute("""
            SELECT ticket_id FROM tickets
            WHERE user_id = ? AND status != 'Closed'
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,)).fetchone()
        return row["ticket_id"] if row else None

def db_get_user_tickets(user_id: int):
    with get_conn() as conn:
        return conn.execute("""
            SELECT ticket_id, status, created_at FROM tickets
            WHERE user_id = ? ORDER BY created_at ASC
        """, (user_id,)).fetchall()

def db_list_tickets(mode: str):
    with get_conn() as conn:
        if mode == "open":
            return conn.execute("""
                SELECT t.ticket_id, u.username
                FROM tickets t LEFT JOIN users u ON t.user_id = u.user_id
                WHERE t.status != 'Closed'
            """).fetchall()
        return conn.execute("""
            SELECT t.ticket_id, u.username
            FROM tickets t LEFT JOIN users u ON t.user_id = u.user_id
            WHERE t.status = 'Closed'
        """).fetchall()

# ---- messages ----
def db_add_message(ticket_id: str, sender: str, content: str, timestamp: str):
    with _db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO messages(ticket_id, sender, content, timestamp)
            VALUES(?, ?, ?, ?)
        """, (ticket_id, sender, content, timestamp))

def db_get_messages(ticket_id: str):
    with get_conn() as conn:
        return conn.execute("""
            SELECT sender, content, timestamp FROM messages
            WHERE ticket_id = ? ORDER BY id ASC
        """, (ticket_id,)).fetchall()

# ---- group_message_map ----
def db_map_message(tg_message_id: int, ticket_id: str):
    with _db_lock, get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO group_message_map(tg_message_id, ticket_id)
            VALUES(?, ?)
        """, (tg_message_id, ticket_id))

def db_get_ticket_by_msg(tg_message_id: int):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT ticket_id FROM group_message_map WHERE tg_message_id = ?",
            (tg_message_id,)
        ).fetchone()
        return row["ticket_id"] if row else None

# ================= RATE LIMITING (in-memory — resets on restart, intentional) =================
_rate_limit: dict = {}

def check_rate_limit(user_id: int) -> bool:
    now = time.time()
    _rate_limit.setdefault(user_id, [])
    _rate_limit[user_id] = [t for t in _rate_limit[user_id] if now - t < 60]
    if len(_rate_limit[user_id]) >= 2:
        return False
    _rate_limit[user_id].append(now)
    return True

# ================= HELPERS =================
def generate_ticket_id(user_id: int) -> str:
    """
    SHA-256(user_id : 128-bit-salt : nanosecond-timestamp)
    Output: BV- + first 10 hex chars  e.g. BV-3f9a1c02b7
    Collision probability: ~1 in 1,099,511,627,776
    """
    while True:
        salt         = secrets.token_hex(16)
        timestamp_ns = str(time.time_ns())
        raw          = f"{user_id}:{salt}:{timestamp_ns}"
        digest       = hashlib.sha256(raw.encode()).hexdigest()
        tid          = "BV-" + digest[:10]
        if not db_ticket_exists(tid):
            return tid

def code(tid: str) -> str:
    return f"<code>{html.escape(tid)}</code>"

def ticket_header(ticket_id: str, status: str) -> str:
    return f"🎫 Ticket ID: {code(ticket_id)}\nStatus: {status}\n\n"

def user_info_block(user) -> str:
    return (
        "User Information\n"
        f"• User ID   : {user.id}\n"
        f"• Username  : @{html.escape(user.username or '')}\n"
        f"• Full Name : {html.escape(user.first_name or '')}\n\n"
    )

def register_user(user):
    db_upsert_user(user.id, user.username or "", user.first_name or "")

# ================= /start =================
async def start(update: Update, context):
    user = update.effective_user
    register_user(user)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎟️ Create Ticket", callback_data="create_ticket")],
        [InlineKeyboardButton("👤 My Profile",    callback_data="profile")]
    ])
    await update.message.reply_text(
        "Hey Sir/Mam 👋\n\n"
        "Welcome to BlockVeil Support.\n"
        "You can contact the BlockVeil team using this bot.\n\n"
        "🔐 Privacy Notice\n"
        "Your information is kept strictly confidential.\n\n"
        "Use the button below to create a support ticket.\n\n"
        "📧 support.blockveil@protonmail.com\n\n"
        "— BlockVeil Support Team",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

# ================= CREATE TICKET =================
async def create_ticket(update: Update, context):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    register_user(user)

    active = db_get_active_ticket(user.id)
    if active:
        await query.message.reply_text(
            f"🎫 You already have an active ticket:\n{code(active)}",
            parse_mode="HTML"
        )
        return

    ticket_id  = generate_ticket_id(user.id)
    created_at = get_bst_now()
    db_create_ticket(ticket_id, user.id, user.username or "", created_at)

    await query.message.reply_text(
        f"🎫 Ticket Created: {code(ticket_id)}\n"
        "Status: Pending\n\n"
        "Please write and submit your issue or suggestion here in a clear and concise manner.\n"
        "Our support team will review it as soon as possible.",
        parse_mode="HTML"
    )

# ================= USER MESSAGE =================
async def user_message(update: Update, context):
    user = update.message.from_user
    register_user(user)

    if not check_rate_limit(user.id):
        await update.message.reply_text(
            "⏱️ You can send at most 2 messages per minute. Please wait a moment.",
            parse_mode="HTML"
        )
        return

    ticket_id = db_get_active_ticket(user.id)
    if not ticket_id:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎟️ Create Ticket", callback_data="create_ticket")]
        ])
        await update.message.reply_text(
            "❗ Please create a ticket first.\n\n"
            "Click the button below to submit a new support ticket.",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return

    ticket = db_get_ticket(ticket_id)
    if ticket["status"] == "Pending":
        db_update_ticket_status(ticket_id, "Processing")
        status = "Processing"
    else:
        status = ticket["status"]

    header       = ticket_header(ticket_id, status) + user_info_block(user) + "Message:\n"
    caption_text = update.message.caption or ""
    safe_caption = html.escape(caption_text) if caption_text else ""
    timestamp    = get_bst_now()
    sender_name  = f"@{user.username}" if user.username else user.first_name or "User"
    sent         = None
    log_text     = ""

    if update.message.text:
        log_text = html.escape(update.message.text)
        sent = await context.bot.send_message(
            chat_id=GROUP_ID, text=header + log_text, parse_mode="HTML"
        )
    elif update.message.photo:
        log_text = "[Photo]"
        sent = await context.bot.send_photo(
            chat_id=GROUP_ID, photo=update.message.photo[-1].file_id,
            caption=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.voice:
        log_text = "[Voice Message]"
        sent = await context.bot.send_voice(
            chat_id=GROUP_ID, voice=update.message.voice.file_id,
            caption=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.video:
        log_text = "[Video]"
        sent = await context.bot.send_video(
            chat_id=GROUP_ID, video=update.message.video.file_id,
            caption=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.document:
        log_text = "[Document]"
        sent = await context.bot.send_document(
            chat_id=GROUP_ID, document=update.message.document.file_id,
            caption=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.audio:
        log_text = "[Audio]"
        sent = await context.bot.send_audio(
            chat_id=GROUP_ID, audio=update.message.audio.file_id,
            caption=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.sticker:
        log_text = "[Sticker]"
        sent = await context.bot.send_sticker(
            chat_id=GROUP_ID, sticker=update.message.sticker.file_id
        )
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.animation:
        log_text = "[Animation/GIF]"
        sent = await context.bot.send_animation(
            chat_id=GROUP_ID, animation=update.message.animation.file_id,
            caption=header + (safe_caption or log_text), parse_mode="HTML"
        )
    elif update.message.video_note:
        log_text = "[Video Note]"
        sent = await context.bot.send_video_note(
            chat_id=GROUP_ID, video_note=update.message.video_note.file_id
        )
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=header + (safe_caption or log_text), parse_mode="HTML"
        )
    else:
        log_text = "[Unsupported message type]"
        await update.message.reply_text(
            "❌ This message type is not supported. Please send text, photo, video, document, audio, sticker, etc.",
            parse_mode="HTML"
        )
        sent = await context.bot.send_message(
            chat_id=GROUP_ID, text=header + log_text, parse_mode="HTML"
        )

    if sent:
        db_map_message(sent.message_id, ticket_id)

    db_add_message(ticket_id, sender_name, log_text, timestamp)

# ================= GROUP REPLY =================
async def group_reply(update: Update, context):
    if not update.message.reply_to_message:
        return

    ticket_id = db_get_ticket_by_msg(update.message.reply_to_message.message_id)
    if not ticket_id:
        return

    ticket = db_get_ticket(ticket_id)
    if not ticket:
        return

    if ticket["status"] == "Closed":
        await update.message.reply_text(
            f"⚠️ Ticket {code(ticket_id)} is already closed. Cannot send reply.",
            parse_mode="HTML"
        )
        return

    user_id      = ticket["user_id"]
    prefix       = f"🎫 Ticket ID: {code(ticket_id)}\n\n"
    caption_text = update.message.caption or ""
    safe_caption = html.escape(caption_text) if caption_text else ""
    timestamp    = get_bst_now()
    log_text     = ""

    try:
        if update.message.text:
            log_text = html.escape(update.message.text)
            await context.bot.send_message(
                chat_id=user_id, text=prefix + log_text, parse_mode="HTML"
            )
        elif update.message.photo:
            log_text = "[Photo]"
            await context.bot.send_photo(
                chat_id=user_id, photo=update.message.photo[-1].file_id,
                caption=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.voice:
            log_text = "[Voice Message]"
            await context.bot.send_voice(
                chat_id=user_id, voice=update.message.voice.file_id,
                caption=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.video:
            log_text = "[Video]"
            await context.bot.send_video(
                chat_id=user_id, video=update.message.video.file_id,
                caption=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.document:
            log_text = "[Document]"
            await context.bot.send_document(
                chat_id=user_id, document=update.message.document.file_id,
                caption=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.audio:
            log_text = "[Audio]"
            await context.bot.send_audio(
                chat_id=user_id, audio=update.message.audio.file_id,
                caption=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.sticker:
            log_text = "[Sticker]"
            await context.bot.send_sticker(
                chat_id=user_id, sticker=update.message.sticker.file_id
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.animation:
            log_text = "[Animation/GIF]"
            await context.bot.send_animation(
                chat_id=user_id, animation=update.message.animation.file_id,
                caption=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        elif update.message.video_note:
            log_text = "[Video Note]"
            await context.bot.send_video_note(
                chat_id=user_id, video_note=update.message.video_note.file_id
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=prefix + (safe_caption or log_text), parse_mode="HTML"
            )
        else:
            log_text = "[Unsupported message type]"
            await context.bot.send_message(
                chat_id=user_id, text=prefix + "Unsupported message type.", parse_mode="HTML"
            )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Failed to send reply to user: {e}", parse_mode="HTML"
        )
        return

    db_add_message(ticket_id, "BlockVeil Support", log_text, timestamp)

# ================= /close =================
async def close_ticket(update: Update, context):
    if update.effective_chat.id != GROUP_ID:
        return

    ticket_id = None
    if context.args:
        ticket_id = context.args[0]
    elif update.message.reply_to_message:
        ticket_id = db_get_ticket_by_msg(update.message.reply_to_message.message_id)

    if not ticket_id or not db_ticket_exists(ticket_id):
        await update.message.reply_text(
            "❌ Ticket not found.\nUse /close BV-XXXXX or reply with /close",
            parse_mode="HTML"
        )
        return

    ticket = db_get_ticket(ticket_id)
    if ticket["status"] == "Closed":
        await update.message.reply_text("⚠️ Ticket already closed.", parse_mode="HTML")
        return

    db_update_ticket_status(ticket_id, "Closed")

    try:
        await context.bot.send_message(
            chat_id=ticket["user_id"],
            text=f"🎫 Ticket ID: {code(ticket_id)}\nStatus: Closed",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Ticket closed but failed to notify user: {e}", parse_mode="HTML"
        )
    else:
        await update.message.reply_text(f"✅ Ticket {code(ticket_id)} closed.", parse_mode="HTML")

# ================= /requestclose =================
async def request_close(update: Update, context):
    if update.effective_chat.type != "private":
        await update.message.reply_text(
            "❌ This command can only be used in private chat with the bot.",
            parse_mode="HTML"
        )
        return

    user = update.message.from_user
    register_user(user)

    if not context.args:
        await update.message.reply_text(
            "❌ Please provide a ticket ID.\nUsage: /requestclose BV-XXXXX",
            parse_mode="HTML"
        )
        return

    ticket_id = context.args[0]
    ticket    = db_get_ticket(ticket_id)

    if not ticket:
        await update.message.reply_text(f"❌ Ticket {code(ticket_id)} not found.", parse_mode="HTML")
        return
    if ticket["user_id"] != user.id:
        await update.message.reply_text("❌ This ticket does not belong to you.", parse_mode="HTML")
        return
    if ticket["status"] == "Closed":
        await update.message.reply_text(f"⚠️ Ticket {code(ticket_id)} is already closed.", parse_mode="HTML")
        return

    username = f"@{user.username}" if user.username else "N/A"
    await context.bot.send_message(
        chat_id=GROUP_ID,
        text=(
            f"🔔 <b>Ticket Close Request</b>\n\n"
            f"User {username} [ User ID : {user.id} ] has requested to close ticket ID {code(ticket_id)}\n\n"
            f"Please review and properly close the ticket."
        ),
        parse_mode="HTML"
    )
    await update.message.reply_text(
        f"✅ Your request to close ticket {code(ticket_id)} has been sent to the support team.\n"
        f"They will review and close it shortly.",
        parse_mode="HTML"
    )

# ================= /send (text only) =================
async def send_direct(update: Update, context):
    if update.effective_chat.id != GROUP_ID:
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n"
            "/send @all <message>\n"
            "/send BV-XXXXX <message>\n"
            "/send @username <message>\n"
            "/send user_id <message>",
            parse_mode="HTML"
        )
        return

    target  = context.args[0]
    message = html.escape(" ".join(context.args[1:]))

    if target == "@all":
        all_users = db_get_all_users()
        total     = len(all_users)
        sent_c = failed_c = 0
        await update.message.reply_text(f"📢 Broadcasting to {total} users...", parse_mode="HTML")
        for row in all_users:
            try:
                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=f"📢 Announcement from BlockVeil Support:\n\n{message}",
                    parse_mode="HTML"
                )
                sent_c += 1
            except Exception as e:
                failed_c += 1
                print(f"Broadcast failed for {row['user_id']}: {e}")
        await update.message.reply_text(
            f"📊 Broadcast Complete:\n✅ Sent: {sent_c}\n❌ Failed: {failed_c}\n👥 Total: {total}",
            parse_mode="HTML"
        )
        return

    user_id       = None
    ticket_id_log = None
    final_message = ""

    if target.startswith("BV-"):
        ticket = db_get_ticket(target)
        if not ticket:
            await update.message.reply_text("❌ Ticket not found.", parse_mode="HTML")
            return
        if ticket["status"] == "Closed":
            await update.message.reply_text("⚠️ Ticket is closed.", parse_mode="HTML")
            return
        user_id       = ticket["user_id"]
        ticket_id_log = target
        final_message = f"🎫 Ticket ID: {code(target)}\n\n{message}"
    elif target.startswith("@"):
        uname = target[1:]
        if not uname:
            await update.message.reply_text("❌ Username cannot be empty.", parse_mode="HTML")
            return
        user_id = db_find_user_by_username(uname)
        if not user_id:
            await update.message.reply_text("❌ User not found.", parse_mode="HTML")
            return
        final_message = f"📩 BlockVeil Support:\n\n{message}"
    else:
        try:
            user_id = int(target)
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID or target.", parse_mode="HTML")
            return
        final_message = f"📩 BlockVeil Support:\n\n{message}"

    if not user_id:
        await update.message.reply_text("❌ User not found.", parse_mode="HTML")
        return

    try:
        await context.bot.send_message(
            chat_id=user_id, text=final_message, parse_mode="HTML"
        )
        if ticket_id_log:
            db_add_message(ticket_id_log, "BlockVeil Support", message, get_bst_now())
        await update.message.reply_text("✅ Message sent successfully.", parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to send: {e}", parse_mode="HTML")

# ================= /open =================
async def open_ticket(update: Update, context):
    if update.effective_chat.id != GROUP_ID or not context.args:
        return

    ticket_id = context.args[0]
    ticket    = db_get_ticket(ticket_id)

    if not ticket:
        await update.message.reply_text("❌ Ticket not found.", parse_mode="HTML")
        return
    if ticket["status"] != "Closed":
        await update.message.reply_text("⚠️ Ticket already open.", parse_mode="HTML")
        return
    if db_get_active_ticket(ticket["user_id"]):
        await update.message.reply_text(
            "❌ This user already has an active ticket, so reopening this ticket at the moment is not possible.",
            parse_mode="HTML"
        )
        return

    db_update_ticket_status(ticket_id, "Processing")

    try:
        await context.bot.send_message(
            chat_id=ticket["user_id"],
            text=f"🎫 Your ticket {code(ticket_id)} has been reopened by support.",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Ticket reopened but failed to notify user: {e}", parse_mode="HTML"
        )
    else:
        await update.message.reply_text(f"✅ Ticket {code(ticket_id)} reopened.", parse_mode="HTML")

# ================= /status =================
async def status_ticket(update: Update, context):
    if not context.args:
        await update.message.reply_text(
            "Use /status BV-XXXXX to check your ticket status.", parse_mode="HTML"
        )
        return

    ticket_id = context.args[0]
    ticket    = db_get_ticket(ticket_id)

    if not ticket:
        await update.message.reply_text(f"❌ Ticket {code(ticket_id)} not found.", parse_mode="HTML")
        return

    if update.effective_chat.type == "private":
        user = update.effective_user
        register_user(user)
        if ticket["user_id"] != user.id:
            await update.message.reply_text(
                "❌ This ticket does not belong to you. Please use your correct Ticket ID.",
                parse_mode="HTML"
            )
            return

    text = (
        f"🎫 Ticket ID: {code(ticket_id)}\n"
        f"Status: {ticket['status']}\n"
        f"Created at: {ticket['created_at']} (BST)"
    )
    if update.effective_chat.id == GROUP_ID:
        user_row = db_get_user(ticket["user_id"])
        uname    = user_row["username"] if user_row else ticket["username"]
        text    += f"\nUser: @{uname}"

    await update.message.reply_text(text, parse_mode="HTML")

# ================= /profile =================
async def profile(update: Update, context):
    if update.callback_query:
        await update.callback_query.answer()
        user    = update.callback_query.from_user
        chat_id = update.callback_query.message.chat_id
    else:
        if update.effective_chat.type != "private":
            await update.message.reply_text(
                "❌ This command can only be used in private chat with the bot.",
                parse_mode="HTML"
            )
            return
        user    = update.effective_user
        chat_id = update.message.chat_id

    register_user(user)
    tickets  = db_get_user_tickets(user.id)
    response = (
        f"👤 <b>My Dashboard</b>\n\n"
        f"Name: {html.escape(user.first_name or '')}\n"
        f"Username: @{html.escape(user.username or 'N/A')}\n"
        f"UID: <code>{user.id}</code>\n\n"
        f"📊 Total Tickets Created: {len(tickets)}\n"
    )
    if tickets:
        response += "\n"
        for i, row in enumerate(tickets, 1):
            response += f"{i}. {code(row['ticket_id'])} — {row['status']}\n"
            response += f"   Created: {row['created_at']}\n\n"
    else:
        response += "\nNo tickets created yet.\n\n"

    response += "⚠️ Please do not share your sensitive information with this bot and never share your Ticket ID with anyone. Only provide it directly to our official support bot."
    await context.bot.send_message(chat_id=chat_id, text=response, parse_mode="HTML")

# ================= /list =================
async def list_tickets(update: Update, context):
    if update.effective_chat.id != GROUP_ID or not context.args:
        return

    mode = context.args[0].lower()
    if mode not in ["open", "close"]:
        await update.message.reply_text(
            "❌ Invalid mode. Use /list open or /list close", parse_mode="HTML"
        )
        return

    rows = db_list_tickets(mode)
    if not rows:
        await update.message.reply_text("No tickets found.", parse_mode="HTML")
        return

    text = "📂 Open Tickets\n\n" if mode == "open" else "📁 Closed Tickets\n\n"
    for i, row in enumerate(rows, 1):
        text += f"{i}. {code(row['ticket_id'])} – @{row['username'] or 'N/A'}\n"
    await update.message.reply_text(text, parse_mode="HTML")

# ================= /export =================
async def export_ticket(update: Update, context):
    if update.effective_chat.id != GROUP_ID or not context.args:
        return

    ticket_id = context.args[0]
    if not db_ticket_exists(ticket_id):
        await update.message.reply_text("❌ Ticket not found.", parse_mode="HTML")
        return

    ticket   = db_get_ticket(ticket_id)
    messages = db_get_messages(ticket_id)

    buf = BytesIO()
    buf.write(f"BlockVeil Support — Ticket Export\n".encode())
    buf.write(f"Ticket ID : {ticket_id}\n".encode())
    buf.write(f"Status    : {ticket['status']}\n".encode())
    buf.write(f"Created   : {ticket['created_at']} BST\n".encode())
    buf.write(f"{'─' * 40}\n\n".encode())
    for row in messages:
        line = f"[{row['timestamp']}] {row['sender']} : {html.unescape(row['content'])}\n"
        buf.write(line.encode())

    buf.seek(0)
    buf.name = f"{ticket_id}.txt"
    await context.bot.send_document(GROUP_ID, document=buf)

# ================= /history =================
async def ticket_history(update: Update, context):
    if update.effective_chat.id != GROUP_ID or not context.args:
        return

    target  = context.args[0]
    user_id = None

    if target.startswith("@"):
        user_id = db_find_user_by_username(target[1:])
    else:
        try:
            user_id = int(target)
        except ValueError:
            pass

    if not user_id:
        await update.message.reply_text("❌ User not found.", parse_mode="HTML")
        return

    tickets = db_get_user_tickets(user_id)
    if not tickets:
        await update.message.reply_text("❌ User has no tickets.", parse_mode="HTML")
        return

    text = f"📋 Ticket History for {target}\n\n"
    for i, row in enumerate(tickets, 1):
        text += f"{i}. {code(row['ticket_id'])} - {row['status']} (Created: {row['created_at']} BST)\n"
    await update.message.reply_text(text, parse_mode="HTML")

# ================= /user =================
async def user_list(update: Update, context):
    if update.effective_chat.id != GROUP_ID:
        return

    all_users = db_get_all_users()
    if not all_users:
        await update.message.reply_text("❌ No users found.", parse_mode="HTML")
        return

    buf = BytesIO()
    for i, row in enumerate(all_users, 1):
        buf.write(f"{i} - @{row['username']} - {row['user_id']}\n".encode())
    buf.seek(0)
    buf.name = "users_list.txt"
    await context.bot.send_document(GROUP_ID, document=buf)

# ================= /which =================
async def which_user(update: Update, context):
    if update.effective_chat.id != GROUP_ID or not context.args:
        return

    target  = context.args[0]
    user_id = None

    if target.startswith("@"):
        user_id = db_find_user_by_username(target[1:])
    elif target.startswith("BV-"):
        ticket = db_get_ticket(target)
        if ticket:
            user_id = ticket["user_id"]
    else:
        try:
            user_id = int(target)
        except ValueError:
            pass

    if not user_id:
        await update.message.reply_text("❌ User not found.", parse_mode="HTML")
        return

    user_row = db_get_user(user_id)
    uname    = user_row["username"] if user_row else "N/A"
    tickets  = db_get_user_tickets(user_id)

    response  = f"👤 <b>User Information</b>\n\n• User ID  : {user_id}\n• Username : @{html.escape(uname)}\n\n"
    if not tickets:
        response += "📊 No tickets created yet."
    else:
        response += f"📊 <b>Created total {len(tickets)} tickets.</b>\n\n"
        for i, row in enumerate(tickets, 1):
            response += f"{i}. {code(row['ticket_id'])} - {row['status']} (Created: {row['created_at']} BST)\n"

    await update.message.reply_text(response, parse_mode="HTML")

# ================= MEDIA SEND COMMANDS (reply-based) =================
async def send_media(update: Update, context, media_type: str):
    if update.effective_chat.id != GROUP_ID:
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            f"❌ Please reply to a {media_type} message with this command.", parse_mode="HTML"
        )
        return

    replied       = update.message.reply_to_message
    file_id       = None
    has_media     = False
    media_caption = replied.caption or ""

    if   media_type == "photo"     and replied.photo:     file_id = replied.photo[-1].file_id; has_media = True
    elif media_type == "document"  and replied.document:  file_id = replied.document.file_id;  has_media = True
    elif media_type == "audio"     and replied.audio:     file_id = replied.audio.file_id;     has_media = True
    elif media_type == "voice"     and replied.voice:     file_id = replied.voice.file_id;     has_media = True
    elif media_type == "video"     and replied.video:     file_id = replied.video.file_id;     has_media = True
    elif media_type == "animation" and replied.animation: file_id = replied.animation.file_id; has_media = True
    elif media_type == "sticker"   and replied.sticker:   file_id = replied.sticker.file_id;   has_media = True

    if not has_media:
        await update.message.reply_text(
            f"❌ The replied message does not contain a {media_type}.", parse_mode="HTML"
        )
        return

    if not context.args:
        await update.message.reply_text(
            f"Usage: Reply to a {media_type} with /send_{media_type} @username or BV-XXXXX or user_id",
            parse_mode="HTML"
        )
        return

    target         = context.args[0]
    custom_caption = html.escape(" ".join(context.args[1:])) if len(context.args) > 1 else ""
    user_id        = None
    ticket_id_log  = None
    prefix         = "📩 BlockVeil Support:\n"

    if target.startswith("BV-"):
        ticket = db_get_ticket(target)
        if not ticket:
            await update.message.reply_text("❌ Ticket not found.", parse_mode="HTML")
            return
        if ticket["status"] == "Closed":
            await update.message.reply_text("⚠️ Ticket is closed.", parse_mode="HTML")
            return
        user_id       = ticket["user_id"]
        ticket_id_log = target
        prefix        = f"🎫 Ticket ID: {code(target)}\n"
    elif target.startswith("@"):
        uname = target[1:]
        if not uname:
            await update.message.reply_text("❌ Username cannot be empty.", parse_mode="HTML")
            return
        user_id = db_find_user_by_username(uname)
        if not user_id:
            await update.message.reply_text("❌ User not found.", parse_mode="HTML")
            return
    else:
        try:
            user_id = int(target)
        except ValueError:
            await update.message.reply_text("❌ Invalid target.", parse_mode="HTML")
            return

    if not user_id:
        await update.message.reply_text("❌ User not found.", parse_mode="HTML")
        return

    cap      = custom_caption or media_caption
    log_text = cap if cap else f"[{media_type.capitalize()}]"

    try:
        if   media_type == "photo":     await context.bot.send_photo(    chat_id=user_id, photo=file_id,     caption=prefix + cap, parse_mode="HTML")
        elif media_type == "document":  await context.bot.send_document( chat_id=user_id, document=file_id,  caption=prefix + cap, parse_mode="HTML")
        elif media_type == "audio":     await context.bot.send_audio(    chat_id=user_id, audio=file_id,     caption=prefix + cap, parse_mode="HTML")
        elif media_type == "voice":     await context.bot.send_voice(    chat_id=user_id, voice=file_id,     caption=prefix + cap, parse_mode="HTML")
        elif media_type == "video":     await context.bot.send_video(    chat_id=user_id, video=file_id,     caption=prefix + cap, parse_mode="HTML")
        elif media_type == "animation": await context.bot.send_animation(chat_id=user_id, animation=file_id, caption=prefix + cap, parse_mode="HTML")
        elif media_type == "sticker":
            await context.bot.send_sticker(chat_id=user_id, sticker=file_id)
            if prefix + cap:
                await context.bot.send_message(chat_id=user_id, text=prefix + cap, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to send: {e}", parse_mode="HTML")
        return

    if ticket_id_log:
        db_add_message(ticket_id_log, "BlockVeil Support", log_text, get_bst_now())

    await update.message.reply_text("✅ Media sent successfully.", parse_mode="HTML")

async def send_photo(u, c):     await send_media(u, c, "photo")
async def send_document(u, c):  await send_media(u, c, "document")
async def send_audio(u, c):     await send_media(u, c, "audio")
async def send_voice(u, c):     await send_media(u, c, "voice")
async def send_video(u, c):     await send_media(u, c, "video")
async def send_animation(u, c): await send_media(u, c, "animation")
async def send_sticker(u, c):   await send_media(u, c, "sticker")

# ================= /exportall =================
async def export_all(update: Update, context):
    if update.effective_chat.id != GROUP_ID:
        return

    with get_conn() as conn:
        users   = [dict(r) for r in conn.execute("SELECT * FROM users").fetchall()]
        tickets = [dict(r) for r in conn.execute("SELECT * FROM tickets").fetchall()]
        msgs    = [dict(r) for r in conn.execute("SELECT * FROM messages").fetchall()]
        gmmap   = [dict(r) for r in conn.execute("SELECT * FROM group_message_map").fetchall()]

    payload = {
        "exported_at": get_bst_now(),
        "users":             users,
        "tickets":           tickets,
        "messages":          msgs,
        "group_message_map": gmmap,
    }

    buf      = BytesIO(json.dumps(payload, ensure_ascii=False, indent=2).encode())
    buf.name = f"blockveil_backup_{get_bst_now().replace(' ', '_').replace(':', '-')}.json"

    await context.bot.send_document(
        chat_id=GROUP_ID,
        document=buf,
        caption=(
            f"🗄 <b>Full Database Export</b>\n\n"
            f"👥 Users    : {len(users)}\n"
            f"🎫 Tickets  : {len(tickets)}\n"
            f"💬 Messages : {len(msgs)}\n"
            f"🗺 Msg Map  : {len(gmmap)}\n\n"
            f"📅 Exported : {get_bst_now()} BST\n\n"
            f"To restore, reply to this file with /importall"
        ),
        parse_mode="HTML"
    )

# ================= /importall =================
async def import_all(update: Update, context):
    if update.effective_chat.id != GROUP_ID:
        return

    # Must reply to a document
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text(
            "❌ Reply to the exported .json file with /importall",
            parse_mode="HTML"
        )
        return

    doc = update.message.reply_to_message.document
    if not doc.file_name.endswith(".json"):
        await update.message.reply_text(
            "❌ File must be a .json export from /exportall",
            parse_mode="HTML"
        )
        return

    # Download and parse
    try:
        file     = await context.bot.get_file(doc.file_id)
        buf      = BytesIO()
        await file.download_to_memory(buf)
        buf.seek(0)
        payload  = json.loads(buf.read().decode())
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to read file: {e}", parse_mode="HTML")
        return

    # Validate structure
    required_keys = {"users", "tickets", "messages", "group_message_map"}
    if not required_keys.issubset(payload.keys()):
        await update.message.reply_text(
            "❌ Invalid backup file. Missing required keys.", parse_mode="HTML"
        )
        return

    users_i = tickets_i = msgs_i = gmmap_i = 0
    users_s = tickets_s = msgs_s = gmmap_s = 0

    try:
        with _db_lock, get_conn() as conn:

            # ---- users ----
            for u in payload["users"]:
                try:
                    conn.execute("""
                        INSERT INTO users(user_id, username, first_name)
                        VALUES(:user_id, :username, :first_name)
                        ON CONFLICT(user_id) DO UPDATE SET
                            username   = excluded.username,
                            first_name = excluded.first_name
                    """, u)
                    users_i += 1
                except Exception:
                    users_s += 1

            # ---- tickets ----
            for t in payload["tickets"]:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO tickets
                            (ticket_id, user_id, username, status, created_at)
                        VALUES(:ticket_id, :user_id, :username, :status, :created_at)
                    """, t)
                    if conn.execute(
                        "SELECT changes()"
                    ).fetchone()[0] == 0:
                        tickets_s += 1
                    else:
                        tickets_i += 1
                except Exception:
                    tickets_s += 1

            # ---- messages (use original id to avoid duplicates) ----
            for m in payload["messages"]:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO messages
                            (id, ticket_id, sender, content, timestamp)
                        VALUES(:id, :ticket_id, :sender, :content, :timestamp)
                    """, m)
                    if conn.execute("SELECT changes()").fetchone()[0] == 0:
                        msgs_s += 1
                    else:
                        msgs_i += 1
                except Exception:
                    msgs_s += 1

            # ---- group_message_map ----
            for g in payload["group_message_map"]:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO group_message_map
                            (tg_message_id, ticket_id)
                        VALUES(:tg_message_id, :ticket_id)
                    """, g)
                    if conn.execute("SELECT changes()").fetchone()[0] == 0:
                        gmmap_s += 1
                    else:
                        gmmap_i += 1
                except Exception:
                    gmmap_s += 1

    except Exception as e:
        await update.message.reply_text(f"❌ Import failed: {e}", parse_mode="HTML")
        return

    exported_at = payload.get("exported_at", "Unknown")
    await update.message.reply_text(
        f"✅ <b>Import Complete</b>\n\n"
        f"📅 Backup date : {exported_at} BST\n\n"
        f"👥 Users    : {users_i} imported, {users_s} skipped\n"
        f"🎫 Tickets  : {tickets_i} imported, {tickets_s} skipped\n"
        f"💬 Messages : {msgs_i} imported, {msgs_s} skipped\n"
        f"🗺 Msg Map  : {gmmap_i} imported, {gmmap_s} skipped\n\n"
        f"⚠️ Skipped rows already existed in the database.",
        parse_mode="HTML"
    )

# ================= INIT =================
init_db()

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start",        start))
app.add_handler(CommandHandler("close",        close_ticket))
app.add_handler(CommandHandler("open",         open_ticket))
app.add_handler(CommandHandler("send",         send_direct))
app.add_handler(CommandHandler("status",       status_ticket))
app.add_handler(CommandHandler("profile",      profile))
app.add_handler(CommandHandler("list",         list_tickets))
app.add_handler(CommandHandler("export",       export_ticket))
app.add_handler(CommandHandler("history",      ticket_history))
app.add_handler(CommandHandler("user",         user_list))
app.add_handler(CommandHandler("which",        which_user))
app.add_handler(CommandHandler("requestclose", request_close))

app.add_handler(CommandHandler("send_photo",     send_photo))
app.add_handler(CommandHandler("send_document",  send_document))
app.add_handler(CommandHandler("send_audio",     send_audio))
app.add_handler(CommandHandler("send_voice",     send_voice))
app.add_handler(CommandHandler("send_video",     send_video))
app.add_handler(CommandHandler("send_animation", send_animation))
app.add_handler(CommandHandler("send_sticker",   send_sticker))

app.add_handler(CommandHandler("exportall",    export_all))
app.add_handler(CommandHandler("importall",    import_all))

app.add_handler(CallbackQueryHandler(create_ticket, pattern="create_ticket"))
app.add_handler(CallbackQueryHandler(profile,       pattern="profile"))

app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, user_message))
app.add_handler(MessageHandler(filters.ChatType.GROUPS  & ~filters.COMMAND, group_reply))

app.run_polling()
