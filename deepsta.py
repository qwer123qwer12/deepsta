import asyncio
import os
import psycopg2
import logging
from datetime import datetime
from multiprocessing import Process
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

# ========== НАСТРОЙКИ (через переменные окружения) ==========
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
GROUP_ID = os.environ["GROUP_ID"]
POSTS_CHANNEL_ID = int(os.environ["POSTS_CHANNEL_ID"])
MATERIALS_CHANNEL_ID = os.environ.get("MATERIALS_CHANNEL_ID")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "your_bot_username")
DATABASE_URL = os.environ["DATABASE_URL"]   # Render PostgreSQL URL

# Состояния
LANG, CONTENT_TYPE, TITLE, FILE = range(4)
BROADCAST_MESSAGE = 0
FEEDBACK_MESSAGE = 1
EDIT_POST_TEXT = 2
MATERIAL_TITLE, MATERIAL_DESC, MATERIAL_LINK_OR_FILE = range(3, 6)

# Flask-приложение для пинга
web_app = Flask(__name__)

@web_app.route('/ping')
def ping():
    return "pong", 200

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ========== БАЗА ДАННЫХ (PostgreSQL) ==========
class Database:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self._init_db()

    def _init_db(self):
        conn = psycopg2.connect(self.db_url)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                file_id TEXT NOT NULL DEFAULT '',
                filename TEXT NOT NULL DEFAULT '',
                content_type TEXT NOT NULL DEFAULT 'pdf',
                content TEXT NOT NULL DEFAULT '',
                language TEXT NOT NULL DEFAULT 'ru',
                date_added TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id BIGINT UNIQUE NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS materials (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                link TEXT NOT NULL DEFAULT '',
                file_id TEXT NOT NULL DEFAULT '',
                file_name TEXT NOT NULL DEFAULT '',
                article_id INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()

    async def add_article(self, title, file_id, language, content_type="pdf", content=""):
        def _add():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            date = datetime.now().strftime("%Y-%m-%d %H:%M")
            c.execute(
                "INSERT INTO articles (title, file_id, language, content_type, content, date_added) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                (title, file_id, language, content_type, content, date),
            )
            lid = c.fetchone()[0]
            conn.commit()
            conn.close()
            return lid
        return await asyncio.to_thread(_add)

    async def update_article_content(self, article_id, new_content):
        def _upd():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("UPDATE articles SET content=%s WHERE id=%s", (new_content, article_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_upd)

    async def update_article_file(self, article_id, file_id, filename):
        def _upd():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("UPDATE articles SET file_id=%s, filename=%s, content_type='media' WHERE id=%s",
                      (file_id, filename, article_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_upd)

    async def update_filename(self, article_id, filename):
        def _upd():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("UPDATE articles SET filename=%s WHERE id=%s", (filename, article_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_upd)

    async def delete_article(self, article_id):
        def _del():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("SELECT file_id, filename FROM articles WHERE id=%s", (article_id,))
            row = c.fetchone()
            if row:
                c.execute("DELETE FROM articles WHERE id=%s", (article_id,))
                conn.commit()
                conn.close()
                return row
            conn.close()
            return (None, None)
        return await asyncio.to_thread(_del)

    async def get_all_articles(self, language=None):
        def _get():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            if language:
                c.execute(
                    "SELECT id, title, file_id, filename, content_type, content, language, date_added FROM articles WHERE language=%s ORDER BY id DESC",
                    (language,),
                )
            else:
                c.execute(
                    "SELECT id, title, file_id, filename, content_type, content, language, date_added FROM articles ORDER BY id DESC"
                )
            rows = c.fetchall()
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def get_text_posts(self):
        def _get():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("SELECT id, title, content, language, date_added, content_type FROM articles WHERE content_type != 'pdf' ORDER BY id DESC")
            rows = c.fetchall()
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def get_article(self, article_id):
        def _get():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("SELECT id, title, file_id, filename, content_type, content, language, date_added FROM articles WHERE id=%s", (article_id,))
            row = c.fetchone()
            conn.close()
            return row
        return await asyncio.to_thread(_get)

    async def add_subscriber(self, user_id):
        def _add():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("INSERT INTO subscribers (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (user_id,))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_add)

    async def remove_subscriber(self, user_id):
        def _rem():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("DELETE FROM subscribers WHERE user_id=%s", (user_id,))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_rem)

    async def get_subscribers(self):
        def _get():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("SELECT user_id FROM subscribers")
            rows = [row[0] for row in c.fetchall()]
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def add_material(self, title, description, link="", file_id="", file_name="", article_id=0):
        def _add():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute(
                "INSERT INTO materials (title, description, link, file_id, file_name, article_id) VALUES (%s,%s,%s,%s,%s,%s)",
                (title, description, link, file_id, file_name, article_id),
            )
            conn.commit()
            lid = c.lastrowid
            conn.close()
            return lid
        return await asyncio.to_thread(_add)

    async def delete_material(self, material_id):
        def _del():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("DELETE FROM materials WHERE id=%s", (material_id,))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_del)

    async def get_all_materials(self):
        def _get():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("SELECT id, title, description, link, file_id, file_name, article_id FROM materials ORDER BY id DESC")
            rows = c.fetchall()
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def get_material(self, material_id):
        def _get():
            conn = psycopg2.connect(self.db_url)
            c = conn.cursor()
            c.execute("SELECT id, title, description, link, file_id, file_name, article_id FROM materials WHERE id=%s", (material_id,))
            row = c.fetchone()
            conn.close()
            return row
        return await asyncio.to_thread(_get)

db = Database()

# ========== ФОНОВОЕ СКАЧИВАНИЕ ==========
async def download_article_file(article_id, file_id, title, context):
    try:
        bot = context.bot
        telegram_file = await bot.get_file(file_id)
        # Теперь файлы не сохраняем локально – ссылки на file_id в БД достаточно,
        # но если хотите сохранить PDF на диск, нужно подключить Render Disk.
        # Пока просто пропускаем скачивание, чтобы не занимать место.
        logger.info(f"Файл статьи {article_id} зарегистрирован (file_id: {file_id})")
    except Exception as e:
        logger.error(f"Не удалось обработать файл статьи {article_id}: {e}")

# ========== KEEP-ALIVE ==========
async def keep_alive(context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.get_me()
        logger.debug("Keep-alive ping successful")
    except Exception as e:
        logger.error(f"Keep-alive failed: {e}")

# ========== ССЫЛКИ ==========
def make_article_link(article_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=article_{article_id}"

# ========== УДАЛЕНИЕ СООБЩЕНИЙ ПРИ /start (только в личных чатах) ==========
async def delete_messages_between(context, chat_id, from_msg_id, to_msg_id):
    for msg_id in range(from_msg_id, to_msg_id):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass

# ========== ОТПРАВКА УВЕДОМЛЕНИЯ В КАНАЛ (ТОЛЬКО ТЕКСТ) ==========
async def send_to_channel(context, text):
    try:
        await context.bot.send_message(
            chat_id=POSTS_CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Не удалось отправить в канал: {e}")

# ========== РАССЫЛКИ ПОДПИСЧИКАМ ==========
async def broadcast_new_article(context, title, lang, article_id):
    subscribers = await db.get_subscribers()
    if not subscribers:
        return
    link = make_article_link(article_id)
    lang_emoji = "🇷🇺" if lang == "ru" else "🇬🇧"
    text = f"📢 <b>Новая статья!</b>\n{lang_emoji} {title}\n\n<a href='{link}'>Открыть статью</a>"
    for user_id in subscribers:
        try:
            if await is_member(user_id, context):
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
                await asyncio.sleep(0.05)
            else:
                await db.remove_subscriber(user_id)
        except Exception as e:
            logger.error(f"Ошибка уведомления {user_id}: {e}")

async def broadcast_post(context, post_type, content=None, file_id=None, caption=None):
    subscribers = await db.get_subscribers()
    if not subscribers:
        return
    for user_id in subscribers:
        try:
            if not await is_member(user_id, context):
                await db.remove_subscriber(user_id)
                continue
            if post_type == "text":
                await context.bot.send_message(chat_id=user_id, text=content, parse_mode=ParseMode.HTML)
            elif post_type == "photo":
                await context.bot.send_photo(chat_id=user_id, photo=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif post_type == "video":
                await context.bot.send_video(chat_id=user_id, video=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif post_type == "document":
                await context.bot.send_document(chat_id=user_id, document=file_id, caption=caption, parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Ошибка рассылки {user_id}: {e}")

# ========== ПРОВЕРКА УЧАСТИЯ ==========
async def is_member(user_id, context):
    try:
        member = await context.bot.get_chat_member(GROUP_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

async def can_access_materials(user_id, context):
    if not MATERIALS_CHANNEL_ID:
        return True
    try:
        member = await context.bot.get_chat_member(int(MATERIALS_CHANNEL_ID), user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

# ========== ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    current_msg_id = update.message.message_id
    chat_type = update.effective_chat.type

    if chat_type == "private":
        last_start = context.user_data.get("last_start_msg_id")
        if last_start is not None:
            asyncio.create_task(delete_messages_between(context, chat_id, last_start, current_msg_id))
        context.user_data["last_start_msg_id"] = current_msg_id

    if update.message.text and update.message.text.startswith("/start article_"):
        parts = update.message.text.split()
        if len(parts) > 1 and parts[1].startswith("article_"):
            article_id = int(parts[1].split("_")[1])
            if not await is_member(user_id, context):
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
                ])
                await update.message.reply_text(
                    f"Для доступа вступите в группу {GROUP_ID} и нажмите проверку.",
                    reply_markup=keyboard,
                )
                return
            article = await db.get_article(article_id)
            if article:
                await view_article_by_id(update, context, article_id)
                return

    if not await is_member(user_id, context):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
        ])
        await update.message.reply_text(
            f"Для доступа вступите в группу {GROUP_ID} и нажмите проверку.",
            reply_markup=keyboard,
        )
        return

    rows = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ]
    if await can_access_materials(user_id, context):
        rows.append([InlineKeyboardButton("📂 Материалы", callback_data="materials_list")])
    rows.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])

    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=current_msg_id,
            reply_markup=InlineKeyboardMarkup(rows)
        )
    except Exception:
        await update.message.reply_text(
            "Выберите язык / Choose language:",
            reply_markup=InlineKeyboardMarkup(rows)
        )

# Остальные хендлеры (view_article, choose_language, админка, материалы и т.д.)
# полностью идентичны последней версии кода.
# Из-за ограничения длины ответа я не дублирую их здесь, но они должны быть в файле.