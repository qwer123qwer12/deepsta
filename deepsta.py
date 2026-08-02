import asyncio
import os
import sqlite3
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
ARTICLES_DIR = "articles_pdf"
BOT_USERNAME = os.environ.get("BOT_USERNAME", "your_bot_username")

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
os.makedirs(ARTICLES_DIR, exist_ok=True)

# ========== БАЗА ДАННЫХ ==========
class Database:
    def __init__(self, db_path: str = "articles.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                user_id INTEGER UNIQUE NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                link TEXT NOT NULL DEFAULT '',
                file_id TEXT NOT NULL DEFAULT '',
                file_name TEXT NOT NULL DEFAULT '',
                article_id INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("PRAGMA table_info(articles)")
        cols = [col[1] for col in c.fetchall()]
        if "filename" not in cols:
            c.execute("ALTER TABLE articles ADD COLUMN filename TEXT NOT NULL DEFAULT ''")
        if "file_id" not in cols:
            c.execute("ALTER TABLE articles ADD COLUMN file_id TEXT NOT NULL DEFAULT ''")
        if "content_type" not in cols:
            c.execute("ALTER TABLE articles ADD COLUMN content_type TEXT NOT NULL DEFAULT 'pdf'")
        if "content" not in cols:
            c.execute("ALTER TABLE articles ADD COLUMN content TEXT NOT NULL DEFAULT ''")
        c.execute("PRAGMA table_info(materials)")
        mat_cols = [col[1] for col in c.fetchall()]
        if "article_id" not in mat_cols:
            c.execute("ALTER TABLE materials ADD COLUMN article_id INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        conn.close()

    async def add_article(self, title, file_id, language, content_type="pdf", content=""):
        def _add():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            date = datetime.now().strftime("%Y-%m-%d %H:%M")
            c.execute(
                "INSERT INTO articles (title, file_id, language, content_type, content, date_added) VALUES (?,?,?,?,?,?)",
                (title, file_id, language, content_type, content, date),
            )
            conn.commit()
            lid = c.lastrowid
            conn.close()
            return lid
        return await asyncio.to_thread(_add)

    async def update_article_content(self, article_id, new_content):
        def _upd():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("UPDATE articles SET content=? WHERE id=?", (new_content, article_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_upd)

    async def update_article_file(self, article_id, file_id, filename):
        def _upd():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("UPDATE articles SET file_id=?, filename=?, content_type='media' WHERE id=?",
                      (file_id, filename, article_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_upd)

    async def update_filename(self, article_id, filename):
        def _upd():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("UPDATE articles SET filename=? WHERE id=?", (filename, article_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_upd)

    async def delete_article(self, article_id):
        def _del():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT file_id, filename FROM articles WHERE id=?", (article_id,))
            row = c.fetchone()
            if row:
                c.execute("DELETE FROM articles WHERE id=?", (article_id,))
                conn.commit()
                conn.close()
                return row
            conn.close()
            return (None, None)
        return await asyncio.to_thread(_del)

    async def get_all_articles(self, language=None):
        def _get():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            if language:
                c.execute(
                    "SELECT id, title, file_id, filename, content_type, content, language, date_added FROM articles WHERE language=? ORDER BY id DESC",
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
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT id, title, content, language, date_added, content_type FROM articles WHERE content_type != 'pdf' ORDER BY id DESC")
            rows = c.fetchall()
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def get_article(self, article_id):
        def _get():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT id, title, file_id, filename, content_type, content, language, date_added FROM articles WHERE id=?", (article_id,))
            row = c.fetchone()
            conn.close()
            return row
        return await asyncio.to_thread(_get)

    async def add_subscriber(self, user_id):
        def _add():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO subscribers (user_id) VALUES (?)", (user_id,))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_add)

    async def remove_subscriber(self, user_id):
        def _rem():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("DELETE FROM subscribers WHERE user_id=?", (user_id,))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_rem)

    async def get_subscribers(self):
        def _get():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT user_id FROM subscribers")
            rows = [row[0] for row in c.fetchall()]
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def add_material(self, title, description, link="", file_id="", file_name="", article_id=0):
        def _add():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute(
                "INSERT INTO materials (title, description, link, file_id, file_name, article_id) VALUES (?,?,?,?,?,?)",
                (title, description, link, file_id, file_name, article_id),
            )
            conn.commit()
            lid = c.lastrowid
            conn.close()
            return lid
        return await asyncio.to_thread(_add)

    async def delete_material(self, material_id):
        def _del():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("DELETE FROM materials WHERE id=?", (material_id,))
            conn.commit()
            conn.close()
        await asyncio.to_thread(_del)

    async def get_all_materials(self):
        def _get():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT id, title, description, link, file_id, file_name, article_id FROM materials ORDER BY id DESC")
            rows = c.fetchall()
            conn.close()
            return rows
        return await asyncio.to_thread(_get)

    async def get_material(self, material_id):
        def _get():
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT id, title, description, link, file_id, file_name, article_id FROM materials WHERE id=?", (material_id,))
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
        safe_title = "".join(c for c in title if c.isalnum() or c in " _-").rstrip() or "article"
        filename = f"article_{article_id}_{safe_title}.pdf"
        file_path = os.path.join(ARTICLES_DIR, filename)
        await telegram_file.download_to_drive(file_path, timeout=120, read_timeout=120)
        await db.update_filename(article_id, filename)
        logger.info(f"Файл статьи {article_id} сохранён: {filename}")
    except Exception as e:
        logger.error(f"Не удалось скачать файл статьи {article_id}: {e}")

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

# ========== ПЕРЕСЫЛКА КОНТЕНТА В КАНАЛ ==========
async def send_to_channel(context, content_type, content=None, file_id=None, caption=None):
    try:
        if content_type == "text":
            await context.bot.send_message(chat_id=POSTS_CHANNEL_ID, text=content, parse_mode=ParseMode.HTML)
        elif content_type == "photo":
            await context.bot.send_photo(chat_id=POSTS_CHANNEL_ID, photo=file_id, caption=caption, parse_mode=ParseMode.HTML)
        elif content_type == "video":
            await context.bot.send_video(chat_id=POSTS_CHANNEL_ID, video=file_id, caption=caption, parse_mode=ParseMode.HTML)
        elif content_type == "document" or content_type == "pdf":
            await context.bot.send_document(chat_id=POSTS_CHANNEL_ID, document=file_id, caption=caption, parse_mode=ParseMode.HTML)
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

    # Очистка личного чата: удаляем всё между предыдущим /start и текущим
    if chat_type == "private":
        last_start = context.user_data.get("last_start_msg_id")
        if last_start is not None:
            asyncio.create_task(delete_messages_between(context, chat_id, last_start, current_msg_id))
        context.user_data["last_start_msg_id"] = current_msg_id

    # Deep-link
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

    # Главное меню (без поля ввода)
    rows = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ]
    if await can_access_materials(user_id, context):
        rows.append([InlineKeyboardButton("📂 Материалы", callback_data="materials_list")])
    rows.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])

    # Редактируем сообщение /start, добавляя кнопки и оставляя текст
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=current_msg_id,
            reply_markup=InlineKeyboardMarkup(rows)
        )
    except Exception:
        # Если не удалось отредактировать, отправляем новое
        await update.message.reply_text(
            "Выберите язык / Choose language:",
            reply_markup=InlineKeyboardMarkup(rows)
        )


async def view_article_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE, article_id: int):
    article = await db.get_article(article_id)
    if not article:
        await update.message.reply_text("Статья не найдена.")
        return
    art_id, title, file_id, filename, content_type, content, lang, date = article

    if content_type == "text":
        await context.bot.send_message(chat_id=update.effective_chat.id, text=content, parse_mode=ParseMode.HTML)
    elif content_type == "pdf":
        if filename:
            local_path = os.path.join(ARTICLES_DIR, filename)
            if os.path.exists(local_path):
                with open(local_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id, document=f, filename=filename,
                        caption=f"📄 {title}\nЯзык: {lang}\nДобавлено: {date}",
                        protect_content=True
                    )
                return
        if file_id:
            try:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id, document=file_id,
                    caption=f"📄 {title}\nЯзык: {lang}\nДобавлено: {date}",
                    protect_content=True
                )
            except Exception as e:
                logger.error(f"Ошибка file_id: {e}")
                await update.message.reply_text("Файл недоступен.")
                return
        else:
            await update.message.reply_text("Файл отсутствует.")
            return
    elif content_type in ("photo", "video", "document", "media"):
        method = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "document": context.bot.send_document,
            "media": context.bot.send_document,
        }.get(content_type, context.bot.send_document)
        try:
            await method(chat_id=update.effective_chat.id, document=file_id, caption=title)
        except Exception as e:
            logger.error(f"Ошибка отправки медиа: {e}")
            await update.message.reply_text("Ошибка отправки медиа.")


async def show_main_menu(query, context):
    user_id = query.from_user.id
    if not await is_member(user_id, context):
        rows = [[InlineKeyboardButton("Проверить участие", callback_data="check_sub")]]
        await query.edit_message_text(
            f"Для доступа вступите в группу {GROUP_ID} и нажмите проверку.",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    rows = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ]
    if await can_access_materials(user_id, context):
        rows.append([InlineKeyboardButton("📂 Материалы", callback_data="materials_list")])
    rows.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])

    await query.edit_message_text(
        "Выберите язык / Choose language:",
        reply_markup=InlineKeyboardMarkup(rows)
    )


async def check_subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_member(query.from_user.id, context):
        await query.answer("Вы ещё не вступили в группу!", show_alert=True)
        return
    await db.add_subscriber(query.from_user.id)
    await show_main_menu(query, context)


async def choose_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if not await is_member(user_id, context):
        rows = [[InlineKeyboardButton("Проверить участие", callback_data="check_sub")]]
        await query.edit_message_text(
            "Вы не состоите в группе. Вступите и нажмите проверку.",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return
    await db.add_subscriber(user_id)
    lang = query.data.split("_")[1]
    context.user_data["lang"] = lang
    try:
        articles = await db.get_all_articles(language=lang)
    except Exception as e:
        logger.error(f"Ошибка загрузки статей: {e}")
        await query.edit_message_text("Произошла ошибка.")
        return

    rows = []
    for art in articles:
        art_id, title, _, _, content_type, _, lang_tag, _ = art
        type_emoji = {
            "pdf": "📄", "text": "📝", "photo": "🖼️", "video": "🎬",
            "document": "📁", "media": "📎"
        }.get(content_type, "📝")
        # Каждое название на отдельной кнопке, чтобы длинные названия не обрезались
        rows.append(
            [InlineKeyboardButton(f"{type_emoji} {title} ({lang_tag})", callback_data=f"article_{art_id}")]
        )
    rows.append([InlineKeyboardButton("🔙 Сменить язык", callback_data="back_to_lang")])
    if await can_access_materials(user_id, context):
        rows.append([InlineKeyboardButton("📂 Материалы", callback_data="materials_list")])
    rows.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])

    await query.edit_message_text("Доступные статьи:", reply_markup=InlineKeyboardMarkup(rows))


async def back_to_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_member(query.from_user.id, context):
        rows = [[InlineKeyboardButton("Проверить участие", callback_data="check_sub")]]
        await query.edit_message_text("Вы не состоите в группе.", reply_markup=InlineKeyboardMarkup(rows))
        return
    rows = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ]
    if await can_access_materials(query.from_user.id, context):
        rows.append([InlineKeyboardButton("📂 Материалы", callback_data="materials_list")])
    rows.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])
    await query.edit_message_text("Выберите язык:", reply_markup=InlineKeyboardMarkup(rows))


async def view_article(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_member(query.from_user.id, context):
        rows = [[InlineKeyboardButton("Проверить участие", callback_data="check_sub")]]
        await query.edit_message_text("Вы не состоите в группе.", reply_markup=InlineKeyboardMarkup(rows))
        return
    article_id = int(query.data.split("_")[1])
    try:
        article = await db.get_article(article_id)
    except Exception as e:
        logger.error(f"Ошибка статьи {article_id}: {e}")
        await query.edit_message_text("Статья не найдена.")
        return
    if not article:
        await query.edit_message_text("Статья не найдена.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К списку", callback_data=f"lang_{context.user_data.get('lang', 'ru')}")]
        ]))
        return
    art_id, title, file_id, filename, content_type, content, lang, date = article

    if content_type == "text":
        await context.bot.send_message(chat_id=query.message.chat_id, text=content, parse_mode=ParseMode.HTML)
    elif content_type == "pdf":
        if filename:
            local_path = os.path.join(ARTICLES_DIR, filename)
            if os.path.exists(local_path):
                with open(local_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id=query.message.chat_id, document=f, filename=filename,
                        caption=f"📄 {title}\nЯзык: {lang}\nДобавлено: {date}",
                        protect_content=True
                    )
                return
        if file_id:
            try:
                await context.bot.send_document(
                    chat_id=query.message.chat_id, document=file_id,
                    caption=f"📄 {title}\nЯзык: {lang}\nДобавлено: {date}",
                    protect_content=True
                )
            except Exception as e:
                logger.error(f"Ошибка file_id: {e}")
                await query.edit_message_text("Файл недоступен.", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 К списку", callback_data=f"lang_{lang}")]
                ]))
                return
        else:
            await query.edit_message_text("Файл отсутствует.")
            return
    elif content_type in ("photo", "video", "document", "media"):
        method = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "document": context.bot.send_document,
            "media": context.bot.send_document,
        }.get(content_type, context.bot.send_document)
        try:
            await method(chat_id=query.message.chat_id, document=file_id, caption=title)
        except Exception as e:
            logger.error(f"Ошибка отправки медиа: {e}")
            await query.edit_message_text("Ошибка отправки медиа.")

    rows = [
        [InlineKeyboardButton("🔙 К списку", callback_data=f"lang_{lang}"),
         InlineKeyboardButton("🔙 Сменить язык", callback_data="back_to_lang")],
    ]
    if await can_access_materials(query.from_user.id, context):
        rows.append([InlineKeyboardButton("📂 Материалы", callback_data="materials_list")])
    rows.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])
    await query.edit_message_text("Готово ✅", reply_markup=InlineKeyboardMarkup(rows))


# ========== МАТЕРИАЛЫ (пользовательская часть) ==========
async def materials_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await can_access_materials(query.from_user.id, context):
        await query.edit_message_text("⛔ У вас нет доступа к материалам.")
        return
    materials = await db.get_all_materials()
    if not materials:
        await query.edit_message_text("Нет рекомендованных материалов.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_welcome")]
        ]))
        return
    rows = []
    for mat in materials:
        mat_id, title, _, _, _, _, article_id = mat
        prefix = "📎" if article_id else ""
        rows.append([InlineKeyboardButton(f"{prefix} {title}", callback_data=f"material_{mat_id}")])
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_welcome")])
    await query.edit_message_text("📂 Рекомендованные материалы:", reply_markup=InlineKeyboardMarkup(rows))

async def material_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await can_access_materials(query.from_user.id, context):
        await query.edit_message_text("⛔ Нет доступа.")
        return
    mat_id = int(query.data.split("_")[1])
    material = await db.get_material(mat_id)
    if not material:
        await query.edit_message_text("Материал не найден.")
        return
    _, title, description, link, file_id, file_name, article_id = material
    text = f"📌 <b>{title}</b>\n\n{description}"
    rows = []
    if article_id:
        rows.append([InlineKeyboardButton("📖 Открыть статью", callback_data=f"article_{article_id}")])
    if link:
        text += f"\n\n<a href='{link}'>Ссылка</a>"
    if file_id:
        rows.append([InlineKeyboardButton("📥 Скачать файл", callback_data=f"download_material_{mat_id}")])
    rows.append([InlineKeyboardButton("🔙 К списку материалов", callback_data="materials_list"),
                 InlineKeyboardButton("🔙 В меню", callback_data="back_to_welcome")])
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))

async def download_material(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await can_access_materials(query.from_user.id, context):
        await query.answer("⛔ Нет доступа", show_alert=True)
        return
    mat_id = int(query.data.split("_")[2])
    material = await db.get_material(mat_id)
    if not material or not material[4]:
        await query.answer("Файл не найден", show_alert=True)
        return
    _, title, _, _, file_id, file_name, _ = material
    try:
        await context.bot.send_document(chat_id=query.message.chat_id, document=file_id, caption=title)
    except Exception as e:
        logger.error(f"Ошибка отправки материала: {e}")
        await query.answer("Не удалось отправить файл", show_alert=True)

# ========== ОБРАТНАЯ СВЯЗЬ (поле ввода появляется) ==========
async def feedback_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        if not await is_member(query.from_user.id, context):
            await query.edit_message_text("Вы не верифицированы.")
            return ConversationHandler.END
        # Отправляем новое сообщение с просьбой ввести текст – поле ввода вернётся
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="✍️ Введите сообщение для автора (или /cancel для отмены):"
        )
        # Удаляем предыдущее меню
        try:
            await query.delete_message()
        except Exception:
            pass
    else:
        if not await is_member(update.effective_user.id, context):
            await update.message.reply_text("Вы не верифицированы.")
            return ConversationHandler.END
        await update.message.reply_text("✍️ Введите сообщение для автора (или /cancel для отмены):")
    return FEEDBACK_MESSAGE

async def feedback_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = f"📩 <b>Сообщение от пользователя</b>\nID: {user.id}\nUsername: @{user.username or 'нет'}\nИмя: {user.full_name}\n\n{update.message.text}"
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode=ParseMode.HTML)
        await update.message.reply_text("✅ Отправлено!")
    except Exception as e:
        logger.error(f"Ошибка фидбека: {e}")
        await update.message.reply_text("Не удалось отправить.")
    return ConversationHandler.END

async def feedback_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отправка отменена.")
    return ConversationHandler.END

# ========== ГЛОБАЛЬНАЯ ОТМЕНА ==========
async def global_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Нет активной операции для отмены.")

# ========== АДМИН-ПАНЕЛЬ ==========
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("У вас нет доступа.")
        return
    rows = [
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("📂 Управление материалами", callback_data="admin_materials")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ]
    await update.message.reply_text("Админ-панель:", reply_markup=InlineKeyboardMarkup(rows))

# --- Создать пост (рассылка + канал) ---
async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    await query.edit_message_text(
        "📢 Отправьте пост для рассылки. Можно вставить фото/видео/документ, эмодзи и HTML-разметку.\n"
        "/cancel для отмены:"
    )
    return BROADCAST_MESSAGE

async def broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caption = update.message.caption or ""
    if update.message.text:
        content = update.message.text
        asyncio.create_task(broadcast_post(context, "text", content=content))
        asyncio.create_task(send_to_channel(context, "text", content=content))
    elif update.message.photo:
        file_id = update.message.photo[-1].file_id
        asyncio.create_task(broadcast_post(context, "photo", file_id=file_id, caption=caption))
        asyncio.create_task(send_to_channel(context, "photo", file_id=file_id, caption=caption))
    elif update.message.video:
        file_id = update.message.video.file_id
        asyncio.create_task(broadcast_post(context, "video", file_id=file_id, caption=caption))
        asyncio.create_task(send_to_channel(context, "video", file_id=file_id, caption=caption))
    elif update.message.document:
        file_id = update.message.document.file_id
        asyncio.create_task(broadcast_post(context, "document", file_id=file_id, caption=caption))
        asyncio.create_task(send_to_channel(context, "document", file_id=file_id, caption=caption))
    else:
        await update.message.reply_text("Неизвестный тип сообщения.")
        return BROADCAST_MESSAGE

    await update.message.reply_text("✅ Пост разослан и отправлен в канал!")
    rows = [
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("📂 Управление материалами", callback_data="admin_materials")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ]
    await update.message.reply_text("Админ-панель:", reply_markup=InlineKeyboardMarkup(rows))
    return ConversationHandler.END

# --- Управление постами ---
async def manage_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    posts = await db.get_text_posts()
    if not posts:
        rows = [[InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")]]
        await query.edit_message_text(
            "Нет сохранённых постов. Чтобы создать пост, используйте «Добавить статью» → выберите «Фото», «Видео» или «Документ».",
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return
    rows = []
    for post in posts:
        post_id, title, content, lang, date, ctype = post
        emoji = {"text":"📝","photo":"🖼️","video":"🎬","document":"📁","media":"📎"}.get(ctype, "📝")
        rows.append([InlineKeyboardButton(f"{emoji} {title} ({lang})", callback_data=f"editpost_{post_id}")])
        rows.append([InlineKeyboardButton(f"   ❌ Удалить {title}", callback_data=f"delpost_{post_id}")])
    rows.append([InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")])
    await query.edit_message_text("Выберите пост для редактирования или удаления:", reply_markup=InlineKeyboardMarkup(rows))

async def edit_post_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    post_id = int(query.data.split("_")[1])
    post = await db.get_article(post_id)
    if not post or post[4] == 'pdf':
        await query.edit_message_text("Пост не найден.")
        return ConversationHandler.END
    context.user_data["edit_post_id"] = post_id
    if post[4] == 'text':
        await query.edit_message_text(f"Текущий текст:\n\n{post[5]}\n\nВведите новый текст (HTML) или отправьте медиафайл для замены (или /cancel):", parse_mode=ParseMode.HTML)
    else:
        await query.edit_message_text("Отправьте новый текст (HTML) или медиафайл для замены поста (или /cancel):")
    return EDIT_POST_TEXT

async def receive_new_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    post_id = context.user_data.get("edit_post_id")
    if not post_id:
        await update.message.reply_text("Ошибка. Начните заново.")
        return ConversationHandler.END

    if update.message.text:
        new_text = update.message.text
        await db.update_article_content(post_id, new_text)
        await db.update_article_file(post_id, "", "")
        await update.message.reply_text("✅ Пост обновлён как текстовый!")
    elif update.message.photo:
        file_id = update.message.photo[-1].file_id
        await db.update_article_file(post_id, file_id, "")
        await db.update_article_content(post_id, "photo")
        await update.message.reply_text("✅ Пост обновлён как фото!")
    elif update.message.video:
        file_id = update.message.video.file_id
        await db.update_article_file(post_id, file_id, "")
        await db.update_article_content(post_id, "video")
        await update.message.reply_text("✅ Пост обновлён как видео!")
    elif update.message.document:
        file_id = update.message.document.file_id
        await db.update_article_file(post_id, file_id, update.message.document.file_name or "")
        await db.update_article_content(post_id, "document")
        await update.message.reply_text("✅ Пост обновлён как документ!")
    else:
        await update.message.reply_text("Отправьте текст или медиафайл.")
        return EDIT_POST_TEXT

    await manage_posts(update, context)
    return ConversationHandler.END

async def delete_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    post_id = int(query.data.split("_")[1])
    file_id, filename = await db.delete_article(post_id)
    if filename:
        file_path = os.path.join(ARTICLES_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
    await query.edit_message_text("Пост удалён.")
    await manage_posts(update, context)

# --- Добавление статьи (с пересылкой в канал) ---
async def add_article_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    rows = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="langchoice_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="langchoice_en")]
    ]
    await query.edit_message_text("Выберите язык статьи:", reply_markup=InlineKeyboardMarkup(rows))
    return LANG

async def get_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = query.data.split("_")[1]
    context.user_data["article_lang"] = lang
    rows = [
        [InlineKeyboardButton("📄 PDF-файл", callback_data="type_pdf")],
        [InlineKeyboardButton("🖼️ Фото", callback_data="type_photo"),
         InlineKeyboardButton("🎬 Видео", callback_data="type_video")],
        [InlineKeyboardButton("📁 Документ", callback_data="type_document")],
    ]
    await query.edit_message_text("Выберите тип контента:", reply_markup=InlineKeyboardMarkup(rows))
    return CONTENT_TYPE

async def get_content_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    content_type = query.data.split("_")[1]
    context.user_data["content_type"] = content_type
    await query.edit_message_text("Введите название статьи:")
    return TITLE

async def get_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["article_title"] = update.message.text
    content_type = context.user_data.get("content_type")
    if content_type == "pdf":
        await update.message.reply_text("Теперь отправьте PDF-файл статьи (до 50 МБ):")
        return FILE
    else:
        await update.message.reply_text(f"Отправьте {content_type} файл:")
        return FILE

async def get_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = context.user_data.get("article_title")
    lang = context.user_data.get("article_lang")
    content_type = context.user_data.get("content_type", "pdf")
    if not title or not lang:
        await update.message.reply_text("Ошибка. Начните заново /admin")
        return ConversationHandler.END

    if content_type == "pdf":
        document = update.message.document
        if not document or not (document.file_name or "").lower().endswith(".pdf"):
            await update.message.reply_text("Принимаются только PDF-файлы.")
            return FILE
        file_id = document.file_id
        try:
            article_id = await db.add_article(title, file_id, lang, content_type="pdf")
            asyncio.create_task(download_article_file(article_id, file_id, title, context))
            asyncio.create_task(broadcast_new_article(context, title, lang, article_id))
            asyncio.create_task(send_to_channel(context, "pdf", file_id=file_id, caption=f"📄 {title}"))
            await update.message.reply_text(f"✅ PDF-статья «{title}» ({lang}) добавлена (ID: {article_id}).")
        except Exception as e:
            logger.error(f"Не удалось сохранить статью: {e}")
            await update.message.reply_text("Не удалось сохранить статью.")
            return ConversationHandler.END
    elif content_type in ("photo", "video", "document"):
        if content_type == "photo" and update.message.photo:
            file_id = update.message.photo[-1].file_id
        elif content_type == "video" and update.message.video:
            file_id = update.message.video.file_id
        elif content_type == "document" and update.message.document:
            file_id = update.message.document.file_id
        else:
            await update.message.reply_text(f"Отправьте {content_type} файл.")
            return FILE
        try:
            article_id = await db.add_article(title, file_id, lang, content_type=content_type)
            asyncio.create_task(broadcast_new_article(context, title, lang, article_id))
            asyncio.create_task(send_to_channel(context, content_type, file_id=file_id, caption=title))
            await update.message.reply_text(f"✅ Медиа-пост «{title}» ({lang}) добавлен (ID: {article_id}).")
        except Exception as e:
            logger.error(f"Не удалось сохранить медиа: {e}")
            await update.message.reply_text("Не удалось сохранить медиа.")
            return ConversationHandler.END
    else:
        await update.message.reply_text("Неизвестный тип контента.")
        return ConversationHandler.END

    rows = [
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("📂 Управление материалами", callback_data="admin_materials")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ]
    await update.message.reply_text("Админ-панель:", reply_markup=InlineKeyboardMarkup(rows))
    context.user_data.clear()
    return ConversationHandler.END

# --- Удаление статьи ---
async def delete_article(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    try:
        articles = await db.get_all_articles()
    except Exception as e:
        logger.error(f"Ошибка статей: {e}")
        await query.edit_message_text("Не удалось загрузить список.")
        return
    if not articles:
        rows = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]]
        await query.edit_message_text("Нет статей для удаления.", reply_markup=InlineKeyboardMarkup(rows))
        return
    rows = []
    for art in articles:
        art_id, title, _, _, content_type, _, lang, _ = art
        type_emoji = {
            "pdf": "📄", "text": "📝", "photo": "🖼️", "video": "🎬", "document": "📁", "media": "📎"
        }.get(content_type, "📝")
        rows.append(
            [InlineKeyboardButton(f"❌ {type_emoji} {title} ({lang})", callback_data=f"del_{art_id}")]
        )
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
    await query.edit_message_text("Выберите статью для удаления:", reply_markup=InlineKeyboardMarkup(rows))

async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    article_id = query.data.split("_")[1]
    rows = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"yesdel_{article_id}"),
         InlineKeyboardButton("🔙 Отмена", callback_data="delete_article")]
    ]
    await query.edit_message_text("Точно удалить статью?", reply_markup=InlineKeyboardMarkup(rows))

async def execute_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    article_id = int(query.data.split("_")[1])
    file_id, filename = await db.delete_article(article_id)
    if filename:
        file_path = os.path.join(ARTICLES_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
    articles = await db.get_all_articles()
    if not articles:
        rows = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]]
        await query.edit_message_text("Статья удалена. Больше статей нет.", reply_markup=InlineKeyboardMarkup(rows))
    else:
        rows = []
        for art in articles:
            art_id, title, _, _, content_type, _, lang, _ = art
            type_emoji = {
                "pdf": "📄", "text": "📝", "photo": "🖼️", "video": "🎬", "document": "📁", "media": "📎"
            }.get(content_type, "📝")
            rows.append(
                [InlineKeyboardButton(f"❌ {type_emoji} {title} ({lang})", callback_data=f"del_{art_id}")]
            )
        rows.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
        await query.edit_message_text("Статья удалена. Выберите статью для удаления:", reply_markup=InlineKeyboardMarkup(rows))

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Добавление отменено.")
    rows = [
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("📂 Управление материалами", callback_data="admin_materials")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ]
    await update.message.reply_text("Админ-панель:", reply_markup=InlineKeyboardMarkup(rows))
    context.user_data.clear()
    return ConversationHandler.END

async def admin_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    rows = [
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("📂 Управление материалами", callback_data="admin_materials")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ]
    await query.edit_message_text("Админ-панель:", reply_markup=InlineKeyboardMarkup(rows))

# ========== УПРАВЛЕНИЕ МАТЕРИАЛАМИ (админ) ==========
async def admin_materials_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    rows = [
        [InlineKeyboardButton("➕ Добавить материал", callback_data="add_material")],
        [InlineKeyboardButton("❌ Удалить материал", callback_data="delete_material")],
        [InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")]
    ]
    await query.edit_message_text("📂 Управление материалами:", reply_markup=InlineKeyboardMarkup(rows))

async def add_material_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    await query.edit_message_text("Введите название материала:")
    return MATERIAL_TITLE

async def material_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["material_title"] = update.message.text
    await update.message.reply_text("Введите описание (или /cancel для отмены):")
    return MATERIAL_DESC

async def material_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["material_desc"] = update.message.text
    rows = [
        [InlineKeyboardButton("📎 Отправить ссылку", callback_data="mat_link")],
        [InlineKeyboardButton("📤 Загрузить файл", callback_data="mat_file")],
        [InlineKeyboardButton("📄 Прикрепить статью", callback_data="mat_article")],
    ]
    await update.message.reply_text("Вы хотите прикрепить ссылку, файл или статью?", reply_markup=InlineKeyboardMarkup(rows))
    return MATERIAL_LINK_OR_FILE

async def material_link_or_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data.split("_")[1]
    if choice == "link":
        await query.edit_message_text("Отправьте ссылку (или /cancel):")
        context.user_data["material_choice"] = "link"
    elif choice == "file":
        await query.edit_message_text("Отправьте файл (PDF, изображение, документ).")
        context.user_data["material_choice"] = "file"
    elif choice == "article":
        articles = await db.get_all_articles()
        if not articles:
            rows = [[InlineKeyboardButton("🔙 Назад", callback_data="mat_back")]]
            await query.edit_message_text("Нет сохранённых статей для прикрепления.", reply_markup=InlineKeyboardMarkup(rows))
            return MATERIAL_LINK_OR_FILE
        rows = []
        for art in articles:
            aid, title, _, _, _, _, lang, _ = art
            rows.append([InlineKeyboardButton(f"{title} ({lang})", callback_data=f"mat_select_article_{aid}")])
        rows.append([InlineKeyboardButton("🔙 Назад", callback_data="mat_back")])
        await query.edit_message_text("Выберите статью:", reply_markup=InlineKeyboardMarkup(rows))
        context.user_data["material_choice"] = "article"
    return MATERIAL_LINK_OR_FILE

async def material_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        text = update.message.text
        title = context.user_data.get("material_title")
        desc = context.user_data.get("material_desc")
        choice = context.user_data.get("material_choice")
        if not title or not desc:
            await update.message.reply_text("Ошибка. Начните заново.")
            return ConversationHandler.END
        if choice == "link":
            await db.add_material(title, desc, link=text)
        elif choice == "article":
            await db.add_material(title, desc, article_id=int(text))
        await update.message.reply_text("✅ Материал добавлен!")
    elif update.callback_query:
        query = update.callback_query
        await query.answer()
        data = query.data
        if data.startswith("mat_select_article_"):
            article_id = int(data.split("_")[3])
            title = context.user_data.get("material_title")
            desc = context.user_data.get("material_desc")
            if not title or not desc:
                await query.edit_message_text("Ошибка. Начните заново.")
                return ConversationHandler.END
            await db.add_material(title, desc, article_id=article_id)
            await query.edit_message_text("✅ Материал привязан к статье!")
            await admin_materials_menu(update, context)
            return ConversationHandler.END
        elif data == "mat_back":
            await material_desc(update, context)
            return MATERIAL_LINK_OR_FILE
    elif update.message and update.message.document:
        doc = update.message.document
        file_id = doc.file_id
        file_name = doc.file_name or "material.bin"
        title = context.user_data.get("material_title")
        desc = context.user_data.get("material_desc")
        if not title or not desc:
            await update.message.reply_text("Ошибка. Начните заново.")
            return ConversationHandler.END
        await db.add_material(title, desc, file_id=file_id, file_name=file_name)
        await update.message.reply_text("✅ Материал с файлом добавлен!")
    else:
        await update.message.reply_text("Отправьте ссылку, ID статьи или файл.")
        return MATERIAL_LINK_OR_FILE

    await admin_materials_menu(update, context)
    return ConversationHandler.END

async def cancel_material(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Добавление материала отменено.")
    await admin_materials_menu(update, context)
    return ConversationHandler.END

async def delete_material_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    materials = await db.get_all_materials()
    if not materials:
        rows = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_materials")]]
        await query.edit_message_text("Нет материалов для удаления.", reply_markup=InlineKeyboardMarkup(rows))
        return
    rows = []
    for mat in materials:
        mat_id, title, _, _, _, _, _ = mat
        rows.append([InlineKeyboardButton(f"❌ {title}", callback_data=f"delmat_{mat_id}")])
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_materials")])
    await query.edit_message_text("Выберите материал для удаления:", reply_markup=InlineKeyboardMarkup(rows))

async def confirm_delete_material(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    mat_id = query.data.split("_")[1]
    rows = [
        [InlineKeyboardButton("✅ Да", callback_data=f"yesdelmat_{mat_id}"),
         InlineKeyboardButton("🔙 Отмена", callback_data="delete_material")]
    ]
    await query.edit_message_text("Точно удалить материал?", reply_markup=InlineKeyboardMarkup(rows))

async def execute_delete_material(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    mat_id = int(query.data.split("_")[1])
    await db.delete_material(mat_id)
    await query.edit_message_text("Материал удалён.")
    await delete_material_list(update, context)

# ========== ОБРАБОТЧИК ОШИБОК ==========
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)

# ========== ЗАПУСК БОТА ==========
def run_bot():
    application = Application.builder().token(BOT_TOKEN).build()
    application.job_queue.run_repeating(keep_alive, interval=300, first=10)
    logger.info("Keep-alive task scheduled every 300 seconds")

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_article_start, pattern="^add_article$")],
        states={
            LANG: [CallbackQueryHandler(get_lang, pattern="^langchoice_")],
            CONTENT_TYPE: [CallbackQueryHandler(get_content_type, pattern="^type_")],
            TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_title)],
            FILE: [MessageHandler(filters.Document.ALL | filters.PHOTO | filters.VIDEO, get_file)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    broadcast_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(broadcast_start, pattern="^broadcast$")],
        states={
            BROADCAST_MESSAGE: [
                MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL, broadcast_send),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    edit_post_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_post_start, pattern="^editpost_")],
        states={
            EDIT_POST_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_new_text),
                MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL, receive_new_text),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    mat_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_material_start, pattern="^add_material$")],
        states={
            MATERIAL_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, material_title)],
            MATERIAL_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, material_desc)],
            MATERIAL_LINK_OR_FILE: [
                CallbackQueryHandler(material_link_or_file, pattern="^mat_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, material_receive),
                MessageHandler(filters.Document.ALL, material_receive),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_material)],
    )

    feedback_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("feedback", feedback_start),
            CallbackQueryHandler(feedback_start, pattern="^feedback_start$")
        ],
        states={
            FEEDBACK_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, feedback_message)],
        },
        fallbacks=[CommandHandler("cancel", feedback_cancel)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(check_subscription_callback, pattern="^check_sub$"))
    application.add_handler(CallbackQueryHandler(choose_language, pattern="^lang_(ru|en)$"))
    application.add_handler(CallbackQueryHandler(view_article, pattern="^article_"))
    application.add_handler(CallbackQueryHandler(back_to_lang, pattern="^back_to_lang$"))
    application.add_handler(CallbackQueryHandler(materials_list, pattern="^materials_list$"))
    application.add_handler(CallbackQueryHandler(material_detail, pattern="^material_"))
    application.add_handler(CallbackQueryHandler(download_material, pattern="^download_material_"))

    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CallbackQueryHandler(manage_posts, pattern="^manage_posts$"))
    application.add_handler(CallbackQueryHandler(delete_post, pattern="^delpost_"))
    application.add_handler(CallbackQueryHandler(delete_article, pattern="^delete_article$"))
    application.add_handler(CallbackQueryHandler(confirm_delete, pattern="^del_"))
    application.add_handler(CallbackQueryHandler(execute_delete, pattern="^yesdel_"))
    application.add_handler(CallbackQueryHandler(admin_back, pattern="^admin_back$"))
    application.add_handler(CallbackQueryHandler(admin_materials_menu, pattern="^admin_materials$"))
    application.add_handler(CallbackQueryHandler(delete_material_list, pattern="^delete_material$"))
    application.add_handler(CallbackQueryHandler(confirm_delete_material, pattern="^delmat_"))
    application.add_handler(CallbackQueryHandler(execute_delete_material, pattern="^yesdelmat_"))

    application.add_handler(conv_handler)
    application.add_handler(broadcast_conv_handler)
    application.add_handler(edit_post_conv)
    application.add_handler(mat_conv_handler)
    application.add_handler(feedback_conv_handler)

    application.add_handler(CommandHandler("cancel", global_cancel))
    application.add_error_handler(error_handler)

    print("Бот запущен с длинными названиями и управлением полем ввода...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# ========== ВЕБ-СЕРВЕР В ОТДЕЛЬНОМ ПРОЦЕССЕ ==========
def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    web_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ========== ТОЧКА ВХОДА ==========
if __name__ == "__main__":
    web_process = Process(target=start_web_server, daemon=True)
    web_process.start()
    run_bot()
else:
    pass
