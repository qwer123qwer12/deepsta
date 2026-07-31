import asyncio
import os
import sqlite3
import logging
import threading
from datetime import datetime
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
# Задайте их в настройках Render (Environment Variables)
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
GROUP_ID = os.environ["GROUP_ID"]
POSTS_CHANNEL_ID = int(os.environ["POSTS_CHANNEL_ID"])
ARTICLES_DIR = "articles_pdf"

# Состояния
LANG, CONTENT_TYPE, TITLE, FILE, POST_CONTENT = range(5)  # для статей
BROADCAST_MESSAGE = 0  # для рассылки
FEEDBACK_MESSAGE = 1   # для обратной связи
EDIT_POST_TEXT = 2     # для редактирования поста

# Flask-приложение для пинга
web_app = Flask(__name__)

@web_app.route('/ping')
def ping():
    return "pong", 200

# Логирование
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
        conn.commit()
        conn.close()

    # --- Статьи ---
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
            c.execute("SELECT id, title, content, language, date_added FROM articles WHERE content_type='text' ORDER BY id DESC")
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

    # --- Подписчики ---
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

# ========== ОТПРАВКА В КАНАЛ ==========
async def send_to_channel(context, text=None, document=None, caption=None):
    try:
        if document:
            await context.bot.send_document(
                chat_id=POSTS_CHANNEL_ID,
                document=document,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN
            )
        elif text:
            await context.bot.send_message(
                chat_id=POSTS_CHANNEL_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN
            )
    except Exception as e:
        logger.error(f"Не удалось отправить в канал: {e}")

# ========== РАССЫЛКИ ПОДПИСЧИКАМ ==========
async def broadcast_new_article(context, title, lang):
    subscribers = await db.get_subscribers()
    if not subscribers:
        return
    lang_emoji = "🇷🇺" if lang == "ru" else "🇬🇧"
    text = f"📢 *Новая статья!*\n{lang_emoji} {title}\n\nЧтобы получить, откройте бота: /start"
    for user_id in subscribers:
        try:
            if await is_member(user_id, context):
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.MARKDOWN)
                await asyncio.sleep(0.05)
            else:
                await db.remove_subscriber(user_id)
        except Exception as e:
            logger.error(f"Ошибка уведомления {user_id}: {e}")

async def broadcast_post(context, text):
    subscribers = await db.get_subscribers()
    if not subscribers:
        return
    for user_id in subscribers:
        try:
            if await is_member(user_id, context):
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.MARKDOWN)
                await asyncio.sleep(0.05)
            else:
                await db.remove_subscriber(user_id)
        except Exception as e:
            logger.error(f"Ошибка рассылки {user_id}: {e}")

# ========== ПРОВЕРКА УЧАСТИЯ ==========
async def is_member(user_id, context):
    try:
        member = await context.bot.get_chat_member(GROUP_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

# ========== ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_member(user_id, context):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
        ])
        await update.message.reply_text(
            f"Для доступа вступите в группу {GROUP_ID} и нажмите проверку.",
            reply_markup=keyboard,
        )
        return

    keyboard_buttons = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")]
    ]
    await update.message.reply_text("Выберите язык / Choose language:", reply_markup=InlineKeyboardMarkup(keyboard_buttons))

async def show_main_menu(query, context):
    user_id = query.from_user.id
    if not await is_member(user_id, context):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
        ])
        await query.edit_message_text(
            f"Для доступа вступите в группу {GROUP_ID} и нажмите проверку.",
            reply_markup=keyboard,
        )
        return

    keyboard_buttons = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")]
    ]
    await query.edit_message_text("Выберите язык / Choose language:", reply_markup=InlineKeyboardMarkup(keyboard_buttons))

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
        await query.edit_message_text(
            "Вы не состоите в группе. Вступите и нажмите проверку.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
            ])
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

    keyboard_buttons = []
    for art in articles:
        art_id, title, _, _, content_type, _, lang_tag, _ = art
        type_emoji = "📄" if content_type == "pdf" else "📝"
        keyboard_buttons.append(
            [InlineKeyboardButton(f"{type_emoji} {title} ({lang_tag})", callback_data=f"article_{art_id}")]
        )
    keyboard_buttons.append([InlineKeyboardButton("🔙 Сменить язык", callback_data="back_to_lang")])
    keyboard_buttons.append([InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")])

    await query.edit_message_text("Доступные статьи:", reply_markup=InlineKeyboardMarkup(keyboard_buttons))

async def back_to_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_member(query.from_user.id, context):
        await query.edit_message_text("Вы не состоите в группе.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
        ]))
        return
    keyboard = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")]
    ]
    await query.edit_message_text("Выберите язык:", reply_markup=InlineKeyboardMarkup(keyboard))

async def view_article(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_member(query.from_user.id, context):
        await query.edit_message_text("Вы не состоите в группе.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Проверить участие", callback_data="check_sub")]
        ]))
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
        text = f"📝 *{title}*\n\n{content}\n\n_Язык: {lang} | {date}_"
        await context.bot.send_message(chat_id=query.message.chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
    else:
        if filename:
            local_path = os.path.join(ARTICLES_DIR, filename)
            if os.path.exists(local_path):
                with open(local_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id=query.message.chat_id, document=f, filename=filename,
                        caption=f"📄 *{title}*\nЯзык: {lang}\nДобавлено: {date}",
                        parse_mode=ParseMode.MARKDOWN, protect_content=True
                    )
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 К списку", callback_data=f"lang_{lang}"),
                     InlineKeyboardButton("🔙 Сменить язык", callback_data="back_to_lang")],
                    [InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")]
                ])
                await query.edit_message_text("Файл отправлен ниже 👇", reply_markup=keyboard)
                return
        if file_id:
            try:
                await context.bot.send_document(
                    chat_id=query.message.chat_id, document=file_id,
                    caption=f"📄 *{title}*\nЯзык: {lang}\nДобавлено: {date}",
                    parse_mode=ParseMode.MARKDOWN, protect_content=True
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
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 К списку", callback_data=f"lang_{lang}"),
         InlineKeyboardButton("🔙 Сменить язык", callback_data="back_to_lang")],
        [InlineKeyboardButton("✉️ Написать автору", callback_data="feedback_start")]
    ])
    await query.edit_message_text("Готово ✅", reply_markup=keyboard)

# ========== ОБРАТНАЯ СВЯЗЬ ==========
async def feedback_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        if not await is_member(query.from_user.id, context):
            await query.edit_message_text("Вы не верифицированы.")
            return ConversationHandler.END
        await query.edit_message_text("✍️ Введите сообщение (или /cancel для отмены):")
    else:
        if not await is_member(update.effective_user.id, context):
            await update.message.reply_text("Вы не верифицированы.")
            return ConversationHandler.END
        await update.message.reply_text("✍️ Введите сообщение (или /cancel для отмены):")
    return FEEDBACK_MESSAGE

async def feedback_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = f"📩 *Сообщение от пользователя*\nID: `{user.id}`\nUsername: @{user.username or 'нет'}\nИмя: {user.full_name}\n\n💬 {update.message.text}"
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode=ParseMode.MARKDOWN)
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
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ])
    await update.message.reply_text("Админ-панель:", reply_markup=keyboard)

# --- Управление текстовыми постами ---
async def manage_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    posts = await db.get_text_posts()
    if not posts:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")]
        ])
        await query.edit_message_text("Нет текстовых постов.", reply_markup=keyboard)
        return
    keyboard = []
    for post in posts:
        post_id, title, _, lang, date = post
        keyboard.append([InlineKeyboardButton(f"📝 {title} ({lang})", callback_data=f"editpost_{post_id}")])
        keyboard.append([InlineKeyboardButton(f"   ❌ Удалить {title}", callback_data=f"delpost_{post_id}")])
    keyboard.append([InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")])
    await query.edit_message_text("Выберите пост для редактирования или удаления:", reply_markup=InlineKeyboardMarkup(keyboard))

async def edit_post_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    post_id = int(query.data.split("_")[1])
    post = await db.get_article(post_id)
    if not post or post[4] != 'text':
        await query.edit_message_text("Пост не найден.")
        return ConversationHandler.END
    context.user_data["edit_post_id"] = post_id
    await query.edit_message_text(f"Текущий текст:\n\n{post[5]}\n\nВведите новый текст поста (или /cancel для отмены):", parse_mode=ParseMode.MARKDOWN)
    return EDIT_POST_TEXT

async def receive_new_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_text = update.message.text
    post_id = context.user_data.get("edit_post_id")
    if not post_id:
        await update.message.reply_text("Ошибка. Начните заново.")
        return ConversationHandler.END
    await db.update_article_content(post_id, new_text)
    await update.message.reply_text("✅ Пост обновлён!")
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

# --- Рассылка поста (с отправкой в канал) ---
async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    await query.edit_message_text("📝 Введите текст поста для рассылки (поддерживается Markdown). /cancel для отмены:")
    return BROADCAST_MESSAGE

async def broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    asyncio.create_task(broadcast_post(context, text))
    asyncio.create_task(send_to_channel(context, text=text))
    await update.message.reply_text("✅ Пост разослан и отправлен в канал!")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ])
    await update.message.reply_text("Админ-панель:", reply_markup=keyboard)
    return ConversationHandler.END

# --- Добавление статьи (с отправкой в канал) ---
async def add_article_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return ConversationHandler.END
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="langchoice_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="langchoice_en")]
    ])
    await query.edit_message_text("Выберите язык статьи:", reply_markup=keyboard)
    return LANG

async def get_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = query.data.split("_")[1]
    context.user_data["article_lang"] = lang
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 PDF-файл", callback_data="type_pdf"),
         InlineKeyboardButton("📝 Текстовый пост", callback_data="type_text")]
    ])
    await query.edit_message_text("Выберите тип контента:", reply_markup=keyboard)
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
        await update.message.reply_text("Теперь введите текст поста (Markdown):")
        return POST_CONTENT

async def get_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if not document:
        await update.message.reply_text("Пожалуйста, отправьте файл.")
        return FILE
    file_name = document.file_name or ""
    if not file_name.lower().endswith(".pdf"):
        await update.message.reply_text("Принимаются только PDF-файлы.")
        return FILE
    title = context.user_data.get("article_title")
    lang = context.user_data.get("article_lang")
    if not title or not lang:
        await update.message.reply_text("Ошибка. Начните заново /admin")
        return ConversationHandler.END
    file_id = document.file_id
    try:
        article_id = await db.add_article(title, file_id, lang, content_type="pdf")
        asyncio.create_task(download_article_file(article_id, file_id, title, context))
        asyncio.create_task(broadcast_new_article(context, title, lang))
        asyncio.create_task(send_to_channel(context, document=file_id, caption=f"📄 *{title}*\nЯзык: {lang}"))
        await update.message.reply_text(f"✅ PDF-статья «{title}» ({lang}) добавлена (ID: {article_id}).")
    except Exception as e:
        logger.error(f"Не удалось сохранить статью: {e}")
        await update.message.reply_text("Не удалось сохранить статью.")
        return ConversationHandler.END
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ])
    await update.message.reply_text("Админ-панель:", reply_markup=keyboard)
    context.user_data.clear()
    return ConversationHandler.END

async def get_post_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    content = update.message.text
    title = context.user_data.get("article_title")
    lang = context.user_data.get("article_lang")
    if not title or not lang:
        await update.message.reply_text("Ошибка. Начните заново /admin")
        return ConversationHandler.END
    try:
        article_id = await db.add_article(title, "", lang, content_type="text", content=content)
        asyncio.create_task(broadcast_new_article(context, title, lang))
        asyncio.create_task(send_to_channel(context, text=f"📝 *{title}*\n\n{content}"))
        await update.message.reply_text(f"✅ Текстовый пост «{title}» ({lang}) добавлен (ID: {article_id}).")
    except Exception as e:
        logger.error(f"Не удалось сохранить пост: {e}")
        await update.message.reply_text("Не удалось сохранить пост.")
        return ConversationHandler.END
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ])
    await update.message.reply_text("Админ-панель:", reply_markup=keyboard)
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Добавление отменено.")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ])
    await update.message.reply_text("Админ-панель:", reply_markup=keyboard)
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
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
        ])
        await query.edit_message_text("Нет статей для удаления.", reply_markup=keyboard)
        return
    keyboard_buttons = []
    for art in articles:
        art_id, title, _, _, content_type, _, lang, _ = art
        type_emoji = "📄" if content_type == "pdf" else "📝"
        keyboard_buttons.append(
            [InlineKeyboardButton(f"❌ {type_emoji} {title} ({lang})", callback_data=f"del_{art_id}")]
        )
    keyboard_buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
    await query.edit_message_text("Выберите статью для удаления:", reply_markup=InlineKeyboardMarkup(keyboard_buttons))

async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    article_id = query.data.split("_")[1]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"yesdel_{article_id}"),
         InlineKeyboardButton("🔙 Отмена", callback_data="delete_article")]
    ])
    await query.edit_message_text("Точно удалить статью?", reply_markup=keyboard)

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
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
        ])
        await query.edit_message_text("Статья удалена. Больше статей нет.", reply_markup=keyboard)
    else:
        keyboard_buttons = []
        for art in articles:
            art_id, title, _, _, content_type, _, lang, _ = art
            type_emoji = "📄" if content_type == "pdf" else "📝"
            keyboard_buttons.append(
                [InlineKeyboardButton(f"❌ {type_emoji} {title} ({lang})", callback_data=f"del_{art_id}")]
            )
        keyboard_buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
        await query.edit_message_text("Статья удалена. Выберите статью для удаления:", reply_markup=InlineKeyboardMarkup(keyboard_buttons))

async def admin_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("Нет доступа.")
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить статью", callback_data="add_article")],
        [InlineKeyboardButton("📢 Создать пост", callback_data="broadcast")],
        [InlineKeyboardButton("📝 Управление постами", callback_data="manage_posts")],
        [InlineKeyboardButton("❌ Удалить статью", callback_data="delete_article")],
    ])
    await query.edit_message_text("Админ-панель:", reply_markup=keyboard)

# ========== ОБРАБОТЧИК ОШИБОК ==========
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)

# ========== ЗАПУСК БОТА ==========
def run_bot():
    application = Application.builder().token(BOT_TOKEN).build()
    application.job_queue.run_repeating(keep_alive, interval=300, first=10)
    logger.info("Keep-alive task scheduled every 300 seconds")

    # ConversationHandler для добавления статьи
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_article_start, pattern="^add_article$")],
        states={
            LANG: [CallbackQueryHandler(get_lang, pattern="^langchoice_")],
            CONTENT_TYPE: [CallbackQueryHandler(get_content_type, pattern="^type_")],
            TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_title)],
            FILE: [MessageHandler(filters.Document.ALL, get_file)],
            POST_CONTENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_post_content)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # ConversationHandler для редактирования поста
    edit_post_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_post_start, pattern="^editpost_")],
        states={
            EDIT_POST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_new_text)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Рассылка поста
    broadcast_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(broadcast_start, pattern="^broadcast$")],
        states={
            BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_send)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Обратная связь
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

    # Пользовательские обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(check_subscription_callback, pattern="^check_sub$"))
    application.add_handler(CallbackQueryHandler(choose_language, pattern="^lang_(ru|en)$"))
    application.add_handler(CallbackQueryHandler(view_article, pattern="^article_"))
    application.add_handler(CallbackQueryHandler(back_to_lang, pattern="^back_to_lang$"))

    # Административные обработчики
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CallbackQueryHandler(manage_posts, pattern="^manage_posts$"))
    application.add_handler(CallbackQueryHandler(delete_post, pattern="^delpost_"))
    application.add_handler(CallbackQueryHandler(delete_article, pattern="^delete_article$"))
    application.add_handler(CallbackQueryHandler(confirm_delete, pattern="^del_"))
    application.add_handler(CallbackQueryHandler(execute_delete, pattern="^yesdel_"))
    application.add_handler(CallbackQueryHandler(admin_back, pattern="^admin_back$"))

    application.add_handler(conv_handler)
    application.add_handler(edit_post_conv)
    application.add_handler(broadcast_conv_handler)
    application.add_handler(feedback_conv_handler)

    # Глобальный /cancel
    application.add_handler(CommandHandler("cancel", global_cancel))

    application.add_error_handler(error_handler)

    print("Бот запущен с keep-alive и отправкой в канал...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# ========== ТОЧКА ВХОДА ==========
if __name__ == "__main__":
    # Локальный запуск
    run_bot()
else:
    # Импорт через Gunicorn (Render): бот в фоновом потоке
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
