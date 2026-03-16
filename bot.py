import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------
# Environment helpers
# ---------------------------

def env_str(*names: str, default: str = "") -> str:
    """Return the first non-empty env var value, stripped of whitespace and quotes."""
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        value = value.strip().strip('"').strip("'").strip()
        if value:
            return value
    return default


def normalize_chat_ref(value: str) -> Optional[object]:
    """Return int chat_id if numeric, otherwise keep @username/string. Empty -> None."""
    value = (value or "").strip()
    if not value:
        return None
    value = value.strip('"').strip("'").strip()
    if not value:
        return None
    if value.lstrip("-").isdigit():
        try:
            return int(value)
        except ValueError:
            return value
    return value


BOT_TOKEN = env_str("BOT_TOKEN")
DB_PATH = env_str("DB_PATH", default=os.getenv("RAILWAY_VOLUME_MOUNT_PATH", ".") + "/web3_course_clean.db")
PROF_TEST_URL = env_str("PROF_TEST_URL", "TEST_URL", default="https://t.me/Web3UPbot")
BONUS_TEXT_URL = env_str("BONUS_TEXT_URL", "BONUS_LESSON_TEXT_URL")
BONUS_VIDEO_URL = env_str("BONUS_VIDEO_URL", "BONUS_LESSON_VIDEO_URL")
COMMUNITY_URL = env_str("COMMUNITY_URL", "GROUP_URL")
CHANNEL_URL = env_str("CHANNEL_URL")
WEBHOOK_URL = env_str("WEBHOOK_URL").rstrip("/")
WEBHOOK_PATH = env_str("WEBHOOK_PATH", default="webhook").strip("/") or "webhook"
WEBHOOK_SECRET = env_str("WEBHOOK_SECRET")
PORT = int(env_str("PORT", default="8080"))

# Chats used for bonus access verification.
# Accepts numeric chat_id (-100...) or @username / public handle.
BONUS_GROUP_CHAT = normalize_chat_ref(
    env_str(
        "BONUS_GROUP_CHAT_ID",
        "REQUIRED_GROUP_CHAT_ID",
        "GROUP_CHAT_ID",
        "GROUP_CHAT",
        default="",
    )
)
BONUS_CHANNEL_CHAT = normalize_chat_ref(
    env_str(
        "BONUS_CHANNEL_CHAT_ID",
        "REQUIRED_CHANNEL_CHAT_ID",
        "CHANNEL_CHAT_ID",
        "CHANNEL_CHAT",
        default="",
    )
)

LESSONS = [
    {
        "number": 1,
        "title": "Почему Web3 — это не только крипта",
        "subtitle": "После урока вы поймёте, почему Web3 — это отдельная цифровая индустрия, а не только монеты и трейдинг.",
        "text_url": env_str("LESSON_1_TEXT_URL", "LESSON1_TEXT_URL"),
        "video_url": env_str("LESSON_1_VIDEO_URL", "LESSON1_VIDEO_URL"),
        "bullets": ["Web1 → Web2 → Web3", "почему Web3 быстро растёт", "почему это новая индустрия"],
    },
    {
        "number": 2,
        "title": "Как появился Web3",
        "subtitle": "После урока вы увидите, какие проблемы решили Bitcoin и Ethereum и почему из этого вырос целый рынок.",
        "text_url": env_str("LESSON_2_TEXT_URL", "LESSON2_TEXT_URL"),
        "video_url": env_str("LESSON_2_VIDEO_URL", "LESSON2_VIDEO_URL"),
        "bullets": ["зачем появился Bitcoin", "какую проблему он решил", "что изменил Ethereum"],
    },
    {
        "number": 3,
        "title": "Как устроена индустрия Web3",
        "subtitle": "После урока у вас будет цельная карта рынка: от блокчейнов и бирж до DeFi, NFT и инфраструктуры.",
        "text_url": env_str("LESSON_3_TEXT_URL", "LESSON3_TEXT_URL"),
        "video_url": env_str("LESSON_3_VIDEO_URL", "LESSON3_VIDEO_URL"),
        "bullets": ["блокчейны", "биржи", "DeFi", "NFT", "GameFi", "инфраструктура"],
    },
    {
        "number": 4,
        "title": "Кто работает в Web3",
        "subtitle": "После урока вы поймёте, что в Web3 растут не только разработчики, но и сильные нетехнические специалисты.",
        "text_url": env_str("LESSON_4_TEXT_URL", "LESSON4_TEXT_URL"),
        "video_url": env_str("LESSON_4_VIDEO_URL", "LESSON4_VIDEO_URL"),
        "bullets": ["Community", "Marketing", "Business Development", "Operations / Support"],
    },
    {
        "number": 5,
        "title": "Как выбрать профессию в Web3",
        "subtitle": "После урока вы сможете сузить выбор и понять, в какую роль логичнее заходить именно вам.",
        "text_url": env_str("LESSON_5_TEXT_URL", "LESSON5_TEXT_URL"),
        "video_url": env_str("LESSON_5_VIDEO_URL", "LESSON5_VIDEO_URL"),
        "bullets": ["кому подходит Community", "кому подходит Marketing", "кому подходит BD", "кому ближе Operations"],
    },
    {
        "number": 6,
        "title": "Как войти в Web3 с нуля",
        "subtitle": "После урока у вас будет простой и реалистичный маршрут входа без хаоса, перегруза и лишней теории.",
        "text_url": env_str("LESSON_6_TEXT_URL", "LESSON6_TEXT_URL"),
        "video_url": env_str("LESSON_6_VIDEO_URL", "LESSON6_VIDEO_URL"),
        "bullets": ["что изучать первым", "где искать первые возможности", "какие шаги сделать в ближайшие 7 дней"],
    },
    {
        "number": 7,
        "title": "Какая профессия в Web3 подходит вам",
        "subtitle": "После урока вы будете готовы выбрать своё направление и перейти к следующему шагу — тесту и бонусу.",
        "text_url": env_str("LESSON_7_TEXT_URL", "LESSON7_TEXT_URL"),
        "video_url": env_str("LESSON_7_VIDEO_URL", "LESSON7_VIDEO_URL"),
        "bullets": ["карьерная диагностика", "выбор направления", "следующий шаг в индустрии"],
    },
]
TOTAL_LESSONS = len(LESSONS)

WELCOME_TEXT = (
    "🚀 <b>Добро пожаловать в курс «Введение в Web3»</b>\n\n"
    "Это короткий и понятный курс, который поможет без перегруза разобраться в Web3, увидеть реальные роли в индустрии и понять, куда двигаться дальше.\n\n"
    "<b>Внутри:</b>\n"
    "• 7 коротких уроков\n"
    "• текстовый и видеоформат\n"
    "• финальный тест на выбор направления\n"
    "• бонусный карьерный навигатор\n\n"
    "Нажмите <b>«🚀 Начать / продолжить курс»</b>, чтобы открыть текущий урок."
)

HELP_TEXT = (
    "ℹ️ <b>Как пользоваться ботом</b>\n\n"
    "• открывайте урок в тексте или видео;\n"
    "• листайте курс кнопками <b>⬅️ Назад</b> и <b>➡️ Дальше</b>;\n"
    "• в конце пройдите тест и откройте бонус.\n\n"
    "<b>Важно:</b> бонус открывается только после подписки на канал и вступления в группу."
)

MAIN_MENU = ReplyKeyboardMarkup(
    [
        ["🚀 Начать / продолжить курс"],
        ["📊 Мой прогресс", "ℹ️ Помощь"],
    ],
    resize_keyboard=True,
)


# ---------------------------
# DB helpers
# ---------------------------

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        return None


def init_db() -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    current_lesson INTEGER DEFAULT 1,
                    max_lesson_opened INTEGER DEFAULT 1,
                    completed INTEGER DEFAULT 0,
                    test_opened_at TEXT,
                    bonus_opened_at TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    lesson_number INTEGER,
                    meta TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_user_id ON analytics_events(user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_event_type ON analytics_events(event_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_events(created_at)")


def log_event(user_id: int, event_type: str, lesson_number: Optional[int] = None, meta: str = "") -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                "INSERT INTO analytics_events (user_id, event_type, lesson_number, meta, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, event_type, lesson_number, meta, now_iso()),
            )


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()


def upsert_user(user_id: int, username: str, first_name: str) -> None:
    ts = now_iso()
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, current_lesson, max_lesson_opened, created_at, updated_at)
                VALUES (?, ?, ?, 1, 1, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    updated_at = excluded.updated_at
                """,
                (user_id, username, first_name, ts, ts),
            )


def update_user_state(
    user_id: int,
    *,
    current_lesson: Optional[int] = None,
    max_lesson_opened: Optional[int] = None,
    completed: Optional[int] = None,
    test_opened_at: Optional[str] = None,
    bonus_opened_at: Optional[str] = None,
) -> None:
    fields = []
    values = []
    if current_lesson is not None:
        fields.append("current_lesson = ?")
        values.append(current_lesson)
    if max_lesson_opened is not None:
        fields.append("max_lesson_opened = ?")
        values.append(max_lesson_opened)
    if completed is not None:
        fields.append("completed = ?")
        values.append(completed)
    if test_opened_at is not None:
        fields.append("test_opened_at = ?")
        values.append(test_opened_at)
    if bonus_opened_at is not None:
        fields.append("bonus_opened_at = ?")
        values.append(bonus_opened_at)
    fields.append("updated_at = ?")
    values.append(now_iso())
    values.append(user_id)
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE user_id = ?", values)


# ---------------------------
# Presentation helpers
# ---------------------------

def get_lesson(number: int) -> dict:
    return LESSONS[number - 1]


def build_lesson_text(lesson_number: int) -> str:
    lesson = get_lesson(lesson_number)
    bullets = "\n".join(f"• {item}" for item in lesson["bullets"])
    return (
        f"<b>Урок {lesson_number} из {TOTAL_LESSONS}</b>\n"
        f"<b>{lesson['title']}</b>\n\n"
        f"{lesson['subtitle']}\n\n"
        f"<b>Что внутри:</b>\n{bullets}"
    )


def lesson_keyboard(lesson_number: int) -> InlineKeyboardMarkup:
    lesson = get_lesson(lesson_number)
    rows = []

    format_row = []
    if lesson.get("text_url"):
        format_row.append(InlineKeyboardButton("📖 Текстовый урок", url=lesson["text_url"]))
    if lesson.get("video_url"):
        format_row.append(InlineKeyboardButton("🎬 Видео урок", url=lesson["video_url"]))
    if format_row:
        rows.append(format_row)

    nav_row = []
    if lesson_number > 1:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"lesson:{lesson_number - 1}"))
    if lesson_number < TOTAL_LESSONS:
        nav_row.append(InlineKeyboardButton("➡️ Дальше", callback_data=f"lesson:{lesson_number + 1}"))
    else:
        nav_row.append(InlineKeyboardButton("🧭 К тесту и бонусу", callback_data="final"))
    rows.append(nav_row)

    return InlineKeyboardMarkup(rows)


def final_text() -> str:
    return (
        "<b>Курс завершён 🚀</b>\n\n"
        "Вы прошли все 7 уроков и уже понимаете базу Web3, роли в индустрии и возможные точки входа.\n\n"
        "<b>Что дальше:</b>\n"
        "• пройдите тест на профессию в Web3;\n"
        "• подпишитесь на канал и вступите в группу;\n"
        "• после проверки откройте бонусный урок."
    )


def final_keyboard() -> InlineKeyboardMarkup:
    rows = []
    if PROF_TEST_URL:
        rows.append([InlineKeyboardButton("🧭 Пройти тест", url=PROF_TEST_URL)])

    join_row = []
    if COMMUNITY_URL:
        join_row.append(InlineKeyboardButton("💬 Группа", url=COMMUNITY_URL))
    if CHANNEL_URL:
        join_row.append(InlineKeyboardButton("📢 Канал", url=CHANNEL_URL))
    if join_row:
        rows.append(join_row)

    rows.append([InlineKeyboardButton("✅ Проверить подписку и открыть бонус", callback_data="bonus:check")])
    rows.append([InlineKeyboardButton("⬅️ К уроку 7", callback_data="lesson:7")])
    return InlineKeyboardMarkup(rows)


def locked_bonus_text(missing_channel: bool, missing_group: bool) -> str:
    missing = []
    if missing_channel:
        missing.append("• подпишитесь на канал")
    if missing_group:
        missing.append("• вступите в группу")
    missing_text = "\n".join(missing) or "• выполните условия доступа"
    return (
        "<b>Бонус пока закрыт</b>\n\n"
        "Чтобы открыть бонусный урок, нужно выполнить оба условия:\n"
        f"{missing_text}\n\n"
        "После этого нажмите <b>«Проверить снова»</b>."
    )


def locked_bonus_keyboard(missing_channel: bool, missing_group: bool) -> InlineKeyboardMarkup:
    rows = []
    join_row = []
    if missing_group and COMMUNITY_URL:
        join_row.append(InlineKeyboardButton("💬 Вступить в группу", url=COMMUNITY_URL))
    if missing_channel and CHANNEL_URL:
        join_row.append(InlineKeyboardButton("📢 Подписаться на канал", url=CHANNEL_URL))
    if join_row:
        rows.append(join_row)
    rows.append([InlineKeyboardButton("✅ Проверить снова", callback_data="bonus:check")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="final")])
    return InlineKeyboardMarkup(rows)


def bonus_text() -> str:
    return (
        "<b>Бонус открыт 🎁</b>\n\n"
        "Вы выполнили условия доступа. Ниже — бонусный урок по карьерной навигации в Web3."
    )


def bonus_keyboard() -> InlineKeyboardMarkup:
    rows = []
    bonus_row = []
    if BONUS_TEXT_URL:
        bonus_row.append(InlineKeyboardButton("🎁 Бонус: текст", url=BONUS_TEXT_URL))
    if BONUS_VIDEO_URL:
        bonus_row.append(InlineKeyboardButton("🎥 Бонус: видео", url=BONUS_VIDEO_URL))
    if bonus_row:
        rows.append(bonus_row)
    extra_row = []
    if COMMUNITY_URL:
        extra_row.append(InlineKeyboardButton("💬 Группа", url=COMMUNITY_URL))
    if CHANNEL_URL:
        extra_row.append(InlineKeyboardButton("📢 Канал", url=CHANNEL_URL))
    if extra_row:
        rows.append(extra_row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="final")])
    return InlineKeyboardMarkup(rows)


def progress_text(user: sqlite3.Row) -> str:
    current = int(user["current_lesson"] or 1)
    max_opened = int(user["max_lesson_opened"] or 1)
    completed = bool(user["completed"])
    percent = round((max_opened / TOTAL_LESSONS) * 100)
    lines = [
        "📊 <b>Мой прогресс</b>",
        "",
        f"Текущий урок: <b>{current} из {TOTAL_LESSONS}</b>",
        f"Открыто уроков: <b>{max_opened} из {TOTAL_LESSONS}</b>",
        f"Прогресс: <b>{percent}%</b>",
    ]
    if completed:
        lines.append("Курс: <b>завершён</b> ✅")
    else:
        lines.append("Курс: <b>в процессе</b>")
    if parse_dt(user["test_opened_at"]):
        lines.append("Тест: <b>открыт</b>")
    if parse_dt(user["bonus_opened_at"]):
        lines.append("Бонус: <b>открыт</b>")
    return "\n".join(lines)


async def safe_edit_or_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
    parse_mode: str = ParseMode.HTML,
) -> None:
    query = update.callback_query
    if query and query.message:
        try:
            await query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return
        except BadRequest as e:
            msg = str(e).lower()
            if "message is not modified" in msg:
                return
            if "there is no text in the message to edit" not in msg and "message can't be edited" not in msg:
                logger.exception("Failed to edit message")
    if update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )


# ---------------------------
# Bonus access verification
# ---------------------------

def membership_ok(status: Optional[str]) -> bool:
    return status in {"creator", "administrator", "member", "restricted"}


async def check_chat_membership(context: ContextTypes.DEFAULT_TYPE, chat_ref: object, user_id: int) -> Optional[bool]:
    if not chat_ref:
        return None
    try:
        member = await context.bot.get_chat_member(chat_id=chat_ref, user_id=user_id)
        return membership_ok(member.status)
    except Forbidden:
        logger.exception("Bot lacks permissions to check membership for chat %s", chat_ref)
        return None
    except TelegramError:
        logger.exception("Failed to check membership for chat %s", chat_ref)
        return None


async def send_bonus_gate_result(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    # If no checks configured, open bonus directly.
    if not BONUS_GROUP_CHAT and not BONUS_CHANNEL_CHAT:
        if BONUS_TEXT_URL or BONUS_VIDEO_URL:
            update_user_state(user_id, bonus_opened_at=now_iso())
            log_event(user_id, "bonus_opened", TOTAL_LESSONS, "checks_disabled")
            await safe_edit_or_send(update, context, bonus_text(), bonus_keyboard())
        else:
            await safe_edit_or_send(
                update,
                context,
                "<b>Бонус пока не добавлен</b>\n\nСсылка на бонусный урок появится позже.",
                final_keyboard(),
            )
        return

    channel_ok = await check_chat_membership(context, BONUS_CHANNEL_CHAT, user_id) if BONUS_CHANNEL_CHAT else True
    group_ok = await check_chat_membership(context, BONUS_GROUP_CHAT, user_id) if BONUS_GROUP_CHAT else True

    # If Telegram API check failed for configured chats, explain honestly.
    if channel_ok is None or group_ok is None:
        await safe_edit_or_send(
            update,
            context,
            "<b>Не удалось проверить подписку</b>\n\n"
            "Проверьте, что бот добавлен администратором в канал и группу, а затем попробуйте снова.",
            final_keyboard(),
        )
        return

    missing_channel = not bool(channel_ok)
    missing_group = not bool(group_ok)

    if not missing_channel and not missing_group:
        if BONUS_TEXT_URL or BONUS_VIDEO_URL:
            update_user_state(user_id, bonus_opened_at=now_iso())
            log_event(user_id, "bonus_opened", TOTAL_LESSONS, "subscription_verified")
            await safe_edit_or_send(update, context, bonus_text(), bonus_keyboard())
        else:
            await safe_edit_or_send(
                update,
                context,
                "<b>Бонус пока не добавлен</b>\n\nСсылка на бонусный урок появится позже.",
                final_keyboard(),
            )
        return

    meta = f"missing_channel={int(missing_channel)};missing_group={int(missing_group)}"
    log_event(user_id, "bonus_locked", TOTAL_LESSONS, meta)
    await safe_edit_or_send(
        update,
        context,
        locked_bonus_text(missing_channel=missing_channel, missing_group=missing_group),
        locked_bonus_keyboard(missing_channel=missing_channel, missing_group=missing_group),
    )


# ---------------------------
# Handlers
# ---------------------------
async def send_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE, lesson_number: int) -> None:
    user = get_user(update.effective_user.id)
    if not user:
        upsert_user(update.effective_user.id, update.effective_user.username or "", update.effective_user.first_name or "")
        user = get_user(update.effective_user.id)
    max_opened = max(int(user["max_lesson_opened"] or 1), lesson_number)
    completed = 1 if lesson_number == TOTAL_LESSONS and int(user["completed"] or 0) else int(user["completed"] or 0)
    update_user_state(update.effective_user.id, current_lesson=lesson_number, max_lesson_opened=max_opened, completed=completed)
    log_event(update.effective_user.id, "lesson_opened", lesson_number)
    await safe_edit_or_send(update, context, build_lesson_text(lesson_number), lesson_keyboard(lesson_number))


async def send_final(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = get_user(update.effective_user.id)
    if not user:
        upsert_user(update.effective_user.id, update.effective_user.username or "", update.effective_user.first_name or "")
        user = get_user(update.effective_user.id)
    if not int(user["completed"] or 0):
        update_user_state(update.effective_user.id, current_lesson=TOTAL_LESSONS, max_lesson_opened=TOTAL_LESSONS, completed=1)
        log_event(update.effective_user.id, "course_completed", TOTAL_LESSONS)
    await safe_edit_or_send(update, context, final_text(), final_keyboard())


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username or "", user.first_name or "")
    if update.message:
        await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU, disable_web_page_preview=True)
    log_event(user.id, "start")


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU, disable_web_page_preview=True)


async def progress_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username or "", user.first_name or "")
    row = get_user(user.id)
    if update.message:
        await update.message.reply_text(progress_text(row), parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU)


async def continue_course(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username or "", user.first_name or "")
    row = get_user(user.id)
    current_lesson = int(row["current_lesson"] or 1)
    completed = int(row["completed"] or 0)
    if completed and current_lesson >= TOTAL_LESSONS:
        await send_final(update, context)
    else:
        current_lesson = max(1, min(TOTAL_LESSONS, current_lesson))
        await send_lesson(update, context, current_lesson)


async def menu_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    if text == "🚀 Начать / продолжить курс":
        await continue_course(update, context)
    elif text == "📊 Мой прогресс":
        await progress_handler(update, context)
    elif text == "ℹ️ Помощь":
        await help_handler(update, context)
    else:
        await update.message.reply_text(
            "Используйте кнопки ниже, чтобы открыть курс, посмотреть прогресс или помощь.",
            reply_markup=MAIN_MENU,
        )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data or ""
    user = update.effective_user
    upsert_user(user.id, user.username or "", user.first_name or "")

    if data == "bonus:check":
        await query.answer("Проверяю подписку...")
        await send_bonus_gate_result(update, context)
        return

    await query.answer()

    if data.startswith("lesson:"):
        lesson_number = int(data.split(":", 1)[1])
        lesson_number = max(1, min(TOTAL_LESSONS, lesson_number))
        await send_lesson(update, context, lesson_number)
        return

    if data == "final":
        update_user_state(update.effective_user.id, test_opened_at=now_iso())
        await send_final(update, context)
        return


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла техническая ошибка. Попробуйте ещё раз через несколько секунд.",
            )
        except Exception:
            logger.exception("Failed to send error message to user")


# ---------------------------
# App bootstrap
# ---------------------------
def build_application() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    for lesson in LESSONS:
        logger.info(
            "Lesson %s URLs | text=%s | video=%s",
            lesson["number"],
            "set" if lesson["text_url"] else "empty",
            "set" if lesson["video_url"] else "empty",
        )
    logger.info("Bonus URLs | text=%s | video=%s", "set" if BONUS_TEXT_URL else "empty", "set" if BONUS_VIDEO_URL else "empty")
    logger.info("Community URL | %s", "set" if COMMUNITY_URL else "empty")
    logger.info("Channel URL | %s", "set" if CHANNEL_URL else "empty")
    logger.info("Bonus group chat for check | %s", BONUS_GROUP_CHAT if BONUS_GROUP_CHAT else "empty")
    logger.info("Bonus channel chat for check | %s", BONUS_CHANNEL_CHAT if BONUS_CHANNEL_CHAT else "empty")

    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, menu_text_handler))
    application.add_error_handler(error_handler)
    return application


async def post_init(application: Application) -> None:
    logger.info("Bot initialized")


def main() -> None:
    init_db()
    app = build_application()
    app.post_init = post_init

    if WEBHOOK_URL:
        webhook_path = f"/{WEBHOOK_PATH}"
        logger.info("Starting bot with webhook on %s%s", WEBHOOK_URL, webhook_path)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=f"{WEBHOOK_URL}{webhook_path}",
            secret_token=WEBHOOK_SECRET or None,
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )
    else:
        logger.info("Starting bot with polling")
        app.run_polling(allowed_updates=["message", "callback_query"], drop_pending_updates=True)


if __name__ == "__main__":
    main()
