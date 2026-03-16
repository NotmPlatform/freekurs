import html
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_GROUP_ID = int(os.getenv("ADMIN_GROUP_ID", "0"))
DB_PATH = os.getenv("DB_PATH", os.getenv("RAILWAY_VOLUME_MOUNT_PATH", ".") + "/web3_course.db")
MIN_LESSON_MINUTES = int(os.getenv("MIN_LESSON_MINUTES", "5"))
PROF_TEST_URL = os.getenv("PROF_TEST_URL", "https://t.me/Web3UPbot")
BONUS_TEXT_URL = os.getenv("BONUS_TEXT_URL", "")
BONUS_VIDEO_URL = os.getenv("BONUS_VIDEO_URL", "")
COMMUNITY_URL = os.getenv("COMMUNITY_URL", "")
CHANNEL_URL = os.getenv("CHANNEL_URL", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "webhook").strip("/") or "webhook"
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

LESSONS = [
    {
        "number": 1,
        "title": "Почему Web3 — это не только крипта",
        "subtitle": "После урока вы поймёте, почему Web3 — это отдельная цифровая индустрия, а не только монеты и трейдинг.",
        "text_url": os.getenv("LESSON_1_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_1_VIDEO_URL", ""),
        "bullets": ["Web1 → Web2 → Web3", "почему Web3 быстро растёт", "почему это новая индустрия"],
    },
    {
        "number": 2,
        "title": "Как появился Web3",
        "subtitle": "После урока вы увидите, какие проблемы решили Bitcoin и Ethereum и почему из этого вырос целый рынок.",
        "text_url": os.getenv("LESSON_2_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_2_VIDEO_URL", ""),
        "bullets": ["зачем появился Bitcoin", "какую проблему он решил", "что изменил Ethereum"],
    },
    {
        "number": 3,
        "title": "Как устроена индустрия Web3",
        "subtitle": "После урока у вас будет цельная карта рынка: от блокчейнов и бирж до DeFi, NFT и инфраструктуры.",
        "text_url": os.getenv("LESSON_3_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_3_VIDEO_URL", ""),
        "bullets": ["блокчейны", "биржи и DeFi", "NFT, GameFi и инфраструктура"],
    },
    {
        "number": 4,
        "title": "Кто работает в Web3",
        "subtitle": "После урока вы поймёте, что в Web3 растут не только разработчики, но и сильные нетехнические специалисты.",
        "text_url": os.getenv("LESSON_4_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_4_VIDEO_URL", ""),
        "bullets": ["Community", "Marketing", "BD и Support / Operations"],
    },
    {
        "number": 5,
        "title": "Как выбрать профессию в Web3",
        "subtitle": "После урока вы сможете сузить выбор и понять, в какую роль логичнее заходить именно вам.",
        "text_url": os.getenv("LESSON_5_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_5_VIDEO_URL", ""),
        "bullets": ["как увидеть свою сильную сторону", "куда проще зайти новичку", "как выбрать одну точку входа"],
    },
    {
        "number": 6,
        "title": "Как войти в Web3 с нуля",
        "subtitle": "После урока у вас будет простой и реалистичный маршрут входа без хаоса, перегруза и лишней теории.",
        "text_url": os.getenv("LESSON_6_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_6_VIDEO_URL", ""),
        "bullets": ["что изучать первым", "где искать возможности", "какие шаги сделать в ближайшие 7 дней"],
    },
    {
        "number": 7,
        "title": "Какая профессия в Web3 подходит вам",
        "subtitle": "После урока вы будете готовы выбрать своё направление и перейти к следующему шагу — тесту и бонусу.",
        "text_url": os.getenv("LESSON_7_TEXT_URL", ""),
        "video_url": os.getenv("LESSON_7_VIDEO_URL", ""),
        "bullets": ["карьерная диагностика", "выбор направления", "следующий шаг в индустрии"],
    },
]

WELCOME_TEXT = (
    "🚀 <b>Бесплатный курс — Введение в Web3</b>\n\n"
    "7 коротких уроков помогут спокойно разобраться в индустрии, увидеть реальные роли и выбрать свой следующий шаг без хаоса и перегруза.\n\n"
    "<b>Что вы получите:</b>\n"
    "• понятную карту Web3;\n"
    "• понимание ролей и точек входа;\n"
    "• тест на профессию и бонусный урок в финале.\n\n"
    "Нажмите <b>«🚀 Начать / продолжить курс»</b>, чтобы открыть текущий урок."
)

HELP_TEXT = (
    "ℹ️ <b>Как пользоваться ботом</b>\n\n"
    "1. Откройте урок в тексте или видео.\n"
    "2. Изучите материал и нажмите <b>«✅ Я изучил урок»</b>.\n"
    f"3. Если вы провели с уроком меньше <b>{MIN_LESSON_MINUTES} минут</b>, бот попросит коротко написать, какую мысль вы забрали из урока.\n"
    "4. После 7 урока вы получите тест и бонусный урок."
)

# =========================
# DB
# =========================


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn



def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row[1] == column_name for row in rows)



def add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    if not column_exists(conn, table_name, column_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")



def init_db() -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    current_lesson INTEGER DEFAULT 0,
                    pending_lesson INTEGER DEFAULT 0,
                    pending_input_type TEXT,
                    pending_next_step TEXT,
                    completed INTEGER DEFAULT 0,
                    final_prompt_shown_at TEXT,
                    bonus_opened_at TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_lessons (
                    user_id INTEGER NOT NULL,
                    lesson_number INTEGER NOT NULL,
                    started_at TEXT,
                    opened_text_at TEXT,
                    opened_video_at TEXT,
                    completed_at TEXT,
                    reflection_text TEXT,
                    question_text TEXT,
                    updated_at TEXT,
                    PRIMARY KEY (user_id, lesson_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_replies (
                    admin_message_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    lesson_number INTEGER NOT NULL,
                    user_text TEXT,
                    input_type TEXT,
                    created_at TEXT
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

            add_column_if_missing(conn, "users", "pending_lesson", "INTEGER DEFAULT 0")
            add_column_if_missing(conn, "users", "pending_input_type", "TEXT")
            add_column_if_missing(conn, "users", "pending_next_step", "TEXT")
            add_column_if_missing(conn, "users", "final_prompt_shown_at", "TEXT")
            add_column_if_missing(conn, "users", "bonus_opened_at", "TEXT")
            add_column_if_missing(conn, "user_lessons", "question_text", "TEXT")
            add_column_if_missing(conn, "admin_replies", "input_type", "TEXT")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_user_id ON analytics_events(user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_event_type ON analytics_events(event_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_events(created_at)")



def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        return None



def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()



def upsert_user(user_id: int, username: str, first_name: str) -> None:
    current_time = now_iso()
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    updated_at = excluded.updated_at
                """,
                (user_id, username, first_name, current_time, current_time),
            )



def update_user_state(
    user_id: int,
    *,
    current_lesson: Optional[int] = None,
    pending_lesson: Optional[int] = None,
    pending_input_type: Optional[str] = None,
    pending_next_step: Optional[str] = None,
    completed: Optional[int] = None,
    final_prompt_shown_at: Optional[str] = None,
    bonus_opened_at: Optional[str] = None,
) -> None:
    fields = []
    values = []

    if current_lesson is not None:
        fields.append("current_lesson = ?")
        values.append(current_lesson)
    if pending_lesson is not None:
        fields.append("pending_lesson = ?")
        values.append(pending_lesson)
    if pending_input_type is not None:
        fields.append("pending_input_type = ?")
        values.append(pending_input_type)
    if pending_next_step is not None:
        fields.append("pending_next_step = ?")
        values.append(pending_next_step)
    if completed is not None:
        fields.append("completed = ?")
        values.append(completed)
    if final_prompt_shown_at is not None:
        fields.append("final_prompt_shown_at = ?")
        values.append(final_prompt_shown_at)
    if bonus_opened_at is not None:
        fields.append("bonus_opened_at = ?")
        values.append(bonus_opened_at)

    if not fields:
        return

    fields.append("updated_at = ?")
    values.append(now_iso())
    values.append(user_id)

    with closing(get_connection()) as conn:
        with conn:
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE user_id = ?", values)



def clear_pending_state(user_id: int) -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                UPDATE users
                SET pending_lesson = 0,
                    pending_input_type = NULL,
                    pending_next_step = NULL,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (now_iso(), user_id),
            )



def ensure_lesson_row(user_id: int, lesson_number: int) -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO user_lessons (user_id, lesson_number, updated_at)
                VALUES (?, ?, ?)
                """,
                (user_id, lesson_number, now_iso()),
            )



def get_user_lesson(user_id: int, lesson_number: int) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM user_lessons WHERE user_id = ? AND lesson_number = ?",
            (user_id, lesson_number),
        ).fetchone()



def update_user_lesson(
    user_id: int,
    lesson_number: int,
    *,
    started_at: Optional[str] = None,
    opened_text_at: Optional[str] = None,
    opened_video_at: Optional[str] = None,
    completed_at: Optional[str] = None,
    reflection_text: Optional[str] = None,
    question_text: Optional[str] = None,
) -> None:
    ensure_lesson_row(user_id, lesson_number)
    fields = []
    values = []

    if started_at is not None:
        fields.append("started_at = ?")
        values.append(started_at)
    if opened_text_at is not None:
        fields.append("opened_text_at = ?")
        values.append(opened_text_at)
    if opened_video_at is not None:
        fields.append("opened_video_at = ?")
        values.append(opened_video_at)
    if completed_at is not None:
        fields.append("completed_at = ?")
        values.append(completed_at)
    if reflection_text is not None:
        fields.append("reflection_text = ?")
        values.append(reflection_text)
    if question_text is not None:
        fields.append("question_text = ?")
        values.append(question_text)

    if not fields:
        return

    fields.append("updated_at = ?")
    values.append(now_iso())
    values.extend([user_id, lesson_number])

    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                f"UPDATE user_lessons SET {', '.join(fields)} WHERE user_id = ? AND lesson_number = ?",
                values,
            )



def get_completed_lessons_count(user_id: int) -> int:
    with closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM user_lessons WHERE user_id = ? AND completed_at IS NOT NULL",
            (user_id,),
        ).fetchone()
    return int(row["cnt"] or 0)



def save_admin_mapping(admin_message_id: int, user_id: int, lesson_number: int, user_text: str, input_type: str) -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO admin_replies (admin_message_id, user_id, lesson_number, user_text, input_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (admin_message_id, user_id, lesson_number, user_text, input_type, now_iso()),
            )



def get_admin_mapping(admin_message_id: int) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM admin_replies WHERE admin_message_id = ?",
            (admin_message_id,),
        ).fetchone()



def log_event(user_id: int, event_type: str, lesson_number: Optional[int] = None, meta: str = "") -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute(
                """
                INSERT INTO analytics_events (user_id, event_type, lesson_number, meta, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, event_type, lesson_number, meta, now_iso()),
            )



def event_exists(user_id: int, event_type: str, lesson_number: Optional[int] = None) -> bool:
    query = "SELECT 1 FROM analytics_events WHERE user_id = ? AND event_type = ?"
    params = [user_id, event_type]
    if lesson_number is None:
        query += " AND lesson_number IS NULL LIMIT 1"
    else:
        query += " AND lesson_number = ? LIMIT 1"
        params.append(lesson_number)
    with closing(get_connection()) as conn:
        row = conn.execute(query, params).fetchone()
    return bool(row)


# =========================
# HELPERS
# =========================


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["🚀 Начать / продолжить курс"], ["📊 Мой прогресс", "ℹ️ Помощь"]],
        resize_keyboard=True,
    )



def format_duration(seconds: int) -> str:
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes} мин. {secs} сек."



def safe_username(user) -> str:
    return f"@{html.escape(user.username)}" if user.username else "не указан"



def lesson_was_opened(user_id: int, lesson_number: int) -> bool:
    lesson_row = get_user_lesson(user_id, lesson_number)
    if not lesson_row:
        return False
    return bool(lesson_row["opened_text_at"] or lesson_row["opened_video_at"])



def lesson_seconds_spent(user_id: int, lesson_number: int) -> int:
    lesson_row = get_user_lesson(user_id, lesson_number)
    if not lesson_row or not lesson_row["started_at"]:
        return 0
    started = parse_dt(lesson_row["started_at"])
    if not started:
        return 0
    return max(0, int((datetime.now(timezone.utc) - started).total_seconds()))



def can_open_next_without_reflection(user_id: int, lesson_number: int) -> bool:
    if not lesson_was_opened(user_id, lesson_number):
        return False
    return lesson_seconds_spent(user_id, lesson_number) >= MIN_LESSON_MINUTES * 60



def remaining_seconds_to_unlock(user_id: int, lesson_number: int) -> int:
    spent = lesson_seconds_spent(user_id, lesson_number)
    need = MIN_LESSON_MINUTES * 60
    return max(0, need - spent)



def lesson_status_lines(user_id: int, lesson_number: int) -> str:
    lesson_row = get_user_lesson(user_id, lesson_number)
    if not lesson_row:
        return ""

    lines = []
    if lesson_row["opened_text_at"]:
        lines.append("✅ Текст открыт")
    else:
        lines.append("▫️ Текст ещё не открыт")

    if lesson_row["opened_video_at"]:
        lines.append("✅ Видео открыто")
    else:
        lines.append("▫️ Видео ещё не открыто")

    if lesson_row["completed_at"]:
        lines.append("✅ Урок отмечен как изученный")
    elif lesson_row["started_at"]:
        lines.append(f"⏱ Прошло с первого открытия: {format_duration(lesson_seconds_spent(user_id, lesson_number))}")

    return "\n".join(lines)



def lesson_keyboard(lesson_number: int) -> InlineKeyboardMarkup:
    lesson = LESSONS[lesson_number - 1]
    buttons = []

    format_row = []
    if lesson["text_url"]:
        format_row.append(InlineKeyboardButton("📖 Текстовый урок", callback_data=f"open_text:{lesson_number}"))
    if lesson["video_url"]:
        format_row.append(InlineKeyboardButton("🎬 Видео урок", callback_data=f"open_video:{lesson_number}"))
    if format_row:
        buttons.append(format_row)

    buttons.append([InlineKeyboardButton("✅ Я изучил урок", callback_data=f"done:{lesson_number}")])
    buttons.append(
        [
            InlineKeyboardButton("✍️ Написать, что понял", callback_data=f"reflect:{lesson_number}:same"),
            InlineKeyboardButton("❓ Задать вопрос", callback_data=f"question:{lesson_number}"),
        ]
    )

    if lesson_number == 3 and COMMUNITY_URL:
        buttons.append([InlineKeyboardButton("💬 Вступить в сообщество 2026up", url=COMMUNITY_URL)])

    return InlineKeyboardMarkup(buttons)



def open_link_keyboard(label: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, url=url)]])



def final_step_keyboard() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton("🧭 Пройти тест на профессию", url=PROF_TEST_URL)]]
    buttons.append([InlineKeyboardButton("🎁 Я прошёл тест — открыть бонус", callback_data="open_bonus_gate")])
    if COMMUNITY_URL:
        buttons.append([InlineKeyboardButton("💬 Сообщество 2026up", url=COMMUNITY_URL)])
    if CHANNEL_URL:
        buttons.append([InlineKeyboardButton("📢 Основной канал", url=CHANNEL_URL)])
    return InlineKeyboardMarkup(buttons)



def bonus_gate_keyboard() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton("🧭 Открыть тест", url=PROF_TEST_URL)]]
    buttons.append([InlineKeyboardButton("✅ Я прошёл тест — открыть бонус", callback_data="open_bonus_confirmed")])
    return InlineKeyboardMarkup(buttons)



def bonus_keyboard() -> Optional[InlineKeyboardMarkup]:
    buttons = []
    if BONUS_TEXT_URL:
        buttons.append([InlineKeyboardButton("🎁 Бонусный урок", url=BONUS_TEXT_URL)])
    if BONUS_VIDEO_URL:
        buttons.append([InlineKeyboardButton("🎬 Бонусное видео", url=BONUS_VIDEO_URL)])
    if COMMUNITY_URL:
        buttons.append([InlineKeyboardButton("💬 Сообщество 2026up", url=COMMUNITY_URL)])
    if CHANNEL_URL:
        buttons.append([InlineKeyboardButton("📢 Основной канал", url=CHANNEL_URL)])
    return InlineKeyboardMarkup(buttons) if buttons else None



def progress_text(user: sqlite3.Row) -> str:
    user_id = int(user["user_id"])
    current_lesson = int(user["current_lesson"] or 0)
    completed = bool(user["completed"])
    completed_count = get_completed_lessons_count(user_id)

    if completed:
        bonus_opened = bool(user["bonus_opened_at"])
        bonus_line = "Бонус открыт ✅" if bonus_opened else "Бонус ещё не открыт."
        return (
            "✅ <b>Курс завершён.</b>\n\n"
            f"Вы прошли <b>{len(LESSONS)} из {len(LESSONS)}</b> уроков.\n"
            f"{bonus_line}\n\n"
            "Теперь пройдите тест на профессию и закрепите маршрут бонусным уроком."
        )

    if current_lesson <= 0:
        return "📍 Вы ещё не начали курс. Нажмите «🚀 Начать / продолжить курс» и откройте первый урок."

    status = lesson_status_lines(user_id, current_lesson)
    status_block = f"\n\n<b>Статус текущего урока:</b>\n{status}" if status else ""

    return (
        "📊 <b>Ваш прогресс</b>\n\n"
        f"Пройдено уроков: <b>{completed_count}/{len(LESSONS)}</b>\n"
        f"Текущий урок: <b>{current_lesson}/{len(LESSONS)}</b>\n"
        f"Тема: <b>{html.escape(LESSONS[current_lesson - 1]['title'])}</b>"
        f"{status_block}\n\n"
        "Откройте текущий урок и после изучения нажмите «✅ Я изучил урок»."
    )


async def safe_edit_or_reply(query, text: str, reply_markup=None) -> None:
    try:
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except BadRequest as exc:
        msg = str(exc).lower()
        if "message is not modified" in msg:
            return
        if "message can't be edited" in msg or "message to edit not found" in msg:
            await query.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return
        raise



def validate_env() -> None:
    for lesson in LESSONS:
        if not lesson["text_url"]:
            logger.warning("LESSON_%s_TEXT_URL не задан", lesson["number"])
        if not lesson["video_url"]:
            logger.warning("LESSON_%s_VIDEO_URL не задан", lesson["number"])

    if not BONUS_TEXT_URL:
        logger.warning("BONUS_TEXT_URL не задан")
    if not BONUS_VIDEO_URL:
        logger.warning("BONUS_VIDEO_URL не задан")
    if not ADMIN_GROUP_ID:
        logger.warning("ADMIN_GROUP_ID не задан. Вопросы и рефлексии не будут отправляться в группу.")


# =========================
# CORE FLOW
# =========================


async def send_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE, lesson_number: int) -> None:
    if lesson_number < 1 or lesson_number > len(LESSONS):
        return

    lesson = LESSONS[lesson_number - 1]
    user = update.effective_user
    if not user:
        return

    upsert_user(user.id, user.username or "", user.first_name or "")
    ensure_lesson_row(user.id, lesson_number)
    update_user_state(
        user.id,
        current_lesson=lesson_number,
        pending_lesson=0,
        pending_input_type="",
        pending_next_step="",
        completed=0,
    )

    bullets = "\n".join(f"• {html.escape(item)}" for item in lesson["bullets"])
    status = lesson_status_lines(user.id, lesson_number)
    status_block = f"\n\n<b>Статус:</b>\n{status}" if status else ""

    format_hint = "Выберите удобный формат: текст или видео." if lesson["text_url"] or lesson["video_url"] else "Материалы урока скоро будут добавлены."

    text = (
        f"<b>Урок {lesson_number} из {len(LESSONS)}</b>\n"
        f"<b>{html.escape(lesson['title'])}</b>\n\n"
        f"{html.escape(lesson['subtitle'])}\n\n"
        f"<b>Что внутри:</b>\n{bullets}\n\n"
        f"{format_hint} После изучения нажмите <b>«✅ Я изучил урок»</b>.\n\n"
        f"<i>Рекомендуемое время на урок: от {MIN_LESSON_MINUTES} минут.</i>"
        f"{status_block}"
    )

    if lesson_number == 3 and COMMUNITY_URL:
        text += "\n\n💬 <b>Хотите не только проходить уроки, но и быть в среде?</b> Вступайте в 2026up."

    if update.callback_query:
        await safe_edit_or_reply(update.callback_query, text, lesson_keyboard(lesson_number))
    else:
        await update.effective_message.reply_text(
            text,
            reply_markup=lesson_keyboard(lesson_number),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


async def send_final_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    upsert_user(user.id, user.username or "", user.first_name or "")
    db_user = get_user(user.id)
    already_completed = bool(db_user and db_user["completed"])

    update_user_state(
        user.id,
        completed=1,
        pending_lesson=0,
        pending_input_type="",
        pending_next_step="",
        current_lesson=len(LESSONS),
        final_prompt_shown_at=(db_user["final_prompt_shown_at"] if db_user and db_user["final_prompt_shown_at"] else now_iso()),
    )

    if not already_completed and not event_exists(user.id, "course_finished", len(LESSONS)):
        log_event(user.id, "course_finished", len(LESSONS))

    if not event_exists(user.id, "test_prompt_shown"):
        log_event(user.id, "test_prompt_shown")

    text = (
        "🏁 <b>Отличная работа — вы завершили вводный блок.</b>\n\n"
        "Теперь у вас уже есть база: вы понимаете, как устроен Web3, какие роли в нём есть и куда логично двигаться дальше.\n\n"
        "<b>Шаг 1:</b> пройдите короткий тест на профессию в Web3.\n"
        "<b>Шаг 2:</b> после теста откройте бонусный урок и зафиксируйте своё направление."
    )

    if update.callback_query:
        await safe_edit_or_reply(update.callback_query, text, final_step_keyboard())
    else:
        await update.effective_message.reply_text(text, reply_markup=final_step_keyboard(), parse_mode=ParseMode.HTML)


async def send_bonus_gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user and not event_exists(user.id, "bonus_gate_shown"):
        log_event(user.id, "bonus_gate_shown")

    text = (
        "🎁 <b>Бонусный урок открывается после теста.</b>\n\n"
        "Сначала пройдите короткий тест на профессию в Web3, чтобы увидеть своё направление. После этого откройте бонус и зафиксируйте следующий шаг."
    )
    if update.callback_query:
        await safe_edit_or_reply(update.callback_query, text, bonus_gate_keyboard())
    else:
        await update.effective_message.reply_text(text, reply_markup=bonus_gate_keyboard(), parse_mode=ParseMode.HTML)


async def send_bonus_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user and not event_exists(user.id, "bonus_opened"):
        log_event(user.id, "bonus_opened")
        update_user_state(user.id, bonus_opened_at=now_iso())

    markup = bonus_keyboard()
    if markup is None:
        text = (
            "🎁 <b>Бонусный урок скоро будет добавлен.</b>\n\n"
            "Тест уже доступен. Как только ссылки на бонус появятся, этот блок сразу заработает."
        )
    else:
        text = (
            "🎁 <b>Бонусный урок</b>\n\n"
            "Здесь вы закрепите выбор направления и увидите, как превратить интерес к Web3 в понятный профессиональный маршрут.\n\n"
            "Откройте текстовую или видео-версию бонуса ниже."
        )

    if update.callback_query:
        await safe_edit_or_reply(update.callback_query, text, markup)
    else:
        await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)


async def ask_reflection(update: Update, context: ContextTypes.DEFAULT_TYPE, lesson_number: int, next_step: str) -> None:
    user = update.effective_user
    if not user:
        return

    upsert_user(user.id, user.username or "", user.first_name or "")
    update_user_state(
        user.id,
        pending_lesson=lesson_number,
        pending_input_type="reflection",
        pending_next_step=next_step,
    )

    spent = lesson_seconds_spent(user.id, lesson_number)
    remaining = remaining_seconds_to_unlock(user.id, lesson_number)

    text = (
        f"⏱ Вы уделили уроку <b>{format_duration(spent)}</b>.\n"
        f"Для автоматического перехода нужно хотя бы <b>{MIN_LESSON_MINUTES} минут</b>.\n"
        f"Осталось: <b>{format_duration(remaining)}</b>.\n\n"
        "Чтобы перейти дальше сразу, коротко напишите, какую мысль вы забрали из урока."
    )

    if update.callback_query:
        await safe_edit_or_reply(update.callback_query, text)
    else:
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


async def ask_question(update: Update, context: ContextTypes.DEFAULT_TYPE, lesson_number: int) -> None:
    user = update.effective_user
    if not user:
        return

    upsert_user(user.id, user.username or "", user.first_name or "")
    update_user_state(
        user.id,
        pending_lesson=lesson_number,
        pending_input_type="question",
        pending_next_step="same",
    )

    text = (
        "❓ <b>Напишите ваш вопрос по уроку</b>\n\n"
        "Отправьте его одним сообщением. Бот перешлёт вопрос в группу кураторов, и админ сможет ответить вам прямо в этом чате."
    )

    if update.callback_query:
        await safe_edit_or_reply(update.callback_query, text)
    else:
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


async def send_lesson_link(query, label: str, url: str, lesson_number: int) -> None:
    await query.message.reply_text(
        (
            f"{label} для урока <b>{lesson_number}</b>.\n"
            "После изучения вернитесь к карточке урока и нажмите <b>«✅ Я изучил урок»</b>."
        ),
        reply_markup=open_link_keyboard(label, url),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# =========================
# HANDLERS
# =========================


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    upsert_user(user.id, user.username or "", user.first_name or "")
    await update.effective_message.reply_text(
        WELCOME_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
        disable_web_page_preview=True,
    )
    if not event_exists(user.id, "start"):
        log_event(user.id, "start")


async def progress_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    upsert_user(user.id, user.username or "", user.first_name or "")
    db_user = get_user(user.id)
    if not db_user:
        await update.effective_message.reply_text(
            "Пока нет прогресса. Нажмите «🚀 Начать / продолжить курс».",
            reply_markup=main_menu_keyboard(),
        )
        return

    await update.effective_message.reply_text(
        progress_text(db_user),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        HELP_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )


async def text_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return

    text = (update.effective_message.text or "").strip()
    if text == "🚀 Начать / продолжить курс":
        user = update.effective_user
        if not user:
            return

        upsert_user(user.id, user.username or "", user.first_name or "")
        db_user = get_user(user.id)
        if not db_user or int(db_user["current_lesson"] or 0) == 0:
            await send_lesson(update, context, 1)
            raise ApplicationHandlerStop

        if bool(db_user["completed"]):
            await send_final_step(update, context)
            raise ApplicationHandlerStop

        await send_lesson(update, context, int(db_user["current_lesson"]))
        raise ApplicationHandlerStop

    if text == "📊 Мой прогресс":
        await progress_handler(update, context)
        raise ApplicationHandlerStop

    if text == "ℹ️ Помощь":
        await help_handler(update, context)
        raise ApplicationHandlerStop


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    user = update.effective_user
    if not user:
        await query.answer()
        return

    await query.answer()
    upsert_user(user.id, user.username or "", user.first_name or "")
    db_user = get_user(user.id)
    data = query.data or ""

    if data == "open_bonus_gate":
        await send_bonus_gate(update, context)
        return

    if data == "open_bonus_confirmed":
        await send_bonus_step(update, context)
        return

    if data.startswith("open_text:") or data.startswith("open_video:"):
        action, lesson_raw = data.split(":", 1)
        lesson_number = int(lesson_raw)
        if not db_user:
            await send_lesson(update, context, 1)
            return

        current_lesson = int(db_user["current_lesson"] or 0)
        if current_lesson != lesson_number:
            await query.answer("Это неактуальная карточка урока. Откройте текущий урок через меню.", show_alert=True)
            return

        lesson = LESSONS[lesson_number - 1]
        url = lesson["text_url"] if action == "open_text" else lesson["video_url"]
        label = "📖 Открыть текстовый урок" if action == "open_text" else "🎬 Открыть видео урок"
        if not url:
            await query.answer("Ссылка для этого формата ещё не добавлена.", show_alert=True)
            return

        lesson_row = get_user_lesson(user.id, lesson_number)
        started_at = lesson_row["started_at"] if lesson_row else None
        started_now = False
        if not started_at:
            started_at = now_iso()
            started_now = True

        if action == "open_text":
            update_user_lesson(user.id, lesson_number, started_at=started_at, opened_text_at=now_iso())
            log_event(user.id, "open_text", lesson_number, "first_open" if started_now else "repeat_open")
        else:
            update_user_lesson(user.id, lesson_number, started_at=started_at, opened_video_at=now_iso())
            log_event(user.id, "open_video", lesson_number, "first_open" if started_now else "repeat_open")

        await send_lesson_link(query, label, url, lesson_number)
        await send_lesson(update, context, lesson_number)
        return

    if data.startswith("question:"):
        lesson_number = int(data.split(":", 1)[1])
        if not db_user or int(db_user["current_lesson"] or 0) != lesson_number:
            await query.answer("Сначала откройте ваш текущий урок.", show_alert=True)
            return
        await ask_question(update, context, lesson_number)
        return

    if data.startswith("reflect:"):
        _, lesson_number_raw, next_step = data.split(":", 2)
        lesson_number = int(lesson_number_raw)
        if not db_user or int(db_user["current_lesson"] or 0) != lesson_number:
            await query.answer("Сначала откройте ваш текущий урок.", show_alert=True)
            return
        await ask_reflection(update, context, lesson_number, next_step)
        return

    if data.startswith("done:"):
        lesson_number = int(data.split(":", 1)[1])

        if not db_user:
            await send_lesson(update, context, 1)
            raise ApplicationHandlerStop

        current_lesson = int(db_user["current_lesson"] or 0)
        if current_lesson != lesson_number:
            await query.answer("Сначала откройте ваш текущий урок.", show_alert=True)
            return

        lesson_row = get_user_lesson(user.id, lesson_number)
        if lesson_row and lesson_row["completed_at"]:
            await query.answer("Этот урок уже отмечен как изученный.", show_alert=True)
            return

        if not lesson_was_opened(user.id, lesson_number):
            await query.answer("Сначала откройте текстовый или видео-урок.", show_alert=True)
            return

        if can_open_next_without_reflection(user.id, lesson_number):
            update_user_lesson(user.id, lesson_number, completed_at=now_iso())
            if not event_exists(user.id, "lesson_completed", lesson_number):
                log_event(user.id, "lesson_completed", lesson_number, "without_reflection")

            if lesson_number >= len(LESSONS):
                await query.message.reply_text("Отлично, вы завершили урок. Переходим к финальному шагу.")
                await send_final_step(update, context)
            else:
                await query.message.reply_text("Отлично, двигаемся дальше. Открываю следующий урок.")
                await send_lesson(update, context, lesson_number + 1)
        else:
            next_step = "finish" if lesson_number >= len(LESSONS) else "next"
            await ask_reflection(update, context, lesson_number, next_step)
        return


async def input_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return

    # Admin replies in moderation group
    if ADMIN_GROUP_ID and message.chat_id == ADMIN_GROUP_ID:
        if not message.reply_to_message:
            return

        mapping = get_admin_mapping(message.reply_to_message.message_id)
        if not mapping:
            return

        reply_text = (message.text or message.caption or "").strip()
        if not reply_text:
            return

        try:
            header = "Ответ куратора" if mapping["input_type"] == "question" else f"Ответ куратора по уроку {mapping['lesson_number']}"
            await context.bot.send_message(
                chat_id=int(mapping["user_id"]),
                text=f"💬 <b>{header}</b>\n\n{html.escape(reply_text)}",
                parse_mode=ParseMode.HTML,
            )
            await message.reply_text("Ответ отправлен пользователю ✅")
            raise ApplicationHandlerStop
        except Exception as exc:
            logger.exception("Не удалось отправить ответ пользователю: %s", exc)
            await message.reply_text("Не удалось отправить ответ пользователю.")
            raise ApplicationHandlerStop
        return

    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return

    db_user = get_user(user.id)
    if not db_user:
        return

    pending_lesson = int(db_user["pending_lesson"] or 0)
    pending_input_type = (db_user["pending_input_type"] or "").strip()
    pending_next_step = (db_user["pending_next_step"] or "").strip() or "same"

    if pending_lesson <= 0 or not pending_input_type:
        return

    user_text = (message.text or message.caption or "").strip()
    if len(user_text) < 5:
        await message.reply_text("Напишите чуть подробнее: хотя бы 1–2 короткие мысли или один полноценный вопрос.")
        return

    user_text_escaped = html.escape(user_text)
    full_name = html.escape(user.full_name or "")
    username_line = safe_username(user)

    admin_title = "Вопрос по уроку" if pending_input_type == "question" else "Рефлексия по уроку"
    body_label = "Вопрос" if pending_input_type == "question" else "Что понял"

    if pending_input_type == "question":
        update_user_lesson(user.id, pending_lesson, question_text=user_text)
        log_event(user.id, "question_sent", pending_lesson)
    else:
        update_user_lesson(
            user.id,
            pending_lesson,
            reflection_text=user_text,
            completed_at=now_iso() if pending_next_step != "same" else None,
        )
        if not event_exists(user.id, "lesson_completed", pending_lesson) and pending_next_step != "same":
            log_event(user.id, "lesson_completed", pending_lesson, "with_reflection")
        log_event(user.id, "reflection_sent", pending_lesson)

    admin_delivery_ok = True
    if ADMIN_GROUP_ID:
        admin_text = (
            f"📝 <b>{admin_title} {pending_lesson}</b>\n\n"
            f"<b>Пользователь:</b> {full_name}\n"
            f"<b>Username:</b> {username_line}\n"
            f"<b>User ID:</b> <code>{user.id}</code>\n"
            f"<b>Тема урока:</b> {html.escape(LESSONS[pending_lesson - 1]['title'])}\n\n"
            f"<b>{body_label}:</b>\n{user_text_escaped}\n\n"
            "Ответьте на это сообщение реплаем, и бот перешлёт ответ пользователю."
        )
        try:
            admin_sent_message = await context.bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=admin_text,
                parse_mode=ParseMode.HTML,
            )
            save_admin_mapping(admin_sent_message.message_id, user.id, pending_lesson, user_text, pending_input_type)
        except Exception as exc:
            admin_delivery_ok = False
            logger.exception("Не удалось отправить сообщение в группу админов: %s", exc)

    if pending_input_type == "question" and ADMIN_GROUP_ID and not admin_delivery_ok:
        await message.reply_text(
            "Сейчас временно не удалось отправить вопрос кураторам. Попробуйте ещё раз чуть позже — ваш вопрос не потерян.",
            reply_markup=main_menu_keyboard(),
        )
        raise ApplicationHandlerStop

    clear_pending_state(user.id)

    if pending_input_type == "question":
        await message.reply_text("Вопрос отправлен кураторам ✅\nВозвращаю вас к текущему уроку.", reply_markup=main_menu_keyboard())
        await send_lesson(update, context, pending_lesson)
        raise ApplicationHandlerStop

    if pending_next_step == "finish":
        note = "\nКураторам пока не удалось отправить ваш ответ, но прогресс сохранён." if ADMIN_GROUP_ID and not admin_delivery_ok else ""
        await message.reply_text(f"Спасибо, ответ принят ✅\nОткрываю финальный шаг курса.{note}", reply_markup=main_menu_keyboard())
        await send_final_step(update, context)
        raise ApplicationHandlerStop

    if pending_next_step == "same":
        note = "\nКураторам пока не удалось отправить ваш ответ, но он сохранён." if ADMIN_GROUP_ID and not admin_delivery_ok else ""
        await message.reply_text(f"Спасибо, ответ принят ✅\nВозвращаю вас к текущему уроку.{note}", reply_markup=main_menu_keyboard())
        await send_lesson(update, context, pending_lesson)
        raise ApplicationHandlerStop

    next_lesson = min(pending_lesson + 1, len(LESSONS))
    note = "\nКураторам пока не удалось отправить ваш ответ, но прогресс сохранён." if ADMIN_GROUP_ID and not admin_delivery_ok else ""
    await message.reply_text(f"Спасибо, ответ принят ✅\nОткрываю следующий урок.{note}", reply_markup=main_menu_keyboard())
    await send_lesson(update, context, next_lesson)
    raise ApplicationHandlerStop


async def unknown_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return
    await update.effective_message.reply_text(
        "Используйте кнопки меню внизу: «🚀 Начать / продолжить курс», «📊 Мой прогресс» или «ℹ️ Помощь».",
        reply_markup=main_menu_keyboard(),
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat and update.effective_chat.type == ChatType.PRIVATE:
            if update.effective_message:
                await update.effective_message.reply_text(
                    "Произошла техническая ошибка. Попробуйте ещё раз через несколько секунд.",
                    reply_markup=main_menu_keyboard(),
                )
    except Exception:
        logger.exception("Failed to notify user about error")


async def post_init(application: Application) -> None:
    logger.info("Бот запущен. База: %s", DB_PATH)
    validate_env()


# =========================
# RUN
# =========================


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в переменных среды")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("progress", progress_handler))
    app.add_handler(CommandHandler("help", help_handler))

    app.add_handler(CallbackQueryHandler(callback_handler))

    text_or_caption = (filters.TEXT | filters.CAPTION) & ~filters.COMMAND
    app.add_handler(MessageHandler(text_or_caption, input_message_handler), group=0)
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, text_menu_handler),
        group=1,
    )
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & text_or_caption, unknown_handler),
        group=2,
    )

    app.add_error_handler(error_handler)

    allowed_updates = ["message", "callback_query"]

    if WEBHOOK_URL:
        logger.info("Webhook started on port %s", PORT)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=f"{WEBHOOK_URL}/{WEBHOOK_PATH}",
            secret_token=WEBHOOK_SECRET or None,
            allowed_updates=allowed_updates,
        )
    else:
        logger.info("Polling started")
        app.run_polling(allowed_updates=allowed_updates)


if __name__ == "__main__":
    main()