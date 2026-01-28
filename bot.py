import os
import time
import random
import io
import csv
from dataclasses import dataclass
from typing import List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==========================================================
# =================== 1) НАСТРОЙКА (Railway Variables) =====
# ==========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")            # ОБЯЗАТЕЛЬНО в Railway Variables
DATABASE_URL = os.environ.get("DATABASE_URL")      # ДОЛЖНА появиться после добавления Postgres
ADMIN_IDS_RAW = os.environ.get("ADMIN_IDS", "")    # например: "123456789" или "1,2,3"

# Превращаем "1,2,3" -> set({1,2,3})
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}

# ==========================================================
# =================== 2) НАСТРОЙКА ИГРЫ ====================
# ==========================================================
QUIZ_SIZE = 20
WRONG_PENALTY_MS = 5000  # +5 секунд за ошибку

# ==========================================================
# =================== 3) ТЕОРИЯ (вставляешь сюда) ==========
# ==========================================================
THEORY_TEXT = """
ВСТАВЬ_СЮДА_ОБЩУЮ_ТЕОРИЮ_ОДНИМ_ТЕКСТОМ.

• Можно списки
• Можно абзацы

Бот сам разобьёт на страницы и будет листать.
"""

# ==========================================================
# =================== 4) ВОПРОСЫ (вставляешь сюда) =========
# Здесь же:
# - hint_wrong: подсказка при неверном ответе
# - explain_right: пояснение при верном ответе
# ==========================================================
@dataclass
class Question:
    text: str
    options: List[str]          # 4 варианта
    correct: int                # индекс 0..3
    hint_wrong: str             # подсказка если ошибка
    explain_right: str          # объяснение если верно
    photo_path: Optional[str] = None  # "assets/q1.png" (опционально)

QUESTIONS: List[Question] = [
    Question(
        text="Сколько будет 7 × 8?",
        options=["54", "56", "58", "64"],
        correct=1,
        hint_wrong="Вспомни: 7×7=49, значит 7×8 на 7 больше.",
        explain_right="7×8 = 56 (таблица умножения).",
    ),
    Question(
        text="Какая планета ближе всего к Солнцу?",
        options=["Венера", "Марс", "Меркурий", "Юпитер"],
        correct=2,
        hint_wrong="Порядок от Солнца начинается с Меркурия.",
        explain_right="Ближе всего к Солнцу — Меркурий.",
    ),
    # ⚠️ Добавь ещё вопросы, чтобы их было >= QUIZ_SIZE
]

# ==========================================================
# =================== 5) УТИЛИТЫ ===========================
# ==========================================================
def now_ts() -> int:
    return int(time.time())

def fmt_ms(ms: int) -> str:
    sec = ms / 1000.0
    m = int(sec // 60)
    s = sec - 60 * m
    return f"{m}:{s:06.3f}"

def is_admin(update: Update) -> bool:
    u = update.effective_user
    return bool(u and u.id in ADMIN_IDS)

def ensure_ready():
    # Печатаем в логи Railway, чтобы было видно причину
    print("BOOT: starting...")
    print("BOOT: BOT_TOKEN set:", bool(BOT_TOKEN))
    print("BOOT: DATABASE_URL set:", bool(DATABASE_URL))
    print("BOOT: ADMIN_IDS:", ADMIN_IDS)

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Добавь BOT_TOKEN в Railway → Service (бот) → Variables.")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан. Добавь Postgres и/или прокинь DATABASE_URL в Variables сервиса бота.")
    if len(QUESTIONS) < QUIZ_SIZE:
        raise RuntimeError(f"Недостаточно вопросов: {len(QUESTIONS)}. Нужно минимум QUIZ_SIZE={QUIZ_SIZE}.")

def chunk_text(text: str, max_chars: int = 900) -> List[str]:
    s = (text or "").strip()
    if not s:
        return ["(Теория пока не заполнена)"]

    paragraphs = [p.strip() for p in s.split("\n\n") if p.strip()]
    pages: List[str] = []
    buf = ""

    for p in paragraphs:
        candidate = (buf + "\n\n" + p).strip() if buf else p
        if len(candidate) <= max_chars:
            buf = candidate
        else:
            if buf:
                pages.append(buf)
                buf = ""
            while len(p) > max_chars:
                pages.append(p[:max_chars])
                p = p[max_chars:]
            buf = p

    if buf:
        pages.append(buf)
    return pages

def build_quiz_order() -> List[int]:
    idx = list(range(len(QUESTIONS)))
    random.shuffle(idx)
    return idx[:QUIZ_SIZE]

def total_time_ms(context: ContextTypes.DEFAULT_TYPE) -> int:
    t0 = float(context.user_data.get("t0", time.time()))
    penalty = int(context.user_data.get("penalty_ms", 0))
    base = int((time.time() - t0) * 1000)
    return base + penalty

# ==========================================================
# =================== 6) POSTGRES: подключение =============
# ==========================================================
def db_connect():
    # dict_row, чтобы удобно читать по именам колонок
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def db_init():
    with db_connect() as con, con.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                first_seen_ts BIGINT NOT NULL,
                last_seen_ts BIGINT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS attempts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                started_ts BIGINT NOT NULL,
                ended_ts BIGINT,
                status TEXT NOT NULL,              -- started|finished|quit
                quiz_size INT NOT NULL,
                wrong_penalty_ms INT NOT NULL,
                wrong_count INT NOT NULL DEFAULT 0,
                penalty_ms INT NOT NULL DEFAULT 0,
                elapsed_ms INT,
                total_ms INT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS answers (
                id BIGSERIAL PRIMARY KEY,
                attempt_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                ts BIGINT NOT NULL,
                pos INT NOT NULL,
                question_index INT NOT NULL,
                option_index INT NOT NULL,
                is_correct BOOLEAN NOT NULL,
                penalty_ms_after INT NOT NULL,
                total_ms_now INT NOT NULL
            )
        """)
        con.commit()

def upsert_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    ts = now_ts()
    with db_connect() as con, con.cursor() as cur:
        cur.execute("SELECT user_id FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO users(user_id, username, full_name, first_seen_ts, last_seen_ts) VALUES(%s,%s,%s,%s,%s)",
                (user_id, username, full_name, ts, ts),
            )
        else:
            cur.execute(
                "UPDATE users SET username=%s, full_name=%s, last_seen_ts=%s WHERE user_id=%s",
                (username, full_name, ts, user_id),
            )
        con.commit()

def log_event(user_id: int, event_type: str, payload_json: Optional[str] = None) -> None:
    with db_connect() as con, con.cursor() as cur:
        cur.execute(
            "INSERT INTO events(ts, user_id, event_type, payload_json) VALUES(%s,%s,%s,%s)",
            (now_ts(), user_id, event_type, payload_json),
        )
        con.commit()

def attempt_start(user_id: int) -> int:
    with db_connect() as con, con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO attempts(user_id, started_ts, status, quiz_size, wrong_penalty_ms)
            VALUES(%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (user_id, now_ts(), "started", QUIZ_SIZE, WRONG_PENALTY_MS),
        )
        attempt_id = int(cur.fetchone()["id"])
        con.commit()
        return attempt_id

def attempt_update_progress(attempt_id: int, wrong_count: int, penalty_ms: int) -> None:
    with db_connect() as con, con.cursor() as cur:
        cur.execute(
            "UPDATE attempts SET wrong_count=%s, penalty_ms=%s WHERE id=%s",
            (wrong_count, penalty_ms, attempt_id),
        )
        con.commit()

def attempt_finish(attempt_id: int, status: str, elapsed_ms: int, penalty_ms: int, wrong_count: int) -> None:
    total = elapsed_ms + penalty_ms
    with db_connect() as con, con.cursor() as cur:
        cur.execute(
            """
            UPDATE attempts
            SET ended_ts=%s, status=%s, elapsed_ms=%s, penalty_ms=%s, wrong_count=%s, total_ms=%s
            WHERE id=%s
            """,
            (now_ts(), status, elapsed_ms, penalty_ms, wrong_count, total, attempt_id),
        )
        con.commit()

def log_answer(
    attempt_id: int,
    user_id: int,
    pos: int,
    question_index: int,
    option_index: int,
    is_correct: bool,
    penalty_ms_after: int,
    total_ms_now: int,
) -> None:
    with db_connect() as con, con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO answers(attempt_id, user_id, ts, pos, question_index, option_index, is_correct, penalty_ms_after, total_ms_now)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (attempt_id, user_id, now_ts(), pos, question_index, option_index, is_correct, penalty_ms_after, total_ms_now),
        )
        con.commit()

def db_clear_all() -> None:
    """Полная очистка статистики (только для админа, с подтверждением)."""
    with db_connect() as con, con.cursor() as cur:
        cur.execute("TRUNCATE TABLE answers RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE attempts RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE events RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE users RESTART IDENTITY")
        con.commit()
# ==========================================================
# =================== 7) КНОПКИ (UI) =======================
# ==========================================================
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Начать тест", callback_data="start_quiz")],
        [InlineKeyboardButton("📚 Теория", callback_data="theory:0")],
        [InlineKeyboardButton("🏆 Лидеры", callback_data="leaderboard")],
        [InlineKeyboardButton("ℹ️ Как играть", callback_data="help")],
    ])

def theory_kb(page: int, total: int) -> InlineKeyboardMarkup:
    prev_btn = InlineKeyboardButton("⬅️", callback_data=f"theory:{page-1}") if page > 0 else InlineKeyboardButton(" ", callback_data="noop")
    next_btn = InlineKeyboardButton("➡️", callback_data=f"theory:{page+1}") if page < total - 1 else InlineKeyboardButton(" ", callback_data="noop")
    return InlineKeyboardMarkup([
        [prev_btn, InlineKeyboardButton("🏠 Меню", callback_data="menu"), next_btn],
        [InlineKeyboardButton("▶️ Начать тест", callback_data="start_quiz")],
    ])

def quiz_kb(current_q_index: int, options: List[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(opt, callback_data=f"ans:{current_q_index}:{i}")] for i, opt in enumerate(options)]
    rows.append([InlineKeyboardButton("🏳️ Сдаться", callback_data="quit"), InlineKeyboardButton("🏠 Меню", callback_data="menu")])
    rows.append([InlineKeyboardButton("🏆 Лидеры", callback_data="leaderboard")])
    return InlineKeyboardMarkup(rows)

def finish_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Пройти ещё раз", callback_data="start_quiz")],
        [InlineKeyboardButton("🏆 Лидеры", callback_data="leaderboard")],
        [InlineKeyboardButton("📚 Теория", callback_data="theory:0")],
        [InlineKeyboardButton("🏠 Меню", callback_data="menu")],
    ])

def stats_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Сводка", callback_data="stats:overview")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="stats:users")],
        [InlineKeyboardButton("🧪 Попытки", callback_data="stats:attempts")],
        [InlineKeyboardButton("🧩 Сложные вопросы", callback_data="stats:hard")],
        [InlineKeyboardButton("🧾 События", callback_data="stats:events")],
        [InlineKeyboardButton("⬇️ Экспорт CSV", callback_data="stats:export")],
        [InlineKeyboardButton("📌 Очистить статистику", callback_data="stats:clear_confirm")],
        [InlineKeyboardButton("🏠 Меню", callback_data="menu")],
    ])

def clear_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, очистить", callback_data="stats:clear_yes")],
        [InlineKeyboardButton("❌ Отмена", callback_data="stats:clear_no")],
    ])

# ==========================================================
# =================== 8) Отправка сообщений =================
# ==========================================================
async def send(update: Update, text: str, reply_markup=None):
    if update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")

async def send_photo(update: Update, path: str, caption: str, reply_markup=None):
    with open(path, "rb") as f:
        if update.callback_query:
            await update.callback_query.message.reply_photo(photo=f, caption=caption, reply_markup=reply_markup, parse_mode="Markdown")
        else:
            await update.message.reply_photo(photo=f, caption=caption, reply_markup=reply_markup, parse_mode="Markdown")

def user_display(u) -> Tuple[int, Optional[str], Optional[str]]:
    return int(u.id), u.username, u.full_name

# ==========================================================
# =================== 9) ЭКРАНЫ (меню/теория/...) ==========
# ==========================================================
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "menu_open")

    await send(
        update,
        "👋 Привет!\n\n"
        f"🎯 Тест: *{QUIZ_SIZE}* вопросов (случайный порядок)\n"
        f"⏱ Штраф за ошибку: *+{WRONG_PENALTY_MS/1000:.0f}с*\n"
        "📚 Перед тестом можно почитать теорию.\n\n"
        "Выбирай:",
        reply_markup=main_menu_kb(),
    )

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "help_open")

    await send(
        update,
        "ℹ️ *Как играть*\n\n"
        "1) Нажми ▶️ *Начать тест*\n"
        "2) Отвечай кнопками\n"
        f"3) Неверно — штраф *+{WRONG_PENALTY_MS/1000:.0f}с* и пробуешь снова\n"
        "4) Верно — следующий вопрос\n"
        "5) В конце — итоговое время и статистика\n",
        reply_markup=main_menu_kb(),
    )

async def show_theory(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "theory_open", payload_json=f'{{"page":{page}}}')

    pages = chunk_text(THEORY_TEXT)
    page = max(0, min(page, len(pages) - 1))
    await send(
        update,
        f"📚 *Теория* ({page+1}/{len(pages)})\n\n{pages[page]}",
        reply_markup=theory_kb(page, len(pages)),
    )

def leaderboard_top(limit: int = 10) -> List[Tuple[str, int]]:
    # лучший total_ms по пользователю среди finished
    with db_connect() as con, con.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                   MIN(a.total_ms) AS best_total
            FROM attempts a
            JOIN users u ON u.user_id = a.user_id
            WHERE a.status='finished' AND a.total_ms IS NOT NULL
            GROUP BY a.user_id, name
            ORDER BY best_total ASC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    return [(r["name"], int(r["best_total"])) for r in rows]

async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "leaderboard_open")

    rows = leaderboard_top(10)
    if not rows:
        await send(update, "Пока нет результатов. Нажми ▶️ Начать тест.", reply_markup=main_menu_kb())
        return

    lines = ["🏆 *Лидеры* (лучшее итоговое время):"]
    for i, (name, ms) in enumerate(rows, 1):
        lines.append(f"{i}. {name} — *{fmt_ms(ms)}*")
    await send(update, "\n".join(lines), reply_markup=main_menu_kb())

# ==========================================================
# =================== 10) ТЕСТ (start/question/finish) =====
# ==========================================================
async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "quiz_start_clicked")

    # состояние попытки
    context.user_data["order"] = build_quiz_order()
    context.user_data["pos"] = 0
    context.user_data["t0"] = time.time()
    context.user_data["penalty_ms"] = 0
    context.user_data["wrong_count"] = 0

    # запись попытки в БД
    attempt_id = None
    if u:
        attempt_id = attempt_start(uid)
        context.user_data["attempt_id"] = attempt_id
        log_event(uid, "attempt_started", payload_json=f'{{"attempt_id":{attempt_id}}}')

    await send(update, "🚀 Поехали! Отвечай кнопками 👇")
    await show_question(update, context)

async def show_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    order: List[int] = context.user_data.get("order", [])
    pos = int(context.user_data.get("pos", 0))

    if not order or pos >= len(order):
        await finish_quiz(update, context, status="finished")
        return

    q_index = order[pos]
    q = QUESTIONS[q_index]

    total_now = total_time_ms(context)
    penalty = int(context.user_data.get("penalty_ms", 0))
    progress = f"🧩 Вопрос {pos+1}/{QUIZ_SIZE}"
    caption = (
        f"{progress}\n"
        f"⏱ Сейчас: *{fmt_ms(total_now)}* (штраф: *{fmt_ms(penalty)}*)\n\n"
        f"*{q.text}*"
    )

    kb = quiz_kb(q_index, q.options)

    if q.photo_path:
        try:
            await send_photo(update, q.photo_path, caption=caption, reply_markup=kb)
            return
        except FileNotFoundError:
            caption += "\n\n_(Картинка не найдена в репозитории)_"

    await send(update, caption, reply_markup=kb)

async def finish_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE, status: str):
    u = update.effective_user
    attempt_id = context.user_data.get("attempt_id")

    wrong = int(context.user_data.get("wrong_count", 0))
    penalty = int(context.user_data.get("penalty_ms", 0))
    elapsed = int((time.time() - float(context.user_data.get("t0", time.time()))) * 1000)
    total = elapsed + penalty

    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "attempt_ended", payload_json=f'{{"status":"{status}","wrong":{wrong},"penalty_ms":{penalty},"total_ms":{total}}}')

    if attempt_id is not None:
        attempt_finish(int(attempt_id), status=status, elapsed_ms=elapsed, penalty_ms=penalty, wrong_count=wrong)

    # очистим сессию
    for k in ["order", "pos", "t0", "penalty_ms", "wrong_count", "attempt_id"]:
        context.user_data.pop(k, None)

    if status == "quit":
        await send(update, "Ок, попытка остановлена.", reply_markup=main_menu_kb())
        return

    await send(
        update,
        "🎉 *Финал!*\n\n"
        f"⏱ Итоговое время: *{fmt_ms(total)}*\n"
        f"❌ Ошибок: *{wrong}* (штраф: *{fmt_ms(penalty)}*)\n\n"
        "Хочешь улучшить результат?",
        reply_markup=finish_kb(),
    )

async def quit_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "quiz_quit_clicked")
    await finish_quiz(update, context, status="quit")

# ==========================================================
# =================== 11) ОБРАБОТКА ОТВЕТОВ (ans:..) ========
# ==========================================================
async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE, q_index: int, opt: int):
    query = update.callback_query
    u = update.effective_user

    order: List[int] = context.user_data.get("order", [])
    pos = int(context.user_data.get("pos", 0))
    attempt_id = context.user_data.get("attempt_id")

    if not order or pos >= len(order):
        await query.message.reply_text("Сессия не активна. Нажми ▶️ Начать тест.")
        return

    current_q_index = order[pos]
    if q_index != current_q_index:
        await query.message.reply_text("Это старые кнопки 🙂 Нажми ▶️ Начать тест заново.")
        return

    q = QUESTIONS[current_q_index]
    total_before = total_time_ms(context)

    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)

    if opt == q.correct:
        # лог ответа (верный)
        if u and attempt_id is not None:
            penalty_after = int(context.user_data.get("penalty_ms", 0))
            log_answer(int(attempt_id), int(u.id), pos, current_q_index, opt, True, penalty_after, total_before)

        context.user_data["pos"] = pos + 1
        await query.message.reply_text(f"✅ Верно!\n{q.explain_right}")
        await show_question(update, context)
        return

    # неверно -> штраф
    context.user_data["penalty_ms"] = int(context.user_data.get("penalty_ms", 0)) + WRONG_PENALTY_MS
    context.user_data["wrong_count"] = int(context.user_data.get("wrong_count", 0)) + 1

    penalty_after = int(context.user_data.get("penalty_ms", 0))
    wrong_count = int(context.user_data.get("wrong_count", 0))

    # обновим прогресс попытки
    if attempt_id is not None:
        attempt_update_progress(int(attempt_id), wrong_count, penalty_after)

    total_after = total_time_ms(context)

    # лог ответа (неверный)
    if u and attempt_id is not None:
        log_answer(int(attempt_id), int(u.id), pos, current_q_index, opt, False, penalty_after, total_after)

    await query.message.reply_text(
        f"❌ Неверно! +{WRONG_PENALTY_MS/1000:.0f}с штраф.\n"
        f"Подсказка: {q.hint_wrong}\n"
        "Попробуй ещё раз 🙂"
    )
# ==========================================================
# =================== 12) АДМИН: СТАТИСТИКА ================
# ==========================================================
def stats_overview_text() -> str:
    with db_connect() as con, con.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM users")
        users = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM attempts")
        attempts = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM attempts WHERE status='finished'")
        finished = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM attempts WHERE status='quit'")
        quits = int(cur.fetchone()["c"])

        cur.execute("SELECT AVG(total_ms) AS a FROM attempts WHERE status='finished' AND total_ms IS NOT NULL")
        avg_total = cur.fetchone()["a"]

        cur.execute("SELECT AVG(wrong_count) AS a FROM attempts WHERE status='finished'")
        avg_wrong = cur.fetchone()["a"]

    avg_total_s = fmt_ms(int(avg_total)) if avg_total is not None else "—"
    avg_wrong_s = f"{float(avg_wrong):.2f}" if avg_wrong is not None else "—"

    return (
        "📌 *Сводка*\n\n"
        f"👥 Пользователей: *{users}*\n"
        f"🧪 Попыток: *{attempts}*\n"
        f"✅ Завершили: *{finished}*\n"
        f"🏳️ Сдались: *{quits}*\n"
        f"⏱ Среднее итоговое время: *{avg_total_s}*\n"
        f"❌ Среднее ошибок: *{avg_wrong_s}*\n"
    )

def stats_users_text(limit: int = 20) -> str:
    with db_connect() as con, con.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(username, full_name, user_id::text) AS name, last_seen_ts
            FROM users
            ORDER BY last_seen_ts DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    lines = [f"👥 *Пользователи* (последние {limit})"]
    for r in rows:
        last_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(r["last_seen_ts"])))
        lines.append(f"• {r['name']} — last: {last_s}")
    return "\n".join(lines)

def stats_attempts_text(limit: int = 20) -> str:
    with db_connect() as con, con.cursor() as cur:
        cur.execute("""
            SELECT a.id,
                   COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                   a.status, a.total_ms, a.wrong_count, a.penalty_ms
            FROM attempts a
            JOIN users u ON u.user_id = a.user_id
            ORDER BY a.id DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    lines = [f"🧪 *Попытки* (последние {limit})"]
    for r in rows:
        total = fmt_ms(int(r["total_ms"])) if r["total_ms"] is not None else "—"
        lines.append(f"• #{r['id']} {r['name']} — {r['status']} — {total} — wrong:{r['wrong_count']} penalty:{fmt_ms(int(r['penalty_ms']))}")
    return "\n".join(lines)

def stats_hard_text(limit: int = 10) -> str:
    with db_connect() as con, con.cursor() as cur:
        cur.execute("""
            SELECT question_index,
                   SUM(CASE WHEN is_correct=false THEN 1 ELSE 0 END) AS wrongs,
                   COUNT(*) AS total
            FROM answers
            GROUP BY question_index
            ORDER BY wrongs DESC, total DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    if not rows:
        return "🧩 *Сложные вопросы*\n\nПока нет данных (нужно, чтобы кто-то отвечал)."

    lines = ["🧩 *Сложные вопросы* (по числу ошибок):"]
    for r in rows:
        qi = int(r["question_index"])
        title = QUESTIONS[qi].text if 0 <= qi < len(QUESTIONS) else f"Вопрос #{qi}"
        lines.append(f"• {title}\n  Ошибок: *{int(r['wrongs'])}* из *{int(r['total'])}*")
    return "\n".join(lines)

def stats_events_text(limit: int = 25) -> str:
    with db_connect() as con, con.cursor() as cur:
        cur.execute("""
            SELECT e.ts,
                   COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                   e.event_type
            FROM events e
            LEFT JOIN users u ON u.user_id = e.user_id
            ORDER BY e.id DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    lines = [f"🧾 *События* (последние {limit})"]
    for r in rows:
        ts_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(r["ts"])))
        lines.append(f"• {ts_s} — {r['name']} — {r['event_type']}")
    return "\n".join(lines)

def export_csv_bytes() -> Tuple[bytes, str]:
    with db_connect() as con, con.cursor() as cur:
        out = io.StringIO()
        w = csv.writer(out)

        out.write("=== USERS ===\n")
        w.writerow(["user_id", "username", "full_name", "first_seen_ts", "last_seen_ts"])
        cur.execute("SELECT user_id, username, full_name, first_seen_ts, last_seen_ts FROM users ORDER BY last_seen_ts DESC")
        for r in cur.fetchall():
            w.writerow([r["user_id"], r["username"], r["full_name"], r["first_seen_ts"], r["last_seen_ts"]])

        out.write("\n=== ATTEMPTS ===\n")
        w.writerow(["id", "user_id", "status", "started_ts", "ended_ts", "wrong_count", "penalty_ms", "elapsed_ms", "total_ms"])
        cur.execute("SELECT id, user_id, status, started_ts, ended_ts, wrong_count, penalty_ms, elapsed_ms, total_ms FROM attempts ORDER BY id DESC")
        for r in cur.fetchall():
            w.writerow([r["id"], r["user_id"], r["status"], r["started_ts"], r["ended_ts"], r["wrong_count"], r["penalty_ms"], r["elapsed_ms"], r["total_ms"]])

        out.write("\n=== ANSWERS ===\n")
        w.writerow(["id", "attempt_id", "user_id", "ts", "pos", "question_index", "option_index", "is_correct", "penalty_ms_after", "total_ms_now"])
        cur.execute("""
            SELECT id, attempt_id, user_id, ts, pos, question_index, option_index, is_correct, penalty_ms_after, total_ms_now
            FROM answers
            ORDER BY id DESC
        """)
        for r in cur.fetchall():
            w.writerow([r["id"], r["attempt_id"], r["user_id"], r["ts"], r["pos"], r["question_index"], r["option_index"],
                        r["is_correct"], r["penalty_ms_after"], r["total_ms_now"]])

    data = out.getvalue().encode("utf-8")
    filename = f"bot_stats_export_{int(time.time())}.csv.txt"
    return data, filename

async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    uid, username, full_name = user_display(u)
    upsert_user(uid, username, full_name)
    log_event(uid, "cmd_myid")
    await update.message.reply_text(f"Твой user_id: {uid}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    uid, username, full_name = user_display(u)
    upsert_user(uid, username, full_name)
    log_event(uid, "cmd_stats")

    if not is_admin(update):
        await update.message.reply_text("Нет доступа.")
        return

    await update.message.reply_text("📊 Меню статистики (только админ):", reply_markup=stats_menu_kb())

async def handle_stats_action(update: Update, action: str):
    if not is_admin(update):
        await send(update, "Нет доступа.")
        return

    if action == "overview":
        await send(update, stats_overview_text(), reply_markup=stats_menu_kb())
    elif action == "users":
        await send(update, stats_users_text(20), reply_markup=stats_menu_kb())
    elif action == "attempts":
        await send(update, stats_attempts_text(20), reply_markup=stats_menu_kb())
    elif action == "hard":
        await send(update, stats_hard_text(10), reply_markup=stats_menu_kb())
    elif action == "events":
        await send(update, stats_events_text(25), reply_markup=stats_menu_kb())
    elif action == "export":
        data, filename = export_csv_bytes()
        bio = io.BytesIO(data)
        bio.name = filename
        if update.callback_query:
            await update.callback_query.message.reply_document(document=InputFile(bio, filename=filename), caption="Экспорт статистики")
        else:
            await update.message.reply_document(document=InputFile(bio, filename=filename), caption="Экспорт статистики")
    elif action == "clear_confirm":
        await send(
            update,
            "⚠️ *ВНИМАНИЕ!* Это удалит ВСЮ статистику (users/events/attempts/answers).\n"
            "Действие необратимо.\n\n"
            "Точно очистить?",
            reply_markup=clear_confirm_kb(),
        )
    elif action == "clear_yes":
        db_clear_all()
        await send(update, "✅ Статистика очищена.", reply_markup=stats_menu_kb())
    elif action == "clear_no":
        await send(update, "Ок, отменено.", reply_markup=stats_menu_kb())
    else:
        await send(update, "Неизвестный пункт.", reply_markup=stats_menu_kb())

# ==========================================================
# =================== 13) ROUTER CALLBACKS =================
# ==========================================================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "callback", payload_json=f'{{"data":"{data}"}}')

    if data == "noop":
        return

    if data == "menu":
        await show_menu(update, context)
        return

    if data == "help":
        await show_help(update, context)
        return

    if data == "leaderboard":
        await show_leaderboard(update, context)
        return

    if data == "start_quiz":
        await start_quiz(update, context)
        return

    if data == "quit":
        await quit_quiz(update, context)
        return

    if data.startswith("theory:"):
        page = int(data.split(":")[1])
        await show_theory(update, context, page)
        return

    if data.startswith("stats:"):
        action = data.split(":")[1]
        await handle_stats_action(update, action)
        return

    if data.startswith("ans:"):
        _, q_index_s, opt_s = data.split(":")
        await handle_answer(update, context, int(q_index_s), int(opt_s))
        return

# ==========================================================
# =================== 14) COMMANDS + TEXT ==================
# ==========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "cmd_start")
    await show_menu(update, context)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # любое сообщение -> меню
    u = update.effective_user
    if u:
        uid, username, full_name = user_display(u)
        upsert_user(uid, username, full_name)
        log_event(uid, "text_message")
    await show_menu(update, context)

# ==========================================================
# =================== 15) MAIN =============================
# ==========================================================
def main():
    ensure_ready()
    db_init()
    print("BOOT: db_init OK")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("stats", cmd_stats))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("BOOT: polling start")
    app.run_polling()

if __name__ == "__main__":
    main()
