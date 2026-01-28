import time
import random
import io
import csv
from dataclasses import dataclass
from typing import List, Optional, Tuple

import psycopg
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
# =================== НАСТРОЙКА КОНТЕНТА ===================
# ==========================================================
BOT_TOKEN = "8069382967:AAHIlwXCyOLlgl4XYgcjlbOSSZllDz4iP4o"
# Railway Postgres обычно сам выставляет DATABASE_URL. Если нет — добавь в Variables.

# Только админ сможет смотреть статистику и чистить её
ADMIN_IDS = {111111111}  # <-- замени на свой Telegram user_id (узнаешь через /myid)

QUIZ_SIZE = 2
WRONG_PENALTY_MS = 5000  # +5 секунд за ошибку

THEORY_TEXT = """
ВСТАВЬ_СЮДА_ОБЩУЮ_ТЕОРИЮ_ОДНИМ_ТЕКСТОМ.

• Можно списки
• Можно абзацы

Бот сам порежет на страницы.
"""

@dataclass
class Question:
    text: str
    options: List[str]         # 4 варианта
    correct: int               # индекс 0..3
    hint_wrong: str            # подсказка при ошибке
    explain_right: str         # пояснение при верном ответе
    photo_path: Optional[str] = None  # "assets/q1.png" (опционально)

# ВОПРОСЫ редактируешь ТОЛЬКО здесь:
QUESTIONS: List[Question] = [
    Question(
        text="Сколько будет 7 × 8?",
        options=["54", "56", "58", "64"],
        correct=1,
        hint_wrong="Подумай: 7×7=49, значит 7×8 на 7 больше.",
        explain_right="7×8 = 56 (таблица умножения).",
    ),
    Question(
        text="Какая планета ближе всего к Солнцу?",
        options=["Венера", "Марс", "Меркурий", "Юпитер"],
        correct=2,
        hint_wrong="Порядок планет от Солнца начинается с Меркурия.",
        explain_right="Ближе всего к Солнцу — Меркурий.",
    ),
    # ДОБАВЬ ещё вопросы, чтобы было >= QUIZ_SIZE
]
# ==========================================================
# =============== КОНЕЦ НАСТРОЙКИ КОНТЕНТА ==================
# ==========================================================


# =========================
# БАЗА (Postgres)
# =========================
import os as _os
if DATABASE_URL is None:
    DATABASE_URL = _os.environ.get("DATABASE_URL")

def now_ts() -> int:
    return int(time.time())

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан. Подключи Railway Postgres или добавь переменную DATABASE_URL.")
    return psycopg.connect(DATABASE_URL)

def db_init():
    with db_connect() as con:
        with con.cursor() as cur:
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

def db_clear_all():
    """Полная очистка статистики. Осторожно: удаляет всё."""
    with db_connect() as con:
        with con.cursor() as cur:
            # порядок важен из-за ссылок attempt->answers
            cur.execute("TRUNCATE TABLE answers RESTART IDENTITY")
            cur.execute("TRUNCATE TABLE attempts RESTART IDENTITY")
            cur.execute("TRUNCATE TABLE events RESTART IDENTITY")
            cur.execute("TRUNCATE TABLE users RESTART IDENTITY")
        con.commit()

def upsert_user(u) -> None:
    ts = now_ts()
    uid = int(u.id)
    username = u.username
    full_name = u.full_name
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE user_id=%s", (uid,))
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    "INSERT INTO users(user_id, username, full_name, first_seen_ts, last_seen_ts) VALUES(%s,%s,%s,%s,%s)",
                    (uid, username, full_name, ts, ts),
                )
            else:
                cur.execute(
                    "UPDATE users SET username=%s, full_name=%s, last_seen_ts=%s WHERE user_id=%s",
                    (username, full_name, ts, uid),
                )
        con.commit()

def log_event(user_id: int, event_type: str, payload_json: Optional[str] = None) -> None:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                "INSERT INTO events(ts, user_id, event_type, payload_json) VALUES(%s,%s,%s,%s)",
                (now_ts(), int(user_id), event_type, payload_json),
            )
        con.commit()

def attempt_start(user_id: int) -> int:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO attempts(user_id, started_ts, status, quiz_size, wrong_penalty_ms)
                VALUES(%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (int(user_id), now_ts(), "started", int(QUIZ_SIZE), int(WRONG_PENALTY_MS)),
            )
            attempt_id = cur.fetchone()[0]
        con.commit()
    return int(attempt_id)

def attempt_update_progress(attempt_id: int, wrong_count: int, penalty_ms: int) -> None:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                "UPDATE attempts SET wrong_count=%s, penalty_ms=%s WHERE id=%s",
                (int(wrong_count), int(penalty_ms), int(attempt_id)),
            )
        con.commit()

def attempt_finish(attempt_id: int, status: str, elapsed_ms: int, penalty_ms: int, wrong_count: int) -> None:
    ended = now_ts()
    total = int(elapsed_ms) + int(penalty_ms)
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                UPDATE attempts
                SET ended_ts=%s, status=%s, elapsed_ms=%s, penalty_ms=%s, wrong_count=%s, total_ms=%s
                WHERE id=%s
                """,
                (ended, status, int(elapsed_ms), int(penalty_ms), int(wrong_count), total, int(attempt_id)),
            )
        con.commit()

def log_answer(attempt_id: int, user_id: int, pos: int, question_index: int, option_index: int,
               is_correct: bool, penalty_ms_after: int, total_ms_now: int) -> None:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO answers(attempt_id, user_id, ts, pos, question_index, option_index, is_correct, penalty_ms_after, total_ms_now)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (int(attempt_id), int(user_id), now_ts(), int(pos), int(question_index), int(option_index),
                 bool(is_correct), int(penalty_ms_after), int(total_ms_now)),
            )
        con.commit()

# =========================
# Теория -> страницы
# =========================
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

# =========================
# UI
# =========================
def fmt_ms(ms: int) -> str:
    sec = ms / 1000.0
    m = int(sec // 60)
    s = sec - 60 * m
    return f"{m}:{s:06.3f}"

def is_admin(update: Update) -> bool:
    u = update.effective_user
    return bool(u and int(u.id) in ADMIN_IDS)

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


# =========================
# ТЕСТ: порядок/время/штраф
# =========================
def ensure_enough_questions():
    if len(QUESTIONS) < QUIZ_SIZE:
        raise RuntimeError(f"В QUESTIONS {len(QUESTIONS)} вопросов, а QUIZ_SIZE={QUIZ_SIZE}. Добавь вопросы или уменьши QUIZ_SIZE.")

def build_quiz_order() -> List[int]:
    idx = list(range(len(QUESTIONS)))
    random.shuffle(idx)
    return idx[:QUIZ_SIZE]

def total_time_ms(context: ContextTypes.DEFAULT_TYPE) -> int:
    t0 = float(context.user_data.get("t0", time.time()))
    penalty = int(context.user_data.get("penalty_ms", 0))
    base = int((time.time() - t0) * 1000)
    return base + penalty


# =========================
# ЛИДЕРБОРД (лучший total_ms по пользователю)
# =========================
def leaderboard_top(limit: int = 10) -> List[Tuple[str, int]]:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                       MIN(a.total_ms) AS best_total
                FROM attempts a
                JOIN users u ON u.user_id = a.user_id
                WHERE a.status='finished' AND a.total_ms IS NOT NULL
                GROUP BY a.user_id, name
                ORDER BY best_total ASC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
    return [(r[0], int(r[1])) for r in rows]


# =========================
# СТАТИСТИКА (админ)
# =========================
def stats_overview() -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            users = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM attempts")
            attempts = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM attempts WHERE status='finished'")
            finished = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM attempts WHERE status='quit'")
            quits = cur.fetchone()[0]
            cur.execute("SELECT AVG(total_ms) FROM attempts WHERE status='finished' AND total_ms IS NOT NULL")
            avg_total = cur.fetchone()[0]
            cur.execute("SELECT AVG(wrong_count) FROM attempts WHERE status='finished'")
            avg_wrong = cur.fetchone()[0]

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

def stats_users(limit: int = 20) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT user_id, COALESCE(username, full_name, user_id::text) AS name, last_seen_ts
                FROM users
                ORDER BY last_seen_ts DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()

    lines = [f"👥 *Пользователи* (последние {limit})"]
    for uid, name, last_ts in rows:
        last_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(last_ts)))
        lines.append(f"• {name} — last: {last_s}")
    return "\n".join(lines)

def stats_attempts(limit: int = 20) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT a.id,
                       COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                       a.status, a.total_ms, a.wrong_count, a.penalty_ms, a.started_ts
                FROM attempts a
                JOIN users u ON u.user_id = a.user_id
                ORDER BY a.id DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()

    lines = [f"🧪 *Попытки* (последние {limit})"]
    for aid, name, status, total_ms, wrong_count, penalty_ms, started_ts in rows:
        total = fmt_ms(int(total_ms)) if total_ms is not None else "—"
        lines.append(f"• #{aid} {name} — {status} — {total} — wrong:{wrong_count} penalty:{fmt_ms(int(penalty_ms))}")
    return "\n".join(lines)

def stats_hard_questions(limit: int = 10) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT question_index,
                       SUM(CASE WHEN is_correct=false THEN 1 ELSE 0 END) AS wrongs,
                       COUNT(*) AS total
                FROM answers
                GROUP BY question_index
                HAVING COUNT(*) > 0
                ORDER BY wrongs DESC, total DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()

    if not rows:
        return "🧩 *Сложные вопросы*\n\nПока нет данных (нужно, чтобы кто-то начал отвечать)."

    lines = ["🧩 *Сложные вопросы* (по числу ошибок):"]
    for qi, wrongs, total in rows:
        qi = int(qi)
        title = QUESTIONS[qi].text if 0 <= qi < len(QUESTIONS) else f"Вопрос #{qi}"
        lines.append(f"• {title}\n  Ошибок: *{int(wrongs)}* из *{int(total)}*")
    return "\n".join(lines)

def stats_recent_events(limit: int = 25) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT e.ts,
                       COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                       e.event_type
                FROM events e
                LEFT JOIN users u ON u.user_id = e.user_id
                ORDER BY e.id DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()

    lines = [f"🧾 *События* (последние {limit})"]
    for ts, name, etype in rows:
        ts_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(ts)))
        lines.append(f"• {ts_s} — {name} — {etype}")
    return "\n".join(lines)

def export_csv_bytes() -> Tuple[bytes, str]:
    with db_connect() as con:
        with con.cursor() as cur:
            out = io.StringIO()
            w = csv.writer(out)

            out.write("=== USERS ===\n")
            w.writerow(["user_id", "username", "full_name", "first_seen_ts", "last_seen_ts"])
            cur.execute("SELECT user_id, username, full_name, first_seen_ts, last_seen_ts FROM users ORDER BY last_seen_ts DESC")
            for r in cur.fetchall():
                w.writerow(list(r))

            out.write("\n=== ATTEMPTS ===\n")
            w.writerow(["id", "user_id", "status", "started_ts", "ended_ts", "wrong_count", "penalty_ms", "elapsed_ms", "total_ms"])
            cur.execute("""
                SELECT id, user_id, status, started_ts, ended_ts, wrong_count, penalty_ms, elapsed_ms, total_ms
                FROM attempts
                ORDER BY id DESC
            """)
            for r in cur.fetchall():
                w.writerow(list(r))

            out.write("\n=== ANSWERS ===\n")
            w.writerow(["id", "attempt_id", "user_id", "ts", "pos", "question_index", "option_index", "is_correct", "penalty_ms_after", "total_ms_now"])
            cur.execute("""
                SELECT id, attempt_id, user_id, ts, pos, question_index, option_index, is_correct, penalty_ms_after, total_ms_now
                FROM answers
                ORDER BY id DESC
            """)
            for r in cur.fetchall():
                w.writerow(list(r))

            data = out.getvalue().encode("utf-8")
            filename = f"bot_stats_export_{int(time.time())}.csv.txt"
            return data, filename

# =========================
# ЭКРАНЫ
# =========================
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        upsert_user(u)
        log_event(u.id, "menu_open")

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
        upsert_user(u)
        log_event(u.id, "help_open")
    await send(
        update,
        "ℹ️ *Как играть*\n\n"
        "1) Нажми ▶️ *Начать тест*\n"
        "2) Отвечай кнопками\n"
        f"3) Неверно — штраф *+{WRONG_PENALTY_MS/1000:.0f}с*\n"
        "4) Верно — следующий вопрос\n"
        "5) В конце — итог и можно улучшать рекорд 🏆",
        reply_markup=main_menu_kb(),
    )

async def show_theory(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    u = update.effective_user
    if u:
        upsert_user(u)
        log_event(u.id, "theory_open", payload_json=f'{{"page":{page}}}')

    pages = chunk_text(THEORY_TEXT)
    page = max(0, min(page, len(pages) - 1))
    await send(
        update,
        f"📚 *Теория* ({page+1}/{len(pages)})\n\n{pages[page]}",
        reply_markup=theory_kb(page, len(pages)),
    )

async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_enough_questions()
    u = update.effective_user
    if u:
        upsert_user(u)
        log_event(u.id, "quiz_start_clicked")

    context.user_data["order"] = build_quiz_order()
    context.user_data["pos"] = 0
    context.user_data["t0"] = time.time()
    context.user_data["penalty_ms"] = 0
    context.user_data["wrong_count"] = 0

    if u:
        attempt_id = attempt_start(u.id)
        context.user_data["attempt_id"] = attempt_id
        log_event(u.id, "attempt_started", payload_json=f'{{"attempt_id":{attempt_id}}}')

    await send(update, "🚀 Поехали! 👇")
    await show_question(update, context)

async def show_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    order: List[int] = context.user_data.get("order", [])
    pos = int(context.user_data.get("pos", 0))

    if not order or pos >= len(order):
        await finish_quiz(update, context, status="finished")
        return

    q_index = order[pos]
    q = QUESTIONS[q_index]

    progress = f"🧩 Вопрос {pos+1}/{QUIZ_SIZE}"
    total_now = total_time_ms(context)
    penalty = int(context.user_data.get("penalty_ms", 0))
    caption = f"{progress}\n⏱ Сейчас: {fmt_ms(total_now)} (штраф: {fmt_ms(penalty)})\n\n*{q.text}*"
    kb = quiz_kb(q_index, q.options)

    if q.photo_path:
        try:
            await send_photo(update, q.photo_path, caption=caption, reply_markup=kb)
            return
        except FileNotFoundError:
            caption += "\n\n_(Картинка не найдена)_"

    await send(update, caption, reply_markup=kb)

async def finish_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE, status: str):
    u = update.effective_user
    attempt_id = context.user_data.get("attempt_id")
    wrong = int(context.user_data.get("wrong_count", 0))
    penalty = int(context.user_data.get("penalty_ms", 0))
    elapsed = int((time.time() - float(context.user_data.get("t0", time.time()))) * 1000)
    total = elapsed + penalty

    if u:
        upsert_user(u)
        log_event(u.id, "attempt_ended", payload_json=f'{{"status":"{status}","wrong":{wrong},"penalty_ms":{penalty},"total_ms":{total}}}')

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
        upsert_user(u)
        log_event(u.id, "quiz_quit_clicked")
    await finish_quiz(update, context, status="quit")

async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        upsert_user(u)
        log_event(u.id, "leaderboard_open")

    rows = leaderboard_top(10)
    if not rows:
        await send(update, "Пока нет результатов. Нажми ▶️ Начать тест.", reply_markup=main_menu_kb())
        return
    lines = ["🏆 *Лидеры* (лучшее итоговое время):"]
    for i, (name, ms) in enumerate(rows, 1):
        lines.append(f"{i}. {name} — *{fmt_ms(ms)}*")
    await send(update, "\n".join(lines), reply_markup=main_menu_kb())

# =========================
# Админ-команды
# =========================
async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    upsert_user(u)
    log_event(u.id, "cmd_myid")
    await update.message.reply_text(f"Твой user_id: {u.id}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    upsert_user(u)
    log_event(u.id, "cmd_stats")

    if not is_admin(update):
        await update.message.reply_text("Нет доступа.")
        return
    await update.message.reply_text("📊 Меню статистики (только админ):", reply_markup=stats_menu_kb())

async def handle_stats_action(update: Update, action: str):
    if not is_admin(update):
        await send(update, "Нет доступа.")
        return

    if action == "overview":
        await send(update, stats_overview(), reply_markup=stats_menu_kb())
    elif action == "users":
        await send(update, stats_users(20), reply_markup=stats_menu_kb())
    elif action == "attempts":
        await send(update, stats_attempts(20), reply_markup=stats_menu_kb())
    elif action == "hard":
        await send(update, stats_hard_questions(10), reply_markup=stats_menu_kb())
    elif action == "events":
        await send(update, stats_recent_events(25), reply_markup=stats_menu_kb())
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
        await send(update, "✅ Статистика полностью очищена.", reply_markup=stats_menu_kb())
    elif action == "clear_no":
        await send(update, "Ок, отменено.", reply_markup=stats_menu_kb())
    else:
        await send(update, "Неизвестный пункт статистики.", reply_markup=stats_menu_kb())

# Эти функции используют QUESTIONS, поэтому объявим после:
def stats_users(limit: int = 20) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(username, full_name, user_id::text) AS name, last_seen_ts
                FROM users
                ORDER BY last_seen_ts DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
    lines = [f"👥 *Пользователи* (последние {limit})"]
    for name, last_ts in rows:
        last_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(last_ts)))
        lines.append(f"• {name} — last: {last_s}")
    return "\n".join(lines)

def stats_attempts(limit: int = 20) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT a.id,
                       COALESCE(u.username, u.full_name, u.user_id::text) AS name,
                       a.status, a.total_ms, a.wrong_count, a.penalty_ms
                FROM attempts a
                JOIN users u ON u.user_id = a.user_id
                ORDER BY a.id DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
    lines = [f"🧪 *Попытки* (последние {limit})"]
    for aid, name, status, total_ms, wrong_count, penalty_ms in rows:
        total = fmt_ms(int(total_ms)) if total_ms is not None else "—"
        lines.append(f"• #{aid} {name} — {status} — {total} — wrong:{wrong_count} penalty:{fmt_ms(int(penalty_ms))}")
    return "\n".join(lines)

def stats_hard_questions(limit: int = 10) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT question_index,
                       SUM(CASE WHEN is_correct=false THEN 1 ELSE 0 END) AS wrongs,
                       COUNT(*) AS total
                FROM answers
                GROUP BY question_index
                ORDER BY wrongs DESC, total DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()

    if not rows:
        return "🧩 *Сложные вопросы*\n\nПока нет данных."

    lines = ["🧩 *Сложные вопросы*:"]
    for qi, wrongs, total in rows:
        qi = int(qi)
        title = QUESTIONS[qi].text if 0 <= qi < len(QUESTIONS) else f"Вопрос #{qi}"
        lines.append(f"• {title}\n  Ошибок: *{int(wrongs)}* из *{int(total)}*")
    return "\n".join(lines)

def stats_recent_events(limit: int = 25) -> str:
    with db_connect() as con:
        with con.cursor() as cur:
            cur.execute("""
                SELECT e.ts, COALESCE(u.username, u.full_name, u.user_id::text) AS name, e.event_type
                FROM events e
                LEFT JOIN users u ON u.user_id = e.user_id
                ORDER BY e.id DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
    lines = [f"🧾 *События* (последние {limit})"]
    for ts, name, etype in rows:
        ts_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(ts)))
        lines.append(f"• {ts_s} — {name} — {etype}")
    return "\n".join(lines)

# =========================
# Callback router
# =========================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    u = update.effective_user
    if u:
        upsert_user(u)

    if data == "noop":
        return

    if u:
        log_event(u.id, "callback", payload_json=f'{{"data":"{data}"}}')

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
        # ans:<question_index>:<option_index>
        _, q_index_s, opt_s = data.split(":")
        q_index = int(q_index_s)
        opt = int(opt_s)

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

        if opt == q.correct:
            if u and attempt_id is not None:
                penalty_after = int(context.user_data.get("penalty_ms", 0))
                log_answer(int(attempt_id), u.id, pos, current_q_index, opt, True, penalty_after, total_before)

            context.user_data["pos"] = pos + 1
            await query.message.reply_text(f"✅ Верно!\n{q.explain_right}")
            await show_question(update, context)
        else:
            context.user_data["penalty_ms"] = int(context.user_data.get("penalty_ms", 0)) + WRONG_PENALTY_MS
            context.user_data["wrong_count"] = int(context.user_data.get("wrong_count", 0)) + 1

            penalty_after = int(context.user_data.get("penalty_ms", 0))
            wrong_count = int(context.user_data.get("wrong_count", 0))

            if attempt_id is not None:
                attempt_update_progress(int(attempt_id), wrong_count, penalty_after)

            total_after = total_time_ms(context)
            if u and attempt_id is not None:
                log_answer(int(attempt_id), u.id, pos, current_q_index, opt, False, penalty_after, total_after)

            await query.message.reply_text(
                f"❌ Неверно! +{WRONG_PENALTY_MS/1000:.0f}с штраф.\n"
                f"Подсказка: {q.hint_wrong}\n"
                "Попробуй ещё раз 🙂"
            )
        return

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        upsert_user(u)
        log_event(u.id, "cmd_start")
    await show_menu(update, context)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        upsert_user(u)
        log_event(u.id, "text_message")
    await show_menu(update, context)

def main():
    if not BOT_TOKEN or BOT_TOKEN == "ВСТАВЬ_СЮДА_ТОКЕН":
        raise RuntimeError("Вставь токен в BOT_TOKEN в начале bot.py")
    db_init()
    ensure_enough_questions()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.run_polling()

if __name__ == "__main__":
    main()



