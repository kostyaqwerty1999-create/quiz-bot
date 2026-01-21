import time
import sqlite3
from dataclasses import dataclass
from typing import Optional, List

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ВАЖНО: если ты хранишь токен прямо в файле, вставь сюда:
BOT_TOKEN = "ВСТАВЬ_СЮДА_ТОКЕН"

# ---------- Вопросы (картинка опционально) ----------
@dataclass
class Question:
    text: str
    options: List[str]
    correct: int
    photo_path: Optional[str] = None  # например "assets/q1.png"

QUESTIONS: List[Question] = [
    Question("Вопрос 1: 2 + 2 = ?", ["3", "4", "5"], 1, None),
    Question("Вопрос 2: Столица Франции?", ["Париж", "Берлин", "Рим"], 0, None),
    Question("Вопрос 3: Выбери Python 🙂", ["Java", "Python", "C++"], 1, None),
]

# ---------- SQLite лидерборд ----------
DB_PATH = "leaderboard.sqlite3"

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            best_ms INTEGER NOT NULL
        )
    """)
    con.commit()
    con.close()

def db_upsert_best(user_id: int, username: str, ms: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT best_ms FROM results WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row is None or ms < row[0]:
        cur.execute(
            "REPLACE INTO results (user_id, username, best_ms) VALUES (?, ?, ?)",
            (user_id, username, ms),
        )
    con.commit()
    con.close()

def db_top(n: int = 10):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT username, best_ms FROM results ORDER BY best_ms ASC LIMIT ?", (n,))
    rows = cur.fetchall()
    con.close()
    return rows

def fmt_ms(ms: int) -> str:
    sec = ms / 1000.0
    m = int(sec // 60)
    s = sec - 60 * m
    return f"{m}:{s:06.3f}"

# ---------- UI ----------
def question_kb(qi: int) -> InlineKeyboardMarkup:
    q = QUESTIONS[qi]
    rows = [[InlineKeyboardButton(opt, callback_data=f"ans:{qi}:{i}")] for i, opt in enumerate(q.options)]
    rows.append([InlineKeyboardButton("🏆 Таблица лидеров", callback_data="lb")])
    rows.append([InlineKeyboardButton("🔄 Начать заново", callback_data="restart")])
    return InlineKeyboardMarkup(rows)

def finish_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏆 Таблица лидеров", callback_data="lb")],
        [InlineKeyboardButton("🔄 Пройти ещё раз", callback_data="restart")],
    ])

async def send(update: Update, text: str, reply_markup=None):
    if update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def send_photo(update: Update, path: str, caption: str, reply_markup=None):
    with open(path, "rb") as f:
        if update.callback_query:
            await update.callback_query.message.reply_photo(photo=f, caption=caption, reply_markup=reply_markup)
        else:
            await update.message.reply_photo(photo=f, caption=caption, reply_markup=reply_markup)

# ---------- Логика ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["q"] = 0
    context.user_data["t0"] = time.time()
    await send(update, "Поехали! 👇")
    await show_question(update, context)

async def show_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    qi = int(context.user_data.get("q", 0))
    if qi >= len(QUESTIONS):
        elapsed = int((time.time() - float(context.user_data.get("t0", time.time()))) * 1000)
        u = update.effective_user
        username = u.username or u.full_name or f"id:{u.id}"
        db_upsert_best(u.id, username, elapsed)

        await send(
            update,
            f"🎉 Поздравляю! Ты прошёл(а) тест!\n⏱ Время: {fmt_ms(elapsed)}",
            reply_markup=finish_kb(),
        )
        return

    q = QUESTIONS[qi]
    kb = question_kb(qi)

    if q.photo_path:
        try:
            await send_photo(update, q.photo_path, q.text, reply_markup=kb)
            return
        except FileNotFoundError:
            await send(update, q.text + "\n\n(Картинка не найдена в репозитории)", reply_markup=kb)
            return

    await send(update, q.text, reply_markup=kb)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "restart":
        context.user_data["q"] = 0
        context.user_data["t0"] = time.time()
        await query.message.reply_text("Ок! Начинаем заново 🚀")
        await show_question(update, context)
        return

    if data == "lb":
        rows = db_top(10)
        if not rows:
            await query.message.reply_text("Пока нет результатов. Нажми /start 🙂")
            return
        lines = ["🏆 Таблица лидеров (лучшее время):"]
        for i, (name, ms) in enumerate(rows, 1):
            lines.append(f"{i}. {name or 'Без имени'} — {fmt_ms(int(ms))}")
        await query.message.reply_text("\n".join(lines))
        return

    if data.startswith("ans:"):
        _, qi_s, oi_s = data.split(":")
        qi, oi = int(qi_s), int(oi_s)
        cur = int(context.user_data.get("q", 0))
        if qi != cur:
            await query.message.reply_text("Это старые кнопки 🙂 Нажми /start.")
            return

        if oi == QUESTIONS[qi].correct:
            context.user_data["q"] = cur + 1
            await query.message.reply_text("✅ Верно!")
            await show_question(update, context)
        else:
            await query.message.reply_text("❌ Неверно, попробуй ещё раз 🙂")

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Напиши /start чтобы начать тест 🙂")

def main():
    if not BOT_TOKEN or BOT_TOKEN == "ВСТАВЬ_СЮДА_ТОКЕН":
        raise RuntimeError("Вставь BOT_TOKEN в bot.py")

    db_init()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.run_polling()

if __name__ == "__main__":
    main()

