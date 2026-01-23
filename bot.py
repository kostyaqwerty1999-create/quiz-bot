import time
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# ВСТАВЬ СВОЙ ТОКЕН
# =========================
BOT_TOKEN = "8069382967:AAHIlwXCyOLlgl4XYgcjlbOSSZllDz4iP4o"

# =========================
# НАСТРОЙКИ
# =========================
DB_PATH = "leaderboard.sqlite3"
QUIZ_SIZE = 20
LB_LIMIT = 10

# =========================
# ДАННЫЕ ТЕСТА
# correct = индекс правильного варианта (0..3)
# explanation = короткое объяснение (показывается при ошибке и/или после ответа)
# photo_path = "assets/q1.png" (опционально)
# =========================
@dataclass
class Question:
    text: str
    options: List[str]
    correct: int
    explanation: str
    photo_path: Optional[str] = None


QUESTIONS: List[Question] = [
    Question("1) Один из известных супергероев носит имя, которое наводит на мысль о том, то он имеет навыки работы с УЗ-дефектоскопами. По легенде, он может видеть сквозь стены, но природа используемых для этого лучей электромагнитная. Образ героя тоже может вводить в заблуждение, но только не того, кто разбирается в физике. Назовите его.", ["Халк", "Супермен", "Бэтмен", "Человек паук"], 2, "Думай еще"),
    Question("2) Какая планета ближе всего к Солнцу?", ["Венера", "Марс", "Меркурий", "Юпитер"], 2, "Ближе всего — Меркурий."),
    Question("3) Столица Австралии?", ["Сидней", "Канберра", "Мельбурн", "Перт"], 1, "Столица — Канберра (не Сидней)."),
    Question("4) Сколько минут в 2 часах?", ["60", "90", "120", "180"], 2, "2 часа = 120 минут."),
    Question("5) Что больше: 0.5 или 0.05?", ["0.05", "0.5", "Они равны", "Нельзя сравнить"], 1, "0.5 = 50%, 0.05 = 5%."),
    Question("6) Корень из 81 равен…", ["7", "8", "9", "10"], 2, "9×9 = 81."),
    Question("7) Какая фигура имеет 3 стороны?", ["Квадрат", "Треугольник", "Круг", "Пятиугольник"], 1, "У треугольника 3 стороны."),
    Question("8) Какой океан самый большой?", ["Атлантический", "Индийский", "Тихий", "Северный Ледовитый"], 2, "Самый большой — Тихий океан."),
    Question("9) Кто написал «Евгений Онегин»?", ["Гоголь", "Лермонтов", "Пушкин", "Толстой"], 2, "Автор — А.С. Пушкин."),
    Question("10) Простое число среди вариантов:", ["21", "27", "29", "33"], 2, "29 делится только на 1 и на 29."),
    Question("11) Сколько байт в 1 КиБ (KiB)?", ["1000", "1024", "2048", "512"], 1, "1 KiB = 1024 байта."),
    Question("12) Единица силы тока:", ["Вольт", "Ом", "Ампер", "Ватт"], 2, "Сила тока измеряется в амперах."),
    Question("13) Столица Италии:", ["Милан", "Рим", "Венеция", "Неаполь"], 1, "Столица Италии — Рим."),
    Question("14) Сколько градусов в прямом угле?", ["45", "90", "180", "360"], 1, "Прямой угол = 90°."),
    Question("15) Самый распространённый газ в атмосфере Земли:", ["Кислород", "Азот", "Углекислый газ", "Гелий"], 1, "Азота около 78%."),
    Question("16) Страна «восходящего солнца»:", ["Китай", "Япония", "Корея", "Таиланд"], 1, "Так называют Японию."),
    Question("17) Самая большая кость человека:", ["Лучевая", "Бедренная", "Плечевая", "Череп"], 1, "Бедренная — самая длинная и массивная."),
    Question("18) В каком году начался XXI век?", ["2000", "2001", "1999", "2010"], 1, "Первые века идут с года 1, поэтому XXI век — с 2001."),
    Question("19) Сколько континентов (часто в РФ выделяют)?", ["5", "6", "7", "8"], 1, "Часто выделяют 6: Евразия, Африка, Сев. Америка, Южн. Америка, Австралия, Антарктида."),
    Question("20) Что такое HTTP?", ["Язык программирования", "Протокол передачи гипертекста", "Операционная система", "База данных"], 1, "HTTP — протокол (правила) обмена данными в вебе."),
]
# Если хочешь картинку к вопросу:
# QUESTIONS[0].photo_path = "assets/q1.png"


# =========================
# ТЕОРИЯ (меню чтения)
# =========================
THEORY_PAGES: List[Tuple[str, str]] = [
    ("Математика: быстро и без ошибок",
     "• Умножение: 7×8=56, 8×8=64 — полезно помнить.\n"
     "• Проценты: 0.5 = 50%, 0.05 = 5%.\n"
     "• Углы: прямой = 90°, развернутый = 180°.\n"
     "• Корни: √81=9, потому что 9×9=81."),
    ("География: что важно запомнить",
     "• Австралия: столица Канберра (часто путают с Сиднеем).\n"
     "• Япония — «страна восходящего солнца».\n"
     "• Самый большой океан — Тихий.\n"
     "• Италия: столица Рим."),
    ("Культура и наука",
     "• «Евгений Онегин» — Пушкин.\n"
     "• Самая большая кость — бедренная.\n"
     "• Атмосфера: больше всего азота.\n"
     "• XXI век начался в 2001 году."),
    ("IT-минимум перед тестом",
     "• 1 KiB = 1024 байта (в вычислениях двоичная кратность).\n"
     "• Ампер — единица силы тока.\n"
     "• HTTP — протокол передачи гипертекста.\n"
     "• Лучший результат — это меньшее время прохождения."),
]


# =========================
# БАЗА ДАННЫХ
# =========================
def db_connect():
    return sqlite3.connect(DB_PATH)

def db_init():
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            best_ms INTEGER NOT NULL,
            last_ms INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
    """)
    con.commit()
    con.close()

def db_save_result(user_id: int, username: str, ms: int):
    now = int(time.time())
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT best_ms, attempts FROM results WHERE user_id = ?", (user_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            "INSERT INTO results(user_id, username, best_ms, last_ms, attempts, updated_at) VALUES(?,?,?,?,?,?)",
            (user_id, username, ms, ms, 1, now),
        )
    else:
        best_ms, attempts = row
        new_best = ms if ms < best_ms else best_ms
        cur.execute(
            "UPDATE results SET username=?, best_ms=?, last_ms=?, attempts=?, updated_at=? WHERE user_id=?",
            (username, new_best, ms, attempts + 1, now, user_id),
        )
    con.commit()
    con.close()

def db_top(limit: int = LB_LIMIT):
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT username, best_ms FROM results ORDER BY best_ms ASC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return rows

def db_rank_of(user_id: int) -> Optional[int]:
    con = db_connect()
    cur = con.cursor()
    # rank = 1 + count пользователей с лучшим временем
    cur.execute("SELECT best_ms FROM results WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row is None:
        con.close()
        return None
    best_ms = row[0]
    cur.execute("SELECT COUNT(*) FROM results WHERE best_ms < ?", (best_ms,))
    better = cur.fetchone()[0]
    con.close()
    return int(better) + 1


# =========================
# UI
# =========================
def fmt_ms(ms: int) -> str:
    sec = ms / 1000.0
    m = int(sec // 60)
    s = sec - 60 * m
    return f"{m}:{s:06.3f}"

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Начать тест", callback_data="start_quiz")],
        [InlineKeyboardButton("📚 Теория", callback_data="theory:0")],
        [InlineKeyboardButton("🏆 Таблица лидеров", callback_data="leaderboard")],
        [InlineKeyboardButton("ℹ️ Как играть", callback_data="help")],
    ])

def theory_kb(page: int) -> InlineKeyboardMarkup:
    prev_btn = InlineKeyboardButton("⬅️ Назад", callback_data=f"theory:{page-1}") if page > 0 else InlineKeyboardButton(" ", callback_data="noop")
    next_btn = InlineKeyboardButton("Вперёд ➡️", callback_data=f"theory:{page+1}") if page < len(THEORY_PAGES)-1 else InlineKeyboardButton(" ", callback_data="noop")
    return InlineKeyboardMarkup([
        [prev_btn, InlineKeyboardButton("🏠 Меню", callback_data="menu"), next_btn],
        [InlineKeyboardButton("▶️ Начать тест", callback_data="start_quiz")],
    ])

def quiz_kb(qi: int) -> InlineKeyboardMarkup:
    q = QUESTIONS[qi]
    rows = [[InlineKeyboardButton(opt, callback_data=f"ans:{qi}:{i}")] for i, opt in enumerate(q.options)]
    rows.append([InlineKeyboardButton("🏆 Лидеры", callback_data="leaderboard")])
    rows.append([InlineKeyboardButton("🏳️ Сдаться", callback_data="quit"), InlineKeyboardButton("🏠 Меню", callback_data="menu")])
    return InlineKeyboardMarkup(rows)

def finish_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Пройти ещё раз", callback_data="start_quiz")],
        [InlineKeyboardButton("🏆 Таблица лидеров", callback_data="leaderboard")],
        [InlineKeyboardButton("📚 Теория", callback_data="theory:0")],
        [InlineKeyboardButton("🏠 Меню", callback_data="menu")],
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


# =========================
# СЦЕНЫ / ЛОГИКА
# =========================
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send(
        update,
        "👋 Привет!\n\n"
        "🎯 Здесь тест из 20 вопросов с вариантами.\n"
        "⏱ Засекается время прохождения.\n"
        "🏆 В лидерах — самое быстрое прохождение.\n\n"
        "Выбирай, что сделать:",
        reply_markup=main_menu_kb(),
    )

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send(
        update,
        "ℹ️ Как играть:\n"
        "1) Нажми ▶️ Начать тест\n"
        "2) Отвечай кнопками (пока не выберешь верный)\n"
        "3) Верно — следующий вопрос\n"
        "4) В конце увидишь своё время и место в рейтинге\n\n"
        "Совет: перед тестом загляни в 📚 Теорию 🙂",
        reply_markup=main_menu_kb(),
    )

async def show_theory(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    page = max(0, min(page, len(THEORY_PAGES) - 1))
    title, body = THEORY_PAGES[page]
    await send(
        update,
        f"📚 Теория ({page+1}/{len(THEORY_PAGES)})\n"
        f"— *{title}*\n\n{body}",
        reply_markup=theory_kb(page),
    )

async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Сброс прогресса
    context.user_data["q"] = 0
    context.user_data["t0"] = time.time()
    await send(update, "🚀 Поехали! Отвечай кнопками 👇")
    await show_question(update, context)

async def show_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    qi = int(context.user_data.get("q", 0))

    if qi >= QUIZ_SIZE:
        elapsed = int((time.time() - float(context.user_data.get("t0", time.time()))) * 1000)
        u = update.effective_user
        username = u.username or u.full_name or f"id:{u.id}"
        db_save_result(u.id, username, elapsed)
        rank = db_rank_of(u.id)

        rank_line = f"🏅 Твоё место: #{rank}" if rank is not None else ""
        await send(
            update,
            "🎉 *Финал!*\n\n"
            f"⏱ Время: *{fmt_ms(elapsed)}*\n"
            f"{rank_line}\n\n"
            "Хочешь улучшить результат?",
            reply_markup=finish_kb(),
        )
        return

    q = QUESTIONS[qi]
    progress = f"🧩 Вопрос {qi+1}/{QUIZ_SIZE}"
    elapsed_now = int((time.time() - float(context.user_data.get('t0', time.time()))) * 1000)
    timer = f"⏱ Сейчас: {fmt_ms(elapsed_now)}"

    text = f"{progress}\n{timer}\n\n{q.text}"

    if q.photo_path:
        try:
            await send_photo(update, q.photo_path, caption=text, reply_markup=quiz_kb(qi))
            return
        except FileNotFoundError:
            text += "\n\n(Картинка не найдена в репозитории)"

    await send(update, text, reply_markup=quiz_kb(qi))

async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = db_top(LB_LIMIT)
    if not rows:
        await send(update, "Пока нет результатов. Нажми ▶️ Начать тест.", reply_markup=main_menu_kb())
        return

    lines = ["🏆 *Таблица лидеров* (лучшее время):"]
    for i, (name, ms) in enumerate(rows, 1):
        lines.append(f"{i}. {name or 'Без имени'} — *{fmt_ms(int(ms))}*")

    await send(update, "\n".join(lines), reply_markup=main_menu_kb())

async def quit_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("q", None)
    context.user_data.pop("t0", None)
    await send(update, "Ок, попытка остановлена. Можешь почитать теорию или начать заново.", reply_markup=main_menu_kb())

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

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

    if data.startswith("ans:"):
        _, qi_s, oi_s = data.split(":")
        qi, oi = int(qi_s), int(oi_s)

        cur = int(context.user_data.get("q", 0))
        # защита от старых кнопок
        if qi != cur:
            await query.message.reply_text("Это старые кнопки 🙂 Нажми ▶️ Начать тест заново.")
            return

        q = QUESTIONS[qi]
        if oi == q.correct:
            context.user_data["q"] = cur + 1
            await query.message.reply_text(f"✅ Верно! {q.explanation}")
            await show_question(update, context)
        else:
            await query.message.reply_text(f"❌ Неверно. Подсказка: {q.explanation}\nПопробуй ещё раз 🙂")
        return

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Чтобы было удобно: любое сообщение показывает меню
    await show_menu(update, context)


def main():
    if not BOT_TOKEN or BOT_TOKEN == "ВСТАВЬ_СЮДА_ТОКЕН":
        raise RuntimeError("Вставь токен в переменную BOT_TOKEN в начале файла bot.py")

    db_init()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.run_polling()


if __name__ == "__main__":
    main()

