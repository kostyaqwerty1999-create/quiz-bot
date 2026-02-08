import os
import time
import random
import io
import csv
from dataclasses import dataclass
from typing import List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==========================================================
# ====================== НАСТРОЙКИ (ENV) ===================
# ==========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_IDS_RAW = os.environ.get("ADMIN_IDS", "")          # "123" или "1,2,3"
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}

# сколько вопросов брать за одно прохождение (по умолчанию 10)
QUESTIONS_PER_RUN = int(os.environ.get("QUESTIONS_PER_RUN", "10"))

# штраф за неправильный ответ (по умолчанию +5 сек)
WRONG_PENALTY_MS = int(os.environ.get("WRONG_PENALTY_MS", "5000"))

# ==========================================================
# ========================= ТЕОРИЯ =========================
# Вставляй одним большим текстом (можно в ENV, но проще здесь)
# ==========================================================
THEORY_TEXT = os.environ.get("THEORY_TEXT", """
ВСТАВЬ_СЮДА_ОБЩУЮ_ТЕОРИЮ_ОДНИМ_ТЕКСТОМ.

Можно списки:
• Пункт 1
• Пункт 2

Можно абзацы — бот порежет на страницы.
""").strip()

# ==========================================================
# ========================= ВОПРОСЫ =========================
# Редактируешь ТОЛЬКО ЭТОТ БЛОК
# ==========================================================
@dataclass
class Question:
    text: str
    options: List[str]         # варианты
    correct: int               # индекс правильного ответа
    hint_wrong: str            # подсказка если неверно
    explain_right: str         # пояснение если верно
    photo_path: Optional[str] = None  # "assets/q1.png" (опционально)

# ---- ТВОИ ВОПРОСЫ (вставлены как есть) ----
QUESTIONS: List[Question] = [
    Question(
        text="Инженеров по неразрушающему контролю часто сравнивают с врачами - диагностами. Медики проводят обследование пациента, чтобы выявить и исключить нарушения работы внутренних органов до появления клинических симптомов. Дефектоскописты проверяют «здоровье» материалов, конструкций и оборудования. И те и другие должны уметь выявить «заболевание» на как можно ранней стадии, правильно поставить диагноз и назначить лечение, а в некоторых случаях выписать листок нетрудоспособности. Если продолжить сравнение инженеров и медиков, то какая инженерная специальность аналогична врачу-хирургу?",
        options=["Инженер-энергетик", "Инженер -схемотехник", "Инженер по разрушающему контролю", "Инженер-программист"],
        correct=2,
        hint_wrong="Подумай еще возможно угадаешь",
        explain_right="Инженер энергетик специализируется на системах энергоснабжения, для человека источником энергии является скорее пищеварительная система, поэтому это наверно гастроэнтеролог. Инженер-схемотехник специализируется на разработке электрических цепей и это наверно ближе к неврологу. Инженер-программист разрабатывает алгоритмы работы системы и это ближе к высшей нервной деятельности и наверно врачу психиатру. А вот хирург, которому необходим прямой доступ к органам ближе к инженеру по разрушающему контролю.",
    ),
    Question(
        text="Задолго до того, как появился термин «неразрушающий контроль», люди осматривали объекты, чтобы определить размер, форму и наличие визуальных дефектов поверхности. В первую очередь такой контроль был распространен на ярмарках, чтобы оценить качество товара. Особый интерес представляли изделия кузнечного дела – мечи, сабли, топоры, кинжалы. Качество стали проверяли ударами по полосе железа. Какой вид неразрушающего контроля использовали кузнецы?",
        options=["Разрушающий", "Акустический", "Визуальный", "Магнитопорошковый"],
        correct=1,
        hint_wrong="А ты думал все так просто будет ",
        explain_right="акустический, кузнецы прислушивались к металлу, чтобы по звуку определить скрытые дефекты. Этот способ можно считать прообразом современного эхо-импульсного метода, когда преобразователь посылает короткие импульсы ультразвуковых колебаний в материал, которые отражаются от дефектов и возвращаются обратно. По времени прохождения сигнала определяется расстояние до дефекта, а по амплитуде отражённого импульса — его размер.",
    ),
    Question(
        text="Во времена Древнего Рима большое внимание уделялось качеству мрамора, из которого изготавливались не только статуи, но и поддерживающие конструкции зданий, которые должны были выдержать значительные нагрузки. Важно было не пропустить малейшие трещины. Для их выявления использовали муку и масло. Дефекты определялся по ряду признаков получившегося рисунка. Масло заполняло малейшие трещины, мука делало рисунок контрастным. Прообразом какого вида неразрушающего контроля является описанный способ древних мастеров?",
        options=["Магнитопорошковый", "Визуальный", "Капиллярный", "Ультразвуковой"],
        correct=2,
        hint_wrong="я тоже в истории плох",
        explain_right="капиллярный контроль или цветная дефектоскопия, при которой проверяемая поверхность смачивается смачивается окрашенной жидкостью, которая заполняет даже самые мельчайшие поры и трещинки, отчетливо обозначая таким образом поверхностные дефекты. При определении сквозных дефектов метод могут также называть «течеисканием».",
    ),
    Question(
        text="В современных супермаркетах можно попробовать себя в качестве дефектоскописта. Покупая арбуз, его простукивают и определяют его зрелость. Это акустический метод контроля. Кроме того, в крупных продуктовых магазинах можно встретить прибор для определения качества одного продукта питания, не разрушая его целостность. Суть метода, лежащего в основе – просвечивание продукта на свет, что позволяет рассмотреть внутренне строение, целостность, дифференцировать инородные предметы. Что это за продукт.",
        options=["Молоко в коробке", "Яйца", "Тушёнка", "Солёные огурцы"],
        correct=1,
        hint_wrong="Сходи в магазин и проверь",
        explain_right="яйца. На самом деле хозяйки знают еще один способ проверки яиц, не разрушая его скорлупу. Определить пригодность яйца в пищу можно по степени его погружения в воду, некачественное яйцо всплывет, а свежее утонет. Вареное яйцо или нет тоже можно не разбивая его. Если не знаешь, спроси у мамы. Мамы они тоже немного дефектоскописты.",
    ),
    Question(
        text="Баба Валя получила отличный урожай огурцов и часть из них решила засолить, а часть пустить на салат. Для засолки требовались некрупные огурчики, без трещин, царапин, равномерно зеленого цвета без желтизны. На салат же можно было пустить все, что не пошло в засолку. Какой метод отбраковки использовала баба Валя?",
        options=["Магнитный", "Ультразвуковой", "Визуальный", "Радиационный"],
        correct=2,
        hint_wrong="Ага конечно думаешь баба Валя имеет образование чтобы этим методом отбраковывать",
        explain_right="баба Валя использовала визуальный контроль, который помогает и дефектоскопистам быстро обнаружить различные изъяны, например сварных соединений, такие как выемки, полости, каверны, свищи, трещины, сколы, разнородность структуры, сторонние включения, непроваренные зоны, неравномерные области шовного профиля. Баба Валя тоже в душе была дефектоскопистом.",
    ),
    Question(
        text="Макс Эйзенхардт или Эрик Магнус Леншерр – персонаж комиксов издательства Marvel Comics. Впервые появился в 1963 году как заклятый враг профессора Икс и его команды людей Икс. Основная его суперспособность – управление электромагнитными полями, обладает сверхчеловеческой силой при необходимости, мастер боевой стратегии. Кроме того, он обладает обширными знаниями в генетике, физике и инженерии. Если бы Макс Эйзенхардт стал дефектоскопистом, на какой вид неразрушающего контроля он бы аттестовался в первую очередь?",
        options=["Радиографический", "Ультразвуковой контроль", "Магнитный контроль", "Пневматический способ контроля"],
        correct=2,
        hint_wrong="Ты не GeeK раз не знаешь о ком идет речь иди почитай комиксы",
        explain_right="речь идет о персонаже под именем Магнето, соответственно магнитный контроль, суть которого заключается в намагничивании проверяемого участка и получении обратно искаженного магнитного силового потока, для него даже не потребует дополнительного оборудования для обнаружения дефектов.",
    ),
    Question(
        text="В произведении Александра Волкова «Волшебник изумрудного города» герои считали себя дефектными и шли к Гудвину, чтобы он помог им. И если у Льва и Страшилы проблемы скорее психологические (один считал, что у него нет храбрости, второй считает что у него нет мозгов), то отсутствие сердца у Железного Дровосека - скорее конструктивный брак, подтвердить который может только дефектоскопист. Какие методы он мог бы использовать?",
        options=["Визуальный", "Ультразвуковой", "Магнитный", "Рентгеновский"],
        correct=1,
        hint_wrong="Не знаю вроде дал нормальное ТЗ и сказал нельзя делать два верных ответа так что если ты думал правильно но ответил неверно все вопросы ЕМС",
        explain_right="визуальный и магнитный методы используют в основном для выявления поверхностных дефектов. Здесь речь скорее подойдет ультразвуковой или рентгеновский метод.",
    ),
    Question(
        text="По версии дефектоскопистов в сказке «Курочка Ряба» был использован один из методов неразрушающего контроля качества золотого яичка. Мышка же, не обладая глубокими знаниями о дефектоскопии, использовала метод разрушающего контроля, разбив яйцо, не разобравшись с полученными результатами. Какой метод использовали дед и баба?",
        options=["Акустический", "Визуальный", "Гидравлический", "Магнитный"],
        correct=0,
        hint_wrong="Сам в шоке от вопросов кинул в воду да посмотрел если не всплыл то все норм, а они давай какие то странные методы используют, мне кажется они с ума сошли но это только мои догадки",
        explain_right="акустический. Дело в том, что дед и баба аккуратно «простукивали» золотое яичко и слушали искажения звука, как обходчики определяют дефекты рельс, ударяя по ним молотком. Яичко было без дефектов, но очень хрупкое.",
    ),
    Question(
        text="В одной из сказок Александра Роу герой должен выбрать свою будущую невесту среди созданных копий. Визуально это сделать невозможно, но герой все-таки справился. Какой метод неразрушающего контроля помог ему в этом?",
        options=["Визуальный", "Акустический", "Тепловой", "Радиографический"],
        correct=2,
        hint_wrong="Если ты думаешь кто это такой я с тобой родной. Взял первую встречную да и все они же копии по логике они одинаковые так че себе проблемы создавать",
        explain_right="тепловой. От живой девушки шло тепло, в то время как копии были холодными. Мамы, даже не будучи дефектоскопистами, тоже применяют этот метод, прикладывая ладонь ко лбу и качественно определяя температуру тела ребенка.",
    ),
    Question(
        text="В знаменитом литературном произведении Р.Р. Толкина все события развиваются вокруг кольца всевластия. Надевая кольцо Фродо становится невидим. Кольцо вероятно смещало оптические характеристики обладателя в невидимую глазу обычного человека область. При этом окружающая действительность меняла свои оптические характеристики, и обладатель кольца становился видим для Всевидящего ока. Согласно расчетам, сделанным энтузиастами, речь идет о инфракрасном излучении. Приставка «инфра-» (от лат. infra-) означает «расположение под чем-либо, ниже чего-либо».  Какой еще вид излучения, не видимый для человеческого глаза, может быть использован в неразрушающем контроле.",
        options=["Ультрачёрный", "Инфрасиний", "Просто белый", "Ультрафиолетовый"],
        correct=3,
        hint_wrong="Я когда увидел ответ ультрачерный чуть со стула не упал. Надеюсь нас не засудят за расизм",
        explain_right="ультрафиолетовый. Диапазон видимого человеком света колеблется в диапазоне длин волн от 380 до 780 нМ. Приставка «ультра-», которая происходит от латинского ultra — «дальше, далее» означает превышение чего-либо, в данном случае пределы видимого света. Но это только для человека. Птицы и насекомые прекрасно видят и в ультрафиолетовом диапазоне.",
    ),
    Question(
        text="Всем известно, что в природе ультразвуковыми методами контроля лучше всех владеют летучие мыши. В темных пещерах мыши сканируют пространство, испуская УЗ импульсы, проверяя пространство на неоднородности Излучателем служат голосовые связки, волна может испускаться через рот или нос. Приемником служат ушные раковины. Летучая мышь – это природный дефектоскоп. Редко бывает, что охотницы остаются без добычи. Что может мешать «сканированию»?",
        options=["Солнечная активность", "Ветер", "Изменение атмосферного давления", "Влажность"],
        correct=3,
        hint_wrong="Тут нечего сказать вроде логичный вопрос если ты конечно закончил университет по летучим мышам.",
        explain_right="погодные условия – повышенная влажность. На каплях воды происходит рассеивание УЗ-волны, крупные капли могут быть ложно восприняты как насекомое.",
    ),
    Question(
        text="Один из известных супергероев носит имя, которое наводит на мысль о том, то он имеет навыки работы с УЗ-дефектоскопами. По легенде, он может видеть сквозь стены, но природа используемых для этого лучей электромагнитная. Образ героя тоже может вводить в заблуждение, но только не того, кто разбирается в физике. Назовите его.",
        options=["Халк", "Супермен", "Бэтмен", "Человек-паук"],
        correct=2,
        hint_wrong="Как я уже говорил если не ответил с первого раза то не Geek",
        explain_right="Бэтмен. Хоть это и Человек-летучая мышь, навыками ультразвуковой интроскопии он не обладает. Ультразвуковые волны по своей природе являются механическими.",
    ),
    Question(
        text="Этим видом неразрушающего контроля пользуются все мальчишки и девчонки, у которых есть велосипед. Он позволяет найти сквозные дефекты на объектах, где герметичность выступает важнейшим эксплуатационным требованием. У велосипедов герметичность важна для камеры колеса. Если колесо начало спускать, то проверяют целостность камеры предварительно накачав ее воздухом и опустив в таз с водой. В полевых условиях каплю воды капают на подозрительное место. Если вода начинает пузыриться, значит камера проколота. Что это за метод?",
        options=["Визуальный", "Течеискание", "Ультразвуковой", "Тепловой"],
        correct=1,
        hint_wrong="Не ну тут стыдно не знать. Хотя если никогда не было велосипеда тогда можно, и да соболезную если у тебя не было велосипеда.",
        explain_right="течеискание. Метод основан на регистрации веществ, проникающих через течи – каналы или пористые участки, нарушающие герметичность конструкции. В данном случае использован пузырьковый метод",
    ),
    Question(
        text="Этот вид неразрушающего контроля очень широко используется на вокзалах и аэропортах. Принцип работы основан на том, что излучение, проникая через объект, поглощается в разной степени в зависимости от плотности материала, датчики улавливают прошедшие лучи и преобразуют их в цифровое изображение, которое выводится на экран. Какой прибор используется?",
        options=["УЗ-дефектоскоп", "Осциллограф", "Рентгеновский сканер", "Вольтметр"],
        correct=2,
        hint_wrong="Промолчу, а хотя тест вроде для детей че так сложно то.",
        explain_right="рентгеновский сканер, который предназначен для проверки багажа на опасные и запрещенные к провозу предметы. Рентгеновский сканер персонального досмотра позволяет обнаружить предметы, материалы и вещества, спрятанные под одеждой.",
    ),
    Question(
        text="Этот прибор вы можете увидеть в обычной школе. Метод, который положен в основу его работы используют инженеры неразрушающего контроля в порошковой дефектоскопии для обнаружения пор и трещин в деталях. Принцип работы основан на том, что при попадании металлического предмета в зону действия однородного магнитного поля возникают вихревые токи, которые создают собственное вторичное электромагнитное поле. Детектор улавливает изменения в результирующем электромагнитном поле. В школе он начинает пищать, предупреждая об опасности. Что это за прибор?",
        options=["Термометр", "Система пожарной сигнализации", "Металлодетектор", "Тревожная кнопка"],
        correct=2,
        hint_wrong="Вот ты не ответил на вопрос, возможно ответ и верный но мне не дали ответ так что я пишу наугад ответ. Ты думаешь это жестоко, но я добавляю эти вопросы в 10 часов вечера, хотя мог бы смотреть мультики.",
        explain_right="Абсолютно верно мой дорогой друг, тут мне ничего не дали так что просто читай текст и трать на это время",
    ),
    Question(
        text="Адвокат Мэтт Мёрдок по прозвищу Сорвиголова согласно комиксу Marvel попал под действие радиоактивного вещества и потерял зрение. При этом у него развилось «радарное чутье» с помощью которого он воспринимает мир вокруг. Эта способность связана со сверхчеловеческой сенсорной, которая включает тактильную чувствительность к малейшим неровностям, обоняние и слух (он слышит биение сердца собеседника). Способ, который он использует для координации в пространстве аналогичен тому, что используют в ультразвуковой дефектоскопии. Что это за метод?",
        options=["Теневой", "Зеркальный", "Дифракционно-временной", "Эхо-метод"],
        correct=3,
        hint_wrong="Я тут подумал вопросы то рандомные то есть тебе может попастья этот вопрос первым, что хочу сказать там есть похожие вопросы там и узнаешь кто не ты.",
        explain_right="эхо-метод, при котором преобразователь генерирует колебания и принимает отраженные от дефектов эхо-сигналы. Размеры и местоположение дефекта оценивают по амплитуде и времени задержки отраженного эхо-сигнала. Мёрдок делает примерно тоже самое. Он производит щелчки, тем самым посылая звуковые импульсы и слушает отражение.",
    ),
    Question(
        text="Дайте определение термину Брак",
        options=["наличия дефектов допускается", "не допускается из-за наличия дефектов", "несоответствие требованиям", "нет правильного ответа"],
        correct=1,
        hint_wrong="а ты думал будут простые вопросы",
        explain_right="Если ответил с первого раза красава респект и уважуха",
    ),
    # ⚠️ ВОТ ЗДЕСЬ ТВОЙ СПИСОК В СООБЩЕНИИ ОБОРВАН.
    # Дальше просто продолжай добавлять Question(...),
]
# ==========================================================
# ====================== КОНЕЦ ВОПРОСОВ ====================
# ==========================================================


# ==========================================================
# ====================== УТИЛИТЫ ===========================
# ==========================================================
def now_ts() -> int:
    return int(time.time())

def is_admin(update: Update) -> bool:
    u = update.effective_user
    return bool(u and u.id in ADMIN_IDS)

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

def fmt_ms(ms: int) -> str:
    sec = ms / 1000.0
    m = int(sec // 60)
    s = sec - 60 * m
    return f"{m}:{s:06.3f}"

def build_quiz_order() -> List[int]:
    if len(QUESTIONS) < QUESTIONS_PER_RUN:
        raise RuntimeError(f"В QUESTIONS={len(QUESTIONS)} вопросов, но QUESTIONS_PER_RUN={QUESTIONS_PER_RUN}. Добавь вопросы или уменьши QUESTIONS_PER_RUN.")
    return random.sample(range(len(QUESTIONS)), k=QUESTIONS_PER_RUN)

def total_time_ms(context: ContextTypes.DEFAULT_TYPE) -> int:
    t0 = float(context.user_data.get("t0", time.time()))
    penalty = int(context.user_data.get("penalty_ms", 0))
    base = int((time.time() - t0) * 1000)
    return base + penalty

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

# ==========================================================
# ====================== POSTGRES ==========================
# ==========================================================
def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан (Railway Variables).")
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
                status TEXT NOT NULL, -- started|finished|quit
                questions_per_run INT NOT NULL,
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

def upsert_user(u) -> Tuple[int, Optional[str], Optional[str]]:
    uid = int(u.id)
    username = u.username
    full_name = u.full_name
    ts = now_ts()
    with db_connect() as con, con.cursor() as cur:
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
    return uid, username, full_name

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
            INSERT INTO attempts(user_id, started_ts, status, questions_per_run, wrong_penalty_ms)
            VALUES(%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (user_id, now_ts(), "started", QUESTIONS_PER_RUN, WRONG_PENALTY_MS),
        )
        attempt_id = int(cur.fetchone()["id"])
        con.commit()
        return attempt_id

def attempt_update_progress(attempt_id: int, wrong_count: int, penalty_ms: int) -> None:
    with db_connect() as con, con.cursor() as cur:
        cur.execute("UPDATE attempts SET wrong_count=%s, penalty_ms=%s WHERE id=%s",
                    (wrong_count, penalty_ms, attempt_id))
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

def log_answer(attempt_id: int, user_id: int, pos: int, question_index: int, option_index: int,
               is_correct: bool, penalty_ms_after: int, total_ms_now: int) -> None:
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
    with db_connect() as con, con.cursor() as cur:
        cur.execute("TRUNCATE TABLE answers RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE attempts RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE events RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE users RESTART IDENTITY")
        con.commit()

# ==========================================================
# ====================== ЛИДЕРЫ ============================
# Лучший total_ms по пользователю
# ==========================================================
def leaderboard_top(limit: int = 10) -> List[Tuple[str, int]]:
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

# ==========================================================
# ====================== STATS (ADMIN) =====================
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
        "Сводка\n\n"
        f"Пользователей: {users}\n"
        f"Попыток: {attempts}\n"
        f"Завершили: {finished}\n"
        f"Сдались: {quits}\n"
        f"Среднее итоговое время: {avg_total_s}\n"
        f"Среднее ошибок: {avg_wrong_s}\n"
        f"Вопросов за тест: {QUESTIONS_PER_RUN}\n"
        f"Штраф за ошибку: {WRONG_PENALTY_MS/1000:.0f} сек\n"
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

    lines = [f"Пользователи (последние {limit})"]
    for r in rows:
        last_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(r["last_seen_ts"])))
        lines.append(f"- {r['name']} (last: {last_s})")
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

    lines = [f"Попытки (последние {limit})"]
    for r in rows:
        total = fmt_ms(int(r["total_ms"])) if r["total_ms"] is not None else "—"
        lines.append(f"- #{r['id']} {r['name']} — {r['status']} — {total} — wrong:{r['wrong_count']} penalty:{fmt_ms(int(r['penalty_ms']))}")
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
        return "Сложные вопросы\n\nПока нет данных (нужно, чтобы кто-то отвечал)."

    lines = ["Сложные вопросы (по ошибкам):"]
    for r in rows:
        qi = int(r["question_index"])
        title = QUESTIONS[qi].text if 0 <= qi < len(QUESTIONS) else f"Вопрос #{qi}"
        lines.append(f"- {title}\n  Ошибок: {int(r['wrongs'])} из {int(r['total'])}")
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

    lines = [f"События (последние {limit})"]
    for r in rows:
        ts_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(r["ts"])))
        lines.append(f"- {ts_s} — {r['name']} — {r['event_type']}")
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
        w.writerow(["id", "user_id", "status", "started_ts", "ended_ts", "wrong_count", "penalty_ms", "elapsed_ms", "total_ms", "questions_per_run", "wrong_penalty_ms"])
        cur.execute("""
            SELECT id, user_id, status, started_ts, ended_ts, wrong_count, penalty_ms, elapsed_ms, total_ms, questions_per_run, wrong_penalty_ms
            FROM attempts
            ORDER BY id DESC
        """)
        for r in cur.fetchall():
            w.writerow([r["id"], r["user_id"], r["status"], r["started_ts"], r["ended_ts"], r["wrong_count"], r["penalty_ms"],
                        r["elapsed_ms"], r["total_ms"], r["questions_per_run"], r["wrong_penalty_ms"]])

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

# ==========================================================
# ====================== UI КНОПКИ =========================
# ==========================================================
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Начать тест", callback_data="start_quiz")],
        [InlineKeyboardButton("Теория", callback_data="theory:0")],
        [InlineKeyboardButton("Лидеры", callback_data="leaderboard")],
        [InlineKeyboardButton("Как играть", callback_data="help")],
    ])

def theory_kb(page: int, total: int) -> InlineKeyboardMarkup:
    prev_btn = InlineKeyboardButton("⬅️", callback_data=f"theory:{page-1}") if page > 0 else InlineKeyboardButton(" ", callback_data="noop")
    next_btn = InlineKeyboardButton("➡️", callback_data=f"theory:{page+1}") if page < total - 1 else InlineKeyboardButton(" ", callback_data="noop")
    return InlineKeyboardMarkup([
        [prev_btn, InlineKeyboardButton("Меню", callback_data="menu"), next_btn],
        [InlineKeyboardButton("Начать тест", callback_data="start_quiz")],
    ])

def quiz_kb(current_q_index: int, options: List[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(opt, callback_data=f"ans:{current_q_index}:{i}")] for i, opt in enumerate(options)]
    rows.append([InlineKeyboardButton("Сдаться", callback_data="quit"), InlineKeyboardButton("Меню", callback_data="menu")])
    rows.append([InlineKeyboardButton("Лидеры", callback_data="leaderboard")])
    return InlineKeyboardMarkup(rows)

def finish_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Пройти ещё раз", callback_data="start_quiz")],
        [InlineKeyboardButton("Лидеры", callback_data="leaderboard")],
        [InlineKeyboardButton("Теория", callback_data="theory:0")],
        [InlineKeyboardButton("Меню", callback_data="menu")],
    ])

def stats_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сводка", callback_data="stats:overview")],
        [InlineKeyboardButton("Пользователи", callback_data="stats:users")],
        [InlineKeyboardButton("Попытки", callback_data="stats:attempts")],
        [InlineKeyboardButton("Сложные вопросы", callback_data="stats:hard")],
        [InlineKeyboardButton("События", callback_data="stats:events")],
        [InlineKeyboardButton("Экспорт CSV", callback_data="stats:export")],
        [InlineKeyboardButton("Очистить статистику", callback_data="stats:clear_confirm")],
        [InlineKeyboardButton("Меню", callback_data="menu")],
    ])

def clear_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ДА, очистить", callback_data="stats:clear_yes")],
        [InlineKeyboardButton("Отмена", callback_data="stats:clear_no")],
    ])

# ==========================================================
# ====================== ЭКРАНЫ ============================
# ==========================================================
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "menu_open")

    await send(
        update,
        f"Привет!\n\n"
        f"Тест: {QUESTIONS_PER_RUN} вопросов (случайно из общего списка)\n"
        f"Штраф за ошибку: +{int(WRONG_PENALTY_MS/1000)} сек\n\n"
        f"Выбирай действие:",
        reply_markup=main_menu_kb(),
    )

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "help_open")

    await send(
        update,
        "Как играть:\n\n"
        "1) Нажми «Начать тест»\n"
        "2) Отвечай кнопками\n"
        f"3) Неверно — штраф +{int(WRONG_PENALTY_MS/1000)} сек\n"
        "4) Верно — следующий вопрос\n"
        "5) В конце будет итог и поздравление\n",
        reply_markup=main_menu_kb(),
    )

async def show_theory(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "theory_open", payload_json=f'{{"page":{page}}}')

    pages = chunk_text(THEORY_TEXT)
    page = max(0, min(page, len(pages) - 1))
    await send(
        update,
        f"Теория ({page+1}/{len(pages)})\n\n{pages[page]}",
        reply_markup=theory_kb(page, len(pages)),
    )

async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "leaderboard_open")

    rows = leaderboard_top(10)
    if not rows:
        await send(update, "Пока нет результатов. Нажми «Начать тест».", reply_markup=main_menu_kb())
        return

    lines = ["Лидеры (лучшее итоговое время):"]
    for i, (name, ms) in enumerate(rows, 1):
        lines.append(f"{i}. {name} — {fmt_ms(ms)}")
    await send(update, "\n".join(lines), reply_markup=main_menu_kb())

# ==========================================================
# ====================== ТЕСТ ==============================
# ==========================================================
async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "quiz_start_clicked")

    order = build_quiz_order()
    context.user_data["order"] = order
    context.user_data["pos"] = 0
    context.user_data["t0"] = time.time()
    context.user_data["penalty_ms"] = 0
    context.user_data["wrong_count"] = 0

    attempt_id = None
    if u:
        attempt_id = attempt_start(int(u.id))
        context.user_data["attempt_id"] = attempt_id
        log_event(int(u.id), "attempt_started", payload_json=f'{{"attempt_id":{attempt_id}}}')

    await send(update, "Поехали!", reply_markup=None)
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
    caption = (
        f"Вопрос {pos+1}/{len(order)}\n"
        f"Время сейчас: {fmt_ms(total_now)} (штраф: {fmt_ms(penalty)})\n\n"
        f"{q.text}"
    )

    kb = quiz_kb(q_index, q.options)

    if q.photo_path:
        try:
            await send_photo(update, q.photo_path, caption=caption, reply_markup=kb)
            return
        except FileNotFoundError:
            caption += "\n\n(Картинка не найдена)"

    await send(update, caption, reply_markup=kb)

async def finish_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE, status: str):
    u = update.effective_user
    attempt_id = context.user_data.get("attempt_id")

    wrong = int(context.user_data.get("wrong_count", 0))
    penalty = int(context.user_data.get("penalty_ms", 0))
    elapsed = int((time.time() - float(context.user_data.get("t0", time.time()))) * 1000)
    total = elapsed + penalty

    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "attempt_ended", payload_json=f'{{"status":"{status}","wrong":{wrong},"penalty_ms":{penalty},"total_ms":{total}}}')

    if attempt_id is not None:
        attempt_finish(int(attempt_id), status=status, elapsed_ms=elapsed, penalty_ms=penalty, wrong_count=wrong)

    # очистим сессию
    for k in ["order", "pos", "t0", "penalty_ms", "wrong_count", "attempt_id"]:
        context.user_data.pop(k, None)

    if status == "quit":
        await send(update, "Ок, попытка остановлена.", reply_markup=main_menu_kb())
        return

    # Финальная фраза (как ты просил)
    await send(
        update,
        "Молодец теперь ты профессионал\n\n"
        f"Итоговое время: {fmt_ms(total)}\n"
        f"Ошибок: {wrong} (штраф: {fmt_ms(penalty)})",
        reply_markup=finish_kb(),
    )

async def quit_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "quiz_quit_clicked")
    await finish_quiz(update, context, status="quit")

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE, q_index: int, opt: int):
    query = update.callback_query
    u = update.effective_user

    order: List[int] = context.user_data.get("order", [])
    pos = int(context.user_data.get("pos", 0))
    attempt_id = context.user_data.get("attempt_id")

    if not order or pos >= len(order):
        await query.message.reply_text("Сессия не активна. Нажми «Начать тест».")
        return

    current_q_index = order[pos]
    if q_index != current_q_index:
        await query.message.reply_text("Это старые кнопки. Начни тест заново.")
        return

    q = QUESTIONS[current_q_index]
    total_before = total_time_ms(context)

    if u:
        uid, _, _ = upsert_user(u)
        log_event(uid, "answer_clicked", payload_json=f'{{"q":{current_q_index},"opt":{opt}}}')

    if opt == q.correct:
        if u and attempt_id is not None:
            penalty_after = int(context.user_data.get("penalty_ms", 0))
            log_answer(int(attempt_id), int(u.id), pos, current_q_index, opt, True, penalty_after, total_before)

        context.user_data["pos"] = pos + 1
        await query.message.reply_text("Верно!\n" + q.explain_right)
        await show_question(update, context)
        return

    # неверно -> штраф
    context.user_data["penalty_ms"] = int(context.user_data.get("penalty_ms", 0)) + WRONG_PENALTY_MS
    context.user_data["wrong_count"] = int(context.user_data.get("wrong_count", 0)) + 1

    penalty_after = int(context.user_data.get("penalty_ms", 0))
    wrong_count = int(context.user_data.get("wrong_count", 0))

    if attempt_id is not None:
        attempt_update_progress(int(attempt_id), wrong_count, penalty_after)

    total_after = total_time_ms(context)

    if u and attempt_id is not None:
        log_answer(int(attempt_id), int(u.id), pos, current_q_index, opt, False, penalty_after, total_after)

    await query.message.reply_text(
        f"Неверно! +{int(WRONG_PENALTY_MS/1000)} сек штраф.\n"
        f"Подсказка: {q.hint_wrong}\n"
        "Попробуй ещё раз."
    )

# ==========================================================
# ====================== ADMIN COMMANDS ====================
# ==========================================================
async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    uid, _, _ = upsert_user(u)
    log_event(uid, "cmd_myid")
    await update.message.reply_text(f"Твой user_id: {uid}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    uid, _, _ = upsert_user(u)
    log_event(uid, "cmd_stats")

    if not is_admin(update):
        await update.message.reply_text("Нет доступа.")
        return

    await update.message.reply_text("Меню статистики (только админ):", reply_markup=stats_menu_kb())

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
            "ВНИМАНИЕ! Это удалит ВСЮ статистику.\nТочно очистить?",
            reply_markup=clear_confirm_kb(),
        )
    elif action == "clear_yes":
        db_clear_all()
        await send(update, "Статистика очищена.", reply_markup=stats_menu_kb())
    elif action == "clear_no":
        await send(update, "Ок, отменено.", reply_markup=stats_menu_kb())
    else:
        await send(update, "Неизвестная команда.", reply_markup=stats_menu_kb())

# ==========================================================
# ====================== ROUTER ============================
# ==========================================================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    u = update.effective_user
    if u:
        uid, _, _ = upsert_user(u)
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

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context)

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    # ошибки будут видны в Railway logs
    print("ERROR:", repr(context.error))

def ensure_ready():
    print("BOOT: BOT_TOKEN:", bool(BOT_TOKEN))
    print("BOOT: DATABASE_URL:", bool(DATABASE_URL))
    print("BOOT: ADMIN_IDS:", ADMIN_IDS)
    print("BOOT: QUESTIONS:", len(QUESTIONS))
    print("BOOT: QUESTIONS_PER_RUN:", QUESTIONS_PER_RUN)
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан (Railway Variables).")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан (Railway Variables).")
    if len(QUESTIONS) < QUESTIONS_PER_RUN:
        raise RuntimeError("Недостаточно вопросов в QUESTIONS.")

def main():
    ensure_ready()
    db_init()
    print("BOOT: db_init OK")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("stats", cmd_stats))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("BOOT: polling start")
    app.run_polling()

if __name__ == "__main__":
    main()
