import os
import time
import threading
import sqlite3
import random
from typing import Optional, List, Dict, Any, Tuple

import requests
import telebot
from telebot import types

from dotenv import load_dotenv
load_dotenv()

# =========================
#  Настройки
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = "https://api.themoviedb.org/3"

if not BOT_TOKEN or not TMDB_API_KEY:
    raise RuntimeError("BOT_TOKEN и TMDB_API_KEY должны быть заданы в переменных окружения")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

DB_PATH = "cinemate_v2.db"

# Жанры TMDb (id -> название по-русски)
TMDB_GENRES = {
    28: "Боевик",
    12: "Приключения",
    16: "Анимация",
    35: "Комедия",
    80: "Криминал",
    99: "Документальный",
    18: "Драма",
    10751: "Семейный",
    14: "Фэнтези",
    36: "Исторический",
    27: "Ужасы",
    10402: "Музыка",
    9648: "Детектив",
    10749: "Романтика",
    878: "Фантастика",
    10770: "ТВ фильм",
    53: "Триллер",
    10752: "Военный",
    37: "Вестерн",
}


# =========================
#  БД
# =========================

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER UNIQUE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_states (
            user_id INTEGER PRIMARY KEY,
            state TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            title TEXT,
            media_type TEXT,
            UNIQUE(user_id, tmdb_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            genre_id INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            status TEXT,      -- watched / unseen / favorite / skipped
            weight INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS calibration_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            title TEXT,
            media_type TEXT,
            status TEXT,
            shown INTEGER DEFAULT 0
        )
    """)

    # На случай старой таблицы без shown
    try:
        c.execute("ALTER TABLE calibration_items ADD COLUMN shown INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    c.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            title TEXT,
            media_type TEXT,  -- 'tv'
            last_air_date TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            title TEXT,
            media_type TEXT,
            UNIQUE(user_id, tmdb_id)
        )
    """)

    conn.commit()
    conn.close()


def get_user_id(chat_id: int) -> int:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    if row:
        user_id = row[0]
        conn.close()
        return user_id
    try:
        c.execute("INSERT INTO users (chat_id) VALUES (?)", (chat_id,))
        conn.commit()
        user_id = c.lastrowid
    except sqlite3.IntegrityError:
        c.execute("SELECT id FROM users WHERE chat_id=?", (chat_id,))
        row = c.fetchone()
        user_id = row[0]
    conn.close()
    return user_id


def get_chat_id(user_id: int) -> Optional[int]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT chat_id FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def set_state(user_id: int, state: Optional[str]):
    conn = get_conn()
    c = conn.cursor()
    if state is None:
        c.execute("DELETE FROM user_states WHERE user_id=?", (user_id,))
    else:
        c.execute("""
            INSERT INTO user_states (user_id, state)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET state=excluded.state
        """, (user_id, state))
    conn.commit()
    conn.close()


def get_state(user_id: int) -> Optional[str]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT state FROM user_states WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def count_favorites(user_id: int) -> int:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM favorites WHERE user_id=?", (user_id,))
    n = c.fetchone()[0]
    conn.close()
    return n


def add_favorite(user_id: int, tmdb_id: int, title: str, media_type: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO favorites (user_id, tmdb_id, title, media_type)
        VALUES (?, ?, ?, ?)
    """, (user_id, tmdb_id, title, media_type))
    conn.commit()
    conn.close()


def get_favorites(user_id: int) -> List[Tuple[int, str, str]]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT tmdb_id, title, media_type FROM favorites WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def get_user_genres(user_id: int) -> List[int]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT genre_id FROM user_genres WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]


def toggle_user_genre(user_id: int, genre_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM user_genres WHERE user_id=? AND genre_id=?", (user_id, genre_id))
    row = c.fetchone()
    if row:
        c.execute("DELETE FROM user_genres WHERE id=?", (row[0],))
    else:
        c.execute("INSERT INTO user_genres (user_id, genre_id) VALUES (?, ?)", (user_id, genre_id))
    conn.commit()
    conn.close()


def add_feedback(user_id: int, tmdb_id: int, status: str):
    weight_map = {"watched": 1, "unseen": 0, "favorite": 5, "skipped": -2}
    weight = weight_map.get(status, 0)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO user_feedback (user_id, tmdb_id, status, weight)
        VALUES (?, ?, ?, ?)
    """, (user_id, tmdb_id, status, weight))
    conn.commit()
    conn.close()


def get_feedback_weights(user_id: int) -> Dict[int, int]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT tmdb_id, weight FROM user_feedback WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def add_calibration_items(user_id: int, items: List[Dict[str, Any]]):
    conn = get_conn()
    c = conn.cursor()
    for it in items:
        tmdb_id = it["id"]
        media_type = it.get("media_type") or ("tv" if it.get("name") else "movie")
        title = build_display_title(it, "Без названия")
        c.execute("""
            INSERT OR IGNORE INTO calibration_items (user_id, tmdb_id, title, media_type, status, shown)
            VALUES (?, ?, ?, ?, NULL, 0)
        """, (user_id, tmdb_id, title, media_type))
    conn.commit()
    conn.close()


def get_next_calibration_batch(user_id: int, limit: int = 3) -> List[Tuple[int, int, str, str]]:
    """Берём только те, что ещё НЕ показывали (shown=0)."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, tmdb_id, title, media_type
        FROM calibration_items
        WHERE user_id=? AND shown=0
        LIMIT ?
    """, (user_id, limit))
    rows = c.fetchall()
    if rows:
        ids = [str(r[0]) for r in rows]
        q = f"UPDATE calibration_items SET shown=1 WHERE id IN ({','.join(ids)})"
        c.execute(q)
        conn.commit()
    conn.close()
    return rows


def set_calibration_status(row_id: int, status: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE calibration_items SET status=? WHERE id=?", (status, row_id))
    conn.commit()
    conn.close()


def add_subscription_for_tv(user_id: int, tmdb_id: int, title: str, last_air_date: Optional[str]):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO subscriptions (user_id, tmdb_id, title, media_type, last_air_date)
        VALUES (?, ?, ?, 'tv', ?)
    """, (user_id, tmdb_id, title, last_air_date))
    conn.commit()
    conn.close()


def get_subscriptions(user_id: int) -> List[Tuple[int, str, str, Optional[str]]]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT tmdb_id, title, media_type, last_air_date
        FROM subscriptions WHERE user_id=?
    """, (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def update_subscription_last_air_date(user_id: int, tmdb_id: int, last_air_date: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE subscriptions
        SET last_air_date=?
        WHERE user_id=? AND tmdb_id=?
    """, (last_air_date, user_id, tmdb_id))
    conn.commit()
    conn.close()


def is_subscribed(user_id: int, tmdb_id: int) -> bool:
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT 1 FROM subscriptions
        WHERE user_id=? AND tmdb_id=?
        LIMIT 1
    """, (user_id, tmdb_id))
    row = c.fetchone()
    conn.close()
    return row is not None


def remove_subscription(user_id: int, tmdb_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM subscriptions WHERE user_id=? AND tmdb_id=?", (user_id, tmdb_id))
    conn.commit()
    conn.close()


def add_watchlist_item(user_id: int, tmdb_id: int, title: str, media_type: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO watchlist (user_id, tmdb_id, title, media_type)
        VALUES (?, ?, ?, ?)
    """, (user_id, tmdb_id, title, media_type))
    conn.commit()
    conn.close()


def remove_watchlist_item(user_id: int, tmdb_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM watchlist WHERE user_id=? AND tmdb_id=?", (user_id, tmdb_id))
    conn.commit()
    conn.close()


def get_watchlist(user_id: int) -> List[Tuple[int, str, str]]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT tmdb_id, title, media_type
        FROM watchlist
        WHERE user_id=?
    """, (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def is_in_watchlist(user_id: int, tmdb_id: int) -> bool:
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT 1 FROM watchlist
        WHERE user_id=? AND tmdb_id=?
        LIMIT 1
    """, (user_id, tmdb_id))
    row = c.fetchone()
    conn.close()
    return row is not None


def get_title_from_db_any(user_id: int, tmdb_id: int) -> Tuple[Optional[str], Optional[str]]:
    """Пробуем достать title+media_type из favorites или watchlist."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT title, media_type FROM favorites
        WHERE user_id=? AND tmdb_id=?
    """, (user_id, tmdb_id))
    row = c.fetchone()
    if not row:
        c.execute("""
            SELECT title, media_type FROM watchlist
            WHERE user_id=? AND tmdb_id=?
        """, (user_id, tmdb_id))
        row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None, None


# =========================
#  TMDb helpers
# =========================

def tmdb_get(endpoint: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = dict(params)
    params["api_key"] = TMDB_API_KEY
    params.setdefault("language", "ru-RU")
    url = f"{TMDB_BASE_URL}{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"TMDb error: {e}")
        return None


def build_display_title(data: Dict[str, Any], fallback: str = "Без названия") -> str:
    ru = data.get("title") or data.get("name")
    orig = data.get("original_title") or data.get("original_name")
    if ru and orig and ru != orig:
        return f"{ru} / {orig}"
    return ru or orig or fallback


def search_tmdb_multi(query: str) -> Optional[Dict[str, Any]]:
    data = tmdb_get("/search/multi", {"query": query})
    if not data or not data.get("results"):
        return None
    for r in data["results"]:
        if r.get("media_type") in ("movie", "tv"):
            return r
    return None


def get_tmdb_details(media_type: str, tmdb_id: int) -> Optional[Dict[str, Any]]:
    if media_type not in ("movie", "tv"):
        return None
    return tmdb_get(f"/{media_type}/{tmdb_id}", {})


def get_external_ids(media_type: str, tmdb_id: int) -> Optional[Dict[str, Any]]:
    if media_type not in ("movie", "tv"):
        return None
    return tmdb_get(f"/{media_type}/{tmdb_id}/external_ids", {})


def get_similar_and_recommended(media_type: str, tmdb_id: int) -> List[Dict[str, Any]]:
    res: List[Dict[str, Any]] = []
    for kind in ("similar", "recommendations"):
        data = tmdb_get(f"/{media_type}/{tmdb_id}/{kind}", {})
        if data and data.get("results"):
            res.extend(data["results"])
    return res


# =========================
#  Онбординг: жанры
# =========================

def build_genre_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    user_genres = set(get_user_genres(user_id))
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for gid, name in TMDB_GENRES.items():
        text = f"✅ {name}" if gid in user_genres else name
        buttons.append(types.InlineKeyboardButton(text, callback_data=f"genre:{gid}"))
    for i in range(0, len(buttons), 2):
        kb.row(*buttons[i:i + 2])
    kb.row(types.InlineKeyboardButton("Готово", callback_data="genre_done"))
    return kb


def start_genre_selection(chat_id: int, user_id: int):
    set_state(user_id, "genre_select")
    kb = build_genre_keyboard(user_id)
    bot.send_message(
        chat_id,
        "Теперь выберем жанры, которые тебе особенно заходят.\n"
        "Можешь отметить несколько. Потом нажми «Готово».",
        reply_markup=kb
    )


# =========================
#  Калибровка похожего (look-alike)
# =========================

def build_calibration_candidates(user_id: int, max_per_fav: int = 10):
    favorites = get_favorites(user_id)
    candidates: Dict[int, Dict[str, Any]] = {}
    for tmdb_id, title, media_type in favorites:
        items = get_similar_and_recommended(media_type, tmdb_id) or []
        for it in items[:max_per_fav]:
            cid = it["id"]
            if cid not in candidates:
                candidates[cid] = it
    add_calibration_items(user_id, list(candidates.values()))


def send_calibration_batch(chat_id: int, user_id: int):
    """
    Показываем максимум 3 тайтла, которые ещё не показывали (shown=0).
    Новую тройку шлём только когда текущие все оценены.
    """
    batch = get_next_calibration_batch(user_id, limit=3)
    if not batch:
        set_state(user_id, None)
        bot.send_message(
            chat_id,
            "Спасибо! Я примерно понял твой вкус.\n"
            "Теперь можешь запросить рекомендации командой /recommend."
        )
        return

    for row_id, tmdb_id, title, media_type in batch:
        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("Смотрел", callback_data=f"calib:{row_id}:watched"),
            types.InlineKeyboardButton("Не смотрел", callback_data=f"calib:{row_id}:unseen"),
        )
        kb.row(
            types.InlineKeyboardButton("❤️ Попал в сердечко", callback_data=f"calib:{row_id}:favorite")
        )
        kind = "Фильм" if media_type == "movie" else "Сериал"
        bot.send_message(
            chat_id,
            f"<b>{title}</b>\n<i>{kind}</i>\n\n"
            "Отметь свою реакцию:",
            reply_markup=kb
        )


# =========================
#  Рекомендации
# =========================

def build_recommendations(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    favorites = get_favorites(user_id)
    if not favorites:
        return []

    user_genres = set(get_user_genres(user_id))
    feedback_weights = get_feedback_weights(user_id)
    watchlist_ids = {row[0] for row in get_watchlist(user_id)}

    candidate_scores: Dict[int, Dict[str, Any]] = {}

    # собираем кандидатов
    for tmdb_id, title, media_type in favorites:
        items = get_similar_and_recommended(media_type, tmdb_id) or []

        for it in items:
            cid = it["id"]

            # не рекомендовать то, что уже в избранном или уже в плейлисте
            if any(cid == f[0] for f in favorites):
                continue
            if cid in watchlist_ids:
                continue

            cmedia = it.get("media_type") or ("tv" if it.get("name") else "movie")
            display_title = build_display_title(it, "Без названия")
            genres = it.get("genre_ids") or []
            rating = it.get("vote_average") or 0.0
            popularity = it.get("popularity") or 0.0

            data = candidate_scores.setdefault(
                cid,
                {
                    "tmdb_id": cid,
                    "title": display_title,
                    "media_type": cmedia,
                    "genres": genres,
                    "rating": rating,
                    "popularity": popularity,
                    "freq": 0,
                    "score": 0.0,
                }
            )
            data["freq"] += 1

    ranked_list: List[Dict[str, Any]] = []

    # скоринг
    for cid, data in candidate_scores.items():
        feedback_bonus = feedback_weights.get(cid, 0)
        # если пользователь явно скипнул похожее, больше не предлагаем
        if feedback_bonus < 0:
            continue

        genres = set(data["genres"])
        genre_overlap = len(genres & user_genres)
        rating = data["rating"]
        popularity = data["popularity"]
        freq = data["freq"]

        score = (
            2.3 * freq +
            1.2 * genre_overlap +
            1.0 * rating +
            0.6 * (popularity / 10.0) +
            2.5 * feedback_bonus
        )

        # лёгкий шум для разнообразия
        score += random.uniform(-0.3, 0.3)

        data["score"] = score
        ranked_list.append(data)

    ranked = sorted(ranked_list, key=lambda x: x["score"], reverse=True)
    return ranked[:limit]


# =========================
#  Слежение за сериалами
# =========================

def subscription_worker():
    while True:
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute("""
                SELECT DISTINCT user_id, tmdb_id, title, last_air_date
                FROM subscriptions
            """)
            subs = c.fetchall()
            conn.close()

            for user_id, tmdb_id, title, last_air_date in subs:
                details = get_tmdb_details("tv", tmdb_id)
                if not details:
                    continue
                new_last_air_date = details.get("last_air_date")
                if new_last_air_date and new_last_air_date != last_air_date:
                    update_subscription_last_air_date(user_id, tmdb_id, new_last_air_date)
                    chat_id = get_chat_id(user_id)
                    if chat_id:
                        bot.send_message(
                            chat_id,
                            f"📺 У сериала <b>{title}</b> появился новый сезон/эпизод.\n"
                            f"Дата последнего выхода: {new_last_air_date}"
                        )
        except Exception as e:
            print(f"subscription_worker error: {e}")

        time.sleep(3600)  # раз в час; можно увеличить


# =========================
#  Хэндлеры
# =========================

@bot.message_handler(commands=['start'])
def handle_start(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    fav_count = count_favorites(user_id)

    if fav_count == 0:
        set_state(user_id, "await_favorites")
        bot.send_message(
            chat_id,
            "Привет! Я помогу подобрать фильмы и сериалы под твой вкус.\n\n"
            "Для начала отправь 3–10 любимых фильмов/сериалов через запятую.\n"
            "Например:\n"
            "<i>Во все тяжкие, Интерстеллар, Мистер Робот</i>"
        )
    else:
        set_state(user_id, None)
        bot.send_message(
            chat_id,
            "С возвращением!\n\n"
            "Команды:\n"
            "• /recommend — подобрать, что посмотреть\n"
            "• /mylikes — показать твой список любимых\n"
            "• /watchlist — плейлист «Буду смотреть»\n"
            "• /mysubs — сериалы, за которыми я слежу\n"
            "• /help — подсказка по функциям"
        )


@bot.message_handler(commands=['help'])
def handle_help(message: types.Message):
    bot.send_message(
        message.chat.id,
        "Я подбираю фильмы и сериалы под твой вкус.\n\n"
        "Как со мной работать:\n"
        "1. Добавь любимые тайтлы через /start (онбординг).\n"
        "2. Отметь жанры, которые нравятся.\n"
        "3. Оцени похожие фильмы/сериалы (Смотрел / Не смотрел / ❤️).\n"
        "4. Используй /recommend, чтобы получить подборку.\n\n"
        "Сервисные команды:\n"
        "• /mylikes — твой список любимых (по карточкам)\n"
        "• /watchlist — плейлист «Буду смотреть»\n"
        "• /mysubs — сериалы под слежением\n"
        "• /recommend — свежие рекомендации"
    )


@bot.message_handler(commands=['mylikes'])
def handle_mylikes(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    favs = get_favorites(user_id)
    if not favs:
        bot.send_message(chat_id, "Пока нет любимых. Запусти /start и добавь несколько.")
        return

    bot.send_message(chat_id, "<b>Твои любимые:</b>")  # заголовок

    for tmdb_id, title, media_type in favs:
        kind = "Фильм" if media_type == "movie" else "Сериал"
        text = f"<b>{title}</b>\n<i>{kind}</i>"

        kb = types.InlineKeyboardMarkup()

        # кнопка плейлиста
        if not is_in_watchlist(user_id, tmdb_id):
            kb.add(types.InlineKeyboardButton(
                "➕ Буду смотреть",
                callback_data=f"wl_add:{tmdb_id}:{media_type}"
            ))

        # кнопка слежения за сезонами, если сериал
        if media_type == "tv":
            if is_subscribed(user_id, tmdb_id):
                kb.add(types.InlineKeyboardButton(
                    "🔕 Не следить за сезонами",
                    callback_data=f"subs_toggle:{tmdb_id}"
                ))
            else:
                kb.add(types.InlineKeyboardButton(
                    "🔔 Следить за сезонами",
                    callback_data=f"subs_toggle:{tmdb_id}"
                ))

        bot.send_message(chat_id, text, reply_markup=kb)


@bot.message_handler(commands=['mysubs'])
def handle_mysubs(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    subs = get_subscriptions(user_id)

    text_lines = []
    if subs:
        text_lines.append("<b>Сериалы, за которыми я слежу:</b>")
        for tmdb_id, title, media_type, last_air_date in subs:
            line = f"• {title}"
            if last_air_date:
                line += f" — последний выход: {last_air_date}"
            text_lines.append(line)
    else:
        text_lines.append("Пока я ни за одним сериалом не слежу.")

    text_lines.append("")
    text_lines.append("Можешь добавить новый сериал для слежения — просто нажми кнопку ниже.")
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("➕ Добавить сериал для слежения", callback_data="subs_add"))
    bot.send_message(chat_id, "\n".join(text_lines), reply_markup=kb)


@bot.message_handler(commands=['watchlist'])
def handle_watchlist(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    wl = get_watchlist(user_id)

    if not wl:
        bot.send_message(
            chat_id,
            "Твой плейлист «Буду смотреть» пока пуст.\n"
            "Запроси /recommend и добавь туда то, что заинтересовало."
        )
        return

    movies = [row for row in wl if row[2] == "movie"]
    tvs = [row for row in wl if row[2] == "tv"]

    lines = ["<b>Плейлист «Буду смотреть»:</b>"]
    if movies:
        lines.append("\n<b>Фильмы:</b>")
        for tmdb_id, title, _ in movies:
            lines.append(f"• {title}")
    if tvs:
        lines.append("\n<b>Сериалы:</b>")
        for tmdb_id, title, _ in tvs:
            lines.append(f"• {title}")

    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(commands=['recommend'])
def handle_recommend(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    recs = build_recommendations(user_id, limit=6)
    if not recs:
        bot.send_message(
            chat_id,
            "Пока мало данных для рекомендаций.\n"
            "Запусти /start и добавь любимые тайтлы, а затем пройди калибровку."
        )
        return

    bot.send_message(chat_id, "<b>Вот что может зайти:</b>")

    for item in recs:
        tmdb_id = item["tmdb_id"]
        title = item["title"]
        media_type = item["media_type"]
        rating = item["rating"]
        genres_ids = item["genres"]

        kind = "Фильм" if media_type == "movie" else "Сериал"
        genres = [TMDB_GENRES.get(gid, "") for gid in genres_ids]
        genres_str = ", ".join([g for g in genres if g])

        # детали + внешние ID для ссылок
        details = get_tmdb_details(media_type, tmdb_id) or {}
        ext = get_external_ids(media_type, tmdb_id) or {}
        imdb_id = ext.get("imdb_id")

        tmdb_url = f"https://www.themoviedb.org/{'movie' if media_type == 'movie' else 'tv'}/{tmdb_id}"
        imdb_url = f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else None

        text_lines = [f"<b>{title}</b>", f"<i>{kind}</i>"]
        if genres_str:
            text_lines.append(f"Жанры: {genres_str}")
        if rating:
            text_lines.append(f"Рейтинг TMDb: {rating:.1f}")

        links = [f'<a href="{tmdb_url}">TMDb</a>']
        if imdb_url:
            links.append(f'<a href="{imdb_url}">IMDb</a>')
        text_lines.append("")
        text_lines.append(" / ".join(links))

        text = "\n".join(text_lines)

        kb = types.InlineKeyboardMarkup()

        # плейлист
        if not is_in_watchlist(user_id, tmdb_id):
            kb.add(types.InlineKeyboardButton(
                "➕ Буду смотреть",
                callback_data=f"wl_add:{tmdb_id}:{media_type}"
            ))

        # слежение за сезонами
        if media_type == "tv":
            if is_subscribed(user_id, tmdb_id):
                kb.add(types.InlineKeyboardButton(
                    "🔕 Не следить за сезонами",
                    callback_data=f"subs_toggle:{tmdb_id}"
                ))
            else:
                kb.add(types.InlineKeyboardButton(
                    "🔔 Следить за сезонами",
                    callback_data=f"subs_toggle:{tmdb_id}"
                ))

        # скип
        kb.add(types.InlineKeyboardButton(
            "👎 Пропустить похожее",
            callback_data=f"rec_skip:{tmdb_id}"
        ))

        bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)


# =========================
#  Обработка текста по состояниям
# =========================

@bot.message_handler(func=lambda m: True)
def handle_text(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    state = get_state(user_id)

    if state == "await_favorites":
        handle_await_favorites(message, user_id)
    elif state == "await_subscribe_title":
        handle_await_subscribe_title(message, user_id)
    else:
        bot.send_message(
            chat_id,
            "Я тебя услышал, но пока лучше пользоваться командами:\n"
            "/recommend, /mylikes, /watchlist, /mysubs, /help"
        )


def handle_await_favorites(message: types.Message, user_id: int):
    chat_id = message.chat.id
    raw = message.text or ""
    titles = [t.strip() for t in raw.split(",") if t.strip()]
    if not titles:
        bot.send_message(chat_id, "Не увидел названий. Напиши через запятую 3–10 фильмов/сериалов.")
        return

    for t in titles:
        result = search_tmdb_multi(t)
        if not result:
            bot.send_message(chat_id, f"Не нашёл ничего подходящего для: {t}")
            continue
        tmdb_id = result["id"]
        media_type = result.get("media_type") or ("tv" if result.get("name") else "movie")
        display_title = build_display_title(result, t)
        add_favorite(user_id, tmdb_id, display_title, media_type)
        bot.send_message(
            chat_id,
            f"Добавил в любимые: <b>{display_title}</b> ({'сериал' if media_type == 'tv' else 'фильм'})"
        )

    total = count_favorites(user_id)
    if total < 3:
        bot.send_message(
            chat_id,
            f"Сейчас у тебя {total} любимых в базе. Добавь ещё пару, чтобы я точнее понял вкус."
        )
        return

    bot.send_message(
        chat_id,
        f"Отлично, у тебя уже {total} любимых тайтлов.\n"
        "Теперь давай уточним жанры."
    )
    start_genre_selection(chat_id, user_id)


def handle_await_subscribe_title(message: types.Message, user_id: int):
    chat_id = message.chat.id
    query = (message.text or "").strip()
    if not query:
        bot.send_message(chat_id, "Напиши название сериала, пожалуйста.")
        return

    result = search_tmdb_multi(query)
    if not result or result.get("media_type") != "tv":
        bot.send_message(chat_id, "Не нашёл сериал с таким названием. Попробуй сформулировать по-другому.")
        return

    tmdb_id = result["id"]
    display_title = build_display_title(result, query)
    details = get_tmdb_details("tv", tmdb_id) or {}
    last_air_date = details.get("last_air_date")

    add_subscription_for_tv(user_id, tmdb_id, display_title, last_air_date)
    set_state(user_id, None)
    bot.send_message(
        chat_id,
        f"Теперь я слежу за новыми сезонами сериала <b>{display_title}</b>."
    )


# =========================
#  Callback'и (жанры, калибровка, плейлист, подписки)
# =========================

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    user_id = get_user_id(chat_id)
    data = call.data or ""

    # Жанры
    if data.startswith("genre:"):
        _, gid_str = data.split(":", 1)
        gid = int(gid_str)
        toggle_user_genre(user_id, gid)
        kb = build_genre_keyboard(user_id)
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=kb)
        bot.answer_callback_query(call.id)
        return

    if data == "genre_done":
        bot.answer_callback_query(call.id)
        set_state(user_id, "calibration")
        bot.send_message(
            chat_id,
            "Теперь покажу несколько похожих тайтлов.\n"
            "Отметь: «Смотрел», «Не смотрел» или «❤️ Попал в сердечко»."
        )
        build_calibration_candidates(user_id)
        send_calibration_batch(chat_id, user_id)
        return

    # Калибровка
    if data.startswith("calib:"):
        # calib:<row_id>:<status>
        _, row_id_str, status = data.split(":", 2)
        row_id = int(row_id_str)
        set_calibration_status(row_id, status)

        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT tmdb_id, title, media_type FROM calibration_items WHERE id=?", (row_id,))
        row = c.fetchone()
        if row:
            tmdb_id, title, media_type = row
            add_feedback(user_id, tmdb_id, status)
            if status == "favorite":
                add_favorite(user_id, tmdb_id, title, media_type)
                if media_type == "tv":
                    details = get_tmdb_details("tv", tmdb_id) or {}
                    last_air_date = details.get("last_air_date")
                    add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)

        # остались ли среди показанных неоценённые
        c.execute("""
            SELECT COUNT(*) FROM calibration_items
            WHERE user_id=? AND shown=1 AND status IS NULL
        """, (user_id,))
        remaining = c.fetchone()[0]
        conn.close()

        bot.answer_callback_query(call.id, "Сохранил 👍")

        if remaining == 0 and get_state(user_id) == "calibration":
            send_calibration_batch(chat_id, user_id)
        return

    # Добавить сериал для слежения из /mysubs
    if data == "subs_add":
        bot.answer_callback_query(call.id)
        set_state(user_id, "await_subscribe_title")
        bot.send_message(
            chat_id,
            "Напиши название сериала, за которым хочешь, чтобы я следил."
        )
        return

    # Тоггл подписки на сериал
    if data.startswith("subs_toggle:"):
        _, tmdb_id_str = data.split(":", 1)
        tmdb_id = int(tmdb_id_str)

        if is_subscribed(user_id, tmdb_id):
            remove_subscription(user_id, tmdb_id)
            bot.answer_callback_query(call.id, "Больше не слежу за сезонами.")
        else:
            title, media_type = get_title_from_db_any(user_id, tmdb_id)
            if not title:
                # подстраховка — дернуть TMDb
                details = get_tmdb_details("tv", tmdb_id) or {}
                title = build_display_title(details, "Без названия")
            details = get_tmdb_details("tv", tmdb_id) or {}
            last_air_date = details.get("last_air_date")
            add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)
            bot.answer_callback_query(call.id, "Теперь слежу за новыми сезонами.")
        return

    # Добавить в плейлист «Буду смотреть»
    if data.startswith("wl_add:"):
        _, tmdb_id_str, media_type = data.split(":", 2)
        tmdb_id = int(tmdb_id_str)

        title, _mt = get_title_from_db_any(user_id, tmdb_id)
        if not title:
            details = get_tmdb_details(media_type, tmdb_id) or {}
            title = build_display_title(details, "Без названия")

        add_watchlist_item(user_id, tmdb_id, title, media_type)
        bot.answer_callback_query(call.id, "Добавил в плейлист «Буду смотреть».")
        return

    # Скип похожего
    if data.startswith("rec_skip:"):
        _, tmdb_id_str = data.split(":", 1)
        tmdb_id = int(tmdb_id_str)
        add_feedback(user_id, tmdb_id, "skipped")
        bot.answer_callback_query(call.id, "Ок, подобные тайтлы буду занижать в рекомендациях.")
        return

    if data == "noop":
        bot.answer_callback_query(call.id, "Ок")
        return

    bot.answer_callback_query(call.id)


# =========================
#  Запуск
# =========================

if __name__ == "__main__":
    init_db()
    threading.Thread(target=subscription_worker, daemon=True).start()
    print("Bot is running...")
    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)