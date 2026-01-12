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
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

if not BOT_TOKEN or not TMDB_API_KEY:
    raise RuntimeError("BOT_TOKEN и TMDB_API_KEY должны быть заданы в переменных окружения")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

DB_PATH = "cinemate_8v.db"

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

MAX_CALIBRATION_ITEMS = 9  # всего look-alike тайтлов на онбординге
RECS_PER_PAGE = 5


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
            status TEXT,      -- watched / unseen / favorite / rec_*
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

    # на случай старой БД без столбца shown
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

    c.execute("""
        CREATE TABLE IF NOT EXISTS recommendation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            times_shown INTEGER DEFAULT 0,
            last_shown_at DATETIME,
            last_action TEXT
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
        conn.close()
        return row[0]
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
    weight_map = {
        "watched": 1,
        "unseen": 0,
        "favorite": 5,
        "rec_watchlist": 3,
        "rec_seen": 2,
        "rec_dislike": -4,
    }
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
        title = it.get("title") or it.get("name") or "Без названия"
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
    # помечаем их как показанные
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


def get_subscriptions(user_id: int) -> List[Tuple[int, int, str, str]]:
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


def add_to_watchlist(user_id: int, tmdb_id: int, title: str, media_type: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO watchlist (user_id, tmdb_id, title, media_type)
        VALUES (?, ?, ?, ?)
    """, (user_id, tmdb_id, title, media_type))
    conn.commit()
    conn.close()


def get_watchlist(user_id: int) -> List[Tuple[int, str, str]]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT tmdb_id, title, media_type FROM watchlist WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def get_recommendation_history(user_id: int) -> Dict[int, Tuple[int, Optional[str]]]:
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT tmdb_id, times_shown, last_action
        FROM recommendation_history
        WHERE user_id=?
    """, (user_id,))
    rows = c.fetchall()
    conn.close()
    return {r[0]: (r[1], r[2]) for r in rows}


def record_recommendation_shown(user_id: int, tmdb_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, times_shown FROM recommendation_history
        WHERE user_id=? AND tmdb_id=?
    """, (user_id, tmdb_id))
    row = c.fetchone()
    if row:
        rid, times = row
        c.execute("""
            UPDATE recommendation_history
            SET times_shown=?, last_shown_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (times + 1, rid))
    else:
        c.execute("""
            INSERT INTO recommendation_history (user_id, tmdb_id, times_shown, last_shown_at)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP)
        """, (user_id, tmdb_id))
    conn.commit()
    conn.close()


def set_recommendation_action(user_id: int, tmdb_id: int, action: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM recommendation_history
        WHERE user_id=? AND tmdb_id=?
    """, (user_id, tmdb_id))
    row = c.fetchone()
    if row:
        c.execute("""
            UPDATE recommendation_history
            SET last_action=?
            WHERE id=?
        """, (action, row[0]))
    else:
        c.execute("""
            INSERT INTO recommendation_history (user_id, tmdb_id, times_shown, last_shown_at, last_action)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?)
        """, (user_id, tmdb_id, action))
    conn.commit()
    conn.close()


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


def get_imdb_url(media_type: str, tmdb_id: int) -> Optional[str]:
    if media_type not in ("movie", "tv"):
        return None
    data = tmdb_get(f"/{media_type}/{tmdb_id}/external_ids", {})
    if not data:
        return None
    imdb_id = data.get("imdb_id")
    if not imdb_id:
        return None
    return f"https://www.imdb.com/title/{imdb_id}"


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

    # ограничиваем общее число калибровочных тайтлов
    all_items = list(candidates.values())
    random.shuffle(all_items)
    all_items = all_items[:MAX_CALIBRATION_ITEMS]
    add_calibration_items(user_id, all_items)


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

        details = get_tmdb_details(media_type, tmdb_id) or {}
        kind = "Фильм" if media_type == "movie" else "Сериал"
        year = None
        if media_type == "movie":
            rd = details.get("release_date")
            if rd:
                year = rd.split("-")[0]
        else:
            fd = details.get("first_air_date")
            if fd:
                year = fd.split("-")[0]

        imdb_url = get_imdb_url(media_type, tmdb_id)
        text = f"<b>{title}</b>\n<i>{kind}"
        if year:
            text += f", {year}"
        text += "</i>\n\nОтметь свою реакцию:"
        if imdb_url:
            text = f"<b>{title}</b>\n<i>{kind}" + (f", {year}" if year else "") + f"</i>\n<a href='{imdb_url}'>Смотреть на IMDb</a>\n\nОтметь свою реакцию:"

        bot.send_message(chat_id, text, reply_markup=kb)


# =========================
#  Рекомендации
# =========================

def build_recommendations(user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    favorites = get_favorites(user_id)
    if not favorites:
        return []

    user_genres = set(get_user_genres(user_id))
    feedback_weights = get_feedback_weights(user_id)
    history = get_recommendation_history(user_id)

    candidate_scores: Dict[int, Dict[str, Any]] = {}

    # собираем кандидатов
    for tmdb_id, title, media_type in favorites:
        items = get_similar_and_recommended(media_type, tmdb_id) or []

        for it in items:
            cid = it["id"]

            # не рекомендовать то, что уже в избранном
            if any(cid == f[0] for f in favorites):
                continue

            cmedia = it.get("media_type") or ("tv" if it.get("name") else "movie")
            ctitle = it.get("title") or it.get("name") or "Без названия"
            genres = it.get("genre_ids") or []
            rating = it.get("vote_average") or 0.0
            popularity = it.get("popularity") or 0.0

            data = candidate_scores.setdefault(
                cid,
                {
                    "tmdb_id": cid,
                    "title": ctitle,
                    "media_type": cmedia,
                    "genres": genres,
                    "rating": rating,
                    "popularity": popularity,
                    "freq": 0,
                    "score": 0.0,
                }
            )
            data["freq"] += 1

    ranked_candidates: List[Dict[str, Any]] = []

    for cid, data in candidate_scores.items():
        hist = history.get(cid)
        if hist:
            times_shown, last_action = hist
            # полный бан
            if last_action == "dislike":
                continue
        else:
            times_shown, last_action = (0, None)

        genres = set(data["genres"])
        genre_overlap = len(genres & user_genres)
        rating = data["rating"]
        popularity = data["popularity"]
        freq = data["freq"]
        feedback_bonus = feedback_weights.get(cid, 0)

        # базовый скоринг
        score = (
            2.3 * freq +
            1.2 * genre_overlap +
            1.0 * rating +
            0.6 * (popularity / 10.0) +
            2.5 * feedback_bonus
        )

        # влияние истории рекомендаций
        if last_action in ("watchlist", "seen", "sub"):
            score += 3.0  # лёгкий буст — раз цепляло, значит не так плохо
        elif times_shown > 0 and not last_action:
            # показывали, но ни разу не кликнули — сильно опускаем вниз
            penalty = min(times_shown, 5) * 6.0
            score -= penalty

        # лёгкий шум для разнообразия
        score += random.uniform(-0.3, 0.3)

        data["score"] = score
        ranked_candidates.append(data)

    ranked = sorted(ranked_candidates, key=lambda x: x["score"], reverse=True)
    return ranked[:limit]


def send_recommendations(chat_id: int, user_id: int):
    recs = build_recommendations(user_id, limit=50)
    if not recs:
        bot.send_message(
            chat_id,
            "Пока мало данных для рекомендаций.\n"
            "Запусти /start и добавь любимые тайтлы, а затем пройди калибровку."
        )
        return

    page = recs[:RECS_PER_PAGE]

    for item in page:
        tmdb_id = item["tmdb_id"]
        title = item["title"]
        media_type = item["media_type"]

        details = get_tmdb_details(media_type, tmdb_id) or {}
        imdb_url = get_imdb_url(media_type, tmdb_id)

        if media_type == "movie":
            kind = "Фильм"
            rd = details.get("release_date")
            year = rd.split("-")[0] if rd else None
        else:
            kind = "Сериал"
            fd = details.get("first_air_date")
            year = fd.split("-")[0] if fd else None

        ru_title = details.get("title") or details.get("name") or title
        orig_title = details.get("original_title") or details.get("original_name")
        poster_path = details.get("poster_path")
        poster_url = TMDB_IMAGE_BASE + poster_path if poster_path else None

        caption_lines = [f"<b>{ru_title}</b>"]
        if orig_title and orig_title != ru_title:
            caption_lines.append(f"<i>{orig_title}</i>")
        info_line = f"{kind}"
        if year:
            info_line += f", {year}"
        caption_lines.append(info_line)

        rating = details.get("vote_average")
        if rating:
            caption_lines.append(f"Рейтинг TMDb: {rating:.1f}")

        genres_ids = details.get("genres") or []
        if genres_ids and isinstance(genres_ids[0], dict):
            genre_names = [g.get("name") for g in genres_ids if g.get("name")]
            if genre_names:
                caption_lines.append("Жанры: " + ", ".join(genre_names))

        if imdb_url:
            caption_lines.append(f"<a href='{imdb_url}'>Смотреть на IMDb</a>")

        overview = details.get("overview")
        if overview:
            caption_lines.append("")
            caption_lines.append(overview[:400] + ("…" if len(overview) > 400 else ""))

        caption = "\n".join(caption_lines)

        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton("➕ В watchlist", callback_data=f"rec:{tmdb_id}:wl"))
        if media_type == "tv":
            kb.row(types.InlineKeyboardButton("📺 Следить за сериалом", callback_data=f"rec:{tmdb_id}:sub"))
        kb.row(
            types.InlineKeyboardButton("✅ Уже смотрел", callback_data=f"rec:{tmdb_id}:seen"),
            types.InlineKeyboardButton("👎 Не предлагать", callback_data=f"rec:{tmdb_id}:dislike"),
        )

        if poster_url:
            try:
                bot.send_photo(chat_id, poster_url, caption=caption, reply_markup=kb, parse_mode="HTML")
            except Exception as e:
                print(f"send_photo error: {e}")
                bot.send_message(chat_id, caption, reply_markup=kb)
        else:
            bot.send_message(chat_id, caption, reply_markup=kb)

        record_recommendation_shown(user_id, tmdb_id)

    # кнопка "ещё рекомендации"
    more_kb = types.InlineKeyboardMarkup()
    more_kb.add(types.InlineKeyboardButton("🔄 Показать ещё рекомендации", callback_data="more_recs"))
    bot.send_message(chat_id, "Хочешь новые реко? Жми кнопку ниже 👇", reply_markup=more_kb)


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

        time.sleep(3600)


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
            "• /watchlist — что отложил «посмотреть позже»\n"
            "• /mysubs — сериалы, за которыми я слежу\n"
            "• /menu — меню с командами\n"
            "• /help — подробная подсказка"
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
        "4. Пользуйся /recommend, чтобы получать подборки.\n\n"
        "Сервисные команды:\n"
        "• /mylikes — твой список любимых\n"
        "• /watchlist — отложенные к просмотру\n"
        "• /mysubs — сериалы под слежением\n"
        "• /menu — показать меню\n"
    )


@bot.message_handler(commands=['menu'])
def handle_menu(message: types.Message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("/recommend", "/mylikes")
    kb.row("/watchlist", "/mysubs")
    kb.row("/help")
    bot.send_message(
        message.chat.id,
        "Вот быстрые команды, чтобы не искать их в истории:",
        reply_markup=kb
    )


@bot.message_handler(commands=['mylikes'])
def handle_mylikes(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    favs = get_favorites(user_id)
    if not favs:
        bot.send_message(chat_id, "Пока нет любимых. Запусти /start и добавь несколько.")
        return

    movies = [f for f in favs if f[2] == "movie"]
    tvs = [f for f in favs if f[2] == "tv"]

    lines = []
    if movies:
        lines.append("<b>Фильмы:</b>")
        for tmdb_id, title, _ in movies:
            lines.append(f"• {title}")
        lines.append("")
    if tvs:
        lines.append("<b>Сериалы:</b>")
        for tmdb_id, title, _ in tvs:
            lines.append(f"• {title}")

    bot.send_message(chat_id, "\n".join(lines))


@bot.message_handler(commands=['watchlist'])
def handle_watchlist(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    wl = get_watchlist(user_id)
    if not wl:
        bot.send_message(chat_id, "Watchlist пуст. Добавляй тайтлы из рекомендаций кнопкой «➕ В watchlist».")
        return

    movies = [f for f in wl if f[2] == "movie"]
    tvs = [f for f in wl if f[2] == "tv"]

    lines = []
    if movies:
        lines.append("<b>Фильмы в watchlist:</b>")
        for tmdb_id, title, _ in movies:
            lines.append(f"• {title}")
        lines.append("")
    if tvs:
        lines.append("<b>Сериалы в watchlist:</b>")
        for tmdb_id, title, _ in tvs:
            lines.append(f"• {title}")

    bot.send_message(chat_id, "\n".join(lines))


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


@bot.message_handler(commands=['recommend'])
def handle_recommend(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    send_recommendations(chat_id, user_id)


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
            "/recommend, /mylikes, /watchlist, /mysubs, /help, /menu"
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
        title = result.get("title") or result.get("name") or t
        add_favorite(user_id, tmdb_id, title, media_type)
        bot.send_message(chat_id, f"Добавил в любимые: <b>{title}</b> ({'сериал' if media_type=='tv' else 'фильм'})")

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
    title = result.get("name") or result.get("title") or query
    details = get_tmdb_details("tv", tmdb_id) or {}
    last_air_date = details.get("last_air_date")

    add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)
    set_state(user_id, None)
    bot.send_message(
        chat_id,
        f"Теперь я слежу за новыми сезонами сериала <b>{title}</b>."
    )


# =========================
#  Callback'и (жанры, калибровка, /mysubs, реко)
# =========================

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    user_id = get_user_id(chat_id)
    data = call.data or ""

    if data.startswith("genre:"):
        _, gid_str = data.split(":", 1)
        gid = int(gid_str)
        toggle_user_genre(user_id, gid)
        kb = build_genre_keyboard(user_id)
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=kb)
        bot.answer_callback_query(call.id)

    elif data == "genre_done":
        bot.answer_callback_query(call.id)
        set_state(user_id, "calibration")
        bot.send_message(
            chat_id,
            "Теперь покажу несколько похожих тайтлов.\n"
            "Отметь: «Смотрел», «Не смотрел» или «❤️ Попал в сердечко»."
        )
        build_calibration_candidates(user_id)
        send_calibration_batch(chat_id, user_id)

    elif data.startswith("calib:"):
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

        c.execute("""
            SELECT COUNT(*) FROM calibration_items
            WHERE user_id=? AND shown=1 AND status IS NULL
        """, (user_id,))
        remaining = c.fetchone()[0]
        conn.close()

        bot.answer_callback_query(call.id, "Сохранил 👍")

        if remaining == 0 and get_state(user_id) == "calibration":
            send_calibration_batch(chat_id, user_id)

    elif data == "subs_add":
        bot.answer_callback_query(call.id)
        set_state(user_id, "await_subscribe_title")
        bot.send_message(
            chat_id,
            "Напиши название сериала, за которым хочешь, чтобы я следил."
        )

    elif data.startswith("rec:"):
        # rec:<tmdb_id>:<action>
        _, tmdb_id_str, action = data.split(":", 2)
        tmdb_id = int(tmdb_id_str)

        # медиа-тип и тайтл достаём из TMDb
        # (редко жмут много, поэтому ок)
        for media_type in ("movie", "tv"):
            details = get_tmdb_details(media_type, tmdb_id)
            if details:
                break
        else:
            details = {}
            media_type = "movie"

        title = details.get("title") or details.get("name") or details.get("original_title") or details.get(
            "original_name") or "Без названия"

        if action == "wl":
            add_to_watchlist(user_id, tmdb_id, title, media_type)
            add_feedback(user_id, tmdb_id, "rec_watchlist")
            set_recommendation_action(user_id, tmdb_id, "watchlist")
            bot.answer_callback_query(call.id, "Добавил в watchlist ✅")

        elif action == "seen":
            add_feedback(user_id, tmdb_id, "rec_seen")
            set_recommendation_action(user_id, tmdb_id, "seen")
            bot.answer_callback_query(call.id, "Окей, учту что ты уже смотрел 👌")

        elif action == "dislike":
            add_feedback(user_id, tmdb_id, "rec_dislike")
            set_recommendation_action(user_id, tmdb_id, "dislike")
            bot.answer_callback_query(call.id, "Больше не буду предлагать 👎")

        elif action == "sub" and media_type == "tv":
            last_air_date = details.get("last_air_date")
            add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)
            set_recommendation_action(user_id, tmdb_id, "sub")
            bot.answer_callback_query(call.id, "Буду следить за новым сезоном 📺")

    elif data == "more_recs":
        bot.answer_callback_query(call.id)
        send_recommendations(chat_id, user_id)

    else:
        bot.answer_callback_query(call.id)


# =========================
#  Запуск
# =========================

if __name__ == "__main__":
    init_db()
    threading.Thread(target=subscription_worker, daemon=True).start()
    print("Bot is running...")
    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)