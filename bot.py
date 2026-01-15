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
DB_PATH = "cinemate_v12.db"

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
            chat_id INTEGER UNIQUE,
            last_visit_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
            status TEXT,      -- watched / unseen / favorite / liked / disliked / blocked
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

    # на случай старой БД без столбца shown — пробуем добавить
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
            last_air_date TEXT,
            last_notified_at TEXT
        )
    """)

    conn.commit()
    conn.close()


def touch_user_visit(user_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE users SET last_visit_at=CURRENT_TIMESTAMP WHERE id=?", (user_id,))
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

    c.execute("INSERT INTO users (chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    user_id = c.lastrowid
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
        "liked": 3,
        "disliked": -2,
        "blocked": -100,
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
    c.execute(
        "SELECT tmdb_id, SUM(weight) FROM user_feedback WHERE user_id=? GROUP BY tmdb_id",
        (user_id,),
    )
    rows = c.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def add_calibration_items(user_id: int, items: List[Dict[str, Any]]):
    """
    Сохраняем не больше 9 тайтлов для look-alike калибровки.
    """
    random.shuffle(items)
    items = items[:9]

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
        INSERT OR IGNORE INTO subscriptions (user_id, tmdb_id, title, media_type, last_air_date, last_notified_at)
        VALUES (?, ?, ?, 'tv', ?, ?)
    """, (user_id, tmdb_id, title, last_air_date, last_air_date))
    conn.commit()
    conn.close()


def get_subscriptions(user_id: int) -> List[Tuple[int, str, str, str]]:
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
        SET last_air_date=?, last_notified_at=?
        WHERE user_id=? AND tmdb_id=?
    """, (last_air_date, last_air_date, user_id, tmdb_id))
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


def get_imdb_link(media_type: str, tmdb_id: int) -> Optional[str]:
    data = tmdb_get(f"/{media_type}/{tmdb_id}/external_ids", {})
    if not data:
        return None
    imdb_id = data.get("imdb_id")
    if imdb_id:
        return f"https://www.imdb.com/title/{imdb_id}"
    return None


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
    С постером + год + жанры + рейтинг + ссылка на IMDb.
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
        # Подтягиваем детали, чтобы отрисовать карточку
        details = get_tmdb_details(media_type, tmdb_id) or {}
        poster_path = details.get("poster_path")
        vote = details.get("vote_average") or 0.0
        release_date = details.get("first_air_date") or details.get("release_date") or ""
        year = release_date[:4] if release_date else "—"

        genre_ids = details.get("genres") or []  # TMDb может вернуть [{id, name}, ...]
        if genre_ids and isinstance(genre_ids[0], dict):
            gids = [g["id"] for g in genre_ids]
        else:
            gids = genre_ids or []

        genres = [TMDB_GENRES.get(gid, "") for gid in gids]
        genres_str = ", ".join([g for g in genres if g])

        # IMDb-ссылка (как в старом билде)
        imdb_id = None
        try:
            external = tmdb_get(f"/{media_type}/{tmdb_id}/external_ids", {})
            if external:
                imdb_id = external.get("imdb_id")
        except Exception:
            imdb_id = None

        imdb_link = f"https://www.imdb.com/title/{imdb_id}" if imdb_id else None

        kind = "Сериал" if media_type == "tv" else "Фильм"

        caption_lines = [
            f"<b>{title}</b>",
            f"<i>{kind}, {year}</i>",
        ]
        if genres_str:
            caption_lines.append(f"Жанры: {genres_str}")
        if vote:
            caption_lines.append(f"Рейтинг TMDb: {vote:.1f}")
        if imdb_link:
            caption_lines.append(f'<a href="{imdb_link}">Ссылка на IMDb</a>')

        caption = "\n".join(caption_lines)

        # Клавиатура как раньше: смотрел / не смотрел / сердечко
        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("Смотрел", callback_data=f"calib:{row_id}:watched"),
            types.InlineKeyboardButton("Не смотрел", callback_data=f"calib:{row_id}:unseen"),
        )
        kb.row(
            types.InlineKeyboardButton("❤️ Попал в сердечко", callback_data=f"calib:{row_id}:favorite")
        )

        if poster_path:
            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
            bot.send_photo(chat_id, poster_url, caption=caption, reply_markup=kb)
        else:
            bot.send_message(chat_id, caption, reply_markup=kb)


# =========================
#  Рекомендации
# =========================

def build_recommendations(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    favorites = get_favorites(user_id)
    if not favorites:
        return []

    user_genres = set(get_user_genres(user_id))
    feedback_weights = get_feedback_weights(user_id)
    fav_ids = {f[0] for f in favorites}

    candidate_scores: Dict[int, Dict[str, Any]] = {}

    # собираем кандидатов
    for tmdb_id, title, media_type in favorites:
        items = get_similar_and_recommended(media_type, tmdb_id) or []

        for it in items:
            cid = it["id"]

            # не рекомендовать то, что уже в избранном
            if cid in fav_ids:
                continue

            cmedia = it.get("media_type") or ("tv" if it.get("name") else "movie")
            ctitle = it.get("title") or it.get("name") or "Без названия"
            genres = it.get("genre_ids") or []
            rating = it.get("vote_average") or 0.0
            popularity = it.get("popularity") or 0.0

            date_str = it.get("release_date") or it.get("first_air_date")
            year = None
            if date_str and len(date_str) >= 4:
                try:
                    year = int(date_str[:4])
                except ValueError:
                    pass

            data = candidate_scores.setdefault(
                cid,
                {
                    "tmdb_id": cid,
                    "title": ctitle,
                    "media_type": cmedia,
                    "genres": genres,
                    "rating": rating,
                    "popularity": popularity,
                    "year": year,
                    "freq": 0,
                    "score": 0.0,
                }
            )
            data["freq"] += 1

    # скоринг
    for cid, data in list(candidate_scores.items()):
        # если тайтл жёстко заблокирован — убираем
        if feedback_weights.get(cid, 0) <= -50:
            del candidate_scores[cid]
            continue

        genres = set(data["genres"])
        genre_overlap = len(genres & user_genres)
        rating = data["rating"]
        popularity = data["popularity"]
        freq = data["freq"]
        feedback_bonus = feedback_weights.get(cid, 0)
        year = data["year"] or 2000

        current_year = 2025  # можно обновить потом
        age = max(0, current_year - year)

        # приоритет по новизне
        if age <= 10:
            freshness = 1.0
        elif age <= 20:
            freshness = 0.4
        else:
            freshness = -0.5

        score = (
            2.3 * freq +
            1.2 * genre_overlap +
            1.0 * rating +
            0.5 * (popularity / 10.0) +
            2.5 * feedback_bonus +
            1.5 * freshness
        )

        # маленький рандом, чтобы выдача чуть перемешивалась
        score += random.uniform(-0.3, 0.3)

        data["score"] = score

    ranked = sorted(candidate_scores.values(), key=lambda x: x["score"], reverse=True)

    # жёстко ограничим долю старых тайтлов
    top_recent = []
    older = []
    for item in ranked:
        year = item.get("year") or 2000
        age = max(0, 2025 - year)
        if age <= 20:
            top_recent.append(item)
        else:
            older.append(item)

    result: List[Dict[str, Any]] = []
    result.extend(top_recent[:limit])
    max_older = max(1, int(len(result) * 0.2))  # до 20% старых
    result.extend(older[:max_older])

    return result[:limit]


def send_recommendation_card(chat_id: int, user_id: int, item: Dict[str, Any]):
    """
    Карточка с постером, рус/англ названиями, жанрами, рейтингом и ссылкой на IMDb.
    """
    tmdb_id = item["tmdb_id"]
    media_type = item["media_type"]
    title = item["title"]

    details = get_tmdb_details(media_type, tmdb_id) or {}
    poster_path = details.get("poster_path")
    orig_title = details.get("original_title") or details.get("original_name")

    year = None
    date_str = details.get("release_date") or details.get("first_air_date")
    if date_str and len(date_str) >= 4:
        try:
            year = int(date_str[:4])
        except ValueError:
            pass

    rating = details.get("vote_average") or item.get("rating") or 0.0
    genres_list = details.get("genres") or []
    genre_names = [g["name"] for g in genres_list] or [
        TMDB_GENRES.get(gid, "") for gid in item.get("genres", [])
    ]
    genres_str = ", ".join([g for g in genre_names if g])

    imdb_link = get_imdb_link(media_type, tmdb_id)

    kind = "Сериал" if media_type == "tv" else "Фильм"

    caption_lines = [f"<b>{title}</b>"]
    if orig_title and orig_title != title:
        caption_lines.append(f"<i>{orig_title}</i>")
    caption_lines.append(f"{kind}{f', {year}' if year else ''}")
    if genres_str:
        caption_lines.append(f"Жанры: {genres_str}")
    if rating:
        caption_lines.append(f"Рейтинг TMDb: {rating:.1f}")
    if imdb_link:
        caption_lines.append(f'<a href="{imdb_link}">Ссылка на IMDb</a>')

    text = "\n".join(caption_lines)

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("❤️ В любимые", callback_data=f"recfav:{tmdb_id}:{media_type}"),
        types.InlineKeyboardButton("👁 Уже смотрел", callback_data=f"recseen:{tmdb_id}:{media_type}"),
    )
    kb.row(
        types.InlineKeyboardButton("🔔 Следить за сериалом", callback_data=f"recsub:{tmdb_id}:{media_type}"),
        types.InlineKeyboardButton("👎 Не предлагать", callback_data=f"recban:{tmdb_id}:{media_type}"),
    )

    if poster_path:
        photo_url = f"{TMDB_IMAGE_BASE}{poster_path}"
        bot.send_photo(chat_id, photo_url, caption=text, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)


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
                new_last_air_date = details.get("last_air_date") or details.get("first_air_date")
                if new_last_air_date and new_last_air_date != last_air_date:
                    # обновился last_air_date -> notify
                    update_subscription_last_air_date(user_id, tmdb_id, new_last_air_date)
                    chat_id = get_chat_id(user_id)
                    if chat_id:
                        bot.send_message(
                            chat_id,
                            f"📺 У сериала <b>{title}</b> вышли новые серии.\n"
                            f"Дата последнего выхода: {new_last_air_date}"
                        )
        except Exception as e:
            print(f"subscription_worker error: {e}")

        time.sleep(3600)  # раз в час; можно увеличить


# =========================
#  Хэндлеры команд
# =========================

@bot.message_handler(commands=['start'])
def handle_start(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    touch_user_visit(user_id)
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
            "• /mysubs — сериалы, за которыми я слежу\n"
            "• /help — подсказка по функциям"
        )


@bot.message_handler(commands=['help'])
def handle_help(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    touch_user_visit(user_id)

    # быстрый доступ к командам
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("/recommend", "/mylikes")
    kb.row("/mysubs", "/help")

    bot.send_message(
        chat_id,
        "Я подбираю фильмы и сериалы под твой вкус.\n\n"
        "Как со мной работать:\n"
        "1. Добавь любимые тайтлы через /start (онбординг).\n"
        "2. Отметь жанры, которые нравятся.\n"
        "3. Оцени похожие фильмы/сериалы (Смотрел / Не смотрел / ❤️).\n"
        "4. Используй /recommend, чтобы получить подборку.\n\n"
        "Сервисные команды:\n"
        "• /mylikes — твой список любимых\n"
        "• /mysubs — сериалы под слежением\n"
        "• /recommend — свежие рекомендации",
        reply_markup=kb
    )


@bot.message_handler(commands=['mylikes'])
def handle_mylikes(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    touch_user_visit(user_id)

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


@bot.message_handler(commands=['mysubs'])
def handle_mysubs(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    touch_user_visit(user_id)

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
    touch_user_visit(user_id)

    recs = build_recommendations(user_id, limit=5)
    if not recs:
        bot.send_message(
            chat_id,
            "Пока мало данных для рекомендаций.\n"
            "Запусти /start и добавь любимые тайтлы, а затем пройди калибровку."
        )
        return

    for item in recs:
        send_recommendation_card(chat_id, user_id, item)

    more_kb = types.InlineKeyboardMarkup()
    more_kb.add(types.InlineKeyboardButton("Хочу ещё рекомендации", callback_data="more_recs"))
    bot.send_message(chat_id, "Хочешь новые реко? Жми:", reply_markup=more_kb)


# =========================
#  Обработка текста по состояниям
# =========================

@bot.message_handler(func=lambda m: True)
def handle_text(message: types.Message):
    chat_id = message.chat.id
    user_id = get_user_id(chat_id)
    touch_user_visit(user_id)
    state = get_state(user_id)

    if state == "await_favorites":
        handle_await_favorites(message, user_id)
    elif state == "await_subscribe_title":
        handle_await_subscribe_title(message, user_id)
    else:
        # фоллбек — подсказываем команды
        bot.send_message(
            chat_id,
            "Я тебя услышал, но пока лучше пользоваться командами:\n"
            "/recommend, /mylikes, /mysubs, /help"
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
        bot.send_message(chat_id, f"Добавил в любимые: <b>{title}</b> ({'сериал' if media_type == 'tv' else 'фильм'})")

    total = count_favorites(user_id)
    if total < 3:
        bot.send_message(
            chat_id,
            f"Сейчас у тебя {total} любимых в базе. Добавь ещё пару, чтобы я точнее понял вкус."
        )
        return

    # Переходим к выбору жанров
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
    last_air_date = details.get("last_air_date") or details.get("first_air_date")

    add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)
    set_state(user_id, None)
    bot.send_message(
        chat_id,
        f"Теперь я слежу за новыми сезонами сериала <b>{title}</b>."
    )


# =========================
#  Callback'и (жанры, калибровка, реко, /mysubs)
# =========================

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    user_id = get_user_id(chat_id)
    data = call.data or ""

    # --- жанры ---
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

    # --- look-alike калибровка ---
    elif data.startswith("calib:"):
        # calib:<row_id>:<status>
        _, row_id_str, status = data.split(":", 2)
        row_id = int(row_id_str)
        set_calibration_status(row_id, status)

        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT tmdb_id, title, media_type FROM calibration_items WHERE id=?", (row_id,))
        row = c.fetchone()
        conn.close()

        if row:
            tmdb_id, title, media_type = row
            add_feedback(user_id, tmdb_id, status)
            if status == "favorite":
                add_favorite(user_id, tmdb_id, title, media_type)
                # если сериал — сразу включаем слежение
                if media_type == "tv":
                    details = get_tmdb_details("tv", tmdb_id) or {}
                    last_air_date = details.get("last_air_date") or details.get("first_air_date")
                    add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)

        # считаем, остались ли среди уже показанных (shown=1) неоценённые
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) FROM calibration_items
            WHERE user_id=? AND shown=1 AND status IS NULL
        """, (user_id,))
        remaining = c.fetchone()[0]
        conn.close()

        bot.answer_callback_query(call.id, "Сохранил 👍")

        # если всё, что показали, уже оценено — шлём следующую тройку
        if remaining == 0 and get_state(user_id) == "calibration":
            send_calibration_batch(chat_id, user_id)

    # --- добавление сериала в слежение ---
    elif data == "subs_add":
        bot.answer_callback_query(call.id)
        set_state(user_id, "await_subscribe_title")
        bot.send_message(
            chat_id,
            "Напиши название сериала, за которым хочешь, чтобы я следил."
        )

    # --- рекомендации: действия ---
    elif data.startswith("recfav:"):
        _, tmdb_id_str, media_type = data.split(":", 2)
        tmdb_id = int(tmdb_id_str)
        details = get_tmdb_details(media_type, tmdb_id) or {}
        title = details.get("title") or details.get("name") or "Без названия"
        add_favorite(user_id, tmdb_id, title, media_type)
        add_feedback(user_id, tmdb_id, "favorite")
        bot.answer_callback_query(call.id, "Добавил в любимые ❤️")

    elif data.startswith("recban:"):
        _, tmdb_id_str, media_type = data.split(":", 2)
        tmdb_id = int(tmdb_id_str)
        add_feedback(user_id, tmdb_id, "blocked")
        bot.answer_callback_query(call.id, "Больше не буду предлагать 👎")

    elif data.startswith("recsub:"):
        _, tmdb_id_str, media_type = data.split(":", 2)
        tmdb_id = int(tmdb_id_str)
        if media_type != "tv":
            bot.answer_callback_query(call.id, "Подписка актуальна только для сериалов.")
        else:
            details = get_tmdb_details("tv", tmdb_id) or {}
            title = details.get("name") or details.get("title") or "Сериал"
            last_air_date = details.get("last_air_date") or details.get("first_air_date")
            add_subscription_for_tv(user_id, tmdb_id, title, last_air_date)
            bot.answer_callback_query(call.id, "Буду следить за новыми сезонами 🔔")

    elif data.startswith("recseen:"):
        # UX: «Как тебе «…»?» после того, как пользователь отметил «Уже смотрел»
        _, tmdb_id_str, media_type = data.split(":", 2)
        tmdb_id = int(tmdb_id_str)

        details = get_tmdb_details(media_type, tmdb_id) or {}
        title = details.get("title") or details.get("name") or "этот тайтл"

        # фиксируем, что пользователь уже смотрел
        add_feedback(user_id, tmdb_id, "watched")
        bot.answer_callback_query(call.id, "Учёл, что ты уже смотрел 👁")

        # задаём уточняющий вопрос
        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("👍 Понравился", callback_data=f"recrate:{tmdb_id}:{media_type}:like"),
            types.InlineKeyboardButton("👎 Не зашёл", callback_data=f"recrate:{tmdb_id}:{media_type}:dislike"),
        )
        bot.send_message(chat_id, f"Как тебе «{title}»?", reply_markup=kb)

    elif data.startswith("recrate:"):
        # дооценка после «Уже смотрел»
        _, tmdb_id_str, media_type, verdict = data.split(":", 3)
        tmdb_id = int(tmdb_id_str)
        status = "liked" if verdict == "like" else "disliked"
        add_feedback(user_id, tmdb_id, status)
        bot.answer_callback_query(call.id, "Сохранил ✏️")

        if status == "liked":
            bot.send_message(chat_id, "Круто, буду подбирать ещё в таком духе 👍")
        else:
            bot.send_message(chat_id, "Понял, такие тайтлы постараюсь не навязывать 👎")

    elif data == "more_recs":
        bot.answer_callback_query(call.id)
        # просто повторно вызываем /recommend для этого чата
        fake_message = types.Message(
            message_id=call.message.message_id,
            date=call.message.date,
            chat=call.message.chat,
            content_type="text",
            options={},
            json_string=""
        )
        fake_message.text = "/recommend"
        handle_recommend(fake_message)

    else:
        bot.answer_callback_query(call.id)


# =========================
#  Запуск
# =========================

if __name__ == "__main__":
    init_db()
    # фоновый поток проверки подписок
    threading.Thread(target=subscription_worker, daemon=True).start()
    print("Bot is running...")
    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)