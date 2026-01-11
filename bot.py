import os
import sqlite3
import threading
import time
import difflib

import requests
from telebot import TeleBot
from dotenv import load_dotenv

# ---------- КОНФИГ ----------

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

bot = TeleBot(BOT_TOKEN)
DB_PATH = "movies.db"

TMDB_BASE_URL = "https://api.themoviedb.org/3"
CHECK_INTERVAL_SECONDS = 60 * 60  # раз в час проверяем новые серии

# Маппинг популярных русских названий -> оригинальные
TITLE_MAP = {
    "острые козырьки": "peaky blinders",
    "голяк": "brassic",
    "йеллоустоун": "yellowstone",
    "во все тяжкие": "breaking bad",
    "бумажный дом": "la casa de papel",
    "игра престолов": "game of thrones",
    "мир дикого запада": "westworld",
    "ходячие мертвецы": "the walking dead",
    "ведьмак": "the witcher",
    "клан сопрано": "the sopranos",
}


# ---------- БАЗА ДАННЫХ ----------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Пользователи
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER UNIQUE
        )
    """)

    # Лайки
    c.execute("""
        CREATE TABLE IF NOT EXISTS likes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            title TEXT,
            media_type TEXT
        )
    """)

    # Подписки на сериалы
    c.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tmdb_id INTEGER,
            title TEXT,
            last_air_date TEXT
        )
    """)

    conn.commit()
    conn.close()


def get_conn():
    return sqlite3.connect(DB_PATH)


def get_user_id(chat_id: int) -> int:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    if row:
        user_id = row[0]
    else:
        c.execute("INSERT INTO users (chat_id) VALUES (?)", (chat_id,))
        conn.commit()
        user_id = c.lastrowid
    conn.close()
    return user_id


def add_like(user_id: int, tmdb_id: int, title: str, media_type: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM likes
        WHERE user_id = ? AND tmdb_id = ?
    """, (user_id, tmdb_id))
    if not c.fetchone():
        c.execute("""
            INSERT INTO likes (user_id, tmdb_id, title, media_type)
            VALUES (?, ?, ?, ?)
        """, (user_id, tmdb_id, title, media_type))
        conn.commit()
    conn.close()


def get_likes(user_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT tmdb_id, title, media_type
        FROM likes
        WHERE user_id = ?
    """, (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def add_or_update_subscription(user_id: int, tmdb_id: int, title: str, last_air_date: str | None):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM subscriptions
        WHERE user_id = ? AND tmdb_id = ?
    """, (user_id, tmdb_id))
    row = c.fetchone()
    if row:
        c.execute("""
            UPDATE subscriptions
            SET last_air_date = ?
            WHERE id = ?
        """, (last_air_date, row[0]))
    else:
        c.execute("""
            INSERT INTO subscriptions (user_id, tmdb_id, title, last_air_date)
            VALUES (?, ?, ?, ?)
        """, (user_id, tmdb_id, title, last_air_date))
    conn.commit()
    conn.close()


def get_all_subscriptions():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT s.id, s.user_id, s.tmdb_id, s.title, s.last_air_date, u.chat_id
        FROM subscriptions s
        JOIN users u ON s.user_id = u.id
    """)
    rows = c.fetchall()
    conn.close()
    return rows


def update_subscription_last_air_date(sub_id: int, new_date: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE subscriptions
        SET last_air_date = ?
        WHERE id = ?
    """, (new_date, sub_id))
    conn.commit()
    conn.close()


# ---------- TMDB ФУНКЦИИ И ФАЗЗИ-ПОИСК ----------

def tmdb_search_multi_raw(query: str, lang: str):
    url = f"{TMDB_BASE_URL}/search/multi"
    params = {
        "api_key": TMDB_API_KEY,
        "language": lang,
        "query": query
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def tmdb_search_multi_fuzzy(query: str):
    """
    Фаззи-поиск по TMDb.
    1) Нормализуем запрос (lowercase).
    2) Если есть в словаре TITLE_MAP — ищем по оригинальному названию
       и сравниваем тоже с ним.
    3) Пробуем ru-RU и en-US, выбираем лучший матч по score.
    Возвращаем (best_item, best_score) или (None, 0.0).
    best_item = { tmdb_id, title, media_type }
    """
    q_lower = query.lower().strip()

    # Если знаем, что это русское название популярного сериала/фильма — мапим
    mapped = TITLE_MAP.get(q_lower)
    compare_base = mapped if mapped else q_lower
    query_for_tmdb = mapped if mapped else query

    best_item = None
    best_score = 0.0

    for lang in ("ru-RU", "en-US"):
        try:
            results = tmdb_search_multi_raw(query_for_tmdb, lang)
        except Exception as e:
            print(f"[search_raw] error for '{query_for_tmdb}' lang={lang}: {e}")
            continue

        for item in results:
            if item.get("media_type") not in ("movie", "tv"):
                continue

            titles = [
                item.get("title"),
                item.get("name"),
                item.get("original_title"),
                item.get("original_name"),
            ]
            titles = [t for t in titles if t]

            item_best = 0.0
            for t in titles:
                score = difflib.SequenceMatcher(
                    None,
                    compare_base,
                    t.lower()
                ).ratio()
                if score > item_best:
                    item_best = score

            if item_best > best_score:
                best_score = item_best
                best_item = {
                    "tmdb_id": item["id"],
                    "title": item.get("title")
                             or item.get("name")
                             or item.get("original_title")
                             or item.get("original_name"),
                    "media_type": item["media_type"],
                }

    return best_item, best_score


def tmdb_similar(media_type: str, tmdb_id: int):
    url = f"{TMDB_BASE_URL}/{media_type}/{tmdb_id}/similar"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "ru-RU",
        "page": 1
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def tmdb_get_tv_details(tv_id: int):
    url = f"{TMDB_BASE_URL}/tv/{tv_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "ru-RU"
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def extract_last_air_date(tv_details: dict) -> str | None:
    last_ep = tv_details.get("last_episode_to_air") or {}
    air_date = last_ep.get("air_date")
    if air_date:
        return air_date
    return tv_details.get("last_air_date")


# ---------- РЕКОМЕНДАЦИИ ----------

def build_recommendations(user_id: int, limit: int = 10):
    likes = get_likes(user_id)
    if not likes:
        return []

    candidates = {}
    liked_ids = {row[0] for row in likes}

    for tmdb_id, title, media_type in likes:
        try:
            sim_items = tmdb_similar(media_type, tmdb_id)
        except Exception as e:
            print(f"[similar] error for {tmdb_id}: {e}")
            continue

        for item in sim_items:
            sid = item["id"]
            if sid in liked_ids:
                continue
            key = (sid, media_type)
            if key not in candidates:
                candidates[key] = {
                    "count": 0,
                    "vote_average": item.get("vote_average", 0),
                    "title": item.get("title") or item.get("name"),
                    "overview": item.get("overview", ""),
                    "media_type": media_type
                }
            candidates[key]["count"] += 1

    sorted_items = sorted(
        candidates.values(),
        key=lambda x: (x["count"], x["vote_average"]),
        reverse=True
    )

    return sorted_items[:limit]


# ---------- ФОНОВЫЙ МОНИТОРИНГ СЕРИАЛОВ ----------

def subscriptions_watcher():
    while True:
        try:
            subs = get_all_subscriptions()
            for sub_id, user_id, tmdb_id, title, last_air_date, chat_id in subs:
                try:
                    details = tmdb_get_tv_details(tmdb_id)
                except Exception as e:
                    print(f"[watcher] error fetching tv details {tmdb_id}: {e}")
                    continue

                current_last = extract_last_air_date(details)
                if not current_last:
                    continue

                if not last_air_date:
                    update_subscription_last_air_date(sub_id, current_last)
                    continue

                if current_last > last_air_date:
                    msg = (
                        f"📺 Вышло что-то новое по сериалу «{title}»!\n"
                        f"Последняя дата выхода эпизода: {current_last}."
                    )
                    try:
                        bot.send_message(chat_id, msg)
                    except Exception as e:
                        print(f"[watcher] error sending message: {e}")

                    update_subscription_last_air_date(sub_id, current_last)

        except Exception as e:
            print(f"[watcher] unexpected error: {e}")

        time.sleep(CHECK_INTERVAL_SECONDS)


# ---------- ХЕНДЛЕРЫ БОТА ----------

@bot.message_handler(commands=["start"])
def handle_start(message):
    init_db()
    get_user_id(message.chat.id)

    text = (
        "Привет! Я подбираю фильмы и сериалы под твой вкус "
        "и слежу за новыми сериями твоих любимых сериалов.\n\n"
        "1️⃣ Отправь мне список фильмов/сериалов, которые тебе понравились — через /like.\n"
        "2️⃣ Я их найду (даже с опечатками и русскими названиями), сохраню, "
        "а по сериалам начну следить за новыми эпизодами.\n"
        "3️⃣ По команде /recommend дам список рекомендаций.\n\n"
        "Начнём с /like."
    )
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=["like"])
def handle_like(message):
    text = (
        "Отправь список своих любимых фильмов/сериалов через запятую.\n\n"
        "Например:\n"
        "Острые козырьки, Голяк, Йеллоустоун"
    )
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=["recommend"])
def handle_recommend(message):
    user_id = get_user_id(message.chat.id)
    likes = get_likes(user_id)
    if len(likes) < 3:
        bot.send_message(
            message.chat.id,
            "Пока мало данных о твоём вкусе. Добавь хотя бы 3 любимых тайтла через /like."
        )
        return

    bot.send_message(message.chat.id, "Подбираю рекомендации...")

    recs = build_recommendations(user_id, limit=10)
    if not recs:
        bot.send_message(message.chat.id, "Пока не нашёл ничего подходящего. Попробуй добавить ещё любимых через /like.")
        return

    lines = ["Вот что тебе может зайти:\n"]
    for item in recs:
        label = "🎬" if item["media_type"] == "movie" else "📺"
        line = f"{label} {item['title']} (рейтинг TMDb: {item['vote_average']:.1f})"
        if item["overview"]:
            line += f"\n  {item['overview'][:200]}..."
        lines.append(line)
        lines.append("")

    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(func=lambda m: True, content_types=["text"])
def handle_text(message):
    user_id = get_user_id(message.chat.id)
    raw = message.text.strip()
    titles = [t.strip() for t in raw.split(",") if t.strip()]

    if not titles:
        bot.send_message(message.chat.id, "Не смог распознать названия. Напиши их через запятую.")
        return

    added = []

    for title in titles:
        try:
            res, score = tmdb_search_multi_fuzzy(title)
        except Exception as e:
            print(f"[search] error for '{title}': {e}")
            res, score = None, 0.0

        if not res or score < 0.5:
            bot.send_message(message.chat.id, f"Не нашёл ничего подходящего для: {title}")
            continue

        add_like(user_id, res["tmdb_id"], res["title"], res["media_type"])
        added.append(f"{res['title']} ({'фильм' if res['media_type'] == 'movie' else 'сериал'})")

        if res["media_type"] == "tv":
            try:
                details = tmdb_get_tv_details(res["tmdb_id"])
                last_air = extract_last_air_date(details)
            except Exception as e:
                print(f"[tv_details] error for {res['tmdb_id']}: {e}")
                last_air = None
            add_or_update_subscription(user_id, res["tmdb_id"], res["title"], last_air)

    if added:
        msg = "Добавил в твои любимые:\n" + "\n".join("• " + a for a in added)
        msg += "\n\nРекомендации — командой /recommend.\n"
        msg += "За сериалами из списка я теперь слежу и сообщу, когда выйдет что-то новое."
        bot.send_message(message.chat.id, msg)
    else:
        bot.send_message(message.chat.id, "Ничего не удалось добавить. Попробуй сформулировать названия точнее.")


# ---------- ЗАПУСК ----------

if __name__ == "__main__":
    init_db()
    watcher_thread = threading.Thread(target=subscriptions_watcher, daemon=True)
    watcher_thread.start()

    print("Bot is running...")
    bot.infinity_polling()