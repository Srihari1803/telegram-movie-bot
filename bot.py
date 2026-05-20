import json
import os
import sqlite3
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from ddgs import DDGS
from rapidfuzz import fuzz

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ==========================================
# Load environment variables
# ==========================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

DB_FILE = "bot.db"
BLOCKED_PATTERNS = ["magnet:", ".torrent", "btih"]


# ==========================================
# Database Setup
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS history (
            user_id INTEGER,
            title TEXT,
            category TEXT,
            searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id INTEGER,
            title TEXT,
            category TEXT
        )
    """)

    conn.commit()
    conn.close()


# ==========================================
# Database Functions
# ==========================================
def save_history(user_id, title, category):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO history (user_id, title, category) VALUES (?, ?, ?)",
        (user_id, title, category)
    )

    conn.commit()
    conn.close()


def get_history(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT title, category, searched_at
        FROM history
        WHERE user_id = ?
        ORDER BY searched_at DESC
        LIMIT 10
    """, (user_id,))

    rows = cur.fetchall()
    conn.close()
    return rows


def add_favorite(user_id, title, category):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Avoid duplicates
    cur.execute("""
        SELECT 1
        FROM favorites
        WHERE user_id = ? AND title = ? AND category = ?
    """, (user_id, title, category))

    if cur.fetchone() is None:
        cur.execute(
            "INSERT INTO favorites (user_id, title, category) VALUES (?, ?, ?)",
            (user_id, title, category)
        )

    conn.commit()
    conn.close()


def get_favorites(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT title, category
        FROM favorites
        WHERE user_id = ?
    """, (user_id,))

    rows = cur.fetchall()
    conn.close()
    return rows


# ==========================================
# Utility Functions
# ==========================================
def is_allowed_link(url):
    url_lower = url.lower()
    return not any(pattern in url_lower for pattern in BLOCKED_PATTERNS)


def load_sites():
    with open("sites.json", "r", encoding="utf-8") as f:
        return json.load(f)


def get_sites(category):
    data = load_sites()
    return data.get(category, [])


# ==========================================
# TMDb Functions
# ==========================================
def get_tmdb_info(title):
    if not TMDB_API_KEY:
        return None

    try:
        url = "https://api.themoviedb.org/3/search/multi"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title,
            "include_adult": "true"
        }

        response = requests.get(url, params=params, timeout=15)
        data = response.json()

        results = data.get("results", [])
        if not results:
            return None

        best = results[0]

        return {
            "title": best.get("title") or best.get("name") or title,
            "year": (
                best.get("release_date")
                or best.get("first_air_date")
                or ""
            )[:4],
            "rating": best.get("vote_average"),
            "overview": best.get("overview"),
            "poster": (
                f"https://image.tmdb.org/t/p/w500{best['poster_path']}"
                if best.get("poster_path")
                else None
            ),
        }

    except Exception:
        return None


# ==========================================
# Search Functions
# ==========================================
def search_site(title, site_url):
    domain = urlparse(site_url).netloc.replace("www.", "")
    query = f'{title} site:{domain}'

    matches = []

    try:
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=5)

            for result in results:
                href = result.get("href") or result.get("url")
                result_title = result.get("title", "")

                if not href:
                    continue

                if not is_allowed_link(href):
                    continue

                score = fuzz.ratio(
                    title.lower(),
                    result_title.lower()
                )

                matches.append((score, href))

    except Exception:
        return None

    if not matches:
        return None

    matches.sort(reverse=True)
    return matches[0][1]


def search_category(title, category):
    sites = get_sites(category)
    found = {}

    for site in sites:
        result = search_site(title, site)

        if result:
            domain = urlparse(site).netloc.replace("www.", "")
            found[domain] = result

        if len(found) >= 8:
            break

    return found


# ==========================================
# Message Formatting
# ==========================================
def build_caption(info, title):
    if info:
        caption = f"🎬 <b>{info['title']}"

        if info["year"]:
            caption += f" ({info['year']})"

        caption += "</b>\n"

        if info["rating"]:
            caption += f"⭐ {info['rating']:.1f}/10\n"

        if info["overview"]:
            overview = info["overview"]
            if len(overview) > 400:
                overview = overview[:400] + "..."

            caption += f"\n{overview}\n"
    else:
        caption = f"🎬 <b>{title}</b>\n"

    caption += "\n🔗 Select a source below:"
    return caption


# ==========================================
# Send Search Results
# ==========================================
async def send_results(update, title, category):
    user_id = update.effective_user.id

    status = await update.message.reply_text("🔍 Searching...")

    info = get_tmdb_info(title)
    search_title = info["title"] if info else title

    results = search_category(search_title, category)

    save_history(user_id, title, category)

    if not results:
        await status.edit_text("❌ No results found.")
        return

    caption = build_caption(info, title)

    keyboard = []

    for domain, url in results.items():
        keyboard.append([
            InlineKeyboardButton(domain, url=url)
        ])

    # Add to favorites immediately
    add_favorite(user_id, title, category)

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        if info and info.get("poster"):
            await update.message.reply_photo(
                photo=info["poster"],
                caption=caption,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
            await status.delete()
        else:
            await status.edit_text(
                caption,
                parse_mode="HTML",
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
    except Exception:
        await status.edit_text(
            caption,
            parse_mode="HTML",
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )


# ==========================================
# Commands
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to Advanced Movie Bot!\n\n"
        "Commands:\n"
        "/movie <title>\n"
        "/tv <title>\n"
        "/anime <title>\n"
        "/game <title>\n"
        "/favorites\n"
        "/history"
    )


async def movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /movie Inception")
        return
    await send_results(update, " ".join(context.args), "movie")


async def tv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /tv Breaking Bad")
        return
    await send_results(update, " ".join(context.args), "tv")


async def anime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /anime Naruto")
        return
    await send_results(update, " ".join(context.args), "anime")


async def game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /game GTA V")
        return
    await send_results(update, " ".join(context.args), "game")


async def favorites(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_favorites(update.effective_user.id)

    if not rows:
        await update.message.reply_text("⭐ No favorites yet.")
        return

    lines = ["⭐ Your Favorites:\n"]

    for title, category in rows:
        lines.append(f"• [{category}] {title}")

    await update.message.reply_text("\n".join(lines))


async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_history(update.effective_user.id)

    if not rows:
        await update.message.reply_text("🕒 No search history.")
        return

    lines = ["🕒 Recent Searches:\n"]

    for title, category, searched_at in rows:
        lines.append(f"• [{category}] {title}")

    await update.message.reply_text("\n".join(lines))


# ==========================================
# Main Function
# ==========================================
def main():
    print("✅ Advanced Bot is running on Railway...")
    print("BOT_TOKEN:", bool(BOT_TOKEN))
    print("TMDB_API_KEY:", bool(TMDB_API_KEY))

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing!")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("movie", movie))
    app.add_handler(CommandHandler("tv", tv))
    app.add_handler(CommandHandler("anime", anime))
    app.add_handler(CommandHandler("game", game))
    app.add_handler(CommandHandler("favorites", favorites))
    app.add_handler(CommandHandler("history", history))

    # Start polling
    app.run_polling(drop_pending_updates=True)


# ==========================================
# Entry Point
# ==========================================
if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("❌ Startup error:")
        traceback.print_exc()
        raise
