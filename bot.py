import json
import os
from urllib.parse import urlparse

import requests
import sqlite3
import time
import re
from dotenv import load_dotenv
from ddgs import DDGS
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

import sqlite3

DB_FILE = "bot.db"


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT,
            searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def save_history(user_id, title):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO history (user_id, title)
        VALUES (?, ?)
    """, (user_id, title))

    conn.commit()
    conn.close()


def get_history(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Delete history older than 7 days
    cur.execute("""
        DELETE FROM history
        WHERE searched_at < datetime('now', '-4 days')
    """)

    conn.commit()

    # Remove duplicates and get latest 10
    cur.execute("""
        SELECT title, MAX(searched_at)
        FROM history
        WHERE user_id = ?
        GROUP BY title
        ORDER BY MAX(searched_at) DESC
        LIMIT 10
    """, (user_id,))

    rows = cur.fetchall()

    conn.close()
    return rows
    CACHE_DURATION = 86400  # 24 hours

RESULTS_PER_PAGE = 8


def add_favorite(user_id, title):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT
        )
    """)

    cur.execute("""
        INSERT INTO favorites (user_id, title)
        VALUES (?, ?)
    """, (user_id, title))

    conn.commit()
    conn.close()


def get_favorites(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT title
        FROM favorites
        WHERE user_id = ?
        ORDER BY id DESC
    """, (user_id,))

    rows = cur.fetchall()

    conn.close()
    return rows


def get_cached(query):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            query TEXT PRIMARY KEY,
            data TEXT,
            created_at INTEGER
        )
    """)

    cur.execute("""
        SELECT data, created_at
        FROM cache
        WHERE query = ?
    """, (query,))

    row = cur.fetchone()

    conn.close()

    if not row:
        return None

    data, created_at = row

    if time.time() - created_at > CACHE_DURATION:
        return None

    return json.loads(data)


def save_cache(query, data):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            query TEXT PRIMARY KEY,
            data TEXT,
            created_at INTEGER
        )
    """)

    cur.execute("""
        INSERT OR REPLACE INTO cache (query, data, created_at)
        VALUES (?, ?, ?)
    """, (
        query,
        json.dumps(data),
        int(time.time())
    ))

    conn.commit()
    conn.close()


def parse_advanced_query(query):
    season_episode = re.search(
        r'(S\\d+E\\d+)',
        query,
        re.IGNORECASE
    )

    season = re.search(
        r'Season\\s+(\\d+)',
        query,
        re.IGNORECASE
    )

    year = re.search(
        r'(19\\d{2}|20\\d{2})',
        query
    )

    return {
        "query": query,
        "season_episode": (
            season_episode.group(1)
            if season_episode else None
        ),
        "season": (
            season.group(1)
            if season else None
        ),
        "year": (
            year.group(1)
            if year else None
        )
    }


# --------------------------------------------------
# Load sites from sites.json
# --------------------------------------------------
def load_sites():
    try:
        with open("sites.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            return data.get("sites", [])

    except Exception:
        pass

    return []


# --------------------------------------------------
# Get movie/TV info from TMDb
# --------------------------------------------------
def get_tmdb_info(title: str):
    if not TMDB_API_KEY:
        return None

    try:
        url = "https://api.themoviedb.org/3/search/multi"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title,
            "include_adult": "true",
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
                (best.get("release_date") or best.get("first_air_date") or "")[:4]
            ),
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


# --------------------------------------------------
# Search one site using DuckDuckGo
# --------------------------------------------------
def search_site(title: str, site_url: str):
    domain = urlparse(site_url).netloc.replace("www.", "")
    query = f'{title} site:{domain}'

    try:
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=5)

            for result in results:
                href = result.get("href") or result.get("url")
                if href and domain in href:
                    return href

    except Exception:
        return None

    return None


# --------------------------------------------------
# Search all configured sites
# --------------------------------------------------
def search_movie(title: str):
    sites = load_sites()
    found = {}

    for site in sites:
        result = search_site(title, site)

        if result:
            domain = urlparse(site).netloc.replace("www.", "")
            found[domain] = result

        # Limit to 8 results
        if len(found) >= 8:
            break

    return found


# --------------------------------------------------
# /start command
# --------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to Movie Search Bot!\n\n"
        "Send me any movie or TV show title.\n"
        "Example: Inception"
    )


# --------------------------------------------------
# /help command
# --------------------------------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 How to use this bot:\n\n"
        "1. Send any movie or TV show title.\n"
        "2. The bot will search configured streaming sites.\n"
        "3. You'll get clickable buttons with links.\n\n"
        "Examples:\n"
        "• Inception\n"
        "• Breaking Bad\n"
        "• Interstellar"
    )


# --------------------------------------------------
# /sites command
# --------------------------------------------------
async def sites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sites = load_sites()

    if not sites:
        await update.message.reply_text("❌ No sites configured.")
        return

    domains = []
    for site in sites:
        domains.append(urlparse(site).netloc.replace("www.", ""))

    await update.message.reply_text(
        "🌐 Configured sites:\n\n" + "\n".join(f"• {d}" for d in domains)
    )


# --------------------------------------------------
# Handle movie title searches
# --------------------------------------------------
async def search_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_title = update.message.text.strip()
    save_history(update.message.from_user.id, raw_title)
    status = await update.message.reply_text("🔍 Searching...")


    # Get TMDb info
    parsed = parse_advanced_query(raw_title)
    info = get_tmdb_info(parsed["query"])

    if info:
        title = info["title"]
    else:
        title = raw_title

        # Search configured sites
    cached = get_cached(title)

    if cached:
        results = cached
    else:
        results = search_movie(title)
        save_cache(title, results)

    if not results:
        await status.edit_text(
            f'❌ "{raw_title}" was not found on any configured sites.'
        )
        return

    # Build caption
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
        caption = f'🎬 <b>Results for "{raw_title}"</b>\n'

    caption += "\n🔗 Select a source below:"

    # Create inline buttons
    keyboard = []
    for domain, url in results.items():
        keyboard.append([
            InlineKeyboardButton(domain, url=url)
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Send with poster if available
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
        
# --------------------------------------------------
# /favorites command
# --------------------------------------------------
async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_favorites(update.message.from_user.id)

    if not rows:
        await update.message.reply_text(
            "❌ No favorites saved."
        )
        return

    text = "⭐ Favorites:\n\n"

    for row in rows:
        text += f"• {row[0]}\n"

    await update.message.reply_text(text)


# --------------------------------------------------
# /history command
# --------------------------------------------------
async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_history(update.message.from_user.id)

    if not rows:
        await update.message.reply_text(
            "❌ No history found."
        )
        return

    text = "🕘 Search History:\n\n"

    for row in rows:
        text += f"• {row[0]}\n"

    await update.message.reply_text(text)

# --------------------------------------------------
# Main function
# --------------------------------------------------
def main():
    print("✅ Bot is running on Railway...")
    print("BOT_TOKEN:", bool(BOT_TOKEN))
    print("TMDB_API_KEY:", bool(TMDB_API_KEY))

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing!")

    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY is missing!")

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("sites", sites_command))

    # Handle normal messages
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, search_title)
    )

    # Start bot
    app.run_polling(drop_pending_updates=True)


# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("❌ Startup error:")
        traceback.print_exc()
        raise
