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
    CallbackQueryHandler,
    filters,
)

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

DB_FILE = "bot.db"

# FIX 1: Moved CACHE_DURATION and RESULTS_PER_PAGE outside get_history()
CACHE_DURATION = 86400  # 24 hours
RESULTS_PER_PAGE = 5
BUTTON_NAMES = {
    "lookmovie2.to": "🎬 LookMovie",
    "goku-tv.pics": "🍿 GokuTV",
    "hurawatch.art": "📺 HuraWatch",
    "www8.123moviesfree.net": "🎥 123Movies",
    "desicinemas.to": "🇮🇳 DesiCinema",
    "uhdmovies.pink": "💎 UHD Movies",
    "moviesmod.farm": "🎞 MoviesMod",
}
OTT_SITES = {
    "DE": {
        "Netflix Germany": "https://www.netflix.com/de/",
        "Disney+ Germany": "https://www.disneyplus.com/de-de",
        "Prime Video Germany": "https://www.primevideo.com"
    },

    "US": {
        "Netflix USA": "https://www.netflix.com",
        "Disney+ USA": "https://www.disneyplus.com",
        "Max USA": "https://www.max.com"
    },

    "GB": {
        "Netflix UK": "https://www.netflix.com/gb/",
        "Disney+ UK": "https://www.disneyplus.com/en-gb"
    },

    "IN": {
        "Netflix India": "https://www.netflix.com/in/",
        "Disney+ Hotstar": "https://www.hotstar.com/in",
        "Prime Video India": "https://www.primevideo.com"
    }
}


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

    # Delete history older than 4 days
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
    
def remove_favorite(user_id, title):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM favorites
        WHERE user_id = ? AND title = ?
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
    # FIX 3: Corrected double-escaped regex patterns (\\d → \d, \\s → \s)
    season_episode = re.search(
        r'(S\d+E\d+)',
        query,
        re.IGNORECASE
    )

    season = re.search(
        r'Season\s+(\d+)',
        query,
        re.IGNORECASE
    )

    year = re.search(
        r'(19\d{2}|20\d{2})',
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

    return found


def search_ott(title):
    countries = ["DE", "US", "GB", "IN"]

    found = {}

    for country in countries:

        for name, site in OTT_SITES[country].items():

            result = search_site(title, site)

            if result:
                found[f"{country} • {name}"] = result


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

    # FIX 2: Fixed caption-building block indentation and if/else structure
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

    # Favorite button
    keyboard.append([
    InlineKeyboardButton(
        "⭐ Favorite",
        callback_data=f"fav:{title}"
    ),
    InlineKeyboardButton(
        "📺 OTT",
        callback_data=f"ott:{title}"
    )
])

    # Movie links
        all_results = list(results.items())

    context.user_data["results"] = all_results
    context.user_data["page"] = 0

    start = 0
    end = RESULTS_PER_PAGE

    for domain, url in all_results[start:end]:
        name = BUTTON_NAMES.get(domain, domain)

        keyboard.append([
            InlineKeyboardButton(name, url=url)
        ])

    if len(all_results) > RESULTS_PER_PAGE:
        keyboard.append([
            InlineKeyboardButton(
                "Next ➡",
                callback_data="next"
            )
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

    for row in rows:
        title = row[0]

        keyboard = [[
            InlineKeyboardButton(
                "❌ Remove",
                callback_data=f"remove:{title}"
            )
        ]]

        await update.message.reply_text(
            f"⭐ {title}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


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
# Favorite button handler
# --------------------------------------------------
async def favorite_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith("fav:"):
        title = data.replace("fav:", "")

        add_favorite(query.from_user.id, title)

        await query.answer(
            text="Added to favorites ⭐",
            show_alert=False
        )
        elif data == "next" or data == "prev":

        all_results = context.user_data.get("results", [])

        page = context.user_data.get("page", 0)

        if data == "next":
            page += 1
        else:
            page -= 1

        if page < 0:
            page = 0

        max_page = (len(all_results) - 1) // RESULTS_PER_PAGE

        if page > max_page:
            page = max_page

        context.user_data["page"] = page

        start = page * RESULTS_PER_PAGE
        end = start + RESULTS_PER_PAGE

        keyboard = []

        for domain, url in all_results[start:end]:
            name = BUTTON_NAMES.get(domain, domain)

            keyboard.append([
                InlineKeyboardButton(name, url=url)
            ])

        nav = []

        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    "⬅ Previous",
                    callback_data="prev"
                )
            )

        if page < max_page:
            nav.append(
                InlineKeyboardButton(
                    "Next ➡",
                    callback_data="next"
                )
            )

        if nav:
            keyboard.append(nav)

        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif data.startswith("remove:"):
        title = data.replace("remove:", "")

        remove_favorite(
            query.from_user.id,
            title
        )

        await query.edit_message_text(
            f"❌ Removed:\n{title}"
        )

        elif data.startswith("ott:"):
        title = data.replace("ott:", "")

        results = search_ott(title)

        if not results:
            await query.edit_message_text(
                text=f"❌ No OTT results found for {title}"
            )
            return

        keyboard = []

        keyboard.append([
            InlineKeyboardButton(
                "⬅ Back",
                callback_data=f"back:{title}"
            )
        ])

        for name, url in results.items():
            keyboard.append([
                InlineKeyboardButton(name, url=url)
            ])

        await query.edit_message_text(
            text=f"📺 OTT Results for {title}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

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

    # FIX 6: Added missing init_db() call
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("sites", sites_command))
    app.add_handler(CommandHandler("favorites", favorites_command))
    app.add_handler(CommandHandler("history", history_command))

    # Handle normal messages
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, search_title)
    )
    # FIX 4: Removed extra closing ) from CallbackQueryHandler
    app.add_handler(
        CallbackQueryHandler(favorite_callback)
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
