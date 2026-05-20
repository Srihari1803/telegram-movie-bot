import json
import os
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from ddgs import DDGS
from telegram import Update
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


def load_sites():
    """Load websites from sites.json"""
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


def normalize_title(title: str) -> str:
    """Use TMDb to get the exact movie/TV title."""
    if not TMDB_API_KEY:
        return title

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
        if results:
            best = results[0]
            return best.get("title") or best.get("name") or title

    except Exception:
        pass

    return title


def search_site(title: str, site_url: str):
    """Search one site using DuckDuckGo."""
    domain = urlparse(site_url).netloc.replace("www.", "")
    query = f"{title} site:{domain}"

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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Send me a movie or TV show title (e.g. Inception)."
    )


async def search_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_title = update.message.text.strip()

    status = await update.message.reply_text("🔍 Searching...")

    # Normalize title using TMDb
    title = normalize_title(raw_title)

    # Load sites
    sites = load_sites()

    if not sites:
        await status.edit_text("❌ No sites found in sites.json.")
        return

    found = []

    # Search all sites
    for site in sites:
        result = search_site(title, site)

        if result:
            domain = urlparse(site).netloc.replace("www.", "")
            found.append((domain, result))

        # Limit results
        if len(found) >= 8:
            break

    # No results
    if not found:
        await status.edit_text(
            f'❌ "{raw_title}" was not found on any of the {len(sites)} sites.'
        )
        return

    # Format results
    lines = [f'🎬 Results for "{title}":\n']

    for domain, url in found:
        lines.append(f"• {domain}\n{url}\n")

    await status.edit_text(
        "\n".join(lines),
        disable_web_page_preview=True,
    )


def main():
    print("✅ Bot is running on Render...")

    # Verify environment variables
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing!")
    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY is missing!")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, search_title)
    )

    # Start polling
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("❌ Startup error:")
        traceback.print_exc()
        raise
