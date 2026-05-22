import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Awaitable, Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
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
from telegram.error import BadRequest

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

DB_FILE = "bot.db"

CACHE_DURATION = 86400  # 24 hours
RESULTS_PER_PAGE = 5
MIN_MATCH_SCORE = 0.55
GOOD_MATCH_SCORE = 0.78
MAX_QUERY_VARIANTS = 3
MAX_SEARCH_RESULTS = 8
MAX_CANDIDATE_LENGTH = 200
MAX_OVERVIEW_LENGTH = 400

CACHE_PREFIX_MOVIE = "movie"
CACHE_PREFIX_OTT = "ott"
CACHE_PREFIX_TMDB = "tmdb"
CACHE_EMPTY_MARKER = {"_empty": True}
SUPPORTED_TMDB_MEDIA_TYPES = {"movie", "tv"}
MAX_TMDB_RATING = 10

CATEGORY_MOVIE = "movie"
CATEGORY_TV = "tv"
CATEGORY_ANIME = "anime"
CATEGORY_GAME = "game"

CATEGORY_META = {
    CATEGORY_MOVIE: {"label": "Movie", "icon": "🎬"},
    CATEGORY_TV: {"label": "TV", "icon": "📺"},
    CATEGORY_ANIME: {"label": "Anime", "icon": "🍿"},
    CATEGORY_GAME: {"label": "Game", "icon": "🎮"},
}

SEARCH_CATEGORIES = [
    CATEGORY_MOVIE,
    CATEGORY_TV,
    CATEGORY_ANIME,
    CATEGORY_GAME,
]
logger = logging.getLogger(__name__)

@contextmanager
def db_connection(commit: bool = False) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_FILE)
    try:
        yield conn
        if commit:
            conn.commit()
    finally:
        conn.close()


def load_category_sites() -> Dict[str, List[str]]:
    try:
        with open("sites.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        logger.exception(
            "Failed to load sites.json. Please verify the file exists and contains valid JSON."
        )
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        CATEGORY_MOVIE: list(data.get(CATEGORY_MOVIE, [])),
        CATEGORY_TV: list(data.get(CATEGORY_TV, [])),
        CATEGORY_ANIME: list(data.get(CATEGORY_ANIME, [])),
        CATEGORY_GAME: list(data.get(CATEGORY_GAME, [])),
    }

BUTTON_NAMES = {
    "lookmovie2.to": "🎬 LookMovie",
    "goku-tv.pics": "🍿 GokuTV",
    "hurawatch.art": "📺 HuraWatch",
    "www8.123moviesfree.net": "🎥 123Movies",
    "ww19.myasiantv.es": "📺 MyAsianTV",
    "desicinemas.to": "🇮🇳 DesiCinema",
    "uhdmovies.pink": "💎 UHD Movies",
    "moviesmod.farm": "🎞 MoviesMod",
}

OTT_SITES = {
    "DE": {
        "Netflix Germany": "https://www.netflix.com/de/",
        "Disney+ Germany": "https://www.disneyplus.com/de-de",
        "Prime Video Germany": "https://www.primevideo.com",
    },
    "US": {
        "Netflix USA": "https://www.netflix.com",
        "Disney+ USA": "https://www.disneyplus.com",
        "Max USA": "https://www.max.com",
    },
    "GB": {
        "Netflix UK": "https://www.netflix.com/gb/",
        "Disney+ UK": "https://www.disneyplus.com/en-gb",
    },
    "IN": {
        "Netflix India": "https://www.netflix.com/in/",
        "Disney+ Hotstar": "https://www.hotstar.com/in",
        "Prime Video India": "https://www.primevideo.com",
    },
}

COUNTRY_INFO = {
    "DE": {"flag": "🇩🇪", "name": "Germany"},
    "US": {"flag": "🇺🇸", "name": "USA"},
    "GB": {"flag": "🇬🇧", "name": "UK"},
    "IN": {"flag": "🇮🇳", "name": "India"},
}



# --------------------------------------------------
# Database helpers
# --------------------------------------------------
def init_db():
    with db_connection(commit=True) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT,
            searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT
        )
    """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            query TEXT PRIMARY KEY,
            data TEXT,
            created_at INTEGER
        )
    """)


def save_history(user_id: int, title: str) -> None:
    with db_connection(commit=True) as conn:
        conn.execute("""
        INSERT INTO history (user_id, title)
        VALUES (?, ?)
    """, (user_id, title))


def get_history(user_id: int) -> List[Tuple[str, str]]:
    with db_connection(commit=True) as conn:
        conn.execute("""
        DELETE FROM history
        WHERE searched_at < datetime('now', '-4 days')
    """)
        rows = conn.execute("""
        SELECT title, MAX(searched_at)
        FROM history
        WHERE user_id = ?
        GROUP BY title
        ORDER BY MAX(searched_at) DESC
        LIMIT 10
    """, (user_id,))
        return rows.fetchall()


def add_favorite(user_id: int, title: str) -> None:
    with db_connection(commit=True) as conn:
        conn.execute("""
        INSERT INTO favorites (user_id, title)
        VALUES (?, ?)
    """, (user_id, title))


def remove_favorite(user_id: int, title: str) -> None:
    with db_connection(commit=True) as conn:
        conn.execute("""
        DELETE FROM favorites
        WHERE user_id = ? AND title = ?
    """, (user_id, title))


def get_favorites(user_id: int) -> List[Tuple[str]]:
    with db_connection() as conn:
        rows = conn.execute("""
        SELECT title
        FROM favorites
        WHERE user_id = ?
        ORDER BY id DESC
    """, (user_id,))
        return rows.fetchall()


def make_cache_key(prefix: str, query: str) -> Optional[str]:
    normalized = normalize_title(query)
    if not normalized:
        return None
    return f"{prefix}:{normalized}"


def get_cached(cache_key: str) -> Optional[Any]:
    with db_connection(commit=True) as conn:
        row = conn.execute("""
        SELECT data, created_at
        FROM cache
        WHERE query = ?
    """, (cache_key,))
        row = row.fetchone()
        if not row:
            return None
        data, created_at = row
        if time.time() - created_at > CACHE_DURATION:
            conn.execute(
                "DELETE FROM cache WHERE query = ?",
                (cache_key,),
            )
            return None
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            conn.execute(
                "DELETE FROM cache WHERE query = ?",
                (cache_key,),
            )
            return None


def save_cache(cache_key: str, data: Any) -> None:
    with db_connection(commit=True) as conn:
        conn.execute("""
        INSERT OR REPLACE INTO cache (query, data, created_at)
        VALUES (?, ?, ?)
    """, (cache_key, json.dumps(data), int(time.time())))


def cleanup_cache() -> None:
    cutoff = int(time.time()) - CACHE_DURATION
    with db_connection(commit=True) as conn:
        conn.execute(
            "DELETE FROM cache WHERE created_at < ?",
            (cutoff,),
        )


def is_empty_cache(cached) -> bool:
    """Check whether a cached payload represents an empty marker."""
    return isinstance(cached, dict) and cached.get("_empty")


# --------------------------------------------------
# Query helpers
# --------------------------------------------------
def parse_advanced_query(query):
    season_episode = re.search(r'(S\d+E\d+)', query, re.IGNORECASE)
    season = re.search(r'Season\s+(\d+)', query, re.IGNORECASE)
    year = re.search(r'(19\d{2}|20\d{2})', query)
    return {
        "query": query,
        "season_episode": season_episode.group(1) if season_episode else None,
        "season": season.group(1) if season else None,
        "year": year.group(1) if year else None,
    }


def normalize_title(title: str) -> str:
    if not title:
        return ""
    text = title.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"['']", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def category_display_label(category: str) -> str:
    meta = CATEGORY_META.get(category, CATEGORY_META[CATEGORY_MOVIE])
    return f"{meta['icon']} {meta['label']}"
def detect_content_type(query: str) -> str:
    try:
        with DDGS() as ddgs:
            results = fetch_ddgs_results(ddgs, f"{query} wikipedia", 5)

        text = " ".join(
            (
                r.get("title", "")
                + " "
                + r.get("body", "")
            )
            for r in results
        ).lower()

        if "tv series" in text or "television series" in text:
            return CATEGORY_TV

        if "anime" in text:
            return CATEGORY_ANIME

        if "video game" in text:
            return CATEGORY_GAME

        if "season" in text:
            return CATEGORY_TV

    except Exception:
        logger.exception("Category detection failed")

    return CATEGORY_MOVIE


def get_category_cache_prefix(category: str) -> str:
    return f"{CACHE_PREFIX_MOVIE}:{category}"


def resolve_cached_category_results(cached_payload: Any) -> Dict[str, str]:
    if is_empty_cache(cached_payload):
        return {}
    if isinstance(cached_payload, dict):
        return cached_payload
    return {}


# --------------------------------------------------
# TMDb info
# --------------------------------------------------
def get_tmdb_info(title: str):
    if not TMDB_API_KEY:
        return None
    if not title:
        return None
    cache_key = make_cache_key(CACHE_PREFIX_TMDB, title)
    if not cache_key:
        return None
    cached = get_cached(cache_key)
    if cached is not None:
        if is_empty_cache(cached):
            return None
        return cached
    try:
        url = "https://api.themoviedb.org/3/search/multi"
        params = {
            "api_key": TMDB_API_KEY,
            "query": title,
            "include_adult": "true",
        }
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if not results:
            save_cache(cache_key, CACHE_EMPTY_MARKER)
            return None
        aliases = []
        for item in results[:5]:
            alias_title = item.get("title") or item.get("name")
            if alias_title:
                aliases.append(alias_title)
        best = results[0]
        media_type = best.get("media_type")
        seasons = []
        origin_country = []
        tmdb_id = best.get("id")
        if media_type == "tv" and tmdb_id:
            try:
                tv_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
                tv_response = requests.get(
                    tv_url,
                    params={"api_key": TMDB_API_KEY},
                    timeout=15,
                )
                tv_response.raise_for_status()
                tv_data = tv_response.json()
                seasons = tv_data.get("seasons") or []
                origin_country = tv_data.get("origin_country") or []
            except Exception:
                logger.exception("TMDB TV details lookup failed for %s", title)
        info = {
            "title": best.get("title") or best.get("name") or title,
            "year": (best.get("release_date") or best.get("first_air_date") or "")[:4],
            "rating": best.get("vote_average"),
            "overview": best.get("overview"),
            "tmdb_id": tmdb_id,
            "poster": (
                f"https://image.tmdb.org/t/p/w500{best['poster_path']}"
                if best.get("poster_path")
                else None
            ),
            "aliases": aliases,
            "media_type": media_type,
            "seasons": seasons,
            "genre_ids": best.get("genre_ids") or [],
            "original_language": best.get("original_language"),
            "origin_country": origin_country,
        }
        save_cache(cache_key, info)
        return info
    except Exception:
        logger.exception("TMDB lookup failed for %s", title)
        return None


# --------------------------------------------------
# Search helpers
# --------------------------------------------------
def fetch_ddgs_results(ddgs: DDGS, query: str, max_results: int) -> List[Dict[str, Any]]:
    """Return a list of DDG results, handling None responses safely."""
    try:
        results = ddgs.text(query, max_results=max_results)
    except Exception:
        # DDGS can raise a variety of network/runtime errors; treat failures as empty.
        logger.exception("DDG search failed for query: %s", query)
        return []
    return list(results or [])


def search_category_site(title: str, site: str) -> Optional[Tuple[str, str]]:
    site_domain = urlparse(site).netloc.replace("www.", "")
    if not site_domain:
        return None
    queries = [
        f'"{title}" site:{site_domain}',
        f"{title} site:{site_domain}",
    ]
    try:
        with DDGS() as ddgs:
            for query in queries:
                logger.info("DDG category search query: %s", query)
                results = fetch_ddgs_results(ddgs, query, MAX_SEARCH_RESULTS)
                logger.info(
                    "DDG category search returned %d results",
                    len(results),
                )
                for item in results:
                    href = item.get("href") or item.get("url")
                    if not href:
                        continue
                    domain = urlparse(href).netloc.replace("www.", "")
                    if site_domain in domain:
                        return site_domain, href
    except Exception:
        logger.exception("Search failed for %s", site_domain)
    return None


def search_category_sites(title: str, category: str) -> Dict[str, str]:
    sites = load_category_sites().get(category, [])
    logger.info("Category=%s Sites count=%s", category, len(sites))

    found = {}

    if not sites:
        return found

    max_workers = min(8, len(sites))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(search_category_site, title, site)
            for site in sites
        ]
        for future in as_completed(futures):
            result = future.result()
            if not result:
                continue
            site_domain, href = result
            found[site_domain] = href

    logger.info("Found %d site results for %s", len(found), title)
    return found


def normalize_provider_name(provider_name: str) -> str:
    """Normalize provider names for TMDB matching by stripping non-alphanumerics."""
    return re.sub(r"[^a-z0-9]+", "", provider_name.lower())


PROVIDER_ALIASES = {
    "primevideo": {"amazonprimevideo", "primevideo"},
    "disneyplus": {"disneyplus", "disneyplushotstar", "hotstar"},
    "max": {"hbomax", "max"},
}


def provider_is_available(normalized_short: str, normalized_names: Set[str]) -> bool:
    if not normalized_short or not normalized_names:
        return False
    if normalized_short in normalized_names:
        return True
    alias_names = PROVIDER_ALIASES.get(normalized_short, set())
    return bool(alias_names.intersection(normalized_names))


def get_tmdb_watch_providers(tmdb_id: int, media_type: str) -> Dict[str, Any]:
    if not TMDB_API_KEY or not tmdb_id or media_type not in SUPPORTED_TMDB_MEDIA_TYPES:
        return {}
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers"
    try:
        response = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=15)
        response.raise_for_status()
        payload = response.json()
        return payload.get("results", {}) if isinstance(payload, dict) else {}
    except Exception:
        logger.exception("TMDB watch provider lookup failed for %s", tmdb_id)
        return {}


def extract_provider_names(country_data: Dict[str, Any]) -> List[str]:
    """Extract TMDB watch provider names from supported offer types."""
    names = []
    for key in ("flatrate", "ads", "free", "rent", "buy"):
        entries = country_data.get(key) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict) and entry.get("provider_name"):
                names.append(entry["provider_name"])
    return names


def build_empty_ott_results() -> Dict[str, List[Dict[str, Any]]]:
    results = {}
    for country_code, providers in OTT_SITES.items():
        results[country_code] = [
            build_ott_provider_result(country_code, provider_name, None)
            for provider_name in providers
        ]
    return results


def search_ott(
    title: str,
    tmdb_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    info = tmdb_info if tmdb_info is not None else get_tmdb_info(title)
    if info is None:
        return build_empty_ott_results()
    tmdb_id = info.get("tmdb_id")
    media_type = info.get("media_type")
    if not tmdb_id or media_type not in SUPPORTED_TMDB_MEDIA_TYPES:
        return build_empty_ott_results()

    tmdb_results = get_tmdb_watch_providers(tmdb_id, media_type)
    found = {}
    for country_code, providers in OTT_SITES.items():
        country_data = tmdb_results.get(country_code, {}) if tmdb_results else {}
        provider_names = extract_provider_names(country_data)
        normalized_names = {
            normalize_provider_name(name) for name in provider_names if name
        }
        found[country_code] = []
        for provider_name, site in providers.items():
            short_name = shorten_ott_provider_name(provider_name, country_code)
            normalized_short = normalize_provider_name(short_name)
            available = provider_is_available(normalized_short, normalized_names)
            url = site if available else None
            found[country_code].append(
                build_ott_provider_result(country_code, provider_name, url)
            )
    return found


# --------------------------------------------------
# Keyboard builder helpers
# --------------------------------------------------
def build_main_keyboard(
    all_results,
    current_title,
    page=0,
    show_episode_guide=False,
    active_category: str = CATEGORY_MOVIE,
):
    """Build the standard results keyboard for a given page."""
    def category_button_text(category: str) -> str:
        meta = CATEGORY_META[category]
        text = f"{meta['icon']} {meta['label']}"
        return f"✅ {text}" if category == active_category else text

    keyboard = [[
        InlineKeyboardButton("⭐ Favorite", callback_data=f"fav:{current_title}"),
        InlineKeyboardButton("📺 OTT", callback_data=f"ott:{current_title}"),
    ]]
    keyboard.append([
        InlineKeyboardButton(category_button_text(CATEGORY_MOVIE), callback_data=f"cat:{CATEGORY_MOVIE}"),
        InlineKeyboardButton(category_button_text(CATEGORY_TV), callback_data=f"cat:{CATEGORY_TV}"),
    ])
    keyboard.append([
        InlineKeyboardButton(category_button_text(CATEGORY_ANIME), callback_data=f"cat:{CATEGORY_ANIME}"),
        InlineKeyboardButton(category_button_text(CATEGORY_GAME), callback_data=f"cat:{CATEGORY_GAME}"),
    ])
    if show_episode_guide:
        keyboard.append([
            InlineKeyboardButton("📚 Episode Guide", callback_data="guide:episodes")
        ])

    start = page * RESULTS_PER_PAGE
    end = start + RESULTS_PER_PAGE

    for domain, url in all_results[start:end]:
        name = BUTTON_NAMES.get(domain, domain)
        keyboard.append([InlineKeyboardButton(name, url=url)])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Previous", callback_data="prev"))
    if end < len(all_results):
        nav.append(InlineKeyboardButton("Next ➡", callback_data="next"))
    if nav:
        keyboard.append(nav)

    return keyboard


async def safe_telegram_call(action: Awaitable[Any], description: str) -> Any:
    """Run a Telegram API call and log BadRequest or unexpected errors."""
    try:
        return await action
    except BadRequest as exc:
        logger.warning("Telegram BadRequest during %s: %s", description, exc)
    except Exception:
        logger.exception("Telegram error during %s", description)
    return None


def shorten_ott_provider_name(provider_name: str, country_code: str) -> str:
    """Return a compact provider label without the trailing country name."""
    country_name = COUNTRY_INFO.get(country_code, {}).get("name", "")
    short_name = provider_name
    if country_name and short_name.endswith(f" {country_name}"):
        short_name = short_name[:-(len(country_name) + 1)]
    country_aliases = {
        "US": "USA",
        "GB": "UK",
    }
    country_alias = country_aliases.get(country_code)
    if country_alias and short_name.endswith(f" {country_alias}"):
        short_name = short_name[:-(len(country_alias) + 1)]
    return short_name.strip()


def build_ott_provider_result(
    country_code: str,
    provider_name: str,
    url: Optional[str],
) -> Dict[str, Any]:
    """Build a provider result entry with display name and availability metadata."""
    return {
        "name": provider_name,
        "display_name": shorten_ott_provider_name(provider_name, country_code),
        "available": bool(url),
        "status": "Available" if url else "Unavailable",
        "url": url,
    }


def format_ott_text(title: str, ott_results: Dict[str, List[Dict[str, Any]]]) -> str:
    """Format OTT availability grouped by country with flags and status."""
    summary = (
        "✅ Available on tracked platforms"
        if has_ott_availability(ott_results)
        else "❌ Not available on tracked platforms"
    )
    lines = [f"📺 OTT Availability for {title}", summary, ""]
    for country_code, providers in ott_results.items():
        country = COUNTRY_INFO.get(country_code, {"flag": "🏳️", "name": country_code})
        lines.append(f"{country['flag']} {country['name']}")
        for provider in providers:
            status_icon = "✅" if provider["available"] else "❌"
            lines.append(
                f"  • {provider['display_name']} — {status_icon} {provider['status']}"
            )
        lines.append("")
    return "\n".join(lines).strip()


def build_ott_keyboard(ott_results: Dict[str, List[Dict[str, Any]]]) -> InlineKeyboardMarkup:
    """Create buttons for available OTT providers with country flags."""
    keyboard = []
    for country_code, providers in ott_results.items():
        country = COUNTRY_INFO.get(country_code, {"flag": "🏳️", "name": country_code})
        available_providers = [provider for provider in providers if provider["available"]]
        for provider in available_providers:
            keyboard.append([
                InlineKeyboardButton(
                    f"{country['flag']} {provider['display_name']}",
                    url=provider["url"],
                )
            ])
    keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back:main")])
    return InlineKeyboardMarkup(keyboard)


def has_ott_availability(ott_results: Dict[str, List[Dict[str, Any]]]) -> bool:
    """Return True when at least one OTT provider is available."""
    return any(
        provider["available"]
        for providers in ott_results.values()
        for provider in providers
    )


def format_season_label(season: Dict[str, Any]) -> str:
    """Build a compact UI label for one season."""
    season_number = season.get("season_number")
    if season_number is None:
        return "📺 Season"
    episode_count = season.get("episode_count")
    if episode_count:
        return f"📺 Season {season_number} ({episode_count} eps)"
    return f"📺 Season {season_number}"


def format_episode_guide_text(title: str, seasons: List[Dict[str, Any]]) -> str:
    """Create grouped season guide text for TV series."""
    lines = [f"📚 Episode Guide for {title}", ""]
    if not seasons:
        lines.append("❌ No season data available.")
        return "\n".join(lines)
    for season in seasons:
        if not isinstance(season, dict):
            continue
        season_number = season.get("season_number")
        if season_number is None:
            continue
        lines.append(format_season_label(season))
    if len(lines) == 2:
        lines.append("❌ No season data available.")
    return "\n".join(lines)


def build_episode_guide_keyboard(seasons: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    """Build season buttons in a cleaner two-column UI."""
    keyboard = []
    row = []
    for season in seasons:
        if not isinstance(season, dict):
            continue
        season_number = season.get("season_number")
        if season_number is None:
            continue
        row.append(
            InlineKeyboardButton(
                f"📺 Season {season_number}",
                callback_data=f"season:{season_number}",
            )
        )
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back:main")])
    return InlineKeyboardMarkup(keyboard)


# --------------------------------------------------
# /start command
# --------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to Movie Search Bot!\n\n"
        "Send any title for automatic category detection.\n"
        "Or use /movie, /tv, /anime, /game.\n"
        "Example: Inception"
    )


# --------------------------------------------------
# /help command
# --------------------------------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 How to use this bot:\n\n"
        "1. Send any title to auto-detect category (Movie/TV/Anime/Game).\n"
        "2. Or search manually with commands:\n"
        "   • /movie <title>\n"
        "   • /tv <title>\n"
        "   • /anime <title>\n"
        "   • /game <title>\n"
        "3. The bot searches configured sites and returns links.\n\n"
        "Examples:\n"
        "• Inception\n"
        "• /movie Interstellar\n"
        "• /anime Attack on Titan\n"
        "• /game GTA 5"
    )


# --------------------------------------------------
# /sites command
# --------------------------------------------------
async def sites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text = "🌐 Configured Sites\n\n"
        sites_by_category = load_category_sites()
        for category, sites in sites_by_category.items():
            text += f"📂 {category.upper()} ({len(sites)})\n"

            for site in sites:
                domain = urlparse(site).netloc.replace("www.", "")
                text += f"• {domain}\n"

            text += "\n"

        await update.message.reply_text(text)

    except Exception:
        await update.message.reply_text(
            "❌ Could not load configured sites. Please verify sites.json exists and is valid."
        )

# --------------------------------------------------
# Search commands
# --------------------------------------------------
def get_command_query(context: ContextTypes.DEFAULT_TYPE) -> str:
    return " ".join(context.args).strip() if context.args else ""


def build_results_caption(
    raw_title: str,
    info: Optional[Dict[str, Any]],
    category: str,
) -> str:
    category_label = category_display_label(category)
    if info:
        caption = f"🎬 <b>{info['title']}"
        if info["year"]:
            caption += f" ({info['year']})"
        caption += "</b>\n"
        caption += f"🧭 Category: {category_label}\n"
        rating = info.get("rating")
        # TMDB ratings are expected on a 0-10 scale.
        if isinstance(rating, (int, float)) and 0 <= rating <= MAX_TMDB_RATING:
            caption += f"⭐ {rating:.1f}/{MAX_TMDB_RATING}\n"
        if info["overview"]:
            overview = info["overview"]
            if len(overview) > MAX_OVERVIEW_LENGTH:
                overview = overview[:MAX_OVERVIEW_LENGTH] + "..."
            caption += f"\n{overview}\n"
    else:
        caption = (
            f'🎬 <b>Results for "{raw_title}"</b>\n'
            f"🧭 Category: {category_label}\n"
        )
    caption += "\n🔗 Select a source below:"
    return caption


async def execute_search(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw_title: str,
    forced_category: Optional[str] = None,
):
    if not update.message:
        return
    save_history(update.message.from_user.id, raw_title)
    status = await safe_telegram_call(
        update.message.reply_text("🔍 Searching..."),
        "send status",
    )

    try:
        parsed = parse_advanced_query(raw_title)
        info = await asyncio.to_thread(get_tmdb_info, parsed["query"])
        title = info["title"] if info else raw_title
        aliases = info.get("aliases", []) if info else []
        is_tv = bool(info and info.get("media_type") == "tv")
        seasons = []
        if info and isinstance(info.get("seasons"), list):
            seasons = [
                season for season in info["seasons"]
                if isinstance(season, dict) and season.get("season_number") is not None
            ]

        # Use TMDB media_type first, fall back to DDG detection
        if forced_category in CATEGORY_META:
            detected_category = forced_category
        elif info:
            if info.get("media_type") == "tv":
                detected_category = CATEGORY_TV
            elif info.get("media_type") == "movie":
                detected_category = CATEGORY_MOVIE
            else:
                detected_category = await asyncio.to_thread(
                    detect_content_type,
                    raw_title,
                )
        else:
            detected_category = await asyncio.to_thread(
                detect_content_type,
                raw_title,
            )

        cache_key = make_cache_key(get_category_cache_prefix(detected_category), title)
        cached = get_cached(cache_key) if cache_key else None
        if cached is not None:
            results = resolve_cached_category_results(cached)
        else:
            results = await asyncio.to_thread(
                search_category_sites,
                title,
                detected_category,
            )
            if cache_key:
                save_cache(cache_key, results or CACHE_EMPTY_MARKER)

        if not results:
            message = f'❌ "{raw_title}" was not found on any configured sites.'
            if status:
                await safe_telegram_call(status.edit_text(message), "edit status empty")
            else:
                await safe_telegram_call(
                    update.message.reply_text(message),
                    "send empty status",
                )
            return

        caption = build_results_caption(raw_title, info, detected_category)

        all_results = list(results.items())
        normalized_title = normalize_title(title)
        context.user_data["results"] = all_results
        context.user_data["category_results"] = {
            detected_category: results
        }
        context.user_data["category_pages"] = {
            category: 0 for category in SEARCH_CATEGORIES
        }
        context.user_data["category_cache_keys"] = (
            {detected_category: cache_key} if cache_key else {}
        )
        context.user_data["current_title"] = title
        context.user_data["raw_title"] = raw_title
        context.user_data["current_tmdb_info"] = info
        context.user_data["current_title_key"] = normalized_title
        context.user_data["current_cache_key"] = cache_key
        context.user_data["current_cache_prefix"] = get_category_cache_prefix(detected_category)
        context.user_data["current_category"] = detected_category
        context.user_data["page"] = 0
        context.user_data["main_caption"] = caption
        context.user_data["current_is_tv"] = is_tv
        context.user_data["current_seasons"] = seasons
        context.user_data.setdefault("aliases", {})[normalized_title] = aliases

        keyboard = build_main_keyboard(
            all_results,
            title,
            page=0,
            show_episode_guide=is_tv and bool(seasons),
            active_category=detected_category,
        )
        reply_markup = InlineKeyboardMarkup(keyboard)

        if info and info.get("poster"):
            sent = await safe_telegram_call(
                update.message.reply_photo(
                    photo=info["poster"],
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                ),
                "send poster",
            )
            if sent and status:
                await safe_telegram_call(status.delete(), "delete status")
            elif status:
                await safe_telegram_call(
                    status.edit_text(
                        caption,
                        parse_mode="HTML",
                        reply_markup=reply_markup,
                        disable_web_page_preview=True,
                    ),
                    "edit status fallback",
                )
        else:
            if status:
                await safe_telegram_call(
                    status.edit_text(
                        caption,
                        parse_mode="HTML",
                        reply_markup=reply_markup,
                        disable_web_page_preview=True,
                    ),
                    "edit status",
                )
            else:
                await safe_telegram_call(
                    update.message.reply_text(
                        caption,
                        parse_mode="HTML",
                        reply_markup=reply_markup,
                        disable_web_page_preview=True,
                    ),
                    "send results",
                )
    except Exception:
        logger.exception("Search failed for %s", raw_title)
        message = "❌ Something went wrong. Please try again."
        if status:
            await safe_telegram_call(status.edit_text(message), "edit status error")
        else:
            await safe_telegram_call(
                update.message.reply_text(message),
                "send error",
            )


async def search_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    raw_title = update.message.text.strip()
    if not raw_title:
        return
    await execute_search(update, context, raw_title)


async def movie_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    query = get_command_query(context)
    if not query:
        await update.message.reply_text("Usage: /movie <title>")
        return
    await execute_search(update, context, query, forced_category=CATEGORY_MOVIE)


async def tv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    query = get_command_query(context)
    if not query:
        await update.message.reply_text("Usage: /tv <title>")
        return
    await execute_search(update, context, query, forced_category=CATEGORY_TV)


async def anime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    query = get_command_query(context)
    if not query:
        await update.message.reply_text("Usage: /anime <title>")
        return
    await execute_search(update, context, query, forced_category=CATEGORY_ANIME)


async def game_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    query = get_command_query(context)
    if not query:
        await update.message.reply_text("Usage: /game <title>")
        return
    await execute_search(update, context, query, forced_category=CATEGORY_GAME)


# --------------------------------------------------
# /favorites command
# --------------------------------------------------
async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_favorites(update.message.from_user.id)
    if not rows:
        await update.message.reply_text("❌ No favorites saved.")
        return
    for row in rows:
        title = row[0]
        keyboard = [[
            InlineKeyboardButton("❌ Remove", callback_data=f"remove:{title}")
        ]]
        await update.message.reply_text(
            f"⭐ {title}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


# --------------------------------------------------
# /history command
# --------------------------------------------------
async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_history(update.message.from_user.id)
    if not rows:
        await update.message.reply_text("❌ No history found.")
        return
    text = "🕘 Search History:\n\n"
    for row in rows:
        text += f"• {row[0]}\n"
    await update.message.reply_text(text)


def get_category_page(context: ContextTypes.DEFAULT_TYPE, category: str) -> int:
    pages = context.user_data.setdefault("category_pages", {})
    return pages.get(category, 0)


def set_category_page(context: ContextTypes.DEFAULT_TYPE, category: str, page: int):
    pages = context.user_data.setdefault("category_pages", {})
    pages[category] = page
    context.user_data["page"] = page


def get_cached_results_for_category(
    context: ContextTypes.DEFAULT_TYPE,
    category: str,
) -> Dict[str, str]:
    title = context.user_data.get("current_title", "")
    if not title:
        return {}
    cache_keys = context.user_data.get("category_cache_keys", {})
    cache_key = cache_keys.get(category)
    if not cache_key:
        cache_key = make_cache_key(get_category_cache_prefix(category), title)
    cached = get_cached(cache_key) if cache_key else None
    if cached is None:
        return {}
    return resolve_cached_category_results(cached)


def get_results_for_category(
    context: ContextTypes.DEFAULT_TYPE,
    category: str,
) -> Dict[str, str]:
    category_results = context.user_data.setdefault("category_results", {})
    results = category_results.get(category)
    if results is not None:
        return results
    results = get_cached_results_for_category(context, category)
    category_results[category] = results
    return results


def set_active_category(context: ContextTypes.DEFAULT_TYPE, category: str):
    category_results = get_results_for_category(context, category)
    all_results = list(category_results.items())
    current_page = get_category_page(context, category)
    max_page = max(0, (len(all_results) - 1) // RESULTS_PER_PAGE) if all_results else 0
    current_page = max(0, min(current_page, max_page))
    set_category_page(context, category, current_page)

    raw_title = context.user_data.get("raw_title", context.user_data.get("current_title", ""))
    info = context.user_data.get("current_tmdb_info")
    context.user_data["results"] = all_results
    context.user_data["current_category"] = category
    context.user_data["current_cache_prefix"] = get_category_cache_prefix(category)
    context.user_data["current_cache_key"] = context.user_data.get(
        "category_cache_keys",
        {},
    ).get(category)
    context.user_data["main_caption"] = build_results_caption(raw_title, info, category)


# --------------------------------------------------
# Callback handler
# --------------------------------------------------
async def favorite_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    if not data:
        await safe_telegram_call(query.answer(text="Invalid action."), "callback answer")
        return
    try:
        # --- Add to favorites ---
        if data.startswith("fav:"):
            title = data.replace("fav:", "")
            add_favorite(query.from_user.id, title)
            await safe_telegram_call(
                query.answer(text="Added to favorites ⭐", show_alert=False),
                "favorite callback",
            )

        # --- Pagination: next / prev ---
        elif data in ("next", "prev"):
            await safe_telegram_call(query.answer(), "pagination callback")
            current_title = context.user_data.get("current_title", "")
            current_category = context.user_data.get("current_category", CATEGORY_MOVIE)
            category_results = get_results_for_category(context, current_category)
            all_results = list(category_results.items())
            page = get_category_page(context, current_category)
            show_episode_guide = (
                current_category == CATEGORY_TV
                and bool(context.user_data.get("current_is_tv"))
                and bool(context.user_data.get("current_seasons", []))
            )

            if not all_results:
                await safe_telegram_call(
                    query.answer(text="No results to paginate."),
                    "pagination empty",
                )
                return

            if data == "next":
                page += 1
            else:
                page -= 1

            max_page = max(0, (len(all_results) - 1) // RESULTS_PER_PAGE)
            page = max(0, min(page, max_page))
            set_category_page(context, current_category, page)
            context.user_data["results"] = all_results

            keyboard = build_main_keyboard(
                all_results,
                current_title,
                page,
                show_episode_guide=show_episode_guide,
                active_category=current_category,
            )
            await safe_telegram_call(
                query.edit_message_reply_markup(
                    reply_markup=InlineKeyboardMarkup(keyboard)
                ),
                "pagination edit markup",
            )

        # --- Category switch ---
        elif data.startswith("cat:"):
            await safe_telegram_call(query.answer(), "category switch callback")
            category = data.replace("cat:", "").strip()
            if category not in SEARCH_CATEGORIES:
                await safe_telegram_call(
                    query.answer(text="Invalid category."),
                    "category switch invalid",
                )
                return

            # If results for this category not yet fetched, search now
            category_results_store = context.user_data.setdefault("category_results", {})
            if category not in category_results_store or not category_results_store[category]:
                title = context.user_data.get("current_title", "")
                if title:
                    category_cache_keys = context.user_data.setdefault(
                        "category_cache_keys",
                        {},
                    )
                    cache_key = category_cache_keys.get(category)
                    if not cache_key:
                        cache_key = make_cache_key(
                            get_category_cache_prefix(category),
                            title,
                        )
                        if cache_key:
                            category_cache_keys[category] = cache_key
                    cached = get_cached(cache_key) if cache_key else None
                    if cached is not None:
                        new_results = resolve_cached_category_results(cached)
                    else:
                        new_results = await asyncio.to_thread(
                            search_category_sites,
                            title,
                            category,
                        )
                        if cache_key:
                            save_cache(cache_key, new_results or CACHE_EMPTY_MARKER)
                    category_results_store[category] = new_results
                    if not new_results:
                        await safe_telegram_call(
                            query.answer(
                                text="No results found in this category",
                                show_alert=False,
                            ),
                            "category switch no results",
                        )
                        return

            set_active_category(context, category)
            current_title = context.user_data.get("current_title", "")
            all_results = context.user_data.get("results", [])
            page = get_category_page(context, category)
            show_episode_guide = (
                category == CATEGORY_TV
                and bool(context.user_data.get("current_is_tv"))
                and bool(context.user_data.get("current_seasons", []))
            )

            keyboard = build_main_keyboard(
                all_results,
                current_title,
                page=page,
                show_episode_guide=show_episode_guide,
                active_category=category,
            )
            caption = context.user_data.get("main_caption", "")
            if not all_results:
                caption = (
                    f"{caption}\n\n❌ No results found for "
                    f"{category_display_label(category)}."
                )
            await safe_telegram_call(
                query.edit_message_text(
                    caption,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    disable_web_page_preview=True,
                ),
                "category switch edit",
            )

        # --- Remove from favorites ---
        elif data.startswith("remove:"):
            title = data.replace("remove:", "")
            remove_favorite(query.from_user.id, title)
            await safe_telegram_call(
                query.edit_message_text(f"❌ Removed:\n{title}"),
                "remove favorite",
            )

        # --- Back to main results ---
        elif data == "back:main":
            await safe_telegram_call(query.answer(), "back to main callback")
            current_title = context.user_data.get("current_title", "")
            current_category = context.user_data.get("current_category", CATEGORY_MOVIE)
            set_active_category(context, current_category)
            all_results = context.user_data.get("results", [])
            main_caption = context.user_data.get(
                "main_caption",
                f'🎬 <b>Results for "{current_title}"</b>\n\n🔗 Select a source below:',
            )
            show_episode_guide = (
                current_category == CATEGORY_TV
                and bool(context.user_data.get("current_is_tv"))
                and bool(context.user_data.get("current_seasons", []))
            )

            if not all_results:
                main_caption = (
                    f"{main_caption}\n\n❌ No results found for "
                    f"{category_display_label(current_category)}."
                )

            keyboard = build_main_keyboard(
                all_results,
                current_title,
                page=get_category_page(context, current_category),
                show_episode_guide=show_episode_guide,
                active_category=current_category,
            )
            await safe_telegram_call(
                query.edit_message_text(
                    main_caption,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    disable_web_page_preview=True,
                ),
                "back to main text",
            )

        # --- Episode guide ---
        elif data == "guide:episodes":
            current_title = context.user_data.get("current_title", "")
            seasons = context.user_data.get("current_seasons", [])
            if not seasons:
                await safe_telegram_call(
                    query.answer(text="No season guide available."),
                    "episode guide empty",
                )
                return
            await safe_telegram_call(query.answer(), "episode guide callback")
            await safe_telegram_call(
                query.edit_message_text(
                    text=format_episode_guide_text(current_title, seasons),
                    reply_markup=build_episode_guide_keyboard(seasons),
                ),
                "episode guide results",
            )

        # --- Season selection ---
        elif data.startswith("season:"):
            season_number = data.replace("season:", "").strip()
            await safe_telegram_call(
                query.answer(text=f"Selected Season {season_number}"),
                "season select callback",
            )

        # --- OTT search ---
        elif data.startswith("ott:"):
            await safe_telegram_call(query.answer(), "ott callback")
            title = data.replace("ott:", "").strip()
            if not title:
                await safe_telegram_call(
                    query.answer(text="Invalid title."),
                    "ott invalid title",
                )
                return

            normalized_title = normalize_title(title)
            ott_cache = context.user_data.setdefault("ott_results", {})
            results = ott_cache.get(normalized_title)

            if results is None:
                cache_key = make_cache_key(CACHE_PREFIX_OTT, title)
                cached = get_cached(cache_key) if cache_key else None
                if cached is not None:
                    results = cached
                else:
                    current_title_key = context.user_data.get("current_title_key")
                    tmdb_info = (
                        context.user_data.get("current_tmdb_info")
                        if current_title_key == normalized_title
                        else None
                    )
                    results = await asyncio.to_thread(search_ott, title, tmdb_info)
                    if cache_key:
                        save_cache(cache_key, results)
                ott_cache[normalized_title] = results

            if not results:
                results = {}

            await safe_telegram_call(
                query.edit_message_text(
                    text=format_ott_text(title, results),
                    reply_markup=build_ott_keyboard(results),
                ),
                "ott results",
            )
        else:
            logger.warning("Unknown callback data: %s", data)
            await safe_telegram_call(
                query.answer(text="Invalid action."),
                "unknown callback",
            )
    except Exception:
        logger.exception("Callback handling failed for %s", data)
        await safe_telegram_call(
            query.answer(text="❌ Something went wrong."),
            "callback error",
        )


# --------------------------------------------------
# Main function
# --------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("✅ Bot is running...")
    logger.info("BOT_TOKEN: %s", bool(BOT_TOKEN))
    logger.info("TMDB_API_KEY: %s", bool(TMDB_API_KEY))

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing!")
    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY is missing!")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("sites", sites_command))
    app.add_handler(CommandHandler("movie", movie_command))
    app.add_handler(CommandHandler("tv", tv_command))
    app.add_handler(CommandHandler("anime", anime_command))
    app.add_handler(CommandHandler("game", game_command))
    app.add_handler(CommandHandler("favorites", favorites_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_title))
    app.add_handler(CallbackQueryHandler(favorite_callback))

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
