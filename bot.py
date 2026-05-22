import json
import logging
import os
import re
import sqlite3
import time
from difflib import SequenceMatcher
from typing import Any, Awaitable
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

GAME_KEYWORDS = {
    "game",
    "games",
    "gaming",
    "pc",
    "steam",
    "xbox",
    "playstation",
    "ps4",
    "ps5",
    "switch",
    "nintendo",
    "apk",
    "repack",
}

ANIME_KEYWORDS = {
    "anime",
    "ova",
    "shonen",
    "seinen",
    "manga",
}

logger = logging.getLogger(__name__)

# Cache sites.json content and its modification time to avoid repeated file reads.
SITES_CACHE = None
SITES_CACHE_LAST_MTIME = None

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

# Change 1: Category-specific site lists
MOVIE_SITES = [
    "https://lookmovie2.to",
    "https://hurawatch.art",
    "https://ww8.123moviesfree.net",
    "https://new1.hdhub4u.limo",
    "https://desicinemas.to",
    "https://uhdmovies.pink",
    "https://moviesmod.farm",
    "https://ww25.soap2day.day",
    "https://themoviesflix.moe",
    "https://mayamovie.org",
    "https://putlocker-hd.xyz",
    "https://cataz.my",
    "https://guideflix.to",
    "https://filmslook.com",
    "https://solarmoviesz.com",
    "https://flixhqz.com",
    "https://movie4k.sx",
    "https://yomovies.courses",
    "https://cinemalux.skin",
    "https://todaypk.beauty",
    "https://vegamovies.diamonds"
]

TV_SITES = [
    "https://goku-tv.pics",
    "https://watchseries.bar",
    "https://newprimewire.cc",
    "https://hurawatch.cx",
    "https://hurawatch.art",
    "https://flixhqz.com",
    "https://ww25.soap2day.day",
    "https://lookmovie2.to",
    "https://ww19.myasiantv.es"
]

ANIME_SITES = [
    "https://animeflix.dad",
    "https://9anime.digital",
    "https://aniwaves.ru",
    "https://gogoanime.by",
    "https://anitaku.com.ro",
    "https://animeheaven.me",
    "https://animixplay.com.ro",
    "https://kickassanime.com.es",
    "https://animefreak.to",
    "https://animedao.in",
    "https://animesuge.cz"
]

GAME_SITES = [
    "https://fitgirl-repacks.site",
    "https://dodi-repacks.site",
    "https://steamunlocked.net",
    "https://oceanofgames.com",
    "https://igg-games.com",
    "https://gamesdrive.net",
    "https://pcgamestorrents.com",
    "https://repack-games.com",
    "https://crohasit.net",
    "https://apunkagames.net",
    "https://gload.to",
    "https://elamigos.site",
    "https://steamrip.com"
]
CATEGORY_SITES = {
    CATEGORY_MOVIE: MOVIE_SITES,
    CATEGORY_TV: TV_SITES,
    CATEGORY_ANIME: ANIME_SITES,
    CATEGORY_GAME: GAME_SITES,
}


# --------------------------------------------------
# Database helpers
# --------------------------------------------------
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            query TEXT PRIMARY KEY,
            data TEXT,
            created_at INTEGER
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
    cur.execute("""
        DELETE FROM history
        WHERE searched_at < datetime('now', '-4 days')
    """)
    conn.commit()
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


def make_cache_key(prefix: str, query: str) -> str | None:
    normalized = normalize_title(query)
    if not normalized:
        return None
    return f"{prefix}:{normalized}"


def delete_cache(cache_key):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM cache
        WHERE query = ?
    """, (cache_key,))
    conn.commit()
    conn.close()


def get_cached(cache_key):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT data, created_at
        FROM cache
        WHERE query = ?
    """, (cache_key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    data, created_at = row
    if time.time() - created_at > CACHE_DURATION:
        delete_cache(cache_key)
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        delete_cache(cache_key)
        return None


def save_cache(cache_key, data):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO cache (query, data, created_at)
        VALUES (?, ?, ?)
    """, (cache_key, json.dumps(data), int(time.time())))
    conn.commit()
    conn.close()


def get_cached_for_titles(prefix: str, titles: list[str]) -> tuple[dict | None, str | None]:
    """Return (cached_data, cache_key), or (None, None) when no cache hit exists."""
    for title in titles:
        if not title:
            continue
        cache_key = make_cache_key(prefix, title)
        if not cache_key:
            continue
        cached = get_cached(cache_key)
        if cached is not None:
            return cached, cache_key
    return None, None


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


def expand_alias_variants(normalized_title: str) -> set[str]:
    """Generate common alias variations from a normalized title string."""
    variants = set()
    if " and " in normalized_title:
        variants.add(normalized_title.replace(" and ", " & "))
    if " & " in normalized_title:
        variants.add(normalized_title.replace(" & ", " and "))
    if " vs " in normalized_title:
        variants.add(normalized_title.replace(" vs ", " versus "))
    if " versus " in normalized_title:
        variants.add(normalized_title.replace(" versus ", " vs "))
    if " part " in normalized_title:
        variants.add(normalized_title.replace(" part ", " pt "))
    if " pt " in normalized_title:
        variants.add(normalized_title.replace(" pt ", " part "))
    if normalized_title.startswith("the "):
        variants.add(normalized_title[4:])
    return variants


def build_match_targets(title: str, aliases=None) -> list[str]:
    """Build normalized match targets using the title and optional aliases."""
    targets = set()
    base = normalize_title(title)
    if base:
        targets.add(base)
        targets.update(expand_alias_variants(base))
    if aliases:
        for alias in aliases:
            normalized_alias = normalize_title(alias)
            if normalized_alias:
                targets.add(normalized_alias)
                targets.update(expand_alias_variants(normalized_alias))
    return [target for target in targets if target]


def build_query_variants(title: str, aliases=None) -> list[str]:
    """Generate a small set of query variants to improve search coverage."""
    variants = []
    seen = set()

    def add_variant(value: str):
        if not value:
            return
        lowered = value.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        variants.append(value)

    add_variant(title)
    if aliases:
        for alias in aliases:
            add_variant(alias)
    normalized = normalize_title(title)
    add_variant(normalized)
    for alias_variant in expand_alias_variants(normalized):
        add_variant(alias_variant)
    return variants[:MAX_QUERY_VARIANTS]


def category_display_label(category: str) -> str:
    meta = CATEGORY_META.get(category, CATEGORY_META[CATEGORY_MOVIE])
    return f"{meta['icon']} {meta['label']}"


def has_keyword(text: str, keywords: set[str]) -> bool:
    words = set(normalize_title(text).split())
    return any(keyword in words for keyword in keywords)


# Change 2: Replaced detect_content_category with detect_content_type (DDG-based)
def detect_content_type(query: str) -> str:
    try:
        with DDGS() as ddgs:
            results = list(
                ddgs.text(
                    f"{query} wikipedia",
                    max_results=5,
                )
            )

        text = " ".join(
            (
                r.get("title", "")
                + " "
                + r.get("body", "")
            )
            for r in results
        ).lower()

        if "video game" in text:
            return CATEGORY_GAME

        if "anime" in text:
            return CATEGORY_ANIME

        if "tv series" in text:
            return CATEGORY_TV

        if "television series" in text:
            return CATEGORY_TV

        if "season" in text:
            return CATEGORY_TV

    except Exception:
        logger.exception("Category detection failed")

    return CATEGORY_MOVIE


def build_category_query_variants(title: str, aliases, category: str) -> list[str]:
    """Add category-aware query hints while keeping existing variant limits."""
    base_variants = build_query_variants(title, aliases)
    if not base_variants:
        return []
    category_hints = {
        CATEGORY_MOVIE: ["movie", "film"],
        CATEGORY_TV: ["tv series", "season"],
        CATEGORY_ANIME: ["anime", "episode"],
        CATEGORY_GAME: ["game", "pc game"],
    }
    hint_values = category_hints.get(category, [])
    variants = list(base_variants)
    for hint in hint_values:
        if len(variants) >= MAX_QUERY_VARIANTS:
            break
        hinted = f"{title} {hint}".strip()
        if hinted.lower() not in {v.lower() for v in variants}:
            variants.append(hinted)
    return variants[:MAX_QUERY_VARIANTS]


def get_category_cache_prefix(category: str) -> str:
    return f"{CACHE_PREFIX_MOVIE}:{category}"


def choose_active_category(
    raw_title: str,
    tmdb_info: dict[str, Any] | None,
    forced_category: str | None,
    category_results: dict[str, dict[str, str]],
) -> str:
    if forced_category in CATEGORY_META:
        return forced_category
    preferred = detect_content_type(raw_title)
    if category_results.get(preferred):
        return preferred
    for category in SEARCH_CATEGORIES:
        if category_results.get(category):
            return category
    return preferred


def resolve_cached_category_results(cached_payload: Any) -> dict[str, str]:
    if is_empty_cache(cached_payload):
        return {}
    if isinstance(cached_payload, dict):
        return cached_payload
    return {}


# --------------------------------------------------
# Load sites from sites.json
# --------------------------------------------------
def load_sites():
    global SITES_CACHE, SITES_CACHE_LAST_MTIME
    try:
        mtime = os.path.getmtime("sites.json")
        if SITES_CACHE is not None and SITES_CACHE_LAST_MTIME == mtime:
            return SITES_CACHE
        with open("sites.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            SITES_CACHE = data
            SITES_CACHE_LAST_MTIME = mtime
            return data
        if isinstance(data, dict):
            SITES_CACHE = data.get("sites", [])
            SITES_CACHE_LAST_MTIME = mtime
            return SITES_CACHE
    except Exception:
        logger.exception("Failed to load sites.json from current directory")
    return []


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
def score_search_result(match_targets, result) -> float:
    """Score a search result against match targets (0.0-1.0)."""
    if not match_targets:
        return 0
    href = result.get("href") or result.get("url") or ""
    slug = urlparse(href).path if href else ""
    slug = re.sub(r"[-_/]+", " ", slug)
    candidate_parts = [
        result.get("title", ""),
        result.get("body", ""),
        slug,
    ]
    candidate_text = " ".join(part for part in candidate_parts if part)
    candidate_normalized = normalize_title(candidate_text)
    if len(candidate_normalized) > MAX_CANDIDATE_LENGTH:
        truncated = candidate_normalized[:MAX_CANDIDATE_LENGTH]
        candidate_normalized = truncated.rsplit(" ", 1)[0] or truncated
    best_score = 0
    for target in match_targets:
        if not target:
            continue
        if target in candidate_normalized:
            best_score = max(best_score, 0.85)
        ratio = SequenceMatcher(None, target, candidate_normalized).ratio()
        best_score = max(best_score, ratio)
    return best_score


def search_site(
    title: str,
    site_url: str,
    match_targets: list[str] | None = None,
    query_variants: list[str] | None = None,
) -> str | None:
    domain = urlparse(site_url).netloc.replace("www.", "")
    if not domain:
        return None
    match_targets = match_targets or build_match_targets(title)
    query_variants = query_variants or build_query_variants(title)
    best_url = None
    best_score = 0
    try:
        with DDGS() as ddgs:
            for query_variant in query_variants:
                query = f'{query_variant} site:{domain}'
                results = ddgs.text(query, max_results=MAX_SEARCH_RESULTS)
                for result in results:
                    href = result.get("href") or result.get("url")
                    if not href or domain not in href:
                        continue
                    score = score_search_result(match_targets, result)
                    if score > best_score:
                        best_score = score
                        best_url = href
                if best_score >= GOOD_MATCH_SCORE:
                    break
    except Exception:
        logger.exception("Search failed for %s on %s", title, domain)
        return None
    if best_score < MIN_MATCH_SCORE:
        return None
    return best_url


def search_movie(
    title: str,
    aliases: list[str] | None = None,
    query_variants: list[str] | None = None,
):
    sites = load_sites()
    if not sites:
        return {}
    match_targets = build_match_targets(title, aliases)
    query_variants = query_variants or build_query_variants(title, aliases)
    found = {}
    for site in sites:
        result = search_site(title, site, match_targets, query_variants)
        if result:
            domain = urlparse(site).netloc.replace("www.", "")
            found[domain] = result
    return found


# Change 3: New fast category-aware search function
def search_category_sites(title: str, category: str) -> dict[str, str]:
    sites = CATEGORY_SITES.get(category, [])
    found = {}

    try:
        with DDGS() as ddgs:
            for site in sites:
                domain = urlparse(site).netloc

                results = ddgs.text(
                    f'"{title}" site:{domain}',
                    max_results=5,
                )

                for item in results:
                    href = item.get("href")

                    if href:
                        found[domain] = href
                        break

    except Exception:
        logger.exception("Search failed")

    return found


def search_ott(title: str, aliases: list[str] | None = None):
    countries = ["DE", "US", "GB", "IN"]
    match_targets = build_match_targets(title, aliases)
    query_variants = build_query_variants(title, aliases)
    found = {}
    for country in countries:
        found[country] = []
        for name, site in OTT_SITES[country].items():
            result = search_site(title, site, match_targets, query_variants)
            found[country].append(build_ott_provider_result(country, name, result))
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


def get_aliases_for_title(
    context: ContextTypes.DEFAULT_TYPE,
    normalized_title: str,
) -> list[str]:
    """Fetch stored aliases for a normalized title."""
    return context.user_data.get("aliases", {}).get(normalized_title, [])


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
    url: str | None,
) -> dict[str, Any]:
    """Build a provider result entry with display name and availability metadata."""
    return {
        "name": provider_name,
        "display_name": shorten_ott_provider_name(provider_name, country_code),
        "available": bool(url),
        "status": "Available" if url else "Unavailable",
        "url": url,
    }


def format_ott_text(title: str, ott_results: dict) -> str:
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


def build_ott_keyboard(ott_results: dict) -> InlineKeyboardMarkup:
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


def has_ott_availability(ott_results: dict) -> bool:
    """Return True when at least one OTT provider is available."""
    return any(
        provider["available"]
        for providers in ott_results.values()
        for provider in providers
    )


def format_season_label(season: dict[str, Any]) -> str:
    """Build a compact UI label for one season."""
    season_number = season.get("season_number")
    if season_number is None:
        return "📺 Season"
    episode_count = season.get("episode_count")
    if episode_count:
        return f"📺 Season {season_number} ({episode_count} eps)"
    return f"📺 Season {season_number}"


def format_episode_guide_text(title: str, seasons: list[dict[str, Any]]) -> str:
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


def build_episode_guide_keyboard(seasons: list[dict[str, Any]]) -> InlineKeyboardMarkup:
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
    sites = load_sites()
    if not sites:
        await update.message.reply_text("❌ No sites configured.")
        return
    domains = [urlparse(site).netloc.replace("www.", "") for site in sites]
    await update.message.reply_text(
        "🌐 Configured sites:\n\n" + "\n".join(f"• {d}" for d in domains)
    )


# --------------------------------------------------
# Search commands
# --------------------------------------------------
def get_command_query(context: ContextTypes.DEFAULT_TYPE) -> str:
    return " ".join(context.args).strip() if context.args else ""


def build_results_caption(
    raw_title: str,
    info: dict[str, Any] | None,
    category: str,
) -> str:
    category_label = category_display_label(category)
    if info:
        caption = f"🎬 <b>{info['title']}"
        if info["year"]:
            caption += f" ({info['year']})"
        caption += "</b>\n"
        caption += f"🧭 Category: {category_label}\n"
        if info["rating"]:
            caption += f"⭐ {info['rating']:.1f}/10\n"
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
    forced_category: str | None = None,
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
        info = get_tmdb_info(parsed["query"])
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
                detected_category = detect_content_type(raw_title)
        else:
            detected_category = detect_content_type(raw_title)

        results = search_category_sites(title, detected_category)

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

        # Store pagination state
        all_results = list(results.items())
        normalized_title = normalize_title(title)
        context.user_data["results"] = all_results
        # Change 5: simplified category_results storage
        context.user_data["category_results"] = {
            detected_category: results
        }
        context.user_data["category_pages"] = {
            category: 0 for category in SEARCH_CATEGORIES
        }
        # Change 6: no cache keys to track
        context.user_data["category_cache_keys"] = {}
        context.user_data["current_title"] = title
        context.user_data["raw_title"] = raw_title
        context.user_data["current_tmdb_info"] = info
        context.user_data["current_title_key"] = normalized_title
        # Change 6: no current cache key
        context.user_data["current_cache_key"] = None
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
) -> dict[str, str]:
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
) -> dict[str, str]:
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
                    new_results = search_category_sites(title, category)
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
                    aliases = get_aliases_for_title(context, normalized_title)
                    results = search_ott(title, aliases=aliases)
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
