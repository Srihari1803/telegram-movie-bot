import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Awaitable, Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from ddgs import DDGS
from supabase import create_client
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
from telegram.error import BadRequest, Conflict

# ─── Environment ────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

SUPABASE_CLIENT: Optional[Any] = None
ADMIN_IDS: Set[int] = set()
START_TIME = time.time()

# ─── File paths ─────────────────────────────────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(_BASE_DIR, "bot.db")
SITES_FILE = os.path.join(_BASE_DIR, "sites.json")

# ─── Tuning constants ────────────────────────────────────────────────────────
DB_TIMEOUT = 30
DB_BUSY_TIMEOUT_MS = 5000

CACHE_DURATION_SUCCESS = 604800  # 7 days
CACHE_DURATION_EMPTY = 3600      # 1 hour
CACHE_CLEANUP_INTERVAL = 86400   # 24 hour

# Sentinel for the fallback cleanup thread (used when JobQueue is unavailable)
_cleanup_thread: Optional[threading.Thread] = None
RESULTS_PER_PAGE = 5
MAX_SEARCH_RESULTS = 8
MAX_OVERVIEW_LENGTH = 400
MAX_TITLE_LENGTH = 120
TOKEN_CACHE_LIMIT = 25

HTTP_TIMEOUT: Tuple[int, int] = (5, 15)   # (connect, read)
HTTP_POOL_SIZE = 20

DDGS_MAX_WORKERS = 5  # keep low to reduce DDGS rate limiting
DDGS_RETRY_COUNT = 3
DDGS_BACKOFF_BASE = 1.0
DDGS_MAX_BACKOFF = 8.0
DDGS_TEST_QUERY = "example.com"  # stable domain for DDGS healthcheck

CACHE_PREFIX_MOVIE = "movie"
CACHE_PREFIX_OTT = "ott"
CACHE_PREFIX_TMDB = "tmdb"
CACHE_PREFIX_DETECT = "detect"
CACHE_EMPTY_MARKER: Dict[str, bool] = {"_empty": True}
EMPTY_CACHE_PAYLOAD = json.dumps(CACHE_EMPTY_MARKER)  # keep in sync with marker

SUPPORTED_TMDB_MEDIA_TYPES: Set[str] = {"movie", "tv"}
MAX_TMDB_RATING = 10

# ─── Categories ─────────────────────────────────────────────────────────────
CATEGORY_MOVIE = "movie"
CATEGORY_TV = "tv"
CATEGORY_ANIME = "anime"
CATEGORY_GAME = "game"

CATEGORY_META: Dict[str, Dict[str, str]] = {
    CATEGORY_MOVIE: {"label": "Movie", "icon": "🎬"},
    CATEGORY_TV:    {"label": "TV",    "icon": "📺"},
    CATEGORY_ANIME: {"label": "Anime", "icon": "🍿"},
    CATEGORY_GAME:  {"label": "Game",  "icon": "🎮"},
}

SEARCH_CATEGORIES: List[str] = [
    CATEGORY_MOVIE,
    CATEGORY_TV,
    CATEGORY_ANIME,
    CATEGORY_GAME,
]

# ─── Logging ────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

SITES_CACHE: Optional[Dict[str, List[str]]] = None
SITES_CACHE_LOCK = threading.Lock()
_DDGS_LOCAL = threading.local()

def get_supabase() -> Any:
    if SUPABASE_CLIENT is None:
        raise RuntimeError("Supabase client is not initialized.")
    return SUPABASE_CLIENT


def load_admin_ids() -> Set[int]:
    raw = os.getenv("ADMIN_IDS", "")
    ids: Set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            logger.warning("Invalid ADMIN_IDS entry: %s", part)
    return ids


def get_ddgs_client() -> DDGS:
    client = getattr(_DDGS_LOCAL, "client", None)
    if client is None:
        client = DDGS()
        _DDGS_LOCAL.client = client
    return client


def reset_ddgs_client() -> None:
    client = getattr(_DDGS_LOCAL, "client", None)
    if client is None:
        return
    close = getattr(client, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            logger.debug("DDGS client close failed", exc_info=True)
    _DDGS_LOCAL.client = None

# ─── HTTP session (shared, with connection pooling) ─────────────────────────
_http_adapter = HTTPAdapter(
    pool_connections=HTTP_POOL_SIZE,
    pool_maxsize=HTTP_POOL_SIZE,
)
HTTP_SESSION = requests.Session()
HTTP_SESSION.mount("http://", _http_adapter)
HTTP_SESSION.mount("https://", _http_adapter)
HTTP_SESSION.headers.update({"User-Agent": "MovieSearchBot/1.0"})

# ─── Static UI data ─────────────────────────────────────────────────────────
BUTTON_NAMES: Dict[str, str] = {
    "lookmovie2.to":        "🎬 LookMovie",
    "goku-tv.pics":         "🍿 GokuTV",
    "hurawatch.art":        "📺 HuraWatch",
    "www8.123moviesfree.net": "🎥 123Movies",
    "ww19.myasiantv.es":    "📺 MyAsianTV",
    "desicinemas.to":       "🇮🇳 DesiCinema",
    "uhdmovies.pink":       "💎 UHD Movies",
    "moviesmod.farm":       "🎞 MoviesMod",
}

OTT_SITES: Dict[str, Dict[str, str]] = {
    "DE": {
        "Netflix Germany":      "https://www.netflix.com/de/",
        "Disney+ Germany":      "https://www.disneyplus.com/de-de",
        "Prime Video Germany":  "https://www.primevideo.com",
    },
    "US": {
        "Netflix USA":  "https://www.netflix.com",
        "Disney+ USA":  "https://www.disneyplus.com",
        "Max USA":      "https://www.max.com",
    },
    "GB": {
        "Netflix UK":   "https://www.netflix.com/gb/",
        "Disney+ UK":   "https://www.disneyplus.com/en-gb",
    },
    "IN": {
        "Netflix India":        "https://www.netflix.com/in/",
        "Disney+ Hotstar":      "https://www.hotstar.com/in",
        "Prime Video India":    "https://www.primevideo.com",
    },
    "CA": {
        "Netflix Canada": "https://www.netflix.com/ca/",
        "Disney+ Canada": "https://www.disneyplus.com/en-ca",
        "Prime Video Canada": "https://www.primevideo.com",
    },

    "AU": {
        "Netflix Australia": "https://www.netflix.com/au/",
        "Disney+ Australia": "https://www.disneyplus.com/en-au",
        "Prime Video Australia": "https://www.primevideo.com",
    },

    "FR": {
        "Netflix France": "https://www.netflix.com/fr/",
        "Disney+ France": "https://www.disneyplus.com/fr-fr",
        "Prime Video France": "https://www.primevideo.com",
    },

    "JP": {
        "Netflix Japan": "https://www.netflix.com/jp/",
        "Disney+ Japan": "https://www.disneyplus.com/ja-jp",
        "Prime Video Japan": "https://www.primevideo.com",
    }
}

COUNTRY_INFO: Dict[str, Dict[str, str]] = {
    "DE": {"flag": "🇩🇪", "name": "Germany"},
    "US": {"flag": "🇺🇸", "name": "USA"},
    "GB": {"flag": "🇬🇧", "name": "UK"},
    "IN": {"flag": "🇮🇳", "name": "India"},
    "AU": {"flag": "🇮🇳", "name": "Australia"},
    "FR": {"flag": "🇮🇳", "name": "France"},
    "JP": {"flag": "🇮🇳", "name": "Japan"},
}

PROVIDER_ALIASES: Dict[str, Set[str]] = {
    "primevideo": {"amazonprimevideo", "primevideo"},
    "disneyplus": {"disneyplus", "disneyplushotstar", "hotstar", "jiohotstar"},
    "max":        {"hbomax", "max"},
}


# ════════════════════════════════════════════════════════════════════════════
# Database
# ════════════════════════════════════════════════════════════════════════════

def _configure_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS}")


@contextmanager
def db_connection(commit: bool = False) -> Iterator[sqlite3.Connection]:
    """
    Yield a configured SQLite connection.
    check_same_thread=False is safe here because each call creates its own
    connection object used only within this context manager's scope.
    """
    conn = sqlite3.connect(DB_FILE, timeout=DB_TIMEOUT, check_same_thread=False)
    _configure_sqlite(conn)
    try:
        yield conn
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with db_connection(commit=True) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                title       TEXT    NOT NULL,
                searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title   TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                query      TEXT    PRIMARY KEY,
                data       TEXT    NOT NULL,
                created_at INTEGER NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS history_user_idx ON history (user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS cache_created_idx ON cache (created_at)"
        )


def save_history(user_id: int, title: str) -> None:
    get_supabase().table("history").insert({
        "user_id": user_id,
        "title": title
    }).execute()


def get_history(user_id: int) -> List[Tuple[str, str]]:
    response = (
        get_supabase().table("history")
        .select("title,searched_at")
        .eq("user_id", user_id)
        .order("searched_at", desc=True)
        .limit(10)
        .execute()
    )

    return [
        (row["title"], row["searched_at"])
        for row in response.data
    ]


def add_favorite(user_id: int, title: str) -> None:
    get_supabase().table("favorites").insert({
        "user_id": user_id,
        "title": title
    }).execute()


def remove_favorite(user_id: int, title: str) -> None:
    (
        get_supabase().table("favorites")
        .delete()
        .eq("user_id", user_id)
        .eq("title", title)
        .execute()
    )


def get_favorites(user_id: int) -> List[Tuple[str]]:
    response = (
        get_supabase().table("favorites")
        .select("title")
        .eq("user_id", user_id)
        .execute()
    )

    return [(row["title"],) for row in response.data]

# ─── Cache helpers ────────────────────────────────────────────────────────────

def make_cache_key(prefix: str, query: str) -> Optional[str]:
    normalized = normalize_title(query)
    if not normalized:
        return None
    return f"{prefix}:{normalized}"


def get_cached(cache_key: str) -> Optional[Any]:
    response = (
        get_supabase().table("cache")
        .select("data,created_at")
        .eq("query", cache_key)
        .execute()
    )

    if not response.data:
        logger.info("Cache MISS: %s", cache_key)
        return None

    row = response.data[0]
    try:
        payload = json.loads(row["data"])
    except Exception:
        logger.exception("Cache decode failed for key=%s", cache_key)
        logger.info("Cache MISS: %s", cache_key)
        return None

    created_at = int(row["created_at"])
    ttl = CACHE_DURATION_EMPTY if is_empty_cache(payload) else CACHE_DURATION_SUCCESS
    logger.debug("Cache TTL for %s: %ds", cache_key, ttl)
    if time.time() - created_at > ttl:
        (
            get_supabase().table("cache")
            .delete()
            .eq("query", cache_key)
            .execute()
        )
        logger.info("Cache MISS: %s", cache_key)
        return None

    logger.info("Cache HIT: %s", cache_key)
    return payload


def save_cache(cache_key: str, data: Any) -> None:
    get_supabase().table("cache").upsert({
        "query": cache_key,
        "data": json.dumps(data),
        "created_at": int(time.time())
    }).execute()


def cache_empty_result(cache_key: str) -> None:
    save_cache(cache_key, CACHE_EMPTY_MARKER)


def cleanup_cache() -> int:
    removed_total = 0
    cutoff_success = int(time.time()) - CACHE_DURATION_SUCCESS

    response = (
        get_supabase().table("cache")
        .delete()
        .lt("created_at", cutoff_success)
        .execute()
    )
    removed_total += len(response.data) if response.data else 0

    cutoff_empty = int(time.time()) - CACHE_DURATION_EMPTY
    response = (
        get_supabase().table("cache")
        .delete()
        .eq("data", EMPTY_CACHE_PAYLOAD)
        .lt("created_at", cutoff_empty)
        .execute()
    )
    removed_total += len(response.data) if response.data else 0

    return removed_total


def is_empty_cache(cached: Any) -> bool:
    return isinstance(cached, dict) and bool(cached.get("_empty"))


def resolve_cached_category_results(cached_payload: Any) -> Dict[str, str]:
    if is_empty_cache(cached_payload):
        return {}
    if isinstance(cached_payload, dict):
        return cached_payload
    return {}


# ════════════════════════════════════════════════════════════════════════════
# Token / user-data helpers
# ════════════════════════════════════════════════════════════════════════════

def build_title_token(title: str) -> Optional[str]:
    normalized = normalize_title(title)
    if not normalized:
        return None
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def store_token(
    context: ContextTypes.DEFAULT_TYPE,
    bucket: str,
    token: str,
    value: str,
    max_entries: int = TOKEN_CACHE_LIMIT,
) -> Optional[str]:
    if not token:
        return None
    store = context.user_data.get(bucket)
    if not isinstance(store, OrderedDict):
        store = OrderedDict(store or {})
        context.user_data[bucket] = store
    store.move_to_end(token) if token in store else None
    store[token] = value
    while len(store) > max_entries:
        store.popitem(last=False)
    return token


def get_token_value(
    context: ContextTypes.DEFAULT_TYPE,
    bucket: str,
    token: str,
) -> Optional[str]:
    store = context.user_data.get(bucket)
    if isinstance(store, (dict, OrderedDict)):
        return store.get(token)
    return None


def reset_search_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        "results", "category_results", "category_pages", "category_cache_keys",
        "current_title", "raw_title", "current_tmdb_info", "current_title_key",
        "current_category", "current_year", "current_title_token", "main_caption",
        "current_is_tv", "current_seasons", "ott_results",
    ):
        context.user_data.pop(key, None)


# ════════════════════════════════════════════════════════════════════════════
# Title / URL utilities
# ════════════════════════════════════════════════════════════════════════════

def normalize_title(title: str) -> str:
    if not title:
        return ""
    text = title.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"['']", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_title_tokens(title: str) -> List[str]:
    normalized = normalize_title(title)
    if not normalized:
        return []
    stop_words = {"the", "a", "an"}
    return [token for token in normalized.split() if token and token not in stop_words]


def fuzzy_title_match(reference: str, candidate: str) -> bool:
    ref_tokens = normalize_title_tokens(reference)
    if not ref_tokens:
        return False
    return fuzzy_title_match_with_tokens(ref_tokens, set(ref_tokens), candidate)


def fuzzy_title_match_with_tokens(
    ref_tokens: List[str],
    ref_set: Set[str],
    candidate: str,
) -> bool:
    cand_tokens = normalize_title_tokens(candidate)
    if not ref_tokens or not cand_tokens:
        return False
    cand_set = set(cand_tokens)
    if ref_tokens == cand_tokens:
        return True
    if ref_set.issubset(cand_set) or cand_set.issubset(ref_set):
        return True
    overlap = len(ref_set.intersection(cand_set))
    ratio = overlap / max(len(ref_set), len(cand_set))
    return ratio >= 0.8


def normalize_site_domain(site: str) -> str:
    if not site:
        return ""
    parsed = urlparse(site)
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "")


def is_valid_http_url(url: str) -> bool:
    if not isinstance(url, str):
        return False
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def is_valid_title(title: str) -> bool:
    if not isinstance(title, str):
        return False
    text = title.strip()
    return bool(text) and len(text) <= MAX_TITLE_LENGTH


def parse_advanced_query(query: str) -> Dict[str, Optional[str]]:
    season_episode = re.search(r"(S\d+E\d+)", query, re.IGNORECASE)
    season = re.search(r"Season\s+(\d+)", query, re.IGNORECASE)
    year = re.search(r"(19\d{2}|20\d{2})", query)
    return {
        "query": query,
        "season_episode": season_episode.group(1) if season_episode else None,
        "season": season.group(1) if season else None,
        "year": year.group(1) if year else None,
    }


def category_display_label(category: str) -> str:
    meta = CATEGORY_META.get(category, CATEGORY_META[CATEGORY_MOVIE])
    return f"{meta['icon']} {meta['label']}"


def get_category_cache_prefix(category: str) -> str:
    return f"{CACHE_PREFIX_MOVIE}:{category}"


def empty_sites_cache() -> Dict[str, List[str]]:
    return {
        CATEGORY_MOVIE: [],
        CATEGORY_TV: [],
        CATEGORY_ANIME: [],
        CATEGORY_GAME: [],
    }


# ════════════════════════════════════════════════════════════════════════════
# Sites loading
# ════════════════════════════════════════════════════════════════════════════

def load_category_sites(refresh: bool = False) -> Dict[str, List[str]]:
    """
    Load sites.json from the same directory as bot.py.
    Returns a dict keyed by category; each value is a deduplicated list of URLs.
    Logs a clear error if the file is missing or malformed.
    """
    global SITES_CACHE
    with SITES_CACHE_LOCK:
        if SITES_CACHE is not None and not refresh:
            return SITES_CACHE
        try:
            with open(SITES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Movie sites loaded: {len(data.get('movie', []))}")
            logger.info(f"TV sites loaded: {len(data.get('tv', []))}")
            logger.info(f"Anime sites loaded: {len(data.get('anime', []))}")
            logger.info(f"Game sites loaded: {len(data.get('game', []))}")
        except FileNotFoundError:
            logger.error(
                "sites.json not found at %s. "
                "Create it next to bot.py. Working dir: %s | Files: %s",
                SITES_FILE,
                os.getcwd(),
                os.listdir(os.getcwd()),
            )
            SITES_CACHE = empty_sites_cache()
            return SITES_CACHE
        except json.JSONDecodeError:
            logger.error("sites.json at %s contains invalid JSON.", SITES_FILE)
            SITES_CACHE = empty_sites_cache()
            return SITES_CACHE
        except Exception:
            logger.exception("Failed to load sites.json from %s", SITES_FILE)
            SITES_CACHE = empty_sites_cache()
            return SITES_CACHE

        if not isinstance(data, dict):
            logger.error("sites.json must be a JSON object keyed by category.")
            SITES_CACHE = empty_sites_cache()
            return SITES_CACHE

    def normalize_sites(entries: Any) -> List[str]:
        if not isinstance(entries, list):
            return []
        seen: Set[str] = set()
        result = []
        for site in entries:
            if not isinstance(site, str) or not site.strip():
                continue
            domain = normalize_site_domain(site)
            if domain and domain not in seen:
                seen.add(domain)
                result.append(site)
        return result

        loaded = {
            CATEGORY_MOVIE: normalize_sites(data.get(CATEGORY_MOVIE, [])),
            CATEGORY_TV:    normalize_sites(data.get(CATEGORY_TV,    [])),
            CATEGORY_ANIME: normalize_sites(data.get(CATEGORY_ANIME, [])),
            CATEGORY_GAME:  normalize_sites(data.get(CATEGORY_GAME,  [])),
        }
        total = sum(len(v) for v in loaded.values())
        logger.info(
            "Loaded sites.json: %d categories, %d sites total",
            len(loaded),
            total,
        )
        for category, sites in loaded.items():
            if not sites:
                logger.warning("No sites configured for category: %s", category)
        SITES_CACHE = loaded
        return loaded


# ════════════════════════════════════════════════════════════════════════════
# Category detection
# ════════════════════════════════════════════════════════════════════════════

def fetch_ddgs_results(ddgs: DDGS, query: str, max_results: int) -> List[Dict[str, Any]]:
    """Materialise DDG results into a list with retries; return [] on failure."""
    for attempt in range(1, DDGS_RETRY_COUNT + 1):
        try:
            raw = ddgs.text(query, max_results=max_results)
            return list(raw or [])
        except (requests.Timeout, TimeoutError):
            logger.warning(
                "DDG timeout (attempt %d/%d) for query: %s",
                attempt,
                DDGS_RETRY_COUNT,
                query,
            )
        except Exception:
            logger.exception(
                "DDG search failed (attempt %d/%d) for query: %s",
                attempt,
                DDGS_RETRY_COUNT,
                query,
            )
        if attempt < DDGS_RETRY_COUNT:
            backoff = min(
                DDGS_BACKOFF_BASE * (2 ** (attempt - 1)),
                DDGS_MAX_BACKOFF,
            )
            logger.info("Retrying DDG search in %.1fs", backoff)
            time.sleep(backoff)
    reset_ddgs_client()
    return []


def detect_content_type(query: str) -> str:
    """
    Run a Wikipedia-biased DDG search to determine whether 'query' is a
    TV show, anime, game, or movie. Defaults to CATEGORY_MOVIE on failure.
    """
    try:
        ddgs = get_ddgs_client()
        ddg_query = f"{query} wikipedia"
        logger.info("Detecting category via DDG: %s", ddg_query)
        results = fetch_ddgs_results(ddgs, ddg_query, 5)
        logger.info("DDG results count: %d", len(results))
        text = " ".join(
            r.get("title", "") + " " + r.get("body", "")
            for r in results
        ).lower()

        # TV / anime / game checks ordered from most specific to least
        if "tv series" in text or "television series" in text:
            return CATEGORY_TV
        if "anime" in text:
            return CATEGORY_ANIME
        if "video game" in text:
            return CATEGORY_GAME
        if "season" in text:
            return CATEGORY_TV
    except Exception:
        logger.exception("Category detection failed for: %s", query)
    return CATEGORY_MOVIE


async def detect_category_with_cache(raw_title: str) -> str:
    """Return a cached detection result or run detection and cache it."""
    detect_key = make_cache_key(CACHE_PREFIX_DETECT, raw_title)
    if detect_key:
        cached = await asyncio.to_thread(get_cached, detect_key)
        if isinstance(cached, str) and cached in CATEGORY_META:
            return cached
    detected = await asyncio.to_thread(detect_content_type, raw_title)
    if detect_key:
        await asyncio.to_thread(save_cache, detect_key, detected)
    return detected


async def resolve_detected_category(
    raw_title: str,
    info: Optional[Dict[str, Any]],
    forced_category: Optional[str],
) -> str:
    """
    Priority: forced_category → TMDB media_type → DDG-based detection.
    Only calls DDG when TMDB gives no clear answer, saving a network round-trip.
    """
    if forced_category in CATEGORY_META:
        return forced_category
    if info:
        media_type = info.get("media_type")
        if media_type == "tv":
            return CATEGORY_TV
        if media_type == "movie":
            return CATEGORY_MOVIE
    return await detect_category_with_cache(raw_title)


# ════════════════════════════════════════════════════════════════════════════
# TMDB
# ════════════════════════════════════════════════════════════════════════════

def get_tmdb_info(title: str) -> Optional[Dict[str, Any]]:
    if not TMDB_API_KEY or not title:
        return None
    cache_key = make_cache_key(CACHE_PREFIX_TMDB, title)
    if not cache_key:
        return None
    cached = get_cached(cache_key)
    if cached is not None:
        return None if is_empty_cache(cached) else cached
    try:
        resp = HTTP_SESSION.get(
            "https://api.themoviedb.org/3/search/multi",
            params={"api_key": TMDB_API_KEY, "query": title, "include_adult": "true"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            cache_empty_result(cache_key)
            return None
        results = data.get("results")
        if not isinstance(results, list) or not results:
            cache_empty_result(cache_key)
            return None

        aliases = [
            item.get("title") or item.get("name")
            for item in results[:5]
            if item.get("title") or item.get("name")
        ]
        best = results[0]
        if not isinstance(best, dict):
            cache_empty_result(cache_key)
            return None

        media_type = best.get("media_type")
        seasons: List[Any] = []
        origin_country: List[str] = []
        tmdb_id = best.get("id")

        if media_type == "tv" and tmdb_id:
            try:
                tv_resp = HTTP_SESSION.get(
                    f"https://api.themoviedb.org/3/tv/{tmdb_id}",
                    params={"api_key": TMDB_API_KEY},
                    timeout=HTTP_TIMEOUT,
                )
                tv_resp.raise_for_status()
                tv_data = tv_resp.json()
                if isinstance(tv_data, dict):
                    seasons = tv_data.get("seasons") or []
                    origin_country = tv_data.get("origin_country") or []
            except Exception:
                logger.exception("TMDB TV detail lookup failed for %s", title)

        info: Dict[str, Any] = {
            "title":            best.get("title") or best.get("name") or title,
            "year":             (best.get("release_date") or best.get("first_air_date") or "")[:4],
            "rating":           best.get("vote_average"),
            "overview":         best.get("overview"),
            "tmdb_id":          tmdb_id,
            "poster":           (
                f"https://image.tmdb.org/t/p/w500{best['poster_path']}"
                if best.get("poster_path") else None
            ),
            "aliases":          aliases,
            "media_type":       media_type,
            "seasons":          seasons,
            "genre_ids":        best.get("genre_ids") or [],
            "original_language": best.get("original_language"),
            "origin_country":   origin_country,
        }
        save_cache(cache_key, info)
        return info
    except Exception:
        logger.exception("TMDB lookup failed for %s", title)
        return None


# ════════════════════════════════════════════════════════════════════════════
# OTT / watch providers
# ════════════════════════════════════════════════════════════════════════════

def normalize_provider_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def provider_is_available(normalized_short: str, normalized_names: Set[str]) -> bool:
    if not normalized_short or not normalized_names:
        return False
    if normalized_short in normalized_names:
        return True
    return bool(PROVIDER_ALIASES.get(normalized_short, set()).intersection(normalized_names))


def get_tmdb_watch_providers(tmdb_id: int, media_type: str) -> Dict[str, Any]:
    if not TMDB_API_KEY or not tmdb_id or media_type not in SUPPORTED_TMDB_MEDIA_TYPES:
        return {}
    try:
        resp = HTTP_SESSION.get(
            f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers",
            params={"api_key": TMDB_API_KEY},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("results", {}) if isinstance(payload, dict) else {}
    except Exception:
        logger.exception("TMDB watch provider lookup failed for id=%s", tmdb_id)
        return {}


def extract_provider_names(country_data: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for key in ("flatrate", "ads", "free", "rent", "buy"):
        for entry in country_data.get(key) or []:
            if isinstance(entry, dict) and entry.get("provider_name"):
                names.append(entry["provider_name"])
    return names


def shorten_ott_provider_name(provider_name: str, country_code: str) -> str:
    country_name = COUNTRY_INFO.get(country_code, {}).get("name", "")
    short = provider_name
    if country_name and short.endswith(f" {country_name}"):
        short = short[:-(len(country_name) + 1)]
    for alias in ("USA", "UK"):
        if short.endswith(f" {alias}"):
            short = short[:-(len(alias) + 1)]
    return short.strip()


def build_ott_provider_result(
    country_code: str,
    provider_name: str,
    url: Optional[str],
) -> Dict[str, Any]:
    return {
        "name":         provider_name,
        "display_name": shorten_ott_provider_name(provider_name, country_code),
        "available":    bool(url),
        "status":       "Available" if url else "Unavailable",
        "url":          url,
    }


def build_empty_ott_results() -> Dict[str, List[Dict[str, Any]]]:
    return {
        country_code: [
            build_ott_provider_result(country_code, name, None)
            for name in providers
        ]
        for country_code, providers in OTT_SITES.items()
    }


def search_ott(
    title: str,
    tmdb_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    info = tmdb_info if tmdb_info is not None else get_tmdb_info(title)
    if not info:
        return build_empty_ott_results()

    tmdb_id = info.get("tmdb_id")
    media_type = info.get("media_type")
    if not tmdb_id or media_type not in SUPPORTED_TMDB_MEDIA_TYPES:
        return build_empty_ott_results()

    tmdb_results = get_tmdb_watch_providers(tmdb_id, media_type)
    found: Dict[str, List[Dict[str, Any]]] = {}
    for country_code, providers in OTT_SITES.items():
        country_data = tmdb_results.get(country_code, {})
        provider_names = extract_provider_names(country_data)
        normalized_names = {normalize_provider_name(n) for n in provider_names if n}
        found[country_code] = []
        for provider_name, site in providers.items():
            short_name = shorten_ott_provider_name(provider_name, country_code)
            available = provider_is_available(normalize_provider_name(short_name), normalized_names)
            found[country_code].append(
                build_ott_provider_result(country_code, provider_name, site if available else None)
            )
    return found


# ════════════════════════════════════════════════════════════════════════════
# Site search (parallelised per-category)
# ════════════════════════════════════════════════════════════════════════════

def search_category_site(
    title: str,
    site: str,
    query_title: Optional[str] = None,
) -> Optional[Tuple[str, str]]:
    """
    Search a single site for *title*.
    Returns (domain, url) on success, None otherwise.
    Two query variants are tried; the first good hit wins.
    """
    site_domain = normalize_site_domain(site)
    if not site_domain:
        return None
    search_title = query_title or title
    ref_tokens = normalize_title_tokens(title)
    ref_set = set(ref_tokens)
    queries = [
        f'"{search_title}" site:{site_domain}',
        f"{search_title} site:{site_domain}",
    ]
    try:
        ddgs = get_ddgs_client()
        logger.info("DDGS domain search: %s", site_domain)
        for query in queries:
            logger.info("DDGS query: %s", query)
            results = fetch_ddgs_results(ddgs, query, MAX_SEARCH_RESULTS)
            logger.info("DDGS results count: %d", len(results))
            for item in results:
                href = item.get("href") or item.get("url")
                if not href or not is_valid_http_url(href):
                    continue
                href_domain = normalize_site_domain(href)
                if not href_domain:
                    continue
                if not (site_domain in href_domain or href_domain in site_domain):
                    continue
                result_text = f"{item.get('title', '')} {item.get('body', '')}".strip()
                if result_text and ref_tokens and not fuzzy_title_match_with_tokens(
                    ref_tokens,
                    ref_set,
                    result_text,
                ):
                    logger.debug(
                        "Skipping result due to title mismatch: %s",
                        result_text,
                    )
                    continue
                logger.info("DDGS selected URL: %s", href)
                return site_domain, href
    except Exception:
        logger.exception("search_category_site failed: title=%s site=%s", title, site_domain)
    return None


def build_search_fallback_queries(
    title: str,
    category: str,
    year: Optional[str] = None,
) -> List[str]:
    if not title:
        return []
    queries: List[str] = [title]
    if year and year not in title:
        queries.append(f"{title} {year}")
    suffix = {
        CATEGORY_MOVIE: "movie",
        CATEGORY_TV: "tv series",
        CATEGORY_ANIME: "anime",
        CATEGORY_GAME: "game",
    }.get(category)
    if suffix:
        queries.append(f"{title} {suffix}")
    seen: Set[str] = set()
    deduped = []
    for entry in queries:
        entry = entry.strip()
        if not entry or entry in seen:
            continue
        seen.add(entry)
        deduped.append(entry)
    return deduped


def _search_category_sites_for_query(
    title: str,
    query_title: str,
    sites: List[str],
) -> Dict[str, str]:
    found: Dict[str, str] = {}
    max_workers = min(DDGS_MAX_WORKERS, len(sites))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_site = {
            executor.submit(search_category_site, title, site, query_title): site
            for site in sites
        }
        for future in as_completed(future_to_site):
            site = future_to_site[future]
            try:
                result = future.result()
            except Exception:
                logger.exception(
                    "Worker failed for title=%s site=%s — skipping",
                    title,
                    normalize_site_domain(site),
                )
                continue
            if result:
                domain, href = result
                found[domain] = href
    return found


def search_category_sites(
    title: str,
    category: str,
    year: Optional[str] = None,
) -> Dict[str, str]:
    """
    Search all sites for *category* in parallel using a thread pool.
    A single failing worker does not abort the whole search.
    Returns a dict of {domain: url}.
    """
    if not title:
        return {}
    sites = load_category_sites().get(category, [])
    logger.info("Searching: %s", title)
    logger.info("Category: %s", category)
    logger.info("Sites count: %d", len(sites))
    if not sites:
        return {}

    fallback_queries = build_search_fallback_queries(title, category, year)
    found: Dict[str, str] = {}
    for index, query_title in enumerate(fallback_queries):
        if index == 0:
            logger.info("Initial search attempt: %s", query_title)
        else:
            logger.info("Fallback search attempt: %s", query_title)
        results = _search_category_sites_for_query(title, query_title, sites)
        logger.info("Matches found: %d", len(results))
        if results:
            found = results
            break

    logger.info(
        "Search completed | title=%s | category=%s | sites=%d | results=%d",
        title,
        category,
        len(sites),
        len(found),
    )
    return found


# ════════════════════════════════════════════════════════════════════════════
# OTT text / keyboard formatting
# ════════════════════════════════════════════════════════════════════════════

def format_ott_text(title: str, ott_results: Dict[str, List[Dict[str, Any]]]) -> str:
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
            icon = "✅" if provider["available"] else "❌"
            lines.append(f"  • {provider['display_name']} — {icon} {provider['status']}")
        lines.append("")
    return "\n".join(lines).strip()


def build_ott_keyboard(ott_results: Dict[str, List[Dict[str, Any]]]) -> InlineKeyboardMarkup:
    keyboard = []
    for country_code, providers in ott_results.items():
        country = COUNTRY_INFO.get(country_code, {"flag": "🏳️", "name": country_code})
        for provider in providers:
            if provider["available"]:
                keyboard.append([InlineKeyboardButton(
                    f"{country['flag']} {provider['display_name']}",
                    url=provider["url"],
                )])
    keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back:main")])
    return InlineKeyboardMarkup(keyboard)


def has_ott_availability(ott_results: Dict[str, List[Dict[str, Any]]]) -> bool:
    return any(
        p["available"]
        for providers in ott_results.values()
        for p in providers
    )


# ════════════════════════════════════════════════════════════════════════════
# Episode guide formatting
# ════════════════════════════════════════════════════════════════════════════

def format_season_label(season: Dict[str, Any]) -> str:
    number = season.get("season_number")
    if number is None:
        return "📺 Season"
    count = season.get("episode_count")
    return f"📺 Season {number} ({count} eps)" if count else f"📺 Season {number}"


def format_episode_guide_text(title: str, seasons: List[Dict[str, Any]]) -> str:
    lines = [f"📚 Episode Guide for {title}", ""]
    valid = [s for s in seasons if isinstance(s, dict) and s.get("season_number") is not None]
    if not valid:
        lines.append("❌ No season data available.")
        return "\n".join(lines)
    for season in valid:
        lines.append(format_season_label(season))
    return "\n".join(lines)


def build_episode_guide_keyboard(seasons: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for season in seasons:
        if not isinstance(season, dict):
            continue
        number = season.get("season_number")
        if number is None:
            continue
        row.append(InlineKeyboardButton(
            f"📺 Season {number}",
            callback_data=f"season:{number}",
        ))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="back:main")])
    return InlineKeyboardMarkup(keyboard)


# ════════════════════════════════════════════════════════════════════════════
# Main results keyboard
# ════════════════════════════════════════════════════════════════════════════

def build_main_keyboard(
    all_results: List[Tuple[str, str]],
    current_title: str,
    page: int = 0,
    show_episode_guide: bool = False,
    active_category: str = CATEGORY_MOVIE,
    title_token: Optional[str] = None,
) -> List[List[InlineKeyboardButton]]:
    def _category_label(cat: str) -> str:
        meta = CATEGORY_META[cat]
        text = f"{meta['icon']} {meta['label']}"
        return f"✅ {text}" if cat == active_category else text

    fav_cb = f"fav:{title_token}" if title_token else "fav"
    ott_cb = f"ott:{title_token}" if title_token else "ott"

    keyboard: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("⭐ Favorite", callback_data=fav_cb),
            InlineKeyboardButton("📺 OTT",      callback_data=ott_cb),
        ],
        [
            InlineKeyboardButton(_category_label(CATEGORY_MOVIE), callback_data=f"cat:{CATEGORY_MOVIE}"),
            InlineKeyboardButton(_category_label(CATEGORY_TV),    callback_data=f"cat:{CATEGORY_TV}"),
        ],
        [
            InlineKeyboardButton(_category_label(CATEGORY_ANIME), callback_data=f"cat:{CATEGORY_ANIME}"),
            InlineKeyboardButton(_category_label(CATEGORY_GAME),  callback_data=f"cat:{CATEGORY_GAME}"),
        ],
    ]
    if show_episode_guide:
        keyboard.append([
            InlineKeyboardButton("📚 Episode Guide", callback_data="guide:episodes")
        ])

    start = page * RESULTS_PER_PAGE
    end   = start + RESULTS_PER_PAGE
    for domain, url in all_results[start:end]:
        keyboard.append([InlineKeyboardButton(BUTTON_NAMES.get(domain, domain), url=url)])

    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Previous", callback_data="prev"))
    if end < len(all_results):
        nav.append(InlineKeyboardButton("Next ➡", callback_data="next"))
    if nav:
        keyboard.append(nav)
    return keyboard


# ════════════════════════════════════════════════════════════════════════════
# Telegram safety wrapper
# ════════════════════════════════════════════════════════════════════════════

async def safe_telegram_call(action: Awaitable[Any], description: str) -> Any:
    try:
        return await action
    except BadRequest as exc:
        logger.warning("Telegram BadRequest [%s]: %s", description, exc)
    except Exception:
        logger.exception("Telegram error [%s]", description)
    return None


def is_admin_user(user_id: int) -> bool:
    return bool(ADMIN_IDS) and user_id in ADMIN_IDS


def format_uptime(seconds: float) -> str:
    remaining = int(seconds)
    days, remaining = divmod(remaining, 86400)
    hours, remaining = divmod(remaining, 3600)
    minutes, secs = divmod(remaining, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or parts:
        parts.append(f"{hours}h")
    if minutes or parts:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def get_cache_entry_count() -> int:
    try:
        response = get_supabase().table("cache").select("query", count="exact").execute()
        count = getattr(response, "count", None)
        if count is not None:
            return int(count)
        return len(response.data) if response.data else 0
    except Exception:
        logger.exception("Failed to fetch cache size")
        return -1


def check_supabase_connection() -> str:
    try:
        get_supabase().table("cache").select("query").limit(1).execute()
        return "OK"
    except Exception as exc:
        logger.exception("Supabase connection check failed")
        return f"Error: {exc}"


def run_ddgs_healthcheck() -> str:
    try:
        ddgs = get_ddgs_client()
        results = fetch_ddgs_results(ddgs, DDGS_TEST_QUERY, 1)
        if results:
            return f"OK ({len(results)} results)"
        return "Warning: No results returned"
    except Exception as exc:
        logger.exception("DDGS healthcheck failed")
        return f"Error: {exc}"


# ════════════════════════════════════════════════════════════════════════════
# Caption builder
# ════════════════════════════════════════════════════════════════════════════

def build_results_caption(
    raw_title: str,
    info: Optional[Dict[str, Any]],
    category: str,
) -> str:
    label = category_display_label(category)
    if info:
        display = info.get("title") or raw_title
        caption = f"🎬 <b>{display}"
        year = info.get("year")
        if year:
            caption += f" ({year})"
        caption += "</b>\n"
        caption += f"🧭 Category: {label}\n"
        rating = info.get("rating")
        if isinstance(rating, (int, float)) and 0 <= rating <= MAX_TMDB_RATING:
            caption += f"⭐ {rating:.1f}/{MAX_TMDB_RATING}\n"
        overview = info.get("overview") or ""
        if overview:
            if len(overview) > MAX_OVERVIEW_LENGTH:
                overview = overview[:MAX_OVERVIEW_LENGTH] + "..."
            caption += f"\n{overview}\n"
    else:
        caption = f'🎬 <b>Results for "{raw_title}"</b>\n🧭 Category: {label}\n'
    caption += "\n🔗 Select a source below:"
    return caption


# ════════════════════════════════════════════════════════════════════════════
# Pagination / category state helpers
# ════════════════════════════════════════════════════════════════════════════

def get_category_page(context: ContextTypes.DEFAULT_TYPE, category: str) -> int:
    return context.user_data.setdefault("category_pages", {}).get(category, 0)


def set_category_page(context: ContextTypes.DEFAULT_TYPE, category: str, page: int) -> None:
    context.user_data.setdefault("category_pages", {})[category] = page
    context.user_data["page"] = page


def get_results_for_category(
    context: ContextTypes.DEFAULT_TYPE,
    category: str,
) -> Dict[str, str]:
    return context.user_data.setdefault("category_results", {}).get(category, {})


def set_active_category(context: ContextTypes.DEFAULT_TYPE, category: str) -> None:
    results = get_results_for_category(context, category)
    all_results = list(results.items())
    page = get_category_page(context, category)
    max_page = max(0, (len(all_results) - 1) // RESULTS_PER_PAGE) if all_results else 0
    page = max(0, min(page, max_page))
    set_category_page(context, category, page)
    raw_title = context.user_data.get("raw_title") or context.user_data.get("current_title", "")
    info = context.user_data.get("current_tmdb_info")
    context.user_data["results"] = all_results
    context.user_data["current_category"] = category
    context.user_data["main_caption"] = build_results_caption(raw_title, info, category)


# ════════════════════════════════════════════════════════════════════════════
# Command handlers
# ════════════════════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "🎬 Welcome to Movie Search Bot!\n\n"
        "Send any title for automatic category detection.\n"
        "Or use /movie, /tv, /anime, /game.\n"
        "Example: Inception"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "📖 How to use this bot:\n\n"
        "1. Send any title to auto-detect (Movie/TV/Anime/Game).\n"
        "2. Or use:\n"
        "   • /movie <title>\n"
        "   • /tv <title>\n"
        "   • /anime <title>\n"
        "   • /game <title>\n"
        "   • /debug (admin only)\n\n"
        "Examples:\n"
        "• Inception\n"
        "• /movie Interstellar\n"
        "• /anime Attack on Titan\n"
        "• /game GTA 5"
    )


async def sites_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    try:
        sites_by_cat = await asyncio.to_thread(load_category_sites)
        if not sites_by_cat:
            await update.message.reply_text(
                "❌ No sites configured. Check that sites.json exists next to bot.py."
            )
            return
        lines = ["🌐 Configured Sites\n"]
        for cat, sites in sites_by_cat.items():
            lines.append(f"📂 {cat.upper()} ({len(sites)})")
            for site in sites:
                lines.append(f"  • {normalize_site_domain(site)}")
            lines.append("")
        await update.message.reply_text("\n".join(lines))
    except Exception:
        logger.exception("sites_command failed")
        await update.message.reply_text("❌ Could not load configured sites.")


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user_id = update.message.from_user.id
    if not is_admin_user(user_id):
        logger.warning("Unauthorized /debug attempt: user_id=%s", user_id)
        await update.message.reply_text("❌ Unauthorized.")
        return

    sites_by_cat = load_category_sites()
    total_sites = sum(len(sites) for sites in sites_by_cat.values())
    cache_size = await asyncio.to_thread(get_cache_entry_count)
    category_cache = context.user_data.get("category_results", {})
    category_cache_count = len(category_cache) if isinstance(category_cache, dict) else 0
    ddgs_status = await asyncio.to_thread(run_ddgs_healthcheck)
    supabase_status = await asyncio.to_thread(check_supabase_connection)
    uptime = format_uptime(time.time() - START_TIME)

    lines = [
        "🛠 Debug Info",
        (
            "Sites loaded: "
            f"{total_sites} "
            f"(movie={len(sites_by_cat.get(CATEGORY_MOVIE, []))}, "
            f"tv={len(sites_by_cat.get(CATEGORY_TV, []))}, "
            f"anime={len(sites_by_cat.get(CATEGORY_ANIME, []))}, "
            f"game={len(sites_by_cat.get(CATEGORY_GAME, []))})"
        ),
        f"Cache size: {cache_size if cache_size >= 0 else 'Unavailable'}",
        f"Category cache count: {category_cache_count}",
        f"DDGS test: {ddgs_status}",
        f"Supabase: {supabase_status}",
        f"Uptime: {uptime}",
    ]
    await update.message.reply_text("\n".join(lines))


def get_command_query(context: ContextTypes.DEFAULT_TYPE) -> str:
    return " ".join(context.args).strip() if context.args else ""


async def execute_search(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw_title: str,
    forced_category: Optional[str] = None,
) -> None:
    if not update.message:
        return
    if not is_valid_title(raw_title):
        await update.message.reply_text(
            f"❌ Please keep titles under {MAX_TITLE_LENGTH} characters."
        )
        return

    await asyncio.to_thread(save_history, update.message.from_user.id, raw_title)
    status = await safe_telegram_call(
        update.message.reply_text("🔍 Searching..."),
        "send status",
    )

    try:
        parsed = parse_advanced_query(raw_title)
        info = await asyncio.to_thread(get_tmdb_info, parsed["query"])
        title = (info.get("title") if info else None) or raw_title
        year = (info.get("year") if info else None) or parsed.get("year")
        is_tv = bool(info and info.get("media_type") == "tv")
        seasons = [
            s for s in (info.get("seasons") or [])
            if isinstance(s, dict) and s.get("season_number") is not None
        ] if info else []

        detected_category = await resolve_detected_category(raw_title, info, forced_category)

        # Check cache before searching
        cache_key = make_cache_key(get_category_cache_prefix(detected_category), title)
        cached = await asyncio.to_thread(get_cached, cache_key) if cache_key else None
        if cached is not None:
            results = resolve_cached_category_results(cached)
        else:
            results = await asyncio.to_thread(search_category_sites, title, detected_category, year)
            if cache_key:
                await asyncio.to_thread(save_cache, cache_key, results or CACHE_EMPTY_MARKER)

        if not results:
            msg = f'❌ "{raw_title}" was not found on any configured sites.'
            if status:
                await safe_telegram_call(status.edit_text(msg), "edit status empty")
            else:
                await safe_telegram_call(update.message.reply_text(msg), "send empty")
            return

        caption = build_results_caption(raw_title, info, detected_category)
        all_results = list(results.items())
        normalized_title = normalize_title(title)
        title_token = build_title_token(title) or ""

        reset_search_state(context)
        context.user_data.update({
            "results":              all_results,
            "category_results":     {detected_category: results},
            "category_pages":       {cat: 0 for cat in SEARCH_CATEGORIES},
            "category_cache_keys":  {detected_category: cache_key} if cache_key else {},
            "current_title":        title,
            "raw_title":            raw_title,
            "current_tmdb_info":    info,
            "current_title_key":    normalized_title,
            "current_category":     detected_category,
            "current_year":         year,
            "current_title_token":  title_token,
            "main_caption":         caption,
            "current_is_tv":        is_tv,
            "current_seasons":      seasons,
        })
        if title_token:
            store_token(context, "title_tokens", title_token, title)

        keyboard = build_main_keyboard(
            all_results, title,
            page=0,
            show_episode_guide=is_tv and bool(seasons),
            active_category=detected_category,
            title_token=title_token,
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
                    status.edit_text(caption, parse_mode="HTML",
                                     reply_markup=reply_markup,
                                     disable_web_page_preview=True),
                    "edit status fallback",
                )
        else:
            target = status or None
            if target:
                await safe_telegram_call(
                    target.edit_text(caption, parse_mode="HTML",
                                     reply_markup=reply_markup,
                                     disable_web_page_preview=True),
                    "edit status",
                )
            else:
                await safe_telegram_call(
                    update.message.reply_text(caption, parse_mode="HTML",
                                              reply_markup=reply_markup,
                                              disable_web_page_preview=True),
                    "send results",
                )
    except Exception:
        logger.exception("execute_search failed for %s", raw_title)
        msg = "❌ Something went wrong. Please try again."
        if status:
            await safe_telegram_call(status.edit_text(msg), "edit status error")
        else:
            await safe_telegram_call(update.message.reply_text(msg), "send error")


async def search_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    raw = update.message.text.strip()
    if raw:
        await execute_search(update, context, raw)


async def _category_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    category: str,
    usage: str,
) -> None:
    if not update.message:
        return
    query = get_command_query(context)
    if not query:
        await update.message.reply_text(f"Usage: {usage}")
        return
    await execute_search(update, context, query, forced_category=category)


async def movie_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _category_command(update, context, CATEGORY_MOVIE, "/movie <title>")

async def tv_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _category_command(update, context, CATEGORY_TV, "/tv <title>")

async def anime_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _category_command(update, context, CATEGORY_ANIME, "/anime <title>")

async def game_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _category_command(update, context, CATEGORY_GAME, "/game <title>")


async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    rows = await asyncio.to_thread(get_favorites, update.message.from_user.id)
    if not rows:
        await update.message.reply_text("❌ No favorites saved.")
        return
    for row in rows:
        title = row[0]
        token = build_title_token(title) or ""
        if token:
            store_token(context, "favorite_tokens", token, title)
        keyboard = [[
            InlineKeyboardButton(
                "❌ Remove",
                callback_data=f"remove:{token}" if token else "remove",
            )
        ]]
        await update.message.reply_text(
            f"⭐ {title}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    rows = await asyncio.to_thread(get_history, update.message.from_user.id)
    if not rows:
        await update.message.reply_text("❌ No history found.")
        return
    text = "🕘 Search History:\n\n" + "".join(f"• {r[0]}\n" for r in rows)
    await update.message.reply_text(text)


# ════════════════════════════════════════════════════════════════════════════
# Callback handler
# ════════════════════════════════════════════════════════════════════════════

async def favorite_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = (query.data or "").strip()
    if not data:
        await safe_telegram_call(query.answer("Invalid action."), "callback empty")
        return

    try:
        # ── Add to favorites ─────────────────────────────────────────────────
        if data.startswith("fav:") or data == "fav":
            token = data[4:] if data.startswith("fav:") else context.user_data.get("current_title_token", "")
            title = (get_token_value(context, "title_tokens", token) if token else None) \
                    or context.user_data.get("current_title", "")
            if not title:
                await safe_telegram_call(
                    query.answer("Search expired — please search again.", show_alert=True),
                    "fav expired",
                )
                return
            await asyncio.to_thread(add_favorite, query.from_user.id, title)
            await safe_telegram_call(
                query.answer("Added to favorites ⭐", show_alert=False),
                "fav add",
            )

        # ── Pagination ────────────────────────────────────────────────────────
        elif data in ("next", "prev"):
            await safe_telegram_call(query.answer(), "pagination")
            cat = context.user_data.get("current_category", CATEGORY_MOVIE)
            results = get_results_for_category(context, cat)
            all_results = list(results.items())
            if not all_results:
                await safe_telegram_call(query.answer("No results to paginate."), "page empty")
                return
            page = get_category_page(context, cat)
            page += 1 if data == "next" else -1
            max_page = max(0, (len(all_results) - 1) // RESULTS_PER_PAGE)
            page = max(0, min(page, max_page))
            set_category_page(context, cat, page)
            context.user_data["results"] = all_results
            show_guide = (
                cat == CATEGORY_TV
                and context.user_data.get("current_is_tv")
                and context.user_data.get("current_seasons")
            )
            kb = build_main_keyboard(
                all_results, context.user_data.get("current_title", ""),
                page=page, show_episode_guide=bool(show_guide),
                active_category=cat,
                title_token=context.user_data.get("current_title_token"),
            )
            await safe_telegram_call(
                query.edit_message_reply_markup(InlineKeyboardMarkup(kb)),
                "pagination edit",
            )

        # ── Category switch ───────────────────────────────────────────────────
        elif data.startswith("cat:"):
            await safe_telegram_call(query.answer(), "cat switch")
            category = data[4:].strip()
            if category not in SEARCH_CATEGORIES:
                await safe_telegram_call(query.answer("Invalid category."), "cat invalid")
                return

            cat_store = context.user_data.setdefault("category_results", {})

            # BUG FIX: use sentinel None to distinguish "not yet searched" from
            # "searched and found nothing" (empty dict).  An empty dict means we
            # already know there are no results — do not search again.
            if category not in cat_store:
                title = context.user_data.get("current_title", "")
                if not title:
                    await safe_telegram_call(
                        query.answer("Search expired — please search again.", show_alert=True),
                        "cat expired",
                    )
                    return

                cat_cache_keys = context.user_data.setdefault("category_cache_keys", {})
                cache_key = cat_cache_keys.get(category) or make_cache_key(
                    get_category_cache_prefix(category), title
                )
                if cache_key:
                    cat_cache_keys[category] = cache_key

                cached = await asyncio.to_thread(get_cached, cache_key) if cache_key else None
                if cached is not None:
                    new_results = resolve_cached_category_results(cached)
                else:
                    new_results = await asyncio.to_thread(
                        search_category_sites,
                        title,
                        category,
                        context.user_data.get("current_year"),
                    )
                    if cache_key:
                        await asyncio.to_thread(
                            save_cache, cache_key, new_results or CACHE_EMPTY_MARKER
                        )

                # Store result (even empty) so we don't re-search on next click
                cat_store[category] = new_results

                if not new_results:
                    # BUG FIX: return after toast so we don't fall through to
                    # set_active_category with empty data
                    await safe_telegram_call(
                        query.answer("No results found in this category", show_alert=False),
                        "cat no results",
                    )
                    return

            set_active_category(context, category)
            current_title = context.user_data.get("current_title", "")
            all_results   = context.user_data.get("results", [])
            page          = get_category_page(context, category)
            show_guide = (
                category == CATEGORY_TV
                and context.user_data.get("current_is_tv")
                and context.user_data.get("current_seasons")
            )
            kb = build_main_keyboard(
                all_results, current_title,
                page=page, show_episode_guide=bool(show_guide),
                active_category=category,
                title_token=context.user_data.get("current_title_token"),
            )
            caption = context.user_data.get("main_caption", "")
            if not all_results:
                caption += f"\n\n❌ No results for {category_display_label(category)}."
            await safe_telegram_call(
                query.edit_message_text(
                    caption, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb),
                    disable_web_page_preview=True,
                ),
                "cat switch edit",
            )

        # ── Remove favorite ───────────────────────────────────────────────────
        elif data.startswith("remove:"):
            token = data[7:]
            # BUG FIX: only use token as title if it's a valid lookup; never
            # use a raw hash digest as a DB value.
            title = get_token_value(context, "favorite_tokens", token) if token else None
            if not title:
                await safe_telegram_call(
                    query.answer("Favorite expired — open /favorites to refresh.", show_alert=True),
                    "remove expired",
                )
                return
            await asyncio.to_thread(remove_favorite, query.from_user.id, title)
            await safe_telegram_call(
                query.edit_message_text(f"❌ Removed:\n{title}"),
                "remove fav",
            )

        # ── Back to main ─────────────────────────────────────────────────────
        elif data == "back:main":
            await safe_telegram_call(query.answer(), "back:main")
            cat = context.user_data.get("current_category", CATEGORY_MOVIE)
            set_active_category(context, cat)
            all_results = context.user_data.get("results", [])
            caption = context.user_data.get(
                "main_caption",
                f'🎬 <b>Results for "{context.user_data.get("current_title", "")}"</b>'
                "\n\n🔗 Select a source below:",
            )
            show_guide = (
                cat == CATEGORY_TV
                and context.user_data.get("current_is_tv")
                and context.user_data.get("current_seasons")
            )
            if not all_results:
                caption += f"\n\n❌ No results for {category_display_label(cat)}."
            kb = build_main_keyboard(
                all_results, context.user_data.get("current_title", ""),
                page=get_category_page(context, cat),
                show_episode_guide=bool(show_guide),
                active_category=cat,
                title_token=context.user_data.get("current_title_token"),
            )
            await safe_telegram_call(
                query.edit_message_text(
                    caption, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb),
                    disable_web_page_preview=True,
                ),
                "back:main edit",
            )

        # ── Episode guide ─────────────────────────────────────────────────────
        elif data == "guide:episodes":
            seasons = context.user_data.get("current_seasons", [])
            if not seasons:
                await safe_telegram_call(query.answer("No season guide available."), "guide empty")
                return
            await safe_telegram_call(query.answer(), "guide episodes")
            await safe_telegram_call(
                query.edit_message_text(
                    text=format_episode_guide_text(
                        context.user_data.get("current_title", ""), seasons
                    ),
                    reply_markup=build_episode_guide_keyboard(seasons),
                ),
                "guide edit",
            )

        # ── Season selection ──────────────────────────────────────────────────
        elif data.startswith("season:"):
            number = data[7:].strip()
            await safe_telegram_call(
                query.answer(f"Selected Season {number}"), "season select"
            )

        # ── OTT lookup ────────────────────────────────────────────────────────
        elif data.startswith("ott:") or data == "ott":
            await safe_telegram_call(query.answer(), "ott")
            token = data[4:] if data.startswith("ott:") else context.user_data.get("current_title_token", "")
            title = (get_token_value(context, "title_tokens", token) if token else None) \
                    or context.user_data.get("current_title", "")
            if not title:
                await safe_telegram_call(
                    query.answer("Search expired — please search again.", show_alert=True),
                    "ott expired",
                )
                return

            norm = normalize_title(title)
            ott_cache = context.user_data.setdefault("ott_results", {})
            ott_data = ott_cache.get(norm)

            if ott_data is None:
                cache_key = make_cache_key(CACHE_PREFIX_OTT, title)
                cached = await asyncio.to_thread(get_cached, cache_key) if cache_key else None
                if cached is not None:
                    ott_data = cached
                else:
                    tmdb_info = (
                        context.user_data.get("current_tmdb_info")
                        if context.user_data.get("current_title_key") == norm
                        else None
                    )
                    ott_data = await asyncio.to_thread(search_ott, title, tmdb_info)
                    if cache_key:
                        await asyncio.to_thread(save_cache, cache_key, ott_data)
                ott_cache[norm] = ott_data

            if not isinstance(ott_data, dict):
                ott_data = {}

            await safe_telegram_call(
                query.edit_message_text(
                    text=format_ott_text(title, ott_data),
                    reply_markup=build_ott_keyboard(ott_data),
                ),
                "ott edit",
            )

        else:
            logger.warning("Unknown callback: %s", data)
            await safe_telegram_call(query.answer("Invalid action."), "unknown cb")

    except Exception:
        logger.exception("favorite_callback failed for data=%s", data)
        await safe_telegram_call(
            query.answer("❌ Something went wrong.", show_alert=False),
            "callback error",
        )


# ════════════════════════════════════════════════════════════════════════════
# Background jobs
# ════════════════════════════════════════════════════════════════════════════

async def cleanup_cache_job(_: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic cache cleanup driven by JobQueue (preferred path)."""
    try:
        removed = await asyncio.to_thread(cleanup_cache)
        if removed:
            logger.info("Cache cleanup: removed %d expired entries", removed)
    except Exception:
        logger.exception("Cache cleanup job failed")


def _fallback_cleanup_loop() -> None:
    """
    Background daemon thread used when python-telegram-bot[job-queue] is not
    installed.  Runs cleanup_cache() every CACHE_CLEANUP_INTERVAL seconds.
    The thread is daemonised so it never prevents process exit.
    """
    logger.info(
        "Fallback cache-cleanup thread started (interval=%ds)", CACHE_CLEANUP_INTERVAL
    )
    while True:
        time.sleep(CACHE_CLEANUP_INTERVAL)
        try:
            removed = cleanup_cache()
            if removed:
                logger.info(
                    "Fallback cleanup: removed %d expired cache entries", removed
                )
        except Exception:
            logger.exception("Fallback cleanup iteration failed")


def start_fallback_cleanup_thread() -> None:
    """Start the daemon cleanup thread exactly once."""
    global _cleanup_thread
    if _cleanup_thread is not None and _cleanup_thread.is_alive():
        return
    _cleanup_thread = threading.Thread(
        target=_fallback_cleanup_loop,
        name="cache-cleanup",
        daemon=True,
    )
    _cleanup_thread.start()


# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    logger.info("✅ Bot starting up...")
    logger.info("BOT_TOKEN present:    %s", bool(BOT_TOKEN))
    logger.info("TMDB_API_KEY present: %s", bool(TMDB_API_KEY))
    logger.info("SUPABASE_URL present: %s", bool(SUPABASE_URL))
    logger.info("SUPABASE_KEY present: %s", bool(SUPABASE_KEY))
    logger.info("Working directory:    %s", os.getcwd())
    logger.info("Bot script location:  %s", _BASE_DIR)
    logger.info("DB file:              %s", DB_FILE)
    logger.info("sites.json path:      %s", SITES_FILE)
    logger.info("sites.json exists:    %s", os.path.isfile(SITES_FILE))

    global SUPABASE_CLIENT
    global ADMIN_IDS

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing from environment / .env")
    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY is missing from environment / .env")
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_URL is missing from environment / .env")
    if not SUPABASE_KEY:
        raise ValueError("SUPABASE_KEY is missing from environment / .env")

    ADMIN_IDS = load_admin_ids()
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS not configured; /debug will be disabled.")

    SUPABASE_CLIENT = create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
    )

    load_category_sites(refresh=True)

    init_db()
    removed = cleanup_cache()
    if removed:
        logger.info("Startup cache cleanup: removed %d stale entries", removed)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",     start))
    app.add_handler(CommandHandler("help",      help_command))
    app.add_handler(CommandHandler("sites",     sites_command))
    app.add_handler(CommandHandler("debug",     debug_command))
    app.add_handler(CommandHandler("movie",     movie_command))
    app.add_handler(CommandHandler("tv",        tv_command))
    app.add_handler(CommandHandler("anime",     anime_command))
    app.add_handler(CommandHandler("game",      game_command))
    app.add_handler(CommandHandler("favorites", favorites_command))
    app.add_handler(CommandHandler("history",   history_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_title))
    app.add_handler(CallbackQueryHandler(favorite_callback))

    # ── JobQueue is optional (needs python-telegram-bot[job-queue]) ──────────
    if app.job_queue is not None:
        app.job_queue.run_repeating(
            cleanup_cache_job,
            interval=CACHE_CLEANUP_INTERVAL,
            first=CACHE_CLEANUP_INTERVAL,
        )
        logger.info("JobQueue cache-cleanup scheduled every %ds", CACHE_CLEANUP_INTERVAL)
    else:
        logger.warning(
            "JobQueue not available (install python-telegram-bot[job-queue] for "
            "scheduled cleanup). Falling back to daemon thread."
        )
        start_fallback_cleanup_thread()

    logger.info("Bot is polling...")
    try:
        app.run_polling(drop_pending_updates=True)
    except Conflict as exc:
        logger.warning(
            "Another bot instance is already running with the same BOT_TOKEN."
        )
        logger.warning("Telegram polling conflict: %s", exc)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("❌ Startup error:")
        traceback.print_exc()
        raise
