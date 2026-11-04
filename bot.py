"""Small Telegram movie search bot with no third-party runtime dependencies."""

from __future__ import annotations

import html
import hmac
import json
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "bot.db"
SITES_FILE = BASE_DIR / "sites.json"
LOGGER = logging.getLogger("movie-bot")
TELEGRAM_API = "https://api.telegram.org/bot{}/{}"
REQUEST_TIMEOUT = 12
CACHE_TTL = 3600
MAX_SEARCH_SITES = 8
MAX_RESULTS = 5


def load_env() -> None:
    """Load a local .env without requiring python-dotenv."""
    env_file = BASE_DIR / ".env"
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


load_env()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
MASTER_CHAT_ID = 5589716681
MASTER_DELETE_PASSWORD = os.getenv("MASTER_DELETE_PASSWORD", "New_Creation")
_last_alert: dict[str, float] = {}
_alert_lock = threading.Lock()


def supabase_enabled() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


def notify_master(subject: str, details: str) -> None:
    """Send operational alerts to the configured master without crashing the bot."""
    now = time.time()
    with _alert_lock:
        if now - _last_alert.get(subject, 0) < 300:
            return
        _last_alert[subject] = now
    try:
        telegram(
            "sendMessage",
            chat_id=MASTER_CHAT_ID,
            text=f"Movie bot alert: {subject}\n{details}",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        LOGGER.error("Could not notify master about %s: %s", subject, exc)


def supabase_request(
    table: str,
    method: str = "GET",
    query: dict[str, str] | None = None,
    payload: object | None = None,
) -> list[dict]:
    if not supabase_enabled():
        raise RuntimeError("Supabase is not configured")
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if query:
        url += "?" + urlencode(query)
    request = Request(
        url,
        data=None if payload is None else json.dumps(payload).encode(),
        method=method,
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "return=representation,resolution=merge-duplicates",
        },
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        result = json.loads(response.read().decode() or "[]")
    if not isinstance(result, list):
        raise RuntimeError("Unexpected Supabase response")
    return result


def remote_cache_get(key: str) -> list[tuple[str, str]] | None:
    rows = supabase_request(
        "bot_cache",
        query={"query": f"eq.{key}", "select": "response,created_at", "limit": "1"},
    )
    if not rows or int(time.time()) - int(rows[0]["created_at"]) >= CACHE_TTL:
        return None
    return [tuple(item) for item in rows[0]["response"]]


def remote_cache_put(key: str, results: list[tuple[str, str]]) -> None:
    supabase_request(
        "bot_cache",
        method="POST",
        payload={"query": key, "response": results, "created_at": int(time.time())},
    )


def remote_add_history(chat_id: int, title: str, category: str) -> None:
    supabase_request(
        "bot_history",
        method="POST",
        payload={
            "chat_id": chat_id,
            "title": title,
            "category": category,
            "created_at": int(time.time()),
        },
    )


def remote_save_favorite(chat_id: int, title: str, url: str) -> None:
    supabase_request(
        "bot_favorites",
        method="POST",
        payload={"chat_id": chat_id, "title": title, "url": url, "created_at": int(time.time())},
    )


def remote_list_favorites(chat_id: int) -> list[tuple[str, str]]:
    rows = supabase_request(
        "bot_favorites",
        query={
            "chat_id": f"eq.{chat_id}",
            "select": "title,url",
            "order": "created_at.desc",
            "limit": "20",
        },
    )
    return [(row["title"], row["url"]) for row in rows]


def remote_list_history(chat_id: int) -> list[tuple[str, str]]:
    rows = supabase_request(
        "bot_history",
        query={
            "chat_id": f"eq.{chat_id}",
            "select": "title,category",
            "order": "created_at.desc",
            "limit": "20",
        },
    )
    return [(row["title"], row["category"]) for row in rows]


def remote_delete_all() -> None:
    for table, column in (
        ("bot_cache", "query"),
        ("bot_history", "id"),
        ("bot_favorites", "id"),
    ):
        supabase_request(table, method="DELETE", query={column: "not.is.null"})


def delete_local_all() -> None:
    connection = db()
    connection.execute("DELETE FROM cache")
    connection.execute("DELETE FROM history")
    connection.execute("DELETE FROM favorites")
    connection.commit()
    connection.close()


def db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_FILE, timeout=10)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS history "
        "(chat_id INTEGER, title TEXT, category TEXT, created_at INTEGER)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS favorites "
        "(chat_id INTEGER, title TEXT, url TEXT, created_at INTEGER, "
        "PRIMARY KEY (chat_id, title, url))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS cache "
        "(query TEXT PRIMARY KEY, response TEXT, created_at INTEGER)"
    )
    connection.commit()
    return connection


def load_sites() -> dict[str, list[str]]:
    try:
        data = json.loads(SITES_FILE.read_text(encoding="utf-8"))
        return {
            key: [url for url in urls if valid_site(url)]
            for key, urls in data.items()
            if isinstance(urls, list)
        }
    except (OSError, ValueError) as exc:
        LOGGER.error("Could not read %s: %s", SITES_FILE, exc)
        return {}


def valid_site(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


SITES = load_sites()


def save_local_history(chat_id: int, title: str, category: str) -> None:
    connection = db()
    connection.execute(
        "INSERT INTO history(chat_id, title, category, created_at) VALUES (?, ?, ?, ?)",
        (chat_id, title, category, int(time.time())),
    )
    connection.commit()
    connection.close()


def save_local_favorite(chat_id: int, title: str, url: str) -> None:
    connection = db()
    connection.execute(
        "INSERT OR IGNORE INTO favorites(chat_id, title, url, created_at) VALUES (?, ?, ?, ?)",
        (chat_id, title, url, int(time.time())),
    )
    connection.commit()
    connection.close()


def local_list_favorites(chat_id: int) -> list[tuple[str, str]]:
    connection = db()
    rows = connection.execute(
        "SELECT title, url FROM favorites WHERE chat_id = ? ORDER BY created_at DESC LIMIT 20",
        (chat_id,),
    ).fetchall()
    connection.close()
    return rows


def local_list_history(chat_id: int) -> list[tuple[str, str]]:
    connection = db()
    rows = connection.execute(
        "SELECT title, category FROM history WHERE chat_id = ? ORDER BY created_at DESC LIMIT 20",
        (chat_id,),
    ).fetchall()
    connection.close()
    return rows


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self.current_href = ""
        self.current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self.current_href = dict(attrs).get("href") or ""
            self.current_text = []

    def handle_data(self, data: str) -> None:
        if self.current_href:
            self.current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self.current_href:
            text = " ".join("".join(self.current_text).split())
            self.links.append((self.current_href, text))
            self.current_href = ""


def fetch(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "telegram-movie-bot/1.0 (+Raspberry Pi)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return response.read(512_000).decode("utf-8", errors="replace")


def search_site(site: str, title: str, category: str) -> list[tuple[str, str]]:
    parsed = urlparse(site)
    domain = parsed.netloc.lower().split(":")[0]
    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(f'site:{domain} {title}')}"
    try:
        parser = LinkParser()
        parser.feed(fetch(search_url))
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        LOGGER.debug("Search failed for %s: %s", domain, exc)
        return []
    results = []
    for href, text in parser.links:
        absolute = urljoin(search_url, href)
        parsed_result = urlparse(absolute)
        if parsed_result.netloc.lower().endswith("duckduckgo.com"):
            redirected = parse_qs(parsed_result.query).get("uddg", [])
            if redirected:
                absolute = redirected[0]
        result_domain = urlparse(absolute).netloc.lower().split(":")[0]
        if result_domain != domain or not absolute.startswith(("http://", "https://")):
            continue
        label = text or title
        if absolute not in {url for url, _ in results}:
            results.append((absolute, label[:120]))
    return results[:2]


def search(title: str, category: str) -> list[tuple[str, str]]:
    key = f"{category}:{title.lower().strip()}"
    remote_failed = False
    if supabase_enabled():
        try:
            cached = remote_cache_get(key)
            if cached is not None:
                return cached
        except (HTTPError, URLError, OSError, KeyError, ValueError, RuntimeError) as exc:
            LOGGER.warning("Supabase cache read failed; using local SQLite: %s", exc)
            notify_master("Supabase unavailable", f"Cache read failed; local fallback enabled.\n{exc}")
            remote_failed = True

    connection: sqlite3.Connection | None = None
    if not supabase_enabled() or remote_failed:
        connection = db()
        row = connection.execute(
            "SELECT response, created_at FROM cache WHERE query = ?", (key,)
        ).fetchone()
        if row and int(time.time()) - row[1] < CACHE_TTL:
            connection.close()
            return [tuple(item) for item in json.loads(row[0])]

    sites = SITES.get(category, [])[:MAX_SEARCH_SITES]
    results: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(sites)))) as pool:
        futures = [pool.submit(search_site, site, title, category) for site in sites]
        for future in as_completed(futures):
            results.extend(future.result())
            if len(results) >= MAX_RESULTS:
                break
    results = results[:MAX_RESULTS]
    if supabase_enabled():
        try:
            remote_cache_put(key, results)
            return results
        except (HTTPError, URLError, OSError, KeyError, ValueError, RuntimeError) as exc:
            LOGGER.warning("Supabase cache write failed; using local SQLite: %s", exc)
            notify_master("Supabase unavailable", f"Cache write failed; local fallback enabled.\n{exc}")
            if connection is None:
                connection = db()
    if connection is None:
        connection = db()
    connection.execute(
        "INSERT OR REPLACE INTO cache(query, response, created_at) VALUES (?, ?, ?)",
        (key, json.dumps(results), int(time.time())),
    )
    connection.commit()
    connection.close()
    return results


def telegram(method: str, **params: object) -> object:
    request = Request(
        TELEGRAM_API.format(BOT_TOKEN, method),
        data=json.dumps(params).encode(),
        headers={"Content-Type": "application/json"},
    )
    api_timeout = max(REQUEST_TIMEOUT + 10, int(params.get("timeout", 0)) + 10)
    with urlopen(request, timeout=api_timeout) as response:
        payload = json.loads(response.read().decode())
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "Telegram API request failed"))
    return payload.get("result")


def send(chat_id: int, text: str) -> None:
    telegram("sendMessage", chat_id=chat_id, text=text, disable_web_page_preview=True)


def escape(text: str) -> str:
    return html.escape(text, quote=False)


def category_command(command: str) -> str:
    return {"movie": "movie", "tv": "tv", "anime": "anime", "game": "game"}.get(
        command, "movie"
    )


def handle_message(message: dict) -> None:
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()
    if not isinstance(chat_id, int) or not text:
        return
    command, _, argument = text.partition(" ")
    command = command.lstrip("/").split("@", 1)[0].lower()
    if command in {"start", "help"}:
        send(
            chat_id,
            "Send a title or use /movie, /tv, /anime, or /game followed by a title.\n"
            "Save a result with /favorite <title> | <url>.",
        )
        return
    if command == "sites":
        counts = ", ".join(f"{key}: {len(value)}" for key, value in SITES.items())
        send(chat_id, f"Configured search sites: {counts or 'none'}")
        return
    if command == "deleteall":
        if chat_id != MASTER_CHAT_ID:
            send(chat_id, "This command is restricted to the bot master.")
            return
        if not hmac.compare_digest(argument.strip(), MASTER_DELETE_PASSWORD):
            send(chat_id, "Invalid password.")
            notify_master("Unauthorized delete attempt", f"Chat ID: {chat_id}")
            return
        failures = []
        try:
            if supabase_enabled():
                remote_delete_all()
        except (HTTPError, URLError, OSError, KeyError, ValueError, RuntimeError, sqlite3.Error) as exc:
            LOGGER.exception("Remote delete-all operation failed")
            failures.append(f"remote: {exc}")
        try:
            delete_local_all()
        except sqlite3.Error as exc:
            LOGGER.exception("Local delete-all operation failed")
            failures.append(f"local: {exc}")
        if failures:
            notify_master("Delete-all failed", "\n".join(failures))
            send(chat_id, "Deletion was incomplete. The master has been notified.")
        else:
            send(chat_id, "All history, favorites, and caches have been deleted.")
        return
    if command == "favorite":
        if "|" not in argument:
            send(chat_id, "Usage: /favorite <title> | <url>")
            return
        title, url = (part.strip() for part in argument.split("|", 1))
        if not title or not valid_site(url):
            send(chat_id, "Provide a title and a valid http(s) URL.")
            return
        if supabase_enabled():
            try:
                remote_save_favorite(chat_id, title, url)
            except (HTTPError, URLError, OSError, KeyError, ValueError, RuntimeError) as exc:
                LOGGER.warning("Supabase favorite save failed; using local SQLite: %s", exc)
                notify_master("Supabase unavailable", f"Favorite save failed; local fallback enabled.\n{exc}")
                save_local_favorite(chat_id, title, url)
        else:
            save_local_favorite(chat_id, title, url)
        send(chat_id, "Saved to favorites.")
        return
    if command in {"favorites", "history"}:
        if supabase_enabled():
            try:
                rows = (
                    remote_list_favorites(chat_id)
                    if command == "favorites"
                    else remote_list_history(chat_id)
                )
            except (HTTPError, URLError, OSError, KeyError, ValueError, RuntimeError) as exc:
                LOGGER.warning("Supabase %s read failed; using local SQLite: %s", command, exc)
                notify_master("Supabase unavailable", f"{command} read failed; local fallback enabled.\n{exc}")
                rows = (
                    local_list_favorites(chat_id)
                    if command == "favorites"
                    else local_list_history(chat_id)
                )
        else:
            rows = (
                local_list_favorites(chat_id)
                if command == "favorites"
                else local_list_history(chat_id)
            )
        if command == "favorites":
            reply = "\n".join(f"{escape(title)}\n{url}" for title, url in rows) or "No favorites yet."
        else:
            reply = "\n".join(f"{escape(category)}: {escape(title)}" for title, category in rows) or "No search history yet."
        send(chat_id, reply)
        return
    category = category_command(command) if command in {"movie", "tv", "anime", "game"} else "movie"
    title = argument.strip() if command in {"movie", "tv", "anime", "game"} else text
    if not title:
        send(chat_id, f"Usage: /{category} <title>")
        return
    send(chat_id, f"Searching for {title}...")
    try:
        results = search(title, category)
        if supabase_enabled():
            try:
                remote_add_history(chat_id, title, category)
            except (HTTPError, URLError, OSError, KeyError, ValueError, RuntimeError) as exc:
                LOGGER.warning("Supabase history write failed; using local SQLite: %s", exc)
                notify_master("Supabase unavailable", f"History write failed; local fallback enabled.\n{exc}")
                save_local_history(chat_id, title, category)
        else:
            save_local_history(chat_id, title, category)
        if results:
            send(chat_id, "\n\n".join(f"{escape(label)}\n{url}" for url, label in results))
        else:
            send(chat_id, "No matching links found. Try a different title.")
    except (HTTPError, URLError, RuntimeError, sqlite3.Error) as exc:
        LOGGER.warning("Request failed for chat %s: %s", chat_id, exc)
        send(chat_id, "The search service is temporarily unavailable. Please try again.")


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"ok\n")

    def log_message(self, *_args: object) -> None:
        return


def start_health_server() -> None:
    try:
        port = int(os.getenv("PORT", "8080"))
        if not 1 <= port <= 65535:
            raise ValueError
        server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
        LOGGER.info("Health server listening on port %d", port)
        server.serve_forever()
    except (OSError, ValueError) as exc:
        LOGGER.warning("Health server disabled: %s", exc)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing from .env or the environment")
    if not supabase_enabled():
        db().close()
    threading.Thread(target=start_health_server, daemon=True, name="health").start()
    telegram(
        "setMyCommands",
        commands=[
            {"command": command, "description": description}
            for command, description in (
                ("start", "Show help"),
                ("movie", "Search for a movie"),
                ("tv", "Search for a TV show"),
                ("anime", "Search for anime"),
                ("game", "Search for a game"),
                ("sites", "Show configured sites"),
                ("favorites", "Show favorites"),
                ("history", "Show search history"),
                ("deleteall", "Delete all stored data (master only)"),
            )
        ],
    )
    LOGGER.info("Bot is running with %d configured sites", sum(map(len, SITES.values())))
    offset = 0
    while True:
        try:
            updates = telegram(
                "getUpdates", offset=offset, timeout=50, allowed_updates=["message"]
            )
            for update in updates or []:
                offset = update["update_id"] + 1
                handle_message(update.get("message", {}))
        except (HTTPError, URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            LOGGER.warning("Polling failed: %s; retrying", exc)
            notify_master("Bot polling fault", str(exc))
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        LOGGER.exception("Fatal bot error")
        notify_master("Bot stopped", str(exc))
        raise
