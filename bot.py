"""Telegram movie-search bot."""

from __future__ import annotations

import base64
import binascii
import html
import hmac
import json
import logging
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "bot.db"
SITES_FILE = BASE_DIR / "sites.json"

LOGGER = logging.getLogger("telegram-movie-bot")
TELEGRAM_API = "https://api.telegram.org/bot{}/{}"

REQUEST_TIMEOUT = 12
CACHE_TTL = 3600
MAX_SEARCH_SITES = 12
MAX_SEARCH_BATCHES = 3
MAX_RESULTS = 10
SEARCH_TIMEOUT = 30
PASSWORD_PROMPT_TTL = 120
MASTER_CHAT_ID = 5589716681


def load_env() -> None:
    env_file = BASE_DIR / ".env"
    if not env_file.is_file():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(
            key.strip(),
            value.strip().strip("\"'")
        )


load_env()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
MASTER_DELETE_PASSWORD = os.getenv(
    "MASTER_DELETE_PASSWORD",
    "New_Creation",
)

_pending_passwords: dict[int, tuple[str, float]] = {}
_alert_lock = threading.Lock()
_last_alert: dict[str, float] = {}


def supabase_enabled() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


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
        "PRIMARY KEY(chat_id, title, url))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS cache "
        "(query TEXT PRIMARY KEY, response TEXT, created_at INTEGER)"
    )
    connection.commit()
    return connection


def valid_site(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def load_sites() -> dict[str, list[str]]:
    try:
        data = json.loads(
            SITES_FILE.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.error("Could not load sites.json: %s", exc)
        return {}

    return {
        key: [url for url in urls if valid_site(url)]
        for key, urls in data.items()
        if isinstance(urls, list)
    }


SITES = load_sites()


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


def notify_master(subject: str, details: str) -> None:
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
        LOGGER.error("Could not notify master: %s", exc)


def remote_cache_get(key: str) -> list[tuple[str, str]] | None:
    rows = supabase_request(
        "bot_cache",
        query={
            "query": f"eq.{key}",
            "select": "response,created_at",
            "limit": "1",
        },
    )

    if not rows:
        return None

    if int(time.time()) - int(rows[0]["created_at"]) >= CACHE_TTL:
        return None

    cached = [tuple(item) for item in rows[0]["response"]]
    return cached or None


def remote_cache_put(
    key: str,
    results: list[tuple[str, str]],
) -> None:
    if results:
        supabase_request(
            "bot_cache",
            method="POST",
            payload={
                "query": key,
                "response": results,
                "created_at": int(time.time()),
            },
        )


def save_history(
    chat_id: int,
    title: str,
    category: str,
) -> None:
    if supabase_enabled():
        try:
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
            return
        except Exception as exc:
            LOGGER.warning("Remote history save failed: %s", exc)

    connection = db()
    connection.execute(
        "INSERT INTO history(chat_id,title,category,created_at) "
        "VALUES(?,?,?,?)",
        (chat_id, title, category, int(time.time())),
    )
    connection.commit()
    connection.close()


def save_favorite(
    chat_id: int,
    title: str,
    url: str,
) -> None:
    if supabase_enabled():
        try:
            supabase_request(
                "bot_favorites",
                method="POST",
                payload={
                    "chat_id": chat_id,
                    "title": title,
                    "url": url,
                    "created_at": int(time.time()),
                },
            )
            return
        except Exception as exc:
            LOGGER.warning("Remote favorite save failed: %s", exc)

    connection = db()
    connection.execute(
        "INSERT OR IGNORE INTO favorites "
        "(chat_id,title,url,created_at) VALUES(?,?,?,?)",
        (chat_id, title, url, int(time.time())),
    )
    connection.commit()
    connection.close()


def list_favorites(chat_id: int) -> list[tuple[str, str]]:
    if supabase_enabled():
        try:
            rows = supabase_request(
                "bot_favorites",
                query={
                    "chat_id": f"eq.{chat_id}",
                    "select": "title,url",
                    "order": "created_at.desc",
                    "limit": "20",
                },
            )
            return [
                (row["title"], row["url"])
                for row in rows
            ]
        except Exception as exc:
            LOGGER.warning("Remote favorites failed: %s", exc)

    connection = db()
    rows = connection.execute(
        "SELECT title,url FROM favorites "
        "WHERE chat_id=? "
        "ORDER BY created_at DESC LIMIT 20",
        (chat_id,),
    ).fetchall()
    connection.close()
    return rows


def list_history(chat_id: int) -> list[tuple[str, str]]:
    if supabase_enabled():
        try:
            rows = supabase_request(
                "bot_history",
                query={
                    "chat_id": f"eq.{chat_id}",
                    "select": "title,category",
                    "order": "created_at.desc",
                    "limit": "20",
                },
            )
            return [
                (row["title"], row["category"])
                for row in rows
            ]
        except Exception as exc:
            LOGGER.warning("Remote history failed: %s", exc)

    connection = db()
    rows = connection.execute(
        "SELECT title,category FROM history "
        "WHERE chat_id=? "
        "ORDER BY created_at DESC LIMIT 20",
        (chat_id,),
    ).fetchall()
    connection.close()
    return rows


def delete_local(scope: str) -> None:
    connection = db()
    tables = {
        "cache": ("cache",),
        "storage": ("history", "favorites"),
        "all": ("cache", "history", "favorites"),
    }

    for table in tables[scope]:
        connection.execute(f"DELETE FROM {table}")

    connection.commit()
    connection.close()


def delete_remote(scope: str) -> None:
    if not supabase_enabled():
        return

    tables = {
        "cache": ("bot_cache", "query"),
        "storage": ("bot_history", "id"),
        "all": ("bot_cache", "query"),
    }

    if scope in {"storage", "all"}:
        supabase_request(
            "bot_history",
            method="DELETE",
            query={"id": "not.is.null"},
        )
        supabase_request(
            "bot_favorites",
            method="DELETE",
            query={"id": "not.is.null"},
        )

    if scope in {"cache", "all"}:
        table, column = tables["cache"]
        supabase_request(
            table,
            method="DELETE",
            query={column: "not.is.null"},
        )


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self.current_href = ""
        self.current_text: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        tag_name = tag.lower()
        if tag_name in {"a", "link"}:
            attributes = dict(attrs)
            self.current_href = (
                attributes.get("href")
                or ("__text__" if tag_name == "link" else "")
            )
            self.current_text = []

    def handle_data(self, data: str) -> None:
        if self.current_href:
            self.current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"a", "link"} and self.current_href:
            text = " ".join(
                "".join(self.current_text).split()
            )
            href = (
                text
                if self.current_href == "__text__"
                else self.current_href
            )
            self.links.append(
                (href, text)
            )
            self.current_href = ""


def fetch(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        },
    )

    with urlopen(
        request,
        timeout=REQUEST_TIMEOUT,
    ) as response:
        return response.read(
            512_000
        ).decode(
            "utf-8",
            errors="replace",
        )


def normalise_domain(value: str) -> str:
    domain = urlparse(value).netloc.lower().split(":")[0].rstrip(".")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def unwrap_redirect(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower().split(":")[0]

    if host.endswith("duckduckgo.com"):
        redirect = parse_qs(
            parsed.query
        ).get("uddg", [])
        if redirect:
            return redirect[0]

    if host.endswith("bing.com"):
        redirect = parse_qs(
            parsed.query
        ).get("u", [])
        if redirect:
            value = redirect[0]
            if value.startswith("a1"):
                encoded = value[2:]
                try:
                    padding = "=" * (
                        -len(encoded) % 4
                    )
                    decoded = base64.urlsafe_b64decode(
                        encoded + padding
                    ).decode(
                        "utf-8",
                        errors="replace",
                    )
                    if decoded.startswith(
                        ("http://", "https://")
                    ):
                        return decoded
                except (ValueError, binascii.Error):
                    pass
            return value

    if host in {"google.com", "www.google.com"}:
        redirect = parse_qs(
            parsed.query
        ).get("q", [])
        if redirect:
            return redirect[0]

    return url


def search_site(
    site: str,
    title: str,
    category: str,
) -> list[tuple[str, str]]:
    del category

    domain = normalise_domain(site)
    slug = quote_plus(title.lower().strip().replace(" ", "-"))
    title_query = quote_plus(title)

    query = quote_plus(
        f'site:{domain} "{title}"'
    )

    search_urls = (
        f"https://www.bing.com/search?format=rss&q={query}",
        f"https://www.bing.com/search?q={query}",
        f"https://search.brave.com/search?q={query}",
        f"https://search.yahoo.com/search?p={query}",
        f"https://www.google.com/search?q={query}",
        f"https://www.ecosia.org/search?q={query}",
        f"https://www.mojeek.com/search?q={query}",
        f"https://html.duckduckgo.com/html/?q={query}",
        f"https://lite.duckduckgo.com/lite/?q={query}",
    )

    results: list[tuple[str, str]] = []

    for search_url in search_urls:
        try:
            parser = LinkParser()
            parser.feed(fetch(search_url))
        except (
            HTTPError,
            URLError,
            TimeoutError,
            ValueError,
        ) as exc:
            LOGGER.debug(
                "Search provider failed for %s: %s",
                domain,
                exc,
            )
            continue

        for href, text in parser.links:
            absolute = unwrap_redirect(
                urljoin(search_url, href)
            )

            if not absolute.startswith(
                ("http://", "https://")
            ):
                continue

            if normalise_domain(absolute) != domain:
                continue

            label = text or title
            if absolute not in {
                url for url, _ in results
            }:
                results.append(
                    (absolute, label[:120])
                )

        if results:
            break

    direct_search_urls = (
        f"{site.rstrip('/')}/search?q={title_query}",
        f"{site.rstrip('/')}/search/{slug}",
        f"{site.rstrip('/')}/search/{title_query}",
        f"{site.rstrip('/')}/search?query={title_query}",
        f"{site.rstrip('/')}/?q={title_query}",
        f"{site.rstrip('/')}/?s={title_query}",
        f"{site.rstrip('/')}/movie/{slug}",
        f"{site.rstrip('/')}/movies/{slug}",
        f"{site.rstrip('/')}/film/{slug}",
        f"{site.rstrip('/')}/watch/{slug}",
        f"{site.rstrip('/')}/tv/{slug}",
        f"{site.rstrip('/')}/title/{slug}",
        f"{site.rstrip('/')}/catalog/{slug}",
    )

    for search_url in direct_search_urls:
        try:
            parser = LinkParser()
            page_body = fetch(search_url)
            parser.feed(page_body)
        except (
            HTTPError,
            URLError,
            TimeoutError,
            ValueError,
        ) as exc:
            LOGGER.debug(
                "Direct search failed for %s: %s",
                domain,
                exc,
            )
            continue

        for href, text in parser.links:
            absolute = urljoin(search_url, href)
            if not absolute.startswith(
                ("http://", "https://")
            ):
                continue

            if normalise_domain(absolute) != domain:
                continue

            if absolute.rstrip("/") == site.rstrip("/"):
                continue

            label = text or title
            if absolute not in {
                url for url, _ in results
            }:
                results.append(
                    (absolute, label[:120])
                )

        title_normalized = re.sub(
            r"[^a-z0-9]+",
            " ",
            canonical_title(title).lower(),
        ).strip()
        page_text = page_body.lower()
        url_slug = slugify_url_title(search_url)
        if (
            "/search" not in search_url.lower()
            and "?s=" not in search_url.lower()
            and "?q=" not in search_url.lower()
            and title_normalized in page_text
            and title_normalized in url_slug
            and search_url not in {
                url for url, _ in results
            }
        ):
            results.append(
                (search_url, canonical_title(title))
            )

        if results:
            break

    title_lower = canonical_title(title).lower().strip()
    search_tokens = {
        token
        for token in title_tokens(title)
        if len(token) >= 2
    }

    def result_rank(
        candidate: tuple[str, str],
    ) -> tuple[int, float, str]:
        url, label = candidate
        url_lower = url.lower()
        label_lower = label.lower()
        combined = f"{label_lower} {url_lower}"

        if any(
            marker in url_lower
            for marker in (
                "/search",
                "?s=",
                "?q=",
                "search?",
                "/search?",
            )
        ):
            return (-100, 0.0, url)

        title_similarity = SequenceMatcher(
            None,
            title_lower,
            label_lower,
        ).ratio()
        url_similarity = SequenceMatcher(
            None,
            title_lower,
            url_lower,
        ).ratio()
        matched_tokens = sum(
            token in combined
            for token in search_tokens
        )

        score = (
            matched_tokens * 20
            + int(title_similarity * 30)
            + int(url_similarity * 20)
        )
        if title_lower in label_lower:
            score += 50
        if title_lower.replace(" ", "-") in url_lower:
            score += 50

        return (
            score,
            max(title_similarity, url_similarity),
            url,
        )

    ranked = sorted(
        results,
        key=result_rank,
        reverse=True,
    )
    exact = [
        candidate
        for candidate in ranked
        if relevant_result(
            title,
            candidate[0],
            candidate[1],
        )
    ]
    return exact[:3]


def search(
    title: str,
    category: str,
) -> list[tuple[str, str]]:
    title = canonical_title(title)
    key = f"{category}:{title.lower().strip()}"

    if supabase_enabled():
        try:
            cached = remote_cache_get(key)
            if cached is not None:
                usable = [
                    item
                    for item in cached
                    if relevant_result(title, item[0], item[1])
                ]
                if usable:
                    return usable[:MAX_RESULTS]
        except Exception as exc:
            LOGGER.warning(
                "Supabase cache read failed: %s",
                exc,
            )

    connection = db()
    row = connection.execute(
        "SELECT response,created_at "
        "FROM cache "
        "WHERE query=?",
        (key,),
    ).fetchone()

    if row and int(time.time()) - row[1] < CACHE_TTL:
        cached = [
            tuple(item)
            for item in json.loads(row[0])
        ]
        cached = [
            item
            for item in cached
        if relevant_result(title, item[0], item[1])
        ]
        if cached:
            connection.close()
            return cached[:MAX_RESULTS]

    sites = SITES.get(category, [])
    results: list[tuple[str, str]] = []
    search_deadline = time.monotonic() + SEARCH_TIMEOUT

    for batch_start in range(
        0,
        len(sites),
        MAX_SEARCH_SITES,
    ):
        if time.monotonic() >= search_deadline:
            break

        if (
            batch_start // MAX_SEARCH_SITES
            >= MAX_SEARCH_BATCHES
        ):
            break

        batch = sites[
            batch_start:
            batch_start + MAX_SEARCH_SITES
        ]

        pool = ThreadPoolExecutor(
            max_workers=min(
                4,
                max(1, len(batch)),
            )
        )
        futures = [
            pool.submit(
                search_site,
                site,
                title,
                category,
            )
            for site in batch
        ]

        try:
            remaining = max(
                0.1,
                search_deadline - time.monotonic(),
            )
            for future in as_completed(
                futures,
                timeout=remaining,
            ):
                try:
                    results.extend(
                        future.result()
                    )
                except Exception as exc:
                    LOGGER.warning(
                        "Search worker failed: %s",
                        exc,
                    )

                if len(results) >= MAX_RESULTS:
                    break
        except TimeoutError:
            LOGGER.debug(
                "Search stopped after %s seconds",
                SEARCH_TIMEOUT,
            )
        finally:
            pool.shutdown(
                wait=False,
                cancel_futures=True,
            )

        if len(results) >= MAX_RESULTS:
            break

    results = results[:MAX_RESULTS]

    if results:
        if supabase_enabled():
            try:
                remote_cache_put(key, results)
            except Exception as exc:
                LOGGER.warning(
                    "Supabase cache write failed: %s",
                    exc,
                )

        connection.execute(
            "INSERT OR REPLACE INTO cache "
            "(query,response,created_at) "
            "VALUES(?,?,?)",
            (
                key,
                json.dumps(results),
                int(time.time()),
            ),
        )
        connection.commit()

    connection.close()
    return results


def telegram(
    method: str,
    **params: object,
) -> object:
    request = Request(
        TELEGRAM_API.format(
            BOT_TOKEN,
            method,
        ),
        data=json.dumps(params).encode(),
        headers={
            "Content-Type": "application/json",
        },
    )

    api_timeout = max(
        REQUEST_TIMEOUT + 20,
        int(params.get("timeout", 0)) + 20,
    )

    with urlopen(
        request,
        timeout=api_timeout,
    ) as response:
        payload = json.loads(
            response.read().decode()
        )

    if not payload.get("ok"):
        raise RuntimeError(
            payload.get(
                "description",
                "Telegram API request failed",
            )
        )

    return payload.get("result")


def send(
    chat_id: int,
    text: str,
    reply_markup: dict | None = None,
) -> None:
    params: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    if reply_markup is not None:
        params["reply_markup"] = reply_markup

    telegram(
        "sendMessage",
        **params,
    )


def result_page_markup(
    chat_id: int,
    page: int,
    page_count: int,
) -> dict:
    del chat_id
    buttons: list[dict] = []
    if page > 0:
        buttons.append(
            {
                "text": "Previous",
                "callback_data": f"results:{page - 1}",
            }
        )
    if page + 1 < page_count:
        buttons.append(
            {
                "text": "Next",
                "callback_data": f"results:{page + 1}",
            }
        )
    return {
        "inline_keyboard": [buttons]
    } if buttons else {}


def send_result_page(
    chat_id: int,
    results: list[tuple[str, str]],
    page: int,
) -> None:
    page_size = 5
    page_count = (
        len(results) + page_size - 1
    ) // page_size
    page = max(0, min(page, page_count - 1))
    start = page * page_size
    current = results[start:start + page_size]

    text = "\n\n".join(
        f"{escape(label)}\n{url}"
        for url, label in current
    )
    text += (
        f"\n\nPage {page + 1}/{page_count}"
    )

    markup = result_page_markup(
        chat_id,
        page,
        page_count,
    )
    send(
        chat_id,
        text,
        markup if markup else None,
    )


def escape(text: str) -> str:
    return html.escape(text, quote=False)


def canonical_title(title: str) -> str:
    normalized = " ".join(
        title.lower()
        .replace("-", " ")
        .replace("_", " ")
        .split()
    )

    replacements = {
        "vampire daires": "vampire diaries",
        "vampire dairy": "vampire diaries",
        "vampire dairies": "vampire diaries",
    }
    return replacements.get(normalized, title.strip())


def title_tokens(title: str) -> list[str]:
    return re.findall(
        r"[a-z0-9]+",
        canonical_title(title).lower(),
    )


def slugify_url_title(url: str) -> str:
    path = urlparse(url).path.lower().strip("/")
    if not path:
        return ""

    segments = [
        segment
        for segment in path.split("/")
        if segment
    ]
    slug = segments[-1]
    slug = "".join(
        char
        if char.isalnum()
        else " "
        for char in slug
    )
    return " ".join(slug.split())


def relevant_result(
    title: str,
    url: str,
    label: str,
) -> bool:
    title_lower = canonical_title(title).lower().strip()
    label_lower = label.lower().strip()
    title_normalized = re.sub(r"[^a-z0-9]+", " ", title_lower).strip()
    title_slug = title_normalized.replace(" ", "-")

    if any(
        marker in url.lower()
        for marker in (
            "/search",
            "?s=",
            "?q=",
            "search?",
            "/category/",
            "/categories/",
            "/genres/",
        )
    ):
        return False

    normalized_title = title_slug
    url_lower = url.lower()
    normalized_slug = slugify_url_title(url)

    title_slug_tokens = title_tokens(title)
    significant_title_tokens = [
        token for token in title_slug_tokens
        if len(token) >= 3
    ]

    url_slug_match = bool(normalized_slug) and (
        SequenceMatcher(
            None,
            title_normalized,
            normalized_slug,
        ).ratio() >= 0.72
        or title_normalized in normalized_slug
        or normalized_title in url_lower
    )
    if url_slug_match:
        return True

    normalized_label = re.sub(r"[^a-z0-9]+", " ", label_lower).strip()
    if (
        title_normalized in normalized_label
        and normalized_slug
        and len(normalized_slug) > len(title_normalized)
        and title_normalized in normalized_slug
    ):
        return True

    if not significant_title_tokens:
        return False

    matched_in_label = sum(
        token in label_lower
        for token in significant_title_tokens
    )
    matched_in_url = sum(
        token in url.lower()
        for token in significant_title_tokens
    )
    return (
        matched_in_label == len(significant_title_tokens)
        and matched_in_url == len(significant_title_tokens)
    )


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header(
            "Content-Type",
            "text/plain; charset=utf-8",
        )
        self.end_headers()
        self.wfile.write(b"ok\n")

    def log_message(
        self,
        *_args: object,
    ) -> None:
        return


def start_health_server() -> None:
    try:
        port = int(
            os.getenv("PORT", "8080")
        )

        server = ThreadingHTTPServer(
            ("0.0.0.0", port),
            HealthHandler,
        )

        LOGGER.info(
            "Health server listening on port %d",
            port,
        )

        server.serve_forever()
    except (OSError, ValueError) as exc:
        LOGGER.error(
            "Health server failed: %s",
            exc,
        )


_pending_passwords: dict[
    int,
    tuple[str, float],
] = {}
_search_pages: dict[
    int,
    tuple[list[tuple[str, str]], int],
] = {}


def delete_for_master(
    chat_id: int,
    command: str,
) -> None:
    scope = {
        "deletecache": "cache",
        "deletestorage": "storage",
        "deleteall": "all",
    }[command]

    if supabase_enabled():
        try:
            delete_remote(scope)
        except Exception as exc:
            LOGGER.error(
                "Remote delete failed: %s",
                exc,
            )
            send(
                chat_id,
                "Remote deletion failed.",
            )
            return

    try:
        delete_local(scope)
    except sqlite3.Error as exc:
        LOGGER.error(
            "Local delete failed: %s",
            exc,
        )
        send(
            chat_id,
            "Local deletion failed.",
        )
        return

    send(
        chat_id,
        "Deletion completed.",
    )


def category_command(command: str) -> str:
    return {
        "movie": "movie",
        "tv": "tv",
        "anime": "anime",
        "game": "game",
    }.get(command, "movie")


def public_commands() -> list[dict[str, str]]:
    return [
        {
            "command": "start",
            "description": "Show help",
        },
        {
            "command": "movie",
            "description": "Search for a movie",
        },
        {
            "command": "tv",
            "description": "Search for a TV show",
        },
        {
            "command": "anime",
            "description": "Search for anime",
        },
        {
            "command": "game",
            "description": "Search for a game",
        },
        {
            "command": "sites",
            "description": "Show configured sites",
        },
        {
            "command": "favorites",
            "description": "Show favorites",
        },
        {
            "command": "history",
            "description": "Show search history",
        },
    ]


def handle_message(message: dict) -> None:
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()

    if not isinstance(chat_id, int) or not text:
        return

    LOGGER.info(
        "Received message from %s: %s",
        chat_id,
        text[:200],
    )

    command, _, argument = text.partition(" ")
    command = command.lstrip("/").split("@", 1)[0].lower()

    pending = _pending_passwords.get(chat_id)
    if pending:
        pending_command, created_at = pending

        if time.time() - created_at >= PASSWORD_PROMPT_TTL:
            _pending_passwords.pop(chat_id, None)
            send(
                chat_id,
                "Password prompt expired.",
            )
            return

        _pending_passwords.pop(chat_id, None)

        if hmac.compare_digest(
            text,
            MASTER_DELETE_PASSWORD,
        ):
            delete_for_master(
                chat_id,
                pending_command,
            )
        else:
            send(
                chat_id,
                "Password required or invalid password.",
            )
        return

    if command in {"start", "help"}:
        send(
            chat_id,
            "Send a title or use /movie, /tv, /anime, or "
            "/game followed by a title.\n"
            "Example: /movie Inception",
        )
        return

    if command == "sites":
        counts = ", ".join(
            f"{key}: {len(value)}"
            for key, value in SITES.items()
        )
        send(
            chat_id,
            f"Configured search sites: {counts or 'none'}",
        )
        return

    if command == "favorites":
        rows = list_favorites(chat_id)
        reply = "\n".join(
            f"{escape(title)}\n{url}"
            for title, url in rows
        ) or "No favorites yet."
        send(chat_id, reply)
        return

    if command == "history":
        rows = list_history(chat_id)
        reply = "\n".join(
            f"{escape(category)}: {escape(title)}"
            for title, category in rows
        ) or "No search history yet."
        send(chat_id, reply)
        return

    if command == "favorite":
        if "|" not in argument:
            send(
                chat_id,
                "Usage: /favorite <title> | <url>",
            )
            return

        title, url = (
            part.strip()
            for part in argument.split("|", 1)
        )

        if not title or not valid_site(url):
            send(
                chat_id,
                "Provide a title and valid http(s) URL.",
            )
            return

        save_favorite(
            chat_id,
            title,
            url,
        )
        send(chat_id, "Saved to favorites.")
        return

    if command in {
        "deletecache",
        "deletestorage",
        "deleteall",
    }:
        if chat_id != MASTER_CHAT_ID:
            send(
                chat_id,
                "This command is restricted.",
            )
            return

        if not argument.strip():
            _pending_passwords[chat_id] = (
                command,
                time.time(),
            )
            send(
                chat_id,
                "Send the password in your next message.",
            )
            return

        if not hmac.compare_digest(
            argument.strip(),
            MASTER_DELETE_PASSWORD,
        ):
            send(
                chat_id,
                "Password required or invalid password.",
            )
            return

        delete_for_master(
            chat_id,
            command,
        )
        return

    if command == "masterstatus":
        if chat_id != MASTER_CHAT_ID:
            return
        send(
            chat_id,
            "Bot is running.\n"
            f"Sites: {sum(map(len, SITES.values()))}\n"
            "Backend: "
            + (
                "Supabase"
                if supabase_enabled()
                else
                "local SQLite"
            ),
        )
        return

    category = (
        category_command(command)
        if command in {
            "movie",
            "tv",
            "anime",
            "game",
        }
        else "movie"
    )

    title = (
        argument.strip()
        if command in {
            "movie",
            "tv",
            "anime",
            "game",
        }
        else text
    )

    if not title:
        send(
            chat_id,
            f"Usage: /{category} <title>",
        )
        return

    send(
        chat_id,
        f"Searching for {title}...",
    )

    try:
        results = search(
            title,
            category,
        )
        save_history(
            chat_id,
            title,
            category,
        )

        if results:
            _search_pages[chat_id] = (
                results,
                0,
            )
            send_result_page(
                chat_id,
                results,
                0,
            )
        else:
            send(
                chat_id,
                "No matching links found. "
                "Try a different title.",
            )

    except Exception as exc:
        LOGGER.warning(
            "Search failed: %s",
            exc,
        )
        send(
            chat_id,
            "The search service is temporarily "
            "unavailable.",
        )


def handle_callback_query(
    callback_query: dict,
) -> None:
    callback_id = callback_query.get("id")
    data = callback_query.get("data") or ""
    message = callback_query.get("message", {})
    chat_id = message.get("chat", {}).get("id")

    if not isinstance(chat_id, int):
        return

    if callback_id:
        try:
            telegram(
                "answerCallbackQuery",
                callback_query_id=callback_id,
            )
        except Exception as exc:
            LOGGER.warning(
                "Could not answer callback query: %s",
                exc,
            )

    if not data.startswith("results:"):
        return

    try:
        page = int(data.split(":", 1)[1])
    except ValueError:
        return

    state = _search_pages.get(chat_id)
    if not state:
        send(chat_id, "Search results expired.")
        return

    results, _old_page = state
    page_size = 5
    page_count = (
        len(results) + page_size - 1
    ) // page_size
    page = max(0, min(page, page_count - 1))
    _search_pages[chat_id] = (
        results,
        page,
    )

    message_id = message.get("message_id")
    if isinstance(message_id, int):
        start = page * page_size
        current = results[start:start + page_size]
        text = "\n\n".join(
            f"{escape(label)}\n{url}"
            for url, label in current
        )
        text += (
            f"\n\nPage {page + 1}/{page_count}"
        )
        markup = result_page_markup(
            chat_id,
            page,
            page_count,
        )
        try:
            telegram(
                "editMessageText",
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                disable_web_page_preview=True,
                reply_markup=(
                    markup if markup else None
                ),
            )
            return
        except Exception as exc:
            LOGGER.warning(
                "Could not edit result page: %s",
                exc,
            )

    send_result_page(
        chat_id,
        results,
        page,
    )


def main() -> None:
    logging.basicConfig(
        level=os.getenv(
            "LOG_LEVEL",
            "INFO",
        ).upper(),
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing"
        )

    if not supabase_enabled():
        db().close()

    threading.Thread(
        target=start_health_server,
        daemon=True,
        name="health",
    ).start()

    try:
        telegram(
            "setMyCommands",
            scope={"type": "default"},
            commands=public_commands(),
        )
    except (
        HTTPError,
        URLError,
        TimeoutError,
        RuntimeError,
        json.JSONDecodeError,
    ) as exc:
        LOGGER.warning(
            "Could not publish Telegram commands: %s",
            exc,
        )

    offset = 0

    while True:
        try:
            updates = telegram(
                "getUpdates",
                offset=offset,
                timeout=45,
                allowed_updates=[
                    "message",
                    "callback_query",
                ],
            )

            for update in updates or []:
                update_id = update["update_id"]
                try:
                    if "callback_query" in update:
                        handle_callback_query(
                            update["callback_query"]
                        )
                    else:
                        handle_message(
                            update.get("message", {})
                        )
                except Exception as exc:
                    LOGGER.exception(
                        "Failed to handle update %s",
                        update_id,
                    )
                    message = update.get("message", {})
                    chat_id = message.get("chat", {}).get("id")
                    if isinstance(chat_id, int):
                        try:
                            send(
                                chat_id,
                                "The bot encountered an error. "
                                "Please try again.",
                            )
                        except Exception:
                            LOGGER.exception(
                                "Could not send update failure reply",
                            )
                finally:
                    offset = update_id + 1

        except (
            HTTPError,
            URLError,
            TimeoutError,
            RuntimeError,
            json.JSONDecodeError,
        ) as exc:
            LOGGER.warning(
                "Polling failed: %s",
                exc,
            )
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        LOGGER.exception(
            "Fatal bot error"
        )
        raise
