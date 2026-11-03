# Movie & TV Show Link Finder Bot

A Telegram bot that searches your saved streaming/download sites and returns the direct link to any movie or TV show you ask for.

---

## Features

- `/movie <title>`, `/tv <title>`, `/anime <title>`, and `/game <title>` search configured sites
- Plain text searches for movies
- `/sites`, `/favorites`, `/history`, and `/favorite <title> | <url>` provide lightweight local storage
- SQLite cache reduces network traffic and keeps Raspberry Pi storage usage low
- Health endpoint at `http://localhost:8080/`

---

## Setup

### 1. Get a Bot Token

1. Open Telegram and search for **@BotFather**
2. Send `/newbot` and follow the steps
3. Copy the token it gives you

### 2. Install dependencies

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` is intentionally empty: the bot uses only the Python standard
library, so it runs on Raspberry Pi OS without compiling lxml or installing large
optional SDKs.

### 3. Configure the token

Copy the example env file and fill in at least the bot token:

```bash
cp .env.example .env
```

Open `.env` and replace the placeholder:

```
BOT_TOKEN=your-telegram-bot-token
```

`TMDB_API_KEY` enables metadata and OTT lookups. `SUPABASE_URL` and
`SUPABASE_KEY` enable shared remote persistence; when they are not set, the bot
stores history, favorites, and cache in the local, ignored `bot.db` file.
`MISTRAL_API_KEY` is optional and only enables `/ai`.
The search layer uses the ordered `DDG_BACKENDS` list (default:
`yahoo,wikipedia,grokipedia`) and queries one backend at a time. Add
`duckduckgo` only if it is available from your deployment network. No general
search API key is currently configured in this project; adding one would
require wiring its credentials through a new environment variable and API
client.

### 4. Run the bot

```bash
python bot.py
```

The bot starts polling Telegram and creates the ignored `bot.db` SQLite file.

---

## Usage

### Search a title

```
/movie Inception
/tv Breaking Bad
/anime Attack on Titan
/game GTA 5
```

The bot also accepts a plain-text title and detects its category automatically.

### View and manage your sites

Use `/sites`, `/favorites`, and `/history` to inspect configured sites and
your saved data.

---

## How the search works

For each configured site, the bot makes a bounded DuckDuckGo HTML search and
only accepts result links whose hostname exactly matches the configured hostname.
At most eight sites are queried with four workers, which keeps memory and CPU
usage reasonable on a Raspberry Pi.

---

## Notes

- Sites must have a publicly accessible search page (no login required)
- The bot searches sites in parallel for speed
- `sites.json` is read from the same folder as `bot.py`
- You can keep the bot running 24/7 on a Raspberry Pi or any Linux server.
- Keep `.env` private and set `BOT_TOKEN` before starting.
- For a low-power Pi, use a 64-bit Raspberry Pi OS image and Python 3.11+.
