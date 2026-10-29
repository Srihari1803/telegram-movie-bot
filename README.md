# 🎬 Movie & TV Show Link Finder Bot

A Telegram bot that searches your saved streaming/download sites and returns the direct link to any movie or TV show you ask for.

---

## Features

- `/search <title>` — searches all your saved sites and returns direct links
- `/addsite <url>` — add a website to the search list (persisted to disk)
- `/removesite <url>` — remove a website
- `/listsites` — view all saved sites
- Smart search: tries 9 different URL patterns per site, scores results, and returns the best match

---

## Setup

### 1. Get a Bot Token

1. Open Telegram and search for **@BotFather**
2. Send `/newbot` and follow the steps
3. Copy the token it gives you

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

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

You should see:
```
✅ Bot is running. Press Ctrl+C to stop.
```

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

For each configured site, the bot queries DuckDuckGo and only accepts HTTPS or
HTTP result links whose hostname exactly matches the configured hostname.
Searches use bounded request timeouts and run in parallel. Links with low confidence are discarded.

---

## Notes

- Sites must have a publicly accessible search page (no login required)
- The bot searches sites in parallel for speed
- `sites.json` is read from the same folder as `bot.py`
- You can keep the bot running 24/7 on a VPS, Raspberry Pi, or any server
- 
