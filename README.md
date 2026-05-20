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

Copy the example env file and fill in your token:

```bash
cp .env.example .env
```

Open `.env` and replace the placeholder:

```
BOT_TOKEN=123456789:ABCdefGhIjKlmNoPQRstuVwXyZ
```

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

### Add sites first

```
/addsite https://www.example-movies.com
/addsite https://another-site.com
```

### Search for a title

```
/search Inception
/search Breaking Bad
/search The Dark Knight
```

The bot will return one direct link per site where it found a match.

### View and manage your sites

```
/listsites
/removesite https://www.example-movies.com
```

---

## How the search works

For each saved site, the bot tries common search URL patterns such as:

- `/?s=title`
- `/search?q=title`
- `/search/title/`
- `/?q=title`
- and more…

It then parses the resulting page, scores all links by how well they match the title (by slug, text, word overlap), and returns the best match. Links with low confidence are discarded.

---

## Notes

- Sites must have a publicly accessible search page (no login required)
- The bot searches sites in parallel for speed
- `sites.json` is created automatically in the same folder as `bot.py`
- You can keep the bot running 24/7 on a VPS, Raspberry Pi, or any server
