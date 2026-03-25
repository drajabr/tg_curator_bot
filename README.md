# tg-curator-bot

Telegram curator bot for owner-only management from DM.

## Capabilities

- Route posts from source chats to destination groups.
- Manage sources per destination.
- Apply group-level and source-level filters.
- Reapply filters to already forwarded messages (and delete mismatches).
- Clean forwarding history (all or by source).
- Per-destination display options (source header, original link, source datetime).
- Bulk source import and optional auto-sync from joined chats.
- Owner DM heartbeat: one pinned status message refreshed every 60 seconds (editable, non-spammy) with uptime and runtime stats.

## Requirements

- Python 3.11+
- Telegram bot token (BotFather)
- Telegram API ID + API Hash if you want to generate a user session locally (https://my.telegram.org)

## Setup

1. Create env file.

```powershell
Copy-Item .env.example .env
```

2. Put your bot token in `.env`.

```env
BOT_TOKEN=123456789:YOUR_BOT_TOKEN
```

## Run Locally

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python .\main.py
```

First start behavior:

- If `BOT_TOKEN` is missing, startup asks for it in the terminal and saves it to `data/data.json`.
- After the bot starts, you can upload `data.json` in the bot DM.
- If the user session is missing or invalid, startup offers guided terminal session generation and reconnects automatically.

## Run with Docker

`docker-compose.yml` pulls `ghcr.io/drajabr/tg_curator_bot:latest` by default.

**First run**:

```bash
docker compose pull
docker compose run --rm tg-curator-bot
```

`docker compose run` attaches your terminal so first-run prompts work.

What happens on first run:

- If `BOT_TOKEN` is missing, you will be prompted for it first.
- The bot then starts, so you can open its DM and upload `data.json` if you already have one.
- If the user session is missing or invalid, the terminal offers built-in session generation.
- If you accept, the app generates the session and reconnects the user client without a manual restart.

**Normal start**:

```bash
docker compose pull
docker compose up -d
```

## API + Session (Important)

The bot needs a Telegram user session to read source chats.

Session setup is handled by onboarding in `main.py`:

- Upload an existing `data.json` in bot DM, or
- Accept the terminal session-generation prompt when startup detects a missing/invalid session.

In both cases, resulting values are stored in `data/data.json`.

## Onboarding Flow

1. Start the bot.
2. If prompted, enter the bot token in terminal.
3. Open bot DM and send `/start`.
4. Upload `data.json`, or accept terminal session generation when prompted at startup.
5. Add bot to destination group(s) with send permission.
6. Configure destinations, sources, filters, and settings.

## Data Files

- `data/data.json`: bot config, owner, destinations, filters, session.
- `data/forward_logs.json`: forwarded-message history.
