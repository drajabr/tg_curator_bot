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

## Requirements

- Python 3.11+
- Telegram bot token (BotFather)
- Telegram API ID + API Hash (https://my.telegram.org)

## Setup

1. Create env file.

```powershell
Copy-Item .env.example .env
```

2. Put your bot token in `.env`.

```env
BOT_TOKEN=123456789:YOUR_BOT_TOKEN
```

3. Session options:
- Option A (recommended): run onboarding/session generator locally and store values in `data/data.json`.
- Option B: provide `BOT_API_ID`, `BOT_API_HASH`, and `USER_SESSION_STRING` via environment.

## Run Locally

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python .\main.py
```

If required config is missing, startup runs console onboarding.

## Run with Docker

`docker-compose.yml` is configured to use a prebuilt image (not local `build: .`).
Default image:

`ghcr.io/ahmed/tg_curator_bot:latest`

If your repository owner/name is different, set `IMAGE_NAME` in `.env`:

```env
IMAGE_NAME=ghcr.io/<owner>/<repo>:latest
```

```bash
docker compose pull
docker compose up -d
```

First run can be attached (`docker compose up`) if onboarding prompts are expected.

## Build and Publish Image (GitHub Actions)

Workflow file: `.github/workflows/docker-image.yml`.

- On push to `main`/`master`, CI builds and pushes to GitHub Container Registry (GHCR).
- On tag push (`v*`), CI also publishes version tags.
- On pull requests, CI builds only (no push).
- Published tags include `latest` (default branch), short commit SHA, and Git tag refs.

After merging to default branch, deploy with:

```bash
docker compose pull
docker compose up -d
```

## API + Session (Important)

The bot needs a Telegram user session to read source chats.

Generate or refresh session from terminal:

```powershell
python .\generate_session.py
```

This stores `api_id`, `api_hash`, and `session_string` in `data/data.json`.

## Onboarding Flow

1. Start the bot.
2. Add bot to destination group(s) with send permission.
3. Open bot DM and send `/start`.
4. In DM, open Destinations and configure sources.
5. Configure Filters / Settings / History cleanup as needed.

## Data Files

- `data/data.json`: bot config, owner, destinations, filters, session.
- `data/forward_logs.json`: forwarded-message history.
