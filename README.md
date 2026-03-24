# tg-curator-bot

Minimal Telegram curator bot:
- Reads source chats with a user session (Pyrogram / MTProto)
- Posts curated messages to destination groups (Bot API)
- Managed from bot DM only (owner-only)

## What it supports

- Source management per destination group
- Group and source filters (allowlist / blocklist)
- Reapply filters to already forwarded messages (with delete)
- Clean forwarding history (all sources or one source)
- Per-group display settings (header and original link)

## Requirements

- Python 3.11+
- Bot token from BotFather
- Telegram API ID and API Hash from https://my.telegram.org

## Quick start (local)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python .\main.py
```

If required values are missing or invalid, startup resets core config in data/data.json and runs terminal onboarding again.

## Quick start (Docker)

```bash
docker compose up --build
```

Keep the first run attached so onboarding questions can appear if credentials are missing.

## Day-to-day usage

1. Add the bot to a destination group and allow it to post.
2. Open DM with the bot and send /start.
3. Configure user session from terminal only (onboarding or python .\generate_session.py).
4. Open Destinations and add or manage sources.
5. Configure Filters, Settings, or Clean History.

## UI consistency checks

Run lightweight regression checks for menu labels and source-list formatting logic:

```powershell
python -m unittest -q tests/test_ui_consistency.py
```

## Session string helper

If Telegram rejects OTP codes entered in chat, generate locally:

```powershell
python .\generate_session.py
```

This stores api_id, api_hash, and session_string in data/data.json.

## Data persistence

Main bot state is stored in data/data.json.
Forwarded-message history is stored separately in data/forward_logs.json.
