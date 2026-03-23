# tg-curator-bot

Telegram feed bot with a hybrid architecture:
- Bot account over Bot API (token-only startup) for UI and posting to destination groups.
- User session over MTProto for reading source channels/groups/topics, including restricted forward sources.

The project is designed for low complexity:
- Single process
- Single JSON state file (`data/data.json`)
- Inline keyboard driven UI
- Token-only startup environment (`BOT_TOKEN`)

## Features

- Owner-only management.
- DM admin panel:
  - Set up user session (`api_id`, `api_hash`, `session_string`).
  - See active groups and source count.
- Group usage panel:
  - Add source from forwarded message, link, or chat ID.
  - Remove source.
  - List sources.
  - Filters menu.
  - Settings menu.
- Source add confirmation:
  - Pulls last 1 source message and reposts it as confirmation.
- Forwarding behavior:
  - Event-driven (push updates from Telegram MTProto), no polling.
  - Re-upload style posting so destination messages look uniform.
  - Header: bold+italic source name and chat ID.
  - Footer: original message link when available.
- Filters:
  - Group-wide filters (applies to all sources in one destination group).
  - Per-source filters.
  - Modes: `blocklist` or `allowlist`.
  - Rule types: keyword, exact message, message type, sender, has-link.
  - Reply-to-forwarded-message quick actions:
    - Block exact text
    - Block sender
    - Extract keywords
- Per-group settings:
  - Show/hide header
  - Show/hide original link footer

## Requirements

- Python 3.11+
- Telegram bot token from BotFather
- User session credentials for source reading (configured in bot DM menu):
  - `api_id` and `api_hash` from `my.telegram.org`
  - `session_string` for your Telegram user account (or login by phone/code)
- `tgcrypto` for MTProto speedup (required by `requirements.txt`)

## How to get API ID and API Hash

You need Telegram API app credentials for user-session setup.

1. Open `https://my.telegram.org`.
2. Log in with the phone number of your Telegram account.
3. Open `API development tools`.
4. Create a new application (any app title/short name is fine).
5. Copy:
  - `api_id`
  - `api_hash`

Use these values later in bot DM during `Set Up User Session`.

## Run with Docker Compose

1. Copy env template:

```bash
cp .env.example .env
```

2. Edit `.env` and set:
  - `BOT_TOKEN`

3. Start:

```bash
docker compose up --build -d
```

4. Open bot DM and send `/start`.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
# edit .env and set BOT_TOKEN
python main.py
```

If installing `tgcrypto` fails on Windows, install Microsoft C++ Build Tools and retry:

```powershell
winget install Microsoft.VisualStudio.2022.BuildTools
```

Then install the C++ workload (Desktop development with C++) from the installer and rerun:

```powershell
pip install -r requirements.txt
```

Note: Python 3.14 may have compatibility issues with some compiled packages. Python 3.11-3.13 is recommended for the smoothest setup.

## Setup flow

1. DM bot with `/start`.
2. Click `Set Up User Session`.
3. Send values in order:
  - API ID
  - API Hash
  - Phone number (or paste Session String directly)
  - OTP / 2FA password when prompted
4. Bot starts user session client.

## Group flow

1. Add the bot to your destination group.
2. In that group, owner sends `/menu`.
3. Use buttons:
   - Add Source
   - Remove Source
   - Source List
   - Filters
   - Settings

## Supported source input

- Forwarded message from source.
- `t.me` link (public/private/invite when join is possible for the user account).
- Numeric chat ID.

## State file

All persistent state is in `data/data.json`.

Top-level model:
- `owner_id`
- `user_session`
- `groups`

Each group stores:
- `settings`
- `group_filters`
- `sources`
- `forward_log`

## CI

GitHub Actions workflow builds Docker image on push/PR:
- `.github/workflows/docker-image.yml`

## Notes

- This project intentionally avoids DB and keeps logic compact.
- Flood wait is handled by retrying after Telegram-specified delay.
- Source reading requires a valid user session; Bot API alone cannot read all restricted/private sources.
- If user-session login fails with API key/authorization errors, make sure the API ID/API Hash entered in DM are valid.
