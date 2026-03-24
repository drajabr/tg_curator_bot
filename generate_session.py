import asyncio
import getpass
import os
from pathlib import Path
from typing import Optional

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded

from tg_curator_bot.storage import Storage


def remove_temp_session_files(session_name: str) -> None:
    for suffix in (".session", ".session-journal"):
        path = Path("data") / f"{session_name}{suffix}"
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass


def ask(prompt: str, default: Optional[str] = None, secret: bool = False) -> str:
    label = f"{prompt} [{default}]: " if default else f"{prompt}: "
    value = getpass.getpass(label) if secret else input(label)
    value = value.strip()
    if value:
        return value
    return default or ""


async def generate_session() -> None:
    load_dotenv()
    storage = Storage("data/data.json")
    state = await storage.read()
    saved = state.get("user_session", {})

    env_api_id = os.getenv("BOT_API_ID", "").strip() or os.getenv("API_ID", "").strip()
    env_api_hash = os.getenv("BOT_API_HASH", "").strip() or os.getenv("API_HASH", "").strip()

    default_api_id = str(saved.get("api_id") or env_api_id or "")
    default_api_hash = str(saved.get("api_hash") or env_api_hash or "")

    print("Telegram user session generator")
    print("This runs the full login locally so the OTP is not shared through the bot chat.")

    raw_api_id = ask("API ID", default_api_id)
    while True:
        try:
            api_id = int(raw_api_id)
            break
        except ValueError:
            raw_api_id = ask("API ID")

    api_hash = ask("API Hash", default_api_hash)
    while not api_hash:
        api_hash = ask("API Hash")

    phone = ask("Phone number in international format")
    while not phone:
        phone = ask("Phone number in international format")

    session_name = "auth_cli_temp"
    remove_temp_session_files(session_name)
    client = Client(session_name, api_id=api_id, api_hash=api_hash, workdir="data")

    try:
        await client.connect()
        sent = await client.send_code(phone)
        phone_code_hash = sent.phone_code_hash

        while True:
            code = ask("Login code (or type resend)")
            if code.lower() == "resend":
                sent = await client.resend_code(phone, phone_code_hash)
                phone_code_hash = sent.phone_code_hash
                print("A new code was requested. Enter the latest code.")
                continue

            otp = "".join(ch for ch in code if ch.isdigit())
            if not otp:
                print("Enter the numeric code from Telegram.")
                continue

            try:
                await client.sign_in(phone, phone_code_hash, otp)
                break
            except SessionPasswordNeeded:
                password = ask("Two-step password", secret=True)
                await client.check_password(password)
                break
            except Exception as exc:
                error_text = str(exc)
                if "PHONE_CODE_EXPIRED" in error_text:
                    sent = await client.resend_code(phone, phone_code_hash)
                    phone_code_hash = sent.phone_code_hash
                    print("That code expired. A fresh code was requested.")
                    continue
                if "PHONE_CODE_INVALID" in error_text:
                    print("Invalid code. Enter the latest code or type resend.")
                    continue
                raise

        session_string = await client.export_session_string()

        def updater(data):
            user_session = data.setdefault("user_session", {})
            user_session["api_id"] = api_id
            user_session["api_hash"] = api_hash
            user_session["session_string"] = session_string
            data["user_session"] = user_session
            return data

        await storage.update(updater)
        print("\nSession saved to data/data.json")
        print("Paste backup copy if needed:\n")
        print(session_string)
    finally:
        if getattr(client, "is_connected", False):
            await client.disconnect()
        remove_temp_session_files(session_name)


if __name__ == "__main__":
    asyncio.run(generate_session())