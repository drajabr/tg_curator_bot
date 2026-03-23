import asyncio
import logging
import os
import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded
from pyrogram.handlers import MessageHandler as PyroMessageHandler
from pyrogram.types import Message
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import RetryAfter
from telegram.ext import Application, CallbackQueryHandler, ContextTypes, MessageHandler, filters

from .filters import evaluate_filters
from .formatting import compose_caption_payload, compose_text_payload, original_message_link, source_header
from .keyboards import (
    add_rule_types,
    dm_admin_menu,
    filters_root,
    group_main_menu,
    message_filter_quick_actions,
    rules_menu,
)
from .storage import Storage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("tg-curator-bot")


def default_group_state() -> Dict[str, Any]:
    return {
        "settings": {
            "show_header": True,
            "show_link": True,
        },
        "group_filters": {
            "mode": "blocklist",
            "rules": [],
        },
        "sources": {},
        "forward_log": {},
    }


def source_key(chat_id: int, topic_id: Optional[int]) -> str:
    return f"{chat_id}|{topic_id or 0}"


def parse_source_key(key: str) -> Tuple[int, Optional[int]]:
    chat_raw, topic_raw = key.split("|", 1)
    chat_id = int(chat_raw)
    topic_id = int(topic_raw)
    return chat_id, (None if topic_id == 0 else topic_id)


class TelegramFeedBot:
    def __init__(self) -> None:
        load_dotenv()
        self.bot_token = os.getenv("BOT_TOKEN", "").strip()
        self.env_api_id = os.getenv("BOT_API_ID", "").strip() or os.getenv("API_ID", "").strip()
        self.env_api_hash = os.getenv("BOT_API_HASH", "").strip() or os.getenv("API_HASH", "").strip()
        self.env_session_string = (
            os.getenv("USER_SESSION_STRING", "").strip()
            or os.getenv("SESSION_STRING", "").strip()
            or os.getenv("PYROGRAM_SESSION_STRING", "").strip()
        )
        if not self.bot_token:
            raise RuntimeError("BOT_TOKEN is required")

        self.data_path = os.getenv("DATA_PATH", "data/data.json")
        self.storage = Storage(self.data_path)

        self.application = Application.builder().token(self.bot_token).build()
        self.bot = self.application.bot
        self.user_client: Optional[Client] = None
        self.bot_id: Optional[int] = None
        self.bot_username: str = ""

        self.pending_inputs: Dict[int, Dict[str, Any]] = {}
        self.pending_locks: Dict[int, asyncio.Lock] = {}

        self.application.add_handler(MessageHandler(filters.ChatType.PRIVATE, self._on_private_update))
        self.application.add_handler(MessageHandler(filters.ChatType.GROUPS, self._on_group_update))
        self.application.add_handler(CallbackQueryHandler(self._on_callback_query_update))

    async def _on_private_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        await self.on_private_message(None, message)

    async def _on_group_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        await self.on_group_message(None, message)

    async def _on_callback_query_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        callback_query = update.callback_query
        if callback_query is None:
            return
        await self.on_callback_query(None, callback_query)

    async def _state(self) -> Dict[str, Any]:
        return await self.storage.read()

    async def _is_owner(self, user_id: Optional[int]) -> bool:
        if user_id is None:
            return False
        state = await self._state()
        owner_id = state.get("owner_id")
        return owner_id is not None and int(owner_id) == int(user_id)

    async def _ensure_owner(self, user_id: int) -> bool:
        state = await self._state()
        if state.get("owner_id") is None:
            state["owner_id"] = user_id
            await self.storage.write(state)
            return True
        return int(state["owner_id"]) == int(user_id)

    def _is_message_mentioning_bot(self, message: Message) -> bool:
        text = message.text or message.caption or ""
        bot_username = (self.bot_username or "").lower()
        if bot_username and f"@{bot_username}" in text.lower():
            return True

        entities = message.entities or message.caption_entities or []
        for entity in entities:
            entity_type = str(getattr(entity, "type", "")).lower()
            if "text_mention" in entity_type:
                mentioned_user = getattr(entity, "user", None)
                if mentioned_user and self.bot_id is not None and int(mentioned_user.id) == int(self.bot_id):
                    return True
                continue
            if "mention" in entity_type and bot_username:
                offset = getattr(entity, "offset", 0)
                length = getattr(entity, "length", 0)
                chunk = text[offset: offset + length]
                if chunk.lower() == f"@{bot_username}":
                    return True
        return False

    def _is_menu_command_for_bot(self, message: Message) -> bool:
        if not message.text:
            return False
        first = message.text.split()[0]
        if not first.startswith("/"):
            return False

        command = first[1:]
        if "@" in command:
            command_name, target = command.split("@", 1)
            if self.bot_username and target.lower() != self.bot_username.lower():
                return False
            command = command_name
        return command in {"start", "menu"}

    def _is_group_message_addressed(self, message: Message) -> bool:
        if self._is_message_mentioning_bot(message):
            return True
        if message.reply_to_message and message.reply_to_message.from_user and self.bot_id is not None:
            return int(message.reply_to_message.from_user.id) == int(self.bot_id)
        return False

    def _forwarded_chat(self, message: Message):
        chat = getattr(message, "forward_from_chat", None)
        if chat is not None:
            return chat
        origin = getattr(message, "forward_origin", None)
        if origin is None:
            return None
        return getattr(origin, "chat", None)

    def _forwarded_user(self, message: Message):
        user = getattr(message, "forward_from", None)
        if user is not None:
            return user
        origin = getattr(message, "forward_origin", None)
        if origin is None:
            return None
        return getattr(origin, "sender_user", None)

    async def _count_groups_sources(self) -> Tuple[int, int]:
        state = await self._state()
        groups = state.get("groups", {})
        source_count = sum(len(g.get("sources", {})) for g in groups.values())
        return len(groups), source_count

    async def _dm_status_text(self) -> str:
        state = await self._state()
        owner_id = state.get("owner_id")
        sess = state.get("user_session", {})
        has_session = bool(sess.get("session_string"))
        has_api = bool(str(sess.get("api_id") or "").strip()) and bool(str(sess.get("api_hash") or "").strip())
        groups, sources = await self._count_groups_sources()

        user_client_state = "Connected" if self.user_client is not None else "Not Connected"
        user_identity = "-"
        if self.user_client is not None:
            try:
                me = await self.user_client.get_me()
                user_identity = f"@{me.username}" if me.username else me.first_name
            except Exception:
                user_identity = "Connected (identity unavailable)"

        lines = [
            "Bot Status",
            f"Owner ID: {owner_id if owner_id is not None else '-'}",
            f"User API credentials configured: {'Yes' if has_api else 'No'}",
            f"User session string configured: {'Yes' if has_session else 'No'}",
            f"User client: {user_client_state}",
            f"User account: {user_identity}",
            f"Destination groups: {groups}",
            f"Total sources: {sources}",
        ]
        return "\n".join(lines)

    async def _ensure_group_registered(self, group_id: int) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            if str(group_id) not in groups:
                groups[str(group_id)] = default_group_state()
            return state

        await self.storage.update(updater)

    async def _get_user_api_credentials(self) -> Tuple[Optional[int], Optional[str]]:
        state = await self._state()
        sess = state.get("user_session", {})
        raw_api_id = str(sess.get("api_id") or "").strip() or self.env_api_id
        api_hash = str(sess.get("api_hash") or "").strip() or self.env_api_hash
        try:
            api_id = int(raw_api_id) if raw_api_id else None
        except ValueError:
            api_id = None
        return api_id, (api_hash or None)

    async def _start_or_restart_user_client(self) -> Tuple[bool, str]:
        state = await self._state()
        sess = state.get("user_session", {})
        session_string = sess.get("session_string")
        api_id, api_hash = await self._get_user_api_credentials()

        if not api_id or not api_hash:
            return False, "User API credentials are missing. Open DM menu and set API ID/API Hash first."

        if not session_string:
            return False, "User session is incomplete"

        if self.user_client is not None:
            try:
                await self.user_client.stop()
            except Exception:
                pass
            self.user_client = None

        try:
            self.user_client = Client(
                "userbot",
                api_id=api_id,
                api_hash=api_hash,
                session_string=str(session_string),
                workdir="data",
            )
            self.user_client.add_handler(PyroMessageHandler(self.on_user_message))
            await self.user_client.start()
            me = await self.user_client.get_me()
            return True, f"Connected as @{me.username or me.first_name}"
        except Exception as exc:
            logger.exception("Failed to start user client")
            self.user_client = None
            return False, f"Failed to start user client: {exc}"

    async def _sync_env_session(self) -> None:
        """If a session string is set in .env, persist it to storage."""
        if not self.env_session_string:
            return
        state = await self._state()
        if state.get("user_session", {}).get("session_string") != self.env_session_string:
            await self._save_session_string(self.env_session_string)
            logger.info("Session string loaded from environment")

    async def _save_session_string(self, session_string: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            user_session = state.setdefault("user_session", {})
            user_session["session_string"] = session_string
            state["user_session"] = user_session
            return state

        await self.storage.update(updater)

    async def _save_user_api_credentials(self, api_id: int, api_hash: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            user_session = state.setdefault("user_session", {})
            user_session["api_id"] = int(api_id)
            user_session["api_hash"] = api_hash.strip()
            state["user_session"] = user_session
            return state

        await self.storage.update(updater)

    def _message_id(self, message: Any) -> int:
        return int(getattr(message, "id", None) or getattr(message, "message_id"))

    async def start(self) -> None:
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        me = await self.bot.get_me()
        self.bot_id = me.id
        self.bot_username = (me.username or "").lower()
        await self._sync_env_session()
        ok, status = await self._start_or_restart_user_client()
        if ok:
            logger.info(status)
        else:
            logger.info("User client not active: %s", status)
        logger.info("Bot started as @%s", me.username)

    async def stop(self) -> None:
        if self.user_client is not None:
            try:
                await self.user_client.stop()
            except Exception:
                pass
        if self.application.updater is not None:
            await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()

    async def on_private_message(self, client: Client, message: Message) -> None:
        if not message.from_user:
            return

        user_id = message.from_user.id
        is_owner = await self._ensure_owner(user_id)
        if not is_owner:
            await message.reply_text("Only the bot owner can manage this bot.")
            return

        pending = self.pending_inputs.get(user_id)
        if pending and pending.get("chat_id") == message.chat.id:
            lock = self.pending_locks.setdefault(user_id, asyncio.Lock())
            async with lock:
                latest = self.pending_inputs.get(user_id)
                if latest and latest.get("chat_id") == message.chat.id:
                    await self._handle_pending_input(message, latest)
            return

        if message.text and message.text.startswith("/"):
            groups, sources = await self._count_groups_sources()
            state = await self._state()
            sess = state.get("user_session", {})
            session_ready = bool(sess.get("session_string"))
            await message.reply_text(
                "Admin Panel",
                reply_markup=dm_admin_menu(session_ready, groups, sources),
            )

    async def on_group_message(self, client: Client, message: Message) -> None:
        if not message.from_user:
            return

        user_id = message.from_user.id
        if not await self._is_owner(user_id):
            return

        addressed = self._is_group_message_addressed(message)
        if not addressed:
            return

        pending = self.pending_inputs.get(user_id)
        if pending and pending.get("chat_id") == message.chat.id:
            lock = self.pending_locks.setdefault(user_id, asyncio.Lock())
            async with lock:
                latest = self.pending_inputs.get(user_id)
                if latest and latest.get("chat_id") == message.chat.id:
                    await self._handle_pending_input(message, latest)
            return

        # Owner replying to a forwarded bot-post opens quick filter actions.
        if message.reply_to_message:
            state = await self._state()
            group_state = state.get("groups", {}).get(str(message.chat.id), {})
            fwd_log = group_state.get("forward_log", {})
            replied_id = str(message.reply_to_message.id)
            if replied_id in fwd_log:
                await message.reply_text(
                    "Filter this forwarded message:",
                    reply_markup=message_filter_quick_actions(message.chat.id, message.reply_to_message.id),
                )
                return

        if self._is_menu_command_for_bot(message) or self._is_message_mentioning_bot(message):
            await self._ensure_group_registered(message.chat.id)
            await message.reply_text(
                "Feed Bot Menu",
                reply_markup=group_main_menu(message.chat.id),
            )
            return

    async def on_callback_query(self, client, callback_query) -> None:
        user = callback_query.from_user
        if not user:
            return

        if not await self._is_owner(user.id):
            await callback_query.answer("Only owner can use this.", show_alert=True)
            return

        data = callback_query.data or ""
        if data == "noop":
            await callback_query.answer()
            return

        if data.startswith("dm:setup_session"):
            self.pending_inputs[user.id] = {
                "kind": "setup_session",
                "step": "api_id",
                "chat_id": callback_query.message.chat.id,
            }
            await callback_query.message.reply_text(
                "Send your Telegram API ID (from my.telegram.org)."
            )
            await callback_query.answer()
            return

        if data == "dm:status":
            groups, sources = await self._count_groups_sources()
            state = await self._state()
            sess = state.get("user_session", {})
            session_ready = bool(sess.get("session_string"))
            await callback_query.message.edit_text(
                await self._dm_status_text(),
                reply_markup=dm_admin_menu(session_ready, groups, sources),
            )
            await callback_query.answer()
            return

        if data.startswith("qk:"):
            await self._handle_keyword_pick_callback(callback_query)
            return

        if data.startswith("q:"):
            await self._handle_quick_filter_callback(callback_query)
            return

        if not data.startswith("g:"):
            await callback_query.answer()
            return

        await self._handle_group_callback(callback_query)

    async def _handle_group_callback(self, callback_query) -> None:
        data = callback_query.data or ""
        parts = data.split(":")
        if len(parts) < 3:
            await callback_query.answer("Invalid action")
            return

        group_id = int(parts[1])
        action = parts[2]

        await self._ensure_group_registered(group_id)

        if action == "back_main":
            await callback_query.message.edit_text("Feed Bot Menu", reply_markup=group_main_menu(group_id))
            await callback_query.answer()
            return

        if action == "add":
            self.pending_inputs[callback_query.from_user.id] = {
                "kind": "add_source",
                "group_id": group_id,
                "chat_id": callback_query.message.chat.id,
            }
            await callback_query.message.reply_text(
                "Send one of:\n"
                "- a forwarded message from the source\n"
                "- a t.me link\n"
                "- a chat ID"
            )
            await callback_query.answer()
            return

        if action == "remove":
            await self._show_remove_source_menu(callback_query, group_id)
            return

        if action == "list":
            await self._show_sources_list(callback_query, group_id)
            return

        if action == "filters":
            await callback_query.message.edit_text("Filters", reply_markup=filters_root(group_id))
            await callback_query.answer()
            return

        if action == "settings":
            await self._show_settings_menu(callback_query, group_id)
            return

        if action == "toggleset":
            if len(parts) < 4:
                await callback_query.answer("Invalid setting")
                return
            setting = parts[3]
            await self._toggle_setting(callback_query, group_id, setting)
            return

        if action == "gf":
            if len(parts) == 3:
                await callback_query.message.edit_text(
                    "Group-Wide Filters",
                    reply_markup=rules_menu(group_id, "gf"),
                )
                await callback_query.answer()
                return

            await self._handle_rules_callback(callback_query, group_id, "gf", parts[3:])
            return

        if action == "sf":
            if len(parts) == 3:
                await self._show_source_filter_selector(callback_query, group_id)
                return
            await self._handle_rules_callback(callback_query, group_id, "sf", parts[3:])
            return

        if action == "sfsel":
            if len(parts) < 4:
                await callback_query.answer("Invalid source")
                return
            s_key = parts[3]
            await callback_query.message.edit_text(
                f"Source Filters ({s_key})",
                reply_markup=rules_menu(group_id, "sf", s_key),
            )
            await callback_query.answer()
            return

        if action == "rm":
            if len(parts) < 4:
                await callback_query.answer("Invalid source")
                return
            s_key = parts[3]
            await self._remove_source(callback_query, group_id, s_key)
            return

        await callback_query.answer()

    async def _handle_rules_callback(self, callback_query, group_id: int, scope: str, tail: List[str]) -> None:
        command = tail[0] if tail else ""
        source_k = tail[1] if len(tail) > 1 else None

        if command == "add":
            await callback_query.message.edit_text(
                "Choose filter rule type",
                reply_markup=add_rule_types(group_id, scope, source_k),
            )
            await callback_query.answer()
            return

        if command == "type":
            if len(tail) < 2:
                await callback_query.answer("Missing type")
                return
            rule_type = tail[1]
            source_k = tail[2] if len(tail) > 2 else None
            self.pending_inputs[callback_query.from_user.id] = {
                "kind": "add_rule",
                "group_id": group_id,
                "scope": scope,
                "source_key": source_k,
                "rule_type": rule_type,
                "chat_id": callback_query.message.chat.id,
            }
            prompts = {
                "keyword": "Send comma-separated keywords (example: spam,ad,promo)",
                "exact": "Send exact message text to match",
                "message_type": "Send one type: text, photo, video, document, audio, voice, animation, sticker, poll, other",
                "sender": "Send a forwarded message, sender IDs, or usernames (example: @username,123456789)",
                "has_link": "Send yes or no",
            }
            await callback_query.message.reply_text(prompts.get(rule_type, "Send rule value"))
            await callback_query.answer()
            return

        if command == "ls":
            await self._list_rules(callback_query, group_id, scope, source_k)
            return

        if command == "rm":
            await self._show_remove_rule_buttons(callback_query, group_id, scope, source_k)
            return

        if command == "del":
            if len(tail) < 2:
                await callback_query.answer("Invalid index")
                return
            index = int(tail[1])
            source_k = tail[2] if len(tail) > 2 else None
            await self._remove_rule(callback_query, group_id, scope, source_k, index)
            return

        if command == "mode":
            await self._switch_filter_mode(callback_query, group_id, scope, source_k)
            return

        await callback_query.answer()

    async def _show_settings_menu(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        settings = g.get("settings", {})
        hdr = "ON" if settings.get("show_header", True) else "OFF"
        lnk = "ON" if settings.get("show_link", True) else "OFF"

        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(f"Header: {hdr}", callback_data=f"g:{group_id}:toggleset:show_header")],
                [InlineKeyboardButton(f"Original Link: {lnk}", callback_data=f"g:{group_id}:toggleset:show_link")],
                [InlineKeyboardButton("Back", callback_data=f"g:{group_id}:back_main")],
            ]
        )
        await callback_query.message.edit_text("Group Settings", reply_markup=kb)
        await callback_query.answer()

    async def _toggle_setting(self, callback_query, group_id: int, setting: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            g = groups.setdefault(str(group_id), default_group_state())
            settings = g.setdefault("settings", {"show_header": True, "show_link": True})
            current = bool(settings.get(setting, True))
            settings[setting] = not current
            return state

        await self.storage.update(updater)
        await self._show_settings_menu(callback_query, group_id)

    async def _show_sources_list(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.message.edit_text("No sources configured for this group.")
            await callback_query.answer()
            return

        lines = ["Sources:"]
        for key, src in sources.items():
            mode = src.get("filters", {}).get("mode", "blocklist")
            rules = len(src.get("filters", {}).get("rules", []))
            lines.append(f"- {src.get('name', 'Unknown')} | {key} | {mode} ({rules} rules)")

        await callback_query.message.edit_text("\n".join(lines))
        await callback_query.answer()

    async def _show_remove_source_menu(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.answer("No sources", show_alert=True)
            return

        buttons = []
        for key, src in sources.items():
            label = src.get("name", key)[:48]
            buttons.append([InlineKeyboardButton(f"Remove {label}", callback_data=f"g:{group_id}:rm:{key}")])
        buttons.append([InlineKeyboardButton("Back", callback_data=f"g:{group_id}:back_main")])

        await callback_query.message.edit_text("Select source to remove:", reply_markup=InlineKeyboardMarkup(buttons))
        await callback_query.answer()

    async def _remove_source(self, callback_query, group_id: int, source_k: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            g = groups.setdefault(str(group_id), default_group_state())
            g.setdefault("sources", {}).pop(source_k, None)
            return state

        await self.storage.update(updater)
        await callback_query.message.edit_text(f"Removed source {source_k}")
        await callback_query.answer("Removed")

    async def _show_source_filter_selector(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.answer("No sources", show_alert=True)
            return

        buttons = []
        for key, src in sources.items():
            label = src.get("name", key)[:48]
            buttons.append([InlineKeyboardButton(label, callback_data=f"g:{group_id}:sfsel:{key}")])
        buttons.append([InlineKeyboardButton("Back", callback_data=f"g:{group_id}:filters")])
        await callback_query.message.edit_text("Select source:", reply_markup=InlineKeyboardMarkup(buttons))
        await callback_query.answer()

    async def _filter_target(self, state: Dict[str, Any], group_id: int, scope: str, source_k: Optional[str]) -> Dict[str, Any]:
        g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
        if scope == "gf":
            return g.setdefault("group_filters", {"mode": "blocklist", "rules": []})
        if not source_k:
            return {"mode": "blocklist", "rules": []}
        src = g.setdefault("sources", {}).setdefault(source_k, {})
        return src.setdefault("filters", {"mode": "blocklist", "rules": []})

    async def _list_rules(self, callback_query, group_id: int, scope: str, source_k: Optional[str]) -> None:
        state = await self._state()
        target = await self._filter_target(state, group_id, scope, source_k)
        rules = target.get("rules", [])
        mode = target.get("mode", "blocklist")
        if not rules:
            await callback_query.message.edit_text(f"Mode: {mode}\nNo rules")
            await callback_query.answer()
            return

        lines = [f"Mode: {mode}"]
        for idx, rule in enumerate(rules):
            lines.append(f"{idx}: {rule}")
        await callback_query.message.edit_text("\n".join(lines))
        await callback_query.answer()

    async def _show_remove_rule_buttons(self, callback_query, group_id: int, scope: str, source_k: Optional[str]) -> None:
        state = await self._state()
        target = await self._filter_target(state, group_id, scope, source_k)
        rules = target.get("rules", [])
        if not rules:
            await callback_query.answer("No rules", show_alert=True)
            return

        buttons = []
        for idx, rule in enumerate(rules):
            label = f"Delete #{idx} {rule.get('type', 'rule')}"[:56]
            if source_k:
                cb = f"g:{group_id}:{scope}:del:{idx}:{source_k}"
            else:
                cb = f"g:{group_id}:{scope}:del:{idx}"
            buttons.append([InlineKeyboardButton(label, callback_data=cb)])

        await callback_query.message.edit_text("Select rule to delete", reply_markup=InlineKeyboardMarkup(buttons))
        await callback_query.answer()

    async def _remove_rule(self, callback_query, group_id: int, scope: str, source_k: Optional[str], index: int) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            if scope == "gf":
                target = g.setdefault("group_filters", {"mode": "blocklist", "rules": []})
            else:
                if not source_k:
                    return state
                src = g.setdefault("sources", {}).setdefault(source_k, {})
                target = src.setdefault("filters", {"mode": "blocklist", "rules": []})
            rules = target.setdefault("rules", [])
            if 0 <= index < len(rules):
                rules.pop(index)
            return state

        await self.storage.update(updater)
        await callback_query.answer("Rule removed")
        await callback_query.message.edit_text("Rule removed.")

    async def _switch_filter_mode(self, callback_query, group_id: int, scope: str, source_k: Optional[str]) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            if scope == "gf":
                target = g.setdefault("group_filters", {"mode": "blocklist", "rules": []})
            else:
                if not source_k:
                    return state
                src = g.setdefault("sources", {}).setdefault(source_k, {})
                target = src.setdefault("filters", {"mode": "blocklist", "rules": []})
            target["mode"] = "allowlist" if target.get("mode") == "blocklist" else "blocklist"
            return state

        data = await self.storage.update(updater)
        target = await self._filter_target(data, group_id, scope, source_k)
        await callback_query.answer(f"Mode: {target.get('mode')}")
        await callback_query.message.edit_text(f"Mode switched to {target.get('mode')}")

    async def _handle_pending_input(self, message: Message, pending: Dict[str, Any]) -> None:
        kind = pending.get("kind")
        if kind == "setup_session":
            await self._handle_setup_session_input(message, pending)
            return

        if kind == "add_source":
            await self._handle_add_source_input(message, pending)
            return

        if kind == "add_rule":
            await self._handle_add_rule_input(message, pending)
            return

    async def _handle_setup_session_input(self, message: Message, pending: Dict[str, Any]) -> None:
        user_id = message.from_user.id
        step = pending.get("step")
        text = (message.text or "").strip()

        if step == "api_id":
            try:
                api_id = int(text)
            except (TypeError, ValueError):
                await message.reply_text("Invalid API ID. Send a numeric API ID.")
                return
            pending["api_id"] = api_id
            pending["step"] = "api_hash"
            await message.reply_text("Now send your Telegram API Hash.")
            return

        if step == "api_hash":
            if not text:
                await message.reply_text("API Hash cannot be empty. Send your API Hash.")
                return
            pending["api_hash"] = text
            await message.reply_text(
                "Send your phone number (international format, e.g. +1234567890).\n"
                "Or paste a full Pyrogram session string directly."
            )
            pending["step"] = "phone"
            return

        if step == "phone":
            api_id = pending.get("api_id")
            api_hash = pending.get("api_hash")
            if not api_id or not api_hash:
                self.pending_inputs.pop(user_id, None)
                await message.reply_text("API credentials are missing. Tap Set Up User Session again.")
                return

            # Allow direct paste of an exported session string to skip OTP flow.
            if len(text) > 100 and (text.startswith("BQ") or text.startswith("AQ")):
                await self._save_user_api_credentials(int(api_id), str(api_hash))
                await self._save_session_string(text)
                self.pending_inputs.pop(user_id, None)
                ok, status = await self._start_or_restart_user_client()
                if ok:
                    await message.reply_text(f"✅ Session saved and connected. {status}")
                else:
                    await message.reply_text(f"Session saved, but user client failed: {status}")
                return

            phone = text
            auth_client = Client(
                "auth_temp",
                api_id=int(api_id),
                api_hash=str(api_hash),
                in_memory=True,
            )
            try:
                await auth_client.connect()
                sent = await auth_client.send_code(phone)
            except Exception as exc:
                try:
                    await auth_client.disconnect()
                except Exception:
                    pass
                await message.reply_text(f"Failed to send code: {exc}\nTry again — send your phone number:")
                return
            pending["phone"] = phone
            pending["phone_code_hash"] = sent.phone_code_hash
            pending["code_sent_at"] = time.monotonic()
            pending["code_attempts"] = 0
            pending["auth_client"] = auth_client
            pending["step"] = "code"
            logger.info(
                "Auth send_code success | user_id=%s | phone=%s | code_type=%s | timeout=%s",
                user_id,
                phone,
                getattr(sent, "type", None),
                getattr(sent, "timeout", None),
            )
            await message.reply_text("A code was sent to your Telegram account. Send it here:")
            return

        if step == "code":
            auth_client: Client = pending["auth_client"]
            phone = pending["phone"]
            phone_code_hash = pending["phone_code_hash"]
            pending["code_attempts"] = int(pending.get("code_attempts", 0)) + 1
            otp = re.sub(r"\D", "", text)
            elapsed = None
            if pending.get("code_sent_at") is not None:
                elapsed = round(time.monotonic() - float(pending["code_sent_at"]), 2)
            if not otp:
                await message.reply_text("Send the numeric confirmation code you received.")
                return
            logger.info(
                "Auth sign_in attempt | user_id=%s | phone=%s | otp_len=%s | elapsed_since_code=%s",
                user_id,
                phone,
                len(otp),
                elapsed,
            )
            try:
                await auth_client.sign_in(phone, phone_code_hash, otp)
            except SessionPasswordNeeded:
                pending["step"] = "password"
                await message.reply_text("Two-step verification is enabled. Send your 2FA password:")
                return
            except Exception as exc:
                err = str(exc)
                logger.warning("Auth sign_in failed | user_id=%s | error=%s", user_id, err)
                if "PHONE_CODE_EXPIRED" in err:
                    await message.reply_text(
                        "This code request is already expired on Telegram side.\n"
                        "Send your phone number again to request a fresh code, then use only the latest code."
                    )
                    try:
                        await auth_client.disconnect()
                    except Exception:
                        pass
                    pending.pop("auth_client", None)
                    pending.pop("phone_code_hash", None)
                    pending.pop("code_sent_at", None)
                    pending["step"] = "phone"
                    return
                if "PHONE_CODE_INVALID" in err:
                    await message.reply_text("Invalid code. Please send the latest code from Telegram.")
                    return
                try:
                    await auth_client.disconnect()
                except Exception:
                    pass
                self.pending_inputs.pop(user_id, None)
                await message.reply_text(f"Sign-in failed: {exc}")
                return
            await self._finish_auth(message, user_id, auth_client)
            return

        if step == "password":
            auth_client: Client = pending["auth_client"]
            try:
                await auth_client.check_password(text)
            except Exception as exc:
                try:
                    await auth_client.disconnect()
                except Exception:
                    pass
                self.pending_inputs.pop(user_id, None)
                await message.reply_text(f"2FA failed: {exc}")
                return
            await self._finish_auth(message, user_id, auth_client)
            return

    async def _finish_auth(self, message: Message, user_id: int, auth_client: Client) -> None:
        pending = self.pending_inputs.get(user_id, {})
        api_id = pending.get("api_id")
        api_hash = pending.get("api_hash")

        try:
            session_string = await auth_client.export_session_string()
        except Exception as exc:
            await message.reply_text(f"Failed to export session: {exc}")
            return
        finally:
            try:
                await auth_client.disconnect()
            except Exception:
                pass

        if api_id and api_hash:
            await self._save_user_api_credentials(int(api_id), str(api_hash))
        await self._save_session_string(session_string)
        self.pending_inputs.pop(user_id, None)
        ok, status = await self._start_or_restart_user_client()
        if ok:
            await message.reply_text(f"✅ Session saved and connected. {status}")
        else:
            await message.reply_text(f"Session saved, but user client failed: {status}")

    async def _resolve_source_from_message(self, message: Message) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if self.user_client is None:
            return None, "User session is not ready. Set it up in DM first."

        forwarded_chat = self._forwarded_chat(message)
        if forwarded_chat:
            c = forwarded_chat
            return {
                "chat_id": c.id,
                "topic_id": None,
                "name": c.title or c.username or str(c.id),
                "username": c.username,
                "type": str(c.type),
            }, None

        text = (message.text or "").strip()
        if not text:
            return None, "Send a forwarded message, link, or chat ID."

        # Raw chat ID
        try:
            chat_id = int(text)
            chat = await self.user_client.get_chat(chat_id)
            return {
                "chat_id": chat.id,
                "topic_id": None,
                "name": chat.title or chat.username or str(chat.id),
                "username": chat.username,
                "type": str(chat.type),
            }, None
        except ValueError:
            pass
        except Exception as exc:
            return None, f"Could not resolve chat ID: {exc}"

        # Link formats
        link_match = re.search(r"https?://t\.me/[^\s]+|t\.me/[^\s]+", text, flags=re.IGNORECASE)
        if not link_match:
            return None, "No valid t.me link found."

        link = link_match.group(0)
        if not link.startswith("http"):
            link = "https://" + link

        try:
            # Private invite
            if "+" in link or "joinchat" in link:
                chat = await self.user_client.join_chat(link)
                return {
                    "chat_id": chat.id,
                    "topic_id": None,
                    "name": chat.title or chat.username or str(chat.id),
                    "username": chat.username,
                    "type": str(chat.type),
                }, None

            # Public or private permalink
            path = link.split("t.me/", 1)[1]
            path = path.split("?", 1)[0].strip("/")
            parts = path.split("/")

            # t.me/c/<internal>/<msg> or t.me/c/<internal>/<topic>/<msg>
            if parts and parts[0] == "c" and len(parts) >= 3:
                internal = int(parts[1])
                chat_id = int(f"-100{internal}")
                topic_id = int(parts[2]) if len(parts) >= 4 else None
                chat = await self.user_client.get_chat(chat_id)
                return {
                    "chat_id": chat.id,
                    "topic_id": topic_id,
                    "name": chat.title or chat.username or str(chat.id),
                    "username": chat.username,
                    "type": str(chat.type),
                }, None

            # t.me/<username> or t.me/<username>/<msg> or t.me/<username>/<topic>/<msg>
            username = parts[0]
            topic_id = int(parts[1]) if len(parts) >= 3 and parts[1].isdigit() else None
            chat = await self.user_client.get_chat(username)
            return {
                "chat_id": chat.id,
                "topic_id": topic_id,
                "name": chat.title or chat.username or str(chat.id),
                "username": chat.username,
                "type": str(chat.type),
            }, None
        except Exception as exc:
            return None, f"Failed to resolve link: {exc}"

    async def _handle_add_source_input(self, message: Message, pending: Dict[str, Any]) -> None:
        user_id = message.from_user.id
        group_id = int(pending["group_id"])

        source, err = await self._resolve_source_from_message(message)
        if err:
            await message.reply_text(err)
            return

        s_key = source_key(source["chat_id"], source.get("topic_id"))

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            g = groups.setdefault(str(group_id), default_group_state())
            sources = g.setdefault("sources", {})
            sources[s_key] = {
                "chat_id": source["chat_id"],
                "topic_id": source.get("topic_id"),
                "name": source.get("name"),
                "username": source.get("username"),
                "type": source.get("type"),
                "filters": {"mode": "blocklist", "rules": []},
            }
            return state

        await self.storage.update(updater)
        self.pending_inputs.pop(user_id, None)

        await message.reply_text(f"Added source: {source['name']} ({s_key})")

        # Pull last 1 message as confirmation.
        if self.user_client:
            try:
                async for m in self.user_client.get_chat_history(source["chat_id"], limit=1):
                    await self._forward_message_to_group(group_id, s_key, m, apply_filters=False)
            except Exception as exc:
                await message.reply_text(f"Source added but could not fetch confirmation message: {exc}")

    async def _handle_add_rule_input(self, message: Message, pending: Dict[str, Any]) -> None:
        user_id = message.from_user.id
        group_id = int(pending["group_id"])
        scope = str(pending["scope"])
        source_k = pending.get("source_key")
        rule_type = str(pending["rule_type"])
        text = (message.text or "").strip()

        if not text and rule_type != "sender":
            await message.reply_text("Please send a value.")
            return

        rule = None
        if rule_type == "keyword":
            values = [x.strip() for x in text.split(",") if x.strip()]
            if not values:
                await message.reply_text("No valid keywords provided.")
                return
            rule = {"type": "keyword", "values": values}

        elif rule_type == "exact":
            rule = {"type": "exact", "value": text}

        elif rule_type == "message_type":
            value = text.lower()
            valid = {"text", "photo", "video", "document", "audio", "voice", "animation", "sticker", "poll", "other"}
            if value not in valid:
                await message.reply_text("Invalid message type.")
                return
            rule = {"type": "message_type", "value": value}

        elif rule_type == "sender":
            vals = []
            fwd_user = self._forwarded_user(message)
            fwd_chat = self._forwarded_chat(message)
            if fwd_user and getattr(fwd_user, "id", None):
                vals.append(int(fwd_user.id))
            if fwd_chat and getattr(fwd_chat, "id", None):
                vals.append(int(fwd_chat.id))

            for item in (text.split(",") if text else []):
                item = item.strip()
                if not item:
                    continue
                try:
                    vals.append(int(item))
                except ValueError:
                    username = item.lstrip("@").strip()
                    if not username:
                        await message.reply_text(f"Invalid sender value: {item}")
                        return
                    if self.user_client is None:
                        await message.reply_text("User session is not ready. Use numeric sender IDs or reconnect user session.")
                        return
                    try:
                        chat = await self.user_client.get_chat(username)
                    except Exception as exc:
                        await message.reply_text(f"Could not resolve username {item}: {exc}")
                        return
                    vals.append(int(chat.id))

            vals = list(dict.fromkeys(vals))
            if not vals:
                await message.reply_text("No sender value provided. Forward a message or send sender IDs/usernames.")
                return
            rule = {"type": "sender", "values": vals}

        elif rule_type == "has_link":
            value = text.lower() in {"yes", "y", "true", "1"}
            rule = {"type": "has_link", "value": value}

        if rule is None:
            await message.reply_text("Unsupported rule type.")
            return

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            if scope == "gf":
                target = g.setdefault("group_filters", {"mode": "blocklist", "rules": []})
            else:
                if not source_k:
                    return state
                src = g.setdefault("sources", {}).setdefault(source_k, {})
                target = src.setdefault("filters", {"mode": "blocklist", "rules": []})
            target.setdefault("rules", []).append(rule)
            return state

        await self.storage.update(updater)
        self.pending_inputs.pop(user_id, None)
        await message.reply_text("Rule added.")

    async def _handle_quick_filter_callback(self, callback_query) -> None:
        parts = (callback_query.data or "").split(":")
        if len(parts) < 4:
            await callback_query.answer("Invalid quick action")
            return

        group_id = int(parts[1])
        destination_message_id = parts[2]
        action = parts[3]

        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        entry = g.get("forward_log", {}).get(str(destination_message_id))
        if not entry:
            await callback_query.answer("No metadata found", show_alert=True)
            return

        source_k = entry.get("source_key")
        source_text = entry.get("text", "")
        sender_id = entry.get("sender_id")

        if action == "exact":
            if not source_text:
                await callback_query.answer("No text/caption for exact rule", show_alert=True)
                return
            rule = {"type": "exact", "value": source_text}
            await self._append_source_rule(group_id, source_k, rule)
            await callback_query.answer("Exact text blocked")
            return

        if action == "sender":
            if sender_id is None:
                await callback_query.answer("No sender metadata", show_alert=True)
                return
            rule = {"type": "sender", "values": [int(sender_id)]}
            await self._append_source_rule(group_id, source_k, rule)
            await callback_query.answer("Sender blocked")
            return

        if action == "keywords":
            words = self._extract_keywords(source_text)
            if not words:
                await callback_query.answer("No keywords found", show_alert=True)
                return
            kb = []
            for w in words[:10]:
                kb.append([InlineKeyboardButton(w, callback_data=f"qk:{group_id}:{destination_message_id}:{w}")])
            await callback_query.message.reply_text("Pick a keyword to block:", reply_markup=InlineKeyboardMarkup(kb))
            await callback_query.answer()
            return

    async def _handle_keyword_pick_callback(self, callback_query) -> None:
        parts = (callback_query.data or "").split(":", 3)
        if len(parts) < 4:
            await callback_query.answer("Invalid keyword action")
            return

        group_id = int(parts[1])
        destination_message_id = parts[2]
        keyword = parts[3].strip()
        if not keyword:
            await callback_query.answer("Keyword missing")
            return

        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        entry = g.get("forward_log", {}).get(str(destination_message_id))
        if not entry:
            await callback_query.answer("No metadata found", show_alert=True)
            return

        source_k = entry.get("source_key")
        if not source_k:
            await callback_query.answer("No source found", show_alert=True)
            return

        await self._append_source_rule(group_id, source_k, {"type": "keyword", "values": [keyword]})
        await callback_query.answer(f"Blocked keyword: {keyword}")

    async def _append_source_rule(self, group_id: int, source_k: str, rule: Dict[str, Any]) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            src = g.setdefault("sources", {}).setdefault(source_k, {})
            target = src.setdefault("filters", {"mode": "blocklist", "rules": []})
            target.setdefault("rules", []).append(rule)
            return state

        await self.storage.update(updater)

    def _extract_keywords(self, text: str) -> List[str]:
        if not text:
            return []
        tokens = re.findall(r"[A-Za-z0-9_]{4,}", text.lower())
        seen = set()
        out = []
        stop = {"this", "that", "with", "from", "http", "https", "there", "have", "will", "your"}
        for token in tokens:
            if token in stop:
                continue
            if token not in seen:
                seen.add(token)
                out.append(token)
        return out

    async def on_user_message(self, client: Client, message: Message) -> None:
        state = await self._state()
        groups = state.get("groups", {})
        if not groups:
            return

        matched_targets: List[Tuple[int, str]] = []
        msg_chat_id = message.chat.id
        msg_thread_id = message.message_thread_id

        for gid_raw, gdata in groups.items():
            gid = int(gid_raw)
            for s_key, src in gdata.get("sources", {}).items():
                src_chat_id = int(src.get("chat_id"))
                src_topic_id = src.get("topic_id")
                if msg_chat_id != src_chat_id:
                    continue
                if src_topic_id is not None and msg_thread_id != src_topic_id:
                    continue
                if gid == msg_chat_id:
                    continue
                matched_targets.append((gid, s_key))

        if not matched_targets:
            return

        # Group by destination group for filter checks and send.
        grouped = defaultdict(list)
        for gid, s_key in matched_targets:
            grouped[gid].append(s_key)

        for gid, keys in grouped.items():
            for s_key in keys:
                await self._forward_message_to_group(gid, s_key, message, apply_filters=True)

    async def _forward_message_to_group(self, group_id: int, s_key: str, message: Message, apply_filters: bool) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id))
        if not g:
            return
        src = g.get("sources", {}).get(s_key)
        if not src:
            return

        if apply_filters:
            if not evaluate_filters(g.get("group_filters", {"mode": "blocklist", "rules": []}), message):
                return
            if not evaluate_filters(src.get("filters", {"mode": "blocklist", "rules": []}), message):
                return

        settings = g.get("settings", {})
        show_header = bool(settings.get("show_header", True))
        show_link = bool(settings.get("show_link", True))

        header = source_header(src.get("name", "Unknown Source"), int(src.get("chat_id")))
        link = original_message_link(int(src.get("chat_id")), int(message.id), src.get("username"))
        sent_message = None

        try:
            text = message.text or ""
            caption = message.caption or ""

            if message.text:
                payload = compose_text_payload(header, text, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_message,
                    group_id,
                    payload or header,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )

            elif message.photo:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_photo,
                    group_id,
                    photo=message.photo.file_id,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.video:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_video,
                    group_id,
                    video=message.video.file_id,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.document:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_document,
                    group_id,
                    document=message.document.file_id,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.audio:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_audio,
                    group_id,
                    audio=message.audio.file_id,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.voice:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_voice,
                    group_id,
                    voice=message.voice.file_id,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.animation:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_animation,
                    group_id,
                    animation=message.animation.file_id,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.sticker:
                # Stickers do not support captions; send header/link as a text block first.
                payload = compose_text_payload(header, "", link, show_header, show_link)
                await self._safe_send(self.bot.send_message, group_id, payload or header, parse_mode=ParseMode.HTML)
                sent_message = await self._safe_send(self.bot.send_sticker, group_id, sticker=message.sticker.file_id)

            elif message.poll:
                payload = compose_text_payload(header, f"[Poll] {message.poll.question}", link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_message,
                    group_id,
                    payload or header,
                    parse_mode=ParseMode.HTML,
                )

            else:
                payload = compose_text_payload(header, "[Unsupported message type]", link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_message,
                    group_id,
                    payload or header,
                    parse_mode=ParseMode.HTML,
                )

            if sent_message is not None:
                await self._log_forward(group_id, self._message_id(sent_message), s_key, message)

        except Exception:
            logger.exception("Failed forwarding to group %s", group_id)

    async def _safe_send(self, func, *args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except RetryAfter as err:
            wait_seconds = int(getattr(err, "retry_after", 1))
            await asyncio.sleep(max(wait_seconds, 1))
            return await func(*args, **kwargs)

    async def _log_forward(self, group_id: int, destination_message_id: int, s_key: str, source_message: Message) -> None:
        sender_id = None
        if source_message.from_user:
            sender_id = source_message.from_user.id
        elif source_message.sender_chat:
            sender_id = source_message.sender_chat.id

        text_blob = (source_message.text or source_message.caption or "").strip()

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            fwd = g.setdefault("forward_log", {})
            fwd[str(destination_message_id)] = {
                "source_key": s_key,
                "sender_id": sender_id,
                "text": text_blob,
            }
            if len(fwd) > 2000:
                # Keep memory bounded.
                keys = list(fwd.keys())
                for k in keys[:300]:
                    fwd.pop(k, None)
            return state

        await self.storage.update(updater)


async def _main() -> None:
    app = TelegramFeedBot()
    await app.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await app.stop()


def run() -> None:
    asyncio.run(_main())
