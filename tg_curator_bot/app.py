import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from html import escape
import logging
import os
import re
import sys
from collections import defaultdict
from itertools import count
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded
from pyrogram.handlers import EditedMessageHandler as PyroEditedMessageHandler, MessageHandler as PyroMessageHandler
from pyrogram.types import Message
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaDocument, InputMediaPhoto, InputMediaVideo, MenuButtonCommands, Update
from telegram.constants import ParseMode
from telegram.constants import ChatMemberStatus
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CallbackQueryHandler, ChatMemberHandler, ContextTypes, MessageHandler, filters

from .filters import evaluate_filters
from .formatting import compose_caption_payload, compose_text_payload, original_message_link, source_header
from .keyboards import (
    add_rule_types,
    bulk_source_import_menu,
    dm_administration_menu,
    dm_admin_menu,
    dm_destination_delete_menu,
    dm_destinations_menu,
    dm_live_events_menu,
    filters_root,
    history_actions_menu,
    history_source_selector_menu_paginated,
    group_settings_menu,
    group_main_menu,
    rules_menu,
    rule_mode_selector,
    source_actions_menu,
    source_filter_selector_menu_paginated,
    source_remove_menu,
    yes_no_buttons,
)
from .storage import DEFAULT_STATE, ForwardLogStorage, Storage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("tg-curator-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def default_group_state() -> Dict[str, Any]:
    return {
        "meta": {
            "title": None,
            "username": None,
        },
        "settings": {
            "show_header": True,
            "show_link": True,
            "show_source_datetime": False,
        },
        "group_filters": {
            "rules": [],
        },
        "source_import": {
            "filter_mode": "all",
            "auto_sync_enabled": False,
        },
        "sources": {},
    }


def source_key(chat_id: int, topic_id: Optional[int]) -> str:
    return f"{chat_id}|{topic_id or 0}"


def parse_source_key(key: str) -> Tuple[int, Optional[int]]:
    chat_raw, topic_raw = key.split("|", 1)
    chat_id = int(chat_raw)
    topic_id = int(topic_raw)
    return chat_id, (None if topic_id == 0 else topic_id)


def message_topic_id(message: Message) -> Optional[int]:
    topic_id = getattr(message, "message_thread_id", None)
    if topic_id is not None:
        return int(topic_id)

    # Pyrogram may expose forum thread linkage through top-reply fields instead
    # of Bot API style message_thread_id.
    for attr_name in ("reply_to_top_message_id", "reply_to_top_id"):
        topic_id = getattr(message, attr_name, None)
        if topic_id is not None:
            return int(topic_id)

    if getattr(message, "forum_topic_created", None) is not None:
        return int(getattr(message, "id", 0) or getattr(message, "message_id"))

    return None


class TelegramFeedBot:
    def __init__(self) -> None:
        load_dotenv()
        self.bot_token = os.getenv("BOT_TOKEN", "").strip()
        self.env_api_id = os.getenv("BOT_API_ID", "").strip() or os.getenv("API_ID", "").strip()
        self.env_api_hash = os.getenv("BOT_API_HASH", "").strip() or os.getenv("API_HASH", "").strip()
        self.env_owner_id = os.getenv("OWNER_ID", "").strip()
        self.env_session_string = (
            os.getenv("USER_SESSION_STRING", "").strip()
            or os.getenv("SESSION_STRING", "").strip()
            or os.getenv("PYROGRAM_SESSION_STRING", "").strip()
        )

        self.data_path = os.getenv("DATA_PATH", "data/data.json")
        self.forward_log_path = os.getenv("FORWARD_LOG_PATH", "data/forward_logs.json")
        self.storage = Storage(self.data_path)
        self.forward_log_storage = ForwardLogStorage(self.forward_log_path)

        self.application: Optional[Application] = None
        self.bot = None
        self.user_client: Optional[Client] = None
        self.bot_id: Optional[int] = None
        self.bot_username: str = ""
        self.chat_username_cache: Dict[int, Optional[str]] = {}

        self.pending_inputs: Dict[int, Dict[str, Any]] = {}
        self.pending_locks: Dict[int, asyncio.Lock] = {}
        self.pending_timeout_seconds = max(int(os.getenv("PENDING_TIMEOUT_SECONDS", "300") or 300), 60)
        self.intent_actions: Dict[str, Dict[str, Any]] = {}
        self._intent_counter = count(1)
        self._media_group_buffers: Dict[tuple, list] = {}
        self._media_group_tasks: Dict[tuple, asyncio.Task] = {}
        self._global_dedupe_hits: Dict[str, float] = {}
        self._bulk_import_sessions: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self._source_test_locks: Dict[int, asyncio.Lock] = {}

    def _now(self) -> float:
        try:
            return asyncio.get_running_loop().time()
        except RuntimeError:
            return 0.0

    def _global_spam_dedupe_config(self, state: Dict[str, Any]) -> Tuple[bool, int]:
        admin_settings = state.get("admin_settings", {})
        enabled = bool(admin_settings.get("global_spam_dedupe_enabled", True))
        window_seconds = int(admin_settings.get("global_spam_dedupe_window_seconds", 10))
        return enabled, max(1, min(300, window_seconds))

    def _chat_type_name(self, value: Any) -> str:
        raw_value = getattr(value, "type", value)
        enum_name = getattr(raw_value, "name", None)
        if isinstance(enum_name, str) and enum_name:
            return enum_name.lower()

        normalized = str(raw_value or "").strip().lower()
        if "." in normalized:
            normalized = normalized.rsplit(".", 1)[-1]
        return normalized

    def _source_import_config(self, group_state: Dict[str, Any]) -> Dict[str, Any]:
        config = {
            "filter_mode": "all",
            "auto_sync_enabled": False,
        }
        raw = group_state.get("source_import", {})
        if isinstance(raw, dict):
            config.update(raw)

        filter_mode = str(config.get("filter_mode") or "all").strip().lower()
        if filter_mode not in {"all", "groups", "channels"}:
            filter_mode = "all"

        return {
            "filter_mode": filter_mode,
            "auto_sync_enabled": bool(config.get("auto_sync_enabled", False)),
        }

    def _source_import_filter_label(self, filter_mode: str) -> str:
        labels = {
            "all": "All chats",
            "groups": "Groups only",
            "channels": "Channels only",
        }
        return labels.get(str(filter_mode or "all").lower(), "All chats")

    def _matches_source_import_filter(self, chat_type: str, filter_mode: str) -> bool:
        normalized_type = self._chat_type_name(chat_type)
        normalized_filter = str(filter_mode or "all").lower()
        if normalized_filter == "channels":
            return normalized_type == "channel"
        if normalized_filter == "groups":
            return normalized_type in {"group", "supergroup"}
        return normalized_type in {"group", "supergroup", "channel"}

    def _normalize_message_blob(self, text: Optional[str]) -> str:
        if not text:
            return ""
        normalized = " ".join(text.split())
        return normalized.lower()[:500]

    def _message_media_unique_id(self, message: Message) -> Optional[str]:
        for attr_name in ("file_unique_id", "document", "photo", "video", "audio", "voice"):
            obj = getattr(message, attr_name, None)
            if isinstance(obj, str):
                return obj
            if hasattr(obj, "file_unique_id"):
                return str(obj.file_unique_id)
        return None

    def _message_signature(self, message: Message) -> str:
        msg_type = type(message).__name__
        text = getattr(message, "caption", None) or getattr(message, "text", None) or ""
        normalized_text = self._normalize_message_blob(text)
        media_id = self._message_media_unique_id(message) or ""
        sig = f"{msg_type}|{normalized_text}|{media_id}"
        return sig[:1500]

    def _should_drop_global_duplicate(self, state: Dict[str, Any], message: Message) -> bool:
        enabled, window_seconds = self._global_spam_dedupe_config(state)
        if not enabled:
            return False
        
        sig = self._message_signature(message)
        now = self._now()
        
        # Clean expired entries
        if len(self._global_dedupe_hits) > 5000:
            cutoff = now - window_seconds
            self._global_dedupe_hits = {k: v for k, v in self._global_dedupe_hits.items() if v > cutoff}
        
        # Check if duplicate
        if sig in self._global_dedupe_hits:
            last_seen = self._global_dedupe_hits[sig]
            if now - last_seen < window_seconds:
                return True
        
        # Update cache
        self._global_dedupe_hits[sig] = now
        return False

    def _set_pending_input(self, user_id: int, payload: Dict[str, Any]) -> None:
        pending = dict(payload)
        now = self._now()
        pending["created_at"] = now
        pending["expires_at"] = now + float(self.pending_timeout_seconds)
        self.pending_inputs[user_id] = pending

    def _pending_is_expired(self, pending: Dict[str, Any]) -> bool:
        expires_at = pending.get("expires_at")
        if expires_at is None:
            return False
        try:
            return self._now() > float(expires_at)
        except (TypeError, ValueError):
            return False

    def _is_cancel_text(self, text: str) -> bool:
        value = (text or "").strip().lower()
        return value in {"cancel", "/cancel", "stop", "back", "exit", "abort"}

    async def _pending_context_banner(self, pending: Dict[str, Any]) -> str:
        kind = str(pending.get("kind") or "").strip()
        group_id_raw = pending.get("group_id")
        context_parts: List[str] = []
        if group_id_raw is not None:
            try:
                group_id = int(group_id_raw)
                state = await self._state()
                group_state = state.get("groups", {}).get(str(group_id), default_group_state())
                context_parts.append(f"Destination: <b>{self._group_display_name(group_id, group_state)}</b>")
            except Exception:
                pass

        if kind == "add_source":
            context_parts.append("Flow: <b>Add Source</b>")
        elif kind == "add_rule":
            scope = "group" if pending.get("scope") == "gf" else "source"
            context_parts.append(f"Flow: <b>Add Rule ({scope})</b>")
        elif kind == "choose_group_intent":
            context_parts.append("Flow: <b>Choose Destination</b>")

        if not context_parts:
            return ""
        return "\n".join(context_parts)

    def _store_intent_action(self, action: Dict[str, Any], ttl_seconds: int = 300) -> str:
        now = self._now()
        self.intent_actions = {
            token: payload
            for token, payload in self.intent_actions.items()
            if now <= float(payload.get("expires_at", now + 1))
        }
        token = format(next(self._intent_counter), "x")
        payload = dict(action)
        payload["expires_at"] = now + float(max(ttl_seconds, 30))
        self.intent_actions[token] = payload
        return token

    def _pop_intent_action(self, token: str) -> Optional[Dict[str, Any]]:
        payload = self.intent_actions.pop(token, None)
        if not payload:
            return None
        try:
            if self._now() > float(payload.get("expires_at", 0)):
                return None
        except (TypeError, ValueError):
            return None
        return payload

    async def _cancel_pending_flow(self, user_id: int, message: Message, reason: str) -> None:
        self.pending_inputs.pop(user_id, None)
        text, session_ready, groups, sources = await self._dm_home_text()
        await message.reply_text(
            f"{reason}\n\n{text}",
            reply_markup=dm_admin_menu(session_ready, groups, sources, show_admin_menu=True),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _expire_pending_flow(self, user_id: int, message: Message, pending: Dict[str, Any]) -> None:
        self.pending_inputs.pop(user_id, None)
        resume_token = self._store_intent_action({"type": "resume_pending", "pending": pending}, ttl_seconds=180)
        banner = await self._pending_context_banner(pending)
        parts = ["This input session expired."]
        if banner:
            parts.append("")
            parts.append(banner)
        await message.reply_text(
            "\n".join(parts),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("▶️ Resume", callback_data=f"x:ia:{resume_token}")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="x:cancel")],
                ]
            ),
            disable_web_page_preview=True,
        )

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

    async def _on_my_chat_member_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        member_update = update.my_chat_member
        if member_update is None:
            return
        await self.on_my_chat_member_update(update, member_update)

    async def _state(self) -> Dict[str, Any]:
        return await self.storage.read()

    async def _forward_logs_state(self) -> Dict[str, Any]:
        return await self.forward_log_storage.read()

    async def _group_forward_history(self, group_id: int) -> Dict[str, Any]:
        state = await self._forward_logs_state()
        history = state.get(str(group_id), {})
        if isinstance(history, dict):
            return history
        return {}

    async def _forward_log_entry(self, group_id: int, destination_message_id: str) -> Optional[Dict[str, Any]]:
        history = await self._group_forward_history(group_id)
        entry = history.get(str(destination_message_id))
        if isinstance(entry, dict):
            return entry
        return None

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

    def _format_source_datetime(self, message: Message) -> str:
        date_value = getattr(message, "date", None)
        if date_value is None:
            return ""
        if isinstance(date_value, datetime):
            if date_value.tzinfo is None:
                date_value = date_value.replace(tzinfo=timezone.utc)
            date_utc = date_value.astimezone(timezone.utc)
            return date_utc.strftime("%Y-%m-%d %H:%M UTC")
        return ""

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
        for attr_name in ("chat", "sender_chat", "source_chat"):
            origin_chat = getattr(origin, attr_name, None)
            if origin_chat is not None:
                return origin_chat
        return None

    def _forwarded_user(self, message: Message):
        user = getattr(message, "forward_from", None)
        if user is not None:
            return user
        origin = getattr(message, "forward_origin", None)
        if origin is None:
            return None
        for attr_name in ("sender_user", "user"):
            origin_user = getattr(origin, attr_name, None)
            if origin_user is not None:
                return origin_user
        return None

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
        owner_identity = await self._owner_identity(owner_id)

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
            f"Owner: {owner_identity}",
            f"User API credentials configured: {'Yes' if has_api else 'No'}",
            f"User session string configured: {'Yes' if has_session else 'No'}",
            f"User client: {user_client_state}",
            f"User account: {user_identity}",
            f"Destination groups: {groups}",
            f"Total sources: {sources}",
        ]
        return "\n".join(lines)

    async def _ensure_group_registered(self, group_id: int, chat: Optional[Any] = None) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            if str(group_id) not in groups:
                groups[str(group_id)] = default_group_state()
            else:
                groups[str(group_id)].setdefault("meta", {}).setdefault("title", None)
                groups[str(group_id)].setdefault("meta", {}).setdefault("username", None)

            if chat is not None:
                meta = groups[str(group_id)].setdefault("meta", {})
                meta["title"] = getattr(chat, "title", None) or getattr(chat, "full_name", None) or meta.get("title")
                meta["username"] = self._normalize_username(getattr(chat, "username", None)) or meta.get("username")
            return state

        await self.storage.update(updater)

    async def _remove_group_registration(self, group_id: int) -> bool:
        removed = False

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal removed
            groups = state.setdefault("groups", {})
            if str(group_id) in groups:
                groups.pop(str(group_id), None)
                removed = True
            return state

        await self.storage.update(updater)
        return removed

    def _can_send_to_chat(self, chat_member: Any) -> bool:
        status = str(getattr(chat_member, "status", ""))
        if status in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}:
            return False
        if status == ChatMemberStatus.RESTRICTED:
            return bool(getattr(chat_member, "can_send_messages", False) or getattr(chat_member, "can_post_messages", False))
        return status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        }

    async def _notify_owner(self, text: str, reply_markup=None) -> None:
        state = await self._state()
        owner_id = state.get("owner_id")
        if owner_id is None or self.bot is None:
            return
        try:
            await self.bot.send_message(int(owner_id), text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=reply_markup)
        except Exception as exc:
            logger.warning("Failed to notify owner | owner_id=%s | error=%s", owner_id, exc)

    async def _clear_owner_dm(self) -> None:
        """Delete every message in the bot-owner DM using the Pyrogram user client."""
        if self.user_client is None or self.bot_id is None:
            return
        try:
            ids: List[int] = []
            async for msg in self.user_client.get_chat_history(self.bot_id):
                ids.append(msg.id)
            for i in range(0, len(ids), 100):
                try:
                    await self.user_client.delete_messages(self.bot_id, ids[i:i + 100], revoke=True)
                except Exception as exc:
                    logger.warning("DM clear batch failed | error=%s", exc)
            logger.info("Cleared %d messages from owner DM", len(ids))
        except Exception as exc:
            logger.warning("Failed to clear owner DM | error=%s", exc)

    async def _send_startup_menu(self) -> None:
        """Send the control panel menu to the owner after clearing DM history."""
        state = await self._state()
        owner_id = state.get("owner_id")
        if owner_id is None or self.bot is None:
            return
        text, session_ready, groups, sources = await self._dm_home_text()
        try:
            await self.bot.send_message(
                int(owner_id),
                text,
                reply_markup=dm_admin_menu(session_ready, groups, sources, show_admin_menu=True),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.warning("Failed to send startup menu | error=%s", exc)

    async def _sync_destinations_from_user_dialogs(self) -> int:
        """
        Discover destination groups where the bot already has posting access.

        This uses the user account dialogs as discoverable chat candidates, then
        verifies bot membership and send permission through Bot API.
        """
        if self.user_client is None or self.bot is None or self.bot_id is None:
            return 0

        added = 0
        state = await self._state()
        registered_ids = set(state.get("groups", {}).keys())

        async for dialog in self.user_client.get_dialogs():
            chat = getattr(dialog, "chat", None)
            if chat is None:
                continue

            chat_type = self._chat_type_name(chat)
            if chat_type not in {"group", "supergroup"}:
                continue

            chat_id = getattr(chat, "id", None)
            if chat_id is None:
                continue

            try:
                member = await self.bot.get_chat_member(int(chat_id), int(self.bot_id))
            except RetryAfter as exc:
                await asyncio.sleep(float(getattr(exc, "retry_after", 1) or 1))
                try:
                    member = await self.bot.get_chat_member(int(chat_id), int(self.bot_id))
                except Exception:
                    continue
            except Exception:
                continue

            if not self._can_send_to_chat(member):
                continue

            already_registered = str(chat_id) in registered_ids
            await self._ensure_group_registered(int(chat_id), chat)
            if not already_registered:
                added += 1
                registered_ids.add(str(chat_id))

        return added

    def _clip_telegram_text(self, text: str, limit: int = 4096) -> str:
        value = str(text or "")
        if len(value) <= limit:
            return value
        suffix = "\n\n... (truncated)"
        keep = max(0, limit - len(suffix))
        truncated = value[:keep] + suffix
        
        # Close any unclosed HTML tags (e.g., <code>, <b>, <i>, <u>, etc.)
        # Find all open tags in the truncated text
        open_tags = re.findall(r'<(\w+)(?:\s[^>]*)?>(?!.*</\1>)', truncated)
        for tag in reversed(open_tags):
            truncated += f"</{tag}>"
        
        return truncated

    async def _safe_edit_message_text(self, message: Any, text: str, reply_markup: Any = None, **kwargs) -> Any:
        try:
            return await message.edit_text(self._clip_telegram_text(text), reply_markup=reply_markup, **kwargs)
        except BadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return message
            if "message_too_long" in str(exc).lower() or "message is too long" in str(exc).lower():
                return await message.edit_text(self._clip_telegram_text(text, limit=3000), reply_markup=reply_markup, **kwargs)
            raise

    async def _safe_edit_chat_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        reply_markup: Any = None,
        **kwargs,
    ) -> bool:
        if self.bot is None:
            return False
        try:
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=self._clip_telegram_text(text),
                reply_markup=reply_markup,
                **kwargs,
            )
            return True
        except BadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return True
            return False
        except Exception:
            return False

    async def _configure_bot_menu(self) -> None:
        if self.bot is None:
            return

        commands = [
            BotCommand("start", "Open control panel"),
            BotCommand("menu", "Open control panel"),
        ]

        try:
            await self.bot.set_my_commands(commands)
        except Exception as exc:
            logger.warning("Failed to configure bot commands | error=%s", exc)

        try:
            await self.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        except Exception as exc:
            logger.warning("Failed to configure chat menu button | error=%s", exc)

    def _group_display_name(self, group_id: int, group_state: Dict[str, Any]) -> str:
        meta = group_state.get("meta", {})
        username = self._normalize_username(meta.get("username"))
        title = str(meta.get("title") or "").strip()
        if title:
            return title
        if username:
            return f"@{username}"
        return f"Destination {group_id}"

    def _source_display_name(self, source_key_value: str, source: Dict[str, Any]) -> str:
        username = self._normalize_username(source.get("username"))
        name = str(source.get("name") or "").strip()
        if name:
            return name
        if username:
            return f"@{username}"
        return source_key_value

    def _sorted_sources(self, group_state: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        sources = group_state.get("sources", {})
        return sorted(
            sources.items(),
            key=lambda item: (self._source_display_name(item[0], item[1]).lower(), item[0]),
        )

    def _sorted_source_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            candidates,
            key=lambda source: (
                self._source_display_name(source_key(int(source["chat_id"]), source.get("topic_id")), source).lower(),
                source_key(int(source["chat_id"]), source.get("topic_id")),
            ),
        )

    def _bulk_import_session_key(self, user_id: int, group_id: int) -> Tuple[int, int]:
        return int(user_id), int(group_id)

    def _bulk_import_categories(self, session: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compute category-level selection data for the bulk import menu."""
        candidates = session.get("all_candidates", [])
        selected = set(session.get("selected_keys", set()))
        folders = session.get("folders", [])

        def _state(cats: List[Dict[str, Any]]) -> str:
            keys = {source_key(int(c["chat_id"]), c.get("topic_id")) for c in cats}
            if not keys:
                return "empty"
            if keys <= selected:
                return "all"
            if keys & selected:
                return "some"
            return "none"

        def _prefix(state: str) -> str:
            return "✅" if state == "all" else ("☑️" if state == "some" else "⬜")

        categories: List[Dict[str, Any]] = []
        groups = [c for c in candidates if self._chat_type_name(c.get("type")) in {"group", "supergroup"}]
        channels = [c for c in candidates if self._chat_type_name(c.get("type")) == "channel"]

        if groups:
            s = _state(groups)
            categories.append({"key": "groups", "label": f"{_prefix(s)} 👥 Groups ({len(groups)})"})
        if channels:
            s = _state(channels)
            categories.append({"key": "channels", "label": f"{_prefix(s)} 📢 Channels ({len(channels)})"})
        for folder in folders:
            fid = folder["id"]
            folder_candidates = [c for c in candidates if c.get("folder_id") == fid]
            if not folder_candidates:
                continue
            s = _state(folder_candidates)
            categories.append({"key": f"folder_{fid}", "label": f"{_prefix(s)} 📂 {folder['title']} ({len(folder_candidates)})"})

        return categories

    def _category_candidates(self, session: Dict[str, Any], cat_key: str) -> List[Dict[str, Any]]:
        candidates = session.get("all_candidates", [])
        if cat_key == "groups":
            return [c for c in candidates if self._chat_type_name(c.get("type")) in {"group", "supergroup"}]
        if cat_key == "channels":
            return [c for c in candidates if self._chat_type_name(c.get("type")) == "channel"]
        if cat_key.startswith("folder_"):
            try:
                fid = int(cat_key[7:])
            except ValueError:
                return []
            return [c for c in candidates if c.get("folder_id") == fid]
        return []

    async def _get_dialog_folders(self) -> List[Dict[str, Any]]:
        """Fetch Telegram dialog folder (filter) names from the user account."""
        if self.user_client is None:
            return []
        try:
            from pyrogram import raw as pyrogram_raw
            result = await self.user_client.invoke(pyrogram_raw.functions.messages.GetDialogFilters())
            folders = []
            for f in result:
                title = getattr(f, "title", None)
                fid = getattr(f, "id", None)
                if title and fid is not None:
                    folders.append({"id": int(fid), "title": str(title)})
            return folders
        except Exception:
            return []

    def _history_source_choices(
        self,
        group_state: Dict[str, Any],
        history: Dict[str, Any],
    ) -> List[Tuple[str, str, int]]:
        source_counts: Dict[str, int] = defaultdict(int)
        for entry in history.values():
            source_key_value = self._entry_source_key(entry)
            if source_key_value:
                source_counts[source_key_value] += 1

        ordered_keys = sorted(
            source_counts.keys(),
            key=lambda key: (
                -source_counts[key],
                self._source_display_name(key, group_state.get("sources", {}).get(key, {})).lower(),
                key,
            ),
        )
        return [
            (
                key,
                self._source_display_name(key, group_state.get("sources", {}).get(key, {})),
                source_counts[key],
            )
            for key in ordered_keys
        ]

    def _entry_source_key(self, entry: Any) -> str:
        if not isinstance(entry, dict):
            return ""

        direct = str(entry.get("source_key") or "").strip()
        if direct:
            return direct

        raw_chat_id = entry.get("source_chat_id")
        if raw_chat_id is None:
            raw_chat_id = entry.get("chat_id")

        if raw_chat_id is None:
            return ""

        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            return ""

        raw_topic_id = entry.get("source_topic_id")
        if raw_topic_id is None:
            raw_topic_id = entry.get("topic_id")

        topic_id: Optional[int]
        if raw_topic_id in (None, "", 0, "0"):
            topic_id = None
        else:
            try:
                topic_id = int(raw_topic_id)
            except (TypeError, ValueError):
                topic_id = None

        return source_key(chat_id, topic_id)

    def _entry_matches_source(self, entry: Any, source_k: str) -> bool:
        if not source_k:
            return False
        return self._entry_source_key(entry) == str(source_k)

    def _normalize_username(self, username: Optional[Any]) -> Optional[str]:
        if username is None:
            return None
        value = str(username).strip().lstrip("@")
        return value or None

    def _identity_label(self, username: Optional[Any], entity_id: Optional[Any], html: bool = False) -> str:
        normalized = self._normalize_username(username)
        if normalized:
            return f"@{normalized}"
        if entity_id is None:
            return "-"
        return f"<code>{int(entity_id)}</code>" if html else str(int(entity_id))

    def _group_identity(self, group_id: int, group_state: Dict[str, Any], html: bool = False) -> str:
        meta = group_state.get("meta", {})
        return self._identity_label(meta.get("username"), group_id, html=html)

    def _source_identity(self, source_key_value: str, source: Dict[str, Any], html: bool = False) -> str:
        chat_id = source.get("chat_id")
        topic_id = source.get("topic_id")
        base = self._identity_label(source.get("username"), chat_id, html=html)
        if topic_id is None:
            if base != "-":
                return base
            return f"<code>{source_key_value}</code>" if html else source_key_value
        if html:
            return f"{base} / topic <code>{int(topic_id)}</code>"
        return f"{base} / topic {int(topic_id)}"

    async def _chat_username(self, chat_id: int) -> Optional[str]:
        if chat_id in self.chat_username_cache:
            return self.chat_username_cache[chat_id]

        username = None
        if self.bot is not None:
            try:
                chat = await self.bot.get_chat(int(chat_id))
                username = getattr(chat, "username", None)
            except Exception:
                username = None

        if username is None and self.user_client is not None:
            try:
                chat = await self.user_client.get_chat(int(chat_id))
                username = getattr(chat, "username", None)
            except Exception:
                username = None

        normalized = self._normalize_username(username)
        self.chat_username_cache[chat_id] = normalized
        return normalized

    async def _owner_identity(self, owner_id: Optional[Any], html: bool = False) -> str:
        if owner_id is None:
            return "-"
        try:
            owner_id_int = int(owner_id)
        except (TypeError, ValueError):
            return str(owner_id)

        username = await self._chat_username(owner_id_int)
        return self._identity_label(username, owner_id_int, html=html)

    def _format_rule(self, rule: Dict[str, Any]) -> str:
        rule_type = str(rule.get("type") or "rule")
        mode = rule.get("mode", "blocklist")
        mode_emoji = "🚫" if mode == "blocklist" else "✅"
        
        if rule_type in {"keyword", "sender"}:
            values = ", ".join(str(value) for value in rule.get("values", [])) or "-"
            return f"{mode_emoji} {rule_type}: {values}"
        if rule_type in {"exact", "message_type"}:
            return f"{mode_emoji} {rule_type}: {rule.get('value', '-') }"
        if rule_type == "has_link":
            return f"{mode_emoji} has_link: {'yes' if rule.get('value') else 'no'}"
        return f"{mode_emoji} {str(rule)}"

    def _bool_label(self, value: bool) -> str:
        return "ON" if value else "OFF"

    def _parse_iso_datetime(self, value: Any) -> Optional[datetime]:
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _event_sort_timestamp(self, entry: Dict[str, Any]) -> float:
        for key in ("logged_at", "source_date"):
            parsed = self._parse_iso_datetime(entry.get(key))
            if parsed is not None:
                return parsed.timestamp()
        return 0.0

    def _event_time_label(self, value: Any) -> str:
        parsed = self._parse_iso_datetime(value)
        if parsed is None:
            return "time n/a"
        now = datetime.now(timezone.utc)
        if parsed.date() == now.date():
            return parsed.strftime("%H:%M:%S UTC")
        return parsed.strftime("%m-%d %H:%M UTC")

    def _live_event_text(self, lines: List[str]) -> str:
        body = "\n".join(lines) if lines else "Waiting for forwarding events..."
        return f"Live Events\n{body}"

    def _trim_live_event_lines(self, lines: List[str], limit: int = 4096) -> List[str]:
        normalized = [" ".join(str(line).split()) for line in lines if str(line).strip()]
        while normalized and len(self._live_event_text(normalized)) > limit:
            normalized.pop(0)
        return normalized

    async def _live_events_screen_text(self) -> str:
        state = await self._state()
        admin_settings = state.get("admin_settings", {})
        live_lines = admin_settings.get("live_events_lines", [])
        if not isinstance(live_lines, list):
            live_lines = []
        return self._live_event_text(self._trim_live_event_lines(live_lines))

    async def _set_live_events_message_id(self, message_id: Optional[int]) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            admin_settings = state.setdefault("admin_settings", {})
            if message_id is None:
                admin_settings.pop("live_events_message_id", None)
            else:
                admin_settings["live_events_message_id"] = int(message_id)
            return state

        await self.storage.update(updater)

    async def _append_live_event_line(self, group_id: int, source_k: str, logged_at_iso: str) -> None:
        state = await self._state()
        owner_id = state.get("owner_id")
        if owner_id is None:
            return

        groups = state.get("groups", {})
        group_state = groups.get(str(group_id), default_group_state())
        destination_name = self._group_display_name(group_id, group_state)

        source_state = group_state.get("sources", {}).get(str(source_k), {})
        source_name = self._source_display_name(str(source_k), source_state)

        parsed = self._parse_iso_datetime(logged_at_iso)
        time_label = parsed.strftime("%H:%M:%S") if parsed else datetime.now(timezone.utc).strftime("%H:%M:%S")

        source_short = " ".join(str(source_name).split())[:60]
        destination_short = " ".join(str(destination_name).split())[:60]
        line = f"# {time_label}: {source_short}>{destination_short}"

        updated_state = await self.storage.update(
            lambda current: self._update_live_events_lines(current, line)
        )

        admin_settings = updated_state.get("admin_settings", {})
        message_id = admin_settings.get("live_events_message_id")
        if message_id is None:
            return

        lines = admin_settings.get("live_events_lines", [])
        if not isinstance(lines, list):
            lines = []
        text = self._live_event_text(self._trim_live_event_lines(lines))
        edited = await self._safe_edit_chat_message_text(
            chat_id=int(owner_id),
            message_id=int(message_id),
            text=text,
            reply_markup=dm_live_events_menu(),
        )
        if not edited:
            await self._set_live_events_message_id(None)

    def _update_live_events_lines(self, state: Dict[str, Any], line: str) -> Dict[str, Any]:
        admin_settings = state.setdefault("admin_settings", {})
        live_lines = admin_settings.get("live_events_lines", [])
        if not isinstance(live_lines, list):
            live_lines = []
        live_lines.append(str(line))
        admin_settings["live_events_lines"] = self._trim_live_event_lines(live_lines)
        return state

    async def _dm_home_text(self) -> Tuple[str, bool, int, int]:
        state = await self._state()
        owner_id = state.get("owner_id")
        sess = state.get("user_session", {})
        session_ready = bool(sess.get("session_string"))
        groups, sources = await self._count_groups_sources()
        owner_identity = await self._owner_identity(owner_id, html=True)
        user_client_state = "Connected" if self.user_client is not None else "Not connected"
        text = (
            "<b>🎛️ Curator Control</b>\n\n"
            f"Owner: {owner_identity}\n"
            f"User session: <b>{'Ready' if session_ready else 'Missing'}</b>\n"
            f"User client: <b>{user_client_state}</b>\n"
            f"Destinations: <b>{groups}</b>\n"
            f"Sources: <b>{sources}</b>\n\n"
            "Add the bot to a destination group and make sure it can send messages there. Destinations appear automatically and all admin actions live in this DM."
        )
        return text, session_ready, groups, sources

    async def _administration_screen_text(self) -> str:
        state = await self._state()
        groups = state.get("groups", {})
        return (
            "<b>🛡️ Administration</b>\n\n"
            "Owner-only controls.\n"
            f"Registered destinations: <b>{len(groups)}</b>\n\n"
            "Use this area to remove destinations from the control panel. "
            "Deleting a destination here also deletes its tracked forwarding history."
        )

    async def _admin_destination_delete_screen_text(self) -> str:
        state = await self._state()
        groups = state.get("groups", {})
        lines = [
            "<b>🗑️ Delete Destination</b>",
            "",
            "Select a destination to remove from the bot control panel.",
            "This also deletes tracked forwarding history for that destination.",
        ]
        if not groups:
            lines.extend(["", "No destination groups are registered yet."])
            return "\n".join(lines)

        lines.append("")
        for group_id_raw, group_state in sorted(groups.items(), key=lambda item: self._group_display_name(int(item[0]), item[1]).lower()):
            group_id = int(group_id_raw)
            name = self._group_display_name(group_id, group_state)
            source_count = len(group_state.get("sources", {}))
            lines.append(f"• <b>{name}</b> — {source_count} sources")
        return "\n".join(lines)

    async def _destination_entries(self) -> List[Tuple[int, str]]:
        state = await self._state()
        entries: List[Tuple[int, str]] = []
        for group_id_raw, group_state in state.get("groups", {}).items():
            group_id = int(group_id_raw)
            name = self._group_display_name(group_id, group_state)
            source_count = len(group_state.get("sources", {}))
            entries.append((group_id, f"{name[:40]} ({source_count})"))
        entries.sort(key=lambda item: item[1].lower())
        return entries

    async def _update_source_import_config(
        self,
        group_id: int,
        *,
        filter_mode: Optional[str] = None,
        auto_sync_enabled: Optional[bool] = None,
    ) -> Dict[str, Any]:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            group_state = groups.setdefault(str(group_id), default_group_state())
            config = self._source_import_config(group_state)
            if filter_mode is not None:
                config["filter_mode"] = str(filter_mode).lower()
            if auto_sync_enabled is not None:
                config["auto_sync_enabled"] = bool(auto_sync_enabled)
            group_state["source_import"] = self._source_import_config({"source_import": config})
            return state

        updated = await self.storage.update(updater)
        return self._source_import_config(updated.get("groups", {}).get(str(group_id), default_group_state()))

    async def _bulk_source_candidates(self, group_id: int, filter_mode: str = "all") -> List[Dict[str, Any]]:
        if self.user_client is None:
            return []

        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        existing_source_keys = set(group_state.get("sources", {}).keys())
        registered_destinations = {int(raw_group_id) for raw_group_id in state.get("groups", {}).keys()}
        candidates_by_key: Dict[str, Dict[str, Any]] = {}

        async for dialog in self.user_client.get_dialogs():
            chat = getattr(dialog, "chat", None)
            if chat is None:
                continue

            chat_type = self._chat_type_name(chat)
            if not self._matches_source_import_filter(chat_type, filter_mode):
                continue

            chat_id = getattr(chat, "id", None)
            if chat_id is None:
                continue

            chat_id = int(chat_id)
            if chat_id in registered_destinations:
                continue

            candidate = self._source_from_chat_entity(chat)
            folder_id = getattr(dialog, "folder_id", None)
            if folder_id:
                candidate["folder_id"] = int(folder_id)
            s_key = source_key(chat_id, candidate.get("topic_id"))
            if s_key in existing_source_keys or s_key in candidates_by_key:
                continue

            candidates_by_key[s_key] = candidate

        return self._sorted_source_candidates(list(candidates_by_key.values()))

    async def _ensure_bulk_import_session(self, user_id: int, group_id: int, refresh: bool = False) -> Dict[str, Any]:
        session_key = self._bulk_import_session_key(user_id, group_id)
        previous = self._bulk_import_sessions.get(session_key)
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        config = self._source_import_config(group_state)
        had_previous = isinstance(previous, dict)

        if had_previous and not refresh:
            previous["auto_sync_enabled"] = config["auto_sync_enabled"]
            return previous

        all_candidates = await self._bulk_source_candidates(group_id, filter_mode="all")
        folders = await self._get_dialog_folders()
        available_keys = {source_key(int(source["chat_id"]), source.get("topic_id")) for source in all_candidates}
        selected_keys = set(previous.get("selected_keys", set())) & available_keys if had_previous else set()
        session = {
            "group_id": int(group_id),
            "all_candidates": all_candidates,
            "folders": folders,
            "selected_keys": selected_keys,
            "auto_sync_enabled": config["auto_sync_enabled"],
        }
        self._bulk_import_sessions[session_key] = session
        return session

    async def _autosync_group_sources(self, group_id: int) -> Dict[str, int]:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        config = self._source_import_config(group_state)
        if not config.get("auto_sync_enabled"):
            return {"eligible": 0, "added": 0}

        candidates = await self._bulk_source_candidates(group_id, filter_mode=config["filter_mode"])
        added = 0
        for source in candidates:
            _, existed = await self._upsert_source(group_id, source)
            if not existed:
                added += 1
        return {"eligible": len(candidates), "added": added}

    async def _autosync_all_group_sources(self) -> Dict[str, int]:
        state = await self._state()
        totals = {"eligible": 0, "added": 0}
        for raw_group_id in state.get("groups", {}).keys():
            result = await self._autosync_group_sources(int(raw_group_id))
            totals["eligible"] += int(result.get("eligible", 0))
            totals["added"] += int(result.get("added", 0))
        return totals

    async def _bulk_add_selected_sources(self, group_id: int, selected_keys: set[str]) -> Dict[str, int]:
        candidates = await self._bulk_source_candidates(group_id, filter_mode="all")
        selected = set(selected_keys)
        added = 0
        eligible = 0
        for source in candidates:
            s_key = source_key(int(source["chat_id"]), source.get("topic_id"))
            if s_key not in selected:
                continue
            eligible += 1
            _, existed = await self._upsert_source(group_id, source)
            if not existed:
                added += 1
        return {"eligible": eligible, "added": added}

    async def _destinations_screen_text(self) -> str:
        state = await self._state()
        groups = state.get("groups", {})
        lines = ["<b>🎯 Destinations</b>"]
        if not groups:
            lines.append("")
            lines.append("No destination groups are registered yet.")
            lines.append("Add the bot to a group and allow it to post there. It will show up here automatically.")
            return "\n".join(lines)

        lines.append("")
        for group_id_raw, group_state in sorted(groups.items(), key=lambda item: self._group_display_name(int(item[0]), item[1]).lower()):
            group_id = int(group_id_raw)
            name = self._group_display_name(group_id, group_state)
            source_count = len(group_state.get("sources", {}))
            identity = self._group_identity(group_id, group_state, html=True)
            lines.append(f"• <b>{name}</b> — {identity} — {source_count} sources")
        return "\n".join(lines)

    async def _destination_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        settings = group_state.get("settings", {})
        group_filters = group_state.get("group_filters", {})
        source_count = len(group_state.get("sources", {}))
        source_filter_count = sum(1 for src in group_state.get("sources", {}).values() if src.get("filters", {}).get("rules"))
        group_identity = self._group_identity(group_id, group_state, html=True)
        return (
            f"<b>{name}</b>\n\n"
            f"Chat: {group_identity}\n"
            f"Sources: <b>{source_count}</b>\n"
            f"Group filter rules: <b>{len(group_filters.get('rules', []))}</b>\n"
            f"Sources with source rules: <b>{source_filter_count}</b>\n"
            f"Header: <b>{self._bool_label(bool(settings.get('show_header', True)))}</b>\n"
            f"Original date/time: <b>{self._bool_label(bool(settings.get('show_source_datetime', False)))}</b>\n"
            f"Original link: <b>{self._bool_label(bool(settings.get('show_link', True)))}</b>"
        )

    def _sources_screen_page_size(self) -> int:
        raw = os.getenv("SOURCES_SCREEN_PAGE_SIZE", "12")
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 12
        return max(5, min(30, value))

    def _selector_page_size(self) -> int:
        raw = os.getenv("SELECTOR_PAGE_SIZE", "8")
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 8
        return max(5, min(25, value))

    async def _sources_screen_text(self, group_id: int, page: int = 0, page_size: Optional[int] = None) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        sources = group_state.get("sources", {})
        import_config = self._source_import_config(group_state)
        sorted_sources = self._sorted_sources(group_state)
        total_sources = len(sorted_sources)
        normalized_page_size = page_size if page_size is not None else self._sources_screen_page_size()
        page_count = max(1, (total_sources + normalized_page_size - 1) // normalized_page_size)
        page = max(0, min(page, page_count - 1))
        start = page * normalized_page_size
        end = min(start + normalized_page_size, total_sources)

        lines = [f"<b>📡 Sources for {name}</b>"]
        lines.append("")
        lines.append(f"Auto-sync new chats: <b>{self._bool_label(import_config['auto_sync_enabled'])}</b>")
        lines.append(f"Bulk import filter: <b>{self._source_import_filter_label(import_config['filter_mode'])}</b>")
        if not sources:
            lines.append("No sources configured yet.")
            lines.append("Use Add Source below, then send a forwarded message, a t.me link, or a chat handle/ID in this DM.")
            return "\n".join(lines)

        lines.append("")
        if page_count > 1:
            lines.append(
                f"Showing <b>{start + 1}-{end}</b> of <b>{total_sources}</b> sources (page <b>{page + 1}/{page_count}</b>)"
            )
            lines.append("")

        for idx, (source_key_value, source) in enumerate(sorted_sources[start:end], start=start + 1):
            source_name = self._source_display_name(source_key_value, source)
            filters_state = source.get("filters", {})
            source_identity = self._source_identity(source_key_value, source, html=True)
            lines.append(
                f"{idx}. <b>{source_name}</b> — {source_identity} — {len(filters_state.get('rules', []))} rules"
            )
        return "\n".join(lines)

    async def _bulk_source_import_screen_text(self, user_id: int, group_id: int) -> Tuple[str, Dict[str, Any]]:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        if self.user_client is None:
            config = self._source_import_config(group_state)
            return (
                f"<b>📚 Bulk Add Sources for {name}</b>\n\n"
                "User session is not ready. Configure it in terminal and restart the bot.",
                {
                    "all_candidates": [],
                    "folders": [],
                    "selected_keys": set(),
                    "auto_sync_enabled": config["auto_sync_enabled"],
                },
            )

        session = await self._ensure_bulk_import_session(user_id, group_id)
        candidates = session.get("all_candidates", [])
        selected_count = len(session.get("selected_keys", set()))
        categories = self._bulk_import_categories(session)

        lines = [f"<b>📚 Bulk Add Sources for {name}</b>"]
        lines.append("")
        lines.append("Tap a category to select or deselect all its chats, then import.")
        lines.append("Destinations and already-added sources are excluded.")
        lines.append("")

        if not candidates:
            lines.append("No joined channels or groups are available to import.")
            return "\n".join(lines), session

        lines.append(f"Available: <b>{len(candidates)}</b> chat{'s' if len(candidates) != 1 else ''}")
        for cat in categories:
            lines.append(f"  • {cat['label']}")
        lines.append("")
        lines.append(f"Selected: <b>{selected_count}</b>")
        lines.append(f"Auto-sync newly joined chats: <b>{self._bool_label(bool(session.get('auto_sync_enabled')))}</b>")

        return "\n".join(lines), session

    async def _filters_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        group_filters = group_state.get("group_filters", {})
        source_filters = sum(1 for source in group_state.get("sources", {}).values() if source.get("filters", {}).get("rules"))
        history = await self._group_forward_history(group_id)
        return (
            f"<b>🧰 Filters for {name}</b>\n\n"
            f"Group filters: <b>{len(group_filters.get('rules', []))}</b> rules\n"
            f"Sources with source-specific rules: <b>{source_filters}</b>\n"
            f"Tracked forwarded messages: <b>{len(history)}</b>\n\n"
            "Use <b>Reapply to Forwarded</b> to re-check already forwarded messages and delete those that no longer pass."
        )

    async def _rules_screen_text(self, group_id: int, scope: str, source_k: Optional[str]) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        target = await self._filter_target(state, group_id, scope, source_k)
        if scope == "gf":
            title = f"Group filters for {self._group_display_name(group_id, group_state)}"
        else:
            source = group_state.get("sources", {}).get(str(source_k), {})
            title = f"Source filters for {self._source_display_name(str(source_k), source)}"
        lines = [f"<b>{title}</b>"]
        rules = target.get("rules", [])
        if not rules:
            lines.append("Rules: none yet")
            return "\n".join(lines)
        lines.append("Rules:")
        for index, rule in enumerate(rules, start=1):
            lines.append(f"{index}. {self._format_rule(rule)}")
        return "\n".join(lines)

    async def _settings_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        settings = group_state.get("settings", {})
        source_count = len(group_state.get("sources", {}))
        return (
            f"<b>⚙️ Settings for {name}</b>\n\n"
            f"Sources: <b>{source_count}</b>\n"
            f"Header: <b>{self._bool_label(bool(settings.get('show_header', True)))}</b>\n"
            f"Original date/time: <b>{self._bool_label(bool(settings.get('show_source_datetime', False)))}</b>\n"
            f"Original link: <b>{self._bool_label(bool(settings.get('show_link', True)))}</b>"
        )

    def _source_test_lock(self, group_id: int) -> asyncio.Lock:
        lock = self._source_test_locks.get(int(group_id))
        if lock is None:
            lock = asyncio.Lock()
            self._source_test_locks[int(group_id)] = lock
        return lock

    def _source_test_status_text(
        self,
        group_id: int,
        *,
        group_state: Dict[str, Any],
        total: int,
        completed: int,
        working: int,
        failing: int,
        current_source: Optional[str] = None,
        recent_failures: Optional[List[str]] = None,
        completed_run: bool = False,
    ) -> str:
        name = self._group_display_name(group_id, group_state)
        remaining = max(total - completed, 0)
        lines = [f"<b>🧪 Source Test for {escape(name)}</b>", ""]
        lines.append("Method: forward the latest source message without filters, then delete the probe.")
        lines.append("")
        lines.append(f"Progress: <b>{completed}/{total}</b>")
        lines.append(f"Working: <b>{working}</b>")
        lines.append(f"Failing: <b>{failing}</b>")
        if completed_run:
            lines.append("Status: <b>Completed</b>")
        else:
            lines.append(f"Remaining: <b>{remaining}</b>")
            lines.append("Status: <b>Running</b>")

        if current_source:
            lines.append(f"Current: <b>{escape(current_source)}</b>")

        failures = recent_failures or []
        if failures:
            lines.append("")
            lines.append("Recent failures:")
            for item in failures[-5:]:
                lines.append(f"- {item}")

        return "\n".join(lines)

    async def _latest_source_probe_message(self, source_chat_id: int, source_topic_id: Optional[int]) -> Optional[Message]:
        if self.user_client is None:
            return None

        limit = max(int(os.getenv("SOURCE_TEST_HISTORY_LIMIT", "100") or 100), 1)
        async for msg in self.user_client.get_chat_history(source_chat_id, limit=limit):
            if source_topic_id is not None and message_topic_id(msg) != int(source_topic_id):
                continue
            return msg
        return None

    async def _probe_source_forwarding(self, group_id: int, s_key: str, source: Dict[str, Any]) -> Tuple[bool, str]:
        if self.user_client is None:
            return False, "User session is not ready"
        if self.bot is None:
            return False, "Bot is not ready"

        source_chat_id = source.get("chat_id")
        if source_chat_id is None:
            return False, "Missing source chat_id"

        try:
            source_chat_id = int(source_chat_id)
        except (TypeError, ValueError):
            return False, "Invalid source chat_id"

        source_topic_id = source.get("topic_id")

        try:
            probe_message = await self._latest_source_probe_message(source_chat_id, source_topic_id)
        except Exception as exc:
            logger.warning(
                "Source test history fetch failed | group_id=%s | source=%s | error=%s",
                group_id,
                s_key,
                exc,
            )
            return False, f"History fetch failed: {escape(str(exc))}"

        if probe_message is None:
            return False, "No recent message found"

        forwarded_message_id = await self._forward_message_to_group(
            group_id,
            s_key,
            probe_message,
            apply_filters=False,
            track_history=False,
            update_last_seen=False,
        )
        if not forwarded_message_id:
            return False, "Probe forward failed"

        deleted = await self._safe_delete_destination_message(group_id, int(forwarded_message_id))
        if not deleted:
            return False, f"Probe sent as message {int(forwarded_message_id)} but cleanup delete failed"

        return True, "ok"

    async def _run_source_tests(self, callback_query, group_id: int) -> None:
        lock = self._source_test_lock(group_id)
        if lock.locked():
            await callback_query.answer("A source test is already running for this destination.", show_alert=True)
            return

        await callback_query.answer("Running source tests...")

        async with lock:
            state = await self._state()
            group_state = state.get("groups", {}).get(str(group_id), default_group_state())
            settings = group_state.get("settings", {})
            admin_settings = state.get("admin_settings", {})
            sources = self._sorted_sources(group_state)

            if self.user_client is None or self.bot is None:
                recent_failures = ["User session or bot client is not ready"]
                await self._safe_edit_message_text(
                    callback_query.message,
                    self._source_test_status_text(
                        group_id,
                        group_state=group_state,
                        total=len(sources),
                        completed=0,
                        working=0,
                        failing=len(sources),
                        recent_failures=recent_failures,
                        completed_run=True,
                    ),
                    reply_markup=group_settings_menu(
                        group_id,
                        bool(settings.get("show_header", True)),
                        bool(settings.get("show_link", True)),
                        bool(settings.get("show_source_datetime", False)),
                        bool(admin_settings.get("global_spam_dedupe_enabled", True)),
                        bool(sources),
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return

            if not sources:
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._settings_screen_text(group_id),
                    reply_markup=group_settings_menu(
                        group_id,
                        bool(settings.get("show_header", True)),
                        bool(settings.get("show_link", True)),
                        bool(settings.get("show_source_datetime", False)),
                        bool(admin_settings.get("global_spam_dedupe_enabled", True)),
                        False,
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return

            total = len(sources)
            completed = 0
            working = 0
            failing = 0
            recent_failures: List[str] = []

            await self._safe_edit_message_text(
                callback_query.message,
                self._source_test_status_text(
                    group_id,
                    group_state=group_state,
                    total=total,
                    completed=completed,
                    working=working,
                    failing=failing,
                    recent_failures=recent_failures,
                    completed_run=False,
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

            for s_key, source in sources:
                source_name = self._source_display_name(s_key, source)
                ok, detail = await self._probe_source_forwarding(group_id, s_key, source)
                completed += 1
                if ok:
                    working += 1
                else:
                    failing += 1
                    recent_failures.append(f"{escape(source_name)}: {detail}")

                await self._safe_edit_message_text(
                    callback_query.message,
                    self._source_test_status_text(
                        group_id,
                        group_state=group_state,
                        total=total,
                        completed=completed,
                        working=working,
                        failing=failing,
                        current_source=(None if completed >= total else source_name),
                        recent_failures=recent_failures,
                        completed_run=(completed >= total),
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

            refreshed_state = await self._state()
            refreshed_group = refreshed_state.get("groups", {}).get(str(group_id), default_group_state())
            refreshed_settings = refreshed_group.get("settings", {})
            await self._safe_edit_message_text(
                callback_query.message,
                self._source_test_status_text(
                    group_id,
                    group_state=refreshed_group,
                    total=total,
                    completed=completed,
                    working=working,
                    failing=failing,
                    recent_failures=recent_failures,
                    completed_run=True,
                ),
                reply_markup=group_settings_menu(
                    group_id,
                    bool(refreshed_settings.get("show_header", True)),
                    bool(refreshed_settings.get("show_link", True)),
                    bool(refreshed_settings.get("show_source_datetime", False)),
                    bool(refreshed_state.get("admin_settings", {}).get("global_spam_dedupe_enabled", True)),
                    bool(refreshed_group.get("sources", {})),
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

    async def _history_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        history = await self._group_forward_history(group_id)
        total_entries = len(history)
        tracked_sources = len({entry.get("source_key") for entry in history.values() if entry.get("source_key")})
        return (
            f"<b>🧹 Clean History for {name}</b>\n\n"
            f"Tracked forwarded messages: <b>{total_entries}</b>\n"
            f"Sources in history: <b>{tracked_sources}</b>\n\n"
            "Clean all tracked history for this destination or remove entries for one source only."
        )

    async def _history_source_selector_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        history = await self._group_forward_history(group_id)
        choices = self._history_source_choices(group_state, history)

        lines = [f"<b>📡 Clean Single Source History for {name}</b>"]
        if not choices:
            lines.append("")
            lines.append("No tracked history exists for this destination yet.")
            return "\n".join(lines)

        lines.append("")
        for source_key_value, _, count in choices:
            source = group_state.get("sources", {}).get(source_key_value, {})
            source_name = self._source_display_name(source_key_value, source)
            source_identity = self._source_identity(source_key_value, source, html=True)
            lines.append(f"• <b>{source_name}</b> — {source_identity} — {count} messages")
        return "\n".join(lines)

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
            return False, "User API credentials are missing. Configure them in terminal and restart the bot."

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
            self.user_client.add_handler(PyroEditedMessageHandler(self.on_user_edited_message))
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

    async def _sync_env_owner(self) -> None:
        if not self.env_owner_id:
            return

        try:
            env_owner_id = int(self.env_owner_id)
        except ValueError:
            logger.warning("OWNER_ID is not numeric; ignoring value")
            return

        state = await self._state()
        if state.get("owner_id") is not None:
            return

        def updater(data: Dict[str, Any]) -> Dict[str, Any]:
            if data.get("owner_id") is None:
                data["owner_id"] = env_owner_id
            return data

        await self.storage.update(updater)
        logger.info("Owner ID loaded from environment")

    def _looks_like_valid_bot_token(self, value: Any) -> bool:
        token = str(value or "").strip()
        return bool(token) and ":" in token and len(token) >= 20

    def _looks_like_valid_api_id(self, value: Any) -> bool:
        try:
            return int(str(value).strip()) > 0
        except (TypeError, ValueError):
            return False

    def _looks_like_valid_api_hash(self, value: Any) -> bool:
        api_hash = str(value or "").strip()
        return bool(re.fullmatch(r"[a-fA-F0-9]{16,64}", api_hash))

    def _looks_like_valid_session_string(self, value: Any) -> bool:
        session_string = str(value or "").strip()
        return len(session_string) > 100 and (session_string.startswith("AQ") or session_string.startswith("BQ"))

    def _state_has_valid_core_config(self, state: Dict[str, Any]) -> bool:
        user_session = state.get("user_session") if isinstance(state, dict) else {}
        if not isinstance(user_session, dict):
            return False

        return (
            self._looks_like_valid_bot_token(state.get("bot_token"))
            and self._looks_like_valid_api_id(user_session.get("api_id"))
            and self._looks_like_valid_api_hash(user_session.get("api_hash"))
            and self._looks_like_valid_session_string(user_session.get("session_string"))
        )

    async def _reset_state_for_onboarding(self, reason: str) -> None:
        await self.storage.write(deepcopy(DEFAULT_STATE))
        logger.warning("State reset before onboarding: %s", reason)

    async def _validate_or_reset_state(self) -> None:
        state = await self._state()
        if self._state_has_valid_core_config(state):
            return
        await self._reset_state_for_onboarding("data.json missing/incomplete/invalid core config")

    async def _save_session_string(self, session_string: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            user_session = state.setdefault("user_session", {})
            user_session["session_string"] = session_string
            state["user_session"] = user_session
            return state

        await self.storage.update(updater)

    async def _save_bot_token(self, bot_token: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            state["bot_token"] = bot_token.strip()
            return state

        await self.storage.update(updater)

    async def _get_configured_bot_token(self) -> str:
        state = await self._state()
        return str(state.get("bot_token") or "").strip() or self.bot_token

    async def _ensure_application(self) -> None:
        if self.application is not None:
            return

        bot_token = await self._get_configured_bot_token()
        if not bot_token:
            raise RuntimeError("BOT_TOKEN is required")

        self.bot_token = bot_token
        self.application = Application.builder().token(self.bot_token).build()
        self.bot = self.application.bot
        self.application.add_handler(MessageHandler(filters.ChatType.PRIVATE, self._on_private_update))
        self.application.add_handler(MessageHandler(filters.ChatType.GROUPS, self._on_group_update))
        self.application.add_handler(ChatMemberHandler(self._on_my_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
        self.application.add_handler(CallbackQueryHandler(self._on_callback_query_update))

    async def _save_user_api_credentials(self, api_id: int, api_hash: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            user_session = state.setdefault("user_session", {})
            user_session["api_id"] = int(api_id)
            user_session["api_hash"] = api_hash.strip()
            state["user_session"] = user_session
            return state

        await self.storage.update(updater)

    def _remove_auth_session_files(self, session_name: Optional[str]) -> None:
        if not session_name:
            return

        for suffix in (".session", ".session-journal"):
            path = os.path.join("data", f"{session_name}{suffix}")
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError as exc:
                logger.warning("Failed to remove auth temp session file | path=%s | error=%s", path, exc)

    async def _disconnect_auth_client(self, auth_client: Optional[Client]) -> None:
        if auth_client is None or not getattr(auth_client, "is_connected", False):
            return

        try:
            await auth_client.disconnect()
        except Exception:
            pass

    async def _request_auth_code(
        self,
        auth_client: Client,
        phone: str,
        phone_code_hash: Optional[str] = None,
    ):
        if not auth_client.is_connected:
            await auth_client.connect()

        if phone_code_hash:
            try:
                return await auth_client.resend_code(phone, phone_code_hash)
            except Exception as exc:
                logger.warning(
                    "Auth resend_code failed, falling back to send_code | phone=%s | error=%s",
                    phone,
                    exc,
                )

        return await auth_client.send_code(phone)

    async def _console_input(self, prompt: str) -> str:
        return await asyncio.to_thread(input, prompt)

    async def _maybe_run_console_onboarding(self) -> None:
        state = await self._state()
        bot_token = str(state.get("bot_token") or "").strip() or self.bot_token
        user_session = state.get("user_session", {})

        api_id = user_session.get("api_id") or self.env_api_id
        api_hash = user_session.get("api_hash") or self.env_api_hash
        session_string = user_session.get("session_string") or self.env_session_string

        if bot_token and api_id and api_hash and session_string:
            return

        if not sys.stdin.isatty():
            logger.info(
                "Interactive onboarding skipped because stdin is not a TTY; configure BOT_TOKEN/BOT_API_ID/BOT_API_HASH/USER_SESSION_STRING in environment."
            )
            return

        print("First-run onboarding: Telegram user session is not configured.")
        print("Stored values will be saved into data/data.json and reused on later starts.")

        await self._run_console_user_onboarding(bot_token, api_id, api_hash, session_string)

    async def _run_console_user_onboarding(
        self,
        initial_bot_token: Optional[str],
        initial_api_id: Optional[Any],
        initial_api_hash: Optional[str],
        initial_session_string: Optional[str],
    ) -> None:
        bot_token = str(initial_bot_token or "").strip()
        while not bot_token:
            bot_token = (await self._console_input("Bot token from BotFather: ")).strip()

        await self._save_bot_token(bot_token)
        self.bot_token = bot_token

        api_id = str(initial_api_id or "").strip()
        while not api_id:
            api_id = (await self._console_input("Telegram API ID: ")).strip()

        while True:
            try:
                numeric_api_id = int(api_id)
                break
            except ValueError:
                api_id = (await self._console_input("API ID must be numeric. Telegram API ID: ")).strip()

        api_hash = str(initial_api_hash or "").strip()
        while not api_hash:
            api_hash = (await self._console_input("Telegram API Hash: ")).strip()

        await self._save_user_api_credentials(numeric_api_id, api_hash)

        session_string = str(initial_session_string or "").strip()
        if session_string:
            await self._save_session_string(session_string)
            print("Existing session string loaded from configuration.")
            return

        auth_session_name = "auth_bootstrap"
        self._remove_auth_session_files(auth_session_name)
        auth_client = Client(
            auth_session_name,
            api_id=numeric_api_id,
            api_hash=api_hash,
            workdir="data",
        )

        try:
            phone = ""
            while not phone:
                phone = (await self._console_input("Phone number in international format (example: +1234567890): ")).strip()

            await auth_client.connect()
            sent = await auth_client.send_code(phone)
            print(
                "A login code was requested from Telegram. "
                f"Type={getattr(sent, 'type', None)} timeout={getattr(sent, 'timeout', None)}"
            )

            while True:
                code_text = (await self._console_input("OTP code (or type resend): ")).strip()
                if code_text.lower() == "resend":
                    sent = await self._request_auth_code(auth_client, phone, sent.phone_code_hash)
                    print(
                        "A new code was requested from Telegram. "
                        f"Type={getattr(sent, 'type', None)} timeout={getattr(sent, 'timeout', None)}"
                    )
                    continue

                otp = re.sub(r"\D", "", code_text)
                if not otp:
                    print("OTP must contain digits only.")
                    continue

                try:
                    await auth_client.sign_in(phone, sent.phone_code_hash, otp)
                    break
                except SessionPasswordNeeded:
                    password = await self._console_input("Two-step verification password: ")
                    await auth_client.check_password(password)
                    break
                except Exception as exc:
                    err = str(exc)
                    logger.warning("Console sign_in failed | phone=%s | error=%s", phone, err)
                    if "PHONE_CODE_EXPIRED" in err:
                        sent = await self._request_auth_code(auth_client, phone, sent.phone_code_hash)
                        print("That code expired. Telegram sent a fresh code; enter the latest one.")
                        continue
                    if "PHONE_CODE_INVALID" in err:
                        print("Invalid code. Use the latest code from Telegram.")
                        continue
                    raise

            session_string = await auth_client.export_session_string()
            await self._save_session_string(session_string)
            print("User session saved to data/data.json.")
        finally:
            await self._disconnect_auth_client(auth_client)
            self._remove_auth_session_files(auth_session_name)

    def _message_id(self, message: Any) -> int:
        return int(getattr(message, "id", None) or getattr(message, "message_id"))

    async def start(self) -> None:
        await self._validate_or_reset_state()
        await self._sync_env_owner()
        await self._sync_env_session()
        await self._maybe_run_console_onboarding()
        await self._ensure_application()
        await self.application.initialize()
        await self.application.start()
        await self._configure_bot_menu()
        # Aggressive polling: instant response on user interaction
        await self.application.updater.start_polling(
            poll_interval=0.0,
            timeout=0,
            allowed_updates=["message", "callback_query", "my_chat_member"],
        )
        me = await self.bot.get_me()
        self.bot_id = me.id
        self.bot_username = (me.username or "").lower()
        ok, status = await self._start_or_restart_user_client()
        if ok:
            logger.info(status)
            try:
                discovered = await self._sync_destinations_from_user_dialogs()
                if discovered:
                    logger.info("Discovered %d existing destination groups from user dialogs", discovered)
                autosynced = await self._autosync_all_group_sources()
                if autosynced.get("added"):
                    logger.info("Auto-synced %d source chats across destinations", autosynced["added"])
                await self._catch_up_all_sources()
            except Exception as exc:
                logger.warning("Startup destination sync failed | error=%s", exc)
        else:
            logger.info("User client not active: %s", status)
        logger.info("Bot started as @%s", me.username)
        await self._clear_owner_dm()
        await self._send_startup_menu()

    async def stop(self) -> None:
        for task in self._media_group_tasks.values():
            task.cancel()
        self._media_group_tasks.clear()
        self._media_group_buffers.clear()
        if self.user_client is not None:
            try:
                await self.user_client.stop()
            except Exception:
                pass
        if self.application is None:
            return
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
            await message.reply_text("Only the bot owner can use this bot.")
            return

        pending = self.pending_inputs.get(user_id)
        if pending and self._pending_is_expired(pending):
            await self._expire_pending_flow(user_id, message, pending)
            return

        incoming_text = (message.text or message.caption or "").strip()
        if pending and self._is_cancel_text(incoming_text):
            await self._cancel_pending_flow(user_id, message, "The current flow was canceled.")
            return

        if pending and pending.get("chat_id") == message.chat.id:
            lock = self.pending_locks.setdefault(user_id, asyncio.Lock())
            async with lock:
                latest = self.pending_inputs.get(user_id)
                if latest and latest.get("chat_id") == message.chat.id:
                    await self._handle_pending_input(message, latest)
            return

        if self._is_cancel_text(incoming_text):
            await message.reply_text("There is no active flow to cancel.")
            return

        if await self._maybe_offer_contextual_quick_actions(message):
            return

        if message.text and message.text.startswith("/"):
            text, session_ready, groups, sources = await self._dm_home_text()
            await message.reply_text(
                text,
                reply_markup=dm_admin_menu(session_ready, groups, sources, show_admin_menu=True),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

    async def on_group_message(self, client: Client, message: Message) -> None:
        await self._ensure_group_registered(message.chat.id, message.chat)

        if not message.from_user:
            return

        user_id = message.from_user.id
        if not await self._is_owner(user_id):
            return

        addressed = self._is_group_message_addressed(message)
        if not addressed:
            return

        if self._is_menu_command_for_bot(message) or self._is_message_mentioning_bot(message):
            await self._ensure_group_registered(message.chat.id, message.chat)
            await message.reply_text(
                "This destination is registered. Open the bot DM and use /start to manage sources, filters, and settings there."
            )
            return

    async def on_my_chat_member_update(self, update: Update, member_update: Any) -> None:
        chat = getattr(member_update, "chat", None)
        if chat is None:
            return

        chat_type = str(getattr(chat, "type", ""))
        if chat_type not in {"group", "supergroup"}:
            return

        old_member = getattr(member_update, "old_chat_member", None)
        new_member = getattr(member_update, "new_chat_member", None)
        if new_member is None:
            return

        old_can_send = self._can_send_to_chat(old_member) if old_member is not None else False
        new_can_send = self._can_send_to_chat(new_member)

        chat_username = self._normalize_username(getattr(chat, "username", None))
        title = getattr(chat, "title", None) or (f"@{chat_username}" if chat_username else f"{chat.id}")
        chat_identity = self._identity_label(chat_username, getattr(chat, "id", None), html=True)
        if new_can_send and not old_can_send:
            await self._ensure_group_registered(chat.id, chat)
            await self._notify_owner(
                "<b>Destination Added</b>\n"
                f"{title}\n"
                f"Chat: {chat_identity}\n"
                "Open DM and tap Destinations to configure sources and filters.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎯 Go to Destinations", callback_data="dm:groups")]]),
            )
            return

        if old_can_send and not new_can_send:
            removed = await self._remove_group_registration(chat.id)
            if removed:
                await self._notify_owner(
                    "<b>Destination Removed</b>\n"
                    f"{title}\n"
                    f"Chat: {chat_identity}\n"
                    "The bot no longer has posting access in this group."
                )

    async def on_callback_query(self, client, callback_query) -> None:
        user = callback_query.from_user
        if not user:
            return

        if not await self._is_owner(user.id):
            await callback_query.answer("Only the bot owner can use this.", show_alert=True)
            return

        data = callback_query.data or ""
        if data == "noop":
            await callback_query.answer()
            return

        if data == "x:cancel":
            self.pending_inputs.pop(user.id, None)
            await self._set_live_events_message_id(None)
            await callback_query.answer("Canceled.")
            text, session_ready, groups, sources = await self._dm_home_text()
            await self._safe_edit_message_text(
                callback_query.message,
                text,
                reply_markup=dm_admin_menu(session_ready, groups, sources, show_admin_menu=True),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return

        if data.startswith("x:ia:"):
            token = data.split(":", 2)[2]
            action = self._pop_intent_action(token)
            if not action:
                await callback_query.answer("This action expired. Try again.", show_alert=True)
                return
            await self._execute_intent_action(callback_query, action)
            return

        if data in {"dm:home", "dm:status"}:
            await self._set_live_events_message_id(None)
            text, session_ready, groups, sources = await self._dm_home_text()
            await self._safe_edit_message_text(
                callback_query.message,
                text,
                reply_markup=dm_admin_menu(session_ready, groups, sources, show_admin_menu=True),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if data == "dm:events":
            text = await self._live_events_screen_text()
            await self._safe_edit_message_text(
                callback_query.message,
                text,
                reply_markup=dm_live_events_menu(),
            )
            try:
                await self._set_live_events_message_id(self._message_id(callback_query.message))
            except Exception:
                await self._set_live_events_message_id(None)
            await callback_query.answer()
            return

        if data == "dm:admin":
            await self._safe_edit_message_text(
                callback_query.message,
                await self._administration_screen_text(),
                reply_markup=dm_administration_menu(),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if data == "dm:admin:destinations:delete":
            destinations = await self._destination_entries()
            if not destinations:
                await callback_query.answer("No destinations to delete.", show_alert=True)
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._administration_screen_text(),
                    reply_markup=dm_administration_menu(),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return
            await self._safe_edit_message_text(
                callback_query.message,
                await self._admin_destination_delete_screen_text(),
                reply_markup=dm_destination_delete_menu(destinations),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if data.startswith("dm:admin:destinations:rm:"):
            raw_group_id = data.rsplit(":", 1)[-1]
            try:
                group_id = int(raw_group_id)
            except ValueError:
                await callback_query.answer("Invalid destination identifier.", show_alert=True)
                return

            removed = await self._remove_group_registration(group_id)
            history_removed = await self._clear_history(group_id)
            if removed:
                await callback_query.answer(
                    f"Destination deleted. Removed {history_removed} history entr{'y' if history_removed == 1 else 'ies'}."
                )
            else:
                await callback_query.answer("Destination not found.", show_alert=True)

            remaining_destinations = await self._destination_entries()
            if remaining_destinations:
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._admin_destination_delete_screen_text(),
                    reply_markup=dm_destination_delete_menu(remaining_destinations),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            else:
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._administration_screen_text(),
                    reply_markup=dm_administration_menu(),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            return

        if data == "dm:groups":
            destinations = await self._destination_entries()
            await self._safe_edit_message_text(
                callback_query.message,
                await self._destinations_screen_text(),
                reply_markup=dm_destinations_menu(destinations),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if data.startswith("dm:group:"):
            group_id = int(data.split(":", 2)[2])
            await self._ensure_group_registered(group_id)
            await self._safe_edit_message_text(
                callback_query.message,
                await self._destination_screen_text(group_id),
                reply_markup=group_main_menu(group_id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
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
            await callback_query.answer("Invalid action.")
            return

        group_id = int(parts[1])
        action = parts[2]

        await self._ensure_group_registered(group_id)

        if action == "back_main":
            await self._safe_edit_message_text(
                callback_query.message,
                await self._destination_screen_text(group_id),
                reply_markup=group_main_menu(group_id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if action == "sources":
            page = 0
            if len(parts) >= 4:
                try:
                    page = int(parts[3])
                except (TypeError, ValueError):
                    page = 0

            autosync_result = {"added": 0}
            if len(parts) == 3:
                autosync_result = await self._autosync_group_sources(group_id)

            state = await self._state()
            group_state = state.get("groups", {}).get(str(group_id), default_group_state())
            sources = group_state.get("sources", {})
            page_size = self._sources_screen_page_size()
            total_sources = len(sources)
            page_count = max(1, (total_sources + page_size - 1) // page_size)
            page = max(0, min(page, page_count - 1))
            await self._safe_edit_message_text(
                callback_query.message,
                await self._sources_screen_text(group_id, page=page, page_size=page_size),
                reply_markup=source_actions_menu(group_id, bool(sources), page=page, page_count=page_count),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            if autosync_result.get("added"):
                await callback_query.answer(f"Auto-synced {autosync_result['added']} new source{'s' if autosync_result['added'] != 1 else ''}.")
            else:
                await callback_query.answer()
            return

        if action == "bulkadd":
            user_id = int(callback_query.from_user.id)

            async def _render_bulk_screen() -> None:
                text, sess = await self._bulk_source_import_screen_text(user_id, group_id)
                categories = self._bulk_import_categories(sess)
                await self._safe_edit_message_text(
                    callback_query.message,
                    text,
                    reply_markup=bulk_source_import_menu(
                        group_id,
                        categories,
                        bool(sess.get("auto_sync_enabled")),
                        len(sess.get("selected_keys", set())),
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

            if len(parts) == 3:
                await self._ensure_bulk_import_session(user_id, group_id, refresh=True)
                await _render_bulk_screen()
                await callback_query.answer()
                return

            mode = parts[3]
            session = await self._ensure_bulk_import_session(user_id, group_id)

            if mode == "noop":
                await callback_query.answer()
                return

            if mode == "refresh":
                session = await self._ensure_bulk_import_session(user_id, group_id, refresh=True)
            elif mode == "cat":
                if len(parts) < 5:
                    await callback_query.answer()
                    return
                cat_key = parts[4]
                cat_candidates = self._category_candidates(session, cat_key)
                cat_keys = {source_key(int(c["chat_id"]), c.get("topic_id")) for c in cat_candidates}
                selected = set(session.get("selected_keys", set()))
                if cat_keys and cat_keys <= selected:
                    selected -= cat_keys
                else:
                    selected |= cat_keys
                session["selected_keys"] = selected
                self._bulk_import_sessions[self._bulk_import_session_key(user_id, group_id)] = session
            elif mode == "autosync":
                await self._update_source_import_config(group_id, auto_sync_enabled=not bool(session.get("auto_sync_enabled")))
                session = await self._ensure_bulk_import_session(user_id, group_id, refresh=False)
            if mode == "run":
                if self.user_client is None:
                    await callback_query.answer("User session is not ready.", show_alert=True)
                    return

                selected_keys = set(session.get("selected_keys", set()))
                if not selected_keys:
                    await callback_query.answer("No sources are selected.", show_alert=True)
                    return

                result = await self._bulk_add_selected_sources(group_id, selected_keys)
                self._bulk_import_sessions.pop(self._bulk_import_session_key(user_id, group_id), None)
                updated_state = await self._state()
                sources = updated_state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
                await self._safe_edit_message_text(
                    callback_query.message,
                    (
                        f"Imported <b>{result['added']}</b> source(s) from <b>{result['eligible']}</b> selected source(s).\n\n"
                        f"{await self._sources_screen_text(group_id)}"
                    ),
                    reply_markup=source_actions_menu(group_id, bool(sources)),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await callback_query.answer(f"Added {result['added']} source{'s' if result['added'] != 1 else ''}.")
                return

            await _render_bulk_screen()
            await callback_query.answer()
            return

        if action == "history":
            state = await self._state()
            sources = state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
            if len(parts) == 3:
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._history_screen_text(group_id),
                    reply_markup=history_actions_menu(group_id, bool(sources)),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await callback_query.answer()
                return

            mode = parts[3]
            if mode == "all":
                removed = await self._clear_history(group_id)
                await callback_query.answer(f"Removed {removed} history entr{'y' if removed == 1 else 'ies'}.")
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._history_screen_text(group_id),
                    reply_markup=history_actions_menu(group_id, bool(sources)),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return

            if mode == "source":
                await self._show_history_source_selector(callback_query, group_id)
                return

            await callback_query.answer("Unknown history option.")
            return

        if action == "add":
            self._set_pending_input(
                callback_query.from_user.id,
                {
                    "kind": "add_source",
                    "group_id": group_id,
                    "chat_id": callback_query.message.chat.id,
                },
            )
            pending = self.pending_inputs.get(callback_query.from_user.id, {})
            banner = await self._pending_context_banner(pending)
            await self._safe_edit_message_text(
                callback_query.message,
                f"{banner}\n\nSend one of:\n"
                "- a forwarded message from the source\n"
                "- a t.me link\n"
                "- a chat handle or ID\n\n"
                "Type /cancel anytime.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="x:cancel")]]),
            )
            await callback_query.answer()
            return

        if action == "remove":
            # Support pagination: g:<gid>:remove:<page>
            page = 0
            if len(parts) >= 4:
                try:
                    page = int(parts[3])
                except Exception:
                    page = 0
            await self._show_remove_source_menu(callback_query, group_id, page=page)
            return

        if action == "list":
            await self._show_sources_list(callback_query, group_id)
            return

        if action == "filters":
            state = await self._state()
            sources = state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
            await self._safe_edit_message_text(
                callback_query.message,
                await self._filters_screen_text(group_id),
                reply_markup=filters_root(group_id, bool(sources)),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if action == "reapply":
            await callback_query.answer("Reapplying filters")
            result = await self._reapply_filters_to_forwarded_messages(group_id)
            state = await self._state()
            sources = state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
            summary = (
                "\n\n<b>Last Reapply</b>\n"
                f"Scanned tracked messages: <b>{result['scanned']}</b>\n"
                f"Deleted from destination: <b>{result['deleted']}</b>\n"
                f"Skipped (delete failed): <b>{result['skipped']}</b>\n"
                f"History entries removed: <b>{result['history_removed']}</b>"
            )
            await self._safe_edit_message_text(
                callback_query.message,
                f"{await self._filters_screen_text(group_id)}{summary}",
                reply_markup=filters_root(group_id, bool(sources)),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return

        if action == "settings":
            await self._show_settings_menu(callback_query, group_id)
            return

        if action == "testsources":
            await self._run_source_tests(callback_query, group_id)
            return

        if action == "toggleset":
            if len(parts) < 4:
                await callback_query.answer("Invalid setting.")
                return
            setting = parts[3]
            await self._toggle_setting(callback_query, group_id, setting)
            return

        if action == "gf":
            if len(parts) == 3:
                await self._safe_edit_message_text(
                    callback_query.message,
                    await self._rules_screen_text(group_id, "gf", None),
                    reply_markup=rules_menu(group_id, "gf"),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
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

        if action == "sfpage":
            page = 0
            if len(parts) >= 4:
                try:
                    page = int(parts[3])
                except (TypeError, ValueError):
                    page = 0
            await self._show_source_filter_selector(callback_query, group_id, page=page)
            return

        if action == "sfsel":
            if len(parts) < 4:
                await callback_query.answer("Invalid source.")
                return
            s_key = parts[3]
            await self._safe_edit_message_text(
                callback_query.message,
                await self._rules_screen_text(group_id, "sf", s_key),
                reply_markup=rules_menu(group_id, "sf", s_key),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if action == "historysrc":
            if len(parts) < 4:
                await callback_query.answer("Invalid source.")
                return
            s_key = parts[3]
            removed = await self._clear_history(group_id, s_key)
            await callback_query.answer(f"Removed {removed} history entr{'y' if removed == 1 else 'ies'}.")
            await self._show_history_source_selector(callback_query, group_id)
            return

        if action == "historypage":
            page = 0
            if len(parts) >= 4:
                try:
                    page = int(parts[3])
                except (TypeError, ValueError):
                    page = 0
            await self._show_history_source_selector(callback_query, group_id, page=page)
            return

        if action == "rm":
            if len(parts) < 4:
                await callback_query.answer("Invalid source.")
                return
            s_key = parts[3]
            await self._remove_source(callback_query, group_id, s_key)
            return

        await callback_query.answer()

    async def _handle_rules_callback(self, callback_query, group_id: int, scope: str, tail: List[str]) -> None:
        command = tail[0] if tail else ""
        source_k = tail[1] if len(tail) > 1 else None

        if command == "add":
            await self._safe_edit_message_text(
                callback_query.message,
                "<b>Choose Filter Rule Type</b>",
                reply_markup=add_rule_types(group_id, scope, source_k),
                parse_mode=ParseMode.HTML,
            )
            await callback_query.answer()
            return

        if command == "type":
            if len(tail) < 2:
                await callback_query.answer("Rule type is missing.")
                return
            rule_type = tail[1]
            source_k = tail[2] if len(tail) > 2 else None

            await self._safe_edit_message_text(
                callback_query.message,
                "Should this rule <b>block</b> or <b>allow</b> messages?",
                reply_markup=rule_mode_selector(group_id, scope, source_k),
                parse_mode=ParseMode.HTML,
            )
            self._set_pending_input(
                callback_query.from_user.id,
                {
                    "kind": "add_rule_mode",
                    "group_id": group_id,
                    "scope": scope,
                    "source_key": source_k,
                    "rule_type": rule_type,
                    "chat_id": callback_query.message.chat.id,
                },
            )
            await callback_query.answer()
            return

        if command == "mode":
            if len(tail) < 2:
                await callback_query.answer("Invalid rule mode.")
                return
            rule_mode = tail[1]
            source_k = tail[2] if len(tail) > 2 else None

            pending = self.pending_inputs.get(callback_query.from_user.id, {})
            if (
                pending.get("kind") != "add_rule_mode"
                or int(pending.get("group_id", 0)) != group_id
                or str(pending.get("scope") or "") != scope
                or (pending.get("source_key") or None) != (source_k or None)
            ):
                await callback_query.answer("Rule type was not found.")
                return

            rule_type = str(pending.get("rule_type") or "")
            if not rule_type:
                await callback_query.answer("Rule type was not found.")
                return

            if rule_type == "has_link":
                yes_cb = f"g:{group_id}:{scope}:haslink:1:{rule_mode}"
                no_cb = f"g:{group_id}:{scope}:haslink:0:{rule_mode}"
                if source_k:
                    yes_cb = f"{yes_cb}:{source_k}"
                    no_cb = f"{no_cb}:{source_k}"
                await self._safe_edit_message_text(
                    callback_query.message,
                    "Choose link rule value:",
                    reply_markup=yes_no_buttons(yes_cb, no_cb),
                )
                await callback_query.answer()
                return

            self._set_pending_input(
                callback_query.from_user.id,
                {
                    "kind": "add_rule",
                    "group_id": group_id,
                    "scope": scope,
                    "source_key": source_k,
                    "rule_type": rule_type,
                    "rule_mode": rule_mode,
                    "chat_id": callback_query.message.chat.id,
                },
            )
            pending = self.pending_inputs.get(callback_query.from_user.id, {})
            banner = await self._pending_context_banner(pending)
            prompts = {
                "keyword": "Send comma-separated keywords (example: spam,ad,promo)",
                "exact": "Send exact message text to match",
                "message_type": "Send one type: text, photo, video, video_note, document, audio, voice, animation, sticker, poll, other",
                "sender": "Send a forwarded message, sender handles/usernames, or sender IDs (example: @username,123456789)",
            }
            await self._safe_edit_message_text(
                callback_query.message,
                f"{banner}\n\n{prompts.get(rule_type, 'Send rule value')}\n\nType /cancel anytime.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="x:cancel")]]),
            )
            await callback_query.answer()
            return

        if command == "haslink":
            if len(tail) < 2:
                await callback_query.answer("Invalid link-rule value.")
                return
            value_raw = tail[1].strip()
            rule_mode = tail[2] if len(tail) > 2 else "blocklist"
            source_k = tail[3] if len(tail) > 3 else None
            rule = {"type": "has_link", "value": value_raw == "1", "mode": rule_mode}
            await self._append_rule(group_id, scope, source_k, rule)
            self.pending_inputs.pop(callback_query.from_user.id, None)
            await callback_query.answer("Rule added.")
            await self._safe_edit_message_text(
                callback_query.message,
                await self._rules_screen_text(group_id, scope, source_k),
                reply_markup=rules_menu(group_id, scope, source_k),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return

        if command == "ls":
            await self._safe_edit_message_text(
                callback_query.message,
                await self._rules_screen_text(group_id, scope, source_k),
                reply_markup=rules_menu(group_id, scope, source_k),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        if command == "rm":
            await self._show_remove_rule_buttons(callback_query, group_id, scope, source_k)
            return

        if command == "del":
            if len(tail) < 2:
                await callback_query.answer("Invalid rule selection.")
                return
            index = int(tail[1])
            source_k = tail[2] if len(tail) > 2 else None
            await self._remove_rule(callback_query, group_id, scope, source_k, index)
            return

        await callback_query.answer()

    async def _show_settings_menu(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        settings = g.get("settings", {})
        admin_settings = state.get("admin_settings", {})
        await self._safe_edit_message_text(
            callback_query.message,
            await self._settings_screen_text(group_id),
            reply_markup=group_settings_menu(
                group_id,
                bool(settings.get("show_header", True)),
                bool(settings.get("show_link", True)),
                bool(settings.get("show_source_datetime", False)),
                bool(admin_settings.get("global_spam_dedupe_enabled", True)),
                bool(g.get("sources", {})),
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()

    async def _toggle_setting(self, callback_query, group_id: int, setting: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            if setting == "global_spam_dedupe_enabled":
                admin_settings = state.setdefault("admin_settings", {"global_spam_dedupe_enabled": True, "global_spam_dedupe_window_seconds": 10})
                current = bool(admin_settings.get(setting, True))
                admin_settings[setting] = not current
            else:
                groups = state.setdefault("groups", {})
                g = groups.setdefault(str(group_id), default_group_state())
                settings = g.setdefault("settings", {"show_header": True, "show_link": True, "show_source_datetime": False})
                current = bool(settings.get(setting, True))
                settings[setting] = not current
            return state

        await self.storage.update(updater)
        await self._show_settings_menu(callback_query, group_id)

    async def _show_sources_list(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        await self._safe_edit_message_text(
            callback_query.message,
            await self._sources_screen_text(group_id),
            reply_markup=source_actions_menu(group_id, bool(sources)),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()

    async def _show_remove_source_menu(self, callback_query, group_id: int, page: int = 0) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.answer("No sources are configured yet.", show_alert=True)
            return

        choices = [(key, self._source_display_name(key, src)[:48]) for key, src in self._sorted_sources(g)]
        page_size = self._selector_page_size()
        total = len(choices)
        page_count = max(1, (total + page_size - 1) // page_size)
        page = max(0, min(page, page_count - 1))
        start = page * page_size
        end = min(start + page_size, total)

        await self._safe_edit_message_text(
            callback_query.message,
            (
                "<b>Select a Source to Remove</b>\n"
                f"Showing <b>{start + 1}-{end}</b> of <b>{total}</b>"
            ),
            reply_markup=source_remove_menu(group_id, choices, page=page, page_size=page_size),
            parse_mode=ParseMode.HTML,
        )
        await callback_query.answer()

    async def _remove_source(self, callback_query, group_id: int, source_k: str) -> None:
        result = await self._remove_source_from_destination(group_id, source_k)
        removed_source = dict(result.get("source") or {})
        deleted = int(result.get("deleted", 0) or 0)
        skipped = int(result.get("skipped", 0) or 0)

        panel_text, panel_markup = await self._home_panel_payload(
            (
                f"Status: source removed from destination. Deleted <b>{deleted}</b> forwarded message(s)"
                + (f", failed <b>{skipped}</b>." if skipped else ".")
            )
        )
        await self._safe_edit_message_text(
            callback_query.message,
            panel_text,
            reply_markup=panel_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        if skipped:
            await callback_query.answer(
                f"Source removed. Deleted {deleted} messages, {skipped} failed.",
                show_alert=True,
            )
        else:
            await callback_query.answer(f"Source removed. Deleted {deleted} messages.")

        await self._offer_leave_source_prompt_if_orphaned(callback_query, removed_source)

    async def _show_source_filter_selector(self, callback_query, group_id: int, page: int = 0) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.answer("No sources are configured yet.", show_alert=True)
            return

        choices = [(key, self._source_display_name(key, src)[:48]) for key, src in self._sorted_sources(g)]
        page_size = self._selector_page_size()
        total = len(choices)
        page_count = max(1, (total + page_size - 1) // page_size)
        page = max(0, min(page, page_count - 1))
        start = page * page_size
        end = min(start + page_size, total)
        await self._safe_edit_message_text(
            callback_query.message,
            (
                "<b>Select a Source Filter Set</b>\n"
                f"Showing <b>{start + 1}-{end}</b> of <b>{total}</b>\n"
                "Numbers are positional and can shift after removals."
            ),
            reply_markup=source_filter_selector_menu_paginated(group_id, choices, page=page, page_size=page_size),
            parse_mode=ParseMode.HTML,
        )
        await callback_query.answer()

    async def _show_history_source_selector(self, callback_query, group_id: int, page: int = 0) -> None:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        history = await self._group_forward_history(group_id)
        ranked_sources = self._history_source_choices(group_state, history)
        if not ranked_sources:
            sources = group_state.get("sources", {})
            await self._safe_edit_message_text(
                callback_query.message,
                await self._history_source_selector_text(group_id),
                reply_markup=history_actions_menu(group_id, bool(sources)),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
            return

        choices = [(key, label[:48]) for key, label, _ in ranked_sources]
        page_size = self._selector_page_size()
        total = len(choices)
        page_count = max(1, (total + page_size - 1) // page_size)
        page = max(0, min(page, page_count - 1))
        start = page * page_size
        end = min(start + page_size, total)
        await self._safe_edit_message_text(
            callback_query.message,
            (
                f"{await self._history_source_selector_text(group_id)}\n\n"
                f"Showing <b>{start + 1}-{end}</b> of <b>{total}</b>"
            ),
            reply_markup=history_source_selector_menu_paginated(group_id, choices, page=page, page_size=page_size),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()

    async def _clear_history(self, group_id: int, source_k: Optional[str] = None) -> int:
        removed = 0

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal removed
            group_history = state.get(str(group_id))
            if not isinstance(group_history, dict) or not group_history:
                return state

            if source_k is None:
                removed = len(group_history)
                state.pop(str(group_id), None)
                return state

            for message_id in list(group_history.keys()):
                entry = group_history.get(message_id)
                if self._entry_matches_source(entry, str(source_k)):
                    group_history.pop(message_id, None)
                    removed += 1

            if not group_history:
                state.pop(str(group_id), None)
            return state

        await self.forward_log_storage.update(updater)
        return removed

    def _build_logged_message_stub(self, entry: Dict[str, Any]) -> Any:
        sender_id = entry.get("sender_id")
        from_user = None
        sender_chat = None
        if isinstance(sender_id, int):
            if sender_id < 0:
                sender_chat = SimpleNamespace(id=sender_id)
            else:
                from_user = SimpleNamespace(id=sender_id)

        message_type = str(entry.get("message_type") or "").strip().lower()
        if not message_type:
            message_type = "text" if entry.get("text") else "other"

        fields = {
            "text": "",
            "caption": None,
            "photo": None,
            "video": None,
            "document": None,
            "audio": None,
            "voice": None,
            "animation": None,
            "sticker": None,
            "poll": None,
            "from_user": from_user,
            "sender_chat": sender_chat,
        }

        if message_type == "text":
            fields["text"] = str(entry.get("text") or "")
        elif message_type in fields:
            fields["caption"] = str(entry.get("text") or "")
            fields[message_type] = True
        else:
            fields["text"] = str(entry.get("text") or "")

        return SimpleNamespace(**fields)

    async def _safe_delete_destination_message(self, group_id: int, destination_message_id: int) -> bool:
        if self.bot is None:
            return False

        try:
            await self.bot.delete_message(chat_id=group_id, message_id=destination_message_id)
            return True
        except RetryAfter as err:
            wait_seconds = int(getattr(err, "retry_after", 1) or 1)
            await asyncio.sleep(max(wait_seconds, 1))
            try:
                await self.bot.delete_message(chat_id=group_id, message_id=destination_message_id)
                return True
            except Exception as exc:
                logger.warning(
                    "Delete after retry failed | group_id=%s | message_id=%s | error=%s",
                    group_id,
                    destination_message_id,
                    exc,
                )
                return False
        except BadRequest as exc:
            if "message to delete not found" in str(exc).lower():
                return True
            logger.warning(
                "Delete failed with bad request | group_id=%s | message_id=%s | error=%s",
                group_id,
                destination_message_id,
                exc,
            )
            return False
        except Exception as exc:
            logger.warning(
                "Delete failed | group_id=%s | message_id=%s | error=%s",
                group_id,
                destination_message_id,
                exc,
            )
            return False

    async def _drop_history_entries(self, group_id: int, destination_message_ids: List[str]) -> int:
        if not destination_message_ids:
            return 0

        removed = 0
        ids_to_remove = {str(message_id) for message_id in destination_message_ids}

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal removed
            group_history = state.get(str(group_id))
            if not isinstance(group_history, dict) or not group_history:
                return state

            for message_id in list(group_history.keys()):
                if message_id in ids_to_remove:
                    group_history.pop(message_id, None)
                    removed += 1

            if not group_history:
                state.pop(str(group_id), None)

            return state

        await self.forward_log_storage.update(updater)
        return removed

    async def _reapply_filters_to_forwarded_messages(self, group_id: int) -> Dict[str, int]:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        group_filters = group_state.get("group_filters", {"rules": []})
        sources = group_state.get("sources", {})

        history = await self._group_forward_history(group_id)
        scanned = 0
        deleted = 0
        skipped = 0
        to_drop: List[str] = []

        for destination_message_id, entry in history.items():
            if not isinstance(entry, dict):
                to_drop.append(str(destination_message_id))
                continue

            scanned += 1
            source_k = entry.get("source_key")
            message_stub = self._build_logged_message_stub(entry)

            if not source_k:
                should_delete = True
            else:
                source = sources.get(str(source_k))
                if not isinstance(source, dict):
                    should_delete = True
                else:
                    group_ok = evaluate_filters(group_filters, message_stub)
                    source_ok = evaluate_filters(source.get("filters", {"rules": []}), message_stub)
                    should_delete = not (group_ok and source_ok)

            if not should_delete:
                continue

            try:
                message_id_value = int(destination_message_id)
            except (TypeError, ValueError):
                to_drop.append(str(destination_message_id))
                continue

            if await self._safe_delete_destination_message(group_id, message_id_value):
                deleted += 1
                to_drop.append(str(destination_message_id))
            else:
                skipped += 1

        history_removed = await self._drop_history_entries(group_id, to_drop)
        return {
            "scanned": scanned,
            "deleted": deleted,
            "skipped": skipped,
            "history_removed": history_removed,
        }

    async def _delete_forwarded_history_for_source(self, group_id: int, source_k: str) -> Dict[str, int]:
        history = await self._group_forward_history(group_id)
        scanned = 0
        deleted = 0
        skipped = 0
        to_drop: List[str] = []

        for destination_message_id, entry in history.items():
            if not isinstance(entry, dict) or entry.get("source_key") != source_k:
                continue

            scanned += 1
            try:
                message_id_value = int(destination_message_id)
            except (TypeError, ValueError):
                to_drop.append(str(destination_message_id))
                continue

            if await self._safe_delete_destination_message(group_id, message_id_value):
                deleted += 1
                to_drop.append(str(destination_message_id))
            else:
                skipped += 1

        history_removed = await self._drop_history_entries(group_id, to_drop)
        return {
            "scanned": scanned,
            "deleted": deleted,
            "skipped": skipped,
            "history_removed": history_removed,
        }

    async def _filter_target(self, state: Dict[str, Any], group_id: int, scope: str, source_k: Optional[str]) -> Dict[str, Any]:
        g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
        if scope == "gf":
            return g.setdefault("group_filters", {"rules": []})
        if not source_k:
            return {"rules": []}
        src = g.setdefault("sources", {}).setdefault(source_k, {})
        return src.setdefault("filters", {"rules": []})

    async def _list_rules(self, callback_query, group_id: int, scope: str, source_k: Optional[str]) -> None:
        await self._safe_edit_message_text(
            callback_query.message,
            await self._rules_screen_text(group_id, scope, source_k),
            reply_markup=rules_menu(group_id, scope, source_k),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()

    async def _show_remove_rule_buttons(self, callback_query, group_id: int, scope: str, source_k: Optional[str]) -> None:
        state = await self._state()
        target = await self._filter_target(state, group_id, scope, source_k)
        rules = target.get("rules", [])
        if not rules:
            await callback_query.answer("No rules are configured yet.", show_alert=True)
            return

        buttons = []
        for idx, rule in enumerate(rules):
            label = f"Delete #{idx + 1} {rule.get('type', 'rule')}"[:56]
            if source_k:
                cb = f"g:{group_id}:{scope}:del:{idx}:{source_k}"
            else:
                cb = f"g:{group_id}:{scope}:del:{idx}"
            buttons.append([InlineKeyboardButton(label, callback_data=cb)])
        back_callback = f"g:{group_id}:gf" if scope == "gf" else f"g:{group_id}:sfsel:{source_k}"
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=back_callback)])

        await self._safe_edit_message_text(
            callback_query.message,
            "<b>Select a Rule to Remove</b>",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML,
        )
        await callback_query.answer()

    async def _remove_rule(self, callback_query, group_id: int, scope: str, source_k: Optional[str], index: int) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            if scope == "gf":
                target = g.setdefault("group_filters", {"rules": []})
            else:
                if not source_k:
                    return state
                src = g.setdefault("sources", {}).setdefault(source_k, {})
                target = src.setdefault("filters", {"rules": []})
            rules = target.setdefault("rules", [])
            if 0 <= index < len(rules):
                rules.pop(index)
            return state

        await self.storage.update(updater)
        await callback_query.answer("Rule removed.")
        await self._safe_edit_message_text(
            callback_query.message,
            await self._rules_screen_text(group_id, scope, source_k),
            reply_markup=rules_menu(group_id, scope, source_k),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _upsert_source(self, group_id: int, source: Dict[str, Any]) -> Tuple[str, bool]:
        s_key = source_key(int(source["chat_id"]), source.get("topic_id"))
        existed = False

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal existed
            groups = state.setdefault("groups", {})
            g = groups.setdefault(str(group_id), default_group_state())
            sources = g.setdefault("sources", {})
            existed = s_key in sources
            sources[s_key] = {
                "chat_id": source["chat_id"],
                "topic_id": source.get("topic_id"),
                "name": source.get("name"),
                "username": source.get("username"),
                "type": source.get("type"),
                "filters": sources.get(s_key, {}).get("filters", {"rules": []}),
            }
            return state

        await self.storage.update(updater)
        return s_key, existed

    async def _resolve_entity_from_text_for_intent(self, text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if self.user_client is None:
            return None, "User session is not ready."

        raw = (text or "").strip()
        if not raw:
            return None, "Empty input"

        # Numeric ID can represent user/chat/channel.
        try:
            numeric_id = int(raw)
            entity = await self.user_client.get_chat(numeric_id)
            chat_type = self._chat_type_name(entity)
            if chat_type in {"private", "bot"}:
                return {
                    "kind": "user",
                    "id": int(entity.id),
                    "label": self._identity_label(getattr(entity, "username", None), int(entity.id)),
                }, None
            return {
                "kind": "chat",
                "source": self._source_from_chat_entity(entity),
            }, None
        except ValueError:
            pass
        except Exception:
            pass

        link_match = re.search(r"https?://t\.me/[^\s]+|t\.me/[^\s]+", raw, flags=re.IGNORECASE)
        handle_match = re.fullmatch(r"@?([A-Za-z][A-Za-z0-9_]{3,31})", raw)
        if not link_match and not handle_match:
            return None, "Send a valid Telegram link, handle, or numeric ID."

        if link_match:
            source, err = await self._resolve_source_from_tme_link(link_match.group(0))
            if err:
                return None, f"Could not resolve entity: {err}"
            if not source:
                return None, "Could not parse link"

            source_type = self._chat_type_name(source.get("type"))
            if source_type in {"private", "bot"}:
                source_id = int(source.get("chat_id"))
                return {
                    "kind": "user",
                    "id": source_id,
                    "label": self._identity_label(source.get("username"), source_id),
                }, None

            return {"kind": "chat", "source": source}, None

        target = handle_match.group(1)
        try:
            entity = await self.user_client.get_chat(target)
        except Exception as exc:
            return None, f"Could not resolve entity: {exc}"

        chat_type = self._chat_type_name(entity)
        if chat_type in {"private", "bot"}:
            return {
                "kind": "user",
                "id": int(entity.id),
                "label": self._identity_label(getattr(entity, "username", None), int(entity.id)),
            }, None

        return {
            "kind": "chat",
            "source": self._source_from_chat_entity(entity),
        }, None

    async def _execute_intent_action(self, callback_query, action: Dict[str, Any]) -> None:
        action_type = str(action.get("type") or "")
        user_id = callback_query.from_user.id

        if action_type == "resume_pending":
            pending = action.get("pending")
            if not isinstance(pending, dict):
                await callback_query.answer("This flow cannot be resumed.", show_alert=True)
                return
            pending_copy = dict(pending)
            pending_copy.pop("created_at", None)
            pending_copy.pop("expires_at", None)
            self._set_pending_input(user_id, pending_copy)
            banner = await self._pending_context_banner(pending_copy)
            msg = "Resumed. Continue with the previous step."
            if banner:
                msg = f"{msg}\n\n{banner}"
            await self._safe_edit_message_text(
                callback_query.message,
                msg,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="x:cancel")]]),
            )
            await callback_query.answer("Resumed.")
            return

        if action_type == "start_add_source":
            group_id = int(action["group_id"])
            self._set_pending_input(
                user_id,
                {
                    "kind": "add_source",
                    "group_id": group_id,
                    "chat_id": callback_query.message.chat.id,
                },
            )
            pending = self.pending_inputs.get(user_id, {})
            banner = await self._pending_context_banner(pending)
            await self._safe_edit_message_text(
                callback_query.message,
                f"{banner}\n\nSend one of:\n- a forwarded message from the source\n- a t.me link\n- a chat handle or ID\n\nType /cancel anytime.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="x:cancel")]]),
            )
            await callback_query.answer()
            return

        if action_type == "add_source_direct":
            group_id = int(action["group_id"])
            source = dict(action.get("source") or {})
            if not source:
                await callback_query.answer("Source data is missing.", show_alert=True)
                return
            s_key, existed = await self._upsert_source(group_id, source)
            source_identity = self._source_identity(s_key, source)
            source_name = self._source_display_name(s_key, source)
            prefix = "Already existed" if existed else "Added source"
            panel_text, panel_markup = await self._home_panel_payload(
                f"Status: {prefix}: <b>{escape(source_name)}</b> ({source_identity})"
            )
            await self._safe_edit_message_text(
                callback_query.message,
                panel_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=panel_markup,
            )
            await callback_query.answer("Done.")
            return

        if action_type == "add_sender_rule":
            group_id = int(action["group_id"])
            scope = str(action.get("scope") or "gf")
            source_k = action.get("source_key")
            sender_id = int(action["sender_id"])
            await self._append_rule(
                group_id,
                scope,
                source_k,
                {"type": "sender", "values": [sender_id], "mode": "blocklist"},
            )
            panel_text, panel_markup = await self._home_panel_payload("Status: sender filter rule added.")
            await self._safe_edit_message_text(
                callback_query.message,
                panel_text,
                reply_markup=panel_markup,
                parse_mode=ParseMode.HTML,
            )
            await callback_query.answer("Rule added.")
            return

        if action_type == "add_exact_rule":
            group_id = int(action["group_id"])
            scope = str(action.get("scope") or "gf")
            source_k = action.get("source_key")
            text_value = str(action.get("text") or "").strip()
            if not text_value:
                await callback_query.answer("There is no text available for this rule.", show_alert=True)
                return
            await self._append_rule(
                group_id,
                scope,
                source_k,
                {"type": "exact", "value": text_value, "mode": "blocklist"},
            )
            panel_text, panel_markup = await self._home_panel_payload("Status: exact-text filter rule added.")
            await self._safe_edit_message_text(
                callback_query.message,
                panel_text,
                reply_markup=panel_markup,
                parse_mode=ParseMode.HTML,
            )
            await callback_query.answer("Rule added.")
            return

        if action_type == "remove_source_everywhere":
            targets = action.get("targets") or []
            if not isinstance(targets, list) or not targets:
                await callback_query.answer("No targets found.", show_alert=True)
                return
            removed_count = 0
            deleted_messages = 0
            failed_deletes = 0
            sample_source: Dict[str, Any] = {}
            for item in targets:
                if not isinstance(item, dict):
                    continue
                try:
                    gid = int(item.get("group_id"))
                except (TypeError, ValueError):
                    continue
                s_key = str(item.get("source_key") or "").strip()
                if not s_key:
                    continue
                result = await self._remove_source_from_destination(gid, s_key)
                if result.get("removed"):
                    removed_count += 1
                    sample_source = sample_source or dict(result.get("source") or {})
                deleted_messages += int(result.get("deleted", 0) or 0)
                failed_deletes += int(result.get("skipped", 0) or 0)

            panel_text, panel_markup = await self._home_panel_payload(
                (
                    f"Status: removed source from <b>{removed_count}</b> destination(s). "
                    f"Deleted <b>{deleted_messages}</b> forwarded message(s)"
                    + (f", failed <b>{failed_deletes}</b>." if failed_deletes else ".")
                )
            )
            await self._safe_edit_message_text(
                callback_query.message,
                panel_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=panel_markup,
            )
            await callback_query.answer("Done.")
            await self._offer_leave_source_prompt_if_orphaned(callback_query, sample_source)
            return

        if action_type == "add_sender_rule_bulk":
            targets = action.get("targets") or []
            if not isinstance(targets, list) or not targets:
                await callback_query.answer("No targets found.", show_alert=True)
                return
            applied = 0
            for item in targets:
                if not isinstance(item, dict):
                    continue
                try:
                    gid = int(item.get("group_id"))
                    sender_id = int(item.get("sender_id"))
                except (TypeError, ValueError):
                    continue
                await self._append_rule(
                    gid,
                    "gf",
                    None,
                    {"type": "sender", "values": [sender_id], "mode": "blocklist"},
                )
                applied += 1
            panel_text, panel_markup = await self._home_panel_payload(
                f"Status: sender rule added to <b>{applied}</b> destination(s)."
            )
            await self._safe_edit_message_text(
                callback_query.message,
                panel_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=panel_markup,
            )
            await callback_query.answer("Done.")
            return

        if action_type == "leave_source_chat":
            if self.user_client is None:
                await callback_query.answer("User client is not connected.", show_alert=True)
                return
            try:
                chat_id = int(action.get("chat_id"))
            except (TypeError, ValueError):
                await callback_query.answer("Invalid chat id.", show_alert=True)
                return
            try:
                await self.user_client.leave_chat(chat_id)
            except Exception as exc:
                await callback_query.answer(f"Could not leave chat: {exc}", show_alert=True)
                return
            panel_text, panel_markup = await self._home_panel_payload("Status: left unused source chat.")
            await self._safe_edit_message_text(
                callback_query.message,
                panel_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=panel_markup,
            )
            await callback_query.answer("Left chat.")
            return

        await callback_query.answer("Unknown action.", show_alert=True)

    async def _home_panel_payload(self, status_line: Optional[str] = None) -> Tuple[str, Any]:
        text, session_ready, groups, sources = await self._dm_home_text()
        if status_line:
            text = f"{status_line}\n\n{text}"
        return text, dm_admin_menu(session_ready, groups, sources, show_admin_menu=True)

    async def _source_usage_locations(self, chat_id: int, topic_id: Optional[int]) -> List[Tuple[int, str]]:
        state = await self._state()
        hits: List[Tuple[int, str]] = []
        for gid_raw, g in state.get("groups", {}).items():
            try:
                gid = int(gid_raw)
            except (TypeError, ValueError):
                continue
            for s_key, src in g.get("sources", {}).items():
                try:
                    src_chat_id = int(src.get("chat_id"))
                except (TypeError, ValueError):
                    continue
                src_topic_id = src.get("topic_id")
                if src_chat_id == int(chat_id) and (src_topic_id or None) == (topic_id or None):
                    hits.append((gid, s_key))
        return hits

    async def _remove_source_from_destination(self, group_id: int, source_k: str) -> Dict[str, Any]:
        cleanup = await self._delete_forwarded_history_for_source(group_id, source_k)
        removed_source: Dict[str, Any] = {}

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal removed_source
            groups = state.setdefault("groups", {})
            g = groups.setdefault(str(group_id), default_group_state())
            removed_source = dict(g.setdefault("sources", {}).pop(source_k, {}) or {})
            return state

        await self.storage.update(updater)
        return {
            "removed": bool(removed_source),
            "source": removed_source,
            "deleted": cleanup.get("deleted", 0),
            "skipped": cleanup.get("skipped", 0),
            "history_removed": cleanup.get("history_removed", 0),
        }

    async def _offer_leave_source_prompt_if_orphaned(self, callback_query, source: Dict[str, Any]) -> None:
        if not source or self.user_client is None:
            return
        try:
            chat_id = int(source.get("chat_id"))
        except (TypeError, ValueError):
            return
        topic_id = source.get("topic_id")
        if await self._source_usage_locations(chat_id, topic_id):
            return

        token = self._store_intent_action({"type": "leave_source_chat", "chat_id": chat_id}, ttl_seconds=300)
        source_name = self._source_display_name(source_key(chat_id, topic_id), source)
        await callback_query.message.reply_text(
            (
                f"Source <b>{escape(source_name)}</b> is no longer used in any destination.\n"
                "Do you want to leave this chat/channel with your user account?"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🚪 Leave Chat", callback_data=f"x:ia:{token}")],
                    [InlineKeyboardButton("Keep", callback_data="x:cancel")],
                ]
            ),
        )

    def _extract_numeric_candidates(self, text: str) -> List[str]:
        if not text:
            return []
        found = re.findall(r"(?<!\d)-?\d{5,}(?!\d)", text)
        deduped: List[str] = []
        seen = set()
        for item in found:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped[:3]

    def _extract_tme_links_from_message(self, message: Message) -> List[str]:
        links: List[str] = []
        text_blob = f"{getattr(message, 'text', '') or ''}\n{getattr(message, 'caption', '') or ''}"
        for match in re.findall(r'https?://t\.me/[^\s<>"]+|t\.me/[^\s<>"]+', text_blob, flags=re.IGNORECASE):
            raw = match.strip().rstrip(".,;)")
            if not raw:
                continue
            if not raw.lower().startswith("http"):
                raw = "https://" + raw
            links.append(raw)

        for entity in list(getattr(message, "entities", []) or []) + list(getattr(message, "caption_entities", []) or []):
            entity_type = str(getattr(entity, "type", "")).lower()
            if "text_link" not in entity_type:
                continue
            url = str(getattr(entity, "url", "") or "").strip()
            if not url:
                continue
            if "t.me/" not in url.lower():
                continue
            links.append(url)

        deduped: List[str] = []
        seen = set()
        for link in links:
            norm = link.strip()
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append(norm)
        return deduped[:5]

    def _extract_forward_payload_hints(self, message: Message) -> Dict[str, Any]:
        text_blob = f"{getattr(message, 'text', '') or ''}\n{getattr(message, 'caption', '') or ''}".strip()
        usernames: set[str] = set()
        chat_ids: set[int] = set()
        message_ids: set[int] = set()

        # Header hints: "<source name> • @username" or "<source name> • -100..."
        first_line = (text_blob.splitlines()[0] if text_blob else "").strip()
        if first_line and "•" in first_line:
            for part in [p.strip() for p in first_line.split("•") if p.strip()]:
                token = part
                if "/ topic" in token:
                    token = token.split("/ topic", 1)[0].strip()
                if token.startswith("@"):
                    usernames.add(token.lstrip("@").lower())
                    continue
                if re.fullmatch(r"-?\d{5,}", token):
                    try:
                        chat_ids.add(int(token))
                    except (TypeError, ValueError):
                        pass

        for link in self._extract_tme_links_from_message(message):
            lowered = link.lower()
            try:
                path = link.split("t.me/", 1)[1]
            except IndexError:
                continue
            path = path.split("?", 1)[0].strip("/")
            parts = [p for p in path.split("/") if p]
            if not parts:
                continue

            # https://t.me/c/<internal>/<msg>[/...]
            if parts[0] == "c" and len(parts) >= 3 and parts[1].isdigit():
                try:
                    chat_ids.add(int(f"-100{int(parts[1])}"))
                except Exception:
                    pass
                if parts[2].isdigit():
                    message_ids.add(int(parts[2]))
                continue

            # https://t.me/<username>/<msg>
            username = parts[0].lstrip("@").lower()
            if re.fullmatch(r"[a-z][a-z0-9_]{3,31}", username):
                usernames.add(username)
            if len(parts) >= 2 and parts[1].isdigit():
                message_ids.add(int(parts[1]))

            # Keep compatibility with links embedded as Original Message text-link.
            if "original" in lowered and len(parts) >= 2 and parts[-1].isdigit():
                message_ids.add(int(parts[-1]))

        return {
            "usernames": usernames,
            "chat_ids": chat_ids,
            "message_ids": message_ids,
        }

    def _match_sources_from_hints(
        self,
        groups: Dict[str, Any],
        hints: Dict[str, Any],
        preferred_group_id: Optional[int] = None,
    ) -> List[Tuple[int, str]]:
        hint_usernames = set(hints.get("usernames", set()) or set())
        hint_chat_ids = set(hints.get("chat_ids", set()) or set())
        matches: List[Tuple[int, str]] = []

        for gid_raw, g in groups.items():
            try:
                gid = int(gid_raw)
            except (TypeError, ValueError):
                continue
            if preferred_group_id is not None and gid != int(preferred_group_id):
                continue
            for s_key, src in g.get("sources", {}).items():
                src_username = self._normalize_username(src.get("username"))
                src_username = src_username.lower() if src_username else None
                src_chat_id = src.get("chat_id")
                by_username = bool(src_username and src_username in hint_usernames)
                by_chat_id = False
                try:
                    by_chat_id = int(src_chat_id) in hint_chat_ids
                except (TypeError, ValueError):
                    by_chat_id = False
                if by_username or by_chat_id:
                    matches.append((gid, s_key))

        return matches

    def _possible_forward_message_ids(self, message: Message) -> List[str]:
        ids: List[str] = []
        direct = getattr(message, "forward_from_message_id", None)
        if direct is not None:
            ids.append(str(direct))

        origin = getattr(message, "forward_origin", None)
        if origin is not None:
            for attr_name in ("message_id", "source_message_id"):
                value = getattr(origin, attr_name, None)
                if value is not None:
                    ids.append(str(value))

        # Some Telegram clients preserve original message id in the API-level field.
        msg_id = getattr(message, "forward_date", None)
        if msg_id is not None and getattr(message, "forward_from_message_id", None) is None:
            # Keep only as a weak hint marker; no numeric use.
            pass

        deduped: List[str] = []
        seen = set()
        for item in ids:
            if not item or item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    async def _find_logged_forward_matches(self, message: Message) -> List[Tuple[int, str, Dict[str, Any]]]:
        candidates = self._possible_forward_message_ids(message)
        if not candidates:
            return []

        logs = await self._forward_logs_state()
        matches: List[Tuple[int, str, Dict[str, Any]]] = []
        for gid_raw, group_history in logs.items():
            if not isinstance(group_history, dict):
                continue
            for msg_id in candidates:
                entry = group_history.get(str(msg_id))
                if isinstance(entry, dict):
                    try:
                        matches.append((int(gid_raw), str(msg_id), entry))
                    except (TypeError, ValueError):
                        continue
        return matches

    async def _maybe_offer_contextual_quick_actions(self, message: Message) -> bool:
        text = (message.text or message.caption or "").strip()
        state = await self._state()
        groups = state.get("groups", {})
        if not groups:
            return False

        forwarded_chat = self._forwarded_chat(message)
        forwarded_user = self._forwarded_user(message)
        log_matches = await self._find_logged_forward_matches(message)
        payload_hints = self._extract_forward_payload_hints(message)
        preferred_group_id = None
        if forwarded_chat and str(getattr(forwarded_chat, "id", "")) in groups:
            preferred_group_id = int(forwarded_chat.id)
        source_hint_matches = self._match_sources_from_hints(groups, payload_hints, preferred_group_id=preferred_group_id)
        if not source_hint_matches:
            source_hint_matches = self._match_sources_from_hints(groups, payload_hints, preferred_group_id=None)
        action_rows: List[List[InlineKeyboardButton]] = []
        lines: List[str] = ["<b>Smart actions</b>"]

        # Intent detection: forwarded from a destination group.
        if forwarded_chat and str(getattr(forwarded_chat, "id", "")) in groups:
            group_id = int(forwarded_chat.id)
            group_state = groups.get(str(group_id), default_group_state())
            group_name = self._group_display_name(group_id, group_state)
            lines.append("")
            lines.append(f"Forward detected from destination: <b>{group_name}</b>")

            # Fast destination-management actions.
            action_rows.append([InlineKeyboardButton("📡 Open Sources", callback_data=f"g:{group_id}:sources")])
            action_rows.append([InlineKeyboardButton("🗑️ Remove Source", callback_data=f"g:{group_id}:remove")])
            action_rows.append([InlineKeyboardButton("🧰 Open Filters", callback_data=f"g:{group_id}:filters")])

            if text:
                token = self._store_intent_action(
                    {"type": "add_exact_rule", "group_id": group_id, "scope": "gf", "source_key": None, "text": text}
                )
                action_rows.append([InlineKeyboardButton("🚫 Block this exact text", callback_data=f"x:ia:{token}")])
            sender_id = None
            if forwarded_user and getattr(forwarded_user, "id", None) is not None:
                sender_id = int(forwarded_user.id)
            elif forwarded_chat and getattr(forwarded_chat, "id", None) is not None:
                sender_id = int(forwarded_chat.id)
            if sender_id is not None:
                token = self._store_intent_action(
                    {
                        "type": "add_sender_rule",
                        "group_id": group_id,
                        "scope": "gf",
                        "source_key": None,
                        "sender_id": sender_id,
                    }
                )
                action_rows.append([InlineKeyboardButton("🚫 Block this sender", callback_data=f"x:ia:{token}")])

        # Intent detection: forwarded chat/channel/group that is not configured as source.
        if forwarded_chat and str(getattr(forwarded_chat, "id", "")) not in groups:
            source_candidate = {
                "chat_id": int(forwarded_chat.id),
                "topic_id": None,
                "name": getattr(forwarded_chat, "title", None) or getattr(forwarded_chat, "username", None) or str(forwarded_chat.id),
                "username": getattr(forwarded_chat, "username", None),
                "type": str(getattr(forwarded_chat, "type", "")),
            }
            matching_groups: List[int] = []
            for gid_raw, g in groups.items():
                gid = int(gid_raw)
                srcs = g.get("sources", {})
                already_exists = any(int(src.get("chat_id", 0)) == int(source_candidate["chat_id"]) for src in srcs.values())
                if not already_exists:
                    matching_groups.append(gid)

            if matching_groups:
                lines.append("")
                lines.append("Detected a forwarded chat/channel not in sources.")
                for gid in matching_groups[:6]:
                    token = self._store_intent_action({"type": "add_source_direct", "group_id": gid, "source": source_candidate})
                    group_name = self._group_display_name(gid, groups.get(str(gid), default_group_state()))
                    action_rows.append([InlineKeyboardButton(f"➕ Add as source -> {group_name[:24]}", callback_data=f"x:ia:{token}")])

        # Fallback: forwarding from destination may still expose original source as origin.
        if forwarded_chat:
            source_hits: List[Tuple[int, str]] = []
            forwarded_chat_id = int(getattr(forwarded_chat, "id", 0) or 0)
            if forwarded_chat_id:
                for gid_raw, g in groups.items():
                    gid = int(gid_raw)
                    for s_key, src in g.get("sources", {}).items():
                        try:
                            if int(src.get("chat_id", 0)) == forwarded_chat_id:
                                source_hits.append((gid, s_key))
                        except Exception:
                            continue

            if source_hits:
                lines.append("")
                lines.append("This forward matches configured source(s).")
                for gid, s_key in source_hits[:4]:
                    group_state = groups.get(str(gid), default_group_state())
                    group_name = self._group_display_name(gid, group_state)
                    source_name = self._source_display_name(s_key, group_state.get("sources", {}).get(s_key, {}))
                    action_rows.append([InlineKeyboardButton(f"🧰 Source filters -> {group_name[:18]}", callback_data=f"g:{gid}:sfsel:{s_key}")])
                    action_rows.append([InlineKeyboardButton(f"🗑️ Remove {source_name[:20]}", callback_data=f"g:{gid}:rm:{s_key}")])

        # Strong signal: message maps to tracked forwarded history in destination(s).
        if log_matches:
            lines.append("")
            lines.append("Matched this message with tracked forwarded history.")
            bulk_sender_targets: List[Dict[str, Any]] = []
            for gid, _, entry in log_matches[:3]:
                gstate = groups.get(str(gid), default_group_state())
                gname = self._group_display_name(gid, gstate)
                action_rows.append([InlineKeyboardButton(f"🧰 Filters -> {gname[:24]}", callback_data=f"g:{gid}:filters")])
                action_rows.append([InlineKeyboardButton(f"📡 Sources -> {gname[:24]}", callback_data=f"g:{gid}:sources")])
                source_k = self._entry_source_key(entry)
                if source_k and source_k in gstate.get("sources", {}):
                    sname = self._source_display_name(source_k, gstate.get("sources", {}).get(source_k, {}))
                    action_rows.append([InlineKeyboardButton(f"🗑️ Remove {sname[:20]}", callback_data=f"g:{gid}:rm:{source_k}")])
                sender_id = entry.get("sender_id")
                if sender_id is not None:
                    bulk_sender_targets.append({"group_id": gid, "sender_id": int(sender_id)})
                    token = self._store_intent_action(
                        {
                            "type": "add_sender_rule",
                            "group_id": gid,
                            "scope": "gf",
                            "source_key": None,
                            "sender_id": int(sender_id),
                        }
                    )
                    action_rows.append([InlineKeyboardButton(f"🚫 Block sender -> {gname[:19]}", callback_data=f"x:ia:{token}")])

            if len(bulk_sender_targets) > 1:
                # Keep one sender id when all matched entries share same sender.
                sender_values = {item["sender_id"] for item in bulk_sender_targets}
                if len(sender_values) == 1:
                    sender_id = next(iter(sender_values))
                    token = self._store_intent_action(
                        {
                            "type": "add_sender_rule_bulk",
                            "targets": [{"group_id": item["group_id"], "sender_id": sender_id} for item in bulk_sender_targets],
                        }
                    )
                    action_rows.append([InlineKeyboardButton("🚫 Block sender in all matched destinations", callback_data=f"x:ia:{token}")])

        # Header/link extraction signal from curated payload itself.
        if source_hint_matches:
            lines.append("")
            lines.append("Extracted source hints from header/link payload.")
            unique_rows = []
            seen_pairs = set()
            bulk_targets: List[Dict[str, Any]] = []
            for gid, s_key in source_hint_matches[:6]:
                if (gid, s_key) in seen_pairs:
                    continue
                seen_pairs.add((gid, s_key))
                gstate = groups.get(str(gid), default_group_state())
                gname = self._group_display_name(gid, gstate)
                src = gstate.get("sources", {}).get(s_key, {})
                sname = self._source_display_name(s_key, src)
                bulk_targets.append({"group_id": gid, "source_key": s_key})
                unique_rows.append([InlineKeyboardButton(f"🧰 Source filters -> {gname[:18]}", callback_data=f"g:{gid}:sfsel:{s_key}")])
                unique_rows.append([InlineKeyboardButton(f"🗑️ Remove {sname[:20]} -> {gname[:14]}", callback_data=f"g:{gid}:rm:{s_key}")])
            if len(bulk_targets) > 1:
                token = self._store_intent_action({"type": "remove_source_everywhere", "targets": bulk_targets})
                unique_rows.insert(0, [InlineKeyboardButton("🧹 Remove Source From All Matched Destinations", callback_data=f"x:ia:{token}")])
            action_rows = unique_rows[:6] + action_rows

        # Text/link/handle/user-id disambiguation.
        if text and not forwarded_chat:
            entity, _ = await self._resolve_entity_from_text_for_intent(text)
            if entity is None:
                for candidate in self._extract_numeric_candidates(text):
                    entity, _ = await self._resolve_entity_from_text_for_intent(candidate)
                    if entity is not None:
                        break
            if entity and entity.get("kind") == "chat":
                source_candidate = dict(entity.get("source") or {})
                if source_candidate:
                    lines.append("")
                    lines.append("Resolved as chat/channel/group.")
                    for gid_raw, g in list(groups.items())[:8]:
                        gid = int(gid_raw)
                        srcs = g.get("sources", {})
                        already_exists = any(int(src.get("chat_id", 0)) == int(source_candidate.get("chat_id", 0)) for src in srcs.values())
                        if already_exists:
                            continue
                        token = self._store_intent_action({"type": "add_source_direct", "group_id": gid, "source": source_candidate})
                        group_name = self._group_display_name(gid, g)
                        action_rows.append([InlineKeyboardButton(f"➕ Add as source -> {group_name[:24]}", callback_data=f"x:ia:{token}")])
            elif entity and entity.get("kind") == "user":
                sender_id = int(entity.get("id"))
                lines.append("")
                lines.append("Resolved as user/account.")
                for gid_raw, g in list(groups.items())[:8]:
                    gid = int(gid_raw)
                    token = self._store_intent_action(
                        {"type": "add_sender_rule", "group_id": gid, "scope": "gf", "source_key": None, "sender_id": sender_id}
                    )
                    group_name = self._group_display_name(gid, g)
                    action_rows.append([InlineKeyboardButton(f"🚫 Add sender rule -> {group_name[:20]}", callback_data=f"x:ia:{token}")])

        # Low-confidence fallback: still show useful intent choices.
        if not action_rows and (forwarded_chat is not None or forwarded_user is not None or bool(text)):
            lines.append("")
            lines.append("Could not fully classify this message. Pick likely intent:")
            for gid_raw, g in list(groups.items())[:3]:
                gid = int(gid_raw)
                gname = self._group_display_name(gid, g)
                token = self._store_intent_action({"type": "start_add_source", "group_id": gid})
                action_rows.append([InlineKeyboardButton(f"➕ Add Source -> {gname[:22]}", callback_data=f"x:ia:{token}")])
                action_rows.append([InlineKeyboardButton(f"📡 Manage Sources -> {gname[:17]}", callback_data=f"g:{gid}:sources")])
                action_rows.append([InlineKeyboardButton(f"🧰 Manage Filters -> {gname[:17]}", callback_data=f"g:{gid}:filters")])

            if forwarded_user and getattr(forwarded_user, "id", None) is not None:
                sender_id = int(forwarded_user.id)
                for gid_raw, g in list(groups.items())[:3]:
                    gid = int(gid_raw)
                    token = self._store_intent_action(
                        {"type": "add_sender_rule", "group_id": gid, "scope": "gf", "source_key": None, "sender_id": sender_id}
                    )
                    gname = self._group_display_name(gid, g)
                    action_rows.append([InlineKeyboardButton(f"🚫 Block sender -> {gname[:20]}", callback_data=f"x:ia:{token}")])

        if not action_rows:
            return False

        action_rows.append([InlineKeyboardButton("❌ Cancel", callback_data="x:cancel")])
        await message.reply_text(
            "\n".join(lines + ["", "Choose what to do next:"]),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(action_rows[:8]),
        )
        return True

    async def _handle_pending_input(self, message: Message, pending: Dict[str, Any]) -> None:
        kind = pending.get("kind")
        if kind == "add_source":
            await self._handle_add_source_input(message, pending)
            return

        if kind == "add_rule":
            await self._handle_add_rule_input(message, pending)
            return

    def _source_from_chat_entity(self, chat: Any, topic_id: Optional[int] = None, join_link: Optional[str] = None) -> Dict[str, Any]:
        source: Dict[str, Any] = {
            "chat_id": chat.id,
            "topic_id": topic_id,
            "name": chat.title or chat.username or str(chat.id),
            "username": chat.username,
            "type": self._chat_type_name(chat),
        }
        if join_link:
            source["join_link"] = join_link
        return source

    def _normalize_tme_link(self, raw_link: str) -> str:
        link = (raw_link or "").strip()
        if not link.lower().startswith("http"):
            link = "https://" + link
        return link

    async def _resolve_source_from_tme_link(self, raw_link: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if self.user_client is None:
            return None, "User session is not ready. Configure it in terminal and restart the bot."

        link = self._normalize_tme_link(raw_link)
        if "t.me/" not in link.lower():
            return None, "Send a valid t.me link."

        try:
            path = link.split("t.me/", 1)[1]
            path = path.split("?", 1)[0].strip("/")
            parts = [part for part in path.split("/") if part]
            if not parts:
                return None, "Send a valid t.me link."

            if "+" in path or "joinchat" in path.lower():
                chat = await self.user_client.join_chat(link)
                return self._source_from_chat_entity(chat, join_link=link), None

            if parts[0] == "c":
                if len(parts) < 3:
                    return None, "Send a valid t.me/c link."
                internal = int(parts[1])
                chat_id = int(f"-100{internal}")
                topic_id = int(parts[2]) if len(parts) >= 4 and parts[2].isdigit() else None
                chat = await self.user_client.get_chat(chat_id)
                return self._source_from_chat_entity(chat, topic_id=topic_id, join_link=link), None

            username = parts[0]
            topic_id = int(parts[1]) if len(parts) >= 3 and parts[1].isdigit() else None

            try:
                await self.user_client.join_chat(username)
            except Exception:
                try:
                    await self.user_client.join_chat(link)
                except Exception:
                    pass

            chat = await self.user_client.get_chat(username)
            return self._source_from_chat_entity(chat, topic_id=topic_id, join_link=link), None
        except Exception as exc:
            return None, f"Failed to resolve link: {exc}"

    async def _resolve_source_from_message(self, message: Message) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if self.user_client is None:
            return None, "User session is not ready. Configure it in terminal and restart the bot."

        forwarded_chat = self._forwarded_chat(message)
        if forwarded_chat:
            c = forwarded_chat
            return self._source_from_chat_entity(c), None

        text = (message.text or "").strip()
        if not text:
            return None, "Send a forwarded message, link, or chat handle/ID."

        # Raw chat ID
        try:
            chat_id = int(text)
            chat = await self.user_client.get_chat(chat_id)
            return self._source_from_chat_entity(chat), None
        except ValueError:
            pass
        except Exception as exc:
            return None, f"Could not resolve chat handle/ID: {exc}"

        handle_match = re.fullmatch(r"@?([A-Za-z][A-Za-z0-9_]{3,31})", text)
        if handle_match:
            try:
                chat = await self.user_client.get_chat(handle_match.group(1))
                return self._source_from_chat_entity(chat), None
            except Exception as exc:
                return None, f"Could not resolve chat handle: {exc}"

        # Link formats
        link_match = re.search(r"https?://t\.me/[^\s]+|t\.me/[^\s]+", text, flags=re.IGNORECASE)
        if not link_match:
            return None, "Send a valid t.me link, chat handle, or numeric ID."

        return await self._resolve_source_from_tme_link(link_match.group(0))

    async def _handle_add_source_input(self, message: Message, pending: Dict[str, Any]) -> None:
        user_id = message.from_user.id
        group_id = int(pending["group_id"])

        source, err = await self._resolve_source_from_message(message)
        if err:
            await message.reply_text(err)
            return

        s_key, existed = await self._upsert_source(group_id, source)
        self.pending_inputs.pop(user_id, None)
        source_identity = self._source_identity(s_key, source)
        source_name = self._source_display_name(s_key, source)
        prefix = "Source already exists" if existed else "Source added"

        await message.reply_text(
            f"{prefix}: {source_name} ({source_identity})\n\n{await self._sources_screen_text(group_id)}",
            reply_markup=source_actions_menu(group_id, True),
            parse_mode=ParseMode.HTML,
        )

    async def _handle_add_rule_input(self, message: Message, pending: Dict[str, Any]) -> None:
        user_id = message.from_user.id
        group_id = int(pending["group_id"])
        scope = str(pending["scope"])
        source_k = pending.get("source_key")
        rule_type = str(pending["rule_type"])
        rule_mode = pending.get("rule_mode", "blocklist")  # Default to blocklist for backward compatibility
        text = (message.text or "").strip()

        if not text and rule_type != "sender":
            await message.reply_text("Send a value to continue.")
            return

        rule = None
        if rule_type == "keyword":
            values = [x.strip() for x in text.split(",") if x.strip()]
            if not values:
                await message.reply_text("No valid keywords were provided.")
                return
            rule = {"type": "keyword", "values": values, "mode": rule_mode}

        elif rule_type == "exact":
            rule = {"type": "exact", "value": text, "mode": rule_mode}

        elif rule_type == "message_type":
            value = text.lower()
            valid = {"text", "photo", "video", "video_note", "document", "audio", "voice", "animation", "sticker", "poll", "other"}
            if value not in valid:
                await message.reply_text("Send a valid message type.")
                return
            rule = {"type": "message_type", "value": value, "mode": rule_mode}

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
                        await message.reply_text(f"Invalid sender value: {item}.")
                        return
                    if self.user_client is None:
                        await message.reply_text("User session is not ready. Use sender handles, usernames, or numeric sender IDs, or reconfigure the session and restart.")
                        return
                    try:
                        chat = await self.user_client.get_chat(username)
                    except Exception as exc:
                        await message.reply_text(f"Could not resolve username {item}: {exc}")
                        return
                    vals.append(int(chat.id))

            vals = list(dict.fromkeys(vals))
            if not vals:
                await message.reply_text("No sender value was provided. Forward a message or send sender handles, usernames, or IDs.")
                return
            rule = {"type": "sender", "values": vals, "mode": rule_mode}

        elif rule_type == "has_link":
            value = text.lower() in {"yes", "y", "true", "1"}
            rule = {"type": "has_link", "value": value, "mode": rule_mode}

        if rule is None:
            await message.reply_text("That rule type is not supported.")
            return

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            if scope == "gf":
                target = g.setdefault("group_filters", {"rules": []})
            else:
                if not source_k:
                    return state
                src = g.setdefault("sources", {}).setdefault(source_k, {})
                target = src.setdefault("filters", {"rules": []})
            target.setdefault("rules", []).append(rule)
            return state

        await self.storage.update(updater)
        self.pending_inputs.pop(user_id, None)
        await message.reply_text(
            await self._rules_screen_text(group_id, scope, source_k),
            reply_markup=rules_menu(group_id, scope, source_k),
            parse_mode=ParseMode.HTML,
        )

    async def _handle_quick_filter_callback(self, callback_query) -> None:
        parts = (callback_query.data or "").split(":")
        if len(parts) < 4:
            await callback_query.answer("Invalid quick action.")
            return

        group_id = int(parts[1])
        destination_message_id = parts[2]
        action = parts[3]

        entry = await self._forward_log_entry(group_id, destination_message_id)
        if not entry:
            await callback_query.answer("No message metadata was found.", show_alert=True)
            return

        source_k = entry.get("source_key")
        source_text = entry.get("text", "")
        sender_id = entry.get("sender_id")

        if action == "exact":
            if not source_text:
                await callback_query.answer("There is no text or caption available for an exact-match rule.", show_alert=True)
                return
            rule = {"type": "exact", "value": source_text}
            await self._append_source_rule(group_id, source_k, rule)
            await callback_query.answer("Exact-match rule added.")
            return

        if action == "sender":
            if sender_id is None:
                await callback_query.answer("No sender metadata was found.", show_alert=True)
                return
            rule = {"type": "sender", "values": [int(sender_id)]}
            await self._append_source_rule(group_id, source_k, rule)
            await callback_query.answer("Sender rule added.")
            return

        if action == "keywords":
            words = self._extract_keywords(source_text)
            if not words:
                await callback_query.answer("No keywords were found.", show_alert=True)
                return
            kb = []
            for w in words[:10]:
                kb.append([InlineKeyboardButton(w, callback_data=f"qk:{group_id}:{destination_message_id}:{w}")])
            await self._safe_edit_message_text(
                callback_query.message,
                "Pick a keyword to block:",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            await callback_query.answer()
            return

    async def _handle_keyword_pick_callback(self, callback_query) -> None:
        parts = (callback_query.data or "").split(":", 3)
        if len(parts) < 4:
            await callback_query.answer("Invalid keyword action.")
            return

        group_id = int(parts[1])
        destination_message_id = parts[2]
        keyword = parts[3].strip()
        if not keyword:
            await callback_query.answer("Keyword missing")
            return

        entry = await self._forward_log_entry(group_id, destination_message_id)
        if not entry:
            await callback_query.answer("No message metadata was found.", show_alert=True)
            return

        source_k = entry.get("source_key")
        if not source_k:
            await callback_query.answer("No source was found for this message.", show_alert=True)
            return

        await self._append_source_rule(group_id, source_k, {"type": "keyword", "values": [keyword], "mode": "blocklist"})
        await callback_query.answer(f"Keyword rule added: {keyword}")

    async def _append_source_rule(self, group_id: int, source_k: str, rule: Dict[str, Any]) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            src = g.setdefault("sources", {}).setdefault(source_k, {})
            target = src.setdefault("filters", {"rules": []})
            target.setdefault("rules", []).append(rule)
            return state

        await self.storage.update(updater)

    async def _append_rule(self, group_id: int, scope: str, source_k: Optional[str], rule: Dict[str, Any]) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
            if scope == "gf":
                target = g.setdefault("group_filters", {"rules": []})
            else:
                if not source_k:
                    return state
                src = g.setdefault("sources", {}).setdefault(source_k, {})
                target = src.setdefault("filters", {"rules": []})
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

    def _source_message_type(self, message: Message) -> str:
        if message.text or message.caption:
            if message.photo:
                return "photo"
            if message.video:
                return "video"
            if message.document:
                return "document"
            if message.audio:
                return "audio"
            if message.voice:
                return "voice"
        if message.video_note:
            return "video_note"
        if message.animation:
            return "animation"
        if message.sticker:
            return "sticker"
        return "text"

        if message.photo:
            return "photo"
        if message.video:
            return "video"
        if message.document:
            return "document"
        if message.audio:
            return "audio"
        if message.voice:
            return "voice"
        if message.video_note:
            return "video_note"
        if message.sticker:
            return "sticker"
        if message.poll:
            return "poll"
        return "other"

    async def on_user_edited_message(self, client: Client, message: Message) -> None:
        """Handle edits to source messages and propagate them to forwarded copies."""
        source_chat_id = getattr(getattr(message, "chat", None), "id", None)
        if source_chat_id is None:
            return
        source_message_id = int(getattr(message, "id", 0) or 0)
        if source_message_id <= 0:
            return
        msg_thread_id = message_topic_id(message)

        state = await self._state()
        groups = state.get("groups", {})
        if not groups:
            return

        # Find groups+sources where this chat is a configured source.
        matched_targets: List[Tuple[int, str]] = []
        for gid_raw, gdata in groups.items():
            gid = int(gid_raw)
            for s_key, src in gdata.get("sources", {}).items():
                try:
                    src_chat_id = int(src.get("chat_id") or 0)
                except (ValueError, TypeError):
                    continue
                if src_chat_id != int(source_chat_id):
                    continue
                src_topic_id = src.get("topic_id")
                if src_topic_id is not None and msg_thread_id != src_topic_id:
                    continue
                if gid == int(source_chat_id):
                    continue
                matched_targets.append((gid, s_key))

        if not matched_targets:
            return

        forward_logs = await self._forward_logs_state()

        for gid, s_key in matched_targets:
            group_history = forward_logs.get(str(gid), {})
            if not isinstance(group_history, dict):
                continue
            group_state = groups.get(str(gid), default_group_state())
            src = group_state.get("sources", {}).get(s_key)
            if not src:
                continue

            settings = group_state.get("settings", {})
            show_header = bool(settings.get("show_header", True))
            show_link = bool(settings.get("show_link", True))
            show_source_datetime = bool(settings.get("show_source_datetime", False))
            source_datetime = self._format_source_datetime(message) if show_source_datetime else ""
            header = source_header(
                self._source_display_name(s_key, src),
                int(src.get("chat_id")),
                src.get("username"),
                src.get("topic_id"),
                source_datetime,
            )
            link = original_message_link(int(src.get("chat_id")), source_message_id, src.get("username"))

            for dest_msg_id_raw, entry in group_history.items():
                if not isinstance(entry, dict):
                    continue
                entry_source_msg_id = entry.get("source_message_id")
                if entry_source_msg_id is None:
                    continue  # Old entry without source_message_id; skip.
                if int(entry_source_msg_id) != source_message_id:
                    continue
                try:
                    if int(entry.get("source_chat_id") or 0) != int(source_chat_id):
                        continue
                except (TypeError, ValueError):
                    continue
                if entry.get("source_key") != s_key:
                    continue

                dest_msg_id = int(dest_msg_id_raw)
                message_type = entry.get("message_type", "text")
                try:
                    if message_type in {"photo", "video", "document", "audio", "voice", "animation"}:
                        caption = message.caption or ""
                        payload = compose_caption_payload(header, caption, link, show_header, show_link)
                        await self.bot.edit_message_caption(
                            chat_id=gid,
                            message_id=dest_msg_id,
                            caption=payload,
                            parse_mode=ParseMode.HTML,
                        )
                    elif message_type in {"sticker", "video_note", "poll"}:
                        pass  # These types don't support text/caption editing.
                    else:
                        text = message.text or ""
                        payload = compose_text_payload(header, text, link, show_header, show_link)
                        await self.bot.edit_message_text(
                            chat_id=gid,
                            message_id=dest_msg_id,
                            text=self._clip_telegram_text(payload or header),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                except BadRequest as exc:
                    if "message is not modified" not in str(exc).lower():
                        logger.warning(
                            "Edit forwarded message failed | group_id=%s | dest_msg_id=%s | error=%s",
                            gid, dest_msg_id, exc,
                        )
                except Exception as exc:
                    logger.warning(
                        "Edit forwarded message failed | group_id=%s | dest_msg_id=%s | error=%s",
                        gid, dest_msg_id, exc,
                    )

    async def on_user_message(self, client: Client, message: Message) -> None:
        state = await self._state()
        groups = state.get("groups", {})
        if not groups:
            return

        matched_targets: List[Tuple[int, str]] = []
        msg_chat_id = message.chat.id
        msg_thread_id = message_topic_id(message)

        for gid_raw, gdata in groups.items():
            gid = int(gid_raw)
            for s_key, src in gdata.get("sources", {}).items():
                try:
                    # Validate source data integrity
                    src_chat_id = src.get("chat_id")
                    if src_chat_id is None:
                        logger.warning(f"Source {s_key} in group {gid_raw} missing chat_id, skipping")
                        continue
                    src_chat_id = int(src_chat_id)
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid chat_id for source {s_key} in group {gid_raw}: {e}")
                    continue
                
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

        await self._mark_source_message_read(client, message)

        # Group by destination group for filter checks and send.
        grouped = defaultdict(list)
        for gid, s_key in matched_targets:
            grouped[gid].append(s_key)

        for gid, keys in grouped.items():
            for s_key in keys:
                try:
                    media_group_id = getattr(message, "media_group_id", None)
                    if media_group_id:
                        await self._buffer_media_group(gid, s_key, message, str(media_group_id))
                    else:
                        await self._forward_message_to_group(gid, s_key, message, apply_filters=True)
                except Exception as e:
                    logger.error(f"Error forwarding message from {msg_chat_id} to group {gid} via source {s_key}: {e}")

    async def _mark_chat_history_read(self, client: Client, chat_id: int, max_id: int, *, reason: str) -> None:
        if max_id <= 0:
            return

        try:
            await client.read_chat_history(chat_id, max_id=max_id)
        except Exception as exc:
            logger.warning(
                "Failed to mark chat history as read | chat_id=%s | max_id=%s | reason=%s | error=%s",
                chat_id,
                max_id,
                reason,
                exc,
            )

    async def _mark_source_message_read(self, client: Client, message: Message) -> None:
        message_id = int(getattr(message, "id", 0) or 0)
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        if chat_id is None or message_id <= 0:
            return

        await self._mark_chat_history_read(client, int(chat_id), message_id, reason="live-source-processing")

    async def _forward_message_to_group(
        self,
        group_id: int,
        s_key: str,
        message: Message,
        apply_filters: bool,
        cached_state: Optional[Dict] = None,
        *,
        track_history: bool = True,
        update_last_seen: bool = True,
    ) -> Optional[int]:
        state = cached_state if cached_state is not None else await self._state()
        g = state.get("groups", {}).get(str(group_id))
        if not g:
            return None
        src = g.get("sources", {}).get(s_key)
        if not src:
            return None

        if apply_filters:
            if not evaluate_filters(g.get("group_filters", {"rules": []}), message):
                return None
            if not evaluate_filters(src.get("filters", {"rules": []}), message):
                return None

        settings = g.get("settings", {})
        show_header = bool(settings.get("show_header", True))
        show_link = bool(settings.get("show_link", True))
        show_source_datetime = bool(settings.get("show_source_datetime", False))
        source_datetime = self._format_source_datetime(message) if show_source_datetime else ""

        header = source_header(
            self._source_display_name(s_key, src),
            int(src.get("chat_id")),
            src.get("username"),
            src.get("topic_id"),
            source_datetime,
        )
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
                    disable_web_page_preview=True,
                )

            elif message.photo:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(
                    self.bot.send_photo,
                    group_id,
                    photo=media,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.video:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(
                    self.bot.send_video,
                    group_id,
                    video=media,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.document:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(
                    self.bot.send_document,
                    group_id,
                    document=media,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.audio:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(
                    self.bot.send_audio,
                    group_id,
                    audio=media,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.voice:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(
                    self.bot.send_voice,
                    group_id,
                    voice=media,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.video_note:
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                # video_note does not support captions; send header/link as preceding text.
                payload = compose_text_payload(header, "", link, show_header, show_link)
                if payload:
                    await self._safe_send(
                        self.bot.send_message,
                        group_id,
                        payload,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                sent_message = await self._safe_send(
                    self.bot.send_video_note,
                    group_id,
                    video_note=media,
                )

            elif message.animation:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(
                    self.bot.send_animation,
                    group_id,
                    animation=media,
                    caption=payload,
                    parse_mode=ParseMode.HTML,
                )

            elif message.sticker:
                # Stickers do not support captions; send header/link as a text block first.
                payload = compose_text_payload(header, "", link, show_header, show_link)
                await self._safe_send(
                    self.bot.send_message,
                    group_id,
                    payload or header,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return None
                sent_message = await self._safe_send(self.bot.send_sticker, group_id, sticker=media)

            elif message.poll:
                payload = compose_text_payload(header, f"[Poll] {message.poll.question}", link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_message,
                    group_id,
                    payload or header,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

            else:
                payload = compose_text_payload(header, "[Unsupported message type]", link, show_header, show_link)
                sent_message = await self._safe_send(
                    self.bot.send_message,
                    group_id,
                    payload or header,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

            if sent_message is not None:
                destination_message_id = self._message_id(sent_message)
                if track_history:
                    await self._log_forward(group_id, destination_message_id, s_key, message)
                if update_last_seen:
                    await self._update_last_seen_msg_id(group_id, s_key, int(getattr(message, "id", 0) or 0))
                return destination_message_id

        except Exception:
            logger.exception("Failed forwarding to group %s", group_id)

        return None

    async def _buffer_media_group(self, group_id: int, s_key: str, message: Message, media_group_id: str) -> None:
        """Buffer a media-group message and schedule a delayed flush."""
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id))
        if not g:
            return
        src = g.get("sources", {}).get(s_key)
        if not src:
            return
        if not evaluate_filters(g.get("group_filters", {"rules": []}), message):
            return
        if not evaluate_filters(src.get("filters", {"rules": []}), message):
            return

        buf_key = (group_id, s_key, media_group_id)
        self._media_group_buffers.setdefault(buf_key, []).append(message)

        existing = self._media_group_tasks.get(buf_key)
        if existing and not existing.done():
            existing.cancel()
        self._media_group_tasks[buf_key] = asyncio.create_task(
            self._delayed_flush_media_group(buf_key)
        )

    async def _delayed_flush_media_group(self, buf_key: tuple) -> None:
        """Wait for remaining album messages to arrive then flush."""
        try:
            await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            pass
        await self._flush_media_group(buf_key)

    async def _flush_media_group(self, buf_key: tuple) -> None:
        """Send buffered album messages as a single send_media_group call."""
        messages = self._media_group_buffers.pop(buf_key, [])
        self._media_group_tasks.pop(buf_key, None)
        if not messages or self.bot is None:
            return

        group_id, s_key, _ = buf_key
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id))
        if not g:
            return
        src = g.get("sources", {}).get(s_key)
        if not src:
            return

        settings = g.get("settings", {})
        show_header = bool(settings.get("show_header", True))
        show_link = bool(settings.get("show_link", True))
        show_source_datetime = bool(settings.get("show_source_datetime", False))

        messages.sort(key=lambda m: getattr(m, "id", 0))
        first_msg = messages[0]
        source_datetime = self._format_source_datetime(first_msg) if show_source_datetime else ""

        header = source_header(
            self._source_display_name(s_key, src),
            int(src.get("chat_id")),
            src.get("username"),
            src.get("topic_id"),
            source_datetime,
        )
        link = original_message_link(int(src.get("chat_id")), int(first_msg.id), src.get("username"))
        first_caption = compose_caption_payload(header, first_msg.caption or "", link, show_header, show_link)

        media_items = []
        for idx, msg in enumerate(messages):
            media_bytes = await self._download_pyrogram_media(msg)
            if media_bytes is None:
                continue
            item_caption = first_caption if idx == 0 else None
            parse = ParseMode.HTML if item_caption else None
            if msg.photo:
                media_items.append(InputMediaPhoto(media=media_bytes, caption=item_caption, parse_mode=parse))
            elif msg.video:
                media_items.append(InputMediaVideo(media=media_bytes, caption=item_caption, parse_mode=parse))
            else:
                media_items.append(InputMediaDocument(media=media_bytes, caption=item_caption, parse_mode=parse))

        if not media_items:
            return

        try:
            sent_messages = await self._safe_send(self.bot.send_media_group, group_id, media=media_items)
            if sent_messages:
                await self._log_forward(group_id, self._message_id(sent_messages[0]), s_key, first_msg)
            else:
                logger.warning("Media group send returned no result | group_id=%s", group_id)
        except Exception:
            logger.exception("Failed to send media group to group %s", group_id)

    def _is_media_valid(self, media) -> bool:
        """Check if downloaded media is non-empty and valid."""
        if media is None:
            return False
        if hasattr(media, "seek") and hasattr(media, "tell"):
            try:
                current_pos = media.tell()
                media.seek(0, 2)  # Seek to end
                size = media.tell()
                media.seek(current_pos)  # Restore position
                return size > 0
            except Exception:
                return False
        return True

    async def _download_pyrogram_media(self, message: Message):
        """Download media from a Pyrogram message as BytesIO for Bot API re-upload."""
        if self.user_client is None:
            return None
        try:
            media = await self.user_client.download_media(message, in_memory=True)
            if not self._is_media_valid(media):
                logger.warning("Downloaded media is empty | message_id=%s", getattr(message, 'id', None))
                return None
            return media
        except Exception:
            logger.exception("Failed to download media for message %s", getattr(message, 'id', None))
            return None

    async def _safe_send(self, func, *args, **kwargs):
        timeout_retries = max(int(os.getenv("SEND_TIMEOUT_RETRIES", "2") or 2), 0)
        max_attempts = 1 + timeout_retries
        func_name = getattr(func, "__name__", "send")

        for attempt in range(1, max_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except RetryAfter as err:
                wait_seconds = int(getattr(err, "retry_after", 1) or 1)
                await asyncio.sleep(max(wait_seconds, 1))
            except (TimedOut, NetworkError) as err:
                if attempt >= max_attempts:
                    logger.warning(
                        "Telegram send failed after %s attempt(s) | method=%s | error=%s",
                        max_attempts,
                        func_name,
                        err,
                    )
                    return None
                backoff_seconds = min(8.0, 1.5 * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff_seconds)

        return None

    async def _update_last_seen_msg_id(self, group_id: int, s_key: str, msg_id: int) -> None:
        if msg_id <= 0:
            return

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            src = state.get("groups", {}).get(str(group_id), {}).get("sources", {}).get(s_key)
            if src is not None:
                current = src.get("last_seen_msg_id")
                if current is None or int(msg_id) > int(current):
                    src["last_seen_msg_id"] = int(msg_id)
            return state

        await self.storage.update(updater)

    async def _catch_up_source(self, group_id: int, s_key: str, source_chat_id: int, source_topic_id: Optional[int], last_seen_msg_id: int) -> int:
        """Fetch messages newer than last_seen_msg_id and forward them. Returns count forwarded."""
        if self.user_client is None:
            return 0

        limit = max(int(os.getenv("CATCHUP_MAX_MESSAGES", "500") or 500), 1)
        missed: List[Message] = []
        newest_scanned_message_id = 0
        try:
            async for msg in self.user_client.get_chat_history(source_chat_id, limit=limit):
                msg_id = int(getattr(msg, "id", 0) or 0)
                if msg_id <= last_seen_msg_id:
                    break
                newest_scanned_message_id = max(newest_scanned_message_id, msg_id)
                if source_topic_id is not None and message_topic_id(msg) != int(source_topic_id):
                    continue
                missed.append(msg)
        except Exception as exc:
            logger.warning("Catch-up history fetch failed | group_id=%s | source=%s | error=%s", group_id, s_key, exc)
            return 0

        if not missed:
            return 0

        missed.reverse()  # oldest first
        forwarded = 0
        for msg in missed:
            try:
                ok = await self._forward_message_to_group(group_id, s_key, msg, apply_filters=True)
                if ok:
                    forwarded += 1
                await asyncio.sleep(0.05)
            except Exception as exc:
                logger.warning("Catch-up forward failed | group_id=%s | source=%s | msg_id=%s | error=%s", group_id, s_key, getattr(msg, "id", None), exc)

        if newest_scanned_message_id > 0:
            await self._mark_chat_history_read(
                self.user_client,
                int(source_chat_id),
                newest_scanned_message_id,
                reason="startup-catch-up",
            )
        return forwarded

    async def _catch_up_all_sources(self) -> None:
        """On startup, forward any messages missed while the bot was offline."""
        if self.user_client is None:
            return

        state = await self._state()
        groups = state.get("groups", {})
        total_caught = 0
        for gid_raw, gdata in groups.items():
            group_id = int(gid_raw)
            for s_key, src in gdata.get("sources", {}).items():
                last_seen = src.get("last_seen_msg_id")
                if last_seen is None:
                    continue
                source_chat_id = src.get("chat_id")
                if not source_chat_id:
                    continue
                source_topic_id = src.get("topic_id")
                try:
                    caught = await self._catch_up_source(group_id, s_key, int(source_chat_id), source_topic_id, int(last_seen))
                    if caught:
                        total_caught += caught
                        logger.info("Catch-up | group_id=%s | source=%s | forwarded=%d", group_id, s_key, caught)
                except Exception as exc:
                    logger.warning("Catch-up failed | group_id=%s | source=%s | error=%s", group_id, s_key, exc)

        if total_caught:
            logger.info("Catch-up complete | total_forwarded=%d", total_caught)

    async def _log_forward(self, group_id: int, destination_message_id: int, s_key: str, source_message: Message) -> None:
        sender_id = None
        if source_message.from_user:
            sender_id = source_message.from_user.id
        elif source_message.sender_chat:
            sender_id = source_message.sender_chat.id

        text_blob = (source_message.text or source_message.caption or "").strip()
        message_type = self._source_message_type(source_message)
        source_date_iso = None
        source_date = getattr(source_message, "date", None)
        if isinstance(source_date, datetime):
            if source_date.tzinfo is None:
                source_date = source_date.replace(tzinfo=timezone.utc)
            source_date_iso = source_date.astimezone(timezone.utc).isoformat()
        logged_at_iso = datetime.now(timezone.utc).isoformat()
        source_chat_id = None
        source_topic_id = None
        try:
            source_chat_id, source_topic_id = parse_source_key(s_key)
        except Exception:
            source_chat_id = None
            source_topic_id = None

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            fwd = state.setdefault(str(group_id), {})
            fwd[str(destination_message_id)] = {
                "source_key": s_key,
                "source_message_id": int(source_message.id),
                "source_chat_id": source_chat_id,
                "source_topic_id": source_topic_id,
                "sender_id": sender_id,
                "text": text_blob,
                "message_type": message_type,
                "source_date": source_date_iso,
                "logged_at": logged_at_iso,
            }
            if len(fwd) > 2000:
                # Keep memory bounded.
                keys = list(fwd.keys())
                for k in keys[:300]:
                    fwd.pop(k, None)
            return state

        await self.forward_log_storage.update(updater)
        await self._append_live_event_line(group_id, s_key, logged_at_iso)


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
