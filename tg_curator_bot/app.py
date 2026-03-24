import asyncio
from copy import deepcopy
import logging
import os
import re
import sys
from collections import defaultdict
from types import SimpleNamespace
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
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaDocument, InputMediaPhoto, InputMediaVideo, MenuButtonCommands, Update
from telegram.constants import ParseMode
from telegram.constants import ChatMemberStatus
from telegram.error import BadRequest, RetryAfter
from telegram.ext import Application, CallbackQueryHandler, ChatMemberHandler, ContextTypes, MessageHandler, filters

from .filters import evaluate_filters
from .formatting import compose_caption_payload, compose_text_payload, original_message_link, source_header
from .keyboards import (
    add_rule_types,
    dm_admin_menu,
    dm_destinations_menu,
    filters_root,
    history_actions_menu,
    history_source_selector_menu,
    group_settings_menu,
    group_main_menu,
    rules_menu,
    source_actions_menu,
    source_filter_selector_menu,
    source_remove_menu,
    yes_no_buttons,
)
from .storage import DEFAULT_STATE, ForwardLogStorage, Storage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("tg-curator-bot")


def default_group_state() -> Dict[str, Any]:
    return {
        "meta": {
            "title": None,
            "username": None,
        },
        "settings": {
            "show_header": True,
            "show_link": True,
        },
        "group_filters": {
            "mode": "blocklist",
            "rules": [],
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
        self._media_group_buffers: Dict[tuple, list] = {}
        self._media_group_tasks: Dict[tuple, asyncio.Task] = {}

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

    async def _notify_owner(self, text: str) -> None:
        state = await self._state()
        owner_id = state.get("owner_id")
        if owner_id is None or self.bot is None:
            return
        try:
            await self.bot.send_message(int(owner_id), text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
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
                reply_markup=dm_admin_menu(session_ready, groups, sources),
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

            chat_type = str(getattr(chat, "type", "")).lower()
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

    async def _safe_edit_message_text(self, message: Any, text: str, reply_markup: Any = None, **kwargs) -> Any:
        try:
            return await message.edit_text(text, reply_markup=reply_markup, **kwargs)
        except BadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return message
            raise

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
        if username:
            return f"@{username}"
        if name:
            return name
        return source_key_value

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
        if rule_type in {"keyword", "sender"}:
            values = ", ".join(str(value) for value in rule.get("values", [])) or "-"
            return f"{rule_type}: {values}"
        if rule_type in {"exact", "message_type"}:
            return f"{rule_type}: {rule.get('value', '-') }"
        if rule_type == "has_link":
            return f"has_link: {'yes' if rule.get('value') else 'no'}"
        return str(rule)

    def _bool_label(self, value: bool) -> str:
        return "ON" if value else "OFF"

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
            f"Group filter mode: <b>{group_filters.get('mode', 'blocklist')}</b>\n"
            f"Group filter rules: <b>{len(group_filters.get('rules', []))}</b>\n"
            f"Sources with source rules: <b>{source_filter_count}</b>\n"
            f"Header: <b>{self._bool_label(bool(settings.get('show_header', True)))}</b>\n"
            f"Original link: <b>{self._bool_label(bool(settings.get('show_link', True)))}</b>"
        )

    async def _sources_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        sources = group_state.get("sources", {})
        lines = [f"<b>📡 Sources for {name}</b>"]
        if not sources:
            lines.append("")
            lines.append("No sources configured yet.")
            lines.append("Use Add Source below, then send a forwarded message, a t.me link, or a chat handle/ID in this DM.")
            return "\n".join(lines)

        lines.append("")
        for source_key_value, source in sources.items():
            source_name = self._source_display_name(source_key_value, source)
            filters_state = source.get("filters", {})
            source_identity = self._source_identity(source_key_value, source, html=True)
            lines.append(
                f"• <b>{source_name}</b> — {source_identity} — {filters_state.get('mode', 'blocklist')} ({len(filters_state.get('rules', []))} rules)"
            )
        return "\n".join(lines)

    async def _filters_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        name = self._group_display_name(group_id, group_state)
        group_filters = group_state.get("group_filters", {})
        source_filters = sum(1 for source in group_state.get("sources", {}).values() if source.get("filters", {}).get("rules"))
        history = await self._group_forward_history(group_id)
        return (
            f"<b>🧰 Filters for {name}</b>\n\n"
            f"Group filters: <b>{group_filters.get('mode', 'blocklist')}</b> with <b>{len(group_filters.get('rules', []))}</b> rules\n"
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
        lines = [f"<b>{title}</b>", "", f"Mode: <b>{target.get('mode', 'blocklist')}</b>"]
        rules = target.get("rules", [])
        if not rules:
            lines.append("Rules: none")
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
        return (
            f"<b>⚙️ Settings for {name}</b>\n\n"
            f"Header: <b>{self._bool_label(bool(settings.get('show_header', True)))}</b>\n"
            f"Original link: <b>{self._bool_label(bool(settings.get('show_link', True)))}</b>"
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
        source_counts: Dict[str, int] = defaultdict(int)
        for entry in history.values():
            source_key_value = str(entry.get("source_key") or "")
            if source_key_value:
                source_counts[source_key_value] += 1

        lines = [f"<b>📡 Clean Single Source History for {name}</b>"]
        if not source_counts:
            lines.append("")
            lines.append("No tracked history exists for this destination yet.")
            return "\n".join(lines)

        lines.append("")
        for source_key_value, count in sorted(source_counts.items(), key=lambda item: item[1], reverse=True):
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
        await self.application.updater.start_polling()
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
            text, session_ready, groups, sources = await self._dm_home_text()
            await message.reply_text(
                text,
                reply_markup=dm_admin_menu(session_ready, groups, sources),
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
                "Open DM and tap Destinations to configure sources and filters."
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
            await callback_query.answer("Only owner can use this.", show_alert=True)
            return

        data = callback_query.data or ""
        if data == "noop":
            await callback_query.answer()
            return

        if data in {"dm:home", "dm:status"}:
            text, session_ready, groups, sources = await self._dm_home_text()
            await self._safe_edit_message_text(
                callback_query.message,
                text,
                reply_markup=dm_admin_menu(session_ready, groups, sources),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await callback_query.answer()
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
            await callback_query.answer("Invalid action")
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
            state = await self._state()
            sources = state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
            await self._safe_edit_message_text(
                callback_query.message,
                await self._sources_screen_text(group_id),
                reply_markup=source_actions_menu(group_id, bool(sources)),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
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
                await callback_query.answer(f"Removed {removed} history entries")
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

            await callback_query.answer("Unknown history option")
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
                "- a chat handle or ID"
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
            await callback_query.message.reply_text(
                "<b>Filter reapply complete</b>\n\n"
                f"Scanned tracked messages: <b>{result['scanned']}</b>\n"
                f"Deleted from destination: <b>{result['deleted']}</b>\n"
                f"Skipped (delete failed): <b>{result['skipped']}</b>\n"
                f"History entries removed: <b>{result['history_removed']}</b>",
                parse_mode=ParseMode.HTML,
            )

            state = await self._state()
            sources = state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
            await self._safe_edit_message_text(
                callback_query.message,
                await self._filters_screen_text(group_id),
                reply_markup=filters_root(group_id, bool(sources)),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
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

        if action == "sfsel":
            if len(parts) < 4:
                await callback_query.answer("Invalid source")
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
                await callback_query.answer("Invalid source")
                return
            s_key = parts[3]
            removed = await self._clear_history(group_id, s_key)
            await callback_query.answer(f"Removed {removed} history entries")
            await self._show_history_source_selector(callback_query, group_id)
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
                await callback_query.answer("Missing type")
                return
            rule_type = tail[1]
            source_k = tail[2] if len(tail) > 2 else None
            if rule_type == "has_link":
                yes_cb = f"g:{group_id}:{scope}:haslink:1"
                no_cb = f"g:{group_id}:{scope}:haslink:0"
                if source_k:
                    yes_cb = f"{yes_cb}:{source_k}"
                    no_cb = f"{no_cb}:{source_k}"
                await callback_query.message.reply_text(
                    "Choose link rule value:",
                    reply_markup=yes_no_buttons(yes_cb, no_cb),
                )
                await callback_query.answer()
                return

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
                "message_type": "Send one type: text, photo, video, video_note, document, audio, voice, animation, sticker, poll, other",
                "sender": "Send a forwarded message, sender handles/usernames, or sender IDs (example: @username,123456789)",
            }
            await callback_query.message.reply_text(prompts.get(rule_type, "Send rule value"))
            await callback_query.answer()
            return

        if command == "haslink":
            if len(tail) < 2:
                await callback_query.answer("Invalid has-link value")
                return
            value_raw = tail[1].strip()
            source_k = tail[2] if len(tail) > 2 else None
            rule = {"type": "has_link", "value": value_raw == "1"}
            await self._append_rule(group_id, scope, source_k, rule)
            await callback_query.answer(f"Rule added: has_link={'yes' if rule['value'] else 'no'}")
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
        await self._safe_edit_message_text(
            callback_query.message,
            await self._settings_screen_text(group_id),
            reply_markup=group_settings_menu(
                group_id,
                bool(settings.get("show_header", True)),
                bool(settings.get("show_link", True)),
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
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
        await self._safe_edit_message_text(
            callback_query.message,
            await self._sources_screen_text(group_id),
            reply_markup=source_actions_menu(group_id, bool(sources)),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()

    async def _show_remove_source_menu(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.answer("No sources", show_alert=True)
            return

        choices = [(key, self._source_display_name(key, src)[:48]) for key, src in sources.items()]

        await self._safe_edit_message_text(
            callback_query.message,
            "<b>Select a Source to Remove</b>",
            reply_markup=source_remove_menu(group_id, choices),
            parse_mode=ParseMode.HTML,
        )
        await callback_query.answer()

    async def _remove_source(self, callback_query, group_id: int, source_k: str) -> None:
        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            groups = state.setdefault("groups", {})
            g = groups.setdefault(str(group_id), default_group_state())
            g.setdefault("sources", {}).pop(source_k, None)
            return state

        await self.storage.update(updater)
        state = await self._state()
        sources = state.get("groups", {}).get(str(group_id), default_group_state()).get("sources", {})
        await self._safe_edit_message_text(
            callback_query.message,
            await self._sources_screen_text(group_id),
            reply_markup=source_actions_menu(group_id, bool(sources)),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer("Removed")

    async def _show_source_filter_selector(self, callback_query, group_id: int) -> None:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = g.get("sources", {})
        if not sources:
            await callback_query.answer("No sources", show_alert=True)
            return

        choices = [(key, self._source_display_name(key, src)[:48]) for key, src in sources.items()]
        await self._safe_edit_message_text(
            callback_query.message,
            "<b>Select a Source Filter Set</b>",
            reply_markup=source_filter_selector_menu(group_id, choices),
            parse_mode=ParseMode.HTML,
        )
        await callback_query.answer()

    async def _show_history_source_selector(self, callback_query, group_id: int) -> None:
        history = await self._group_forward_history(group_id)
        if not history:
            await callback_query.answer("No history to clean", show_alert=True)
            return

        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        source_keys = sorted({entry.get("source_key") for entry in history.values() if entry.get("source_key")})
        choices = [
            (key, self._source_display_name(key, group_state.get("sources", {}).get(key, {}))[:48])
            for key in source_keys
        ]
        await self._safe_edit_message_text(
            callback_query.message,
            await self._history_source_selector_text(group_id),
            reply_markup=history_source_selector_menu(group_id, choices),
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
                if isinstance(entry, dict) and entry.get("source_key") == source_k:
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
        group_filters = group_state.get("group_filters", {"mode": "blocklist", "rules": []})
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
                    source_ok = evaluate_filters(source.get("filters", {"mode": "blocklist", "rules": []}), message_stub)
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

    async def _filter_target(self, state: Dict[str, Any], group_id: int, scope: str, source_k: Optional[str]) -> Dict[str, Any]:
        g = state.setdefault("groups", {}).setdefault(str(group_id), default_group_state())
        if scope == "gf":
            return g.setdefault("group_filters", {"mode": "blocklist", "rules": []})
        if not source_k:
            return {"mode": "blocklist", "rules": []}
        src = g.setdefault("sources", {}).setdefault(source_k, {})
        return src.setdefault("filters", {"mode": "blocklist", "rules": []})

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
        back_callback = f"g:{group_id}:gf" if scope == "gf" else f"g:{group_id}:sfsel:{source_k}"
        buttons.append([InlineKeyboardButton("Back", callback_data=back_callback)])

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
        await self._safe_edit_message_text(
            callback_query.message,
            await self._rules_screen_text(group_id, scope, source_k),
            reply_markup=rules_menu(group_id, scope, source_k),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

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
        await self._safe_edit_message_text(
            callback_query.message,
            await self._rules_screen_text(group_id, scope, source_k),
            reply_markup=rules_menu(group_id, scope, source_k),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _handle_pending_input(self, message: Message, pending: Dict[str, Any]) -> None:
        kind = pending.get("kind")
        if kind == "add_source":
            await self._handle_add_source_input(message, pending)
            return

        if kind == "add_rule":
            await self._handle_add_rule_input(message, pending)
            return

    async def _resolve_source_from_message(self, message: Message) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if self.user_client is None:
            return None, "User session is not ready. Configure it in terminal and restart the bot."

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
            return None, "Send a forwarded message, link, or chat handle/ID."

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
            return None, f"Could not resolve chat handle/ID: {exc}"

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
        source_identity = self._source_identity(s_key, source)

        await message.reply_text(
            f"Added source: {source['name']} ({source_identity})",
            reply_markup=source_actions_menu(group_id, True),
        )

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
            valid = {"text", "photo", "video", "video_note", "document", "audio", "voice", "animation", "sticker", "poll", "other"}
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
                        await message.reply_text("User session is not ready. Use sender handles/usernames or numeric sender IDs, or reconfigure session in terminal and restart.")
                        return
                    try:
                        chat = await self.user_client.get_chat(username)
                    except Exception as exc:
                        await message.reply_text(f"Could not resolve username {item}: {exc}")
                        return
                    vals.append(int(chat.id))

            vals = list(dict.fromkeys(vals))
            if not vals:
                await message.reply_text("No sender value provided. Forward a message or send sender handles/usernames/IDs.")
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
        await message.reply_text(
            "Rule added.",
            reply_markup=rules_menu(group_id, scope, source_k),
        )

    async def _handle_quick_filter_callback(self, callback_query) -> None:
        parts = (callback_query.data or "").split(":")
        if len(parts) < 4:
            await callback_query.answer("Invalid quick action")
            return

        group_id = int(parts[1])
        destination_message_id = parts[2]
        action = parts[3]

        entry = await self._forward_log_entry(group_id, destination_message_id)
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

        entry = await self._forward_log_entry(group_id, destination_message_id)
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

    async def _append_rule(self, group_id: int, scope: str, source_k: Optional[str], rule: Dict[str, Any]) -> None:
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
                media_group_id = getattr(message, "media_group_id", None)
                if media_group_id:
                    await self._buffer_media_group(gid, s_key, message, str(media_group_id))
                else:
                    await self._forward_message_to_group(gid, s_key, message, apply_filters=True)

    async def _forward_message_to_group(self, group_id: int, s_key: str, message: Message, apply_filters: bool) -> bool:
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id))
        if not g:
            return False
        src = g.get("sources", {}).get(s_key)
        if not src:
            return False

        if apply_filters:
            if not evaluate_filters(g.get("group_filters", {"mode": "blocklist", "rules": []}), message):
                return False
            if not evaluate_filters(src.get("filters", {"mode": "blocklist", "rules": []}), message):
                return False

        settings = g.get("settings", {})
        show_header = bool(settings.get("show_header", True))
        show_link = bool(settings.get("show_link", True))

        header = source_header(
            src.get("name", "Unknown Source"),
            int(src.get("chat_id")),
            src.get("username"),
            src.get("topic_id"),
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
                    disable_web_page_preview=False,
                )

            elif message.photo:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return False
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
                    return False
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
                    return False
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
                    return False
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
                    return False
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
                    return False
                # video_note does not support captions; send header/link as preceding text.
                payload = compose_text_payload(header, "", link, show_header, show_link)
                if payload:
                    await self._safe_send(self.bot.send_message, group_id, payload, parse_mode=ParseMode.HTML)
                sent_message = await self._safe_send(
                    self.bot.send_video_note,
                    group_id,
                    video_note=media,
                )

            elif message.animation:
                payload = compose_caption_payload(header, caption, link, show_header, show_link)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return False
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
                await self._safe_send(self.bot.send_message, group_id, payload or header, parse_mode=ParseMode.HTML)
                media = await self._download_pyrogram_media(message)
                if media is None:
                    return False
                sent_message = await self._safe_send(self.bot.send_sticker, group_id, sticker=media)

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
                return True

        except Exception:
            logger.exception("Failed forwarding to group %s", group_id)

        return False

    async def _buffer_media_group(self, group_id: int, s_key: str, message: Message, media_group_id: str) -> None:
        """Buffer a media-group message and schedule a delayed flush."""
        state = await self._state()
        g = state.get("groups", {}).get(str(group_id))
        if not g:
            return
        src = g.get("sources", {}).get(s_key)
        if not src:
            return
        if not evaluate_filters(g.get("group_filters", {"mode": "blocklist", "rules": []}), message):
            return
        if not evaluate_filters(src.get("filters", {"mode": "blocklist", "rules": []}), message):
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

        messages.sort(key=lambda m: getattr(m, "id", 0))
        first_msg = messages[0]

        header = source_header(
            src.get("name", "Unknown Source"),
            int(src.get("chat_id")),
            src.get("username"),
            src.get("topic_id"),
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
        except Exception:
            logger.exception("Failed to send media group to group %s", group_id)

    async def _download_pyrogram_media(self, message: Message):
        """Download media from a Pyrogram message as BytesIO for Bot API re-upload."""
        if self.user_client is None:
            return None
        try:
            return await self.user_client.download_media(message, in_memory=True)
        except Exception:
            logger.exception("Failed to download media for message %s", getattr(message, 'id', None))
            return None

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
        message_type = self._source_message_type(source_message)

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            fwd = state.setdefault(str(group_id), {})
            fwd[str(destination_message_id)] = {
                "source_key": s_key,
                "sender_id": sender_id,
                "text": text_blob,
                "message_type": message_type,
            }
            if len(fwd) > 2000:
                # Keep memory bounded.
                keys = list(fwd.keys())
                for k in keys[:300]:
                    fwd.pop(k, None)
            return state

        await self.forward_log_storage.update(updater)


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
