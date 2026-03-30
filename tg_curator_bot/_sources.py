from html import escape
from typing import Any, Dict, List, Optional, Tuple

from pyrogram.types import Message
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from ._screens import default_group_state
from .keyboards import source_actions_menu, source_filter_selector_menu_paginated, source_remove_menu


def source_key(chat_id: int, topic_id: Optional[int]) -> str:
    return f"{chat_id}|{topic_id or 0}"


class _SourcesMixin:
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

    def _bulk_import_session_key(self, user_id: int, group_id: int) -> Tuple[int, int]:
        return int(user_id), int(group_id)

    def _bulk_import_categories(self, session: Dict[str, Any]) -> List[Dict[str, Any]]:
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
            state = _state(groups)
            categories.append({"key": "groups", "label": f"{_prefix(state)} 👥 Groups ({len(groups)})"})
        if channels:
            state = _state(channels)
            categories.append({"key": "channels", "label": f"{_prefix(state)} 📢 Channels ({len(channels)})"})
        for folder in folders:
            folder_id = folder["id"]
            folder_candidates = [c for c in candidates if c.get("folder_id") == folder_id]
            if not folder_candidates:
                continue
            state = _state(folder_candidates)
            categories.append(
                {"key": f"folder_{folder_id}", "label": f"{_prefix(state)} 📂 {folder['title']} ({len(folder_candidates)})"}
            )

        return categories

    def _category_candidates(self, session: Dict[str, Any], cat_key: str) -> List[Dict[str, Any]]:
        candidates = session.get("all_candidates", [])
        if cat_key == "groups":
            return [c for c in candidates if self._chat_type_name(c.get("type")) in {"group", "supergroup"}]
        if cat_key == "channels":
            return [c for c in candidates if self._chat_type_name(c.get("type")) == "channel"]
        if cat_key.startswith("folder_"):
            try:
                folder_id = int(cat_key[7:])
            except ValueError:
                return []
            return [c for c in candidates if c.get("folder_id") == folder_id]
        return []

    async def _get_dialog_folders(self) -> List[Dict[str, Any]]:
        if self.user_client is None:
            return []
        try:
            from pyrogram import raw as pyrogram_raw

            result = await self.user_client.invoke(pyrogram_raw.functions.messages.GetDialogFilters())
            folders = []
            for folder in result:
                title = getattr(folder, "title", None)
                folder_id = getattr(folder, "id", None)
                if title and folder_id is not None:
                    folders.append({"id": int(folder_id), "title": str(title)})
            return folders
        except Exception:
            return []

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
            candidate_key = source_key(chat_id, candidate.get("topic_id"))
            if candidate_key in existing_source_keys or candidate_key in candidates_by_key:
                continue

            candidates_by_key[candidate_key] = candidate

        return self._sorted_source_candidates(list(candidates_by_key.values()))

    async def _ensure_bulk_import_session(self, user_id: int, group_id: int, refresh: bool = False) -> Dict[str, Any]:
        session_key = self._bulk_import_session_key(user_id, group_id)
        now = self._now()
        previous = self._bulk_import_sessions.get(session_key)
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        config = self._source_import_config(group_state)
        had_previous = isinstance(previous, dict)

        if had_previous and not refresh:
            previous["auto_sync_enabled"] = config["auto_sync_enabled"]
            previous["updated_at"] = now
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
            "updated_at": now,
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
            candidate_key = source_key(int(source["chat_id"]), source.get("topic_id"))
            if candidate_key not in selected:
                continue
            eligible += 1
            _, existed = await self._upsert_source(group_id, source)
            if not existed:
                added += 1
        return {"eligible": eligible, "added": added}

    async def _show_sources_list(self, callback_query, group_id: int) -> None:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = group_state.get("sources", {})
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
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = group_state.get("sources", {})
        if not sources:
            await callback_query.answer("No sources are configured yet.", show_alert=True)
            return

        choices = [(key, self._source_display_name(key, src)[:48]) for key, src in self._sorted_sources(group_state)]
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
        state_before = await self._state()
        group_state_before = state_before.get("groups", {}).get(str(group_id), default_group_state())
        settings_before = group_state_before.get("settings", {})
        auto_leave = bool(settings_before.get("auto_leave_after_source_delete", False))

        result = await self._remove_source_from_destination(group_id, source_k)
        removed_source = dict(result.get("source") or {})
        deleted = int(result.get("deleted", 0) or 0)
        skipped = int(result.get("skipped", 0) or 0)

        self._queue_callback_message_for_cleanup(callback_query)
        if skipped:
            await callback_query.answer(
                f"Source removed. Deleted {deleted} messages, {skipped} failed.",
                show_alert=True,
            )
        else:
            await callback_query.answer(f"Source removed. Deleted {deleted} messages.")

        await self._send_flow_acknowledge(
            callback_query.message.chat.id,
            callback_query.from_user.id,
            "source.removed",
            deleted=deleted,
            failed_suffix=self._flow_failed_suffix(skipped),
        )

        await self._offer_leave_source_prompt_if_orphaned(callback_query, removed_source, auto_leave=auto_leave)

    async def _show_source_filter_selector(self, callback_query, group_id: int, page: int = 0) -> None:
        state = await self._state()
        group_state = state.get("groups", {}).get(str(group_id), default_group_state())
        sources = group_state.get("sources", {})
        if not sources:
            await callback_query.answer("No sources are configured yet.", show_alert=True)
            return

        choices = [(key, self._source_display_name(key, src)[:48]) for key, src in self._sorted_sources(group_state)]
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

    async def _upsert_source(self, group_id: int, source: Dict[str, Any]) -> Tuple[str, bool]:
        source_k = source_key(int(source["chat_id"]), source.get("topic_id"))
        existed = False

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal existed
            groups = state.setdefault("groups", {})
            group_state = groups.setdefault(str(group_id), default_group_state())
            sources = group_state.setdefault("sources", {})
            existed = source_k in sources
            sources[source_k] = {
                "chat_id": source["chat_id"],
                "topic_id": source.get("topic_id"),
                "name": source.get("name"),
                "username": source.get("username"),
                "type": source.get("type"),
                "filters": sources.get(source_k, {}).get("filters", {"rules": []}),
            }
            return state

        await self.storage.update(updater)
        return source_k, existed

    async def _source_usage_locations(self, chat_id: int, topic_id: Optional[int]) -> List[Tuple[int, str]]:
        state = await self._state()
        hits: List[Tuple[int, str]] = []
        for gid_raw, group_state in state.get("groups", {}).items():
            try:
                gid = int(gid_raw)
            except (TypeError, ValueError):
                continue
            for source_k, src in group_state.get("sources", {}).items():
                try:
                    src_chat_id = int(src.get("chat_id"))
                except (TypeError, ValueError):
                    continue
                src_topic_id = src.get("topic_id")
                if src_chat_id == int(chat_id) and (src_topic_id or None) == (topic_id or None):
                    hits.append((gid, source_k))
        return hits

    async def _remove_source_from_destination(self, group_id: int, source_k: str) -> Dict[str, Any]:
        cleanup = await self._delete_forwarded_history_for_source(group_id, source_k)
        removed_source: Dict[str, Any] = {}

        def updater(state: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal removed_source
            groups = state.setdefault("groups", {})
            group_state = groups.setdefault(str(group_id), default_group_state())
            removed_source = dict(group_state.setdefault("sources", {}).pop(source_k, {}) or {})
            return state

        await self.storage.update(updater)
        return {
            "removed": bool(removed_source),
            "source": removed_source,
            "deleted": cleanup.get("deleted", 0),
            "skipped": cleanup.get("skipped", 0),
            "history_removed": cleanup.get("history_removed", 0),
        }

    async def _offer_leave_source_prompt_if_orphaned(self, callback_query, source: Dict[str, Any], *, auto_leave: bool = False) -> None:
        if not source or self.user_client is None:
            return
        try:
            chat_id = int(source.get("chat_id"))
        except (TypeError, ValueError):
            return
        topic_id = source.get("topic_id")
        if await self._source_usage_locations(chat_id, topic_id):
            return

        source_name = self._source_display_name(source_key(chat_id, topic_id), source)
        message_chat = getattr(getattr(callback_query, "message", None), "chat", None)
        chat_id_for_reply = int(message_chat.id) if message_chat is not None else None
        user_id = int(getattr(getattr(callback_query, "from_user", None), "id", 0) or 0)
        if auto_leave:
            try:
                await self.user_client.leave_chat(chat_id)
                if chat_id_for_reply is not None:
                    await self._send_flow_acknowledge(
                        chat_id_for_reply,
                        user_id,
                        "source.auto_leave_done",
                        source_name=escape(source_name),
                    )
            except Exception as exc:
                if chat_id_for_reply is not None:
                    await self._send_flow_acknowledge(
                        chat_id_for_reply,
                        user_id,
                        "source.auto_leave_failed",
                        source_name=escape(source_name),
                        error_text=escape(str(exc)),
                    )
            return

        token = self._store_intent_action({"type": "leave_source_chat", "chat_id": chat_id}, ttl_seconds=300)
        await callback_query.message.reply_text(
            self._flow_text("common.leave_orphan_source_prompt", source_name=escape(source_name)),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🚪 Leave Chat", callback_data=f"x:ia:{token}")],
                    [InlineKeyboardButton("Keep", callback_data="x:ack:home")],
                ]
            ),
        )

    async def _handle_add_source_input(self, message: Message, pending: Dict[str, Any]) -> None:
        user_id = message.from_user.id
        group_id = int(pending["group_id"])

        source, err = await self._resolve_source_from_message(message)
        if err:
            await self._reply_private_and_queue_cleanup(message, user_id, err)
            return

        source_k, existed = await self._upsert_source(group_id, source)
        self.pending_inputs.pop(user_id, None)
        source_identity = self._source_identity(source_k, source)
        source_name = self._source_display_name(source_k, source)
        await self._send_flow_acknowledge(
            message.chat.id,
            user_id,
            "source.exists" if existed else "source.added",
            source_name=escape(source_name),
            source_identity=source_identity,
        )