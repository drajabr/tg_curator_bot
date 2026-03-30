import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from ._screens import default_group_state
from .filters import evaluate_filters
from .keyboards import reapply_rule_prompt_menu, rules_menu


logger = logging.getLogger("tg-curator-bot")


class _RulesMixin:
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

    async def _retroapply_filters_in_range(self, group_id: int, since: Optional[datetime]) -> Dict[str, int]:
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
                continue

            if since is not None:
                logged_at = self._parse_iso_datetime(entry.get("logged_at"))
                if logged_at is None or logged_at < since:
                    continue

            scanned += 1
            source_k = entry.get("source_key")
            message_stub = self._build_logged_message_stub(entry)

            if not source_k:
                continue

            source = sources.get(str(source_k))
            if not isinstance(source, dict):
                continue

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
        await callback_query.answer(self._flow_text("common.rule_removed"))
        await self._safe_edit_message_text(
            callback_query.message,
            await self._rules_screen_text(group_id, scope, source_k),
            reply_markup=rules_menu(group_id, scope, source_k),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    def _flow_failed_suffix(self, skipped: int) -> str:
        skipped_count = int(skipped or 0)
        return f", failed <b>{skipped_count}</b>" if skipped_count else ""

    async def _prompt_reapply_rule(
        self,
        group_id: int,
        chat_id: int,
        user_id: int,
        *,
        edit_message=None,
    ) -> None:
        token = self._store_intent_action(
            {"type": "reapply_rule", "group_id": group_id, "chat_id": chat_id},
            ttl_seconds=300,
        )
        prompt_text = self._flow_text("rule.reapply_prompt")
        markup = reapply_rule_prompt_menu(token)
        if edit_message is not None:
            await self._safe_edit_message_text(
                edit_message,
                prompt_text,
                reply_markup=markup,
            )
        else:
            if self.bot is None:
                return
            try:
                sent = await self.bot.send_message(
                    int(chat_id),
                    prompt_text,
                    reply_markup=markup,
                    disable_notification=True,
                )
                try:
                    self._queue_flow_cleanup_message(user_id, self._message_id(sent))
                except Exception:
                    pass
            except Exception as exc:
                logger.warning("Failed to send reapply prompt | chat_id=%s | error=%s", chat_id, exc)

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