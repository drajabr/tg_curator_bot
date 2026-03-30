from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple


class _HistoryMixin:
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

        return f"{chat_id}|{topic_id or 0}"

    def _entry_matches_source(self, entry: Any, source_k: str) -> bool:
        if not source_k:
            return False
        return self._entry_source_key(entry) == str(source_k)