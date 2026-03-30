import os
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Dict, List, Optional, Tuple


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
            "auto_leave_after_source_delete": False,
            "backfill_enabled": True,
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


class _ScreensMixin:
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
            self._flow_text("screen.bot_status_title"),
            self._flow_text("screen.bot_status_owner", owner=owner_identity),
            self._flow_text("screen.bot_status_api", value=("Yes" if has_api else "No")),
            self._flow_text("screen.bot_status_session", value=("Yes" if has_session else "No")),
            self._flow_text("screen.bot_status_client", value=user_client_state),
            self._flow_text("screen.bot_status_account", value=user_identity),
            self._flow_text("screen.bot_status_destinations", value=groups),
            self._flow_text("screen.bot_status_sources", value=sources),
        ]
        return "\n".join(lines)

    def _format_uptime_duration(self) -> str:
        delta = datetime.now(timezone.utc) - self.started_at_utc
        total_seconds = max(0, int(delta.total_seconds()))
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days:
            return f"{days}d {hours:02d}h {minutes:02d}m"
        return f"{hours:02d}h {minutes:02d}m {seconds:02d}s"

    async def _forwarded_entry_count(self) -> int:
        logs = await self._forward_logs_state()
        count_entries = 0
        for value in logs.values():
            if isinstance(value, dict):
                count_entries += len(value)
        return count_entries

    def _format_elapsed(self, past_utc: datetime) -> str:
        now = datetime.now(timezone.utc)
        delta = max(0, int((now - past_utc).total_seconds()))
        days, remainder = divmod(delta, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days:
            return f"{days}d {hours}h"
        if hours:
            return f"{hours}h {minutes}m"
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    async def _forward_runtime_stats(self) -> Dict[str, Any]:
        logs = await self._forward_logs_state()
        total = 0
        recent_1h = 0
        latest: Optional[datetime] = None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

        for per_group in logs.values():
            if not isinstance(per_group, dict):
                continue
            total += len(per_group)
            for entry in per_group.values():
                if not isinstance(entry, dict):
                    continue
                parsed = self._parse_iso_datetime(entry.get("logged_at") or entry.get("source_date"))
                if parsed is None:
                    continue
                if parsed >= cutoff:
                    recent_1h += 1
                if latest is None or parsed > latest:
                    latest = parsed

        return {
            "total": total,
            "recent_1h": recent_1h,
            "latest": latest,
        }

    async def _heartbeat_status_text(self) -> str:
        state = await self._state()
        owner_id = state.get("owner_id")
        owner_identity = await self._owner_identity(owner_id, html=True)
        groups, sources = await self._count_groups_sources()
        forward_stats = await self._forward_runtime_stats()
        forwarded_entries = int(forward_stats.get("total", 0))
        recent_1h = int(forward_stats.get("recent_1h", 0))
        latest_forwarded = forward_stats.get("latest")
        user_client_state = "Connected" if self.user_client is not None else "Not connected"
        last_check = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if latest_forwarded is None:
            last_forwarded_line = self._flow_text("screen.heartbeat_never")
        else:
            last_forwarded_line = (
                f"<code>{latest_forwarded.strftime('%Y-%m-%d %H:%M:%S UTC')}</code> "
                f"(<i>{self._format_elapsed(latest_forwarded)} ago</i>)"
            )
        running_media_tasks = sum(1 for task in self._media_group_tasks.values() if not task.done())
        authorized_admins = len(self._authorized_admin_ids_from_state(state))
        return self._flow_text(
            "screen.heartbeat",
            last_check=last_check,
            uptime=self._format_uptime_duration(),
            owner_identity=owner_identity,
            user_client_state=user_client_state,
            authorized_admins=authorized_admins,
            groups=groups,
            sources=sources,
            forwarded_entries=forwarded_entries,
            recent_1h=recent_1h,
            last_forwarded_line=last_forwarded_line,
            pending_flows=len(self.pending_inputs),
            running_media_tasks=running_media_tasks,
            dedupe_count=len(self._global_dedupe_last_signature_by_source),
        )

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
        body = "\n".join(lines) if lines else self._flow_text("screen.live_events_waiting")
        return self._flow_text("screen.live_events", body=body)

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

    async def _dm_home_text(self) -> Tuple[str, bool, int, int]:
        state = await self._state()
        owner_id = state.get("owner_id")
        sess = state.get("user_session", {})
        session_ready = bool(sess.get("session_string"))
        groups, sources = await self._count_groups_sources()
        owner_identity = await self._owner_identity(owner_id, html=True)
        user_client_state = "Connected" if self.user_client is not None else "Not connected"
        text = self._flow_text(
            "screen.dm_home",
            owner_identity=owner_identity,
            session_state=("Ready" if session_ready else "Missing"),
            user_client_state=user_client_state,
            groups=groups,
            sources=sources,
        )
        return text, session_ready, groups, sources

    async def _administration_screen_text(self) -> str:
        state = await self._state()
        groups = state.get("groups", {})
        return self._flow_text("screen.administration", destination_count=len(groups))

    async def _authorization_screen_text(self) -> str:
        state = await self._state()
        owner_id = state.get("owner_id")
        owner_label = await self._owner_identity(owner_id, html=True)
        authorized_ids = [int(admin_id) for admin_id in state.get("authorized_admin_ids", []) if str(admin_id).lstrip("-").isdigit()]
        meta = state.get("authorized_admin_meta", {})

        lines = [
            self._flow_text("screen.authorization_title"),
            "",
            self._flow_text("screen.authorization_owner", owner_label=owner_label),
            self._flow_text("screen.authorization_count", count=len(authorized_ids)),
            "",
            self._flow_text("screen.authorization_help"),
        ]

        if authorized_ids:
            lines.append("")
            for admin_id in sorted(authorized_ids):
                admin_meta = meta.get(str(admin_id), {}) if isinstance(meta, dict) else {}
                username = self._normalize_username(admin_meta.get("username") if isinstance(admin_meta, dict) else None)
                lines.append(f"• {self._identity_label(username, admin_id, html=True)}")

        return "\n".join(lines)

    async def _authorization_remove_screen_text(self) -> str:
        entries = await self._authorization_admin_entries()
        lines = [
            self._flow_text("screen.authorization_remove_title"),
            "",
            self._flow_text("screen.authorization_remove_help"),
        ]
        if not entries:
            lines.extend(["", self._flow_text("screen.authorization_remove_empty")])
        return "\n".join(lines)

    async def _authorization_admin_entries(self) -> List[Tuple[int, str]]:
        state = await self._state()
        authorized_ids = [int(admin_id) for admin_id in state.get("authorized_admin_ids", []) if str(admin_id).lstrip("-").isdigit()]
        meta = state.get("authorized_admin_meta", {})
        entries: List[Tuple[int, str]] = []

        for admin_id in sorted(set(authorized_ids)):
            admin_meta = meta.get(str(admin_id), {}) if isinstance(meta, dict) else {}
            username = self._normalize_username(admin_meta.get("username") if isinstance(admin_meta, dict) else None)
            entries.append((admin_id, self._identity_label(username, admin_id, html=False)))

        return entries

    async def _admin_destination_delete_screen_text(self) -> str:
        state = await self._state()
        groups = state.get("groups", {})
        lines = [
            self._flow_text("screen.destination_delete_title"),
            "",
            self._flow_text("screen.destination_delete_help_1"),
            self._flow_text("screen.destination_delete_help_2"),
        ]
        if not groups:
            lines.extend(["", self._flow_text("screen.destination_delete_empty")])
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

    async def _destinations_screen_text(self) -> str:
        state = await self._state()
        groups = state.get("groups", {})
        lines = [self._flow_text("screen.destinations_title")]
        if not groups:
            lines.append("")
            lines.append(self._flow_text("screen.destinations_empty"))
            lines.append(self._flow_text("screen.destinations_empty_help"))
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
        group_state = self._group_state(state, group_id) or default_group_state()
        name = self._group_display_name(group_id, group_state)
        settings = group_state.get("settings", {})
        group_filters = group_state.get("group_filters", {})
        source_count = len(group_state.get("sources", {}))
        source_filter_count = sum(1 for src in group_state.get("sources", {}).values() if src.get("filters", {}).get("rules"))
        group_identity = self._group_identity(group_id, group_state, html=True)
        return self._flow_text(
            "screen.destination",
            name=name,
            group_identity=group_identity,
            source_count=source_count,
            group_filter_count=len(group_filters.get("rules", [])),
            source_filter_count=source_filter_count,
            show_header=self._bool_label(bool(settings.get("show_header", True))),
            show_source_datetime=self._bool_label(bool(settings.get("show_source_datetime", False))),
            show_link=self._bool_label(bool(settings.get("show_link", True))),
            backfill_enabled=self._bool_label(bool(settings.get("backfill_enabled", True))),
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
        group_state = self._group_state(state, group_id) or default_group_state()
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
        lines.append(self._flow_text("screen.sources_autosync", value=self._bool_label(import_config["auto_sync_enabled"])))
        lines.append(self._flow_text("screen.sources_filter", value=self._source_import_filter_label(import_config["filter_mode"])))
        if not sources:
            lines.append(self._flow_text("screen.sources_empty"))
            lines.append(self._flow_text("screen.sources_empty_help"))
            return "\n".join(lines)

        lines.append("")
        if page_count > 1:
            lines.append(
                self._flow_text("screen.sources_pagination", start=start + 1, end=end, total=total_sources, page=page + 1, page_count=page_count)
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
        group_state = self._group_state(state, group_id) or default_group_state()
        name = self._group_display_name(group_id, group_state)
        if self.user_client is None:
            config = self._source_import_config(group_state)
            return (
                self._flow_text("screen.bulk_sources_not_ready", name=name),
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

        lines = [self._flow_text("screen.bulk_sources_title", name=name)]
        lines.append("")
        lines.append(self._flow_text("screen.bulk_sources_help_1"))
        lines.append(self._flow_text("screen.bulk_sources_help_2"))
        lines.append("")

        if not candidates:
            lines.append(self._flow_text("screen.bulk_sources_empty"))
            return "\n".join(lines), session

        lines.append(self._flow_text("screen.bulk_sources_available", count=len(candidates), plural=("s" if len(candidates) != 1 else "")))
        for cat in categories:
            lines.append(f"  • {cat['label']}")
        lines.append("")
        lines.append(self._flow_text("screen.bulk_sources_selected", count=selected_count))
        lines.append(self._flow_text("screen.bulk_sources_autosync", value=self._bool_label(bool(session.get("auto_sync_enabled")))))

        return "\n".join(lines), session

    async def _filters_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = self._group_state(state, group_id) or default_group_state()
        name = self._group_display_name(group_id, group_state)
        group_filters = group_state.get("group_filters", {})
        source_filters = sum(1 for source in group_state.get("sources", {}).values() if source.get("filters", {}).get("rules"))
        history = await self._group_forward_history(group_id)
        return self._flow_text(
            "screen.filters",
            name=name,
            group_filter_count=len(group_filters.get("rules", [])),
            source_filters=source_filters,
            history_count=len(history),
        )

    async def _rules_screen_text(self, group_id: int, scope: str, source_k: Optional[str]) -> str:
        state = await self._state()
        group_state = self._group_state(state, group_id) or default_group_state()
        target = await self._filter_target(state, group_id, scope, source_k)
        if scope == "gf":
            title = self._flow_text("screen.rules_group_title", name=self._group_display_name(group_id, group_state))
        else:
            source = group_state.get("sources", {}).get(str(source_k), {})
            title = self._flow_text("screen.rules_source_title", name=self._source_display_name(str(source_k), source))
        lines = [f"<b>{title}</b>"]
        rules = target.get("rules", [])
        if not rules:
            lines.append(self._flow_text("screen.rules_empty"))
            return "\n".join(lines)
        lines.append(self._flow_text("screen.rules_label"))
        for index, rule in enumerate(rules, start=1):
            lines.append(f"{index}. {self._format_rule(rule)}")
        return "\n".join(lines)

    async def _settings_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = self._group_state(state, group_id) or default_group_state()
        name = self._group_display_name(group_id, group_state)
        settings = group_state.get("settings", {})
        source_count = len(group_state.get("sources", {}))
        return self._flow_text(
            "screen.settings",
            name=name,
            source_count=source_count,
            show_header=self._bool_label(bool(settings.get("show_header", True))),
            show_source_datetime=self._bool_label(bool(settings.get("show_source_datetime", False))),
            show_link=self._bool_label(bool(settings.get("show_link", True))),
            auto_leave=self._bool_label(bool(settings.get("auto_leave_after_source_delete", False))),
            backfill_enabled=self._bool_label(bool(settings.get("backfill_enabled", True))),
        )

    async def _history_screen_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = self._group_state(state, group_id) or default_group_state()
        name = self._group_display_name(group_id, group_state)
        history = await self._group_forward_history(group_id)
        total_entries = len(history)
        tracked_sources = len({entry.get("source_key") for entry in history.values() if entry.get("source_key")})
        return self._flow_text(
            "screen.history",
            name=name,
            total_entries=total_entries,
            tracked_sources=tracked_sources,
        )

    async def _history_source_selector_text(self, group_id: int) -> str:
        state = await self._state()
        group_state = self._group_state(state, group_id) or default_group_state()
        name = self._group_display_name(group_id, group_state)
        history = await self._group_forward_history(group_id)
        choices = self._history_source_choices(group_state, history)

        lines = [self._flow_text("screen.history_source_title", name=name)]
        if not choices:
            lines.append("")
            lines.append(self._flow_text("screen.history_source_empty"))
            return "\n".join(lines)

        lines.append("")
        for source_key_value, _, count in choices:
            source = group_state.get("sources", {}).get(source_key_value, {})
            source_name = self._source_display_name(source_key_value, source)
            source_identity = self._source_identity(source_key_value, source, html=True)
            lines.append(f"• <b>{source_name}</b> — {source_identity} — {count} messages")
        return "\n".join(lines)