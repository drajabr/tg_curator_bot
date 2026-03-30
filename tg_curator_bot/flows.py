from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class FlowDefinition:
    key: str
    text_template: str


FLOW_DEFINITIONS: Dict[str, FlowDefinition] = {
    "common.only_authorized": FlowDefinition(
        key="common.only_authorized",
        text_template="Only the owner or authorized admins can use this.",
    ),
    "common.owner_only_admin": FlowDefinition(
        key="common.owner_only_admin",
        text_template="Administration is owner-only.",
    ),
    "common.canceled": FlowDefinition(
        key="common.canceled",
        text_template="Canceled.",
    ),
    "common.current_flow_canceled": FlowDefinition(
        key="common.current_flow_canceled",
        text_template="The current flow was canceled.",
    ),
    "common.no_active_flow": FlowDefinition(
        key="common.no_active_flow",
        text_template="There is no active flow to cancel.",
    ),
    "common.acknowledged": FlowDefinition(
        key="common.acknowledged",
        text_template="Acknowledged.",
    ),
    "common.action_expired": FlowDefinition(
        key="common.action_expired",
        text_template="This action expired. Try again.",
    ),
    "common.done": FlowDefinition(
        key="common.done",
        text_template="Done.",
    ),
    "common.unknown_action": FlowDefinition(
        key="common.unknown_action",
        text_template="Unknown action.",
    ),
    "common.rule_added": FlowDefinition(
        key="common.rule_added",
        text_template="Rule added.",
    ),
    "common.rule_removed": FlowDefinition(
        key="common.rule_removed",
        text_template="Rule removed.",
    ),
    "common.invalid_quick_action": FlowDefinition(
        key="common.invalid_quick_action",
        text_template="Invalid quick action.",
    ),
    "common.invalid_keyword_action": FlowDefinition(
        key="common.invalid_keyword_action",
        text_template="Invalid keyword action.",
    ),
    "common.keyword_missing": FlowDefinition(
        key="common.keyword_missing",
        text_template="Keyword missing",
    ),
    "common.no_message_metadata": FlowDefinition(
        key="common.no_message_metadata",
        text_template="No message metadata was found.",
    ),
    "common.no_source_for_message": FlowDefinition(
        key="common.no_source_for_message",
        text_template="No source was found for this message.",
    ),
    "common.pick_keyword_prompt": FlowDefinition(
        key="common.pick_keyword_prompt",
        text_template="Pick a keyword to block:",
    ),
    "common.leave_orphan_source_prompt": FlowDefinition(
        key="common.leave_orphan_source_prompt",
        text_template=(
            "Source <b>{source_name}</b> is no longer used in any destination.\n"
            "Do you want to leave this chat/channel with your user account?"
        ),
    ),
    "screen.live_events_waiting": FlowDefinition(
        key="screen.live_events_waiting",
        text_template="Waiting for forwarding events...",
    ),
    "screen.live_events": FlowDefinition(
        key="screen.live_events",
        text_template="Live Events\n{body}",
    ),
    "screen.dm_home": FlowDefinition(
        key="screen.dm_home",
        text_template=(
            "<b>🎛️ Curator Control</b>\n\n"
            "Owner: {owner_identity}\n"
            "User session: <b>{session_state}</b>\n"
            "User client: <b>{user_client_state}</b>\n"
            "Destinations: <b>{groups}</b>\n"
            "Sources: <b>{sources}</b>\n\n"
            "Add the bot to a destination group and make sure it can send messages there. "
            "Destinations appear automatically and all admin actions live in this DM."
        ),
    ),
    "screen.administration": FlowDefinition(
        key="screen.administration",
        text_template=(
            "<b>🛡️ Administration</b>\n\n"
            "Owner-only controls.\n"
            "Registered destinations: <b>{destination_count}</b>\n\n"
            "Use this area to remove destinations from the control panel, manage authorization, "
            "and export/import the bot state. "
            "Deleting a destination here also deletes its tracked forwarding history."
        ),
    ),
    "screen.authorization_title": FlowDefinition(
        key="screen.authorization_title",
        text_template="<b>✅ Authorization</b>",
    ),
    "screen.authorization_owner": FlowDefinition(
        key="screen.authorization_owner",
        text_template="Owner: {owner_label}",
    ),
    "screen.authorization_count": FlowDefinition(
        key="screen.authorization_count",
        text_template="Authorized admins: <b>{count}</b>",
    ),
    "screen.authorization_help": FlowDefinition(
        key="screen.authorization_help",
        text_template="Authorized admins can access admin-only controls without using the owner account.",
    ),
    "screen.authorization_remove_title": FlowDefinition(
        key="screen.authorization_remove_title",
        text_template="<b>🗑️ Remove Authorized Admin</b>",
    ),
    "screen.authorization_remove_help": FlowDefinition(
        key="screen.authorization_remove_help",
        text_template="Select an admin to revoke authorization.",
    ),
    "screen.authorization_remove_empty": FlowDefinition(
        key="screen.authorization_remove_empty",
        text_template="No authorized admins found.",
    ),
    "screen.destination_delete_title": FlowDefinition(
        key="screen.destination_delete_title",
        text_template="<b>🗑️ Delete Destination</b>",
    ),
    "screen.destination_delete_help_1": FlowDefinition(
        key="screen.destination_delete_help_1",
        text_template="Select a destination to remove from the bot control panel.",
    ),
    "screen.destination_delete_help_2": FlowDefinition(
        key="screen.destination_delete_help_2",
        text_template="This also deletes tracked forwarding history for that destination.",
    ),
    "screen.destination_delete_empty": FlowDefinition(
        key="screen.destination_delete_empty",
        text_template="No destination groups are registered yet.",
    ),
    "screen.destinations_title": FlowDefinition(
        key="screen.destinations_title",
        text_template="<b>🎯 Destinations</b>",
    ),
    "screen.destinations_empty": FlowDefinition(
        key="screen.destinations_empty",
        text_template="No destination groups are registered yet.",
    ),
    "screen.destinations_empty_help": FlowDefinition(
        key="screen.destinations_empty_help",
        text_template="Add the bot to a group and allow it to post there. It will show up here automatically.",
    ),
    "screen.destination": FlowDefinition(
        key="screen.destination",
        text_template=(
            "<b>{name}</b>\n\n"
            "Chat: {group_identity}\n"
            "Sources: <b>{source_count}</b>\n"
            "Group filter rules: <b>{group_filter_count}</b>\n"
            "Sources with source rules: <b>{source_filter_count}</b>\n"
            "Header: <b>{show_header}</b>\n"
            "Original date/time: <b>{show_source_datetime}</b>\n"
            "Original link: <b>{show_link}</b>\n"
            "Backfill after restart: <b>{backfill_enabled}</b>"
        ),
    ),
    "screen.sources_autosync": FlowDefinition(
        key="screen.sources_autosync",
        text_template="Auto-sync new chats: <b>{value}</b>",
    ),
    "screen.sources_filter": FlowDefinition(
        key="screen.sources_filter",
        text_template="Bulk import filter: <b>{value}</b>",
    ),
    "screen.sources_empty": FlowDefinition(
        key="screen.sources_empty",
        text_template="No sources configured yet.",
    ),
    "screen.sources_empty_help": FlowDefinition(
        key="screen.sources_empty_help",
        text_template="Use Add Source below, then send a forwarded message, a t.me link, or a chat handle/ID in this DM.",
    ),
    "screen.sources_pagination": FlowDefinition(
        key="screen.sources_pagination",
        text_template="Showing <b>{start}-{end}</b> of <b>{total}</b> sources (page <b>{page}/{page_count}</b>)",
    ),
    "screen.bulk_sources_not_ready": FlowDefinition(
        key="screen.bulk_sources_not_ready",
        text_template=(
            "<b>📚 Bulk Add Sources for {name}</b>\n\n"
            "User session is not ready. Configure it in terminal and restart the bot."
        ),
    ),
    "screen.bulk_sources_title": FlowDefinition(
        key="screen.bulk_sources_title",
        text_template="<b>📚 Bulk Add Sources for {name}</b>",
    ),
    "screen.bulk_sources_help_1": FlowDefinition(
        key="screen.bulk_sources_help_1",
        text_template="Tap a category to select or deselect all its chats, then import.",
    ),
    "screen.bulk_sources_help_2": FlowDefinition(
        key="screen.bulk_sources_help_2",
        text_template="Destinations and already-added sources are excluded.",
    ),
    "screen.bulk_sources_empty": FlowDefinition(
        key="screen.bulk_sources_empty",
        text_template="No joined channels or groups are available to import.",
    ),
    "screen.bulk_sources_available": FlowDefinition(
        key="screen.bulk_sources_available",
        text_template="Available: <b>{count}</b> chat{plural}",
    ),
    "screen.bulk_sources_selected": FlowDefinition(
        key="screen.bulk_sources_selected",
        text_template="Selected: <b>{count}</b>",
    ),
    "screen.bulk_sources_autosync": FlowDefinition(
        key="screen.bulk_sources_autosync",
        text_template="Auto-sync newly joined chats: <b>{value}</b>",
    ),
    "screen.filters": FlowDefinition(
        key="screen.filters",
        text_template=(
            "<b>🧰 Filters for {name}</b>\n\n"
            "Group filters: <b>{group_filter_count}</b> rules\n"
            "Sources with source-specific rules: <b>{source_filters}</b>\n"
            "Tracked forwarded messages: <b>{history_count}</b>\n\n"
            "Use <b>Reapply to Forwarded</b> to re-check already forwarded messages and delete those that no longer pass."
        ),
    ),
    "screen.rules_group_title": FlowDefinition(
        key="screen.rules_group_title",
        text_template="Group filters for {name}",
    ),
    "screen.rules_source_title": FlowDefinition(
        key="screen.rules_source_title",
        text_template="Source filters for {name}",
    ),
    "screen.rules_empty": FlowDefinition(
        key="screen.rules_empty",
        text_template="Rules: none yet",
    ),
    "screen.rules_label": FlowDefinition(
        key="screen.rules_label",
        text_template="Rules:",
    ),
    "screen.settings": FlowDefinition(
        key="screen.settings",
        text_template=(
            "<b>⚙️ Settings for {name}</b>\n\n"
            "Sources: <b>{source_count}</b>\n"
            "Header: <b>{show_header}</b>\n"
            "Original date/time: <b>{show_source_datetime}</b>\n"
            "Original link: <b>{show_link}</b>\n"
            "Auto leave after source delete: <b>{auto_leave}</b>\n"
            "Backfill after restart: <b>{backfill_enabled}</b>"
        ),
    ),
    "screen.source_test_title": FlowDefinition(
        key="screen.source_test_title",
        text_template="<b>🧪 Source Test for {name}</b>",
    ),
    "screen.source_test_method": FlowDefinition(
        key="screen.source_test_method",
        text_template="Method: forward the latest source message without filters, then delete the probe.",
    ),
    "screen.source_test_progress": FlowDefinition(
        key="screen.source_test_progress",
        text_template="Progress: <b>{completed}/{total}</b>",
    ),
    "screen.source_test_working": FlowDefinition(
        key="screen.source_test_working",
        text_template="Working: <b>{working}</b>",
    ),
    "screen.source_test_failing": FlowDefinition(
        key="screen.source_test_failing",
        text_template="Failing: <b>{failing}</b>",
    ),
    "screen.source_test_status_completed": FlowDefinition(
        key="screen.source_test_status_completed",
        text_template="Status: <b>Completed</b>",
    ),
    "screen.source_test_remaining": FlowDefinition(
        key="screen.source_test_remaining",
        text_template="Remaining: <b>{remaining}</b>",
    ),
    "screen.source_test_status_running": FlowDefinition(
        key="screen.source_test_status_running",
        text_template="Status: <b>Running</b>",
    ),
    "screen.source_test_current": FlowDefinition(
        key="screen.source_test_current",
        text_template="Current: <b>{current}</b>",
    ),
    "screen.source_test_recent_failures": FlowDefinition(
        key="screen.source_test_recent_failures",
        text_template="Recent failures:",
    ),
    "screen.history": FlowDefinition(
        key="screen.history",
        text_template=(
            "<b>🧹 Clean History for {name}</b>\n\n"
            "Tracked forwarded messages: <b>{total_entries}</b>\n"
            "Sources in history: <b>{tracked_sources}</b>\n\n"
            "Clean all tracked history for this destination or remove entries for one source only."
        ),
    ),
    "screen.history_source_title": FlowDefinition(
        key="screen.history_source_title",
        text_template="<b>📡 Clean Single Source History for {name}</b>",
    ),
    "screen.history_source_empty": FlowDefinition(
        key="screen.history_source_empty",
        text_template="No tracked history exists for this destination yet.",
    ),
    "screen.bot_status_title": FlowDefinition(
        key="screen.bot_status_title",
        text_template="Bot Status",
    ),
    "screen.bot_status_owner": FlowDefinition(
        key="screen.bot_status_owner",
        text_template="Owner: {owner}",
    ),
    "screen.bot_status_api": FlowDefinition(
        key="screen.bot_status_api",
        text_template="User API credentials configured: {value}",
    ),
    "screen.bot_status_session": FlowDefinition(
        key="screen.bot_status_session",
        text_template="User session string configured: {value}",
    ),
    "screen.bot_status_client": FlowDefinition(
        key="screen.bot_status_client",
        text_template="User client: {value}",
    ),
    "screen.bot_status_account": FlowDefinition(
        key="screen.bot_status_account",
        text_template="User account: {value}",
    ),
    "screen.bot_status_destinations": FlowDefinition(
        key="screen.bot_status_destinations",
        text_template="Destination groups: {value}",
    ),
    "screen.bot_status_sources": FlowDefinition(
        key="screen.bot_status_sources",
        text_template="Total sources: {value}",
    ),
    "screen.heartbeat_never": FlowDefinition(
        key="screen.heartbeat_never",
        text_template="Never",
    ),
    "screen.heartbeat": FlowDefinition(
        key="screen.heartbeat",
        text_template=(
            "<b>🟢 Bot Heartbeat</b>\n"
            "Last check: <code>{last_check}</code>\n"
            "Uptime: <b>{uptime}</b>\n"
            "Owner: {owner_identity}\n"
            "User client: <b>{user_client_state}</b>\n"
            "Authorized admins: <b>{authorized_admins}</b>\n"
            "Destinations: <b>{groups}</b>\n"
            "Sources: <b>{sources}</b>\n"
            "Tracked forwards: <b>{forwarded_entries}</b>\n"
            "Forwards in last 1h: <b>{recent_1h}</b>\n"
            "Last forwarded: {last_forwarded_line}\n"
            "Pending flows: <b>{pending_flows}</b>\n"
            "Media groups in-flight: <b>{running_media_tasks}</b>\n"
            "Dedupe sources tracked: <b>{dedupe_count}</b>"
        ),
    ),
    "admin.authorized_removed": FlowDefinition(
        key="admin.authorized_removed",
        text_template="Authorized admin removed.",
    ),
    "admin.authorized_added": FlowDefinition(
        key="admin.authorized_added",
        text_template="Authorized admin added: <b>{label}</b>",
    ),
    "admin.authorized_exists": FlowDefinition(
        key="admin.authorized_exists",
        text_template="Admin is already authorized: <b>{label}</b>",
    ),
    "admin.export_sent": FlowDefinition(
        key="admin.export_sent",
        text_template="Export sent.",
    ),
    "admin.import_done": FlowDefinition(
        key="admin.import_done",
        text_template="Import completed. State has been replaced from uploaded data.json.",
    ),
    "admin.destination_deleted": FlowDefinition(
        key="admin.destination_deleted",
        text_template="Destination deleted. Removed <b>{history_removed}</b> history {entry_word}.",
    ),
    "history.cleared_all": FlowDefinition(
        key="history.cleared_all",
        text_template="Removed <b>{removed}</b> history {entry_word}.",
    ),
    "history.cleared_source": FlowDefinition(
        key="history.cleared_source",
        text_template="Removed <b>{removed}</b> history {entry_word} for the selected source.",
    ),
    "source.added": FlowDefinition(
        key="source.added",
        text_template="Source added: <b>{source_name}</b> ({source_identity})",
    ),
    "source.exists": FlowDefinition(
        key="source.exists",
        text_template="Source already exists: <b>{source_name}</b> ({source_identity})",
    ),
    "source.removed": FlowDefinition(
        key="source.removed",
        text_template=(
            "Source removed from destination. Deleted <b>{deleted}</b> forwarded message(s)"
            "{failed_suffix}."
        ),
    ),
    "source.removed_bulk": FlowDefinition(
        key="source.removed_bulk",
        text_template=(
            "Removed source from <b>{removed_count}</b> destination(s). "
            "Deleted <b>{deleted_messages}</b> forwarded message(s){failed_suffix}."
        ),
    ),
    "rule.sender_added": FlowDefinition(
        key="rule.sender_added",
        text_template="Sender filter rule added.",
    ),
    "rule.exact_added": FlowDefinition(
        key="rule.exact_added",
        text_template="Exact-text filter rule added.",
    ),
    "rule.bulk_sender_added": FlowDefinition(
        key="rule.bulk_sender_added",
        text_template="Sender rule added to <b>{applied}</b> destination(s).",
    ),
    "rule.manual_added": FlowDefinition(
        key="rule.manual_added",
        text_template="Rule added.",
    ),
    "rule.reapply_prompt": FlowDefinition(
        key="rule.reapply_prompt",
        text_template=(
            "✅ Rule added.\n\n"
            "Retroactively apply it to messages already forwarded to this destination?\n\n"
            "Choose a time window:"
        ),
    ),
    "rule.reapply_custom_prompt": FlowDefinition(
        key="rule.reapply_custom_prompt",
        text_template="How many days back should messages be checked? Send a number (e.g. 30):",
    ),
    "rule.reapply_done": FlowDefinition(
        key="rule.reapply_done",
        text_template=(
            "✅ Reapply complete — scanned <b>{scanned}</b> message(s), "
            "deleted <b>{deleted}</b>{failed_suffix}."
        ),
    ),
    "rule.reapply_none": FlowDefinition(
        key="rule.reapply_none",
        text_template="Skipped retroactive reapply.",
    ),
    "rule.reapply_invalid_days": FlowDefinition(
        key="rule.reapply_invalid_days",
        text_template="Send a positive whole number of days (e.g. 30).",
    ),
    "rule.exact_quick_added": FlowDefinition(
        key="rule.exact_quick_added",
        text_template="Exact-match rule added.",
    ),
    "rule.sender_quick_added": FlowDefinition(
        key="rule.sender_quick_added",
        text_template="Sender rule added.",
    ),
    "rule.keyword_quick_added": FlowDefinition(
        key="rule.keyword_quick_added",
        text_template="Keyword rule added: {keyword}",
    ),
    "rule.no_text_for_exact": FlowDefinition(
        key="rule.no_text_for_exact",
        text_template="There is no text or caption available for an exact-match rule.",
    ),
    "rule.no_sender_metadata": FlowDefinition(
        key="rule.no_sender_metadata",
        text_template="No sender metadata was found.",
    ),
    "rule.no_keywords_found": FlowDefinition(
        key="rule.no_keywords_found",
        text_template="No keywords were found.",
    ),
    "intent.resume_unavailable": FlowDefinition(
        key="intent.resume_unavailable",
        text_template="This flow cannot be resumed.",
    ),
    "intent.resumed": FlowDefinition(
        key="intent.resumed",
        text_template="Resumed.",
    ),
    "intent.resumed_continue": FlowDefinition(
        key="intent.resumed_continue",
        text_template="Resumed. Continue with the previous step.",
    ),
    "source.data_missing": FlowDefinition(
        key="source.data_missing",
        text_template="Source data is missing.",
    ),
    "source.left_chat_toast": FlowDefinition(
        key="source.left_chat_toast",
        text_template="Left chat.",
    ),
    "source.invalid_chat_id": FlowDefinition(
        key="source.invalid_chat_id",
        text_template="Invalid chat id.",
    ),
    "source.leave_chat_failed": FlowDefinition(
        key="source.leave_chat_failed",
        text_template="Could not leave chat: {error}",
    ),
    "source.user_client_not_connected": FlowDefinition(
        key="source.user_client_not_connected",
        text_template="User client is not connected.",
    ),
    "source.auto_leave_done": FlowDefinition(
        key="source.auto_leave_done",
        text_template="Auto left unused source <b>{source_name}</b> from user account.",
    ),
    "source.auto_leave_failed": FlowDefinition(
        key="source.auto_leave_failed",
        text_template="Could not auto leave <b>{source_name}</b>: <b>{error_text}</b>",
    ),
    "source.left_chat": FlowDefinition(
        key="source.left_chat",
        text_template="Left unused source chat.",
    ),
}


def render_flow_text(flow_key: str, **context: str) -> str:
    definition = FLOW_DEFINITIONS.get(flow_key)
    if definition is None:
        return str(context.get("fallback") or "Done.")

    rendered = definition.text_template
    for key, value in context.items():
        rendered = rendered.replace("{" + key + "}", str(value))

    return rendered
