import re
from typing import Any, Dict, Iterable


def _message_type(message) -> str:
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
    if message.animation:
        return "animation"
    if message.sticker:
        return "sticker"
    if message.poll:
        return "poll"
    return "other"


def _message_text(message) -> str:
    return (message.text or message.caption or "").strip()


def _has_link(text: str) -> bool:
    return bool(re.search(r"https?://|t\.me/|www\.", text, flags=re.IGNORECASE))


def _rule_matches(rule: Dict[str, Any], message) -> bool:
    rule_type = rule.get("type")
    text = _message_text(message)
    text_low = text.lower()

    if rule_type == "keyword":
        values = [str(v).strip().lower() for v in rule.get("values", []) if str(v).strip()]
        return any(v in text_low for v in values)

    if rule_type == "exact":
        value = str(rule.get("value", "")).strip()
        return bool(value) and text == value

    if rule_type == "message_type":
        value = str(rule.get("value", "")).strip().lower()
        return bool(value) and _message_type(message) == value

    if rule_type == "sender":
        sender_id = None
        if message.from_user:
            sender_id = message.from_user.id
        elif message.sender_chat:
            sender_id = message.sender_chat.id

        values = rule.get("values", [])
        normalized = set()
        for item in values:
            try:
                normalized.add(int(item))
            except (TypeError, ValueError):
                continue
        return sender_id in normalized if sender_id is not None else False

    if rule_type == "has_link":
        value = bool(rule.get("value", True))
        return _has_link(text) if value else (not _has_link(text))

    return False


def evaluate_filters(filter_obj: Dict[str, Any], message) -> bool:
    """
    Evaluate filters with support for per-rule modes.
    
    - Rules can have individual "mode" fields (allowlist/blocklist)
    - If a rule lacks a mode, falls back to filter-level mode
    - Allowlist rules: message must match at least one
    - Blocklist rules: message must not match any
    - Message passes only if all rule conditions are satisfied
    """
    rules = filter_obj.get("rules", [])
    if not isinstance(rules, Iterable):
        rules = []
    
    default_mode = str(filter_obj.get("mode", "blocklist")).lower()
    
    # Separate rules by their mode (per-rule mode takes precedence)
    allowlist_rules = []
    blocklist_rules = []
    
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        rule_mode = str(rule.get("mode", default_mode)).lower()
        if rule_mode == "allowlist":
            allowlist_rules.append(rule)
        else:
            blocklist_rules.append(rule)
    
    # Check allowlist rules: at least one must match (if any exist)
    if allowlist_rules:
        if not any(_rule_matches(rule, message) for rule in allowlist_rules):
            return False
    
    # Check blocklist rules: none must match (if any exist)
    if blocklist_rules:
        if any(_rule_matches(rule, message) for rule in blocklist_rules):
            return False
    
    # If no rules exist, default behavior based on filter-level mode
    if not rules:
        return default_mode == "blocklist"  # Empty blocklist = pass, empty allowlist = fail
    
    return True
