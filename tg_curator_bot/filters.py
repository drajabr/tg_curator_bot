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
    mode = str(filter_obj.get("mode", "blocklist")).lower()
    rules = filter_obj.get("rules", [])
    if not isinstance(rules, Iterable):
        rules = []

    matched = any(_rule_matches(rule, message) for rule in rules if isinstance(rule, dict))

    if mode == "allowlist":
        return matched
    return not matched
