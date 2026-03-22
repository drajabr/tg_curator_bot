from html import escape


def source_header(name: str, chat_id: int) -> str:
    safe = escape((name or "Unknown Source").strip())
    return f"<b><i>{safe} • {chat_id}</i></b>"


def original_message_link(chat_id: int, message_id: int, username: str | None = None) -> str:
    if username:
        return f"https://t.me/{username}/{message_id}"

    raw = str(chat_id)
    if raw.startswith("-100"):
        internal = raw[4:]
        return f"https://t.me/c/{internal}/{message_id}"
    return ""


def compose_text_payload(header: str, body: str, link: str, show_header: bool, show_link: bool) -> str:
    parts = []
    if show_header:
        parts.append(header)
    if body:
        parts.append(escape(body))
    if show_link and link:
        parts.append(f"<a href=\"{link}\">Original Message</a>")
    return "\n\n".join(parts).strip()


def compose_caption_payload(header: str, original_caption: str, link: str, show_header: bool, show_link: bool) -> str:
    parts = []
    if show_header:
        parts.append(header)
    if original_caption:
        parts.append(escape(original_caption))
    if show_link and link:
        parts.append(f"<a href=\"{link}\">Original Message</a>")
    caption = "\n\n".join(parts).strip()
    return caption[:1024]
