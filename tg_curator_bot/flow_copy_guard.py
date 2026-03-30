import ast
from pathlib import Path
from typing import List, Set


_MESSAGE_CALLS = {
    "reply_text",
    "answer",
    "send_message",
    "_safe_edit_message_text",
}


def _call_name(node: ast.Call) -> str:
    fn = node.func
    if isinstance(fn, ast.Attribute):
        return str(fn.attr)
    if isinstance(fn, ast.Name):
        return str(fn.id)
    return ""


def _extract_text_arg(call_name: str, node: ast.Call):
    for kw in node.keywords or []:
        if kw.arg == "text":
            return kw.value

    if call_name == "send_message":
        # send_message(chat_id, text, ...)
        return node.args[1] if len(node.args) >= 2 else None

    if call_name in {"reply_text", "answer"}:
        return node.args[0] if node.args else None

    if call_name == "_safe_edit_message_text":
        # _safe_edit_message_text(message, text, ...)
        return node.args[1] if len(node.args) >= 2 else None

    return None


def collect_inline_message_literals(file_path: str) -> List[str]:
    source = Path(file_path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    found: Set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        call_name = _call_name(node)
        if call_name not in _MESSAGE_CALLS:
            continue

        text_arg = _extract_text_arg(call_name, node)
        if isinstance(text_arg, ast.Constant) and isinstance(text_arg.value, str):
            value = text_arg.value.strip()
            if value:
                found.add(value)

    return sorted(found)
