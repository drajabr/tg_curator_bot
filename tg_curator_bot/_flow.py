import logging
from typing import Any, Optional, Tuple

from pyrogram.types import Message
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .flows import render_flow_text
from .keyboards import dm_admin_menu


logger = logging.getLogger("tg-curator-bot")


class _FlowMixin:
    async def _home_panel_payload(self, status_line: Optional[str] = None, user_id: Optional[int] = None) -> Tuple[str, Any]:
        text, session_ready, groups, sources = await self._dm_home_text()
        if status_line:
            text = f"{status_line}\n\n{text}"
        show_admin_menu = await self._is_owner(user_id)
        return text, dm_admin_menu(session_ready, groups, sources, show_admin_menu=show_admin_menu)

    def _history_entry_word(self, count: int) -> str:
        return "entry" if int(count) == 1 else "entries"

    def _flow_text(self, flow_key: str, **context: Any) -> str:
        return render_flow_text(flow_key, **context)

    def _queue_flow_cleanup_message(self, user_id: Optional[int], message_id: Optional[int]) -> None:
        if user_id is None or message_id is None:
            return
        uid = int(user_id)
        mid = int(message_id)
        bucket = self.flow_cleanup_message_ids.setdefault(uid, [])
        if mid in bucket:
            return
        bucket.append(mid)
        if len(bucket) > 50:
            self.flow_cleanup_message_ids[uid] = bucket[-50:]

    def _queue_callback_message_for_cleanup(self, callback_query) -> None:
        message = getattr(callback_query, "message", None)
        user = getattr(callback_query, "from_user", None)
        if message is None or user is None:
            return
        try:
            self._queue_flow_cleanup_message(int(user.id), self._message_id(message))
        except Exception:
            return

    async def _cleanup_queued_flow_messages(
        self,
        chat_id: int,
        user_id: Optional[int],
        skip_message_id: Optional[int] = None,
    ) -> None:
        if self.bot is None or user_id is None:
            return
        queued = list(self.flow_cleanup_message_ids.pop(int(user_id), []))
        skip_id = None if skip_message_id is None else int(skip_message_id)
        for message_id in queued:
            if skip_id is not None and int(message_id) == skip_id:
                continue
            try:
                await self.bot.delete_message(chat_id=int(chat_id), message_id=int(message_id))
            except Exception:
                pass

    async def _reply_private_and_queue_cleanup(self, message: Message, user_id: Optional[int], text: str, **kwargs) -> Optional[Message]:
        sent = await message.reply_text(text, **kwargs)
        try:
            self._queue_flow_cleanup_message(user_id, self._message_id(sent))
        except Exception:
            pass
        return sent

    async def _cleanup_processed_private_user_message(self, message: Message) -> None:
        if self.bot is None:
            return
        chat = getattr(message, "chat", None)
        from_user = getattr(message, "from_user", None)
        if chat is None or from_user is None:
            return
        if self._chat_type_name(getattr(chat, "type", None)) != "private":
            return
        if bool(getattr(from_user, "is_bot", False)):
            return
        try:
            await self.bot.delete_message(chat_id=int(chat.id), message_id=self._message_id(message))
        except Exception:
            pass

    async def _send_home_panel_message(self, chat_id: int, user_id: Optional[int]) -> None:
        if self.bot is None:
            return
        panel_text, panel_markup = await self._home_panel_payload(user_id=user_id)
        try:
            await self.bot.send_message(
                int(chat_id),
                panel_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=panel_markup,
                disable_notification=True,
            )
        except Exception as exc:
            logger.warning("Failed to send home panel message | chat_id=%s | error=%s", chat_id, exc)

    async def _send_flow_acknowledge(self, chat_id: int, user_id: Optional[int], flow_key: str, **context: Any) -> None:
        await self._send_acknowledge_notification(chat_id, user_id, self._flow_text(flow_key, **context))

    async def _send_acknowledge_notification(self, chat_id: int, user_id: Optional[int], text: str) -> None:
        if self.bot is None:
            return
        body = str(text or "").strip()
        hint = "Tap acknowledge to clean flow messages and return to the main menu."
        if hint.lower() not in body.lower():
            body = f"{body}\n\n{hint}" if body else hint
        try:
            sent = await self.bot.send_message(
                int(chat_id),
                body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                disable_notification=True,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("✅ Acknowledge", callback_data="x:ack:home")]]
                ),
            )
            try:
                self._queue_flow_cleanup_message(user_id, self._message_id(sent))
            except Exception:
                pass
        except Exception as exc:
            logger.warning("Failed to send acknowledge notification | chat_id=%s | error=%s", chat_id, exc)