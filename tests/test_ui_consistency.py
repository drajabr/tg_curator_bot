import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tg_curator_bot.app import TelegramFeedBot
from tg_curator_bot.keyboards import (
    backfill_actions_menu,
    backfill_source_selector_menu,
    bulk_source_import_menu,
    dm_authorization_prompt_menu,
    dm_authorization_remove_menu,
    dm_administration_menu,
    dm_destination_delete_menu,
    dm_destinations_menu,
    group_main_menu,
    source_actions_menu,
)
from tg_curator_bot.storage import ForwardLogStorage, Storage


class UIConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bot = TelegramFeedBot()

    def test_source_display_name_prefers_name_over_handle(self) -> None:
        label = self.bot._source_display_name(
            "-1001|0",
            {"name": "Tech News", "username": "technews"},
        )
        self.assertEqual(label, "Tech News")

    def test_source_display_name_falls_back_to_handle(self) -> None:
        label = self.bot._source_display_name(
            "-1001|0",
            {"name": "", "username": "technews"},
        )
        self.assertEqual(label, "@technews")

    def test_sorted_sources_uses_label_then_key(self) -> None:
        group_state = {
            "sources": {
                "-1003|0": {"name": "Zoo", "username": None},
                "-1001|0": {"name": "Alpha", "username": None},
                "-1002|0": {"name": "Alpha", "username": None},
            }
        }
        ordered = self.bot._sorted_sources(group_state)
        self.assertEqual([item[0] for item in ordered], ["-1001|0", "-1002|0", "-1003|0"])

    def test_history_source_choices_ranked_by_count_then_name(self) -> None:
        group_state = {
            "sources": {
                "b": {"name": "Beta", "username": None},
                "a": {"name": "Alpha", "username": None},
                "c": {"name": "Gamma", "username": None},
            }
        }
        history = {
            "1": {"source_key": "b"},
            "2": {"source_key": "a"},
            "3": {"source_key": "b"},
            "4": {"source_key": "a"},
            "5": {"source_key": "c"},
        }
        ranked = self.bot._history_source_choices(group_state, history)
        self.assertEqual(ranked[0], ("a", "Alpha", 2))
        self.assertEqual(ranked[1], ("b", "Beta", 2))
        self.assertEqual(ranked[2], ("c", "Gamma", 1))

    def test_entry_source_key_falls_back_to_chat_and_topic_fields(self) -> None:
        entry = {"source_chat_id": -100123, "source_topic_id": 77}
        self.assertEqual(self.bot._entry_source_key(entry), "-100123|77")

    def test_entry_source_key_falls_back_to_legacy_chat_field(self) -> None:
        entry = {"chat_id": -100123}
        self.assertEqual(self.bot._entry_source_key(entry), "-100123|0")

    def test_entry_matches_source_with_fallback_fields(self) -> None:
        entry = {"source_chat_id": -100123, "source_topic_id": None}
        self.assertTrue(self.bot._entry_matches_source(entry, "-100123|0"))

    def test_cancel_text_aliases(self) -> None:
        self.assertTrue(self.bot._is_cancel_text("cancel"))
        self.assertTrue(self.bot._is_cancel_text("/cancel"))
        self.assertTrue(self.bot._is_cancel_text("Back"))
        self.assertFalse(self.bot._is_cancel_text("continue"))

    def test_standardized_back_labels(self) -> None:
        destinations = dm_destinations_menu([(123, "Demo")])
        self.assertEqual(destinations.inline_keyboard[-1][0].text, "↩️ Back")

        group_menu = group_main_menu(123)
        self.assertEqual(group_menu.inline_keyboard[-1][0].text, "↩️ Back")

    def test_administration_menu_includes_delete_destination(self) -> None:
        admin_menu = dm_administration_menu()
        labels = [row[0].text for row in admin_menu.inline_keyboard]
        self.assertIn("🗑️ Delete Destination", labels)

    def test_destination_delete_menu_uses_admin_remove_callback(self) -> None:
        menu = dm_destination_delete_menu([(123, "Demo (2)")])
        self.assertEqual(menu.inline_keyboard[0][0].callback_data, "dm:admin:destinations:rm:123")

    def test_group_main_menu_includes_backfill_button(self) -> None:
        group_menu = group_main_menu(123)
        labels = [row[0].text for row in group_menu.inline_keyboard]
        self.assertIn("📥 Backfill", labels)

    def test_backfill_actions_menu_single_source_visibility(self) -> None:
        with_sources = backfill_actions_menu(123, True)
        labels_with_sources = [row[0].text for row in with_sources.inline_keyboard]
        self.assertIn("📥 Backfill All Sources", labels_with_sources)
        self.assertIn("📡 Backfill Single Source", labels_with_sources)

        without_sources = backfill_actions_menu(123, False)
        labels_without_sources = [row[0].text for row in without_sources.inline_keyboard]
        self.assertIn("📥 Backfill All Sources", labels_without_sources)
        self.assertNotIn("📡 Backfill Single Source", labels_without_sources)

    def test_backfill_source_selector_menu_uses_backfillsrc_callback(self) -> None:
        menu = backfill_source_selector_menu(123, [("-100|0", "Alpha")])
        self.assertEqual(menu.inline_keyboard[0][0].callback_data, "g:123:backfillsrc:-100|0")

    def test_source_actions_menu_includes_bulk_add_button(self) -> None:
        menu = source_actions_menu(123, False)
        labels = [row[0].text for row in menu.inline_keyboard]
        self.assertIn("📚 Bulk Add Sources", labels)

    def test_bulk_source_import_menu_shows_selection_controls(self) -> None:
        menu = bulk_source_import_menu(123, [("-100|0", "Alpha")], {"-100|0"}, "channels", True, 1)
        labels = [button.text for row in menu.inline_keyboard for button in row]
        self.assertIn("☑️ Alpha", labels)
        self.assertIn("✅ Channels", labels)
        self.assertIn("🔄 Auto-Sync New Chats: ON", labels)
        self.assertIn("✅ Import Selected (1)", labels)

    def test_chat_type_name_normalizes_pyrogram_enum_style_values(self) -> None:
        self.assertEqual(self.bot._chat_type_name("ChatType.CHANNEL"), "channel")
        self.assertEqual(self.bot._chat_type_name(SimpleNamespace(type="ChatType.SUPERGROUP")), "supergroup")

    def test_authorization_remove_menu_callbacks(self) -> None:
        prompt_menu = dm_authorization_prompt_menu()
        prompt_labels = [row[0].text for row in prompt_menu.inline_keyboard]
        self.assertIn("🗑️ Remove Authorized Admin", prompt_labels)

        remove_menu = dm_authorization_remove_menu([(123456, "@helper")])
        self.assertEqual(remove_menu.inline_keyboard[0][0].callback_data, "dm:admin:authorize:rm:123456")

    def test_delete_forwarded_history_for_source_deletes_and_drops_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "forward_logs.json"
            self.bot.forward_log_storage = ForwardLogStorage(str(path))

            async def scenario() -> None:
                await self.bot.forward_log_storage.write(
                    {
                        "100": {
                            "10": {"source_key": "src-a"},
                            "11": {"source_key": "src-b"},
                            "12": {"source_key": "src-a"},
                        }
                    }
                )
                calls = []

                async def fake_delete(group_id: int, message_id: int) -> bool:
                    calls.append((group_id, message_id))
                    return True

                self.bot._safe_delete_destination_message = fake_delete
                result = await self.bot._delete_forwarded_history_for_source(100, "src-a")
                history = await self.bot._group_forward_history(100)

                self.assertEqual(calls, [(100, 10), (100, 12)])
                self.assertEqual(result["scanned"], 2)
                self.assertEqual(result["deleted"], 2)
                self.assertEqual(result["skipped"], 0)
                self.assertEqual(result["history_removed"], 2)
                self.assertEqual(set(history.keys()), {"11"})

            import asyncio

            asyncio.run(scenario())

    def test_delete_forwarded_history_for_source_keeps_failed_deletes_in_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "forward_logs.json"
            self.bot.forward_log_storage = ForwardLogStorage(str(path))

            async def scenario() -> None:
                await self.bot.forward_log_storage.write(
                    {
                        "200": {
                            "20": {"source_key": "src-a"},
                            "21": {"source_key": "src-a"},
                        }
                    }
                )

                async def fake_delete(group_id: int, message_id: int) -> bool:
                    return message_id == 20

                self.bot._safe_delete_destination_message = fake_delete
                result = await self.bot._delete_forwarded_history_for_source(200, "src-a")
                history = await self.bot._group_forward_history(200)

                self.assertEqual(result["scanned"], 2)
                self.assertEqual(result["deleted"], 1)
                self.assertEqual(result["skipped"], 1)
                self.assertEqual(result["history_removed"], 1)
                self.assertEqual(set(history.keys()), {"21"})

            import asyncio

            asyncio.run(scenario())

    def test_resolve_source_from_message_public_link_attempts_join(self) -> None:
        async def scenario() -> None:
            chat = SimpleNamespace(id=-100777, title="Source", username="sourcechat", type="supergroup")
            self.bot.user_client = SimpleNamespace(
                join_chat=AsyncMock(return_value=chat),
                get_chat=AsyncMock(return_value=chat),
            )
            message = SimpleNamespace(text="https://t.me/sourcechat/123", forward_from_chat=None, forward_origin=None)

            source, err = await self.bot._resolve_source_from_message(message)

            self.assertIsNone(err)
            self.assertIsNotNone(source)
            assert source is not None
            self.assertEqual(source["chat_id"], -100777)
            self.assertEqual(source["username"], "sourcechat")
            self.assertEqual(source.get("join_link"), "https://t.me/sourcechat/123")
            self.bot.user_client.join_chat.assert_awaited_once_with("sourcechat")

        import asyncio

        asyncio.run(scenario())

    def test_resolve_entity_from_text_for_intent_uses_link_join_fallback(self) -> None:
        async def scenario() -> None:
            chat = SimpleNamespace(id=-100888, title="Intent Source", username="intentsource", type="channel")
            self.bot.user_client = SimpleNamespace(
                join_chat=AsyncMock(return_value=chat),
                get_chat=AsyncMock(return_value=chat),
            )

            entity, err = await self.bot._resolve_entity_from_text_for_intent("t.me/intentsource/42")

            self.assertIsNone(err)
            self.assertEqual(entity.get("kind"), "chat")
            source = entity.get("source")
            self.assertEqual(source.get("chat_id"), -100888)
            self.assertEqual(source.get("join_link"), "https://t.me/intentsource/42")
            self.bot.user_client.join_chat.assert_awaited_once_with("intentsource")

        import asyncio

        asyncio.run(scenario())

    def test_bulk_source_candidates_excludes_existing_private_and_registered_destinations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "data.json"
            self.bot.storage = Storage(str(path))

            async def scenario() -> None:
                await self.bot.storage.write(
                    {
                        "owner_id": None,
                        "authorized_admin_ids": [],
                        "authorized_admin_meta": {},
                        "bot_token": None,
                        "user_session": {"api_id": None, "api_hash": None, "session_string": None},
                        "admin_settings": {"global_spam_dedupe_enabled": True, "global_spam_dedupe_window_seconds": 10},
                        "groups": {
                            "-10010": {
                                "meta": {"title": "Dest", "username": None},
                                "settings": {"show_header": True, "show_link": True, "show_source_datetime": False},
                                "group_filters": {"rules": []},
                                "sources": {
                                    "-10020|0": {
                                        "chat_id": -10020,
                                        "topic_id": None,
                                        "name": "Existing Source",
                                        "username": None,
                                        "type": "channel",
                                        "filters": {"rules": []},
                                    }
                                },
                            },
                            "-10030": {
                                "meta": {"title": "Other Dest", "username": None},
                                "settings": {"show_header": True, "show_link": True, "show_source_datetime": False},
                                "group_filters": {"rules": []},
                                "sources": {},
                            },
                        },
                        "owner_dm_message_ids": [],
                    }
                )

                async def dialog_iter():
                    chats = [
                        SimpleNamespace(chat=SimpleNamespace(id=-10020, title="Existing Source", username=None, type="ChatType.CHANNEL")),
                        SimpleNamespace(chat=SimpleNamespace(id=-10030, title="Other Dest", username=None, type="ChatType.SUPERGROUP")),
                        SimpleNamespace(chat=SimpleNamespace(id=-10040, title="Fresh Group", username="freshgroup", type="ChatType.SUPERGROUP")),
                        SimpleNamespace(chat=SimpleNamespace(id=55, title=None, username="person", type="ChatType.PRIVATE")),
                    ]
                    for item in chats:
                        yield item

                self.bot.user_client = SimpleNamespace(get_dialogs=dialog_iter)

                candidates = await self.bot._bulk_source_candidates(-10010)

                self.assertEqual(len(candidates), 1)
                self.assertEqual(candidates[0]["chat_id"], -10040)
                self.assertEqual(candidates[0]["username"], "freshgroup")

            import asyncio

            asyncio.run(scenario())

    def test_bulk_source_candidates_respect_channels_and_groups_filters(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "data.json"
            self.bot.storage = Storage(str(path))

            async def scenario() -> None:
                await self.bot.storage.write(
                    {
                        "owner_id": None,
                        "authorized_admin_ids": [],
                        "authorized_admin_meta": {},
                        "bot_token": None,
                        "user_session": {"api_id": None, "api_hash": None, "session_string": None},
                        "admin_settings": {"global_spam_dedupe_enabled": True, "global_spam_dedupe_window_seconds": 10},
                        "groups": {
                            "-10010": {
                                "meta": {"title": "Dest", "username": None},
                                "settings": {"show_header": True, "show_link": True, "show_source_datetime": False},
                                "group_filters": {"rules": []},
                                "sources": {},
                            }
                        },
                        "owner_dm_message_ids": [],
                    }
                )

                async def dialog_iter():
                    chats = [
                        SimpleNamespace(chat=SimpleNamespace(id=-10040, title="Fresh Group", username="freshgroup", type="ChatType.SUPERGROUP")),
                        SimpleNamespace(chat=SimpleNamespace(id=-10050, title="News", username="newsfeed", type="ChatType.CHANNEL")),
                    ]
                    for item in chats:
                        yield item

                self.bot.user_client = SimpleNamespace(get_dialogs=dialog_iter)

                all_candidates = await self.bot._bulk_source_candidates(-10010, filter_mode="all")
                group_candidates = await self.bot._bulk_source_candidates(-10010, filter_mode="groups")
                channel_candidates = await self.bot._bulk_source_candidates(-10010, filter_mode="channels")

                self.assertEqual({item["chat_id"] for item in all_candidates}, {-10040, -10050})
                self.assertEqual([item["chat_id"] for item in group_candidates], [-10040])
                self.assertEqual([item["chat_id"] for item in channel_candidates], [-10050])

            import asyncio

            asyncio.run(scenario())

    def test_autosync_group_sources_respects_saved_filter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "data.json"
            self.bot.storage = Storage(str(path))

            async def scenario() -> None:
                await self.bot.storage.write(
                    {
                        "owner_id": None,
                        "authorized_admin_ids": [],
                        "authorized_admin_meta": {},
                        "bot_token": None,
                        "user_session": {"api_id": None, "api_hash": None, "session_string": None},
                        "admin_settings": {"global_spam_dedupe_enabled": True, "global_spam_dedupe_window_seconds": 10},
                        "groups": {
                            "-10010": {
                                "meta": {"title": "Dest", "username": None},
                                "settings": {"show_header": True, "show_link": True, "show_source_datetime": False},
                                "group_filters": {"rules": []},
                                "source_import": {"filter_mode": "channels", "auto_sync_enabled": True},
                                "sources": {},
                            }
                        },
                        "owner_dm_message_ids": [],
                    }
                )

                async def dialog_iter():
                    chats = [
                        SimpleNamespace(chat=SimpleNamespace(id=-10040, title="Fresh Group", username="freshgroup", type="ChatType.SUPERGROUP")),
                        SimpleNamespace(chat=SimpleNamespace(id=-10050, title="News", username="newsfeed", type="ChatType.CHANNEL")),
                    ]
                    for item in chats:
                        yield item

                self.bot.user_client = SimpleNamespace(get_dialogs=dialog_iter)

                result = await self.bot._autosync_group_sources(-10010)
                state = await self.bot.storage.read()
                saved_sources = state["groups"]["-10010"]["sources"]

                self.assertEqual(result, {"eligible": 1, "added": 1})
                self.assertEqual(set(saved_sources.keys()), {"-10050|0"})

            import asyncio

            asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
