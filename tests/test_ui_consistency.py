import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tg_curator_bot.app import TelegramFeedBot
from tg_curator_bot.keyboards import (
    bulk_source_import_menu,
    dm_authorization_prompt_menu,
    dm_authorization_remove_menu,
    dm_administration_menu,
    dm_admin_menu,
    dm_destination_delete_menu,
    dm_destinations_menu,
    dm_live_events_menu,
    group_main_menu,
    group_settings_menu,
    source_actions_menu,
    source_filter_selector_menu_paginated,
    source_remove_menu,
    history_source_selector_menu_paginated,
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

    def test_home_menu_includes_live_events_button(self) -> None:
        menu = dm_admin_menu(True, 3, 9, show_admin_menu=True)
        labels = [row[0].text for row in menu.inline_keyboard]
        self.assertIn("📌 Live Events", labels)

    def test_live_events_menu_shows_back_only(self) -> None:
        menu = dm_live_events_menu()
        labels = [button.text for row in menu.inline_keyboard for button in row]
        self.assertEqual(labels, ["↩️ Back"])

    def test_administration_menu_includes_delete_destination(self) -> None:
        admin_menu = dm_administration_menu()
        labels = [row[0].text for row in admin_menu.inline_keyboard]
        self.assertIn("🗑️ Delete Destination", labels)

    def test_destination_delete_menu_uses_admin_remove_callback(self) -> None:
        menu = dm_destination_delete_menu([(123, "Demo (2)")])
        self.assertEqual(menu.inline_keyboard[0][0].callback_data, "dm:admin:destinations:rm:123")

    def test_source_actions_menu_includes_bulk_add_button(self) -> None:
        menu = source_actions_menu(123, False)
        labels = [row[0].text for row in menu.inline_keyboard]
        self.assertIn("📚 Bulk Add Sources", labels)

    def test_source_actions_menu_shows_navigation_when_multiple_pages(self) -> None:
        menu = source_actions_menu(123, True, page=1, page_count=3)
        labels = [button.text for row in menu.inline_keyboard for button in row]
        self.assertIn("⬅️ Prev", labels)
        self.assertIn("📄 2/3", labels)
        self.assertIn("Next ➡️", labels)

    def test_source_remove_menu_numbers_rows(self) -> None:
        menu = source_remove_menu(123, [("a", "Alpha"), ("b", "Beta")], page=0, page_size=8)
        first_row_label = menu.inline_keyboard[0][0].text
        second_row_label = menu.inline_keyboard[1][0].text
        self.assertTrue(first_row_label.startswith("🗑️ #1 "))
        self.assertTrue(second_row_label.startswith("🗑️ #2 "))

    def test_source_filter_selector_menu_paginated_has_navigation(self) -> None:
        sources = [(str(i), f"Source {i}") for i in range(1, 13)]
        menu = source_filter_selector_menu_paginated(123, sources, page=1, page_size=5)
        labels = [button.text for row in menu.inline_keyboard for button in row]
        self.assertIn("⬅️ Prev", labels)
        self.assertIn("📄 2/3", labels)
        self.assertIn("Next ➡️", labels)
        self.assertTrue(any(label.startswith("📡 #6 ") for label in labels))

    def test_history_source_selector_menu_paginated_has_navigation(self) -> None:
        sources = [(str(i), f"Source {i}") for i in range(1, 11)]
        menu = history_source_selector_menu_paginated(321, sources, page=1, page_size=4)
        labels = [button.text for row in menu.inline_keyboard for button in row]
        self.assertIn("⬅️ Prev", labels)
        self.assertIn("📄 2/3", labels)
        self.assertIn("Next ➡️", labels)
        self.assertTrue(any(label.startswith("📡 #5 ") for label in labels))

    def test_bulk_source_import_menu_shows_selection_controls(self) -> None:
        categories = [
            {"key": "groups", "label": "⬜ 👥 Groups (3)"},
            {"key": "channels", "label": "✅ 📢 Channels (1)"},
            {"key": "folder_7", "label": "☑️ 📂 Work (2)"},
        ]
        menu = bulk_source_import_menu(123, categories, True, 3)
        labels = [button.text for row in menu.inline_keyboard for button in row]
        self.assertIn("⬜ 👥 Groups (3)", labels)
        self.assertIn("✅ 📢 Channels (1)", labels)
        self.assertIn("☑️ 📂 Work (2)", labels)
        self.assertIn("🔄 Auto-Sync New Chats: ON", labels)
        self.assertIn("✅ Import Selected (3)", labels)
        self.assertIn("🔁 Refresh", labels)

    def test_group_settings_menu_shows_source_test_only_when_sources_exist(self) -> None:
        with_sources = group_settings_menu(123, True, True, False, True, True)
        with_sources_labels = [row[0].text for row in with_sources.inline_keyboard]
        self.assertIn("🧪 Test Sources", with_sources_labels)

        without_sources = group_settings_menu(123, True, True, False, True, False)
        without_sources_labels = [row[0].text for row in without_sources.inline_keyboard]
        self.assertNotIn("🧪 Test Sources", without_sources_labels)

    def test_chat_type_name_normalizes_pyrogram_enum_style_values(self) -> None:
        self.assertEqual(self.bot._chat_type_name("ChatType.CHANNEL"), "channel")
        self.assertEqual(self.bot._chat_type_name(SimpleNamespace(type="ChatType.SUPERGROUP")), "supergroup")

    def test_on_user_message_marks_matched_source_as_read_even_if_not_forwarded(self) -> None:
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
                            "999": {
                                "meta": {"title": "Dest", "username": None},
                                "settings": {"show_header": True, "show_link": True, "show_source_datetime": False},
                                "group_filters": {"rules": []},
                                "source_import": {"filter_mode": "all", "auto_sync_enabled": False},
                                "sources": {
                                    "-100123|0": {
                                        "chat_id": -100123,
                                        "topic_id": None,
                                        "name": "Source",
                                        "username": None,
                                        "filters": {"rules": [{"type": "exact", "value": "never matches"}]},
                                    }
                                },
                            }
                        },
                        "owner_dm_message_ids": [],
                    }
                )

                client = SimpleNamespace(read_chat_history=AsyncMock())
                self.bot._forward_message_to_group = AsyncMock(return_value=None)
                message = SimpleNamespace(
                    id=42,
                    chat=SimpleNamespace(id=-100123),
                    text="hello world",
                    caption=None,
                    media_group_id=None,
                    message_thread_id=None,
                )

                await self.bot.on_user_message(client, message)

                client.read_chat_history.assert_awaited_once_with(-100123, max_id=42)
                self.bot._forward_message_to_group.assert_awaited_once()

            import asyncio

            asyncio.run(scenario())

    def test_on_user_message_drops_exact_duplicate_within_window(self) -> None:
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
                        "admin_settings": {"global_spam_dedupe_enabled": True, "global_spam_dedupe_window_seconds": 60},
                        "groups": {
                            "999": {
                                "meta": {"title": "Dest", "username": None},
                                "settings": {"show_header": True, "show_link": True, "show_source_datetime": False},
                                "group_filters": {"rules": []},
                                "source_import": {"filter_mode": "all", "auto_sync_enabled": False},
                                "sources": {
                                    "-100123|0": {
                                        "chat_id": -100123,
                                        "topic_id": None,
                                        "name": "Source",
                                        "username": None,
                                        "filters": {"rules": []},
                                    }
                                },
                            }
                        },
                        "owner_dm_message_ids": [],
                    }
                )

                client = SimpleNamespace(read_chat_history=AsyncMock())
                self.bot._forward_message_to_group = AsyncMock(return_value=777)

                message = SimpleNamespace(
                    id=50,
                    chat=SimpleNamespace(id=-100123),
                    text="same text",
                    caption=None,
                    media_group_id=None,
                    message_thread_id=None,
                    photo=None,
                    video=None,
                    document=None,
                    audio=None,
                    voice=None,
                    video_note=None,
                    animation=None,
                    sticker=None,
                    poll=None,
                )

                await self.bot.on_user_message(client, message)
                await self.bot.on_user_message(client, message)

                self.assertEqual(self.bot._forward_message_to_group.await_count, 1)

            import asyncio

            asyncio.run(scenario())

    def test_source_message_type_detects_photo_without_caption(self) -> None:
        message = SimpleNamespace(
            text=None,
            caption=None,
            photo=True,
            video=None,
            document=None,
            audio=None,
            voice=None,
            video_note=None,
            animation=None,
            sticker=None,
            poll=None,
        )

        self.assertEqual(self.bot._source_message_type(message), "photo")

    def test_catch_up_source_marks_scanned_messages_as_read_even_if_not_forwarded(self) -> None:
        async def scenario() -> None:
            async def history_iter():
                for message_id in (105, 104, 103, 100):
                    yield SimpleNamespace(id=message_id, message_thread_id=None)

            self.bot.user_client = SimpleNamespace(
                get_chat_history=lambda chat_id, limit: history_iter(),
                read_chat_history=AsyncMock(),
            )
            self.bot._forward_message_to_group = AsyncMock(return_value=None)

            forwarded = await self.bot._catch_up_source(999, "-100123|0", -100123, None, 100)

            self.assertEqual(forwarded, 0)
            self.bot.user_client.read_chat_history.assert_awaited_once_with(-100123, max_id=105)
            self.assertEqual(self.bot._forward_message_to_group.await_count, 3)

        import asyncio

        asyncio.run(scenario())

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

    def test_live_events_screen_uses_saved_lines(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            data_path = Path(temp_dir) / "data.json"
            logs_path = Path(temp_dir) / "forward_logs.json"
            self.bot.storage = Storage(str(data_path))
            self.bot.forward_log_storage = ForwardLogStorage(str(logs_path))

            async def scenario() -> None:
                await self.bot.storage.write(
                    {
                        "owner_id": None,
                        "authorized_admin_ids": [],
                        "authorized_admin_meta": {},
                        "bot_token": None,
                        "user_session": {"api_id": None, "api_hash": None, "session_string": None},
                        "admin_settings": {
                            "global_spam_dedupe_enabled": True,
                            "global_spam_dedupe_window_seconds": 10,
                            "live_events_lines": [
                                "# 10:00:00: Source A>News Dest",
                                "# 10:01:00: Source B>News Dest",
                            ],
                        },
                        "groups": {},
                        "owner_dm_message_ids": [],
                    }
                )

                text = await self.bot._live_events_screen_text()

                self.assertIn("Live Events", text)
                self.assertIn("# 10:00:00: Source A>News Dest", text)
                self.assertIn("# 10:01:00: Source B>News Dest", text)

            import asyncio

            asyncio.run(scenario())

    def test_trim_live_event_lines_drops_oldest_when_limit_hit(self) -> None:
        lines = [
            "# 10:00:00: Source A>Destination A",
            "# 10:00:01: Source B>Destination B",
            "# 10:00:02: Source C>Destination C",
        ]

        trimmed = self.bot._trim_live_event_lines(lines, limit=80)

        self.assertLessEqual(len(self.bot._live_event_text(trimmed)), 80)
        self.assertNotIn("# 10:00:00: Source A>Destination A", trimmed)
        self.assertIn("# 10:00:02: Source C>Destination C", trimmed)


if __name__ == "__main__":
    unittest.main()
