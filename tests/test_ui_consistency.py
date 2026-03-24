import unittest
import tempfile
from pathlib import Path

from tg_curator_bot.app import TelegramFeedBot
from tg_curator_bot.keyboards import backfill_actions_menu, backfill_source_selector_menu, dm_destinations_menu, group_main_menu
from tg_curator_bot.storage import ForwardLogStorage


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


if __name__ == "__main__":
    unittest.main()
