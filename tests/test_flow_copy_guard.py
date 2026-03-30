import unittest

from tg_curator_bot.flow_copy_guard import collect_inline_message_literals


APPROVED_INLINE_MESSAGE_LITERALS = {
    "<b>Choose Filter Rule Type</b>",
    "<b>Select a Rule to Remove</b>",
    "<b>➕ Add Authorized Admin</b>\n\nSend a numeric Telegram user ID or @username in this DM.\nSend <code>cancel</code> to stop.",
    "<b>📥 Import data.json</b>\n\nSend the exported <code>data.json</code> file as a document in this DM.\nSend <code>cancel</code> to stop.",
    "A source test is already running for this destination.",
    "Admin was not in authorized list.",
    "Administration import is owner-only.",
    "Authorized admin removed.",
    "Bot API is not ready.",
    "Choose link rule value:",
    "Could not resolve admin identity.",
    "Destination not found.",
    "Export sent.",
    "Invalid action.",
    "Invalid admin identifier.",
    "Invalid data.json format: root value must be a JSON object.",
    "Invalid destination identifier.",
    "Invalid link-rule value.",
    "Invalid rule mode.",
    "Invalid rule selection.",
    "Invalid setting.",
    "Invalid source.",
    "No authorized admins found.",
    "No destinations to delete.",
    "No rules are configured yet.",
    "No sender value was provided. Forward a message or send sender handles, usernames, or IDs.",
    "No sources are configured yet.",
    "No sources are selected.",
    "No targets found.",
    "No valid keywords were provided.",
    "Only the owner or authorized admins can use this bot.",
    "Please send the exported data.json as a document.",
    "Reapplying filters",
    "Rule added.",
    "Rule type is missing.",
    "Rule type was not found.",
    "Running backfill...",
    "Running source tests...",
    "Send a valid message type.",
    "Send a value to continue.",
    "Should this rule <b>block</b> or <b>allow</b> messages?",
    "That rule type is not supported.",
    "There is no text available for this rule.",
    "This destination is registered. Open the bot DM and use /start to manage sources, filters, and settings there.",
    "Unknown history option.",
    "User session is not ready.",
    "User session is not ready. Use sender handles, usernames, or numeric sender IDs, or reconfigure the session and restart.",
    "data.json was not found.",
}


class FlowCopyGuardTests(unittest.TestCase):
    def test_no_new_inline_message_literals(self) -> None:
        found = set(collect_inline_message_literals("tg_curator_bot/app.py"))
        new_literals = sorted(found - APPROVED_INLINE_MESSAGE_LITERALS)
        self.assertEqual(
            new_literals,
            [],
            msg=(
                "New inline user-facing literals were found in app.py. "
                "Move them to tg_curator_bot/flows.py and render via _flow_text, "
                "or intentionally update APPROVED_INLINE_MESSAGE_LITERALS in this test.\n"
                + "\n".join(new_literals)
            ),
        )
