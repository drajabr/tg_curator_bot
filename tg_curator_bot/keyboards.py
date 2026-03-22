from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def dm_admin_menu(session_ready: bool, group_count: int, source_count: int):
    status = "User Session: ✅ Ready" if session_ready else "User Session: ❌ Not Set"
    buttons = [
        [InlineKeyboardButton(status, callback_data="noop")],
        [InlineKeyboardButton("Status", callback_data="dm:status")],
        [InlineKeyboardButton("Set Up User Session", callback_data="dm:setup_session")],
        [InlineKeyboardButton(f"Active Groups: {group_count} | Sources: {source_count}", callback_data="noop")],
    ]
    return InlineKeyboardMarkup(buttons)


def group_main_menu(group_id: int):
    gid = str(group_id)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Add Source", callback_data=f"g:{gid}:add")],
            [InlineKeyboardButton("Remove Source", callback_data=f"g:{gid}:remove")],
            [InlineKeyboardButton("Source List", callback_data=f"g:{gid}:list")],
            [InlineKeyboardButton("Filters", callback_data=f"g:{gid}:filters")],
            [InlineKeyboardButton("Settings", callback_data=f"g:{gid}:settings")],
        ]
    )


def filters_root(group_id: int):
    gid = str(group_id)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Group-Wide Filters", callback_data=f"g:{gid}:gf")],
            [InlineKeyboardButton("Per-Source Filters", callback_data=f"g:{gid}:sf")],
            [InlineKeyboardButton("Back", callback_data=f"g:{gid}:back_main")],
        ]
    )


def rules_menu(group_id: int, scope: str, source_key: str | None = None):
    gid = str(group_id)
    src = f":{source_key}" if source_key else ""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Add Rule", callback_data=f"g:{gid}:{scope}:add{src}")],
            [InlineKeyboardButton("Remove Rule", callback_data=f"g:{gid}:{scope}:rm{src}")],
            [InlineKeyboardButton("List Rules", callback_data=f"g:{gid}:{scope}:ls{src}")],
            [InlineKeyboardButton("Switch Mode", callback_data=f"g:{gid}:{scope}:mode{src}")],
        ]
    )


def add_rule_types(group_id: int, scope: str, source_key: str | None = None):
    gid = str(group_id)
    src = f":{source_key}" if source_key else ""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Keyword", callback_data=f"g:{gid}:{scope}:type:keyword{src}")],
            [InlineKeyboardButton("Exact Message", callback_data=f"g:{gid}:{scope}:type:exact{src}")],
            [InlineKeyboardButton("Message Type", callback_data=f"g:{gid}:{scope}:type:message_type{src}")],
            [InlineKeyboardButton("Sender", callback_data=f"g:{gid}:{scope}:type:sender{src}")],
            [InlineKeyboardButton("Has Link", callback_data=f"g:{gid}:{scope}:type:has_link{src}")],
        ]
    )


def yes_no_buttons(yes_data: str, no_data: str):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Yes", callback_data=yes_data), InlineKeyboardButton("No", callback_data=no_data)]]
    )


def message_filter_quick_actions(group_id: int, destination_message_id: int):
    gid = str(group_id)
    mid = str(destination_message_id)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Block Exact Text", callback_data=f"q:{gid}:{mid}:exact")],
            [InlineKeyboardButton("Block Sender", callback_data=f"q:{gid}:{mid}:sender")],
            [InlineKeyboardButton("Extract Keywords", callback_data=f"q:{gid}:{mid}:keywords")],
            [InlineKeyboardButton("Cancel", callback_data="noop")],
        ]
    )
