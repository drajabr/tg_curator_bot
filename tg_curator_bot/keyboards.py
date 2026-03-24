from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def dm_admin_menu(session_ready: bool, group_count: int, source_count: int):
    buttons = [
        [InlineKeyboardButton("🔄 Refresh", callback_data="dm:home")],
        [InlineKeyboardButton(f"🎯 Destinations ({group_count})", callback_data="dm:groups")],
    ]
    return InlineKeyboardMarkup(buttons)


def dm_destinations_menu(destinations):
    buttons = [
        [InlineKeyboardButton(f"🎯 {label}", callback_data=f"dm:group:{group_id}")]
        for group_id, label in destinations
    ]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data="dm:home")])
    return InlineKeyboardMarkup(buttons)


def group_main_menu(group_id: int):
    gid = str(group_id)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📡 Sources", callback_data=f"g:{gid}:sources")],
            [InlineKeyboardButton("📥 Backfill", callback_data=f"g:{gid}:backfill")],
            [InlineKeyboardButton("🧹 Clean History", callback_data=f"g:{gid}:history")],
            [InlineKeyboardButton("🧰 Filters", callback_data=f"g:{gid}:filters")],
            [InlineKeyboardButton("⚙️ Settings", callback_data=f"g:{gid}:settings")],
            [InlineKeyboardButton("↩️ Back", callback_data="dm:groups")],
        ]
    )


def source_actions_menu(group_id: int, has_sources: bool):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton("➕ Add Source", callback_data=f"g:{gid}:add")]]
    if has_sources:
        buttons.append([InlineKeyboardButton("🗑️ Remove Source", callback_data=f"g:{gid}:remove")])
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")])
    return InlineKeyboardMarkup(buttons)


def source_backfill_menu(group_id: int, source_key: str):
    gid = str(group_id)
    skey = str(source_key)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🧪 Last Message", callback_data=f"x:bf:last:{gid}:{skey}")],
            [InlineKeyboardButton("📅 Today", callback_data=f"x:bf:today:{gid}:{skey}")],
            [InlineKeyboardButton("🗓️ Custom (1-30 days)", callback_data=f"x:bf:custom:{gid}:{skey}")],
            [InlineKeyboardButton("⏭️ Skip", callback_data=f"x:bf:skip:{gid}:{skey}")],
        ]
    )


def filters_root(group_id: int, has_sources: bool):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton("🧱 Group Filters", callback_data=f"g:{gid}:gf")]]
    if has_sources:
        buttons.append([InlineKeyboardButton("📡 Source Filters", callback_data=f"g:{gid}:sf")])
    buttons.append([InlineKeyboardButton("🔁 Reapply to Forwarded", callback_data=f"g:{gid}:reapply")])
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")])
    return InlineKeyboardMarkup(buttons)


def rules_menu(group_id: int, scope: str, source_key: str | None = None):
    gid = str(group_id)
    src = f":{source_key}" if source_key else ""
    back_callback = f"g:{gid}:filters" if not source_key else f"g:{gid}:sf"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Add Rule", callback_data=f"g:{gid}:{scope}:add{src}")],
            [InlineKeyboardButton("🗑️ Remove Rule", callback_data=f"g:{gid}:{scope}:rm{src}")],
            [InlineKeyboardButton("↩️ Back", callback_data=back_callback)],
        ]
    )


def add_rule_types(group_id: int, scope: str, source_key: str | None = None):
    gid = str(group_id)
    src = f":{source_key}" if source_key else ""
    back_callback = f"g:{gid}:gf" if scope == "gf" else f"g:{gid}:sfsel:{source_key}"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔎 Keyword", callback_data=f"g:{gid}:{scope}:type:keyword{src}")],
            [InlineKeyboardButton("🧾 Exact Message", callback_data=f"g:{gid}:{scope}:type:exact{src}")],
            [InlineKeyboardButton("🧩 Message Type", callback_data=f"g:{gid}:{scope}:type:message_type{src}")],
            [InlineKeyboardButton("👤 Sender", callback_data=f"g:{gid}:{scope}:type:sender{src}")],
            [InlineKeyboardButton("🔗 Has Link", callback_data=f"g:{gid}:{scope}:type:has_link{src}")],
            [InlineKeyboardButton("↩️ Back", callback_data=back_callback)],
        ]
    )


def rule_mode_selector(group_id: int, scope: str, source_key: str | None = None):
    """Select whether a rule should block or allow messages."""
    gid = str(group_id)
    src = f":{source_key}" if source_key else ""
    back_callback = f"g:{gid}:{scope}:add{src}"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚫 Block", callback_data=f"g:{gid}:{scope}:mode:blocklist{src}")],
            [InlineKeyboardButton("✅ Allow", callback_data=f"g:{gid}:{scope}:mode:allowlist{src}")],
            [InlineKeyboardButton("↩️ Back", callback_data=back_callback)],
        ]
    )


def group_settings_menu(group_id: int, show_header: bool, show_link: bool):
    gid = str(group_id)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"🧷 Header: {'ON' if show_header else 'OFF'}", callback_data=f"g:{gid}:toggleset:show_header")],
            [InlineKeyboardButton(f"🔗 Original Link: {'ON' if show_link else 'OFF'}", callback_data=f"g:{gid}:toggleset:show_link")],
            [InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")],
        ]
    )


def source_remove_menu(group_id: int, sources):
    gid = str(group_id)
    buttons = [
        [InlineKeyboardButton(f"🗑️ Remove {label}", callback_data=f"g:{gid}:rm:{key}")]
        for key, label in sources
    ]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:sources")])
    return InlineKeyboardMarkup(buttons)


def source_filter_selector_menu(group_id: int, sources):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton(f"📡 {label}", callback_data=f"g:{gid}:sfsel:{key}")] for key, label in sources]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:filters")])
    return InlineKeyboardMarkup(buttons)


def history_actions_menu(group_id: int, has_sources: bool):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton("🧹 Clean All Sources", callback_data=f"g:{gid}:history:all")]]
    if has_sources:
        buttons.append([InlineKeyboardButton("📡 Clean Single Source", callback_data=f"g:{gid}:history:source")])
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")])
    return InlineKeyboardMarkup(buttons)


def history_source_selector_menu(group_id: int, sources):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton(f"📡 {label}", callback_data=f"g:{gid}:historysrc:{key}")] for key, label in sources]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:history")])
    return InlineKeyboardMarkup(buttons)


def backfill_actions_menu(group_id: int, has_sources: bool):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton("📥 Backfill All Sources", callback_data=f"g:{gid}:backfill:all")]]
    if has_sources:
        buttons.append([InlineKeyboardButton("📡 Backfill Single Source", callback_data=f"g:{gid}:backfill:source")])
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")])
    return InlineKeyboardMarkup(buttons)


def backfill_source_selector_menu(group_id: int, sources):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton(f"📡 {label}", callback_data=f"g:{gid}:backfillsrc:{key}")] for key, label in sources]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:backfill")])
    return InlineKeyboardMarkup(buttons)


def yes_no_buttons(yes_data: str, no_data: str):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Yes", callback_data=yes_data), InlineKeyboardButton("❌ No", callback_data=no_data)]]
    )
