from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def dm_admin_menu(session_ready: bool, group_count: int, source_count: int, show_admin_menu: bool = False):
    buttons = [
        [InlineKeyboardButton("🔄 Refresh", callback_data="dm:home")],
        [InlineKeyboardButton(f"🎯 Destinations ({group_count})", callback_data="dm:groups")],
        [InlineKeyboardButton("📌 Live Events", callback_data="dm:events")],
    ]
    if show_admin_menu:
        buttons.append([InlineKeyboardButton("🛡️ Administration", callback_data="dm:admin")])
    return InlineKeyboardMarkup(buttons)


def dm_live_events_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Back", callback_data="dm:home")]])


def dm_administration_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Authorization", callback_data="dm:admin:authorize")],
            [InlineKeyboardButton("🗑️ Delete Destination", callback_data="dm:admin:destinations:delete")],
            [InlineKeyboardButton("📤 Export data.json", callback_data="dm:admin:data:export")],
            [InlineKeyboardButton("📥 Import data.json", callback_data="dm:admin:data:import")],
            [InlineKeyboardButton("↩️ Back", callback_data="dm:home")],
        ]
    )


def dm_authorization_prompt_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Add Authorized Admin", callback_data="dm:admin:authorize:add")],
            [InlineKeyboardButton("🗑️ Remove Authorized Admin", callback_data="dm:admin:authorize:remove")],
            [InlineKeyboardButton("↩️ Back", callback_data="dm:admin")],
        ]
    )


def dm_authorization_remove_menu(admin_choices):
    buttons = [
        [InlineKeyboardButton(f"🗑️ {label}", callback_data=f"dm:admin:authorize:rm:{admin_id}")]
        for admin_id, label in admin_choices
    ]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data="dm:admin:authorize")])
    return InlineKeyboardMarkup(buttons)


def dm_destination_delete_menu(destinations):
    buttons = [
        [InlineKeyboardButton(f"🗑️ {label}", callback_data=f"dm:admin:destinations:rm:{group_id}")]
        for group_id, label in destinations
    ]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data="dm:admin")])
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
            [InlineKeyboardButton("🧹 Clean History", callback_data=f"g:{gid}:history")],
            [InlineKeyboardButton("🧰 Filters", callback_data=f"g:{gid}:filters")],
            [InlineKeyboardButton("⚙️ Settings", callback_data=f"g:{gid}:settings")],
            [InlineKeyboardButton("↩️ Back", callback_data="dm:groups")],
        ]
    )


def source_actions_menu(group_id: int, has_sources: bool, page: int = 0, page_count: int = 1):
    gid = str(group_id)
    buttons = [
        [InlineKeyboardButton("➕ Add Source", callback_data=f"g:{gid}:add")],
        [InlineKeyboardButton("📚 Bulk Add Sources", callback_data=f"g:{gid}:bulkadd")],
    ]
    if has_sources:
        buttons.append([InlineKeyboardButton("🗑️ Remove Source", callback_data=f"g:{gid}:remove")])
    if has_sources and page_count > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"g:{gid}:sources:{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{page_count}", callback_data="noop"))
        if page < page_count - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"g:{gid}:sources:{page+1}"))
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")])
    return InlineKeyboardMarkup(buttons)


def bulk_source_import_menu(
    group_id: int,
    categories,
    auto_sync_enabled: bool,
    selected_count: int,
):
    """Category-level bulk import menu.

    categories: list of {"key": str, "label": str} produced by App._bulk_import_categories().
    """
    gid = str(group_id)
    buttons = []

    for cat in categories:
        buttons.append([InlineKeyboardButton(cat["label"], callback_data=f"g:{gid}:bulkadd:cat:{cat['key']}")])

    buttons.append(
        [
            InlineKeyboardButton(
                f"🔄 Auto-Sync New Chats: {'ON' if auto_sync_enabled else 'OFF'}",
                callback_data=f"g:{gid}:bulkadd:autosync",
            )
        ]
    )
    buttons.append([InlineKeyboardButton("🔁 Refresh", callback_data=f"g:{gid}:bulkadd:refresh")])
    buttons.append([InlineKeyboardButton(f"✅ Import Selected ({selected_count})", callback_data=f"g:{gid}:bulkadd:run")])
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:sources")])
    return InlineKeyboardMarkup(buttons)





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


def group_settings_menu(
    group_id: int,
    show_header: bool,
    show_link: bool,
    show_source_datetime: bool,
    auto_leave_after_source_delete: bool,
    global_spam_dedupe_enabled: bool,
    has_sources: bool,
):
    gid = str(group_id)
    buttons = [
        [InlineKeyboardButton(f"🧷 Header: {'ON' if show_header else 'OFF'}", callback_data=f"g:{gid}:toggleset:show_header")],
        [InlineKeyboardButton(f"🕒 Original Date/Time: {'ON' if show_source_datetime else 'OFF'}", callback_data=f"g:{gid}:toggleset:show_source_datetime")],
        [InlineKeyboardButton(f"🔗 Original Link: {'ON' if show_link else 'OFF'}", callback_data=f"g:{gid}:toggleset:show_link")],
        [InlineKeyboardButton(f"🚪 Auto Leave After Source Delete: {'ON' if auto_leave_after_source_delete else 'OFF'}", callback_data=f"g:{gid}:toggleset:auto_leave_after_source_delete")],
        [InlineKeyboardButton(f"🛡️ Global Spam Dedupe (10s): {'ON' if global_spam_dedupe_enabled else 'OFF'}", callback_data=f"g:{gid}:toggleset:global_spam_dedupe_enabled")],
    ]
    if has_sources:
        buttons.append([InlineKeyboardButton("🧪 Test Sources", callback_data=f"g:{gid}:testsources")])
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"dm:group:{gid}")])
    return InlineKeyboardMarkup(buttons)



def source_remove_menu(group_id: int, sources, page: int = 0, page_size: int = 8):
    gid = str(group_id)
    total = len(sources)
    page_count = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, page_count - 1))
    start = page * page_size
    end = start + page_size
    page_sources = sources[start:end]
    buttons = [
        [InlineKeyboardButton(f"🗑️ #{start + index} {label}", callback_data=f"g:{gid}:rm:{key}")]
        for index, (key, label) in enumerate(page_sources, start=1)
    ]
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"g:{gid}:remove:{page-1}"))
    nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{page_count}", callback_data=f"g:{gid}:remove:{page}"))
    if page < page_count - 1:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"g:{gid}:remove:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:sources")])
    return InlineKeyboardMarkup(buttons)


def source_filter_selector_menu(group_id: int, sources):
    gid = str(group_id)
    buttons = [[InlineKeyboardButton(f"📡 {label}", callback_data=f"g:{gid}:sfsel:{key}")] for key, label in sources]
    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:filters")])
    return InlineKeyboardMarkup(buttons)


def source_filter_selector_menu_paginated(group_id: int, sources, page: int = 0, page_size: int = 8):
    gid = str(group_id)
    total = len(sources)
    page_count = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, page_count - 1))
    start = page * page_size
    end = start + page_size
    page_sources = sources[start:end]

    buttons = [
        [InlineKeyboardButton(f"📡 #{start + index} {label}", callback_data=f"g:{gid}:sfsel:{key}")]
        for index, (key, label) in enumerate(page_sources, start=1)
    ]

    if total > 0:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"g:{gid}:sfpage:{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{page_count}", callback_data="noop"))
        if page < page_count - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"g:{gid}:sfpage:{page+1}"))
        buttons.append(nav_buttons)

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


def history_source_selector_menu_paginated(group_id: int, sources, page: int = 0, page_size: int = 8):
    gid = str(group_id)
    total = len(sources)
    page_count = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, page_count - 1))
    start = page * page_size
    end = start + page_size
    page_sources = sources[start:end]

    buttons = [
        [InlineKeyboardButton(f"📡 #{start + index} {label}", callback_data=f"g:{gid}:historysrc:{key}")]
        for index, (key, label) in enumerate(page_sources, start=1)
    ]

    if total > 0:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"g:{gid}:historypage:{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{page_count}", callback_data="noop"))
        if page < page_count - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"g:{gid}:historypage:{page+1}"))
        buttons.append(nav_buttons)

    buttons.append([InlineKeyboardButton("↩️ Back", callback_data=f"g:{gid}:history")])
    return InlineKeyboardMarkup(buttons)



def yes_no_buttons(yes_data: str, no_data: str):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Yes", callback_data=yes_data), InlineKeyboardButton("❌ No", callback_data=no_data)]]
    )
