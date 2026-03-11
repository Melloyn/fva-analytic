import calendar

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

MONTHS_RU_SHORT = {
    1: "Янв", 2: "Фев", 3: "Мар", 4: "Апр",
    5: "Май", 6: "Июн", 7: "Июл", 8: "Авг",
    9: "Сен", 10: "Окт", 11: "Ноя", 12: "Дек",
}


def main_menu_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Сегодня"), KeyboardButton(text="🧾 Средний чек")],
            [KeyboardButton(text="🏃 Официанты"), KeyboardButton(text="🍽 ABC меню")],
            [KeyboardButton(text="🗓 Отчет за период"), KeyboardButton(text="📅 Выручка по дням недели")],
            [KeyboardButton(text="🍳 Кухня / бар"), KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите отчёт"
    )


def weekday_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Все дни недели", callback_data="weekday:mode:all")],
            [InlineKeyboardButton(text="Конкретный день", callback_data="weekday:mode:single")],
            [InlineKeyboardButton(text="Назад", callback_data="action:back")],
        ]
    )


def weekday_month_picker_kb(year: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="◀", callback_data="weekday:year_shift:-1"),
            InlineKeyboardButton(text=str(year), callback_data="weekday:noop"),
            InlineKeyboardButton(text="▶", callback_data="weekday:year_shift:1"),
        ]
    ]
    month_buttons = [
        InlineKeyboardButton(text=MONTHS_RU_SHORT[m], callback_data=f"weekday:month:{m}")
        for m in range(1, 13)
    ]
    for i in range(0, 12, 3):
        rows.append(month_buttons[i:i + 3])
    rows.append([
        InlineKeyboardButton(text="Назад", callback_data="weekday:back_mode"),
        InlineKeyboardButton(text="Отмена", callback_data="weekday:cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def weekday_select_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Пн", callback_data="weekday:day:0"),
                InlineKeyboardButton(text="Вт", callback_data="weekday:day:1"),
                InlineKeyboardButton(text="Ср", callback_data="weekday:day:2"),
                InlineKeyboardButton(text="Чт", callback_data="weekday:day:3"),
            ],
            [
                InlineKeyboardButton(text="Пт", callback_data="weekday:day:4"),
                InlineKeyboardButton(text="Сб", callback_data="weekday:day:5"),
                InlineKeyboardButton(text="Вс", callback_data="weekday:day:6"),
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data="weekday:back_month"),
                InlineKeyboardButton(text="Отмена", callback_data="weekday:cancel"),
            ],
        ]
    )


def date_picker_year_kb(year: int) -> InlineKeyboardMarkup:
    year_choices = [year - 1, year, year + 1]
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="◀", callback_data="daterange:year_shift:-1"),
                InlineKeyboardButton(text=str(year), callback_data="daterange:noop"),
                InlineKeyboardButton(text="▶", callback_data="daterange:year_shift:1"),
            ],
            [InlineKeyboardButton(text=str(y), callback_data=f"daterange:set_year:{y}") for y in year_choices],
            [
                InlineKeyboardButton(text="Назад", callback_data="daterange:back"),
                InlineKeyboardButton(text="Отмена", callback_data="daterange:cancel"),
            ],
        ]
    )


def date_picker_month_kb() -> InlineKeyboardMarkup:
    month_buttons = [
        InlineKeyboardButton(text=MONTHS_RU_SHORT[m], callback_data=f"daterange:set_month:{m}")
        for m in range(1, 13)
    ]
    rows = []
    for i in range(0, 12, 3):
        rows.append(month_buttons[i:i + 3])
    rows.append([
        InlineKeyboardButton(text="Назад", callback_data="daterange:back"),
        InlineKeyboardButton(text="Отмена", callback_data="daterange:cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def date_picker_day_kb(year: int, month: int) -> InlineKeyboardMarkup:
    days_in_month = calendar.monthrange(year, month)[1]
    day_buttons = [InlineKeyboardButton(text=str(d), callback_data=f"daterange:set_day:{d}") for d in range(1, days_in_month + 1)]
    rows = []
    for i in range(0, len(day_buttons), 7):
        rows.append(day_buttons[i:i + 7])
    rows.append([
        InlineKeyboardButton(text="◀", callback_data="daterange:day_nav:-1"),
        InlineKeyboardButton(text=f"{MONTHS_RU_SHORT[month]} {year}", callback_data="daterange:noop"),
        InlineKeyboardButton(text="▶", callback_data="daterange:day_nav:1"),
    ])
    rows.append([
        InlineKeyboardButton(text="Назад", callback_data="daterange:back"),
        InlineKeyboardButton(text="Отмена", callback_data="daterange:cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kitchen_bar_segment_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Бар", callback_data="kbar:seg:bar")],
            [InlineKeyboardButton(text="Бар по барам", callback_data="kbar:seg:bar_by_bars")],
            [InlineKeyboardButton(text="Кухня МЕСТО", callback_data="kbar:seg:kitchen_mesto")],
            [InlineKeyboardButton(text="Кухня burger", callback_data="kbar:seg:kitchen_burger")],
            [InlineKeyboardButton(text="Кухня по цехам", callback_data="kbar:seg:workshops")],
            [InlineKeyboardButton(text="Назад", callback_data="kbar:exit")],
        ]
    )


def kitchen_bar_metric_kb(segment: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Выручка", callback_data=f"kbar:metric:{segment}:revenue"),
                InlineKeyboardButton(text="Количество", callback_data=f"kbar:metric:{segment}:quantity"),
            ],
            [
                InlineKeyboardButton(text="Топ позиций", callback_data=f"kbar:metric:{segment}:top"),
                InlineKeyboardButton(text="ABC", callback_data=f"kbar:metric:{segment}:abc"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="kbar:back:seg")],
        ]
    )


def kitchen_workshops_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Выручка по цехам", callback_data="kbar:workshops:revenue")],
            [InlineKeyboardButton(text="Количество по цехам", callback_data="kbar:workshops:quantity")],
            [InlineKeyboardButton(text="ABC по цехам", callback_data="kbar:workshops:abc")],
            [InlineKeyboardButton(text="Назад", callback_data="kbar:back:seg")],
        ]
    )


def abc_segment_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Бар", callback_data="abcseg:bar")],
            [InlineKeyboardButton(text="Бар по барам", callback_data="abcseg:bar_by_bars")],
            [InlineKeyboardButton(text="Кухня МЕСТО", callback_data="abcseg:kitchen_mesto")],
            [InlineKeyboardButton(text="Кухня burger", callback_data="abcseg:kitchen_burger")],
            [InlineKeyboardButton(text="Кухня по цехам", callback_data="abcseg:workshops")],
            [InlineKeyboardButton(text="Назад", callback_data="abcseg:exit")],
        ]
    )


def bar_section_picker_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Бар burger", callback_data=f"{prefix}:bar_burger")],
            [InlineKeyboardButton(text="МЕСТО Бар", callback_data=f"{prefix}:mesto_bar")],
            [InlineKeyboardButton(text="Назад", callback_data="kbar:back:seg")],
        ]
    )


def bar_section_metric_kb(section_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Выручка", callback_data=f"kbarbar:metric:{section_key}:revenue"),
                InlineKeyboardButton(text="Количество", callback_data=f"kbarbar:metric:{section_key}:quantity"),
            ],
            [
                InlineKeyboardButton(text="Топ позиций", callback_data=f"kbarbar:metric:{section_key}:top"),
                InlineKeyboardButton(text="ABC", callback_data=f"kbarbar:metric:{section_key}:abc"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="kbarbar:back")],
        ]
    )


def abc_bar_section_picker_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Бар burger", callback_data="abcsegbar:bar_burger")],
            [InlineKeyboardButton(text="МЕСТО Бар", callback_data="abcsegbar:mesto_bar")],
            [InlineKeyboardButton(text="Назад", callback_data="abcseg:back")],
        ]
    )


def abc_back_to_segments_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="abcseg:back")],
        ]
    )


def abc_back_to_bar_picker_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="abcsegbar:back")],
        ]
    )


def today_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сегодня", callback_data="today:1"),
                InlineKeyboardButton(text="7 дней", callback_data="today:7"),
                InlineKeyboardButton(text="30 дней", callback_data="today:30"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="today:back")]
        ]
    )


def waiters_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Топ-5", callback_data="waiters:5"),
                InlineKeyboardButton(text="Топ-10", callback_data="waiters:10"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="waiters:back")]
        ]
    )


def abc_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="По выручке", callback_data="abc:revenue"),
                InlineKeyboardButton(text="По количеству", callback_data="abc:quantity"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="abc:back")]
        ]
    )


def back_inline_kb(callback_data: str = "action:back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data=callback_data)]
        ]
    )


def weekday_result_back_kb(target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data=f"weekday:result_back:{target}")],
        ]
    )


def daterange_result_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="daterange:result_back")],
        ]
    )
