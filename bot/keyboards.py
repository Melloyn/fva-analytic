from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

def main_menu_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Сегодня"), KeyboardButton(text="🧾 Средний чек")],
            [KeyboardButton(text="🏃 Официанты"), KeyboardButton(text="🍽 ABC меню")],
            [KeyboardButton(text="🍳 Кухня / бар"), KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите отчёт"
    )

def today_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сегодня", callback_data="today:1"),
                InlineKeyboardButton(text="7 дней", callback_data="today:7"),
                InlineKeyboardButton(text="30 дней", callback_data="today:30"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="action:back")]
        ]
    )

def waiters_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Топ-5", callback_data="waiters:5"),
                InlineKeyboardButton(text="Топ-10", callback_data="waiters:10"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="action:back")]
        ]
    )

def abc_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="По выручке", callback_data="abc:revenue"),
                InlineKeyboardButton(text="По количеству", callback_data="abc:quantity"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="action:back")]
        ]
    )

def back_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="action:back")]
        ]
    )
