from aiogram import F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from bot.keyboards import (
    main_menu_reply_keyboard,
    today_inline_kb,
    waiters_inline_kb,
    abc_inline_kb,
    back_inline_kb
)
from bot.services import (
    get_revenue_report_text,
    get_revenue_report_by_date_range_text,
    get_revenue_by_weekday_month_text,
    get_avg_check_text,
    get_waiters_text,
    get_abc_menu_text,
    get_kitchen_bar_text,
    get_help_text,
)

router = Router()


class ReportDateStates(StatesGroup):
    waiting_date_from = State()
    waiting_date_to = State()
    waiting_month = State()


# ====================
# Reply Keyboard Handlers (Main Menu)
# ====================

@router.message(CommandStart())
async def cmd_start(message: Message):
    text = (
        "Добро пожаловать в FVA Analytic.\n"
        "Выберите нужный отчет ниже."
    )
    await message.answer(text, reply_markup=main_menu_reply_keyboard())

@router.message(F.text == "📊 Сегодня")
async def msg_today(message: Message):
    # Default is 1 day
    text = get_revenue_report_text(days=1)
    await message.answer(text, reply_markup=today_inline_kb())

@router.message(F.text == "🧾 Средний чек")
async def msg_avg_check(message: Message):
    text = get_avg_check_text()
    await message.answer(text, reply_markup=back_inline_kb())

@router.message(F.text == "🏃 Официанты")
async def msg_waiters(message: Message):
    # Default is top 5
    text = get_waiters_text(limit=5)
    await message.answer(text, reply_markup=waiters_inline_kb())

@router.message(F.text == "🍽 ABC меню")
async def msg_abc(message: Message):
    # Default is by revenue
    text = get_abc_menu_text(sort_by="revenue")
    await message.answer(text, reply_markup=abc_inline_kb())

@router.message(F.text == "🍳 Кухня / бар")
async def msg_kitchen_bar(message: Message):
    text = get_kitchen_bar_text()
    await message.answer(text, reply_markup=back_inline_kb())

@router.message(F.text == "ℹ️ Помощь")
async def msg_help(message: Message):
    text = get_help_text()
    await message.answer(text, reply_markup=back_inline_kb())


@router.message(F.text == "🗓 Отчет за период")
async def msg_date_range_start(message: Message, state: FSMContext):
    await state.set_state(ReportDateStates.waiting_date_from)
    await state.update_data(date_from=None)
    await message.answer(
        "Введите дату начала периода в формате ДД.ММ.ГГГГ.\n"
        "Для отмены отправьте: Отмена",
        reply_markup=back_inline_kb(),
    )


@router.message(F.text == "📅 Выручка по дням недели")
async def msg_weekday_month_start(message: Message, state: FSMContext):
    await state.set_state(ReportDateStates.waiting_month)
    await message.answer(
        "Введите месяц в формате ГГГГ-ММ (например, 2026-03).\n"
        "Для отмены отправьте: Отмена",
        reply_markup=back_inline_kb(),
    )


@router.message(ReportDateStates.waiting_date_from, F.text.casefold() == "отмена")
@router.message(ReportDateStates.waiting_date_to, F.text.casefold() == "отмена")
@router.message(ReportDateStates.waiting_month, F.text.casefold() == "отмена")
async def msg_cancel_state(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Действие отменено. Главное меню.", reply_markup=main_menu_reply_keyboard())


@router.message(ReportDateStates.waiting_date_from)
async def msg_date_range_from_input(message: Message, state: FSMContext):
    date_from = (message.text or "").strip()
    await state.update_data(date_from=date_from)
    await state.set_state(ReportDateStates.waiting_date_to)
    await message.answer(
        "Введите дату окончания периода в формате ДД.ММ.ГГГГ.\n"
        "Для отмены отправьте: Отмена",
        reply_markup=back_inline_kb(),
    )


@router.message(ReportDateStates.waiting_date_to)
async def msg_date_range_to_input(message: Message, state: FSMContext):
    date_to = (message.text or "").strip()
    data = await state.get_data()
    date_from = data.get("date_from", "")
    text = get_revenue_report_by_date_range_text(date_from=date_from, date_to=date_to)
    await state.clear()
    await message.answer(text, reply_markup=main_menu_reply_keyboard())


@router.message(ReportDateStates.waiting_month)
async def msg_weekday_month_input(message: Message, state: FSMContext):
    month = (message.text or "").strip()
    text = get_revenue_by_weekday_month_text(month_text=month)
    await state.clear()
    await message.answer(text, reply_markup=main_menu_reply_keyboard())


# ====================
# Inline Keyboard Handlers (Contextual Actions)
# ====================

@router.callback_query(F.data.startswith("today:"))
async def cb_today_period(callback: CallbackQuery):
    days = int(callback.data.split(":")[1])
    text = get_revenue_report_text(days=days)
    try:
        await callback.message.edit_text(text, reply_markup=today_inline_kb())
    except Exception:
        pass
    await callback.answer()

@router.callback_query(F.data.startswith("waiters:"))
async def cb_waiters_limit(callback: CallbackQuery):
    limit = int(callback.data.split(":")[1])
    text = get_waiters_text(limit=limit)
    try:
        await callback.message.edit_text(text, reply_markup=waiters_inline_kb())
    except Exception:
        pass
    await callback.answer()

@router.callback_query(F.data.startswith("abc:"))
async def cb_abc_sort(callback: CallbackQuery):
    sort_by = callback.data.split(":")[1]
    text = get_abc_menu_text(sort_by=sort_by)
    try:
        await callback.message.edit_text(text, reply_markup=abc_inline_kb())
    except Exception:
        pass
    await callback.answer()

@router.callback_query(F.data == "action:back")
async def cb_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    # Send a new message to re-trigger the reply keyboard visibility clearly if needed,
    # or just delete the inline keyboard message and say "Главное меню".
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer("Главное меню", reply_markup=main_menu_reply_keyboard())
    await callback.answer()
