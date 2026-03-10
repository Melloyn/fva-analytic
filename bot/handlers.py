from datetime import date

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
    back_inline_kb,
    weekday_mode_kb,
    weekday_month_picker_kb,
    weekday_select_kb,
    date_picker_year_kb,
    date_picker_month_kb,
    date_picker_day_kb,
    MONTHS_RU_SHORT,
)
from bot.services import (
    get_revenue_report_text,
    get_revenue_report_by_date_range_text,
    get_revenue_by_weekday_month_text,
    get_revenue_by_weekday_month_day_text,
    get_avg_check_text,
    get_waiters_text,
    get_abc_menu_text,
    get_kitchen_bar_text,
    get_help_text,
)

router = Router()


class BotFlowStates(StatesGroup):
    weekday_mode = State()
    weekday_month = State()
    weekday_day = State()
    daterange_pick = State()


def _month_shift(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    new_year = total // 12
    new_month = total % 12 + 1
    return new_year, new_month


def _format_date_ru(d: date) -> str:
    return d.strftime("%d.%m.%Y")


async def _render_daterange_picker(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    phase = data.get("dr_phase", "start")
    step = data.get("dr_step", "year")
    year = int(data.get("dr_year", date.today().year))
    month = int(data.get("dr_month", date.today().month))
    start_iso = data.get("dr_start_date")

    phase_text = "начала" if phase == "start" else "окончания"
    if step == "year":
        text = f"🗓 Выбор даты {phase_text}: выберите год"
        kb = date_picker_year_kb(year)
    elif step == "month":
        text = f"🗓 Выбор даты {phase_text}: выберите месяц ({year})"
        kb = date_picker_month_kb()
    else:
        text = f"🗓 Выбор даты {phase_text}: выберите день ({MONTHS_RU_SHORT[month]} {year})"
        kb = date_picker_day_kb(year, month)

    if start_iso:
        start_date = date.fromisoformat(start_iso)
        text += f"\n\nДата начала: {_format_date_ru(start_date)}"

    await callback.message.edit_text(text, reply_markup=kb)


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
    text = get_revenue_report_text(days=1)
    await message.answer(text, reply_markup=today_inline_kb())


@router.message(F.text == "🧾 Средний чек")
async def msg_avg_check(message: Message):
    text = get_avg_check_text()
    await message.answer(text, reply_markup=back_inline_kb())


@router.message(F.text == "🏃 Официанты")
async def msg_waiters(message: Message):
    text = get_waiters_text(limit=5)
    await message.answer(text, reply_markup=waiters_inline_kb())


@router.message(F.text == "🍽 ABC меню")
async def msg_abc(message: Message):
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


@router.message(F.text == "📅 Выручка по дням недели")
async def msg_weekday_mode_start(message: Message, state: FSMContext):
    await state.set_state(BotFlowStates.weekday_mode)
    await state.update_data(wd_mode=None, wd_year=date.today().year, wd_month=None)
    await message.answer("Выберите режим отчета по дням недели:", reply_markup=weekday_mode_kb())


@router.message(F.text == "🗓 Отчет за период")
async def msg_daterange_start(message: Message, state: FSMContext):
    today = date.today()
    await state.set_state(BotFlowStates.daterange_pick)
    await state.update_data(
        dr_phase="start",
        dr_step="year",
        dr_year=today.year,
        dr_month=today.month,
        dr_start_date=None,
    )
    await message.answer("🗓 Выбор даты начала: выберите год", reply_markup=date_picker_year_kb(today.year))


# ====================
# Weekday Flow
# ====================

@router.callback_query(F.data.startswith("weekday:"))
async def cb_weekday_flow(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "noop":
        await callback.answer()
        return

    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("Действие отменено.", reply_markup=back_inline_kb())
        await callback.answer()
        return

    data = await state.get_data()

    if action == "mode":
        mode = parts[2]
        year = date.today().year
        await state.set_state(BotFlowStates.weekday_month)
        await state.update_data(wd_mode=mode, wd_year=year, wd_month=None)
        await callback.message.edit_text("Выберите год и месяц:", reply_markup=weekday_month_picker_kb(year))
        await callback.answer()
        return

    if action == "back_mode":
        await state.set_state(BotFlowStates.weekday_mode)
        await callback.message.edit_text("Выберите режим отчета по дням недели:", reply_markup=weekday_mode_kb())
        await callback.answer()
        return

    if action == "year_shift":
        year = int(data.get("wd_year", date.today().year)) + int(parts[2])
        await state.update_data(wd_year=year)
        await callback.message.edit_text("Выберите год и месяц:", reply_markup=weekday_month_picker_kb(year))
        await callback.answer()
        return

    if action == "month":
        month = int(parts[2])
        year = int(data.get("wd_year", date.today().year))
        mode = data.get("wd_mode", "all")
        month_text = f"{year}-{month:02d}"

        if mode == "all":
            text = get_revenue_by_weekday_month_text(month_text=month_text)
            await state.clear()
            await callback.message.edit_text(text, reply_markup=back_inline_kb())
            await callback.answer()
            return

        await state.set_state(BotFlowStates.weekday_day)
        await state.update_data(wd_month=month_text)
        await callback.message.edit_text(
            f"Выберите день недели для {month_text}:",
            reply_markup=weekday_select_kb(),
        )
        await callback.answer()
        return

    if action == "back_month":
        year = int(data.get("wd_year", date.today().year))
        await state.set_state(BotFlowStates.weekday_month)
        await callback.message.edit_text("Выберите год и месяц:", reply_markup=weekday_month_picker_kb(year))
        await callback.answer()
        return

    if action == "day":
        month_text = data.get("wd_month")
        weekday_idx = int(parts[2])
        text = get_revenue_by_weekday_month_day_text(month_text=month_text or "", weekday_idx=weekday_idx)
        await state.clear()
        await callback.message.edit_text(text, reply_markup=back_inline_kb())
        await callback.answer()
        return

    await callback.answer()


# ====================
# Date Range Flow
# ====================

@router.callback_query(F.data.startswith("daterange:"))
async def cb_daterange_flow(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "noop":
        await callback.answer()
        return

    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("Действие отменено.", reply_markup=back_inline_kb())
        await callback.answer()
        return

    data = await state.get_data()
    phase = data.get("dr_phase", "start")
    step = data.get("dr_step", "year")
    year = int(data.get("dr_year", date.today().year))
    month = int(data.get("dr_month", date.today().month))
    start_iso = data.get("dr_start_date")

    if action == "back":
        if step == "month":
            step = "year"
        elif step == "day":
            step = "month"
        else:
            if phase == "end" and start_iso:
                start_date = date.fromisoformat(start_iso)
                phase = "start"
                step = "day"
                year, month = start_date.year, start_date.month
            else:
                await state.clear()
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                await callback.message.answer("Главное меню", reply_markup=main_menu_reply_keyboard())
                await callback.answer()
                return

    elif action == "year_shift":
        year += int(parts[2])
    elif action == "set_year":
        year = int(parts[2])
        step = "month"
    elif action == "set_month":
        month = int(parts[2])
        step = "day"
    elif action == "day_nav":
        year, month = _month_shift(year, month, int(parts[2]))
        step = "day"
    elif action == "set_day":
        day = int(parts[2])
        picked_date = date(year, month, day)

        if phase == "start":
            await state.update_data(dr_start_date=picked_date.isoformat())
            phase = "end"
            step = "year"
            year = picked_date.year
            month = picked_date.month
        else:
            if not start_iso:
                await callback.answer("Дата начала не выбрана", show_alert=True)
                return
            start_date = date.fromisoformat(start_iso)
            if picked_date < start_date:
                await callback.answer("Дата окончания раньше даты начала", show_alert=True)
                return

            text = get_revenue_report_by_date_range_text(
                date_from=_format_date_ru(start_date),
                date_to=_format_date_ru(picked_date),
            )
            await state.clear()
            await callback.message.edit_text(text, reply_markup=back_inline_kb())
            await callback.answer()
            return

    await state.update_data(dr_phase=phase, dr_step=step, dr_year=year, dr_month=month)
    await _render_daterange_picker(callback, state)
    await callback.answer()


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
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer("Главное меню", reply_markup=main_menu_reply_keyboard())
    await callback.answer()
