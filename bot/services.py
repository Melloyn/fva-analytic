from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from backend.ingestion.loader import load_file, detect_report_type, build_mapping, prepare_kpi_df
from backend.analytics.metrics import calculate_waiters_metrics, calculate_food_usage_metrics
from backend.utils.format import format_rub
from backend.utils.normalize import normalize_number_series, normalize_col_name
from bot.formatters import clean_dish_name, format_percent_change

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

NO_DATA_MESSAGE = "Нет подготовленных данных.\nЗагрузите отчёты через приложение."

class MockFile:
    def __init__(self, path: Path):
        self.name = path.name
        self._path = path
    def getvalue(self):
        with open(self._path, "rb") as f:
            return f.read()

def _get_kpi_df(target_type: str) -> Optional[pd.DataFrame]:
    if not PROCESSED_DIR.exists():
        return None
        
    preferred = PROCESSED_DIR / f"{target_type}_clean.csv"
    paths_to_try = [preferred] + list(PROCESSED_DIR.glob("*.csv"))
    
    for path in paths_to_try:
        if not path.exists():
            continue
            
        parsed_df, parse_info, load_err = load_file(MockFile(path))
        if load_err or parsed_df is None:
            continue
            
        report_type = detect_report_type(parsed_df)
        if report_type == target_type:
            mapping = build_mapping(parsed_df, report_type)
            df_kpi = prepare_kpi_df(parsed_df, mapping, report_type)
            return df_kpi
            
    return None

def get_revenue_report_text(days: int = 1) -> str:
    df_kpi = _get_kpi_df("waiters")
    if df_kpi is None:
        df_kpi = _get_kpi_df("revenue_by_day")
    if df_kpi is None:
        return NO_DATA_MESSAGE
        
    date_str = ""
    latest_df = df_kpi
    prev_df = None
    
    if "date" in df_kpi.columns:
        dates = pd.to_datetime(df_kpi["date"], errors="coerce", dayfirst=True)
        valid_dates = dates.dropna()
        if not valid_dates.empty:
            latest = valid_dates.max()
            start_date = latest - pd.Timedelta(days=days - 1)
            
            mask = (dates.dt.date >= start_date.date()) & (dates.dt.date <= latest.date())
            latest_df = df_kpi[mask].copy()
            
            if days == 1:
                date_str = latest.strftime("%d.%m.%Y")
                unique_dates = sorted(list(set(valid_dates.dt.date)))
                if len(unique_dates) > 1:
                    idx = unique_dates.index(latest.date())
                    if idx > 0:
                        prev_date = unique_dates[idx-1]
                        prev_df = df_kpi[dates.dt.date == prev_date].copy()
            else:
                date_str = f"за {days} дн. ({start_date.strftime('%d.%m.%Y')} - {latest.strftime('%d.%m.%Y')})"

    metrics = calculate_waiters_metrics(latest_df)
    if not metrics.get("ok"):
        return f"Ошибка расчетов: {metrics.get('reason')}"
        
    total_rev = metrics.get('total_revenue', 0.0)
    checks_count = metrics.get('checks_count', 0)
    avg_check = metrics.get('avg_check', 0.0)
    
    title = "📊 Сегодня\n" if days == 1 else f"📊 Отчет {date_str}\n"
    
    lines = [
        title,
        f"Выручка: {format_rub(total_rev)}",
        f"Чеков: {checks_count}",
        f"Средний чек: {format_rub(avg_check)}"
    ]
    
    if days == 1 and prev_df is not None:
        prev_metrics = calculate_waiters_metrics(prev_df)
        prev_rev = prev_metrics.get('total_revenue', 0.0)
        if prev_rev > 0:
            delta_pct = (total_rev - prev_rev) / prev_rev * 100.0
            lines.append("")
            lines.append(f"Изменение к вчера: {format_percent_change(delta_pct)}")
            
    return "\n".join(lines)

def get_avg_check_text() -> str:
    df_kpi = _get_kpi_df("waiters")
    if df_kpi is None:
        df_kpi = _get_kpi_df("revenue_by_day")
    if df_kpi is None:
        return NO_DATA_MESSAGE
        
    date_str = ""
    latest_df = df_kpi
    prev_df = None
    
    if "date" in df_kpi.columns:
        dates = pd.to_datetime(df_kpi["date"], errors="coerce", dayfirst=True)
        valid_dates = dates.dropna()
        if not valid_dates.empty:
            latest_date = valid_dates.max().date()
            latest_df = df_kpi[dates.dt.date == latest_date].copy()
            date_str = latest_date.strftime("%d.%m.%Y")
            
            unique_dates = sorted(list(set(valid_dates.dt.date)))
            if len(unique_dates) > 1:
                idx = unique_dates.index(latest_date)
                if idx > 0:
                    prev_date = unique_dates[idx-1]
                    prev_df = df_kpi[dates.dt.date == prev_date].copy()
                    
    latest_metrics = calculate_waiters_metrics(latest_df)
    if not latest_metrics.get("ok"):
        return f"Ошибка расчетов: {latest_metrics.get('reason')}"
        
    avg_latest = latest_metrics.get("avg_check", 0.0)
    
    if prev_df is not None:
        prev_metrics = calculate_waiters_metrics(prev_df)
        avg_prev = prev_metrics.get("avg_check", 0.0)
        
        delta = avg_latest - avg_prev
        delta_pct = (delta / avg_prev * 100.0) if avg_prev else 0.0
        sign = "+" if delta > 0 else ""
        
        return (
            f"🧾 Средний чек\n\n"
            f"Сегодня: {format_rub(avg_latest)}\n"
            f"Вчера: {format_rub(avg_prev)}\n"
            f"Изменение: {sign}{format_rub(delta)} ({format_percent_change(delta_pct)})"
        )
    else:
        suffix = f" ({date_str})" if date_str else ""
        return f"🧾 Средний чек{suffix}\n\nСегодня: {format_rub(avg_latest)}\nСравнение недоступно: нет предыдущего периода."

def get_waiters_text(limit: int = 5) -> str:
    df_kpi = _get_kpi_df("waiters")
    if df_kpi is None:
        return NO_DATA_MESSAGE
        
    metrics = calculate_waiters_metrics(df_kpi)
    if not metrics.get("ok"):
        return f"Ошибка расчетов: {metrics.get('reason')}"
        
    table = metrics.get("waiter_table")
    if table is None or table.empty:
        return "🏃 Нет валидных данных по официантам."
        
    top = table.head(limit)
    lines = [
        "🏃 Топ официантов\n"
    ]
    for i, row in enumerate(top.itertuples(index=False), 1):
        lines.append(f"{i}. {row.waiter} — {format_rub(float(row.revenue))}")
        
    lines.append("")
    lines.append(f"Показаны {limit} лучших по выручке.")
    return "\n".join(lines)

def get_abc_menu_text(sort_by: str = "revenue") -> str:
    df_kpi = _get_kpi_df("food_usage")
    if df_kpi is None:
        return NO_DATA_MESSAGE
        
    metrics = calculate_food_usage_metrics(df_kpi)
    if not metrics.get("ok"):
        return f"Ошибка расчетов: {metrics.get('reason')}"
        
    top_rev = metrics.get("top_revenue")
    top_qty = metrics.get("top_quantity")
    
    if top_rev is None and top_qty is None:
        return "🍽 Нет валидных данных для ABC-анализа."
        
    lines = ["🍽 ABC меню\n"]
    
    if sort_by == "revenue":
        if top_rev is None or top_rev.empty:
            return "🍽 Нет валидных данных по выручке."
            
        total_rev = float(top_rev["revenue"].sum())
        work_abc = top_rev.copy()
        work_abc["share"] = work_abc["revenue"] / total_rev if total_rev > 0 else 0.0
        work_abc["cum_share"] = work_abc["share"].cumsum()
        
        def abc_class(cum_share: float) -> str:
            if cum_share <= 0.8: return "A"
            if cum_share <= 0.95: return "B"
            return "C"

        work_abc["abc"] = work_abc["cum_share"].apply(abc_class)
        counts = work_abc["abc"].value_counts().to_dict()
        
        lines.append(f"A: {counts.get('A', 0)}")
        lines.append(f"B: {counts.get('B', 0)}")
        lines.append(f"C: {counts.get('C', 0)}\n")
        
        lines.append("Топ-5 блюд по выручке:")
        for i, row in enumerate(work_abc.head(5).itertuples(index=False), 1):
            lines.append(f"{i}. {clean_dish_name(row.dish)} — {format_rub(float(row.revenue))}")
            
    elif sort_by == "quantity":
        if top_qty is None or top_qty.empty:
            return "🍽 Нет валидных данных по количеству."
            
        lines.append("Топ-5 блюд по количеству:")
        for i, row in enumerate(top_qty.head(5).itertuples(index=False), 1):
            lines.append(f"{i}. {clean_dish_name(row.dish)} — {int(row.quantity)} шт")
            
    return "\n".join(lines)

def get_kitchen_bar_text() -> str:
    df_kpi = _get_kpi_df("food_usage")
    if df_kpi is None:
        return (
            "🍳 Кухня / бар\n\n"
            "Недостаточно данных для анализа кухни/бара.\n"
            "Загрузите отчет, где есть колонка цеха / станции / категории."
        )
        
    norm_cols = {c: normalize_col_name(str(c)) for c in df_kpi.columns}
    def find_col(priorities):
        for p in priorities:
            p_norm = normalize_col_name(p)
            for c, n in norm_cols.items():
                if n == p_norm or p_norm in n: return c
        return None
        
    station_col = find_col(["station", "цех", "станция", "подразделение", "категория", "группа", "кухня", "бар"])
    if not station_col:
        return (
            "🍳 Кухня / бар\n\n"
            "Недостаточно данных для анализа кухни/бара.\n"
            "Загрузите отчет, где есть колонка цеха / станции / категории."
        )
        
    metrics = calculate_food_usage_metrics(df_kpi)
    if not metrics.get("ok"):
        return f"Ошибка расчетов: {metrics.get('reason')}"
        
    revenue_col = metrics.get("revenue_col_name")
    quantity_col = metrics.get("quantity_col_name")
    
    metric_col = revenue_col if revenue_col else quantity_col
    
    if not metric_col or metric_col not in df_kpi.columns:
        return (
            "🍳 Кухня / бар\n\n"
            "Недостаточно данных для анализа кухни/бара.\n"
            "Загрузите отчет, где есть числовая метрика."
        )
        
    work = df_kpi[[station_col, metric_col]].copy()
    work[station_col] = work[station_col].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
    work = work.dropna(subset=[station_col])
    
    if work.empty:
        return (
            "🍳 Кухня / бар\n\n"
            "Недостаточно данных для анализа кухни/бара.\n"
            "Загрузите отчет, где заполнены колонки цехов."
        )
        
    work[metric_col] = normalize_number_series(work[metric_col])
    
    grouped = work.groupby(station_col, dropna=False)[metric_col].sum().sort_values(ascending=False).head(8)
    
    # Try to group by kitchen/bar broadly if there are matching words
    kitchen_sum = 0.0
    bar_sum = 0.0
    for name, value in grouped.items():
        name_lower = str(name).lower()
        if "бар" in name_lower or "bar" in name_lower:
            bar_sum += float(value)
        else:
            kitchen_sum += float(value)
            
    lines = ["🍳 Кухня / бар\n"]
    
    if metric_col == revenue_col:
        lines.append(f"Кухня: {format_rub(kitchen_sum)}")
        lines.append(f"Бар: {format_rub(bar_sum)}")
    else:
        lines.append(f"Кухня: {int(kitchen_sum)} шт")
        lines.append(f"Бар: {int(bar_sum)} шт")
        
    lines.append("\nТоп нагрузки:")
    for i, (name, value) in enumerate(grouped.items(), 1):
        if metric_col == revenue_col:
            lines.append(f"{i}. {name} — {format_rub(float(value))}")
        else:
            lines.append(f"{i}. {name} — {int(value)} шт")
            
    return "\n".join(lines)

def get_help_text() -> str:
    return (
        "ℹ️ Помощь\n\n"
        "Доступные разделы:\n"
        "📊 Сегодня — краткая сводка\n"
        "🧾 Средний чек — текущая динамика\n"
        "🏃 Официанты — топ по выручке\n"
        "🍽 ABC меню — анализ меню\n"
        "🍳 Кухня / бар — загрузка подразделений\n\n"
        "Выберите нужный раздел через меню ниже."
    )
