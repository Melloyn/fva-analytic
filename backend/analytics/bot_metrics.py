from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from backend.utils.normalize import normalize_number_series


def calculate_revenue_by_weekday_for_month(df_kpi: pd.DataFrame, year: int, month: int) -> Dict[str, Any]:
    if "date" not in df_kpi.columns or "revenue" not in df_kpi.columns:
        return {"ok": False, "reason": "Недостаточно данных: нужны колонки date и revenue."}

    work = df_kpi[["date", "revenue"]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce", dayfirst=True)
    work["revenue"] = normalize_number_series(work["revenue"])
    work = work.dropna(subset=["date"])

    if work.empty:
        return {"ok": False, "reason": "Нет валидных дат после очистки."}

    month_mask = (work["date"].dt.year == year) & (work["date"].dt.month == month)
    month_df = work[month_mask].copy()
    if month_df.empty:
        return {"ok": False, "reason": "Нет данных за выбранный месяц."}

    month_df["weekday_idx"] = month_df["date"].dt.weekday  # 0=Mon .. 6=Sun
    weekday_series = month_df.groupby("weekday_idx")["revenue"].sum()

    ordered = []
    for idx in range(7):
        ordered.append((idx, float(weekday_series.get(idx, 0.0))))

    return {"ok": True, "weekday_revenue": ordered}


def calculate_revenue_for_weekday_in_month(
    df_kpi: pd.DataFrame,
    year: int,
    month: int,
    weekday_idx: int,
) -> Dict[str, Any]:
    if weekday_idx < 0 or weekday_idx > 6:
        return {"ok": False, "reason": "Некорректный индекс дня недели: ожидается значение от 0 до 6."}

    base = calculate_revenue_by_weekday_for_month(df_kpi=df_kpi, year=year, month=month)
    if not base.get("ok"):
        return base

    day_value = 0.0
    for idx, value in base.get("weekday_revenue", []):
        if int(idx) == weekday_idx:
            day_value = float(value)
            break

    return {"ok": True, "weekday_idx": weekday_idx, "revenue": day_value}
