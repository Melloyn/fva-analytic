import pandas as pd
from typing import Dict, Any
from backend.utils.normalize import normalize_number_series


def calculate_waiters_metrics(df_kpi: pd.DataFrame) -> Dict[str, Any]:
    if "revenue" not in df_kpi.columns:
        return {"ok": False, "reason": "Недостаточно данных: не найдена колонка revenue."}

    if "check_id" in df_kpi.columns:
        uniq_checks = int(df_kpi["check_id"].nunique(dropna=True))
    else:
        uniq_checks = 0

    checks_logic = "nunique(check_id)"
    if uniq_checks > 10:
        checks_count = uniq_checks
    else:
        checks_count = int(len(df_kpi))
        checks_logic = "fallback len(df_kpi)"

    total_revenue = float(df_kpi["revenue"].sum())
    avg_check = total_revenue / checks_count if checks_count > 0 else 0.0

    if "waiter" not in df_kpi.columns:
        return {
            "ok": True,
            "total_revenue": total_revenue,
            "checks_count": checks_count,
            "avg_check": avg_check,
            "waiter_table": None,
            "checks_logic": checks_logic,
            "warning": "Недостаточно данных: не найдена колонка waiter."
        }

    waiter_table = (
        df_kpi.groupby("waiter", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    waiter_table = waiter_table[
        ~waiter_table["waiter"].astype(str).str.strip().str.lower().isin(["", "none", "nan"])
    ]

    return {
        "ok": True,
        "total_revenue": total_revenue,
        "checks_count": checks_count,
        "avg_check": avg_check,
        "waiter_table": waiter_table,
        "checks_logic": checks_logic
    }


def calculate_revenue_by_day_metrics(df_kpi: pd.DataFrame) -> Dict[str, Any]:
    if "date" not in df_kpi.columns or "revenue" not in df_kpi.columns:
        return {"ok": False, "reason": "Недостаточно данных для отчёта выручки по дням."}

    work = df_kpi[["date", "revenue"]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce", dayfirst=True)
    work["revenue"] = normalize_number_series(work["revenue"])
    work = work.dropna(subset=["date"])
    
    if work.empty:
        return {"ok": False, "reason": "После очистки нет валидных данных по датам."}

    series = work.groupby("date")["revenue"].sum().sort_index()

    split = None
    day_table = None
    pivot = None

    if "cashbox" in df_kpi.columns or "payment_type" in df_kpi.columns:
        cashbox = df_kpi["cashbox"] if "cashbox" in df_kpi.columns else pd.Series([pd.NA] * len(df_kpi), index=df_kpi.index)
        payment = df_kpi["payment_type"] if "payment_type" in df_kpi.columns else pd.Series([pd.NA] * len(df_kpi), index=df_kpi.index)

        cashbox = cashbox.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
        payment = payment.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})

        cashbox_num = normalize_number_series(cashbox.fillna(""))
        cashbox_non_empty = int(cashbox.notna().sum())
        cashbox_numeric_ratio = (
            int((cashbox_num > 0).sum()) / cashbox_non_empty if cashbox_non_empty > 0 else 0.0
        )
        cashbox_is_numeric_like = cashbox_numeric_ratio > 0.7

        split_base = df_kpi.copy()
        split_base["revenue"] = normalize_number_series(split_base["revenue"])

        if payment.notna().sum() > 0 and cashbox_is_numeric_like:
            split_base["dimension"] = payment.fillna("Неизвестно")
        elif payment.notna().sum() > 0 and cashbox.notna().sum() > 0 and not cashbox_is_numeric_like:
            split_base["dimension"] = (payment.fillna("Неизвестно") + " / " + cashbox.fillna("Неизвестно"))
        elif payment.notna().sum() > 0:
            split_base["dimension"] = payment.fillna("Неизвестно")
        else:
            split_base["dimension"] = cashbox.fillna("Неизвестно")

        split = (
            split_base.groupby("dimension", dropna=False)["revenue"]
            .sum()
            .reset_index()
            .sort_values("revenue", ascending=False)
            .rename(columns={"dimension": "Касса/тип оплаты"})
        )

        day_split = split_base.copy()
        day_split["date"] = pd.to_datetime(day_split["date"], errors="coerce", dayfirst=True)
        day_split = day_split.dropna(subset=["date"])
        
        if not day_split.empty:
            day_split["date"] = day_split["date"].dt.date
            day_table = (
                day_split.groupby(["date", "dimension"], dropna=False)["revenue"]
                .sum()
                .reset_index()
                .sort_values(["date", "revenue"], ascending=[True, False])
                .rename(columns={"date": "Дата", "dimension": "Касса/тип оплаты"})
            )

            totals = (
                day_table.groupby("Касса/тип оплаты")["revenue"]
                .sum()
                .sort_values(ascending=False)
            )
            top_dims = totals.head(8).index.tolist()
            pivot = (
                day_table[day_table["Касса/тип оплаты"].isin(top_dims)]
                .pivot_table(
                    index="Дата",
                    columns="Касса/тип оплаты",
                    values="revenue",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .sort_index()
            )

    return {
        "ok": True,
        "daily_series": series,
        "split": split,
        "day_table": day_table,
        "pivot": pivot
    }


def calculate_food_usage_metrics(df_kpi: pd.DataFrame) -> Dict[str, Any]:
    dish_candidates = [c for c in ["dish", "Код", "Наименование", "Товар", "Позиция"] if c in df_kpi.columns]
    if not dish_candidates:
        return {"ok": False, "reason": "Недостаточно данных: не найдена колонка блюда (dish/Товар/Позиция)."}

    def dish_score(col: str) -> int:
        s = (
            df_kpi[col]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA, "Блюдо": pd.NA, "Код": pd.NA, "Итого": pd.NA})
        )
        return int(s.notna().sum())

    dish_col = max(dish_candidates, key=dish_score)

    work = df_kpi.copy()
    work[dish_col] = (
        work[dish_col]
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA, "Блюдо": pd.NA, "Код": pd.NA, "Итого": pd.NA})
    )
    # Remove leading numeric codes from dish names, e.g., "1278Борщ" -> "Борщ"
    work[dish_col] = work[dish_col].str.replace(r'^\d+\s*', '', regex=True)
    work = work.dropna(subset=[dish_col])

    # Find revenue col
    norm_cols = {c: str(c).lower().strip() for c in df_kpi.columns}
    revenue_col = None
    for target in ["оплачено", "сумма", "выручка", "итого", "revenue"]:
        matched = [c for c, n in norm_cols.items() if target in n]
        if matched:
            revenue_col = matched[0]
            break

    # Find quantity col
    quantity_col = None
    for target in ["кол-во", "количество", "quantity", "qty"]:
        matched = [c for c, n in norm_cols.items() if target in n]
        if matched:
            quantity_col = matched[0]
            break

    if not revenue_col and not quantity_col:
        return {"ok": False, "reason": "Недостаточно данных: не найдены подходящие числовые метрики (Сумма или Кол-во)."}

    result = {"ok": True, "dish_column": dish_col}
    
    if revenue_col:
        work[revenue_col] = normalize_number_series(work[revenue_col])
        top_rev = (
            work.groupby(dish_col, dropna=False)[revenue_col]
            .sum()
            .reset_index()
            .sort_values(revenue_col, ascending=False)
        ).rename(columns={dish_col: "dish", revenue_col: "revenue"})
        result["top_revenue"] = top_rev
        result["revenue_col_name"] = revenue_col
    else:
        result["top_revenue"] = None
        result["revenue_col_name"] = None

    if quantity_col:
        work[quantity_col] = normalize_number_series(work[quantity_col])
        top_qty = (
            work.groupby(dish_col, dropna=False)[quantity_col]
            .sum()
            .reset_index()
            .sort_values(quantity_col, ascending=False)
        ).rename(columns={dish_col: "dish", quantity_col: "quantity"})
        result["top_quantity"] = top_qty
        result["quantity_col_name"] = quantity_col
    else:
        result["top_quantity"] = None
        result["quantity_col_name"] = None

    return result
