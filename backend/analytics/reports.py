import pandas as pd
import streamlit as st

from backend.analytics.metrics import (
    calculate_waiters_metrics,
    calculate_revenue_by_day_metrics,
    calculate_food_usage_metrics
)
from backend.utils.format import format_rub


def render_waiters(df_kpi: pd.DataFrame, diagnostics_mode: bool):
    metrics = calculate_waiters_metrics(df_kpi)
    
    if not metrics.get("ok"):
        st.warning(metrics.get("reason", "Ошибка вычислений."))
        return "error"
        
    c1, c2, c3 = st.columns(3)
    c1.metric("Total revenue", format_rub(metrics.get("total_revenue", 0)))
    c2.metric("Checks count", metrics.get("checks_count", 0))
    c3.metric("Average check", format_rub(metrics.get("avg_check", 0)))
    
    if "warning" in metrics:
        st.warning(metrics["warning"])
        
    waiter_table = metrics.get("waiter_table")
    if waiter_table is not None:
        waiter_display = waiter_table.copy()
        waiter_display["revenue"] = waiter_display["revenue"].apply(format_rub)
        st.subheader("Выручка по официантам")
        st.dataframe(waiter_display, use_container_width=True)

    checks_logic = metrics.get("checks_logic", "n/a")
    if diagnostics_mode and checks_logic != "nunique(check_id)":
        st.warning("check_id малоуникален, для checks_count применён fallback: len(df_kpi).")
        
    return checks_logic


def render_revenue_by_day(df_kpi: pd.DataFrame):
    metrics = calculate_revenue_by_day_metrics(df_kpi)
    
    if not metrics.get("ok"):
        st.warning(metrics.get("reason", "Ошибка вычислений."))
        return
        
    series = metrics["daily_series"]
    st.subheader("Выручка по дням")
    st.line_chart(series)

    table = series.reset_index().rename(columns={"revenue": "Выручка", "date": "Дата"})
    table["Выручка"] = table["Выручка"].apply(format_rub)
    st.dataframe(table, use_container_width=True)
    
    split = metrics.get("split")
    if split is not None:
        split_display = split.copy()
        split_display["revenue"] = split_display["revenue"].apply(format_rub)
        st.subheader("Выручка по кассам/типам оплаты")
        st.dataframe(split_display, use_container_width=True)
        
    day_table = metrics.get("day_table")
    if day_table is not None:
        day_display = day_table.copy()
        day_display["revenue"] = day_display["revenue"].apply(format_rub)
        st.subheader("Выручка по дням и типам оплаты")
        st.dataframe(day_display, use_container_width=True)
        
    pivot = metrics.get("pivot")
    if pivot is not None and not pivot.empty:
        st.line_chart(pivot)


def render_food_usage(df_kpi: pd.DataFrame):
    metrics = calculate_food_usage_metrics(df_kpi)
    
    if not metrics.get("ok"):
        st.warning(metrics.get("reason", "Ошибка вычислений."))
        return
        
    top_rev = metrics.get("top_revenue")
    top_qty = metrics.get("top_quantity")
    
    if top_rev is not None and not top_rev.empty:
        st.subheader("Топ блюд по выручке")
        disp = top_rev.head(20).copy()
        disp["revenue"] = disp["revenue"].apply(format_rub)
        st.dataframe(disp, use_container_width=True)
        st.bar_chart(top_rev.head(20).set_index("dish")["revenue"])
        
    if top_qty is not None and not top_qty.empty:
        st.subheader("Топ блюд по количеству")
        st.dataframe(top_qty.head(20), use_container_width=True)
        st.bar_chart(top_qty.head(20).set_index("dish")["quantity"])
