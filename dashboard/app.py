from pathlib import Path

import pandas as pd
import streamlit as st

from backend.analytics.reports import (
    render_food_usage,
    render_revenue_by_day,
    render_waiters,
)
from backend.ingestion.loader import (
    build_mapping,
    detect_report_type,
    is_kitchen_bar_section_report_filename,
    load_file,
    prepare_kpi_df,
)
from backend.utils.format import format_money_columns_for_display
from backend.utils.normalize import normalize_number_series


st.set_page_config(page_title="FVA Analytic", layout="wide")

st.title("FVA Analytic — MVP")
st.write("Загрузите отчёт R-Keeper/1C в формате CSV или XLSX.")

diagnostics_mode = st.sidebar.checkbox("Diagnostics mode", value=False)
format_money_preview = st.sidebar.checkbox("Format money columns", value=True)


def clear_loaded_reports():
    for key in ["waiters", "revenue_by_day", "food_usage"]:
        if key in st.session_state:
            del st.session_state[key]


if st.sidebar.button("Reset loaded reports"):
    clear_loaded_reports()
    st.rerun()


def show_diagnostics(
    parse_info: dict,
    report_type: str,
    parsed_df: pd.DataFrame,
    mapping: dict,
    df_kpi: pd.DataFrame,
    checks_logic: str,
):
    st.subheader("Diagnostics")
    st.write(f"Encoding: `{parse_info.get('encoding')}`")
    st.write(f"Delimiter: `{parse_info.get('delimiter')}`")
    st.write(f"Header row index: `{parse_info.get('header_row_index')}`")
    st.write(f"Detected report type: `{report_type}`")
    st.write(f"Shape: `{parsed_df.shape}`")
    st.write("Final columns:")
    st.write(list(parsed_df.columns))
    st.write("Mapping (canonical -> source):")
    st.write(mapping)

    if mapping.get("revenue") and mapping["revenue"] in parsed_df.columns:
        st.write("Raw revenue sample (source, first 10):")
        st.write(parsed_df[mapping["revenue"]].head(10))
    if "revenue" in df_kpi.columns:
        st.write("Cleaned revenue sample (df_kpi, first 10):")
        st.write(df_kpi["revenue"].head(10))

    canonical_cols = [
        c
        for c in [
            "waiter",
            "check_id",
            "guests",
            "revenue",
            "date",
            "dish",
            "quantity",
            "checks_count",
            "cashbox",
            "payment_type",
        ]
        if c in df_kpi.columns
    ]
    if canonical_cols:
        st.write("Canonical dtypes:")
        st.write(df_kpi[canonical_cols].dtypes.astype(str).to_dict())

    if "check_id" in df_kpi.columns:
        st.write(f"check_id nunique(dropna=True): `{df_kpi['check_id'].nunique(dropna=True)}`")
        st.write(f"checks_count logic: `{checks_logic}`")

    attempts = parse_info.get("attempts", [])
    if attempts:
        st.write("Parse attempts:")
        st.dataframe(pd.DataFrame(attempts), use_container_width=True)


def save_loaded_report(report_type: str, parsed_df: pd.DataFrame, df_kpi: pd.DataFrame, parse_info: dict, mapping: dict):
    st.session_state[report_type] = {
        "parsed_df": parsed_df,
        "df_kpi": df_kpi,
        "parse_info": parse_info,
        "mapping": mapping,
    }


def get_loaded_report(report_type: str):
    return st.session_state.get(report_type)


def download_clean_df(report_type: str, df_kpi: pd.DataFrame):
    csv_data = df_kpi.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label=f"Download {report_type}_clean.csv",
        data=csv_data,
        file_name=f"{report_type}_clean.csv",
        mime="text/csv",
        key=f"download_{report_type}",
    )



tab_upload, tab_waiters, tab_revenue, tab_food, tab_diag = st.tabs(
    ["Upload & Preview", "Waiters", "Revenue by day", "Food usage", "Diagnostics"]
)

with tab_upload:
    uploaded_files = st.file_uploader(
        "Файл отчета",
        type=["csv", "xlsx"],
        accept_multiple_files=True,
        key="main_uploader",
    )

    if uploaded_files:
        processed_previews = []
        batch_types = {}

        for uploaded_file in uploaded_files:
            if is_kitchen_bar_section_report_filename(uploaded_file.name):
                processed_dir = Path(__file__).resolve().parents[1] / "data" / "processed"
                processed_dir.mkdir(parents=True, exist_ok=True)
                target_path = processed_dir / "kitchen_bar_by_station.csv"
                target_path.write_bytes(uploaded_file.getvalue())
                st.success(
                    f"{uploaded_file.name}: сохранен как kitchen_bar_by_station.csv "
                    "для секционного kitchen/bar parser (в обход generic strict CSV loader)."
                )
                continue

            parsed_df, parse_info, load_error = load_file(uploaded_file)
            if load_error or parsed_df is None:
                st.error(f"{uploaded_file.name}: {load_error or 'Не удалось загрузить файл.'}")
                continue

            report_type = detect_report_type(parsed_df)
            mapping = build_mapping(parsed_df, report_type)



            if report_type in batch_types:
                st.warning(
                    f"{uploaded_file.name}: report_type '{report_type}' уже был загружен в этом батче "
                    f"({batch_types[report_type]}). Последний файл заменит предыдущий."
                )
            batch_types[report_type] = uploaded_file.name

            df_kpi = prepare_kpi_df(parsed_df, mapping, report_type)
            save_loaded_report(report_type, parsed_df, df_kpi, parse_info, mapping)
            st.success(f"{uploaded_file.name}: Saved report: {report_type} ({parsed_df.shape[0]}, {parsed_df.shape[1]})")
            processed_previews.append((uploaded_file.name, report_type, parsed_df))

        if processed_previews:
            st.subheader("Preview данных")
            for file_name, report_type, parsed_df in processed_previews:
                with st.expander(f"{file_name} → {report_type}", expanded=False):
                    preview_df = parsed_df.head(50).copy().where(pd.notna(parsed_df.head(50)), "")
                    if format_money_preview:
                        preview_df = format_money_columns_for_display(preview_df)
                    st.dataframe(preview_df, use_container_width=True)
    else:
        loaded = [k for k in ["waiters", "revenue_by_day", "food_usage"] if k in st.session_state]
        if loaded:
            st.info(f"Уже загружены отчёты: {', '.join(loaded)}")
        else:
            st.info("Загрузите файл, чтобы сохранить отчёт в сессии.")

with tab_waiters:
    data = get_loaded_report("waiters")
    if data is None:
        st.info("Upload a Waiters report first (официанты/чеки).")
    else:
        checks_logic = render_waiters(data["df_kpi"], diagnostics_mode)
        st.session_state["waiters_checks_logic"] = checks_logic
        download_clean_df("waiters", data["df_kpi"])

with tab_revenue:
    data = get_loaded_report("revenue_by_day")
    if data is None:
        st.info("Upload a Revenue by day report first (дата/выручка).")
    else:
        render_revenue_by_day(data["df_kpi"])
        download_clean_df("revenue_by_day", data["df_kpi"])

with tab_food:
    data = get_loaded_report("food_usage")
    if data is None:
        st.info("Upload a Food usage report first (блюда/количество).")
    else:
        render_food_usage(data["df_kpi"])
        download_clean_df("food_usage", data["df_kpi"])

with tab_diag:
    if not diagnostics_mode:
        st.info("Enable 'Diagnostics mode' in sidebar to inspect loaded reports.")
    else:
        loaded_keys = [k for k in ["waiters", "revenue_by_day", "food_usage"] if k in st.session_state]
        if not loaded_keys:
            st.info("Нет загруженных отчётов для диагностики.")
        else:
            selected = st.selectbox("Report to inspect", loaded_keys)
            selected_data = st.session_state[selected]
            checks_logic = st.session_state.get(f"{selected}_checks_logic", "n/a")
            show_diagnostics(
                selected_data["parse_info"],
                selected,
                selected_data["parsed_df"],
                selected_data["mapping"],
                selected_data["df_kpi"],
                checks_logic,
            )
