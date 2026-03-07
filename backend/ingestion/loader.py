import re
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.utils.normalize import (
    is_blank_series,
    normalize_col_name,
    normalize_number_series,
    normalize_text,
)


def find_header_row_index(decoded_text: str, delimiter: str) -> Optional[int]:
    keywords = [
        "официант", "номер", "чек", "сумма", "гостей",
        "дата", "выручка", "блюдо", "количество", "расход", "итого",
    ]
    lines = decoded_text.splitlines()
    for idx, line in enumerate(lines):
        if line.count(delimiter) < 3:
            continue
        norm_line = normalize_text(line)
        hits = sum(1 for kw in keywords if kw in norm_line)
        if hits >= 2:
            return idx
    return None


def parse_csv_bytes(raw: bytes) -> Tuple[Optional[pd.DataFrame], Dict, Optional[str]]:
    attempts: List[Dict] = []
    chosen_encoding = None
    chosen_delimiter = None
    chosen_header_index = None
    last_error = None

    for encoding in ["cp1251", "utf-8", "latin1"]:
        try:
            decoded = raw.decode(encoding)
        except Exception as err:
            attempts.append(
                {
                    "encoding": encoding,
                    "delimiter": None,
                    "header_row_index": None,
                    "status": "decode_failed",
                    "error": str(err),
                }
            )
            last_error = err
            continue

        lines = decoded.splitlines()
        for delimiter in [";", ","]:
            header_idx = find_header_row_index(decoded, delimiter)
            if header_idx is None:
                attempts.append(
                    {
                        "encoding": encoding,
                        "delimiter": delimiter,
                        "header_row_index": None,
                        "status": "header_not_found",
                        "error": "",
                    }
                )
                continue

            try:
                # Read with fixed names to avoid losing rows when field count varies.
                raw_df = pd.read_csv(
                    BytesIO(raw),
                    sep=delimiter,
                    encoding=encoding,
                    skiprows=header_idx,
                    engine="python",
                    on_bad_lines="skip",
                    header=None,
                    names=list(range(80)),
                    dtype=str,
                )
                if raw_df.empty:
                    raise ValueError("Пустой DataFrame после чтения CSV")

                # Header is first parsed line after skiprows.
                header_row = raw_df.iloc[0].fillna("").astype(str).tolist()
                columns = []
                for i, v in enumerate(header_row):
                    col = str(v).replace("\u00A0", " ").replace("\u202F", " ").strip()
                    if not col:
                        col = f"Unnamed: {i}"
                    columns.append(col)

                df = raw_df.iloc[1:].copy()
                df.columns = columns

                # Column name normalization.
                df.columns = [
                    re.sub(r"\s+", " ", str(c).replace("\u00A0", " ").replace("\u202F", " ").strip())
                    for c in df.columns
                ]

                # Drop fully empty rows.
                df = df.replace(r"^\s*$", pd.NA, regex=True).replace({"nan": pd.NA, "NaN": pd.NA, "none": pd.NA, "None": pd.NA}).dropna(how="all")

                # Drop garbage columns only when they are fully empty.
                drop_cols = []
                for c in df.columns:
                    c_norm = normalize_col_name(c)
                    is_unnamed = c_norm.startswith("unnamed:") or c_norm == ""
                    if not is_unnamed:
                        continue
                    col_blank = is_blank_series(df[c]).all()
                    if col_blank:
                        drop_cols.append(c)
                if drop_cols:
                    df = df.drop(columns=drop_cols, errors="ignore")

                df = df.reset_index(drop=True)
                if df.shape[1] < 2:
                    raise ValueError("Слишком мало колонок после очистки")

                chosen_encoding = encoding
                chosen_delimiter = delimiter
                chosen_header_index = header_idx

                attempts.append(
                    {
                        "encoding": encoding,
                        "delimiter": delimiter,
                        "header_row_index": header_idx,
                        "status": "success",
                        "error": "",
                    }
                )

                info = {
                    "encoding": chosen_encoding,
                    "delimiter": chosen_delimiter,
                    "header_row_index": chosen_header_index,
                    "attempts": attempts,
                    "decoded_preview": lines[:20],
                }
                return df, info, None
            except Exception as err:
                last_error = err
                attempts.append(
                    {
                        "encoding": encoding,
                        "delimiter": delimiter,
                        "header_row_index": header_idx,
                        "status": "parse_failed",
                        "error": str(err),
                    }
                )

    info = {
        "encoding": chosen_encoding,
        "delimiter": chosen_delimiter,
        "header_row_index": chosen_header_index,
        "attempts": attempts,
    }
    return None, info, f"Не удалось распарсить CSV: {last_error}"


def find_column_by_priority(df: pd.DataFrame, priorities: List[str]) -> Optional[str]:
    normalized = {c: normalize_col_name(c) for c in df.columns}
    for needle in priorities:
        needle_norm = normalize_col_name(needle)
        exact = [c for c, n in normalized.items() if n == needle_norm]
        if exact:
            return exact[0]
        contains = [c for c, n in normalized.items() if needle_norm in n]
        if contains:
            return contains[0]
    return None


def numeric_candidate_score(series: pd.Series) -> Tuple[int, int, float]:
    s = series.astype(str)
    s = s.str.replace("\u00A0", "", regex=False)
    s = s.str.replace("\u202F", "", regex=False)
    s = s.str.replace(r"\s+", "", regex=True)
    s = s.str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^0-9\.\-]", "", regex=True)
    numeric = pd.to_numeric(s, errors="coerce")
    valid_count = int(numeric.notna().sum())
    nonzero_count = int((numeric.fillna(0) > 0).sum())
    total_sum = float(numeric.fillna(0).sum())
    return nonzero_count, valid_count, total_sum


def choose_revenue_source(df: pd.DataFrame, priorities: List[str]) -> Optional[str]:
    normalized = {c: normalize_col_name(c) for c in df.columns}
    candidates: List[str] = []

    for needle in priorities:
        needle_norm = normalize_col_name(needle)
        exact = [c for c, n in normalized.items() if n == needle_norm]
        contains = [c for c, n in normalized.items() if needle_norm in n]
        for c in exact + contains:
            if c not in candidates:
                candidates.append(c)

    # Backup money-like candidates if priority match is weak/empty.
    for c, n in normalized.items():
        if any(k in n for k in ["сум", "выруч", "итого", "revenue", "оплач"]):
            if c not in candidates:
                candidates.append(c)

    if not candidates:
        return None

    # Respect priority when it has meaningful numeric payload.
    for c in candidates:
        nonzero_count, valid_count, total_sum = numeric_candidate_score(df[c])
        if nonzero_count > 0 and valid_count > 10 and total_sum > 0:
            return c

    # Fallback: choose strongest numeric candidate.
    best = max(candidates, key=lambda c: numeric_candidate_score(df[c]))
    return best


def looks_like_id_column(series: pd.Series) -> bool:
    numeric = normalize_number_series(series)
    valid = numeric[numeric > 0]
    if valid.empty:
        return False
    uniq_ratio = valid.nunique(dropna=True) / max(len(valid), 1)
    median = float(valid.median())
    max_val = float(valid.max())
    return uniq_ratio > 0.95 and median > 100000 and max_val > 1000000


def find_best_revenue_fallback(df: pd.DataFrame) -> Optional[str]:
    excluded_name_parts = [
        "дата", "date", "код", "чек", "check", "гостей", "гости", "кол-во",
        "количество", "quantity", "waiter", "официант", "валюта",
    ]
    candidates: List[str] = []
    for c in df.columns:
        n = normalize_col_name(c)
        if any(part in n for part in excluded_name_parts):
            continue
        candidates.append(c)

    if not candidates:
        return None

    scored: List[Tuple[Tuple[int, int, float, float], str]] = []
    for c in candidates:
        numeric = normalize_number_series(df[c])
        nonzero = int((numeric > 0).sum())
        valid = int(numeric.notna().sum())
        total = float(numeric.sum())
        if nonzero < 5 or total <= 0:
            continue
        if looks_like_id_column(df[c]):
            continue
        # Prefer columns with larger sums, then more non-zero rows.
        score = (1, nonzero, total, float(numeric.max()))
        scored.append((score, c))

    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def detect_report_type(df: pd.DataFrame) -> str:
    ncols = [normalize_col_name(c) for c in df.columns]
    has_waiter = any("официант" in c or "waiter" in c for c in ncols)
    has_check = any("номер чека" in c or c == "чек" for c in ncols)
    has_date = any(c == "дата" or " date" in f" {c}" for c in ncols)
    has_revenue = any(("сумма" in c) or ("выручка" in c) or ("итого" in c) for c in ncols)
    has_dish = any(("блюдо" in c) or ("наименование" in c) or ("товар" in c) or ("позиция" in c) for c in ncols)
    has_qty = any(("количество" in c) or ("кол-во" in c) or (c == "расход") for c in ncols)

    if has_waiter and has_check:
        return "waiters"
    if has_date and has_revenue:
        return "revenue_by_day"
    if has_dish and has_qty:
        return "food_usage"
    return "unknown"


def build_mapping(df: pd.DataFrame, report_type: str) -> Dict[str, Optional[str]]:
    common = {
        "revenue": choose_revenue_source(df, ["сумма", "выручка", "итого", "revenue", "сум/чек"]),
    }

    # Revenue safeguard: if mapped source is empty/zero, fallback to strongest numeric money-like column.
    selected_revenue = common.get("revenue")
    if selected_revenue and selected_revenue in df.columns:
        selected_revenue_num = normalize_number_series(df[selected_revenue])
        if float(selected_revenue_num.sum()) <= 0 or int((selected_revenue_num > 0).sum()) < 5:
            fallback_revenue = find_best_revenue_fallback(df)
            if fallback_revenue:
                common["revenue_auto_fallback"] = fallback_revenue
                common["revenue"] = fallback_revenue
    elif not selected_revenue:
        fallback_revenue = find_best_revenue_fallback(df)
        if fallback_revenue:
            common["revenue_auto_fallback"] = fallback_revenue
            common["revenue"] = fallback_revenue

    if report_type == "waiters":
        common.update(
            {
                "waiter": find_column_by_priority(df, ["официант", "waiter"]),
                "check_id": find_column_by_priority(df, ["номер чека", "чек", "check_id"]),
                "guests": find_column_by_priority(df, ["гостей", "гости", "guests"]),
            }
        )
    elif report_type == "revenue_by_day":
        common.update(
            {
                "date": find_column_by_priority(df, ["дата", "date"]),
                "checks_count": find_column_by_priority(df, ["чеков", "кол-во чеков", "checks"]),
                "cashbox": find_column_by_priority(df, ["касса место", "касса", "station"]),
                "payment_type": find_column_by_priority(df, ["код", "валюта", "payment", "type"]),
            }
        )
    elif report_type == "food_usage":
        common.update(
            {
                "dish": find_column_by_priority(df, ["блюдо", "наименование", "товар", "позиция", "dish"]),
                "quantity": find_column_by_priority(df, ["количество", "кол-во", "qty", "quantity"]),
                "station": find_column_by_priority(df, ["цех", "станция", "подразделение", "категория", "группа"]),
            }
        )
    return common


def apply_canonical_mapping(parsed_df: pd.DataFrame, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    df_kpi = parsed_df.copy()
    for canonical, source in mapping.items():
        if not source or source not in df_kpi.columns:
            continue
        # Keep original source columns to avoid collisions when one source
        # is reused by several canonical targets (e.g. revenue + payment_type).
        df_kpi[canonical] = parsed_df[source]
    return df_kpi


def clean_waiters_rows(df_kpi: pd.DataFrame) -> pd.DataFrame:
    if "waiter" not in df_kpi.columns:
        return df_kpi

    waiter = df_kpi["waiter"].astype(str).str.strip()
    norm_waiter = waiter.str.lower()
    bad_pattern = r"(отчет|наименование|дата|официант|итого|страниц|page)"
    mask_valid = (
        waiter.ne("")
        & norm_waiter.ne("nan")
        & norm_waiter.ne("none")
        & ~norm_waiter.str.contains(bad_pattern, na=False)
    )
    return df_kpi[mask_valid].reset_index(drop=True)


def prepare_kpi_df(parsed_df: pd.DataFrame, mapping: Dict[str, Optional[str]], report_type: str) -> pd.DataFrame:
    df_kpi = apply_canonical_mapping(parsed_df, mapping)

    if "revenue" in df_kpi.columns:
        df_kpi["revenue"] = normalize_number_series(df_kpi["revenue"])
    if "guests" in df_kpi.columns:
        df_kpi["guests"] = normalize_number_series(df_kpi["guests"])
    if "quantity" in df_kpi.columns:
        df_kpi["quantity"] = normalize_number_series(df_kpi["quantity"])
    if "checks_count" in df_kpi.columns:
        df_kpi["checks_count"] = normalize_number_series(df_kpi["checks_count"])
    if "cashbox" in df_kpi.columns:
        df_kpi["cashbox"] = df_kpi["cashbox"].astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA, "none": pd.NA})
    if "payment_type" in df_kpi.columns:
        df_kpi["payment_type"] = df_kpi["payment_type"].astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA, "none": pd.NA})

    if "check_id" in df_kpi.columns:
        cid = df_kpi["check_id"].astype(str).str.strip()
        cid = cid.str.replace(r"\.0$", "", regex=True)
        cid = cid.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
        df_kpi["check_id"] = cid

    if report_type == "waiters":
        df_kpi = clean_waiters_rows(df_kpi)

    return df_kpi


def load_file(uploaded_file) -> Tuple[Optional[pd.DataFrame], Dict, Optional[str]]:
    name = uploaded_file.name.lower()
    if name.endswith(".xlsx"):
        try:
            df = pd.read_excel(uploaded_file)
            df.columns = [
                re.sub(r"\s+", " ", str(c).replace("\u00A0", " ").replace("\u202F", " ").strip())
                for c in df.columns
            ]
            df = df.dropna(how="all").reset_index(drop=True)
            info = {"encoding": None, "delimiter": None, "header_row_index": None, "attempts": []}
            return df, info, None
        except Exception as err:
            return None, {"attempts": []}, f"Ошибка чтения XLSX: {err}"

    if name.endswith(".csv"):
        return parse_csv_bytes(uploaded_file.getvalue())

    return None, {"attempts": []}, "Поддерживаются только CSV и XLSX."
