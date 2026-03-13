import re
import csv
from html import unescape
from html.parser import HTMLParser
from io import BytesIO
from io import StringIO
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backend.utils.normalize import (
    is_blank_series,
    normalize_col_name,
    normalize_number_series,
    normalize_text,
)


CSV_ENCODINGS = ["cp1251", "utf-8-sig", "utf-8", "latin1"]
CSV_DELIMITERS = [";", ",", "\t"]
HEADER_KEYWORDS = [
    "официант", "номер", "чек", "гостей", "дата", "выручка", "сумма",
    "итого", "блюдо", "наименование", "товар", "кол-во", "количество",
    "расход", "оплачено", "касса", "валюта", "код",
    "date", "revenue", "amount", "paid", "check", "dish", "quantity", "code",
]
SERVICE_ROW_HINTS = [
    "отчет",
    "наименование ресторана",
    "дата:",
    "расход блюд",
    "выручка станций по дням",
]


def _clean_cell(value: Any) -> str:
    return str(value).replace("\u00A0", " ").replace("\u202F", " ").strip()


def _trim_trailing_blank(row: List[str]) -> List[str]:
    out = list(row)
    while out and _clean_cell(out[-1]) == "":
        out.pop()
    return out


def _has_meaningful_cells(row: List[str]) -> bool:
    return any(_clean_cell(c) != "" for c in row)


def _is_numeric_like(token: str) -> bool:
    t = _clean_cell(token)
    if not t:
        return False
    t = t.replace(" ", "").replace(",", ".")
    t = re.sub(r"[^0-9\.\-]", "", t)
    if t in {"", "-", ".", "-."}:
        return False
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", t))


def _is_date_like(token: str) -> bool:
    t = _clean_cell(token)
    if not t:
        return False
    dt = pd.to_datetime(pd.Series([t]), errors="coerce", dayfirst=True).iloc[0]
    return pd.notna(dt)


def _non_empty_tokens_from_row(row: pd.Series) -> List[str]:
    vals: List[str] = []
    for v in row.tolist():
        c = _clean_cell(v)
        if c and c.lower() not in {"nan", "none", "<na>"}:
            vals.append(c)
    return vals


def reconstruct_waiters_layout(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Dict[str, Any], Optional[str]]:
    cols = ["Официант", "Номер Чека", "Чеков", "Гостей", "Сумма", "Сум/чек", "Сум/гост", "Сум/гост/час"]
    out_rows: List[Dict[str, Any]] = []
    invalid_rows = 0

    for _, row in df.iterrows():
        vals = _non_empty_tokens_from_row(row)
        if not vals:
            continue
        first_norm = normalize_text(vals[0])
        if "итого" in first_norm or "отчет" in first_norm:
            continue
        if len(vals) < 8:
            invalid_rows += 1
            continue

        mapped = vals[:8]
        if not _is_numeric_like(mapped[2]) or not _is_numeric_like(mapped[3]):
            invalid_rows += 1
            continue
        if not all(_is_numeric_like(x) for x in mapped[4:8]):
            invalid_rows += 1
            continue
        out_rows.append(dict(zip(cols, mapped)))

    total = len(out_rows) + invalid_rows
    if not out_rows:
        return None, {"rows_total": total, "rows_invalid": invalid_rows}, "Не удалось реконструировать waiters: нет валидных строк."
    if total > 0 and (invalid_rows / total) > 0.35:
        return None, {"rows_total": total, "rows_invalid": invalid_rows}, "Небезопасная реконструкция waiters: слишком много невыравниваемых строк."
    return pd.DataFrame(out_rows), {"rows_total": total, "rows_invalid": invalid_rows}, None


def reconstruct_food_usage_layout(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Dict[str, Any], Optional[str]]:
    cols = ["Код", "Блюдо", "Кол-во", "Сумма", "Скидка", "Оплачено"]
    out_rows: List[Dict[str, Any]] = []
    invalid_rows = 0
    split_code_name_used = 0

    for _, row in df.iterrows():
        vals = _non_empty_tokens_from_row(row)
        if not vals:
            continue
        if "итого" in normalize_text(vals[0]) or "расход блюд" in normalize_text(vals[0]):
            continue

        code = ""
        dish = ""
        rest: List[str] = []

        m = re.match(r"^(\d{2,})(.+)$", vals[0])
        if m:
            code = m.group(1).strip()
            dish = m.group(2).strip()
            rest = vals[1:]
            split_code_name_used += 1
        elif len(vals) >= 6:
            code = vals[0]
            dish = vals[1]
            rest = vals[2:]
        else:
            invalid_rows += 1
            continue

        if not code or not dish or len(rest) < 3:
            invalid_rows += 1
            continue

        qty = rest[0]
        amount = rest[1]
        discount = ""
        paid = ""
        if len(rest) >= 4:
            discount = rest[2]
            paid = rest[3]
        else:
            paid = rest[2]

        if not _is_numeric_like(qty) or not _is_numeric_like(amount) or not _is_numeric_like(paid):
            invalid_rows += 1
            continue
        if discount and not _is_numeric_like(discount):
            invalid_rows += 1
            continue

        out_rows.append(
            {
                "Код": code,
                "Блюдо": dish,
                "Кол-во": qty,
                "Сумма": amount,
                "Скидка": discount,
                "Оплачено": paid,
            }
        )

    total = len(out_rows) + invalid_rows
    if not out_rows:
        return None, {"rows_total": total, "rows_invalid": invalid_rows}, "Не удалось реконструировать food_usage: нет валидных строк."
    if total > 0 and (invalid_rows / total) > 0.35:
        return None, {"rows_total": total, "rows_invalid": invalid_rows}, "Небезопасная реконструкция food_usage: слишком много невыравниваемых строк."
    return (
        pd.DataFrame(out_rows),
        {"rows_total": total, "rows_invalid": invalid_rows, "split_code_name_used": split_code_name_used},
        None,
    )


def reconstruct_revenue_by_day_layout(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Dict[str, Any], Optional[str]]:
    cols = ["Дата", "Код", "Касса Место", "Станция", "Итого"]
    out_rows: List[Dict[str, Any]] = []
    invalid_rows = 0
    cashbox_from_header_count = 0
    station_from_dynamic_count = 0

    raw_cols = list(df.columns)
    norm_cols = [normalize_col_name(c) for c in raw_cols]

    def find_col_idx(candidates: List[str]) -> Optional[int]:
        for needle in candidates:
            n = normalize_col_name(needle)
            for i, c in enumerate(norm_cols):
                if c == n:
                    return i
            for i, c in enumerate(norm_cols):
                if n in c:
                    return i
        return None

    date_idx = find_col_idx(["дата", "date"])
    code_idx = find_col_idx(["код", "code"])
    cashbox_idx = find_col_idx(["касса место", "касса", "cashbox"])
    total_idx = find_col_idx(["итого", "total"])

    core_names = {"дата", "код", "валюта", "касса место", "итого", "date", "code", "currency", "cashbox", "total"}
    station_header_idx = None
    for i, c in enumerate(norm_cols):
        if c.startswith("unnamed:") or c == "":
            continue
        if c in core_names:
            continue
        if any(x in c for x in ["отчет", "наименование", "дата:", "выручка станций"]):
            continue
        station_header_idx = i
        break

    def first_non_empty(row_vals: List[str], indices: List[int], used: set) -> Tuple[str, Optional[int]]:
        for idx in indices:
            if idx < 0 or idx >= len(row_vals) or idx in used:
                continue
            val = _clean_cell(row_vals[idx])
            if val:
                return val, idx
        return "", None

    def first_numeric(row_vals: List[str], indices: List[int], used: set) -> Tuple[str, Optional[int]]:
        for idx in indices:
            if idx < 0 or idx >= len(row_vals) or idx in used:
                continue
            val = _clean_cell(row_vals[idx])
            if _is_numeric_like(val):
                return val, idx
        return "", None

    for _, row in df.iterrows():
        row_vals = [_clean_cell(v) for v in row.tolist()]
        vals = [v for v in row_vals if v and v.lower() not in {"nan", "none", "<na>"}]
        if not vals:
            continue
        if "итого" in normalize_text(vals[0]) or "выручка станций по дням" in normalize_text(vals[0]):
            continue

        used_positions: set = set()

        date_candidates = []
        if date_idx is not None:
            date_candidates.extend([date_idx, date_idx + 1, date_idx - 1])
        date_candidates.extend([0, 1, 2])
        date_val, date_pos = first_non_empty(row_vals, date_candidates, used_positions)
        if date_pos is not None:
            used_positions.add(date_pos)
        if not _is_date_like(date_val):
            invalid_rows += 1
            continue

        code_candidates = []
        if code_idx is not None:
            code_candidates.extend([code_idx, code_idx + 1, code_idx - 1])
        code_candidates.extend([1, 2, 3, 4])
        code_val, code_pos = first_non_empty(row_vals, code_candidates, used_positions)
        if code_pos is not None:
            used_positions.add(code_pos)
        if not code_val:
            invalid_rows += 1
            continue

        total_candidates = []
        if total_idx is not None:
            total_candidates.extend([total_idx, total_idx + 1, total_idx - 1, total_idx + 2])
        total_candidates.extend([len(row_vals) - 1, len(row_vals) - 2, len(row_vals) - 3])
        total_val, total_pos = first_numeric(row_vals, total_candidates, used_positions)
        if total_pos is not None:
            used_positions.add(total_pos)
        if not total_val:
            invalid_rows += 1
            continue

        cashbox_candidates = []
        if cashbox_idx is not None:
            cashbox_candidates.extend([cashbox_idx, cashbox_idx + 1, cashbox_idx - 1, cashbox_idx + 2])
        if total_pos is not None:
            cashbox_candidates.extend([total_pos - 1, total_pos - 2])
        cashbox_val, cashbox_pos = first_numeric(row_vals, cashbox_candidates, used_positions)
        if cashbox_pos is not None:
            used_positions.add(cashbox_pos)
            if cashbox_idx is not None and cashbox_pos in {cashbox_idx, cashbox_idx + 1, cashbox_idx - 1}:
                cashbox_from_header_count += 1
        if not cashbox_val:
            invalid_rows += 1
            continue

        station_candidates = []
        if station_header_idx is not None:
            station_candidates.extend([station_header_idx, station_header_idx + 1, station_header_idx + 2, station_header_idx - 1])
        if cashbox_pos is not None:
            station_candidates.extend([cashbox_pos + 1, cashbox_pos - 1, cashbox_pos + 2])
        station_val, station_pos = first_numeric(row_vals, station_candidates, used_positions)
        if station_pos is not None:
            used_positions.add(station_pos)
            station_from_dynamic_count += 1

        out_rows.append(
            {
                "Дата": date_val,
                "Код": code_val,
                "Касса Место": cashbox_val,
                "Станция": station_val,
                "Итого": total_val,
            }
        )

    total = len(out_rows) + invalid_rows
    if not out_rows:
        return None, {"rows_total": total, "rows_invalid": invalid_rows}, "Не удалось реконструировать revenue_by_day: нет валидных строк."
    if total > 0 and (invalid_rows / total) > 0.35:
        return None, {"rows_total": total, "rows_invalid": invalid_rows}, "Небезопасная реконструкция revenue_by_day: слишком много невыравниваемых строк."
    return (
        pd.DataFrame(out_rows),
        {
            "rows_total": total,
            "rows_invalid": invalid_rows,
            "cashbox_from_header_count": cashbox_from_header_count,
            "station_from_dynamic_count": station_from_dynamic_count,
        },
        None,
    )


def apply_sparse_alignment(df: pd.DataFrame, detected_type: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any], Optional[str]]:
    norm_cols = [normalize_col_name(c) for c in df.columns]
    is_sparse = any(c.startswith("unnamed:") or c == "" for c in norm_cols)
    if not is_sparse:
        return df, {"applied": False, "rule": "not_sparse"}, None

    if detected_type == "waiters":
        aligned, stats, err = reconstruct_waiters_layout(df)
        return aligned, {"applied": True, "rule": "waiters", **stats}, err
    if detected_type == "food_usage":
        aligned, stats, err = reconstruct_food_usage_layout(df)
        return aligned, {"applied": True, "rule": "food_usage", **stats}, err
    if detected_type == "revenue_by_day":
        aligned, stats, err = reconstruct_revenue_by_day_layout(df)
        return aligned, {"applied": True, "rule": "revenue_by_day", **stats}, err
    return df, {"applied": False, "rule": "none"}, None


def scan_csv_rows(decoded_text: str, delimiter: str) -> List[List[str]]:
    reader = csv.reader(StringIO(decoded_text), delimiter=delimiter, quotechar='"')
    rows: List[List[str]] = []
    for row in reader:
        rows.append([_clean_cell(c) for c in row])
    return rows


def detect_csv_format(raw: bytes) -> Dict[str, Any]:
    attempts: List[Dict[str, Any]] = []
    best: Optional[Dict[str, Any]] = None

    for encoding in CSV_ENCODINGS:
        try:
            decoded = raw.decode(encoding)
        except Exception as err:
            attempts.append(
                {
                    "encoding": encoding,
                    "delimiter": None,
                    "status": "decode_failed",
                    "error": str(err),
                }
            )
            continue

        for delimiter in CSV_DELIMITERS:
            try:
                rows = scan_csv_rows(decoded, delimiter)
            except Exception as err:
                attempts.append(
                    {
                        "encoding": encoding,
                        "delimiter": delimiter,
                        "status": "scan_failed",
                        "error": str(err),
                    }
                )
                continue

            non_empty_rows = [_trim_trailing_blank(r) for r in rows if _has_meaningful_cells(r)]
            widths = [len(r) for r in non_empty_rows]
            mode_width = Counter(widths).most_common(1)[0][0] if widths else 0
            width_consistency = (
                sum(1 for w in widths if w == mode_width) / len(widths)
                if widths else 0.0
            )
            multi_col_ratio = (
                sum(1 for w in widths if w >= 3) / len(widths)
                if widths else 0.0
            )

            header_signals = 0
            for row in non_empty_rows[:30]:
                norm_line = normalize_text(" ".join(row))
                if sum(1 for kw in HEADER_KEYWORDS if kw in norm_line) >= 2:
                    header_signals += 1

            score = (
                (25.0 if mode_width >= 3 else 0.0)
                + (width_consistency * 35.0)
                + (multi_col_ratio * 80.0)
                + (10.0 if header_signals > 0 else 0.0)
                + min(len(non_empty_rows), 50) / 10.0
            )

            attempt = {
                "encoding": encoding,
                "delimiter": delimiter,
                "status": "scanned",
                "error": "",
                "mode_width": mode_width,
                "width_consistency": round(width_consistency, 4),
                "multi_col_ratio": round(multi_col_ratio, 4),
                "header_signals": header_signals,
                "score": round(score, 2),
            }
            attempts.append(attempt)

            if best is None or score > best["score"]:
                best = {
                    "encoding": encoding,
                    "delimiter": delimiter,
                    "decoded_text": decoded,
                    "rows": rows,
                    "mode_width": mode_width,
                    "width_consistency": width_consistency,
                    "score": score,
                }

    return {"best": best, "attempts": attempts}


def find_header_row(rows: List[List[str]], max_scan_rows: int = 40) -> Optional[int]:
    best_idx: Optional[int] = None
    best_score = float("-inf")

    for idx, row in enumerate(rows[:max_scan_rows]):
        row_trim = _trim_trailing_blank(row)
        if not _has_meaningful_cells(row_trim):
            continue

        non_empty = [_clean_cell(c) for c in row_trim if _clean_cell(c) != ""]
        if len(non_empty) < 3:
            continue

        norm_cells = [normalize_text(c) for c in non_empty]
        keyword_hits = sum(1 for kw in HEADER_KEYWORDS if any(kw in c for c in norm_cells))
        service_hits = sum(1 for hint in SERVICE_ROW_HINTS if any(hint in c for c in norm_cells))
        text_like = sum(1 for c in non_empty if not _is_numeric_like(c))
        numeric_like = sum(1 for c in non_empty if _is_numeric_like(c))

        score = (keyword_hits * 5) + text_like - (numeric_like * 2) - (service_hits * 3)
        if score > best_score:
            best_score = score
            best_idx = idx

    if best_idx is None or best_score < 6:
        return None
    return best_idx


def merge_header_rows(header_row: List[str], next_row: List[str]) -> Tuple[List[str], bool]:
    head = _trim_trailing_blank(header_row)
    nxt = _trim_trailing_blank(next_row)
    if not head or not nxt:
        return head, False

    non_empty_next = [(i, _clean_cell(v)) for i, v in enumerate(nxt) if _clean_cell(v) != ""]
    if not non_empty_next:
        return head, False

    # Conservative merge: only tiny continuation fragments like "с" in split "Сум/гост/ча" + "с".
    if len(non_empty_next) > 2 or any(len(v) > 5 for _, v in non_empty_next):
        return head, False

    merged = list(head)
    max_len = max(len(merged), len(nxt))
    if len(merged) < max_len:
        merged.extend([""] * (max_len - len(merged)))

    for i, value in non_empty_next:
        base = _clean_cell(merged[i]) if i < len(merged) else ""
        if base:
            merged[i] = f"{base}{value}"
        else:
            merged[i] = value
    return _trim_trailing_blank(merged), True


def validate_row_widths(
    rows: List[List[str]],
    data_start_idx: int,
    expected_width: int,
    max_rows: int = 300,
) -> Dict[str, Any]:
    sample_rows = []
    for r in rows[data_start_idx:data_start_idx + max_rows]:
        rr = _trim_trailing_blank(r)
        if not _has_meaningful_cells(rr):
            continue
        # Skip obvious service/footer rows to avoid false width alarms.
        joined = normalize_text(" ".join(rr))
        if any(h in joined for h in SERVICE_ROW_HINTS):
            continue
        sample_rows.append(rr)

    if not sample_rows:
        return {"ok": False, "reason": "После заголовка нет данных.", "sample_size": 0}

    widths = [len(r) for r in sample_rows]
    mode_width = Counter(widths).most_common(1)[0][0]
    effective_width = max(expected_width, mode_width)

    severe = [w for w in widths if abs(w - effective_width) > 2]
    off = [w for w in widths if abs(w - effective_width) > 1]
    off_ratio = len(off) / len(widths)
    severe_ratio = len(severe) / len(widths)

    if severe_ratio > 0.05:
        return {
            "ok": False,
            "reason": (
                "Небезопасная структура CSV: системно встречаются строки с шириной, "
                f"сильно отличающейся от ожидаемой ({effective_width})."
            ),
            "sample_size": len(widths),
            "off_ratio": round(off_ratio, 4),
            "severe_ratio": round(severe_ratio, 4),
            "mode_width": mode_width,
            "expected_width": expected_width,
            "effective_width": effective_width,
            "min_width": min(widths),
            "max_width": max(widths),
        }

    if off_ratio > 0.25:
        return {
            "ok": False,
            "reason": (
                "Небезопасная структура CSV: слишком много строк со смещенной шириной. "
                "Есть риск сдвига бизнес-колонок (quantity/amount/payment)."
            ),
            "sample_size": len(widths),
            "off_ratio": round(off_ratio, 4),
            "severe_ratio": round(severe_ratio, 4),
            "mode_width": mode_width,
            "expected_width": expected_width,
            "effective_width": effective_width,
            "min_width": min(widths),
            "max_width": max(widths),
        }

    return {
        "ok": True,
        "sample_size": len(widths),
        "off_ratio": round(off_ratio, 4),
        "severe_ratio": round(severe_ratio, 4),
        "mode_width": mode_width,
        "expected_width": expected_width,
        "effective_width": effective_width,
        "min_width": min(widths),
        "max_width": max(widths),
    }


def parse_csv_bytes(raw: bytes) -> Tuple[Optional[pd.DataFrame], Dict, Optional[str]]:
    fmt = detect_csv_format(raw)
    attempts = fmt.get("attempts", [])
    best = fmt.get("best")

    if not best:
        return None, {"attempts": attempts}, "Не удалось определить формат CSV (encoding/delimiter)."

    rows = best["rows"]
    decoded = best["decoded_text"]
    header_idx = find_header_row(rows)
    if header_idx is None:
        info = {
            "encoding": best["encoding"],
            "delimiter": best["delimiter"],
            "header_row_index": None,
            "header_merged": False,
            "row_width_consistent": False,
            "attempts": attempts,
            "suspicious_conditions": ["header_not_found"],
            "decoded_preview": decoded.splitlines()[:20],
        }
        return (
            None,
            info,
            "Не удалось надежно определить строку заголовка. Проверьте экспорт R-Keeper: корректный разделитель и наличие одной явной строки заголовка.",
        )

    header_row = _trim_trailing_blank(rows[header_idx])
    header_merged = False
    header_span = 1
    if header_idx + 1 < len(rows):
        merged, merged_ok = merge_header_rows(header_row, rows[header_idx + 1])
        if merged_ok:
            header_row = merged
            header_merged = True
            header_span = 2

    if len(header_row) < 3:
        info = {
            "encoding": best["encoding"],
            "delimiter": best["delimiter"],
            "header_row_index": header_idx,
            "header_merged": header_merged,
            "row_width_consistent": False,
            "attempts": attempts,
            "suspicious_conditions": ["header_too_short"],
            "decoded_preview": decoded.splitlines()[:20],
        }
        return None, info, "Небезопасный заголовок CSV: слишком мало колонок после определения заголовка."

    width_check = validate_row_widths(rows, header_idx + header_span, expected_width=len(header_row))
    if not width_check.get("ok"):
        info = {
            "encoding": best["encoding"],
            "delimiter": best["delimiter"],
            "header_row_index": header_idx,
            "header_merged": header_merged,
            "row_width_consistent": False,
            "row_width_check": width_check,
            "attempts": attempts,
            "suspicious_conditions": ["row_width_inconsistent"],
            "decoded_preview": decoded.splitlines()[:20],
        }
        guidance = (
            "Рекомендуется экспорт с единым разделителем и стабильной шириной строк, "
            "без поврежденных кавычек и случайных переносов внутри записей."
        )
        return None, info, f"{width_check.get('reason')} {guidance}"

    target_width = int(width_check.get("effective_width", len(header_row)))
    columns: List[str] = []
    seen: Dict[str, int] = {}
    for i in range(target_width):
        v = header_row[i] if i < len(header_row) else ""
        col = re.sub(r"\s+", " ", _clean_cell(v))
        if not col:
            col = f"Unnamed: {i}"
        cnt = seen.get(col, 0) + 1
        seen[col] = cnt
        if cnt > 1:
            col = f"{col}_{cnt}"
        columns.append(col)

    data_rows: List[List[str]] = []
    suspicious_conditions: List[str] = []
    rows_with_extra_cells = 0
    total_non_empty_rows = 0
    for row in rows[header_idx + header_span:]:
        row_trim = _trim_trailing_blank(row)
        if not _has_meaningful_cells(row_trim):
            continue
        joined = normalize_text(" ".join(row_trim))
        if any(h in joined for h in SERVICE_ROW_HINTS):
            continue
        total_non_empty_rows += 1
        if len(row_trim) > len(columns):
            extra = row_trim[len(columns):]
            if any(_clean_cell(v) != "" for v in extra):
                rows_with_extra_cells += 1
                continue
        row_norm = row_trim + [""] * max(0, len(columns) - len(row_trim))
        data_rows.append(row_norm[:len(columns)])

    if total_non_empty_rows == 0:
        return None, {"attempts": attempts}, "CSV прочитан, но после заголовка нет данных."

    extra_ratio = rows_with_extra_cells / total_non_empty_rows
    if rows_with_extra_cells > 0:
        suspicious_conditions.append("data_row_has_extra_cells")
    if rows_with_extra_cells > 5 or extra_ratio > 0.02:
        return (
            None,
            {
                "encoding": best["encoding"],
                "delimiter": best["delimiter"],
                "header_row_index": header_idx,
                "header_merged": header_merged,
                "row_width_consistent": False,
                "row_width_check": width_check,
                "attempts": attempts,
                "rows_with_extra_cells": rows_with_extra_cells,
                "rows_with_extra_cells_ratio": round(extra_ratio, 4),
                "suspicious_conditions": suspicious_conditions,
                "decoded_preview": decoded.splitlines()[:20],
            },
            (
                "Обнаружено слишком много строк с лишними значениями правее заголовка. "
                "Это небезопасно: вероятен системный сдвиг бизнес-колонок."
            ),
        )

    if not data_rows:
        return None, {"attempts": attempts}, "CSV прочитан, но после заголовка нет данных."

    df = pd.DataFrame(data_rows, columns=columns, dtype=str)
    df = (
        df.replace(r"^\s*$", pd.NA, regex=True)
        .replace({"nan": pd.NA, "NaN": pd.NA, "none": pd.NA, "None": pd.NA})
        .dropna(how="all")
        .reset_index(drop=True)
    )

    drop_cols = []
    for c in df.columns:
        c_norm = normalize_col_name(c)
        is_unnamed = c_norm.startswith("unnamed:") or c_norm == ""
        if not is_unnamed:
            continue
        if is_blank_series(_get_series_by_name(df, c)).all():
            drop_cols.append(c)
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")

    if df.shape[1] < 2:
        return None, {"attempts": attempts}, "Слишком мало колонок после безопасной очистки CSV."

    detected_type_raw = detect_report_type(df)
    aligned_df, alignment_diag, alignment_err = apply_sparse_alignment(df, detected_type_raw)
    if alignment_err:
        info = {
            "encoding": best["encoding"],
            "delimiter": best["delimiter"],
            "header_row_index": header_idx,
            "header_merged": header_merged,
            "row_width_consistent": True,
            "row_width_check": width_check,
            "attempts": attempts,
            "rows_with_extra_cells": rows_with_extra_cells,
            "rows_with_extra_cells_ratio": round(extra_ratio, 4),
            "suspicious_conditions": suspicious_conditions + ["sparse_alignment_failed"],
            "decoded_preview": decoded.splitlines()[:20],
            "detected_report_type": detected_type_raw,
            "sparse_alignment": alignment_diag,
        }
        return None, info, alignment_err

    if aligned_df is None or aligned_df.empty:
        return None, {"attempts": attempts}, "После выравнивания sparse CSV не осталось валидных данных."

    df = aligned_df
    detected_type = detect_report_type(df)
    if detected_type == "unknown":
        info = {
            "encoding": best["encoding"],
            "delimiter": best["delimiter"],
            "header_row_index": header_idx,
            "header_merged": header_merged,
            "row_width_consistent": True,
            "row_width_check": width_check,
            "attempts": attempts,
            "suspicious_conditions": suspicious_conditions + ["unknown_report_type"],
            "decoded_preview": decoded.splitlines()[:20],
            "detected_report_type": detected_type,
            "detected_report_type_raw": detected_type_raw,
            "sparse_alignment": alignment_diag,
        }
        return (
            None,
            info,
            "CSV прочитан, но не распознаны обязательные бизнес-колонки (дата/выручка, официант/чек или блюдо/кол-во).",
        )

    info = {
        "encoding": best["encoding"],
        "delimiter": best["delimiter"],
        "header_row_index": header_idx,
        "header_merged": header_merged,
        "row_width_consistent": True,
        "row_width_check": width_check,
        "attempts": attempts,
        "rows_with_extra_cells": rows_with_extra_cells,
        "rows_with_extra_cells_ratio": round(extra_ratio, 4),
        "suspicious_conditions": suspicious_conditions,
        "decoded_preview": decoded.splitlines()[:20],
        "detected_report_type": detected_type,
        "detected_report_type_raw": detected_type_raw,
        "sparse_alignment": alignment_diag,
    }
    return df, info, None


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


def _get_series_by_name(df: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name not in df.columns:
        return pd.Series(index=df.index, dtype="object")
    selected = df[column_name]
    if isinstance(selected, pd.DataFrame):
        if selected.shape[1] == 0:
            return pd.Series(index=df.index, dtype="object")
        return selected.iloc[:, 0]
    return selected


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
        nonzero_count, valid_count, total_sum = numeric_candidate_score(_get_series_by_name(df, c))
        if nonzero_count > 0 and valid_count > 10 and total_sum > 0:
            return c

    # Fallback: choose strongest numeric candidate.
    best = max(candidates, key=lambda c: numeric_candidate_score(_get_series_by_name(df, c)))
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
        series = _get_series_by_name(df, c)
        numeric = normalize_number_series(series)
        nonzero = int((numeric > 0).sum())
        valid = int(numeric.notna().sum())
        total = float(numeric.sum())
        if nonzero < 5 or total <= 0:
            continue
        if looks_like_id_column(series):
            continue
        # Prefer columns with larger sums, then more non-zero rows.
        score = (1, nonzero, total, float(numeric.max()))
        scored.append((score, c))

    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def detect_report_type(df: pd.DataFrame, source_name: Optional[str] = None) -> str:
    ncols = [normalize_col_name(c) for c in df.columns]
    source_norm = normalize_col_name(source_name or "")
    has_waiter = any("официант" in c or "waiter" in c for c in ncols)
    has_check = any("номер чека" in c or c == "чек" for c in ncols)
    has_date = any(c == "дата" or " date" in f" {c}" for c in ncols)
    has_revenue = any(("сумма" in c) or ("выручка" in c) or ("итого" in c) or ("revenue" in c) for c in ncols)
    has_dish = any(("блюдо" in c) or ("наименование" in c) or ("товар" in c) or ("позиция" in c) for c in ncols)
    has_qty = any(("количество" in c) or ("кол-во" in c) or (c == "расход") for c in ncols)
    has_paid = any(("оплач" in c) or ("paid" in c) for c in ncols)
    has_category = any(("категор" in c) or ("category" in c) for c in ncols)
    has_station = any(("станц" in c) or ("касса" in c) or ("station" in c) or ("место" in c) for c in ncols)
    has_guests = any(("гостей" in c) or ("гости" in c) or ("guests" in c) for c in ncols)
    has_checks_count = any(("чеков" in c) or ("checks" in c) or ("кол-во чеков" in c) for c in ncols)

    # Distinct HTML/XLS report classes to avoid collapsing unrelated uploads into "unknown".
    if has_waiter and has_dish and has_qty and (has_revenue or has_paid):
        return "waiters_dishes_sales"
    if has_category and has_dish and has_qty and (has_revenue or has_paid):
        return "sales_by_categories"
    if has_date and (has_revenue or has_paid) and has_station:
        return "revenue_by_stations_by_day"
    if has_date and (has_revenue or has_paid) and (has_checks_count or has_guests):
        return "revenue_checks_by_day"

    if has_waiter and has_check:
        return "waiters"
    if has_date and has_revenue:
        return "revenue_by_day"
    if has_dish and has_qty:
        return "food_usage"

    # Secondary safeguard for noisy HTML headers when business columns are not cleanly extracted.
    if "stations" in source_norm and "day" in source_norm:
        return "revenue_by_stations_by_day"
    if "checks" in source_norm and "day" in source_norm:
        return "revenue_checks_by_day"
    if "waiters" in source_norm and "dishes" in source_norm:
        return "waiters_dishes_sales"
    if "categories" in source_norm:
        return "sales_by_categories"
    return "unknown"


def build_mapping(df: pd.DataFrame, report_type: str) -> Dict[str, Optional[str]]:
    common = {
        "revenue": choose_revenue_source(df, ["сумма", "выручка", "итого", "revenue", "сум/чек"]),
    }

    # Revenue safeguard: if mapped source is empty/zero, fallback to strongest numeric money-like column.
    selected_revenue = common.get("revenue")
    if selected_revenue and selected_revenue in df.columns:
        selected_revenue_num = normalize_number_series(_get_series_by_name(df, selected_revenue))
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
    elif report_type in {"revenue_by_day", "revenue_checks_by_day", "revenue_by_stations_by_day"}:
        common.update(
            {
                "date": find_column_by_priority(df, ["дата", "date"]),
                "checks_count": find_column_by_priority(df, ["чеков", "кол-во чеков", "checks"]),
                "cashbox": find_column_by_priority(df, ["касса место", "касса", "station"]),
                "payment_type": find_column_by_priority(df, ["код", "валюта", "payment", "type"]),
            }
        )
    elif report_type in {"food_usage", "sales_by_categories", "waiters_dishes_sales"}:
        common.update(
            {
                "dish": find_column_by_priority(df, ["блюдо", "наименование", "товар", "позиция", "dish"]),
                "quantity": find_column_by_priority(df, ["количество", "кол-во", "qty", "quantity"]),
                "station": find_column_by_priority(df, ["цех", "станция", "подразделение", "категория", "группа"]),
            }
        )
        if report_type == "waiters_dishes_sales":
            common["waiter"] = find_column_by_priority(df, ["официант", "waiter"])
    return common


def apply_canonical_mapping(parsed_df: pd.DataFrame, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    df_kpi = parsed_df.copy()
    for canonical, source in mapping.items():
        if not source or source not in df_kpi.columns:
            continue
        # Keep original source columns to avoid collisions when one source
        # is reused by several canonical targets (e.g. revenue + payment_type).
        df_kpi[canonical] = _get_series_by_name(parsed_df, source)
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


def _normalize_spreadsheet_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    raw_columns = [
        re.sub(r"\s+", " ", str(c).replace("\u00A0", " ").replace("\u202F", " ").strip())
        for c in df.columns
    ]
    counts: Dict[str, int] = {}
    unique_columns: List[str] = []
    for idx, col in enumerate(raw_columns):
        base = col or f"unnamed_{idx + 1}"
        counts[base] = counts.get(base, 0) + 1
        unique_columns.append(base if counts[base] == 1 else f"{base}__{counts[base]}")
    df.columns = unique_columns
    return df.dropna(how="all").reset_index(drop=True)


def _small_text_quality_score(text: str) -> int:
    sample = text[:400]
    cyr = len(re.findall(r"[А-Яа-яЁё]", sample))
    mojibake = len(re.findall(r"[ÃÄÅÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîï]", sample))
    return cyr - mojibake * 4


def _mojibake_marker_count(text: str) -> int:
    sample = text[:20000]
    return (
        sample.count("Äàòà")
        + sample.count("Ð")
        + sample.count("Ñ")
        + len(re.findall(r"[ÃÄÅÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞß]", sample))
    )


def _repair_html_text_if_needed(html_text: str) -> str:
    base_score = _text_quality_score(html_text)
    base_markers = _mojibake_marker_count(html_text)
    if base_markers == 0 and base_score >= 0:
        return html_text

    best = html_text
    best_score = base_score
    best_markers = base_markers
    for src, dst in [("latin1", "cp1251"), ("latin1", "utf-8"), ("cp1251", "utf-8")]:
        try:
            candidate = html_text.encode(src).decode(dst)
        except Exception:
            continue
        candidate_score = _text_quality_score(candidate)
        candidate_markers = _mojibake_marker_count(candidate)
        if candidate_markers < best_markers and candidate_score > best_score + 5:
            best = candidate
            best_score = candidate_score
            best_markers = candidate_markers
    return best


def _repair_mojibake_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    original = _clean_cell(value)
    if not original:
        return original

    best = original
    best_score = _small_text_quality_score(original)
    for src, dst in [("latin1", "cp1251"), ("latin1", "utf-8"), ("cp1251", "utf-8")]:
        try:
            candidate = original.encode(src).decode(dst)
        except Exception:
            continue
        candidate = _clean_cell(candidate)
        score = _small_text_quality_score(candidate)
        if score > best_score + 1:
            best = candidate
            best_score = score
    return best


def _postprocess_html_table_df(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work.columns = [_repair_mojibake_text(str(c)) for c in work.columns]

    for idx in range(work.shape[1]):
        col = work.iloc[:, idx]
        if pd.api.types.is_object_dtype(col) or pd.api.types.is_string_dtype(col):
            work.iloc[:, idx] = col.map(_repair_mojibake_text)

    current_header_score = _header_keyword_score([str(c) for c in work.columns])
    best_idx: Optional[int] = None
    best_score = current_header_score
    for idx in range(min(len(work), 6)):
        row = [_repair_mojibake_text(_clean_cell(v)) for v in work.iloc[idx].tolist()]
        non_empty = sum(1 for v in row if v)
        if non_empty < 2:
            continue
        row_score = _header_keyword_score(row)
        is_period_row = any(normalize_col_name(v).startswith("дата ") and ":" in str(v).lower() for v in row)
        if is_period_row:
            continue
        if row_score >= max(2, current_header_score + 1) and row_score > best_score:
            best_idx = idx
            best_score = row_score

    if best_idx is not None:
        header = [_repair_mojibake_text(_clean_cell(v)) for v in work.iloc[best_idx].tolist()]
        body = work.iloc[best_idx + 1 :].reset_index(drop=True).copy()
        body.columns = header
        work = body

    return _normalize_spreadsheet_df(work)


def _parse_xlsx_bytes(raw: bytes) -> Tuple[Optional[pd.DataFrame], Dict, Optional[str]]:
    try:
        df = pd.read_excel(BytesIO(raw))
        info = {"encoding": None, "delimiter": None, "header_row_index": None, "attempts": []}
        return _normalize_spreadsheet_df(df), info, None
    except Exception as err:
        return None, {"attempts": []}, f"Ошибка чтения XLSX: {err}"


def _looks_like_html_xls(raw: bytes) -> bool:
    head = raw[:4096].decode("latin1", errors="ignore").lower()
    return any(marker in head for marker in ["<!doctype html", "<html", "<table", "<tr", "<td", "<meta"])


def _extract_declared_html_charset(raw: bytes) -> Optional[str]:
    head = raw[:8192].decode("latin1", errors="ignore")
    m = re.search(r"charset\s*=\s*['\"]?\s*([A-Za-z0-9_\-]+)", head, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip().lower()


def _header_keyword_score(cells: List[str]) -> int:
    keywords = [
        "дата", "категор", "блюдо", "код", "кол-во", "количество",
        "сумма", "оплач", "официант", "чеков", "гостей", "касса",
        "станц", "место",
    ]
    score = 0
    for cell in cells:
        norm = normalize_col_name(cell)
        if not norm:
            continue
        if any(key in norm for key in keywords):
            score += 1
    return score


def _promote_html_header(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[int]]:
    if df.empty:
        return df, None

    scan_limit = min(len(df), 12)
    best_idx: Optional[int] = None
    best_score = -1
    best_non_empty = -1

    for idx in range(scan_limit):
        row = [_repair_mojibake_text(_clean_cell(v)) for v in df.iloc[idx].tolist()]
        non_empty = sum(1 for v in row if v)
        if non_empty < 2:
            continue
        keyword_score = _header_keyword_score(row)
        text_like = sum(1 for v in row if v and _to_float(v) is None)
        penalty = 2 if any(normalize_col_name(v).startswith("дата ") and ":" in str(v).lower() for v in row) else 0
        score = keyword_score * 10 + text_like - penalty
        if score > best_score or (score == best_score and non_empty > best_non_empty):
            best_idx = idx
            best_score = score
            best_non_empty = non_empty

    if best_idx is None or best_score < 10:
        return df, None

    header = [_repair_mojibake_text(_clean_cell(v)) for v in df.iloc[best_idx].tolist()]
    body = df.iloc[best_idx + 1 :].reset_index(drop=True).copy()
    body.columns = header
    return body, best_idx


def _text_quality_score(text: str) -> int:
    sample = text[:20000]
    cyr = len(re.findall(r"[А-Яа-яЁё]", sample))
    mojibake = len(re.findall(r"[ÃÄÅÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîï]", sample))
    return cyr - mojibake * 3


def _dataframe_quality_score(df: pd.DataFrame) -> int:
    header_cells = [str(c) for c in df.columns]
    head_values: List[str] = []
    if not df.empty:
        head_values = [str(v) for v in df.head(8).fillna("").to_numpy().flatten().tolist()]
    header_score = _header_keyword_score(header_cells) * 30
    cyr = len(re.findall(r"[А-Яа-яЁё]", " ".join(header_cells + head_values)[:12000]))
    mojibake = len(re.findall(r"[ÃÄÅÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîï]", " ".join(header_cells + head_values)[:12000]))
    return header_score + cyr - mojibake * 5


class _HTMLTableExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: List[List[List[str]]] = []
        self._table_stack = 0
        self._current_table: Optional[List[List[str]]] = None
        self._current_row: Optional[List[str]] = None
        self._current_cell: Optional[List[str]] = None

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        tag = tag.lower()
        if tag == "table":
            self._table_stack += 1
            if self._table_stack == 1:
                self._current_table = []
        elif tag == "tr" and self._table_stack == 1:
            self._current_row = []
        elif tag in {"td", "th"} and self._table_stack == 1 and self._current_row is not None:
            self._current_cell = []
        elif tag == "br" and self._current_cell is not None:
            self._current_cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            cell = _clean_cell(unescape("".join(self._current_cell)))
            self._current_row.append(cell)
            self._current_cell = None
        elif tag == "tr" and self._table_stack == 1 and self._current_row is not None and self._current_table is not None:
            self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._table_stack > 0:
            if self._table_stack == 1 and self._current_table is not None:
                self.tables.append(self._current_table)
                self._current_table = None
            self._table_stack -= 1

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)


def _select_best_html_table(tables: List[List[List[str]]]) -> Optional[pd.DataFrame]:
    best_df: Optional[pd.DataFrame] = None
    best_score = -1

    for table in tables:
        cleaned_rows = [_trim_trailing_blank([_clean_cell(cell) for cell in row]) for row in table]
        cleaned_rows = [row for row in cleaned_rows if _has_meaningful_cells(row)]
        if len(cleaned_rows) < 2:
            continue
        width = max((len(row) for row in cleaned_rows), default=0)
        if width < 2:
            continue

        normalized_rows = [row + [""] * (width - len(row)) for row in cleaned_rows]
        non_empty_cells = sum(1 for row in normalized_rows for cell in row if _clean_cell(cell))
        score = len(normalized_rows) * width + non_empty_cells
        if score <= best_score:
            continue

        candidate = pd.DataFrame(normalized_rows)
        candidate, header_idx = _promote_html_header(candidate)
        quality_bonus = _dataframe_quality_score(candidate) if candidate is not None and not candidate.empty else 0
        total_score = score + quality_bonus + (15 if header_idx is not None else 0)
        if total_score <= best_score:
            continue

        best_score = total_score
        if candidate is not None and not candidate.empty:
            best_df = candidate
        else:
            header = normalized_rows[0]
            body = normalized_rows[1:] if len(normalized_rows) > 1 else []
            best_df = pd.DataFrame(body, columns=header)

    return best_df


def _parse_html_xls_bytes(raw: bytes, attempts: List[Dict[str, Any]]) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    declared_charset = _extract_declared_html_charset(raw)
    encodings: List[str] = []
    for enc in [declared_charset, "utf-8", "utf-8-sig", "cp1251", "latin1"]:
        if enc and enc not in encodings:
            encodings.append(enc)

    best_result: Optional[Tuple[pd.DataFrame, str, int]] = None

    for encoding in encodings:
        try:
            html_text = raw.decode(encoding)
        except Exception as err:
            attempts.append({"parser": "html_table", "encoding": encoding, "status": "decode_failed", "error": str(err)})
            continue
        repaired_html_text = _repair_html_text_if_needed(html_text)
        html_repaired = repaired_html_text != html_text
        html_text = repaired_html_text

        try:
            extractor = _HTMLTableExtractor()
            extractor.feed(html_text)
            extractor.close()
            df = _select_best_html_table(extractor.tables)
            if df is not None and not df.empty:
                promoted_df, header_idx = _promote_html_header(df)
                normalized_df = _postprocess_html_table_df(promoted_df if promoted_df is not None and not promoted_df.empty else df)
                quality = _text_quality_score(html_text) + _dataframe_quality_score(normalized_df)
                attempts.append(
                    {
                        "parser": "html_custom",
                        "encoding": encoding,
                        "status": "ok",
                        "error": "",
                        "tables_found": len(extractor.tables),
                        "shape": list(normalized_df.shape),
                        "header_row_index": header_idx,
                        "quality": quality,
                        "html_repaired": html_repaired,
                    }
                )
                if best_result is None or quality > best_result[2]:
                    best_result = (normalized_df, encoding, quality)
            raise ValueError("Подходящая HTML-таблица не найдена.")
        except Exception as err:
            attempts.append({"parser": "html_custom", "encoding": encoding, "status": "failed", "error": str(err)})

        try:
            tables = pd.read_html(StringIO(html_text))
            if not tables:
                raise ValueError("HTML-таблицы не найдены.")
            best_df: Optional[pd.DataFrame] = None
            best_table_quality = -10**9
            best_header_idx: Optional[int] = None
            for table in tables:
                promoted_df, header_idx = _promote_html_header(table)
                normalized_df = _postprocess_html_table_df(promoted_df if promoted_df is not None and not promoted_df.empty else table)
                table_quality = _dataframe_quality_score(normalized_df)
                if table_quality > best_table_quality:
                    best_table_quality = table_quality
                    best_df = normalized_df
                    best_header_idx = header_idx
            if best_df is None or best_df.empty:
                raise ValueError("HTML-таблицы не содержат пригодных данных.")
            quality = _text_quality_score(html_text) + best_table_quality
            attempts.append(
                {
                    "parser": "html_table",
                    "encoding": encoding,
                    "status": "ok",
                    "error": "",
                    "tables_found": len(tables),
                    "header_row_index": best_header_idx,
                    "quality": quality,
                    "html_repaired": html_repaired,
                }
            )
            if best_result is None or quality > best_result[2]:
                best_result = (best_df, encoding, quality)
        except Exception as err:
            attempts.append({"parser": "html_table", "encoding": encoding, "status": "failed", "error": str(err)})

    if best_result is not None:
        return best_result[0], best_result[1], None
    return None, None, "Не удалось прочитать HTML-таблицы из XLS-экспорта."


def _parse_xls_bytes(raw: bytes) -> Tuple[Optional[pd.DataFrame], Dict, Optional[str]]:
    attempts: List[Dict[str, Any]] = []
    html_hint = _looks_like_html_xls(raw)

    if html_hint:
        df, encoding, _html_err = _parse_html_xls_bytes(raw, attempts)
        if df is not None:
            info = {
                "encoding": encoding,
                "delimiter": None,
                "header_row_index": None,
                "attempts": attempts,
                "html_hint": True,
            }
            return df, info, None
    else:
        try:
            df = pd.read_excel(BytesIO(raw))
            attempts.append({"parser": "legacy_excel", "status": "ok", "error": ""})
            info = {"encoding": None, "delimiter": None, "header_row_index": None, "attempts": attempts, "html_hint": False}
            return _normalize_spreadsheet_df(df), info, None
        except Exception as err:
            attempts.append({"parser": "legacy_excel", "status": "failed", "error": str(err)})

    if not html_hint:
        df, encoding, _html_err = _parse_html_xls_bytes(raw, attempts)
        if df is not None:
            info = {
                "encoding": encoding,
                "delimiter": None,
                "header_row_index": None,
                "attempts": attempts,
                "html_hint": False,
            }
            return df, info, None

    return (
        None,
        {"attempts": attempts, "html_hint": html_hint},
        (
            "Не удалось разобрать XLS-файл ни как legacy Excel, ни как HTML-экспорт. "
            "Проверьте экспорт из R-Keeper и попробуйте выгрузить файл повторно."
        ),
    )


def load_file(uploaded_file) -> Tuple[Optional[pd.DataFrame], Dict, Optional[str]]:
    name = uploaded_file.name.lower()
    raw = uploaded_file.getvalue()
    if name.endswith(".xlsx"):
        return _parse_xlsx_bytes(raw)

    if name.endswith(".xls"):
        return _parse_xls_bytes(raw)

    if name.endswith(".csv"):
        return parse_csv_bytes(raw)

    return None, {"attempts": []}, "Поддерживаются только CSV, XLSX и XLS."


def is_kitchen_bar_section_report_filename(file_name: str) -> bool:
    name = (file_name or "").strip().lower()
    base_name = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    return base_name == "kitchen_bar_by_station.csv"
