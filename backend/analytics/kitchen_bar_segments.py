from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


def _decode_bytes(raw: bytes) -> str:
    for enc in ("utf-8-sig", "cp1251", "utf-8", "latin1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("latin1", errors="replace")


def _to_float(value: str) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip().replace("\xa0", " ").replace(" ", "")
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _is_section_header(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if t.startswith("итого"):
        return False
    if any(x in t for x in ["расход блюд", "наименование ресторана", "дата:"]):
        return False
    if any(x in t for x in ["код", "блюдо", "кол-во", "сумма", "оплачено"]):
        return False
    return any(x in t for x in ["бар", "кух", "цех", "место", "суши"])


def _segment_from_section(section: str) -> str:
    s = section.lower()
    if "бар" in s and "кух" not in s:
        return "bar"
    if any(x in s for x in ["кух", "цех", "суши", "гор.", "хол.", "горяч", "холод"]):
        return "kitchen"
    if "место" in s and "бар" in s:
        return "bar"
    if "место" in s:
        # Conservative fallback for workshop-like names.
        return "kitchen"
    return "unknown"


def _normalize_workshop(section: str) -> str:
    s = section.strip()
    low = s.lower()
    if "гор" in low and "цех" in low:
        return "Гор. цех"
    if "хол" in low and "цех" in low:
        return "Хол. цех"
    if "суш" in low:
        return "Суши"
    if "burger" in low and "кух" in low:
        return "burger кухня"
    return s


def _parse_item_name(cell: str) -> str:
    text = (cell or "").strip()
    if not text:
        return ""
    m = re.match(r"^\d+\s*(.+)$", text)
    return m.group(1).strip() if m else text


def load_kitchen_bar_rows(base_dir: Path) -> Dict[str, Any]:
    candidates = [
        base_dir / "data" / "processed" / "kitchen_bar_by_station.csv",
        base_dir / "data" / "kitchen_bar_by_station.csv",
        base_dir / "kitchen_bar_by_station.csv",
    ]
    candidates.extend(sorted((base_dir / "data" / "processed").glob("*kitchen*bar*station*.csv")))

    src = None
    for path in candidates:
        if path.exists():
            src = path
            break

    if src is None:
        return {"ok": False, "reason": "Не найден kitchen_bar_by_station.csv."}

    text = _decode_bytes(src.read_bytes())
    rows = []
    current_section = "Неизвестный раздел"
    section_headers_found = 0

    for raw_line in text.splitlines():
        cells = [c.strip() for c in raw_line.split(";")]
        non_empty = [c for c in cells if c]
        if not non_empty:
            continue

        first = non_empty[0]
        low_first = first.lower()

        if _is_section_header(first) and len(non_empty) <= 2:
            current_section = first
            section_headers_found += 1
            continue

        if low_first.startswith("итого"):
            continue

        if any(x in low_first for x in ["код", "блюдо", "кол-во", "сумма", "оплачено"]):
            continue

        nums = [_to_float(c) for c in cells if _to_float(c) is not None]
        if not nums:
            continue

        item_name = _parse_item_name(first)
        if not item_name:
            continue

        quantity = float(nums[0]) if len(nums) >= 1 else 0.0
        amount = float(nums[1]) if len(nums) >= 2 else 0.0
        discount = float(nums[2]) if len(nums) >= 3 else 0.0
        paid = float(nums[-1]) if len(nums) >= 2 else amount
        revenue = paid if paid != 0 else amount

        segment = _segment_from_section(current_section)
        workshop_name = _normalize_workshop(current_section) if segment == "kitchen" else ""

        rows.append(
            {
                "section_name": current_section,
                "segment_type": segment,
                "workshop_name": workshop_name,
                "item": item_name,
                "quantity": quantity,
                "amount": amount,
                "discount": discount,
                "paid": paid,
                "revenue": revenue,
            }
        )

    if not rows:
        return {"ok": False, "reason": "В отчете нет валидных строк позиций для сегментного анализа."}

    df = pd.DataFrame(rows)
    segment_counts = df["segment_type"].value_counts().to_dict()
    classified = int(segment_counts.get("bar", 0)) + int(segment_counts.get("kitchen", 0))
    if section_headers_found == 0 or classified == 0:
        return {
            "ok": False,
            "reason": (
                "Структура kitchen_bar_by_station.csv не распознана: "
                "не удалось выделить секции бар/кухня. "
                "Проверьте формат экспорта (section-based отчет)."
            ),
        }

    return {"ok": True, "df": df, "source": str(src)}


def aggregate_segment_metric(df: pd.DataFrame, segment: str, metric: str) -> Dict[str, Any]:
    work = df[df["segment_type"] == segment].copy()
    if work.empty:
        return {"ok": False, "reason": "Нет данных для выбранного сегмента."}

    if metric == "revenue":
        value = float(work["revenue"].sum())
    elif metric == "quantity":
        value = float(work["quantity"].sum())
    else:
        return {"ok": False, "reason": "Неподдерживаемая метрика сегмента."}

    return {
        "ok": True,
        "value": value,
        "positions": int(work["item"].nunique(dropna=True)),
    }


def top_items_by_segment(df: pd.DataFrame, segment: str, metric: str, limit: int = 5) -> Dict[str, Any]:
    work = df[df["segment_type"] == segment].copy()
    if work.empty:
        return {"ok": False, "reason": "Нет данных для выбранного сегмента."}

    metric_col = "revenue" if metric == "revenue" else "quantity"
    grouped = (
        work.groupby("item", dropna=False)[metric_col]
        .sum()
        .reset_index()
        .sort_values(metric_col, ascending=False)
        .head(limit)
    )
    return {"ok": True, "table": grouped, "metric_col": metric_col}


def abc_by_segment(df: pd.DataFrame, segment: str) -> Dict[str, Any]:
    work = df[df["segment_type"] == segment].copy()
    if work.empty:
        return {"ok": False, "reason": "Нет данных для выбранного сегмента."}

    grouped = (
        work.groupby("item", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    total = float(grouped["revenue"].sum())
    if total <= 0:
        return {"ok": False, "reason": "Нет валидной выручки для ABC."}

    grouped["share"] = grouped["revenue"] / total
    grouped["cum_share"] = grouped["share"].cumsum()

    def abc_class(cum_share: float) -> str:
        if cum_share <= 0.8:
            return "A"
        if cum_share <= 0.95:
            return "B"
        return "C"

    grouped["abc"] = grouped["cum_share"].apply(abc_class)
    return {"ok": True, "table": grouped}


def workshops_metric(df: pd.DataFrame, metric: str) -> Dict[str, Any]:
    work = df[df["segment_type"] == "kitchen"].copy()
    if work.empty:
        return {"ok": False, "reason": "Нет данных по кухне/цехам."}

    metric_col = "revenue" if metric == "revenue" else "quantity"
    grouped = (
        work.groupby("workshop_name", dropna=False)[metric_col]
        .sum()
        .reset_index()
        .sort_values(metric_col, ascending=False)
    )
    return {"ok": True, "table": grouped, "metric_col": metric_col}


def workshops_abc(df: pd.DataFrame, top_n: int = 3) -> Dict[str, Any]:
    work = df[df["segment_type"] == "kitchen"].copy()
    if work.empty:
        return {"ok": False, "reason": "Нет данных по кухне/цехам."}

    blocks = []
    for workshop, wdf in work.groupby("workshop_name", dropna=False):
        grouped = (
            wdf.groupby("item", dropna=False)["revenue"]
            .sum()
            .reset_index()
            .sort_values("revenue", ascending=False)
            .head(top_n)
        )
        blocks.append({"workshop": workshop, "top": grouped})

    return {"ok": True, "blocks": blocks}
