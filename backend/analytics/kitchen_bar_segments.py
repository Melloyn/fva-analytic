from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

BAR_SECTION_NAMES = ["Бар burger", "МЕСТО Бар"]
BAR_SECTION_KEYS = {
    "bar_burger": "Бар burger",
    "mesto_bar": "МЕСТО Бар",
}
KITCHEN_SECTION_NAMES = ["Кухня burger", "СУШИ-М Кухня"]
KITCHEN_WORKSHOP_SECTION_NAMES = [
    "МЕСТО Гор. цех",
    "МЕСТО Хол.цех",
    "Место Гор. + Хол. цех",
    "СУШИ-М Кухня",
    "Кухня burger",
]

EXACT_SECTION_RULES = {
    "бар burger": {"section_name": "Бар burger", "segment": "bar", "workshop": ""},
    "место бар": {"section_name": "МЕСТО Бар", "segment": "bar", "workshop": ""},
    "кухня burger": {"section_name": "Кухня burger", "segment": "kitchen", "workshop": "Кухня burger"},
    "суши-м кухня": {"section_name": "СУШИ-М Кухня", "segment": "kitchen", "workshop": "СУШИ-М Кухня"},
    "место гор. цех": {"section_name": "МЕСТО Гор. цех", "segment": "kitchen", "workshop": "МЕСТО Гор. цех"},
    "место хол.цех": {"section_name": "МЕСТО Хол.цех", "segment": "kitchen", "workshop": "МЕСТО Хол.цех"},
    "место хол. цех": {"section_name": "МЕСТО Хол.цех", "segment": "kitchen", "workshop": "МЕСТО Хол.цех"},
    "место гор. + хол. цех": {"section_name": "Место Гор. + Хол. цех", "segment": "kitchen", "workshop": "Место Гор. + Хол. цех"},
    "место гор.+хол. цех": {"section_name": "Место Гор. + Хол. цех", "segment": "kitchen", "workshop": "Место Гор. + Хол. цех"},
}


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


def _is_numeric_like(value: str) -> bool:
    return _to_float(value) is not None


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


def _normalize_section_key(section: str) -> str:
    return re.sub(r"\s+", " ", section.strip().lower())


def _segment_from_section(section: str) -> str:
    key = _normalize_section_key(section)
    exact = EXACT_SECTION_RULES.get(key)
    if exact:
        return str(exact["segment"])

    # Secondary conservative fallback for sections outside fixed mapping.
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
    key = _normalize_section_key(section)
    exact = EXACT_SECTION_RULES.get(key)
    if exact:
        return str(exact["workshop"])

    # Secondary fallback for unknown kitchen sections.
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


def _find_explicit_section(cells: list[str]) -> Optional[str]:
    for cell in cells:
        norm_cell = _normalize_section_key(cell)
        for key, rule in EXACT_SECTION_RULES.items():
            if norm_cell == key or norm_cell.endswith(key):
                return str(rule["section_name"])
    return None


def _extract_item_name(non_empty_cells: list[str]) -> str:
    # Prefer the first non-numeric business text token to avoid treating raw code as item.
    for cell in non_empty_cells:
        norm = _normalize_section_key(cell)
        if norm in EXACT_SECTION_RULES:
            continue
        if any(x in norm for x in ["код", "блюдо", "кол-во", "сумма", "оплачено", "итого"]):
            continue
        if _is_numeric_like(cell):
            continue
        name = _parse_item_name(cell)
        if name and not _is_numeric_like(name):
            return name
    return _parse_item_name(non_empty_cells[0]) if non_empty_cells else ""


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

        explicit_section = _find_explicit_section(non_empty)
        if explicit_section:
            current_section = explicit_section
            section_headers_found += 1
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

        item_name = _extract_item_name(non_empty)
        if not item_name:
            continue

        # Parse numeric metrics from the tail to avoid code/index numbers at row start.
        if len(nums) >= 4:
            quantity = float(nums[-4])
            amount = float(nums[-3])
            discount = float(nums[-2])
            paid = float(nums[-1])
        elif len(nums) == 3:
            quantity = float(nums[-3])
            amount = float(nums[-2])
            discount = 0.0
            paid = float(nums[-1])
        elif len(nums) == 2:
            quantity = float(nums[-2])
            amount = float(nums[-1])
            discount = 0.0
            paid = amount
        else:
            quantity = float(nums[-1])
            amount = 0.0
            discount = 0.0
            paid = 0.0

        revenue = paid if paid != 0 else amount

        canonical_section = _find_explicit_section([current_section]) or current_section
        segment = _segment_from_section(canonical_section)
        workshop_name = _normalize_workshop(canonical_section) if segment == "kitchen" else ""

        rows.append(
            {
                "section_name": canonical_section,
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


def _filter_by_sections(df: pd.DataFrame, section_names: Optional[list[str]]) -> pd.DataFrame:
    if not section_names:
        return df
    allowed = {_normalize_section_key(x) for x in section_names}
    section_norm = df["section_name"].astype(str).map(_normalize_section_key)
    return df[section_norm.isin(allowed)].copy()


def aggregate_segment_metric(
    df: pd.DataFrame,
    segment: str,
    metric: str,
    section_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    work = df[df["segment_type"] == segment].copy()
    work = _filter_by_sections(work, section_names)
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


def top_items_by_segment(
    df: pd.DataFrame,
    segment: str,
    metric: str,
    limit: int = 5,
    section_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    work = df[df["segment_type"] == segment].copy()
    work = _filter_by_sections(work, section_names)
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


def abc_by_segment(
    df: pd.DataFrame,
    segment: str,
    section_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    work = df[df["segment_type"] == segment].copy()
    work = _filter_by_sections(work, section_names)
    if work.empty:
        return {"ok": False, "reason": "Нет данных для выбранного сегмента."}

    if segment == "bar":
        # Explicit exclusion rule requested by product: hookah items must not affect bar ABC.
        mask_hookah = work["item"].astype(str).str.lower().str.contains("кальян", na=False)
        work = work[~mask_hookah].copy()
        if work.empty:
            return {"ok": False, "reason": "После исключения кальянов нет данных для ABC — Бар."}

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


def workshops_metric(
    df: pd.DataFrame,
    metric: str,
    section_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    work = df[df["segment_type"] == "kitchen"].copy()
    work = _filter_by_sections(work, section_names)
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


def workshops_abc(
    df: pd.DataFrame,
    top_n: int = 3,
    section_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    work = df[df["segment_type"] == "kitchen"].copy()
    work = _filter_by_sections(work, section_names)
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
