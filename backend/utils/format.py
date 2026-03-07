import pandas as pd

from backend.utils.normalize import normalize_col_name


def format_rub(x: float) -> str:
    try:
        value = float(x)
    except Exception:
        value = 0.0
    s = f"{value:,.2f}"
    s = s.replace(",", " ").replace(".", ",")
    return f"{s} руб"


def format_money_columns_for_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        n = normalize_col_name(c)
        if any(k in n for k in ["сум", "выруч", "revenue", "итого"]):
            if pd.api.types.is_numeric_dtype(out[c]):
                out[c] = out[c].apply(format_rub)
    return out
