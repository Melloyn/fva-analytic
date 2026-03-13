import re

import pandas as pd


def normalize_text(value: str) -> str:
    text = str(value).replace("\u00A0", " ").replace("\u202F", " ").strip().lower()
    return re.sub(r"\s+", " ", text)


def normalize_col_name(value: str) -> str:
    return normalize_text(value)


def normalize_number_series(s: pd.Series) -> pd.Series:
    if isinstance(s, pd.DataFrame):
        if s.shape[1] == 0:
            return pd.Series(dtype="float64")
        s = s.iloc[:, 0]
    s = s.astype(str)
    s = s.str.replace("\u00A0", "", regex=False)
    s = s.str.replace("\u202F", "", regex=False)
    s = s.str.replace(r"\s+", "", regex=True)
    s = s.str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^0-9\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0)


def is_blank_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().replace({"nan": "", "None": "", "none": ""}) == ""
