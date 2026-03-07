import pandas as pd

def compute_basic_kpis(df: pd.DataFrame) -> dict:
    """
    Черновой расчёт KPI.
    Требование: в df должна быть колонка с суммой (например: 'Сумма', 'Итого', 'Amount', 'Total').
    И колонка с идентификатором чека (например: 'Чек', 'Номер чека', 'CheckId').
    Если их нет — вернём нули и подсказку.
    """

    # Попробуем угадать колонку суммы
    sum_candidates = ["Сумма", "Итого", "Amount", "Total", "Sum"]
    check_candidates = ["Чек", "Номер чека", "Check", "CheckId", "Receipt", "ReceiptId"]

    sum_col = next((c for c in sum_candidates if c in df.columns), None)
    check_col = next((c for c in check_candidates if c in df.columns), None)

    if sum_col is None:
        return {"total_revenue": 0.0, "checks_count": 0, "avg_check": 0.0, "note": "Не найдена колонка суммы"}

    total_revenue = float(pd.to_numeric(df[sum_col], errors="coerce").fillna(0).sum())

    if check_col is None:
        # если нет чека — считаем количество строк как "чеки" (временно, грубо)
        checks_count = int(len(df))
    else:
        checks_count = int(df[check_col].nunique(dropna=True))

    avg_check = (total_revenue / checks_count) if checks_count > 0 else 0.0

    return {
        "total_revenue": total_revenue,
        "checks_count": checks_count,
        "avg_check": avg_check,
        "note": "OK"
    }