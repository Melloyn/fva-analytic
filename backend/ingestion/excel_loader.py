import pandas as pd

# ВАЖНО:
# Мы НЕ знаем реальные названия колонок в вашем Excel.
# Поэтому делаем "мягкий" загрузчик: читает всё и чуть чистит базово.
def load_sales_excel(file) -> pd.DataFrame:
    df = pd.read_excel(file)

    # Нормализация названий колонок: убрать пробелы по краям
    df.columns = [str(c).strip() for c in df.columns]

    # Удалим полностью пустые строки
    df = df.dropna(how="all").reset_index(drop=True)

    return df