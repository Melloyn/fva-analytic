import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.ingestion.loader import parse_csv_bytes


def _assert_ok(name: str, raw: bytes):
    df, info, err = parse_csv_bytes(raw)
    assert err is None, f"{name}: expected success, got error: {err}; info={info}"
    assert df is not None and not df.empty, f"{name}: expected non-empty df"
    return df, info


def _assert_fail(name: str, raw: bytes):
    df, info, err = parse_csv_bytes(raw)
    assert df is None, f"{name}: expected fail, got df with shape={None if df is None else df.shape}"
    assert err, f"{name}: expected explicit error message"
    return info, err


def run():
    # 1) Sparse waiters: money columns must not shift right.
    raw1 = (
        ";;;;;;Отчет по официантам\n"
        ";;;;;;Наименование Ресторана: MESTO\n"
        ";Официант;;Номер Чека;Чеков;;Гостей;Сумма;Сум/чек;Сум/гост;Сум/гост/ча\n"
        ";;;;;;;;;;с\n"
        ";Агаева Саманта;;1960043;;1,00;1,00;;2570,00;2570,00;2570,00;1620,60\n"
    ).encode("cp1251")
    df1, info1 = _assert_ok("case1", raw1)
    assert info1["delimiter"] == ";"
    assert list(df1.columns) == ["Официант", "Номер Чека", "Чеков", "Гостей", "Сумма", "Сум/чек", "Сум/гост", "Сум/гост/час"]
    r1 = df1.iloc[0].to_dict()
    assert r1["Официант"] == "Агаева Саманта"
    assert r1["Сумма"] == "2570,00"
    assert r1["Сум/чек"] == "2570,00"

    # 2) Comma-delimited with quoted values.
    raw2 = (
        "Date,Code,Revenue\n"
        "\"2026-03-01\",\"1\",\"1234.50\"\n"
        "\"2026-03-02\",\"1\",\"2234.50\"\n"
    ).encode("utf-8")
    df2, info2 = _assert_ok("case2", raw2)
    assert info2["delimiter"] == ","
    assert "Revenue" in df2.columns

    # 3) Sparse revenue_by_day with service rows.
    raw3 = (
        "service line\n"
        "meta line\n"
        ";;;;Выручка станций по дням\n"
        ";Дата;;Код;Валюта;;burger;Касса Место;;Итого\n"
        ";01.03.2026;;1Рубли;;;;28 933.80;197 432.40;;226 366.20\n"
    ).encode("cp1251")
    df3, info3 = _assert_ok("case3", raw3)
    assert list(df3.columns) == ["Дата", "Код", "Касса Место", "Станция", "Итого"]
    r3 = df3.iloc[0].to_dict()
    assert r3["Дата"] == "01.03.2026"
    assert r3["Код"] == "1Рубли"
    assert r3["Касса Место"] == "28 933.80"
    assert r3["Станция"] == "197 432.40"
    assert r3["Итого"] == "226 366.20"

    # 4) Sparse food_usage with concatenated Код+Блюдо.
    raw4 = (
        ";;;;;;;Расход блюд\n"
        ";Код;;;Блюдо;;;Кол-во;Сумма;Скидка;;Оплачено\n"
        ";170Фахитас с говядиной;;;;;;;9,00;7 740,00;-344,00;;7 396,00\n"
    ).encode("cp1251")
    df4, info4 = _assert_ok("case4", raw4)
    assert list(df4.columns) == ["Код", "Блюдо", "Кол-во", "Сумма", "Скидка", "Оплачено"]
    r4 = df4.iloc[0].to_dict()
    assert r4["Код"] == "170"
    assert r4["Блюдо"] == "Фахитас с говядиной"
    assert r4["Кол-во"] == "9,00"
    assert r4["Сумма"] == "7 740,00"
    assert r4["Оплачено"] == "7 396,00"

    # 5) Dangerously inconsistent row widths -> fail.
    raw5 = (
        "A;B;C\n"
        "1;2;3\n"
        "1;2;3;4;5;6;7\n"
        "1;2\n"
    ).encode("utf-8")
    _assert_fail("case5", raw5)

    # 6) Formally opens but semantically unsafe (systematically non-numeric business metrics) -> fail.
    raw6 = (
        ";Код;;;Блюдо;;;Кол-во;Сумма;Скидка;;Оплачено\n"
        ";170Блюдо A;;;;;;;qwe;zzz;-10,00;;abc\n"
        ";171Блюдо B;;;;;;;qwe;zzz;-20,00;;abc\n"
        ";172Блюдо C;;;;;;;qwe;zzz;-30,00;;abc\n"
        ";173Блюдо D;;;;;;;qwe;zzz;-40,00;;abc\n"
        ";174Блюдо E;;;;;;;qwe;zzz;-50,00;;abc\n"
        ";175Блюдо F;;;;;;;qwe;zzz;-60,00;;abc\n"
        ";176Блюдо G;;;;;;;qwe;zzz;-70,00;;abc\n"
    ).encode("cp1251")
    _assert_fail("case6", raw6)

    # 7) Missing required business columns -> explicit fail.
    raw7 = (
        "A;B;C\n"
        "foo;bar;baz\n"
        "1;2;3\n"
    ).encode("utf-8")
    _assert_fail("case7", raw7)

    print("All CSV loader validation scenarios passed.")


if __name__ == "__main__":
    run()
