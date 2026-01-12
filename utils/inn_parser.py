import json
import re
from typing import Any


_RE_MONEY = re.compile(r"([0-9][0-9\s]*)\s*₽")
_RE_INN = re.compile(r"\bИНН\b.*?`?(\d{10}|\d{12})`?", re.IGNORECASE)
_RE_OGRN = re.compile(r"\bОГРН\b.*?`?(\d{13})`?", re.IGNORECASE)


def _money_to_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if not digits:
        return None
    return int(digits)


def _find_money_after(label: str, text: str) -> int | None:
    # Ищем строку вида "**Выручка:** 8 967 000 ₽"
    m = re.search(rf"{re.escape(label)}\s*[:：]?\s*([0-9][0-9\s]*)\s*₽", text)
    if not m:
        return None
    return _money_to_int(m.group(1))


def parse_inn_result_text(text: str) -> dict[str, Any]:
    """
    Парсит ответ бота по /inn из обычного текста (Markdown).
    Возвращает словарь полей для записи в inn_results.
    """
    out: dict[str, Any] = {
        "inn": None,
        "ogrn": None,
        "company_name": None,
        "okved": None,
        "reg_date": None,
        "company_status": None,
        "director_name": None,
        "director_inn": None,
        "employees_count": None,
        "revenue_2024": None,
        "income_2024": None,
        "expenses_2024": None,
        "authorized_capital": None,
        "address": None,
        "founders_json": None,
        "raw_text": text,
    }

    # Название компании — часто первая жирная строка: 🏢 **ООО "ФЕНИКС ПЛЮС"**
    m = re.search(r"^\s*.*\*\*(.+?)\*\*\s*$", text, flags=re.MULTILINE)
    if m:
        out["company_name"] = m.group(1).strip()

    # ОКВЭД — строка вида: __46.90 — Торговля ...__
    m = re.search(r"__\s*([^_]+?)\s*__", text)
    if m:
        out["okved"] = m.group(1).strip()

    m = _RE_INN.search(text)
    if m:
        out["inn"] = m.group(1)

    m = _RE_OGRN.search(text)
    if m:
        out["ogrn"] = m.group(1)

    # Дата регистрации: "**Дата регистрации:** 24.01.2013 (4736 дней назад)" -> берём только дату.
    # Важно: в тексте часто есть Markdown "**" вокруг "Дата регистрации:".
    m = re.search(r"Дата регистрации:\*+\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text)
    if not m:
        m = re.search(r"Дата регистрации:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text)
    if m:
        out["reg_date"] = m.group(1)

    m = re.search(r"\*\*Статус:\*\*\s*([^\n]+)", text)
    if m:
        out["company_status"] = m.group(1).strip()

    # Директор: Смагина Ирина Робертовна (ИНН 780419031060)
    m = re.search(r"Директор:\s*([^(]+)\(\s*ИНН\s*([0-9]{10,12})\s*\)", text)
    if m:
        out["director_name"] = m.group(1).strip()
        out["director_inn"] = m.group(2).strip()

    # Сотрудников: "**Сотрудников:** 10"
    m = re.search(r"\*\*Сотрудников:\*\*\s*([0-9]+)", text)
    if m:
        out["employees_count"] = int(m.group(1))

    # Финансовые показатели (2024):
    out["revenue_2024"] = _find_money_after("**Выручка:**", text)
    out["income_2024"] = _find_money_after("**Доход:**", text)
    out["expenses_2024"] = _find_money_after("**Расходы:**", text)

    m = re.search(r"\*\*Уставный капитал:\*\*\s*([0-9][0-9\s]*)\s*₽", text)
    if m:
        out["authorized_capital"] = _money_to_int(m.group(1))

    m = re.search(r"\*\*Адрес:\*\*\s*([^\n]+)", text)
    if m:
        out["address"] = m.group(1).strip()

    founders = _parse_founders(text)
    if founders is not None:
        out["founders_json"] = json.dumps(founders, ensure_ascii=False)

    return out


def _parse_founders(text: str) -> list[dict[str, Any]] | None:
    # Блок после "**📝 Учредители:**" до пустой строки или до "👁"
    m = re.search(r"\*\*📝 Учредители:\*\*\s*\n([\s\S]+)", text)
    if not m:
        return None

    tail = m.group(1)
    tail = tail.split("\n\n", 1)[0]
    tail = tail.split("\n👁", 1)[0]

    founders: list[dict[str, Any]] = []
    for line in [ln.strip() for ln in tail.splitlines() if ln.strip()]:
        # Пример: "Смагина Ирина Робертовна, ИНН 780419031060, доля 100%"
        fm = re.search(
            r"^(?P<name>.+?),\s*ИНН\s*(?P<inn>\d{10,12})(?:,\s*доля\s*(?P<share>[0-9]+)%?)?$",
            line,
        )
        if fm:
            founders.append(
                {
                    "name": fm.group("name").strip(),
                    "inn": fm.group("inn").strip(),
                    "share_percent": int(fm.group("share")) if fm.group("share") else None,
                    "raw_line": line,
                }
            )
        else:
            founders.append({"raw_line": line})

    return founders

