import sqlite3
from typing import Any

from database.sqlite_db import utc_now_iso


def insert_source_query(conn: sqlite3.Connection, query_text: str) -> int:
    now = utc_now_iso()
    cur = conn.execute(
        "INSERT INTO source_queries(query_text, created_at) VALUES(?, ?)",
        (query_text, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def upsert_inn_result(
    conn: sqlite3.Connection,
    *,
    source_query_id: int,
    inn: str,
    raw_text: str,
    ogrn: str | None = None,
    company_name: str | None = None,
    okved: str | None = None,
    reg_date: str | None = None,
    company_status: str | None = None,
    director_name: str | None = None,
    director_inn: str | None = None,
    revenue_2024: int | None = None,
    income_2024: int | None = None,
    expenses_2024: int | None = None,
    authorized_capital: int | None = None,
    address: str | None = None,
    founders_json: str | None = None,
) -> None:
    now = utc_now_iso()

    conn.execute(
        """
        INSERT INTO inn_results(
            inn,
            ogrn,
            company_name,
            okved,
            reg_date,
            company_status,
            director_name,
            director_inn,
            revenue_2024,
            income_2024,
            expenses_2024,
            authorized_capital,
            address,
            founders_json,
            raw_text,
            source_query_id,
            created_at,
            updated_at
        )
        VALUES(
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(inn) DO UPDATE SET
            ogrn=excluded.ogrn,
            company_name=excluded.company_name,
            okved=excluded.okved,
            reg_date=excluded.reg_date,
            company_status=excluded.company_status,
            director_name=excluded.director_name,
            director_inn=excluded.director_inn,
            revenue_2024=excluded.revenue_2024,
            income_2024=excluded.income_2024,
            expenses_2024=excluded.expenses_2024,
            authorized_capital=excluded.authorized_capital,
            address=excluded.address,
            founders_json=excluded.founders_json,
            raw_text=excluded.raw_text,
            source_query_id=excluded.source_query_id,
            updated_at=excluded.updated_at
        ;
        """,
        (
            inn,
            ogrn,
            company_name,
            okved,
            reg_date,
            company_status,
            director_name,
            director_inn,
            revenue_2024,
            income_2024,
            expenses_2024,
            authorized_capital,
            address,
            founders_json,
            raw_text,
            source_query_id,
            now,
            now,
        ),
    )
    conn.commit()


def build_upsert_kwargs(parsed: dict[str, Any]) -> dict[str, Any]:
    # Убираем None для обязательных полей, чтобы не случайно уронить upsert.
    if not parsed.get("inn"):
        raise ValueError("В тексте результата не найден ИНН (inn).")
    if not parsed.get("raw_text"):
        raise ValueError("raw_text обязателен.")

    return {
        "inn": parsed.get("inn"),
        "ogrn": parsed.get("ogrn"),
        "company_name": parsed.get("company_name"),
        "okved": parsed.get("okved"),
        "reg_date": parsed.get("reg_date"),
        "company_status": parsed.get("company_status"),
        "director_name": parsed.get("director_name"),
        "director_inn": parsed.get("director_inn"),
        "revenue_2024": parsed.get("revenue_2024"),
        "income_2024": parsed.get("income_2024"),
        "expenses_2024": parsed.get("expenses_2024"),
        "authorized_capital": parsed.get("authorized_capital"),
        "address": parsed.get("address"),
        "founders_json": parsed.get("founders_json"),
        "raw_text": parsed.get("raw_text"),
    }

