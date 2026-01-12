import os
import sqlite3
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    # Храним исходные запросы (что именно пользователь отправил боту).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_queries (
            id INTEGER PRIMARY KEY,
            query_text TEXT NOT NULL,
            created_at DATETIME NOT NULL
        );
        """
    )

    # Результаты /inn. inn делаем UNIQUE, чтобы можно было делать UPSERT.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inn_results (
            id INTEGER PRIMARY KEY,
            inn TEXT NOT NULL UNIQUE,
            ogrn TEXT,
            company_name TEXT,
            okved TEXT,
            reg_date TEXT,
            company_status TEXT,
            director_name TEXT,
            director_inn TEXT,
            revenue_2024 INTEGER,
            income_2024 INTEGER,
            expenses_2024 INTEGER,
            authorized_capital INTEGER,
            address TEXT,
            founders_json TEXT,
            raw_text TEXT NOT NULL,
            source_query_id INTEGER NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            FOREIGN KEY (source_query_id) REFERENCES source_queries (id)
        );
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inn_results_source_query_id
        ON inn_results(source_query_id);
        """
    )

