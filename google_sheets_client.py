import logging
import os
import random
import re
import time
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from dotenv import load_dotenv
from google.auth.exceptions import TransportError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import HttpLib2Error

load_dotenv()

GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
WORKSHEET_TITLE_MAX_LEN = 100
INVALID_WORKSHEET_TITLE_RE = re.compile(r"[\[\]\:\*\?\/\\]")
RETRYABLE_HTTP_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class GoogleSheetsConfig:
    credentials_file: Path
    spreadsheet_id: str
    clients_sheet_name: str
    billing_log_sheet_name: str
    audit_log_sheet_name: str
    retry_attempts: int
    retry_base_delay_seconds: float
    retry_max_delay_seconds: float


@dataclass(frozen=True)
class SpreadsheetInfo:
    spreadsheet_id: str
    title: str
    worksheet_titles: tuple[str, ...]


@dataclass(frozen=True)
class WorksheetRow:
    row_number: int
    values: dict[str, str]


def setup_logging() -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("google_sheets")


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required .env value: {name}")
    return value


def get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def get_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return float(raw)


def load_config() -> GoogleSheetsConfig:
    credentials_raw = get_required_env("GOOGLE_CREDENTIALS_FILE")
    spreadsheet_id = get_required_env("GOOGLE_SHEET_ID")
    credentials_file = Path(credentials_raw).expanduser()
    if not credentials_file.exists():
        raise RuntimeError(f"Google credentials file not found: {credentials_file}")
    return GoogleSheetsConfig(
        credentials_file=credentials_file,
        spreadsheet_id=spreadsheet_id,
        clients_sheet_name=os.getenv("GOOGLE_CLIENTS_SHEET_NAME", "clients").strip() or "clients",
        billing_log_sheet_name=os.getenv("GOOGLE_BILLING_LOG_SHEET_NAME", "billing_log").strip() or "billing_log",
        audit_log_sheet_name=os.getenv("GOOGLE_AUDIT_LOG_SHEET_NAME", "bot_audit").strip() or "bot_audit",
        retry_attempts=get_int_env("GOOGLE_SHEETS_RETRY_ATTEMPTS", 5),
        retry_base_delay_seconds=get_float_env("GOOGLE_SHEETS_RETRY_BASE_DELAY_SECONDS", 1.0),
        retry_max_delay_seconds=get_float_env("GOOGLE_SHEETS_RETRY_MAX_DELAY_SECONDS", 20.0),
    )


def build_credentials(config: GoogleSheetsConfig) -> Credentials:
    return Credentials.from_service_account_file(
        str(config.credentials_file),
        scopes=[GOOGLE_SHEETS_SCOPE],
    )


def build_sheets_service(config: GoogleSheetsConfig):
    credentials = build_credentials(config)
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, HttpError):
        return exc.resp.status in RETRYABLE_HTTP_STATUSES
    return isinstance(exc, (TransportError, HttpLib2Error, TimeoutError, OSError))


def execute_with_retries(request, config: GoogleSheetsConfig, *, log: logging.Logger | None = None, operation: str) -> dict:
    query_log = log or logging.getLogger("google_sheets")
    last_error: Exception | None = None

    for attempt in range(1, config.retry_attempts + 1):
        try:
            return request.execute()
        except Exception as exc:
            last_error = exc
            if attempt >= config.retry_attempts or not is_retryable_exception(exc):
                raise

            base_delay = config.retry_base_delay_seconds * (2 ** (attempt - 1))
            capped_delay = min(base_delay, config.retry_max_delay_seconds)
            sleep_seconds = capped_delay + random.uniform(0, min(0.5, capped_delay / 2))
            query_log.warning(
                "Google Sheets %s failed on attempt %s/%s: %s. Retry in %.2fs",
                operation,
                attempt,
                config.retry_attempts,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    assert last_error is not None
    raise last_error


def get_spreadsheet_info(service, spreadsheet_id: str) -> SpreadsheetInfo:
    config = load_config()
    response = execute_with_retries(
        service.spreadsheets().get(spreadsheetId=spreadsheet_id),
        config,
        operation="spreadsheets.get",
    )
    sheets = response.get("sheets", [])
    worksheet_titles = tuple(
        sheet.get("properties", {}).get("title", "Untitled")
        for sheet in sheets
    )
    return SpreadsheetInfo(
        spreadsheet_id=spreadsheet_id,
        title=response.get("properties", {}).get("title", "Untitled"),
        worksheet_titles=worksheet_titles,
    )


def check_connection(log: logging.Logger | None = None) -> SpreadsheetInfo:
    query_log = log or logging.getLogger("google_sheets")
    config = load_config()
    service = build_sheets_service(config)
    info = get_spreadsheet_info(service, config.spreadsheet_id)
    query_log.info(
        "Connected to Google Sheets: title=%r worksheets=%s credentials=%s",
        info.title,
        len(info.worksheet_titles),
        config.credentials_file,
    )
    return info


def sanitize_worksheet_title(title: str) -> str:
    cleaned = INVALID_WORKSHEET_TITLE_RE.sub("_", title).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = "results"
    return cleaned[:WORKSHEET_TITLE_MAX_LEN]


def make_unique_worksheet_title(existing_titles: Sequence[str], base_title: str) -> str:
    normalized_base = sanitize_worksheet_title(base_title)
    existing = set(existing_titles)
    if normalized_base not in existing:
        return normalized_base

    suffix = 2
    while True:
        candidate_suffix = f"_{suffix}"
        max_base_len = WORKSHEET_TITLE_MAX_LEN - len(candidate_suffix)
        candidate = f"{normalized_base[:max_base_len]}{candidate_suffix}"
        if candidate not in existing:
            return candidate
        suffix += 1


def column_number_to_letter(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be >= 1")

    letters: list[str] = []
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters.append(chr(ord("A") + remainder))
    return "".join(reversed(letters))


def normalize_bool(value: str | bool | None, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def normalize_decimal(value: str | int | float | Decimal | None, default: Decimal | None = None) -> Decimal:
    if value in (None, ""):
        if default is not None:
            return default
        raise ValueError("Decimal value is required")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        if default is not None:
            return default
        raise ValueError(f"Invalid decimal value: {value}") from exc


def get_sheet_values(service, spreadsheet_id: str, worksheet_title: str) -> list[list[str]]:
    config = load_config()
    response = execute_with_retries(
        service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{worksheet_title}'",
        ),
        config,
        operation="spreadsheets.values.get",
    )
    values = response.get("values", [])
    return [
        ["" if value is None else str(value) for value in row]
        for row in values
    ]


def read_table_rows(service, spreadsheet_id: str, worksheet_title: str) -> tuple[list[str], list[WorksheetRow]]:
    values = get_sheet_values(service, spreadsheet_id, worksheet_title)
    if not values:
        return [], []

    headers = [str(value).strip() for value in values[0]]
    rows: list[WorksheetRow] = []
    header_count = len(headers)
    for row_index, row_values in enumerate(values[1:], start=2):
        padded = list(row_values) + [""] * max(0, header_count - len(row_values))
        row_dict = {
            header: padded[position] if position < len(padded) else ""
            for position, header in enumerate(headers)
            if header
        }
        if not any(value.strip() for value in row_dict.values()):
            continue
        rows.append(WorksheetRow(row_number=row_index, values=row_dict))
    return headers, rows


def append_rows(service, spreadsheet_id: str, worksheet_title: str, rows: Iterable[Sequence[str | None]]) -> None:
    config = load_config()
    normalized_rows = [
        ["" if value is None else str(value) for value in row]
        for row in rows
    ]
    if not normalized_rows:
        return

    execute_with_retries(
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{worksheet_title}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": normalized_rows},
        ),
        config,
        operation="spreadsheets.values.append",
    )


def update_row_values(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
    row_number: int,
    values: Sequence[str | None],
) -> None:
    config = load_config()
    if row_number < 1:
        raise ValueError("Row number must be >= 1")
    if not values:
        return

    end_column = column_number_to_letter(len(values))
    normalized_values = [["" if value is None else str(value) for value in values]]
    execute_with_retries(
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{worksheet_title}'!A{row_number}:{end_column}{row_number}",
            valueInputOption="RAW",
            body={"values": normalized_values},
        ),
        config,
        operation="spreadsheets.values.update.row",
    )


def clear_worksheet(service, spreadsheet_id: str, worksheet_title: str) -> None:
    config = load_config()
    execute_with_retries(
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{worksheet_title}'",
            body={},
        ),
        config,
        operation="spreadsheets.values.clear",
    )


def ensure_worksheet_with_headers(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
    headers: Sequence[str],
    *,
    rows: int = 1000,
    cols: int | None = None,
    rewrite_on_mismatch: bool = False,
) -> str:
    if not headers:
        raise ValueError("Headers are required")

    info = get_spreadsheet_info(service, spreadsheet_id)
    normalized_title = sanitize_worksheet_title(worksheet_title)
    if normalized_title not in info.worksheet_titles:
        create_worksheet(
            service,
            spreadsheet_id,
            normalized_title,
            rows=rows,
            cols=cols or len(headers),
        )

    existing_values = get_sheet_values(service, spreadsheet_id, normalized_title)
    expected_headers = [str(header) for header in headers]
    if not existing_values:
        write_rows(service, spreadsheet_id, normalized_title, [expected_headers])
        return normalized_title

    current_headers = [str(value).strip() for value in existing_values[0]]
    if current_headers != expected_headers:
        if rewrite_on_mismatch:
            clear_worksheet(service, spreadsheet_id, normalized_title)
            write_rows(service, spreadsheet_id, normalized_title, [expected_headers])
            return normalized_title
        raise RuntimeError(
            f"Worksheet {normalized_title!r} has unexpected headers. "
            f"Expected {expected_headers}, got {current_headers}"
        )
    return normalized_title


def append_dict_row(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
    headers: Sequence[str],
    row: dict[str, str | int | float | Decimal | None],
) -> None:
    append_rows(
        service,
        spreadsheet_id,
        worksheet_title,
        [[row.get(header) for header in headers]],
    )


def update_dict_row(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
    row_number: int,
    headers: Sequence[str],
    row: dict[str, str | int | float | Decimal | None],
) -> None:
    update_row_values(
        service,
        spreadsheet_id,
        worksheet_title,
        row_number,
        [row.get(header) for header in headers],
    )


def create_worksheet(service, spreadsheet_id: str, title: str, *, rows: int = 1000, cols: int = 26) -> str:
    config = load_config()
    info = get_spreadsheet_info(service, spreadsheet_id)
    worksheet_title = make_unique_worksheet_title(info.worksheet_titles, title)
    request_body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": worksheet_title,
                        "gridProperties": {
                            "rowCount": rows,
                            "columnCount": cols,
                        },
                    }
                }
            }
        ]
    }
    execute_with_retries(
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body,
        ),
        config,
        operation="spreadsheets.batchUpdate.addSheet",
    )
    return worksheet_title


def write_rows(service, spreadsheet_id: str, worksheet_title: str, rows: Iterable[Sequence[str | None]]) -> None:
    config = load_config()
    normalized_rows = [
        ["" if value is None else str(value) for value in row]
        for row in rows
    ]
    if not normalized_rows:
        return

    execute_with_retries(
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{worksheet_title}'!A1",
            valueInputOption="RAW",
            body={"values": normalized_rows},
        ),
        config,
        operation="spreadsheets.values.update",
    )


def append_table(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
    headers: Sequence[str],
    rows: Iterable[Sequence[str | None]],
) -> None:
    write_rows(service, spreadsheet_id, worksheet_title, [headers, *list(rows)])


def main() -> None:
    log = setup_logging()
    try:
        config = load_config()
        info = check_connection(log)
    except HttpError as exc:
        raise SystemExit(f"Google Sheets API error: {exc}") from exc
    except Exception as exc:
        raise SystemExit(f"Google Sheets connection failed: {exc}") from exc

    print("Google Sheets connection OK")
    print(f"credentials_file: {config.credentials_file}")
    print(f"spreadsheet_id: {info.spreadsheet_id}")
    print(f"spreadsheet_title: {info.title}")
    print(f"worksheets: {len(info.worksheet_titles)}")
    if info.worksheet_titles:
        print("worksheet_titles:")
        for title in info.worksheet_titles:
            print(f"- {title}")


if __name__ == "__main__":
    main()
