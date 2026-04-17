import logging
import os
import random
import re
import time
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
    retry_attempts: int
    retry_base_delay_seconds: float
    retry_max_delay_seconds: float


@dataclass(frozen=True)
class SpreadsheetInfo:
    spreadsheet_id: str
    title: str
    worksheet_titles: tuple[str, ...]


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
