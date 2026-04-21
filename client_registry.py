from __future__ import annotations

from datetime import datetime, timezone

import google_sheets_client

CLIENTS_HEADERS = [
    "chat_id",
    "client_name",
    "is_active",
    "request_balance",
    "allow_negative_balance",
    "created_at",
    "updated_at",
    "notes",
]

BILLING_LOG_HEADERS = [
    "created_at",
    "chat_id",
    "client_name",
    "file_name",
    "message_id",
    "rows_total",
    "successful_telegram_requests",
    "successful_inn_requests",
    "successful_phone_requests",
    "requests_charged",
    "request_balance_before",
    "request_balance_after",
    "status",
    "result_worksheet_title",
    "comment",
]

AUDIT_LOG_HEADERS = [
    "created_at",
    "chat_id",
    "message_id",
    "event_type",
    "file_name",
    "status",
    "details",
]

INN_REQUEST_NON_BILLABLE_STATUSES = {
    "",
    "input_error",
    "no_response",
    "not_found",
    "report_link_missing",
    "timeout",
}


def now_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_int(value: str | int | None, default: int = 0) -> int:
    if value in (None, ""):
        return default
    return int(str(value).strip())


def ensure_registry_sheets(service, config: google_sheets_client.GoogleSheetsConfig) -> None:
    google_sheets_client.ensure_worksheet_with_headers(
        service,
        config.spreadsheet_id,
        config.clients_sheet_name,
        CLIENTS_HEADERS,
        rows=200,
        rewrite_on_mismatch=True,
    )
    google_sheets_client.ensure_worksheet_with_headers(
        service,
        config.spreadsheet_id,
        config.billing_log_sheet_name,
        BILLING_LOG_HEADERS,
        rows=1000,
        rewrite_on_mismatch=True,
    )
    google_sheets_client.ensure_worksheet_with_headers(
        service,
        config.spreadsheet_id,
        config.audit_log_sheet_name,
        AUDIT_LOG_HEADERS,
        rows=1000,
        rewrite_on_mismatch=True,
    )


def build_client_record(row: google_sheets_client.WorksheetRow) -> dict[str, object]:
    values = row.values
    chat_id = str(values.get("chat_id", "")).strip()
    if not chat_id:
        raise RuntimeError(f"Empty chat_id in clients row {row.row_number}")

    client_name = str(values.get("client_name", "")).strip() or f"chat_{chat_id}"
    return {
        "row_number": row.row_number,
        "raw_values": values,
        "chat_id": chat_id,
        "client_name": client_name,
        "is_active": google_sheets_client.normalize_bool(values.get("is_active"), default=False),
        "request_balance": parse_int(values.get("request_balance"), 0),
        "allow_negative_balance": google_sheets_client.normalize_bool(
            values.get("allow_negative_balance"),
            default=False,
        ),
        "created_at": str(values.get("created_at", "")).strip(),
        "updated_at": str(values.get("updated_at", "")).strip(),
        "notes": str(values.get("notes", "")).strip(),
    }


def get_client_by_chat_id(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    chat_id: int | str,
) -> dict[str, object] | None:
    _, rows = google_sheets_client.read_table_rows(
        service,
        config.spreadsheet_id,
        config.clients_sheet_name,
    )
    chat_id_str = str(chat_id)
    for row in rows:
        if str(row.values.get("chat_id", "")).strip() == chat_id_str:
            return build_client_record(row)
    return None


def validate_client_access(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    chat_id: int | str,
) -> dict[str, object]:
    client = get_client_by_chat_id(service, config, chat_id)
    if client is None:
        return {
            "ok": False,
            "status": "unregistered",
            "message": "Этот чат не подключен к сервису. Обратитесь к менеджеру.",
            "client": None,
        }

    if not bool(client["is_active"]):
        return {
            "ok": False,
            "status": "inactive",
            "message": "Обработка для этого чата отключена. Обратитесь к менеджеру.",
            "client": client,
        }

    request_balance = int(client["request_balance"])
    allow_negative = bool(client["allow_negative_balance"])
    if not allow_negative and request_balance < 1:
        return {
            "ok": False,
            "status": "insufficient_balance",
            "message": (
                "Недостаточно баланса запросов для запуска обработки. "
                f"Остаток запросов: {request_balance}."
            ),
            "client": client,
        }

    return {
        "ok": True,
        "status": "allowed",
        "message": "",
        "client": client,
    }


def is_successful_inn_request(row: dict[str, str | None]) -> bool:
    if row.get("entity_type") == "phone":
        return False

    phone_lookup_status = (row.get("phone_lookup_status") or "").strip()
    if phone_lookup_status in INN_REQUEST_NON_BILLABLE_STATUSES:
        return False

    phone_source = (row.get("phone_source") or "").strip()
    if phone_source == "ip_web_flow":
        # For IP we bill the Telegram step if the bot returned a report link,
        # even if the subsequent web parsing failed.
        return True

    return phone_lookup_status == "found"


def is_successful_phone_request(row: dict[str, str | None]) -> bool:
    return (row.get("summary_status") or "").strip() == "found"


def count_successful_telegram_requests(results: list[dict[str, str | None]]) -> dict[str, int]:
    successful_inn_requests = sum(1 for row in results if is_successful_inn_request(row))
    successful_phone_requests = sum(1 for row in results if is_successful_phone_request(row))
    return {
        "successful_inn_requests": successful_inn_requests,
        "successful_phone_requests": successful_phone_requests,
        "successful_telegram_requests": successful_inn_requests + successful_phone_requests,
    }


def calculate_charge(
    client: dict[str, object],
    results: list[dict[str, str | None]],
) -> dict[str, object]:
    rows_total = len(results)
    request_metrics = count_successful_telegram_requests(results)
    successful_telegram_requests = request_metrics["successful_telegram_requests"]
    return {
        "rows_total": rows_total,
        "successful_telegram_requests": successful_telegram_requests,
        "successful_inn_requests": request_metrics["successful_inn_requests"],
        "successful_phone_requests": request_metrics["successful_phone_requests"],
        "requests_charged": successful_telegram_requests,
        "has_charge": successful_telegram_requests > 0,
    }


def calculate_max_possible_charge(
    *,
    phone_rows: int,
    inn_rows: int,
) -> int:
    return phone_rows + (inn_rows * 2)


def build_client_sheet_row(client: dict[str, object], *, request_balance: int) -> dict[str, str]:
    raw_values = dict(client["raw_values"])
    raw_values["chat_id"] = str(client["chat_id"])
    raw_values["client_name"] = str(client["client_name"])
    raw_values["is_active"] = "true" if bool(client["is_active"]) else "false"
    raw_values["request_balance"] = str(request_balance)
    raw_values["allow_negative_balance"] = "true" if bool(client["allow_negative_balance"]) else "false"
    raw_values["created_at"] = str(client["created_at"])
    raw_values["updated_at"] = now_timestamp()
    raw_values["notes"] = str(client["notes"])
    return {header: str(raw_values.get(header, "")) for header in CLIENTS_HEADERS}


def append_billing_log(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    entry: dict[str, str | int | None],
) -> None:
    google_sheets_client.append_dict_row(
        service,
        config.spreadsheet_id,
        config.billing_log_sheet_name,
        BILLING_LOG_HEADERS,
        entry,
    )


def append_audit_log(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    *,
    chat_id: int | str,
    message_id: int | str | None,
    event_type: str,
    file_name: str | None,
    status: str,
    details: str,
) -> None:
    google_sheets_client.append_dict_row(
        service,
        config.spreadsheet_id,
        config.audit_log_sheet_name,
        AUDIT_LOG_HEADERS,
        {
            "created_at": now_timestamp(),
            "chat_id": str(chat_id),
            "message_id": "" if message_id is None else str(message_id),
            "event_type": event_type,
            "file_name": file_name or "",
            "status": status,
            "details": details,
        },
    )


def apply_charge(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    client: dict[str, object],
    charge: dict[str, object],
    *,
    file_name: str,
    message_id: int | None,
    result_worksheet_title: str | None,
    comment: str,
    status: str,
) -> dict[str, object]:
    request_balance_before = int(client["request_balance"])
    requests_charged = int(charge["requests_charged"])
    request_balance_after = request_balance_before - requests_charged
    updated_row = build_client_sheet_row(client, request_balance=request_balance_after)
    google_sheets_client.update_dict_row(
        service,
        config.spreadsheet_id,
        config.clients_sheet_name,
        int(client["row_number"]),
        CLIENTS_HEADERS,
        updated_row,
    )

    append_billing_log(
        service,
        config,
        {
            "created_at": now_timestamp(),
            "chat_id": str(client["chat_id"]),
            "client_name": str(client["client_name"]),
            "file_name": file_name,
            "message_id": "" if message_id is None else str(message_id),
            "rows_total": str(charge["rows_total"]),
            "successful_telegram_requests": str(charge["successful_telegram_requests"]),
            "successful_inn_requests": str(charge["successful_inn_requests"]),
            "successful_phone_requests": str(charge["successful_phone_requests"]),
            "requests_charged": str(requests_charged),
            "request_balance_before": str(request_balance_before),
            "request_balance_after": str(request_balance_after),
            "status": status,
            "result_worksheet_title": result_worksheet_title or "",
            "comment": comment,
        },
    )

    updated_client = dict(client)
    updated_client["request_balance"] = request_balance_after
    updated_client["updated_at"] = updated_row["updated_at"]
    return {
        "client": updated_client,
        "request_balance_before": request_balance_before,
        "request_balance_after": request_balance_after,
        "requests_charged": requests_charged,
    }


def log_blocked_attempt(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    *,
    chat_id: int | str,
    message_id: int | None,
    file_name: str,
    status: str,
    comment: str,
    client: dict[str, object] | None,
) -> None:
    append_billing_log(
        service,
        config,
        {
            "created_at": now_timestamp(),
            "chat_id": str(chat_id),
            "client_name": "" if client is None else str(client["client_name"]),
            "file_name": file_name,
            "message_id": "" if message_id is None else str(message_id),
            "rows_total": "0",
            "successful_telegram_requests": "0",
            "successful_inn_requests": "0",
            "successful_phone_requests": "0",
            "requests_charged": "0",
            "request_balance_before": (
                "0"
                if client is None
                else str(client["request_balance"])
            ),
            "request_balance_after": (
                "0"
                if client is None
                else str(client["request_balance"])
            ),
            "status": status,
            "result_worksheet_title": "",
            "comment": comment,
        },
    )

