from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import google_sheets_client

CLIENTS_HEADERS = [
    "chat_id",
    "client_name",
    "is_active",
    "tariff_name",
    "price_per_success_row",
    "balance",
    "currency",
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
    "rows_successful",
    "price_per_success_row",
    "amount_charged",
    "balance_before",
    "balance_after",
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

BILLABLE_RESULT_FIELDS = (
    "found_phone",
    "summary_fio",
    "summary_email",
    "summary_telegram",
    "site_url",
)

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


def get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def format_decimal(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.01"))
    return format(normalized, "f")


def ensure_registry_sheets(service, config: google_sheets_client.GoogleSheetsConfig) -> None:
    google_sheets_client.ensure_worksheet_with_headers(
        service,
        config.spreadsheet_id,
        config.clients_sheet_name,
        CLIENTS_HEADERS,
        rows=200,
    )
    google_sheets_client.ensure_worksheet_with_headers(
        service,
        config.spreadsheet_id,
        config.billing_log_sheet_name,
        BILLING_LOG_HEADERS,
        rows=1000,
    )
    google_sheets_client.ensure_worksheet_with_headers(
        service,
        config.spreadsheet_id,
        config.audit_log_sheet_name,
        AUDIT_LOG_HEADERS,
        rows=1000,
    )


def build_client_record(row: google_sheets_client.WorksheetRow) -> dict[str, object]:
    values = row.values
    chat_id = str(values.get("chat_id", "")).strip()
    if not chat_id:
        raise RuntimeError(f"Empty chat_id in clients row {row.row_number}")

    price = google_sheets_client.normalize_decimal(values.get("price_per_success_row"), Decimal("0"))
    balance = google_sheets_client.normalize_decimal(values.get("balance"), Decimal("0"))
    client_name = str(values.get("client_name", "")).strip() or f"chat_{chat_id}"
    currency = str(values.get("currency", "")).strip() or "RUB"

    return {
        "row_number": row.row_number,
        "raw_values": values,
        "chat_id": chat_id,
        "client_name": client_name,
        "is_active": google_sheets_client.normalize_bool(values.get("is_active"), default=False),
        "tariff_name": str(values.get("tariff_name", "")).strip(),
        "price_per_success_row": price,
        "balance": balance,
        "currency": currency,
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

    price = client["price_per_success_row"]
    balance = client["balance"]
    allow_negative = bool(client["allow_negative_balance"])
    if not allow_negative and isinstance(price, Decimal) and isinstance(balance, Decimal) and price > balance:
        return {
            "ok": False,
            "status": "insufficient_balance",
            "message": (
                "Недостаточно баланса для запуска обработки. "
                f"Тариф за успешную строку: {format_decimal(price)} {client['currency']}, "
                f"остаток: {format_decimal(balance)} {client['currency']}."
            ),
            "client": client,
        }

    return {
        "ok": True,
        "status": "allowed",
        "message": "",
        "client": client,
    }


def has_billable_data(row: dict[str, str | None]) -> bool:
    for field in BILLABLE_RESULT_FIELDS:
        value = row.get(field)
        if value is not None and str(value).strip():
            return True
    return False


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
    price = client["price_per_success_row"]
    if not isinstance(price, Decimal):
        raise RuntimeError("Client price_per_success_row must be Decimal")
    amount_charged = price * successful_telegram_requests
    return {
        "rows_total": rows_total,
        "rows_successful": successful_telegram_requests,
        "successful_telegram_requests": successful_telegram_requests,
        "successful_inn_requests": request_metrics["successful_inn_requests"],
        "successful_phone_requests": request_metrics["successful_phone_requests"],
        "price_per_success_row": price,
        "amount_charged": amount_charged,
        "has_charge": amount_charged > 0,
    }


def calculate_max_possible_charge(
    client: dict[str, object],
    *,
    rows_total: int,
) -> Decimal:
    price = client["price_per_success_row"]
    if not isinstance(price, Decimal):
        raise RuntimeError("Client price_per_success_row must be Decimal")
    return price * rows_total


def build_client_sheet_row(client: dict[str, object], *, balance: Decimal) -> dict[str, str]:
    raw_values = dict(client["raw_values"])
    raw_values["chat_id"] = str(client["chat_id"])
    raw_values["client_name"] = str(client["client_name"])
    raw_values["is_active"] = "true" if bool(client["is_active"]) else "false"
    raw_values["tariff_name"] = str(client["tariff_name"])
    raw_values["price_per_success_row"] = format_decimal(client["price_per_success_row"])
    raw_values["balance"] = format_decimal(balance)
    raw_values["currency"] = str(client["currency"])
    raw_values["allow_negative_balance"] = "true" if bool(client["allow_negative_balance"]) else "false"
    raw_values["created_at"] = str(client["created_at"])
    raw_values["updated_at"] = now_timestamp()
    raw_values["notes"] = str(client["notes"])
    return {header: str(raw_values.get(header, "")) for header in CLIENTS_HEADERS}


def append_billing_log(
    service,
    config: google_sheets_client.GoogleSheetsConfig,
    entry: dict[str, str | int | Decimal | None],
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
    balance_before = client["balance"]
    amount_charged = charge["amount_charged"]
    if not isinstance(balance_before, Decimal) or not isinstance(amount_charged, Decimal):
        raise RuntimeError("Balance and amount_charged must be Decimal values")

    balance_after = balance_before - amount_charged
    updated_row = build_client_sheet_row(client, balance=balance_after)
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
            "rows_successful": str(charge["rows_successful"]),
            "price_per_success_row": format_decimal(charge["price_per_success_row"]),
            "amount_charged": format_decimal(amount_charged),
            "balance_before": format_decimal(balance_before),
            "balance_after": format_decimal(balance_after),
            "status": status,
            "result_worksheet_title": result_worksheet_title or "",
            "comment": comment,
        },
    )

    updated_client = dict(client)
    updated_client["balance"] = balance_after
    updated_client["updated_at"] = updated_row["updated_at"]
    return {
        "client": updated_client,
        "balance_before": balance_before,
        "balance_after": balance_after,
        "amount_charged": amount_charged,
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
            "rows_successful": "0",
            "price_per_success_row": (
                "0.00"
                if client is None
                else format_decimal(client["price_per_success_row"])
            ),
            "amount_charged": "0.00",
            "balance_before": (
                "0.00"
                if client is None
                else format_decimal(client["balance"])
            ),
            "balance_after": (
                "0.00"
                if client is None
                else format_decimal(client["balance"])
            ),
            "status": status,
            "result_worksheet_title": "",
            "comment": comment,
        },
    )

