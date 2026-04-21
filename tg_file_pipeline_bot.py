import asyncio
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
from telethon import TelegramClient

import client_registry
import google_sheets_client
import run_pipeline
from telethon_client_factory import build_telegram_client, open_url

load_dotenv()

SUPPORTED_EXTENSIONS = {".xlsx", ".xlsm", ".csv"}
TEMPLATE_FILENAME_PREFIXES = ("шаблон", "template")


def setup_logging() -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    return logging.getLogger("tg_file_pipeline_bot")


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required .env value: {name}")
    return value


def get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def telegram_api_request(token: str, method: str, params: dict | None = None) -> dict:
    query = urllib.parse.urlencode(params or {})
    url = f"https://api.telegram.org/bot{token}/{method}"
    if query:
        url = f"{url}?{query}"

    with open_url(url, timeout=70) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if not data.get("ok"):
        raise RuntimeError(f"Telegram Bot API error: {data}")
    return data


def telegram_api_post_multipart(
    token: str,
    method: str,
    fields: dict[str, str],
    file_field: str,
    file_path: Path,
    filename: str | None = None,
) -> dict:
    boundary = f"----CodexBoundary{uuid4().hex}"
    file_name = filename or file_path.name
    body = bytearray()

    for key, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"))
        body.extend(str(value).encode("utf-8"))
        body.extend(b"\r\n")

    body.extend(f"--{boundary}\r\n".encode("utf-8"))
    body.extend(
        (
            f'Content-Disposition: form-data; name="{file_field}"; filename="{file_name}"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode("utf-8")
    )
    body.extend(file_path.read_bytes())
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )

    with open_url(request, timeout=120) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if not data.get("ok"):
        raise RuntimeError(f"Telegram Bot API error: {data}")
    return data


async def bot_api_request(token: str, method: str, params: dict | None = None) -> dict:
    return await asyncio.to_thread(telegram_api_request, token, method, params)


async def bot_api_post_multipart(
    token: str,
    method: str,
    fields: dict[str, str],
    file_field: str,
    file_path: Path,
    filename: str | None = None,
) -> dict:
    return await asyncio.to_thread(
        telegram_api_post_multipart,
        token,
        method,
        fields,
        file_field,
        file_path,
        filename,
    )


def extract_message(update: dict) -> dict | None:
    return update.get("message") or update.get("edited_message")


def is_supported_filename(filename: str) -> bool:
    return Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS


def is_template_filename(filename: str) -> bool:
    stem = Path(filename).stem.strip().casefold()
    return any(stem.startswith(prefix) for prefix in TEMPLATE_FILENAME_PREFIXES)


def sanitize_stem(name: str) -> str:
    sanitized = re.sub(r"[^A-Za-zА-Яа-я0-9._-]+", "_", name).strip("._")
    return sanitized or f"job_{int(time.time())}"


def build_google_worksheet_title(file_name: str) -> str:
    stem = sanitize_stem(Path(file_name).stem)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return f"{stem}_{timestamp}"


async def safe_registry_side_effect(
    func,
    log: logging.Logger,
    *args,
    operation: str,
    **kwargs,
):
    try:
        return await asyncio.to_thread(func, *args, **kwargs)
    except Exception:
        log.exception("Registry operation failed: %s", operation)
        return None


async def send_message(token: str, chat_id: int, text: str, reply_to_message_id: int | None = None) -> dict:
    params: dict[str, str | int] = {"chat_id": chat_id, "text": text}
    if reply_to_message_id:
        params["reply_to_message_id"] = reply_to_message_id
    return await bot_api_request(token, "sendMessage", params)


async def edit_message(token: str, chat_id: int, message_id: int, text: str) -> dict:
    params: dict[str, str | int] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    return await bot_api_request(token, "editMessageText", params)


async def safe_edit_message(token: str, chat_id: int, message_id: int, text: str, log: logging.Logger) -> None:
    try:
        await edit_message(token, chat_id, message_id, text)
    except Exception as exc:
        log.warning("Failed to edit status message: %s", exc)


async def send_document(
    token: str,
    chat_id: int,
    file_path: Path,
    *,
    caption: str | None = None,
    reply_to_message_id: int | None = None,
) -> dict:
    fields: dict[str, str] = {"chat_id": str(chat_id)}
    if caption:
        fields["caption"] = caption
    if reply_to_message_id:
        fields["reply_to_message_id"] = str(reply_to_message_id)
    return await bot_api_post_multipart(token, "sendDocument", fields, "document", file_path)


async def download_file(token: str, file_id: str, destination: Path) -> Path:
    file_info = await bot_api_request(token, "getFile", {"file_id": file_id})
    file_path = file_info["result"]["file_path"]
    download_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    data = await asyncio.to_thread(lambda: open_url(download_url, timeout=120).read())
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    return destination


def count_statuses(rows: list[dict[str, str | None]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = row.get("pipeline_status") or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def has_value(value: str | None) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def count_present(rows: list[dict[str, str | None]], key: str) -> int:
    return sum(1 for row in rows if has_value(row.get(key)))


def count_present_any(rows: list[dict[str, str | None]], keys: tuple[str, ...]) -> int:
    return sum(1 for row in rows if any(has_value(row.get(key)) for key in keys))


def build_collection_metrics(rows: list[dict[str, str | None]]) -> list[tuple[str, int]]:
    return [
        ("Телефоны", count_present(rows, "found_phone")),
        ("ФИО", count_present(rows, "summary_fio")),
        ("Email", count_present(rows, "summary_email")),
        ("Telegram", count_present(rows, "summary_telegram")),
        ("Telegram URL", count_present(rows, "telegram_url")),
        ("Записная книжка", count_present(rows, "phone_books")),
        ("WhatsApp", count_present(rows, "whatsapp_url")),
        ("VK", count_present_any(rows, ("vk_text", "vk_urls"))),
        ("Instagram", count_present_any(rows, ("instagram_text", "instagram_urls"))),
        ("OK", count_present_any(rows, ("ok_text", "ok_urls"))),
        ("MAX", count_present_any(rows, ("max_text", "max_url"))),
        ("Ссылки на отчёт", count_present(rows, "site_url")),
    ]


def build_query_metrics(rows: list[dict[str, str | None]]) -> list[tuple[str, int]]:
    inn_queries = 0
    phone_queries = 0

    for row in rows:
        entity_type = row.get("entity_type")
        if entity_type == "phone":
            phone_queries += 1
            continue

        if has_value(row.get("source_inn")):
            inn_queries += 1
            if has_value(row.get("found_phone")):
                phone_queries += 1

    return [
        ("По ИНН", inn_queries),
        ("По телефону", phone_queries),
        ("Всего", inn_queries + phone_queries),
    ]


def format_metric_lines(items: list[tuple[str, int]]) -> str:
    return "\n".join(f"{label}: {value}" for label, value in items)


def build_completion_report(rows: list[dict[str, str | None]]) -> tuple[str, str]:
    status_counts = count_statuses(rows)
    collection_metrics = build_collection_metrics(rows)
    query_metrics = build_query_metrics(rows)
    status_metrics = sorted(status_counts.items())

    detailed = (
        "Обработка завершена.\n"
        f"Строк: {len(rows)}\n\n"
        "Собрано:\n"
        f"{format_metric_lines(collection_metrics)}\n\n"
        "Запросы к Telegram:\n"
        f"{format_metric_lines(query_metrics)}\n\n"
        "Статусы:\n"
        f"{format_metric_lines(status_metrics)}\n\n"
        "Отправляю результат."
    )

    short = (
        f"Строк: {len(rows)}\n"
        f"Запросы: {query_metrics[-1][1]} "
        f"(ИНН {query_metrics[0][1]} / тел {query_metrics[1][1]})\n"
        f"Телефоны: {collection_metrics[0][1]}, ФИО: {collection_metrics[1][1]}, "
        f"Email: {collection_metrics[2][1]}, отчёты: {collection_metrics[-1][1]}"
    )
    return detailed, short


def build_billing_report(
    client: dict[str, object],
    charge: dict[str, object],
    *,
    billing_enabled: bool,
    balance_after=None,
    error_message: str | None = None,
) -> tuple[str, str]:
    if not billing_enabled:
        return "Биллинг:\nОтключен", "Биллинг: отключен"

    currency = str(client.get("currency") or "RUB")
    price = client_registry.format_decimal(charge["price_per_success_row"])
    if error_message:
        detailed = (
            "Биллинг:\n"
            f"Клиент: {client['client_name']}\n"
            f"Успешных строк: {charge['rows_successful']}\n"
            f"Тариф: {price} {currency}\n"
            f"Ошибка списания: {error_message}"
        )
        short = f"Биллинг: ошибка ({error_message})"
        return detailed, short

    detailed = (
        "Биллинг:\n"
        f"Клиент: {client['client_name']}\n"
        f"Успешных строк: {charge['rows_successful']}\n"
        f"Тариф: {price} {currency}\n"
        f"Списано: {client_registry.format_decimal(charge['amount_charged'])} {currency}\n"
        f"Остаток: {client_registry.format_decimal(balance_after)} {currency}"
    )
    short = (
        f"Биллинг: {charge['rows_successful']} строк, "
        f"списано {client_registry.format_decimal(charge['amount_charged'])} {currency}, "
        f"остаток {client_registry.format_decimal(balance_after)} {currency}"
    )
    return detailed, short


def export_results_to_google_sheets(file_name: str, rows: list[dict[str, str | None]], log: logging.Logger) -> str:
    config = google_sheets_client.load_config()
    service = google_sheets_client.build_sheets_service(config)
    worksheet_title = google_sheets_client.create_worksheet(
        service,
        config.spreadsheet_id,
        build_google_worksheet_title(file_name),
        rows=max(1000, len(rows) + 10),
        cols=max(26, len(run_pipeline.PIPELINE_FIELDNAMES) + 2),
    )
    headers = [run_pipeline.PIPELINE_COLUMN_LABELS.get(field, field) for field in run_pipeline.PIPELINE_FIELDNAMES]
    values = [
        [row.get(field) for field in run_pipeline.PIPELINE_FIELDNAMES]
        for row in rows
    ]
    google_sheets_client.append_table(
        service,
        config.spreadsheet_id,
        worksheet_title,
        headers,
        values,
    )
    log.info("Google Sheets export completed: spreadsheet=%s worksheet=%s", config.spreadsheet_id, worksheet_title)
    return worksheet_title


async def process_input_file(
    client: TelegramClient,
    bot_entity,
    *,
    input_rows: list[run_pipeline.InputRow],
    input_path: Path,
    output_csv: Path,
    output_xlsx: Path,
    token: str,
    chat_id: int,
    status_message_id: int,
    log: logging.Logger,
    headless: bool,
    debug_dir: Path,
    step_delay_seconds: int,
    row_delay_seconds: int,
    bot_message_echo: bool,
) -> list[dict[str, str | None]]:
    rows = input_rows
    if not rows:
        raise RuntimeError("Во входном файле нет строк для обработки")

    results: list[dict[str, str | None]] = []
    await safe_edit_message(
        token,
        chat_id,
        status_message_id,
        f"Файл получен. Найдено строк: {len(rows)}.\nНачинаю обработку.",
        log,
    )

    for index, item in enumerate(rows, start=1):
        entity_type = run_pipeline.detect_entity_type(item.source_name)
        await safe_edit_message(
            token,
            chat_id,
            status_message_id,
            (
                f"Обработка {index}/{len(rows)}\n"
                f"Тип: {entity_type}\n"
                f"ИНН: {item.source_inn or 'не распознан'}\n"
                f"Название: {item.source_name or '-'}"
            ),
            log,
        )

        row = await run_pipeline.resolve_row(
            client,
            bot_entity,
            item,
            log=log,
            headless=headless,
            debug_dir=debug_dir,
            step_delay_seconds=step_delay_seconds,
            bot_message_echo=bot_message_echo,
        )
        run_pipeline.append_pipeline_result(output_csv, row)
        results.append(row)

        await safe_edit_message(
            token,
            chat_id,
            status_message_id,
            (
                f"Строка {index}/{len(rows)} завершена и сохранена.\n"
                f"Статус: {row['pipeline_status']}\n"
                f"Телефон: {row['found_phone'] or 'не найден'}"
            ),
            log,
        )

        if index < len(rows) and row_delay_seconds > 0:
            await asyncio.sleep(row_delay_seconds)

    run_pipeline.write_pipeline_results_xlsx(output_xlsx, results)
    return results


async def handle_document_message(
    client: TelegramClient,
    bot_entity,
    *,
    token: str,
    chat_id: int,
    message: dict,
    jobs_dir: Path,
    sheets_config: google_sheets_client.GoogleSheetsConfig,
    registry_service,
    google_sheets_enabled: bool,
    billing_enabled: bool,
    headless: bool,
    debug_dir: Path,
    step_delay_seconds: int,
    row_delay_seconds: int,
    bot_message_echo: bool,
    log: logging.Logger,
) -> None:
    document = message.get("document") or {}
    file_name = document.get("file_name") or "input.xlsx"
    message_id = message.get("message_id")
    unregistered_chat_mode = os.getenv("UNREGISTERED_CHAT_MODE", "reject").strip().lower() or "reject"

    try:
        access = await asyncio.to_thread(
            client_registry.validate_client_access,
            registry_service,
            sheets_config,
            chat_id,
        )
    except Exception as exc:
        log.exception("Failed to validate client access")
        await send_message(
            token,
            chat_id,
            f"Не удалось проверить настройки клиента:\n{exc}",
            reply_to_message_id=message_id,
        )
        return

    client_config = access.get("client")
    if not access["ok"]:
        details = str(access["message"])
        if access["status"] == "unregistered" and unregistered_chat_mode != "reject":
            log.info("Ignoring unregistered chat %s due to UNREGISTERED_CHAT_MODE=%s", chat_id, unregistered_chat_mode)
            return
        await safe_registry_side_effect(
            client_registry.log_blocked_attempt,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            file_name=file_name,
            status=str(access["status"]),
            comment=details,
            client=client_config,
            operation="log_blocked_attempt",
        )
        await safe_registry_side_effect(
            client_registry.append_audit_log,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            event_type="document_rejected",
            file_name=file_name,
            status=str(access["status"]),
            details=details,
            operation="append_audit_log.document_rejected",
        )
        await send_message(
            token,
            chat_id,
            details,
            reply_to_message_id=message_id,
        )
        return

    assert client_config is not None
    await safe_registry_side_effect(
        client_registry.append_audit_log,
        log,
        registry_service,
        sheets_config,
        chat_id=chat_id,
        message_id=message_id,
        event_type="document_received",
        file_name=file_name,
        status="accepted",
        details=f"Клиент: {client_config['client_name']}",
        operation="append_audit_log.document_received",
    )

    if not is_supported_filename(file_name):
        message_text = "Поддерживаются только файлы .xlsx, .xlsm и .csv"
        await safe_registry_side_effect(
            client_registry.log_blocked_attempt,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            file_name=file_name,
            status="unsupported_file",
            comment=message_text,
            client=client_config,
            operation="log_blocked_attempt.unsupported_file",
        )
        await send_message(
            token,
            chat_id,
            message_text,
            reply_to_message_id=message_id,
        )
        return

    if is_template_filename(file_name):
        message_text = (
            "Этот файл выглядит как шаблон и не запускается в обработку. "
            "Переименуйте файл и отправьте его заново, если хотите начать поиск."
        )
        await safe_registry_side_effect(
            client_registry.log_blocked_attempt,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            file_name=file_name,
            status="template_file",
            comment=message_text,
            client=client_config,
            operation="log_blocked_attempt.template_file",
        )
        await send_message(
            token,
            chat_id,
            message_text,
            reply_to_message_id=message_id,
        )
        return

    job_id = f"{int(time.time())}_{uuid4().hex[:8]}"
    job_dir = jobs_dir / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    input_path = job_dir / file_name
    result_stem = sanitize_stem(Path(file_name).stem)
    output_csv = job_dir / f"{result_stem}_result.csv"
    output_xlsx = job_dir / f"{result_stem}_result.xlsx"

    status = await send_message(
        token,
        chat_id,
        f"Файл {file_name} принят. Скачиваю и готовлю обработку...",
        reply_to_message_id=message_id,
    )
    status_message_id = status["result"]["message_id"]

    try:
        await safe_registry_side_effect(
            client_registry.append_audit_log,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            event_type="processing_started",
            file_name=file_name,
            status="started",
            details=f"Клиент: {client_config['client_name']}",
            operation="append_audit_log.processing_started",
        )
        await download_file(token, document["file_id"], input_path)
        input_rows = run_pipeline.load_input_rows(input_path)
        max_possible_charge = client_registry.calculate_max_possible_charge(
            client_config,
            rows_total=len(input_rows),
        )
        if (
            not bool(client_config["allow_negative_balance"])
            and max_possible_charge > client_config["balance"]
        ):
            currency = str(client_config.get("currency") or "RUB")
            raise RuntimeError(
                "Недостаточно баланса для безопасного запуска файла. "
                f"Максимально возможное списание: {client_registry.format_decimal(max_possible_charge)} {currency}, "
                f"остаток: {client_registry.format_decimal(client_config['balance'])} {currency}."
            )
        results = await process_input_file(
            client,
            bot_entity,
            input_rows=input_rows,
            input_path=input_path,
            output_csv=output_csv,
            output_xlsx=output_xlsx,
            token=token,
            chat_id=chat_id,
            status_message_id=status_message_id,
            log=log,
            headless=headless,
            debug_dir=debug_dir,
            step_delay_seconds=step_delay_seconds,
            row_delay_seconds=row_delay_seconds,
            bot_message_echo=bot_message_echo,
        )
    except Exception as exc:
        log.exception("File processing failed")
        await safe_registry_side_effect(
            client_registry.log_blocked_attempt,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            file_name=file_name,
            status="processing_failed",
            comment=str(exc),
            client=client_config,
            operation="log_blocked_attempt.processing_failed",
        )
        await safe_registry_side_effect(
            client_registry.append_audit_log,
            log,
            registry_service,
            sheets_config,
            chat_id=chat_id,
            message_id=message_id,
            event_type="processing_finished",
            file_name=file_name,
            status="failed",
            details=str(exc),
            operation="append_audit_log.processing_failed",
        )
        await safe_edit_message(
            token,
            chat_id,
            status_message_id,
            f"Ошибка обработки файла {file_name}:\n{exc}",
            log,
        )
        return

    google_sheets_status = "Google Sheets: отключено"
    worksheet_title: str | None = None
    if google_sheets_enabled:
        try:
            worksheet_title = await asyncio.to_thread(export_results_to_google_sheets, file_name, results, log)
        except Exception as exc:
            log.exception("Google Sheets export failed")
            google_sheets_status = f"Google Sheets: ошибка экспорта ({exc})"
        else:
            google_sheets_status = f"Google Sheets: лист {worksheet_title}"

    charge = client_registry.calculate_charge(client_config, results)
    billing_error_message: str | None = None
    balance_after = client_config["balance"]
    if billing_enabled:
        try:
            billing_result = await asyncio.to_thread(
                client_registry.apply_charge,
                registry_service,
                sheets_config,
                client_config,
                charge,
                file_name=file_name,
                message_id=message_id,
                result_worksheet_title=worksheet_title,
                comment=google_sheets_status,
                status="charged" if charge["has_charge"] else "charged_zero",
            )
        except Exception as exc:
            log.exception("Billing update failed")
            billing_error_message = str(exc)
            await safe_registry_side_effect(
                client_registry.append_audit_log,
                log,
                registry_service,
                sheets_config,
                chat_id=chat_id,
                message_id=message_id,
                event_type="billing",
                file_name=file_name,
                status="failed",
                details=billing_error_message,
                operation="append_audit_log.billing_failed",
            )
        else:
            client_config = billing_result["client"]
            balance_after = billing_result["balance_after"]
    else:
        await safe_registry_side_effect(
            client_registry.append_billing_log,
            log,
            registry_service,
            sheets_config,
            {
                "created_at": client_registry.now_timestamp(),
                "chat_id": str(client_config["chat_id"]),
                "client_name": str(client_config["client_name"]),
                "file_name": file_name,
                "message_id": "" if message_id is None else str(message_id),
                "rows_total": str(charge["rows_total"]),
                "rows_successful": str(charge["rows_successful"]),
                "price_per_success_row": client_registry.format_decimal(charge["price_per_success_row"]),
                "amount_charged": "0.00",
                "balance_before": client_registry.format_decimal(client_config["balance"]),
                "balance_after": client_registry.format_decimal(client_config["balance"]),
                "status": "billing_disabled",
                "result_worksheet_title": worksheet_title or "",
                "comment": google_sheets_status,
            },
            operation="append_billing_log.billing_disabled",
        )

    billing_report, short_billing_report = build_billing_report(
        client_config,
        charge,
        billing_enabled=billing_enabled,
        balance_after=balance_after,
        error_message=billing_error_message,
    )

    detailed_report, short_report = build_completion_report(results)
    await safe_edit_message(
        token,
        chat_id,
        status_message_id,
        f"{detailed_report}\n\n{billing_report}\n\n{google_sheets_status}",
        log,
    )

    await safe_registry_side_effect(
        client_registry.append_audit_log,
        log,
        registry_service,
        sheets_config,
        chat_id=chat_id,
        message_id=message_id,
        event_type="processing_finished",
        file_name=file_name,
        status="completed" if billing_error_message is None else "completed_with_billing_error",
        details=(
            f"Успешных строк: {charge['rows_successful']}; "
            f"Google Sheets: {google_sheets_status}; "
            f"Биллинг: {short_billing_report}"
        ),
        operation="append_audit_log.processing_completed",
    )

    caption = (
        f"Готово: {file_name}\n"
        f"{short_report}\n"
        f"{short_billing_report}\n"
        f"{google_sheets_status}"
    )
    try:
        await send_document(
            token,
            chat_id,
            output_xlsx,
            caption=caption,
            reply_to_message_id=message_id,
        )
        log.info("Result file sent to chat %s: %s", chat_id, output_xlsx)
    except Exception as exc:
        log.exception("Failed to send result document")
        await safe_edit_message(
            token,
            chat_id,
            status_message_id,
            (
                f"Обработка завершена, но не удалось отправить файл.\n"
                f"Ошибка: {exc}\n"
                f"Результат сохранён локально: {output_xlsx}"
            ),
            log,
        )


async def bootstrap_offset(token: str, log: logging.Logger) -> int | None:
    try:
        response = await bot_api_request(token, "getUpdates", {"timeout": 0})
    except Exception as exc:
        log.warning("Failed to bootstrap updates offset: %s", exc)
        return None

    updates = response.get("result", [])
    if not updates:
        return None
    return updates[-1]["update_id"] + 1


async def main() -> None:
    log = setup_logging()
    token = get_required_env("TG_BOT_TOKEN")
    jobs_dir = Path(os.getenv("TG_BOT_JOBS_DIR", "tg_bot_jobs").strip() or "tg_bot_jobs")
    google_sheets_enabled = get_bool_env("GOOGLE_SHEETS_EXPORT_ENABLED", True)
    billing_enabled = get_bool_env("BILLING_ENABLED", True)

    api_id, api_hash, session_name, bot_username, headless, debug_dir, step_delay_seconds, row_delay_seconds, bot_message_echo = run_pipeline.load_runtime_config()
    sheets_config = google_sheets_client.load_config()
    registry_service = google_sheets_client.build_sheets_service(sheets_config)
    await asyncio.to_thread(client_registry.ensure_registry_sheets, registry_service, sheets_config)
    client = build_telegram_client(session_name, api_id, api_hash)

    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            "Telegram session is not authorized. Run `python qr_login.py` first to create "
            f"`{session_name}.session`."
        )

    bot_entity = await client.get_entity(bot_username)
    me = await bot_api_request(token, "getMe")
    log.info("Connected file-bot @%s", me["result"].get("username"))
    log.info("Connected query-bot %s", bot_username)

    offset = await bootstrap_offset(token, log)
    processed_message_ids: set[tuple[int, int]] = set()

    try:
        while True:
            params = {
                "timeout": 60,
                "allowed_updates": json.dumps(["message", "edited_message"]),
            }
            if offset is not None:
                params["offset"] = offset

            try:
                response = await bot_api_request(token, "getUpdates", params)
            except urllib.error.URLError as exc:
                log.warning("Network error while polling bot updates: %s", exc)
                await asyncio.sleep(3)
                continue
            except Exception as exc:
                log.warning("Bot API polling error: %s", exc)
                await asyncio.sleep(3)
                continue

            updates = response.get("result", [])
            for update in updates:
                offset = update["update_id"] + 1
                message = extract_message(update)
                if not message:
                    continue

                chat = message.get("chat") or {}
                chat_id = chat.get("id")
                message_id = message.get("message_id")
                if chat_id is None or message_id is None:
                    continue

                message_key = (chat_id, message_id)
                if message_key in processed_message_ids:
                    continue
                processed_message_ids.add(message_key)

                document = message.get("document")
                if not document:
                    continue

                await handle_document_message(
                    client,
                    bot_entity,
                    token=token,
                    chat_id=chat_id,
                    message=message,
                    jobs_dir=jobs_dir,
                    sheets_config=sheets_config,
                    registry_service=registry_service,
                    google_sheets_enabled=google_sheets_enabled,
                    billing_enabled=billing_enabled,
                    headless=headless,
                    debug_dir=debug_dir,
                    step_delay_seconds=step_delay_seconds,
                    row_delay_seconds=row_delay_seconds,
                    bot_message_echo=bot_message_echo,
                    log=log,
                )
    finally:
        if client.is_connected():
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
