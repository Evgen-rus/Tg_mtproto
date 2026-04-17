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


async def process_input_file(
    client: TelegramClient,
    bot_entity,
    *,
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
    rows = run_pipeline.load_input_rows(input_path)
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
    headless: bool,
    debug_dir: Path,
    step_delay_seconds: int,
    row_delay_seconds: int,
    bot_message_echo: bool,
    log: logging.Logger,
) -> None:
    document = message.get("document") or {}
    file_name = document.get("file_name") or "input.xlsx"
    if not is_supported_filename(file_name):
        await send_message(
            token,
            chat_id,
            "Поддерживаются только файлы .xlsx, .xlsm и .csv",
            reply_to_message_id=message.get("message_id"),
        )
        return

    if is_template_filename(file_name):
        await send_message(
            token,
            chat_id,
            (
                "Этот файл выглядит как шаблон и не запускается в обработку. "
                "Переименуйте файл и отправьте его заново, если хотите начать поиск."
            ),
            reply_to_message_id=message.get("message_id"),
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
        reply_to_message_id=message.get("message_id"),
    )
    status_message_id = status["result"]["message_id"]

    try:
        await download_file(token, document["file_id"], input_path)
        results = await process_input_file(
            client,
            bot_entity,
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
        await safe_edit_message(
            token,
            chat_id,
            status_message_id,
            f"Ошибка обработки файла {file_name}:\n{exc}",
            log,
        )
        return

    counts = count_statuses(results)
    summary_parts = [f"{key}: {value}" for key, value in sorted(counts.items())]
    summary_text = ", ".join(summary_parts) if summary_parts else "без статусов"
    await safe_edit_message(
        token,
        chat_id,
        status_message_id,
        (
            f"Обработка завершена.\n"
            f"Строк: {len(results)}\n"
            f"Итог: {summary_text}\n"
            f"Отправляю результат."
        ),
        log,
    )

    caption = f"Готово: {file_name}\nСтрок: {len(results)}\n{summary_text}"
    try:
        await send_document(
            token,
            chat_id,
            output_xlsx,
            caption=caption,
            reply_to_message_id=message.get("message_id"),
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
    target_chat_id = int(get_required_env("ID_TG_CHAT"))
    jobs_dir = Path(os.getenv("TG_BOT_JOBS_DIR", "tg_bot_jobs").strip() or "tg_bot_jobs")

    api_id, api_hash, session_name, bot_username, headless, debug_dir, step_delay_seconds, row_delay_seconds, bot_message_echo = run_pipeline.load_runtime_config()
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
                if chat_id != target_chat_id or message_id is None:
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
