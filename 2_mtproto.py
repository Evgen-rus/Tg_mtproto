import asyncio
import logging
import os
import re

from dotenv import load_dotenv
from telethon import TelegramClient, events

from database.inn_storage import build_upsert_kwargs, insert_source_query, upsert_inn_result
from database.sqlite_db import connect, init_schema
from utils.inn_parser import parse_inn_result_text

load_dotenv()


def setup_logging() -> logging.Logger:
    level_raw = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_raw, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    # Telethon довольно шумный — оставим его логи на WARNING.
    logging.getLogger("telethon").setLevel(logging.WARNING)
    return logging.getLogger("mtproto")


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"В .env не задано значение для {name}. "
            f"Добавь строку вида: {name}=..."
        )
    return value.strip()


def load_config() -> tuple[int, str, str, str]:
    api_id_raw = get_required_env("API_ID")
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise RuntimeError("API_ID в .env должен быть числом (int).") from exc

    api_hash = get_required_env("API_HASH")
    session_name = get_required_env("SESSION_NAME")
    bot = get_required_env("BOT")  # @username
    return api_id, api_hash, session_name, bot


def print_buttons(msg) -> None:
    if not getattr(msg, "buttons", None):
        return
    print("[buttons]")
    for row in msg.buttons:
        print(" | ".join(btn.text for btn in row))


async def ainput(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


def print_incoming(prefix: str, msg) -> None:
    text = msg.text or "[сообщение без текста]"
    print(f"\n{prefix} {text}")
    print_buttons(msg)
    print("> ", end="", flush=True)


async def main() -> None:
    log = setup_logging()
    api_id, api_hash, session_name, bot_username = load_config()
    client = TelegramClient(session_name, api_id, api_hash)

    # Влияние: скрипт начнёт создавать файл БД и записывать туда результаты /inn.
    db_path = os.getenv("DB_PATH", "tg_results.db").strip()
    conn = connect(db_path)
    init_schema(conn)
    log.info("DB initialized: %s", db_path)

    # Сопоставляем ИНН из запроса "/inn 123..." -> id записи в source_queries,
    # чтобы затем проставить source_query_id в inn_results.
    pending_inn_queries: dict[str, int] = {}

    try:
        await client.start()
        bot_entity = await client.get_entity(bot_username)

        # 1) Любые новые сообщения от бота
        @client.on(events.NewMessage(from_users=bot_entity))
        async def on_bot_new_message(event):
            print_incoming("<", event.message)

        # 2) Любые редактирования сообщений от бота (частый кейс “идёт поиск” -> “результат”)
        @client.on(events.MessageEdited(from_users=bot_entity))
        async def on_bot_edited_message(event):
            print_incoming("< [edit]", event.message)

            text = event.message.text or ""
            # Пишем в БД только финальный ответ, когда есть ИНН.
            if "ИНН" not in text:
                log.debug("Skip edited message: no INN marker")
                return

            try:
                parsed = parse_inn_result_text(text)
                inn = parsed.get("inn")
                if not inn:
                    log.debug("Skip edited message: INN not parsed")
                    return

                source_query_id = pending_inn_queries.get(inn)
                if source_query_id is None:
                    # Если почему-то не нашли исходный запрос — всё равно сохраним,
                    # создав "служебную" запись источника.
                    source_query_id = insert_source_query(conn, query_text="(auto) ответ без сопоставленного запроса")
                    log.warning(
                        "No source query matched for inn=%s. Created auto source_query_id=%s",
                        inn,
                        source_query_id,
                    )
                else:
                    log.info("Matched source_query_id=%s for inn=%s", source_query_id, inn)

                kwargs = build_upsert_kwargs(parsed)

                # Логируем только распарсенные поля (без полного raw_text, чтобы не засорять лог).
                raw_text_len = len(kwargs.get("raw_text") or "")
                log_fields = {k: v for k, v in kwargs.items() if k != "raw_text"}
                missing_fields = sorted([k for k, v in log_fields.items() if v is None])
                log.info(
                    "Parsed fields for inn=%s (raw_text_len=%s): %s",
                    inn,
                    raw_text_len,
                    log_fields,
                )
                if missing_fields:
                    log.info("Missing (None) fields for inn=%s: %s", inn, missing_fields)
                upsert_inn_result(conn, source_query_id=source_query_id, **kwargs)
                log.info("Saved to DB: inn=%s source_query_id=%s", inn, source_query_id)
            except Exception as exc:
                # Важно: не падаем из-за БД/парсинга.
                log.exception("Failed to save edited message to DB: %s", exc)
                print(f"\n[warn] Не удалось сохранить результат в БД: {exc}")
                print("> ", end="", flush=True)

        print("Готово. Пиши текст. Выход: /exit\n")

        while True:
            text = (await ainput("> ")).strip()
            if not text:
                continue
            if text == "/exit":
                break

            # печатаем то, что отправили
            print(f"[you] {text}")

            # Если это /inn, заранее создаём source_query и запоминаем ID.
            if text.lower().startswith("/inn"):
                m = re.search(r"/inn\s+(\d{10}|\d{12})\b", text, flags=re.IGNORECASE)
                if m:
                    inn = m.group(1)
                    qid = insert_source_query(conn, query_text=text)
                    pending_inn_queries[inn] = qid
                    log.info("Source query logged: inn=%s source_query_id=%s", inn, qid)
                else:
                    log.debug("Command starts with /inn but INN not matched: %r", text)

            await client.send_message(bot_entity, text)

        await client.disconnect()
    finally:
        try:
            conn.close()
        except Exception:
            pass
        if client.is_connected():
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
