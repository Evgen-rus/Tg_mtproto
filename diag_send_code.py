# diag_send_code.py
# Диагностика: явно вызывает send_code_request и печатает то, что вернул Telegram.
# Запуск:
#   1) (опционально) удали файл SESSION_NAME.session для "чистого" логина
#   2) python diag_send_code.py
#
# .env (пример):
# API_ID=123456
# API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# SESSION_NAME=tg_user
# PHONE=+79231231212   # можно не задавать — тогда спросит

import asyncio
import logging
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
    PhoneNumberUnoccupiedError,
    SessionPasswordNeededError,
)

load_dotenv()


def get_required_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or not v.strip():
        raise RuntimeError(f"В .env не задано значение для {name}. Добавь строку: {name}=...")
    return v.strip()


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("telethon").setLevel(getattr(logging, level, logging.INFO))


def dump_sent_code(sent) -> None:
    print("\n=== Telegram send_code_request() response ===")
    print(f"type(sent): {type(sent)}")
    # repr часто содержит важные поля (type/timeout/next_type/phone_code_hash)
    print(f"repr: {sent!r}")

    # Безопасно пробуем достать частые атрибуты (если есть)
    for attr in ("type", "timeout", "next_type", "phone_code_hash"):
        if hasattr(sent, attr):
            print(f"{attr}: {getattr(sent, attr)}")

    # Иногда внутри есть sent.type, у которого тоже есть полезные поля
    if hasattr(sent, "type"):
        t = getattr(sent, "type")
        print("\n--- sent.type details ---")
        print(f"type(sent.type): {type(t)}")
        print(f"repr(sent.type): {t!r}")
    print("===========================================\n")


async def ainput(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


async def main() -> None:
    setup_logging()
    log = logging.getLogger("diag")

    api_id = int(get_required_env("API_ID"))
    api_hash = get_required_env("API_HASH")
    session_name = get_required_env("SESSION_NAME")

    phone = os.getenv("PHONE", "").strip()
    if not phone:
        phone = (await ainput("Phone in E.164 (пример +79231231212): ")).strip()

    client = TelegramClient(session_name, api_id, api_hash)

    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            print("Уже авторизован в этой сессии (.session существует).")
            print(f"me.id={getattr(me, 'id', None)} username={getattr(me, 'username', None)}")
            print("Если нужна повторная проверка логина — удали файл SESSION_NAME.session и запусти снова.")
            return

        log.info("Вызов send_code_request для phone=%s", phone)

        try:
            sent = await client.send_code_request(phone)
        except PhoneNumberInvalidError:
            print("Ошибка: PhoneNumberInvalidError (неверный формат/номер). Используй +7923... без пробелов.")
            return
        except PhoneNumberBannedError:
            print("Ошибка: PhoneNumberBannedError (номер заблокирован Telegram).")
            return
        except PhoneNumberUnoccupiedError:
            print("Ошибка: PhoneNumberUnoccupiedError (номер не зарегистрирован в Telegram).")
            return
        except PhoneNumberFloodError:
            print("Ошибка: PhoneNumberFloodError (слишком много запросов на этот номер). Подожди и повтори позже.")
            return
        except FloodWaitError as e:
            print(f"Ошибка: FloodWaitError (лимит). Подожди {e.seconds} секунд и повтори.")
            return

        dump_sent_code(sent)

        code = (await ainput("Введи код (который пришёл ВНУТРИ Telegram): ")).strip()
        if not code:
            print("Код пустой — выходим.")
            return

        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
        except SessionPasswordNeededError:
            pwd = await ainput("Включена 2FA. Введи пароль Telegram: ")
            await client.sign_in(password=pwd)

        me = await client.get_me()
        print("\nSUCCESS: авторизация завершена.")
        print(f"me.id={getattr(me, 'id', None)} username={getattr(me, 'username', None)}")
        print(f"session file: {session_name}.session (создан рядом со скриптом)")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
