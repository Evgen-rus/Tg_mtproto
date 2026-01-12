import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import TimeoutError

load_dotenv()


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


async def main() -> None:
    api_id, api_hash, session_name, bot_username = load_config()
    client = TelegramClient(session_name, api_id, api_hash)

    try:
        await client.start()  # после diag_qr_login уже не спросит код
        bot_entity = await client.get_entity(bot_username)

        print("Готово. Пиши текст. Выход: /exit\n")

        while True:
            text = (await ainput("> ")).strip()
            if not text:
                continue
            if text == "/exit":
                break

            try:
                async with client.conversation(bot_entity, timeout=30) as conv:
                    await conv.send_message(text)
                    resp = await conv.get_response()
                    print(f"< {resp.text or '[ответ без текста]'}")
                    print_buttons(resp)
            except TimeoutError:
                print("< [нет ответа за 30 сек]")
            except Exception as e:
                print(f"< [ошибка: {e}]")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
