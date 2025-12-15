import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import TimeoutError

load_dotenv()


def get_required_env(name: str) -> str:
    """Берёт обязательную переменную из окружения и даёт понятную ошибку."""
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"В .env не задано значение для {name}. "
            f"Добавь строку вида: {name}=..."
        )
    return value.strip()


def load_config() -> tuple[int, str, str, str]:
    """
    Загружаем конфиг из .env.

    Ожидается .env:
    - API_ID=...
    - API_HASH=...
    - SESSION_NAME=...
    - BOT=@username_бота
    """
    api_id_raw = get_required_env("API_ID")
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise RuntimeError("API_ID в .env должен быть числом (int).") from exc

    api_hash = get_required_env("API_HASH")
    session_name = get_required_env("SESSION_NAME")
    bot = get_required_env("BOT")
    return api_id, api_hash, session_name, bot


def print_buttons(msg):
    if not msg.buttons:
        return
    print("[buttons]")
    for row in msg.buttons:
        print(" | ".join(btn.text for btn in row))


async def main():
    api_id, api_hash, session_name, bot = load_config()
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()  # при первом запуске спросит телефон/код/2FA :contentReference[oaicite:3]{index=3}

    print("Готово. Пиши текст. Выход: /exit\n")

    while True:
        text = input("> ").strip()
        if not text:
            continue
        if text == "/exit":
            break

        # Conversation = самый простой способ "отправил -> дождался ответа"
        try:
            async with client.conversation(bot, timeout=30) as conv:
                await conv.send_message(text)
                resp = await conv.get_response()
                print(f"< {resp.text or '[ответ без текста]'}")
                print_buttons(resp)
        except TimeoutError:
            print("< [нет ответа за 30 сек]")
        except Exception as e:
            print(f"< [ошибка: {e}]")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
