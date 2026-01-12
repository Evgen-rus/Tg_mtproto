import asyncio
import os
import getpass

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

load_dotenv()


def req(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Нет {name} в .env")
    return v


async def main() -> None:
    api_id = int(req("API_ID"))
    api_hash = req("API_HASH")
    session_name = req("SESSION_NAME")

    client = TelegramClient(session_name, api_id, api_hash)
    await client.connect()

    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"Уже авторизован: id={me.id} username={me.username}")
            return

        qr = await client.qr_login()

        # Печать QR в консоль (ASCII)
        import qrcode
        q = qrcode.QRCode(border=1)
        q.add_data(qr.url)
        q.make(fit=True)
        q.print_ascii(invert=True)

        print("\nТелефон: Telegram → Настройки → Устройства → Подключить устройство → сканируй QR.\n")

        try:
            await qr.wait()
        except SessionPasswordNeededError:
            pwd = getpass.getpass("Включена 2FA. Введи пароль Telegram: ")
            await client.sign_in(password=pwd)

        me = await client.get_me()
        print(f"\nSUCCESS: авторизация завершена: id={me.id} username={me.username}")
        print(f"session file: {session_name}.session")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
