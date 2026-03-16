import asyncio
import os

import qrcode
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError

load_dotenv()


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required .env value: {name}")
    return value


def load_config() -> tuple[int, str, str]:
    api_id = int(get_required_env("API_ID"))
    api_hash = get_required_env("API_HASH")
    session_name = get_required_env("SESSION_NAME")
    return api_id, api_hash, session_name


def print_qr(url: str) -> None:
    qr_code = qrcode.QRCode(border=1)
    qr_code.add_data(url)
    qr_code.make(fit=True)
    qr_code.print_ascii(invert=True)


async def main() -> None:
    api_id, api_hash, session_name = load_config()
    client = TelegramClient(session_name, api_id, api_hash)

    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"Session already authorized: id={me.id} username={me.username}")
            print(f"Session file: {session_name}.session")
            return

        print("\nOpen Telegram -> Settings -> Devices -> Link Desktop Device, then scan the QR code.\n")

        while not await client.is_user_authorized():
            qr = await client.qr_login()
            print_qr(qr.url)
            try:
                await qr.wait()
            except asyncio.TimeoutError:
                print("\nQR code expired. Generating a new QR code...\n")
                continue
            except SessionPasswordNeededError:
                print("Telegram requested the 2FA cloud password for this account.")
                while True:
                    password = input("Telegram 2FA password: ").strip()
                    if not password:
                        raise RuntimeError("Empty 2FA password. Login cancelled.")
                    try:
                        await client.sign_in(password=password)
                        break
                    except PasswordHashInvalidError:
                        print("Invalid 2FA password. Try again.")

        me = await client.get_me()
        print(f"\nSuccess: id={me.id} username={me.username}")
        print(f"Session file: {session_name}.session")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
