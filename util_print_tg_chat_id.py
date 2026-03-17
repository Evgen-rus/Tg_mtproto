import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

from dotenv import load_dotenv

load_dotenv()


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

    with urllib.request.urlopen(url, timeout=70) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if not data.get("ok"):
        raise RuntimeError(f"Telegram Bot API error: {data}")
    return data


def extract_chat(update: dict) -> dict | None:
    for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
        message = update.get(key)
        if message and message.get("chat"):
            return message["chat"]
    return None


def describe_chat(chat: dict) -> str:
    chat_id = chat.get("id")
    chat_type = chat.get("type", "unknown")
    title = chat.get("title") or ""
    username = chat.get("username") or ""
    full_name = " ".join(part for part in [chat.get("first_name"), chat.get("last_name")] if part)

    parts = [f"chat_id={chat_id}", f"type={chat_type}"]
    if title:
        parts.append(f"title={title}")
    if full_name:
        parts.append(f"name={full_name}")
    if username:
        parts.append(f"username=@{username}")
    return " | ".join(parts)


def main() -> None:
    token = get_required_env("TG_BOT_TOKEN")
    offset: int | None = None
    seen_chat_ids: set[int] = set()

    me = telegram_api_request(token, "getMe")["result"]
    print(f"Bot is ready: @{me.get('username')}")
    print("Add the bot to the target group and send any message there.")
    print("The script will print the group chat_id.\n")

    while True:
        params = {
            "timeout": 60,
            "allowed_updates": json.dumps(
                ["message", "edited_message", "channel_post", "edited_channel_post"]
            ),
        }
        if offset is not None:
            params["offset"] = offset

        try:
            response = telegram_api_request(token, "getUpdates", params)
        except urllib.error.URLError as exc:
            print(f"[warn] Network error: {exc}. Retry in 3 sec.")
            time.sleep(3)
            continue
        except Exception as exc:
            print(f"[warn] API error: {exc}. Retry in 3 sec.")
            time.sleep(3)
            continue

        updates = response.get("result", [])
        for update in updates:
            update_id = update["update_id"]
            offset = update_id + 1

            chat = extract_chat(update)
            if not chat:
                continue

            chat_id = chat.get("id")
            if chat_id in seen_chat_ids:
                continue

            seen_chat_ids.add(chat_id)
            print(describe_chat(chat))
            print(f"Put this in .env: ID_TG_CHAT={chat_id}\n")


if __name__ == "__main__":
    main()
