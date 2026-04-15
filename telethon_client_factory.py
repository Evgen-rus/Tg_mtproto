import os
import urllib.request

import socks
import sockshandler
from telethon import TelegramClient


def get_proxy_settings() -> tuple[str, str, int] | None:
    use_proxy = os.getenv("USE_PROXY", "false").strip().lower() == "true"
    if not use_proxy:
        return None

    proxy_type = os.getenv("PROXY_TYPE", "socks5").strip() or "socks5"
    proxy_host = os.getenv("PROXY_HOST", "127.0.0.1").strip() or "127.0.0.1"
    proxy_port = int(os.getenv("PROXY_PORT", "10808").strip() or "10808")
    return proxy_type, proxy_host, proxy_port


def build_telegram_client(session_name: str, api_id: int, api_hash: str) -> TelegramClient:
    proxy = get_proxy_settings()
    if proxy is None:
        return TelegramClient(session_name, api_id, api_hash)
    return TelegramClient(session_name, api_id, api_hash, proxy=proxy)


def build_url_opener() -> urllib.request.OpenerDirector:
    proxy = get_proxy_settings()
    if proxy is None:
        return urllib.request.build_opener()

    proxy_type, proxy_host, proxy_port = proxy
    proxy_type = proxy_type.lower()

    if proxy_type in {"http", "https"}:
        proxy_url = f"{proxy_type}://{proxy_host}:{proxy_port}"
        proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        return urllib.request.build_opener(proxy_handler)

    if proxy_type == "socks5":
        return urllib.request.build_opener(
            sockshandler.SocksiPyHandler(socks.PROXY_TYPE_SOCKS5, proxy_host, proxy_port)
        )

    if proxy_type == "socks4":
        return urllib.request.build_opener(
            sockshandler.SocksiPyHandler(socks.PROXY_TYPE_SOCKS4, proxy_host, proxy_port)
        )

    raise RuntimeError(f"Unsupported PROXY_TYPE for urllib requests: {proxy_type}")


def open_url(request_or_url: str | urllib.request.Request, *, timeout: int):
    opener = build_url_opener()
    return opener.open(request_or_url, timeout=timeout)
