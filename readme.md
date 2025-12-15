Ок, делаем **MTProto-вариант (самый стабильный)** на **Telethon**: в терминале пишешь → скрипт отправляет `@Neyroosint_test_bot` → ждёт ответ → печатает.

Telethon работает как клиент Telegram (под твоим аккаунтом) и умеет `send_message` и “разговоры” (`conversation`). ([docs.telethon.dev][1])
https://telethon-01914.readthedocs.io/en/latest/extra/basic/getting-started.html
---

## 1) Что нужно один раз подготовить

1. Получи `api_id` и `api_hash` на `my.telegram.org` (это обязательно для MTProto-клиентов). ([telethon-01914.readthedocs.io][2])
2. Установи Telethon:

```bash
pip install -U telethon
```

([docs.telethon.dev][3])

---

## 2) Скрипт `tg_mtproto_mvp.py`


---

## 3) Запуск

```bash
python tg_mtproto_mvp.py
```

При первом запуске Telethon попросит:

* номер телефона
* код из Telegram
* (если включено) пароль 2FA

---

## 4) Что добавить в `.gitignore` (важно)

```gitignore
*.session
*.session-journal
```

---

Если бот отвечает **несколькими сообщениями подряд** или через **кнопки/меню**, скажи — я расширю MVP: будем собирать все ответы за окно времени и уметь “нажимать” кнопки.

[1]: https://docs.telethon.dev/en/stable/modules/client.html?utm_source=chatgpt.com "TelegramClient — Telethon 1.42.0 documentation"
[2]: https://telethon-01914.readthedocs.io/en/latest/extra/basic/getting-started.html?utm_source=chatgpt.com "Getting Started — Telethon 0.19.1 documentation"
[3]: https://docs.telethon.dev/en/stable/basic/installation.html?utm_source=chatgpt.com "Installation — Telethon 1.42.0 documentation"
