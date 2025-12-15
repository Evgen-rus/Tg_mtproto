# Telegram MTProto MVP (Telethon) — терминал -> бот -> ответ

Скрипт работает как Telegram-клиент под моим аккаунтом:
пишу сообщение в терминале -> отправляется боту `@***` -> жду ответ -> печатаю в терминал.

## Как получить `API_ID` и `API_HASH` (для Telethon / MTProto)

1) Открой Telegram Core и залогинься своим номером:
   https://my.telegram.org  

2) Перейди в раздел **API development tools**. 

3) Нажми **Create new application** (если формы ещё нет — она появится сама).
   Заполни поля (достаточно App title и Short name; URL можно не указывать). 

4) После создания на странице появятся значения:
   - `api_id`
   - `api_hash`
   Скопируй их в `.env`. :contentReference[oaicite:3]{index=3}

Важно: `api_hash` — секрет, не публикуй и не коммить в git.


## Установка
```bash
pip install -r requirements.txt
````

## Создай файл .env

```env
API_ID=123456
API_HASH=YOUR_HASH
SESSION_NAME=tg_user
BOT=@Neyroosint_test_bot
```

## Запуск

```bash
python tg_mtproto_mvp.py
```

## Первый запуск

Telethon попросит:

* номер телефона
* код из Telegram
* (если включено) пароль 2FA

После этого рядом появится файл сессии: `<SESSION_NAME>.session`

## Управление

* Пиши текст в терминале — увидишь ответ строкой ниже
* Выход: `/exit`

## .gitignore (обязательно)

```gitignore
.env
*.session
*.session-journal
```

## Примечания

* Это MTProto (не bot token), работает от твоего аккаунта.
* Не спамь: соблюдай лимиты Telegram, иначе могут быть ограничения.