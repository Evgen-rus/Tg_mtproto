import asyncio
import csv
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events

load_dotenv()

COMPANY_INN_RE = re.compile(r"\bИНН\s*:\s*(\d{10}|\d{12})\b")
DIRECTOR_RE = re.compile(
    r"(?:Директор|Генеральный директор|Руководитель)\s*:\s*([^\n(]+?)(?:\s*\(ИНН\s*(\d{10,12})\))?(?:\n|$)"
)
FIO_RE = re.compile(r"\bФИО\s*:\s*([^\n]+)")
PHONE_RE = re.compile(r"\bТелефон\s*:\s*([+0-9][0-9()\-\s]{8,})")
EMAIL_RE = re.compile(r"\bEmail\s*:\s*([^\s\n]+)", re.IGNORECASE)
PERSON_INN_RE = re.compile(r"\bИНН\s*:\s*(\d{10}|\d{12})\b")


@dataclass
class CompanyCard:
    company_inn: str | None = None
    company_name: str | None = None
    director_name: str | None = None
    director_inn: str | None = None


@dataclass
class PersonCard:
    fio: str | None = None
    phone: str | None = None
    email: str | None = None
    inn: str | None = None
    raw_text: str | None = None


@dataclass
class QueryState:
    requested_inn: str
    done: asyncio.Event = field(default_factory=asyncio.Event)
    phase: str = "await_company"
    company: CompanyCard | None = None
    person: PersonCard | None = None
    error: str | None = None


def setup_logging() -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    return logging.getLogger("director_phone")


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required .env value: {name}")
    return value


def load_config() -> tuple[int, str, str, str, Path]:
    api_id = int(get_required_env("API_ID"))
    api_hash = get_required_env("API_HASH")
    session_name = get_required_env("SESSION_NAME")
    bot_username = get_required_env("BOT")
    results_csv = Path(os.getenv("RESULTS_CSV", "results.csv").strip() or "results.csv")
    return api_id, api_hash, session_name, bot_username, results_csv


async def ainput(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


def get_message_text(message) -> str:
    return (getattr(message, "raw_text", None) or getattr(message, "text", None) or "").strip()


def print_buttons(message) -> None:
    if not getattr(message, "buttons", None):
        return
    print("[buttons]")
    for row in message.buttons:
        print(" | ".join((button.text or "").strip() for button in row))


def print_incoming(prefix: str, message) -> None:
    text = get_message_text(message) or "[empty message]"
    print(f"\n{prefix} {text}")
    print_buttons(message)
    print("> ", end="", flush=True)


def extract_company_name(text: str) -> str | None:
    for line in text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.lower().startswith("/inn"):
            continue
        if candidate.startswith(("ИНН:", "ОГРН:", "Дата регистрации:", "Статус:", "Директор:")):
            break
        if candidate.startswith(("Финансовые показатели", "Адрес:", "Сотрудников:")):
            continue
        if re.match(r"^\d{2}\.\d{2}\b", candidate):
            continue
        if candidate.startswith("👁"):
            continue
        return candidate
    return None


def parse_company_card(text: str) -> CompanyCard | None:
    inn_match = COMPANY_INN_RE.search(text)
    director_match = DIRECTOR_RE.search(text)
    if not inn_match and not director_match:
        return None
    return CompanyCard(
        company_inn=inn_match.group(1) if inn_match else None,
        company_name=extract_company_name(text),
        director_name=director_match.group(1).strip() if director_match else None,
        director_inn=director_match.group(2).strip() if director_match and director_match.group(2) else None,
    )


def normalize_phone(raw_phone: str | None) -> str | None:
    if not raw_phone:
        return None
    cleaned = re.sub(r"[^\d+]", "", raw_phone)
    return cleaned or None


def parse_person_card(text: str) -> PersonCard | None:
    fio_match = FIO_RE.search(text)
    phone_match = PHONE_RE.search(text)
    email_match = EMAIL_RE.search(text)
    inn_match = PERSON_INN_RE.search(text)
    if not any((fio_match, phone_match, email_match)):
        return None
    return PersonCard(
        fio=fio_match.group(1).strip() if fio_match else None,
        phone=normalize_phone(phone_match.group(1)) if phone_match else None,
        email=email_match.group(1).strip() if email_match else None,
        inn=inn_match.group(1) if inn_match else None,
        raw_text=text,
    )


def find_button_text(message, target_name: str | None) -> str | None:
    if not target_name or not getattr(message, "buttons", None):
        return None

    normalized_target = " ".join(target_name.split()).casefold()
    fallback: str | None = None

    for row in message.buttons:
        for button in row:
            text = (button.text or "").strip()
            normalized_text = " ".join(text.split()).casefold()
            if normalized_text == normalized_target:
                return text
            if normalized_target and normalized_target in normalized_text and fallback is None:
                fallback = text
    return fallback


def normalize_inn(user_input: str) -> str | None:
    match = re.search(r"(\d{10}|\d{12})", user_input)
    if not match:
        return None
    return match.group(1)


def append_result(results_csv: Path, state: QueryState) -> None:
    if not state.company or not state.person:
        return

    results_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not results_csv.exists()
    with results_csv.open("a", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "company_inn",
                "company_name",
                "director_name",
                "director_inn",
                "person_fio",
                "phone",
                "email",
                "person_inn",
            ],
        )
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "company_inn": state.company.company_inn,
                "company_name": state.company.company_name,
                "director_name": state.company.director_name,
                "director_inn": state.company.director_inn,
                "person_fio": state.person.fio,
                "phone": state.person.phone,
                "email": state.person.email,
                "person_inn": state.person.inn,
            }
        )


async def main() -> None:
    log = setup_logging()
    api_id, api_hash, session_name, bot_username, results_csv = load_config()
    client = TelegramClient(session_name, api_id, api_hash)
    current_query: QueryState | None = None

    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telegram session is not authorized. Run `python qr_login.py` first to create "
                f"`{session_name}.session`."
            )

        bot_entity = await client.get_entity(bot_username)
        log.info("Connected to bot %s", bot_username)

        async def handle_bot_message(event, prefix: str) -> None:
            nonlocal current_query

            message = event.message
            text = get_message_text(message)
            print_incoming(prefix, message)

            if current_query is None or current_query.done.is_set():
                return

            if current_query.phase == "await_company":
                company = parse_company_card(text)
                if not company or not company.director_name:
                    return

                button_text = find_button_text(message, company.director_name)
                if not button_text:
                    current_query.error = f"Director button not found: {company.director_name}"
                    current_query.done.set()
                    return

                current_query.company = company
                current_query.phase = "await_person"
                log.info(
                    "Clicking director button for company_inn=%s director=%s",
                    company.company_inn,
                    company.director_name,
                )
                await message.click(text=button_text)
                return

            if current_query.phase == "await_person":
                person = parse_person_card(text)
                if not person:
                    return

                current_query.person = person
                current_query.phase = "done"
                current_query.done.set()

        @client.on(events.NewMessage(from_users=bot_entity))
        async def on_new_message(event):
            await handle_bot_message(event, "<")

        @client.on(events.MessageEdited(from_users=bot_entity))
        async def on_edited_message(event):
            await handle_bot_message(event, "< [edit]")

        print("Ready. Enter INN or /exit.\n")

        while True:
            user_input = (await ainput("> ")).strip()
            if not user_input:
                continue
            if user_input == "/exit":
                break

            inn = normalize_inn(user_input)
            if not inn:
                print("[warn] Could not parse INN from input")
                continue

            current_query = QueryState(requested_inn=inn)
            command = f"/inn {inn}"
            print(f"[you] {command}")
            await client.send_message(bot_entity, command)

            try:
                await asyncio.wait_for(current_query.done.wait(), timeout=90)
            except asyncio.TimeoutError:
                print(f"[warn] Timeout while waiting for result for INN {inn}")
                current_query = None
                continue

            if current_query.error:
                print(f"[warn] {current_query.error}")
                current_query = None
                continue

            if not current_query.person:
                print(f"[warn] No person card received for INN {inn}")
                current_query = None
                continue

            append_result(results_csv, current_query)

            print("\n[result]")
            print(f"company_inn: {current_query.company.company_inn if current_query.company else inn}")
            print(f"director: {current_query.company.director_name if current_query.company else ''}")
            print(f"phone: {current_query.person.phone or 'not found'}")
            print(f"email: {current_query.person.email or 'not found'}")
            print(f"saved_to: {results_csv}")
            print()

            current_query = None
    finally:
        if client.is_connected():
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
