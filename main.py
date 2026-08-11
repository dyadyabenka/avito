"""
Сниппер объявлений по ноутбукам для Авито с полуавтоматическим предложением цены.

Схема работы:
  источник -> новые объявления -> разбор характеристик (LLM) -> оценка цены
  -> карточка в Telegram с кнопками -> человек решает.

Запуск:  python avito_sniper.py
Переменные окружения — см. .env.example
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import statistics
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sniper")


# ---------------------------------------------------------------- конфигурация


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return value


@dataclass
class Config:
    telegram_token: str = field(default_factory=lambda: _env("TELEGRAM_TOKEN"))
    owner_id: int = field(default_factory=lambda: int(_env("OWNER_ID")))

    # OpenAI-совместимый эндпоинт: DeepSeek, Anthropic через прокси, локальная модель
    llm_base_url: str = field(
        default_factory=lambda: _env("LLM_BASE_URL", "https://api.deepseek.com/v1")
    )
    llm_key: str = field(default_factory=lambda: _env("LLM_API_KEY", ""))
    llm_model: str = field(default_factory=lambda: _env("LLM_MODEL", "deepseek-chat"))

    db_path: str = field(default_factory=lambda: _env("DB_PATH", "sniper.db"))
    poll_seconds: int = field(default_factory=lambda: int(_env("POLL_SECONDS", "120")))

    # ссылки на сохранённые фильтры Авито, через запятую
    search_urls: list[str] = field(
        default_factory=lambda: [
            u.strip() for u in _env("SEARCH_URLS", "").split(",") if u.strip()
        ]
    )

    # экономика: желаемая маржа и резерв на предпродажную подготовку
    target_margin: float = field(
        default_factory=lambda: float(_env("TARGET_MARGIN", "0.25"))
    )
    prep_reserve: int = field(default_factory=lambda: int(_env("PREP_RESERVE", "3000")))
    max_price: int = field(default_factory=lambda: int(_env("MAX_PRICE", "80000")))

    # пока нет живого источника — работаем на mock_listings.json
    use_mock: bool = field(
        default_factory=lambda: _env("USE_MOCK", "1") not in ("0", "false", "no")
    )


CFG = Config()


# ------------------------------------------------------------------- хранилище

SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id           TEXT PRIMARY KEY,
    title        TEXT NOT NULL,
    price        INTEGER NOT NULL,
    url          TEXT NOT NULL,
    description  TEXT DEFAULT '',
    model_key    TEXT DEFAULT '',
    specs_json   TEXT DEFAULT '{}',
    risks_json   TEXT DEFAULT '[]',
    fair_price   INTEGER,
    offer_price  INTEGER,
    status       TEXT DEFAULT 'new',     -- new | offered | skipped | gone
    first_seen   INTEGER NOT NULL,
    last_seen    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_model ON listings(model_key);
"""


class Store:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def is_known(self, listing_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM listings WHERE id = ?", (listing_id,))
        return cur.fetchone() is not None

    def insert(self, row: dict[str, Any]) -> None:
        now = int(time.time())
        self.conn.execute(
            """INSERT OR IGNORE INTO listings
               (id, title, price, url, description, model_key, specs_json,
                risks_json, fair_price, offer_price, status, first_seen, last_seen)
               VALUES (:id, :title, :price, :url, :description, :model_key,
                       :specs_json, :risks_json, :fair_price, :offer_price,
                       'new', :now, :now)""",
            {**row, "now": now},
        )
        self.conn.commit()

    def touch(self, listing_id: str) -> None:
        self.conn.execute(
            "UPDATE listings SET last_seen = ? WHERE id = ?",
            (int(time.time()), listing_id),
        )
        self.conn.commit()

    def set_status(self, listing_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE listings SET status = ? WHERE id = ?", (status, listing_id)
        )
        self.conn.commit()

    def set_offer(self, listing_id: str, offer: int) -> None:
        self.conn.execute(
            "UPDATE listings SET offer_price = ? WHERE id = ?", (offer, listing_id)
        )
        self.conn.commit()

    def get(self, listing_id: str) -> sqlite3.Row | None:
        cur = self.conn.execute("SELECT * FROM listings WHERE id = ?", (listing_id,))
        return cur.fetchone()

    def prices_for_model(self, model_key: str) -> list[int]:
        """История цен по этой же модели — основа для медианы."""
        cur = self.conn.execute(
            "SELECT price FROM listings WHERE model_key = ? AND price > 0",
            (model_key,),
        )
        return [r["price"] for r in cur.fetchall()]

    def stats(self) -> dict[str, int]:
        cur = self.conn.execute(
            "SELECT status, COUNT(*) c FROM listings GROUP BY status"
        )
        return {r["status"]: r["c"] for r in cur.fetchall()}


STORE = Store(CFG.db_path)


# --------------------------------------------------------------------- источник


@dataclass
class RawListing:
    id: str
    title: str
    price: int
    url: str
    description: str = ""


class MockSource:
    """Источник для отладки: читает mock_listings.json.

    Нужен, чтобы весь конвейер — разбор, оценка, карточка, кнопки —
    можно было проверить не трогая площадку.
    """

    def __init__(self, path: str = "mock_listings.json"):
        self.path = path

    async def fetch(self) -> list[RawListing]:
        if not os.path.exists(self.path):
            log.warning("Нет файла %s — источник пустой", self.path)
            return []
        with open(self.path, encoding="utf-8") as f:
            data = json.load(f)
        return [RawListing(**item) for item in data]


def _unescape(text: str) -> str:
    """Раскодируем \\uXXXX в заголовке. Через json, а не unicode_escape:
    последний ломает кириллицу, если она пришла как есть."""
    try:
        return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        return text


class HttpSource:
    """Простое чтение страницы выдачи по сохранённому фильтру.

    Это самое хрупкое место всего проекта: разметка меняется, ответ может
    прийти с проверкой вместо данных. Здесь нет никаких обходов защиты —
    если площадка отдаёт заглушку, конвейер просто останется без данных.
    Держите этот класс изолированным, чтобы его можно было заменить
    на другой способ получения данных, не трогая остальной код.
    """

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9",
    }

    def __init__(self, urls: list[str]):
        self.urls = urls

    async def fetch(self) -> list[RawListing]:
        out: list[RawListing] = []
        async with httpx.AsyncClient(
            headers=self.HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in self.urls:
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                except Exception as exc:  # noqa: BLE001
                    log.warning("Не удалось получить %s: %s", url, exc)
                    continue
                items = self._extract(resp.text)
                if not items:
                    log.warning(
                        "Страница получена, но объявлений в ответе нет — "
                        "скорее всего пришла заглушка вместо выдачи"
                    )
                out.extend(items)
        return out

    @staticmethod
    def _extract(html: str) -> list[RawListing]:
        """Достаём объявления из встроенного в страницу JSON.

        Ссылку берём готовую из поля пути, а не собираем из id: у объявления
        адрес вида /город/категория/название_id, и восстановить его по одному
        id нельзя — получится несуществующий путь и редирект на общий раздел.
        """
        results: list[RawListing] = []
        seen: set[str] = set()

        for m in re.finditer(r'"id"\s*:\s*(\d{6,})', html):
            item_id = m.group(1)
            if item_id in seen:
                continue
            window = html[m.end() : m.end() + 2500]

            title_m = re.search(r'"title"\s*:\s*"([^"]{5,200})"', window)
            price_m = re.search(r'"price"\s*:\s*\{[^}]*?"value"\s*:\s*(\d+)', window)
            path_m = re.search(r'"urlPath"\s*:\s*"(/[^"]{10,300})"', window)
            if not (title_m and price_m and path_m):
                continue

            path = path_m.group(1)
            # путь объявления заканчивается тем же id — так отсеиваем
            # статьи журнала и прочие блоки, случайно попавшие в разметку
            if not path.rstrip("/").endswith(item_id):
                continue

            seen.add(item_id)
            results.append(
                RawListing(
                    id=item_id,
                    title=_unescape(title_m.group(1)),
                    price=int(price_m.group(1)),
                    url="https://www.avito.ru" + path,
                )
            )
        return results


def make_source():
    if CFG.use_mock or not CFG.search_urls:
        log.info("Источник: mock_listings.json")
        return MockSource()
    log.info("Источник: %d сохранённых фильтров", len(CFG.search_urls))
    return HttpSource(CFG.search_urls)


# ------------------------------------------------------- разбор характеристик

SPEC_PROMPT = """Ты разбираешь объявление о продаже ноутбука на доске объявлений.
Верни ТОЛЬКО валидный JSON без пояснений и без markdown-разметки.

Схема:
{
  "brand": "производитель или null",
  "model": "модель или null",
  "cpu": "процессор кратко, например 'i5-1135G7' или null",
  "ram_gb": число или null,
  "storage_gb": число или null,
  "gpu": "видеокарта или null",
  "screen_inch": число или null,
  "year": число или null,
  "condition": "new | good | worn | broken | unknown",
  "risks": ["короткие метки проблем"],
  "confidence": число от 0 до 1
}

В risks клади метки только если они прямо следуют из текста:
"не включается", "на запчасти", "нет зарядки", "не проверял",
"следы залития", "битые пиксели", "менялась видеокарта", "нет фото экрана".

Текст объявления:
"""


async def parse_specs(raw: RawListing) -> dict[str, Any]:
    """Разбор заголовка и описания в структуру. Без ключа — грубый запасной разбор."""
    if not CFG.llm_key:
        return _fallback_specs(raw)

    payload = {
        "model": CFG.llm_model,
        "messages": [
            {"role": "user", "content": SPEC_PROMPT + f"{raw.title}\n\n{raw.description}"}
        ],
        "temperature": 0,
        "max_tokens": 500,
    }
    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                f"{CFG.llm_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {CFG.llm_key}"},
                json=payload,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001
        log.warning("Разбор через модель не удался (%s), беру запасной вариант", exc)
        return _fallback_specs(raw)

    cleaned = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        log.warning("Модель вернула не JSON, беру запасной вариант")
        return _fallback_specs(raw)


RISK_PATTERNS = {
    "не включается": r"не включ|не запуска",
    "на запчасти": r"на запчаст|под восстановлен",
    "нет зарядки": r"без заряд|нет заряд|нет блока пит",
    "не проверял": r"не провер|не тестир|как есть",
    "следы залития": r"зали[втл]|попадание жидкост",
    "битые пиксели": r"битые пиксел|засветы|полос[аы] на экран",
    "менялась видеокарта": r"реболл|замена видеочип|прогрев",
}


def _fallback_specs(raw: RawListing) -> dict[str, Any]:
    """Грубый разбор регулярками — работает без модели, но неточно."""
    text = f"{raw.title} {raw.description}".lower()
    ram = re.search(r"(\d{1,2})\s*(?:гб|gb)\s*(?:озу|ram|оперативн)", text)
    if not ram:
        ram = re.search(r"(?:озу|ram|оперативн\w*)\s*(\d{1,2})\s*(?:гб|gb)", text)
    storage = re.search(r"(\d{3,4})\s*(?:гб|gb)\s*(?:ssd|hdd|nvme)", text)
    cpu = re.search(r"\b(i[3579][- ]?\d{4,5}\w*|ryzen\s*\d\s*\d{4}\w*)\b", text)
    brand = re.search(
        r"\b(asus|acer|lenovo|hp|dell|msi|apple|macbook|huawei|honor|xiaomi|samsung)\b",
        text,
    )
    return {
        "brand": brand.group(1) if brand else None,
        "model": None,
        "cpu": cpu.group(1) if cpu else None,
        "ram_gb": int(ram.group(1)) if ram else None,
        "storage_gb": int(storage.group(1)) if storage else None,
        "gpu": None,
        "screen_inch": None,
        "year": None,
        "condition": "unknown",
        "risks": [label for label, pat in RISK_PATTERNS.items() if re.search(pat, text)],
        "confidence": 0.3,
    }


def model_key(specs: dict[str, Any]) -> str:
    """Нормализованный ключ модели — по нему складывается история цен."""
    parts = [
        str(specs.get("brand") or "?").lower(),
        str(specs.get("cpu") or "?").lower().replace(" ", ""),
        f"{specs.get('ram_gb') or '?'}gb",
        f"{specs.get('storage_gb') or '?'}ssd",
    ]
    return "|".join(parts)


# ---------------------------------------------------------------------- оценка


@dataclass
class Estimate:
    fair: int | None          # медиана рынка по этой модели
    offer: int | None         # сколько предлагать продавцу
    sample: int               # на скольких объявлениях построена медиана
    risks: list[str]
    verdict: str              # take | check | skip


def estimate_price(raw: RawListing, specs: dict[str, Any]) -> Estimate:
    risks = list(specs.get("risks") or [])
    key = model_key(specs)
    history = [p for p in STORE.prices_for_model(key) if p > 0]

    fair = int(statistics.median(history)) if len(history) >= 4 else None

    # объявления с флагами риска не оцениваем — только глазами
    hard_risks = {"не включается", "на запчасти", "следы залития", "менялась видеокарта"}
    if hard_risks & set(risks):
        return Estimate(fair, None, len(history), risks, "check")

    if raw.price > CFG.max_price:
        return Estimate(fair, None, len(history), risks, "skip")

    if fair is None:
        # истории мало — считаем от цены объявления, консервативно
        offer = int(raw.price * 0.85) - CFG.prep_reserve
        verdict = "check"
    else:
        ceiling = int(fair * (1 - CFG.target_margin)) - CFG.prep_reserve
        offer = min(ceiling, int(raw.price * 0.9))
        verdict = "take" if raw.price <= ceiling * 1.15 else "skip"

    offer = max(offer, 1000)
    offer = int(round(offer / 500) * 500)  # круглая сумма выглядит как торг, а не расчёт
    return Estimate(fair, offer, len(history), risks, verdict)


# ------------------------------------------------------------- текст сообщения


def rub(amount: int) -> str:
    """Сумма с пробелами между разрядами: 24000 -> '24 000 ₽'."""
    return f"{amount:,}".replace(",", "\u00a0") + "\u00a0₽"


def compose_message(specs: dict[str, Any], est: Estimate) -> str:
    """Первое сообщение продавцу: вопрос по состоянию + цена с обоснованием."""
    reason = "батарея и подготовка под замену"
    if "не проверял" in est.risks:
        reason = "продавец не проверял, беру с риском"
    elif "битые пиксели" in est.risks:
        reason = "дефекты экрана"

    questions = "сколько держит батарея от розетки, разбирался ли для чистки, есть ли следы залития"
    if specs.get("gpu"):
        questions += ", не грелся ли под нагрузкой"

    if est.offer:
        return (
            f"Здравствуйте! Интересует ваш ноутбук. Подскажите, пожалуйста: {questions}?\n\n"
            f"Если всё в порядке — готов забрать сегодня за {rub(est.offer)} наличными, "
            f"подъеду сам. Цена ниже вашей, ориентируюсь на то, что {reason}. "
            f"Если не подходит — скажите вашу минимальную."
        )
    return (
        f"Здравствуйте! Интересует ваш ноутбук. Подскажите, пожалуйста: {questions}? "
        f"Если состояние нормальное — готов подъехать сегодня."
    )


VERDICT_LABEL = {
    "take": "берём",
    "check": "проверить лично",
    "skip": "дорого",
}


def compose_card(raw: RawListing, specs: dict[str, Any], est: Estimate) -> str:
    spec_line = " / ".join(
        str(v)
        for v in (
            specs.get("brand"),
            specs.get("cpu"),
            f"{specs['ram_gb']} ГБ" if specs.get("ram_gb") else None,
            f"{specs['storage_gb']} ГБ" if specs.get("storage_gb") else None,
            specs.get("gpu"),
        )
        if v
    ) or "характеристики не распознаны"

    lines = [
        f"{raw.title}",
        f"Цена в объявлении: {rub(raw.price)}",
        f"Характеристики: {spec_line}",
    ]
    if est.fair:
        lines.append(
            f"Медиана по модели: {rub(est.fair)} (по {est.sample} объявлениям)"
        )
    else:
        lines.append(f"Медианы пока нет — в базе {est.sample} объявлений этой модели")
    if est.offer:
        lines.append(f"Предложить: {rub(est.offer)}")
    if est.risks:
        lines.append("Риски: " + ", ".join(est.risks))
    lines.append(f"Вердикт: {VERDICT_LABEL.get(est.verdict, est.verdict)}")
    lines.append(raw.url)
    return "\n".join(lines)


def keyboard(listing_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Взять", callback_data=f"take:{listing_id}"),
                InlineKeyboardButton(text="Своя цена", callback_data=f"edit:{listing_id}"),
                InlineKeyboardButton(text="Мимо", callback_data=f"skip:{listing_id}"),
            ]
        ]
    )


# ------------------------------------------------------------------------- бот

dp = Dispatcher()
PENDING_EDIT: dict[int, str] = {}  # user_id -> listing_id, ждём цену числом


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.answer(
        "Сниппер запущен. Присылаю новые объявления по вашим фильтрам.\n"
        "Кнопки: Взять — готовый текст для продавца, Своя цена — пересчитать, "
        "Мимо — больше не показывать.\n"
        "/stats — что накопилось в базе."
    )


@dp.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    s = STORE.stats()
    total = sum(s.values())
    await message.answer(
        f"Всего объявлений в базе: {total}\n"
        f"Новых: {s.get('new', 0)}\n"
        f"Написали: {s.get('offered', 0)}\n"
        f"Пропущено: {s.get('skipped', 0)}"
    )


@dp.callback_query(F.data.startswith("take:"))
async def on_take(callback: CallbackQuery) -> None:
    listing_id = callback.data.split(":", 1)[1]
    row = STORE.get(listing_id)
    if row is None:
        await callback.answer("Объявление не найдено")
        return

    specs = json.loads(row["specs_json"])
    est = Estimate(
        fair=row["fair_price"],
        offer=row["offer_price"],
        sample=0,
        risks=json.loads(row["risks_json"]),
        verdict="take",
    )
    STORE.set_status(listing_id, "offered")
    await callback.message.answer(
        "Текст для продавца — скопируйте и вставьте в чат объявления:\n\n"
        + compose_message(specs, est)
        + f"\n\n{row['url']}"
    )
    await callback.answer("Готово")


@dp.callback_query(F.data.startswith("edit:"))
async def on_edit(callback: CallbackQuery) -> None:
    listing_id = callback.data.split(":", 1)[1]
    PENDING_EDIT[callback.from_user.id] = listing_id
    await callback.message.answer("Пришлите свою цену числом, пересчитаю текст.")
    await callback.answer()


@dp.callback_query(F.data.startswith("skip:"))
async def on_skip(callback: CallbackQuery) -> None:
    listing_id = callback.data.split(":", 1)[1]
    STORE.set_status(listing_id, "skipped")
    await callback.answer("Пропущено")


@dp.message(F.text.regexp(r"^\d[\d\s]*$"))
async def on_custom_price(message: Message) -> None:
    listing_id = PENDING_EDIT.pop(message.from_user.id, None)
    if listing_id is None:
        return
    row = STORE.get(listing_id)
    if row is None:
        await message.answer("Объявление не найдено")
        return

    price = int(re.sub(r"\s", "", message.text))
    STORE.set_offer(listing_id, price)
    STORE.set_status(listing_id, "offered")
    specs = json.loads(row["specs_json"])
    est = Estimate(row["fair_price"], price, 0, json.loads(row["risks_json"]), "take")
    await message.answer(
        "Текст для продавца:\n\n" + compose_message(specs, est) + f"\n\n{row['url']}"
    )


# ----------------------------------------------------------------- цикл опроса


async def process_listing(bot: Bot, raw: RawListing) -> None:
    specs = await parse_specs(raw)
    est = estimate_price(raw, specs)

    STORE.insert(
        {
            "id": raw.id,
            "title": raw.title,
            "price": raw.price,
            "url": raw.url,
            "description": raw.description,
            "model_key": model_key(specs),
            "specs_json": json.dumps(specs, ensure_ascii=False),
            "risks_json": json.dumps(est.risks, ensure_ascii=False),
            "fair_price": est.fair,
            "offer_price": est.offer,
        }
    )

    # молча копим статистику по тому, что заведомо не берём
    if est.verdict == "skip":
        STORE.set_status(raw.id, "skipped")
        log.info("Пропуск (дорого): %s — %s ₽", raw.title[:50], raw.price)
        return

    await bot.send_message(
        CFG.owner_id,
        compose_card(raw, specs, est),
        reply_markup=keyboard(raw.id),
        disable_web_page_preview=False,
    )


async def poll_loop(bot: Bot) -> None:
    source = make_source()
    log.info("Цикл опроса запущен, интервал %d с", CFG.poll_seconds)
    while True:
        try:
            listings = await source.fetch()
            fresh = 0
            for raw in listings:
                if STORE.is_known(raw.id):
                    STORE.touch(raw.id)
                    continue
                fresh += 1
                await process_listing(bot, raw)
            log.info("Опрос: %d объявлений, из них новых %d", len(listings), fresh)
        except Exception as exc:  # noqa: BLE001
            log.exception("Ошибка в цикле опроса: %s", exc)
        await asyncio.sleep(CFG.poll_seconds)


async def main() -> None:
    bot = Bot(CFG.telegram_token)
    asyncio.create_task(poll_loop(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
