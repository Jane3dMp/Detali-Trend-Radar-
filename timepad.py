"""
Сборщик Timepad — использует публичный API поиска событий.
Города: Москва, Санкт-Петербург.

Timepad имеет публичный API: https://dev.timepad.ru/api/
GET https://api.timepad.ru/v1/events?
    cities={city_id}
    &keywords={keyword}
    &starts_at_min={date}
    &category_ids=452,453,454  (мастер-классы, хобби, развлечения)
    &limit=50

Не требует токена для базового поиска.
"""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

log = logging.getLogger("trend_radar.timepad")

TIMEPAD_API = "https://api.timepad.ru/v1/events"

# Категории Timepad, релевантные для мастер-классов:
# 452 — Обучение и развитие
# 453 — Хобби и творчество
# 454 — Развлечения
# 379 — Еда и напитки
CATEGORY_IDS = "452,453,454,379"

HEADERS = {
    "User-Agent": "TrendRadar/0.2",
    "Accept": "application/json",
}


def _search_events(keyword: str, city_id: str, limit: int = 50) -> list[dict]:
    """
    Ищет события через Timepad API.
    Возвращает список событий из JSON-ответа.
    """
    # Ищем события на ближайшие 60 дней (чтобы видеть актуальное предложение)
    starts_min = datetime.now().strftime("%Y-%m-%d")
    starts_max = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")

    params = {
        "limit": limit,
        "skip": 0,
        "cities": city_id,
        "keywords": keyword,
        "starts_at_min": starts_min,
        "starts_at_max": starts_max,
        "category_ids": CATEGORY_IDS,
        "sort": "+starts_at",
        "fields": "name,min_price,max_price,url,categories",
    }

    try:
        r = requests.get(TIMEPAD_API, params=params, headers=HEADERS, timeout=15)

        if r.status_code == 429:
            log.warning(f"  Timepad rate-limit, пауза 15 сек")
            time.sleep(15)
            r = requests.get(TIMEPAD_API, params=params, headers=HEADERS, timeout=15)

        if r.status_code != 200:
            log.warning(f"  Timepad HTTP {r.status_code} для '{keyword}' / {city_id}")
            return []

        data = r.json()
        return data.get("values", [])

    except requests.RequestException as e:
        log.error(f"  Сеть Timepad: {e}")
        return []
    except (json.JSONDecodeError, KeyError) as e:
        log.error(f"  Парсинг Timepad: {e}")
        return []


def _extract_price(event: dict) -> Optional[float]:
    """Извлекает цену из события Timepad."""
    # min_price — минимальная цена билета
    for field in ("min_price", "max_price"):
        val = event.get(field)
        if val is not None:
            try:
                p = float(val)
                if p > 0:
                    return p
            except (ValueError, TypeError):
                pass
    return None


def fetch_keyword_city(keyword: str, city: dict) -> dict:
    """
    Ищет события по ключевику в одном городе через Timepad API.
    Возвращает: {events_count, avg_price, currency, sample_titles}
    """
    result = {
        "events_count": 0,
        "avg_price": None,
        "currency": "RUB",
        "sample_titles": [],
    }

    events = _search_events(keyword, city["city_id"])
    result["events_count"] = len(events)

    # Цены
    prices = []
    for ev in events:
        p = _extract_price(ev)
        if p is not None:
            prices.append(p)

    if prices:
        result["avg_price"] = round(sum(prices) / len(prices), 2)

    # Названия для отладки и для AI
    result["sample_titles"] = [
        ev.get("name", "")[:100]
        for ev in events[:5]
        if ev.get("name")
    ]

    return result


def collect(keywords_flat: list, cities: list) -> list[dict]:
    """
    Прогоняет русскоязычные ключевики по Москве и СПб.
    Возвращает список строк для записи в БД.
    """
    rows = []
    total = len(keywords_flat) * len(cities)
    counter = 0

    for item in keywords_flat:
        kw = item["keyword"]
        cat = item["category"]

        city_results = []

        for city in cities:
            counter += 1
            log.info(f"[{counter}/{total}] Timepad: '{kw}' / {city['name_ru']}")

            res = fetch_keyword_city(kw, city)
            city_results.append({
                "city": city["name"],
                **res,
            })

            # Пауза 1–2 сек (Timepad не такой строгий как Eventbrite)
            time.sleep(random.uniform(1.0, 2.0))

        # Агрегация
        total_events = sum(r["events_count"] for r in city_results)
        prices_weighted = [
            (r["avg_price"], r["events_count"])
            for r in city_results
            if r["avg_price"] is not None and r["events_count"] > 0
        ]
        avg_price = None
        if prices_weighted:
            tw = sum(w for _, w in prices_weighted)
            if tw > 0:
                avg_price = round(sum(p * w for p, w in prices_weighted) / tw, 2)

        top_city = max(city_results, key=lambda r: r["events_count"])["city"]

        all_titles = []
        for r in city_results:
            all_titles.extend(r["sample_titles"])

        rows.append({
            "keyword": kw,
            "category": cat,
            "source": "timepad",
            "events_count": total_events,
            "avg_price": avg_price,
            "currency": "RUB",
            "top_city": top_city,
            "sample_titles": json.dumps(all_titles[:5], ensure_ascii=False),
        })

    return rows
