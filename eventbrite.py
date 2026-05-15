"""
Сборщик Eventbrite — парсит публичные страницы поиска по ключевикам.
Города: London, Berlin.
Извлекает: количество событий, средняя цена, топ-3 названия.
"""

import json
import time
import random
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("trend_radar.eventbrite")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _parse_events_from_html(html: str) -> list[dict]:
    """Извлекает события из JSON-LD блоков страницы Eventbrite."""
    soup = BeautifulSoup(html, "lxml")
    events = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "{}")
        except (json.JSONDecodeError, TypeError):
            continue

        if isinstance(data, list):
            for item in data:
                if item.get("@type") == "Event":
                    events.append(item)
        elif isinstance(data, dict):
            if data.get("@type") == "Event":
                events.append(data)
            elif data.get("@type") == "ItemList":
                for el in data.get("itemListElement", []):
                    item = el.get("item", el)
                    if isinstance(item, dict) and item.get("@type") == "Event":
                        events.append(item)

    return events


def _extract_price(event: dict) -> Optional[float]:
    """Извлекает цену из offers события."""
    offers = event.get("offers", [])
    if isinstance(offers, dict):
        offers = [offers]
    for offer in offers:
        for field in ("price", "lowPrice"):
            val = offer.get(field)
            if val:
                try:
                    p = float(val)
                    if p > 0:
                        return p
                except (ValueError, TypeError):
                    pass
    return None


def fetch_keyword_city(keyword: str, city: dict) -> dict:
    """
    Парсит одну страницу поиска Eventbrite.
    Возвращает: {events_count, avg_price, currency, sample_titles}
    """
    slug = keyword.lower().replace(" ", "-").replace("&", "and")
    url = f"{city['base_url']}/d/{city['search_prefix']}/{slug}/"

    result = {
        "events_count": 0,
        "avg_price": None,
        "currency": None,
        "sample_titles": [],
    }

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 429:
            log.warning(f"  Rate-limited на {city['name']}, пауза 30 сек")
            time.sleep(30)
            r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code != 200:
            log.warning(f"  HTTP {r.status_code}: {url}")
            return result

        events = _parse_events_from_html(r.text)
        result["events_count"] = len(events)

        # Цены
        prices = []
        for ev in events:
            p = _extract_price(ev)
            if p is not None:
                prices.append(p)

        if prices:
            result["avg_price"] = round(sum(prices) / len(prices), 2)

        # Валюта (берём из первого найденного)
        for ev in events:
            offers = ev.get("offers", [])
            if isinstance(offers, dict):
                offers = [offers]
            for offer in offers:
                cur = offer.get("priceCurrency")
                if cur:
                    result["currency"] = cur
                    break
            if result["currency"]:
                break

        # Топ-3 названия (для отладки и для AI потом)
        result["sample_titles"] = [
            ev.get("name", "")[:100]
            for ev in events[:5]
            if ev.get("name")
        ]

    except requests.RequestException as e:
        log.error(f"  Сеть Eventbrite: {e}")
    except Exception as e:
        log.error(f"  Парсинг Eventbrite: {e}")

    return result


def collect(keywords_flat: list, cities: list) -> list[dict]:
    """
    Прогоняет все ключевики по всем городам Eventbrite.
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
            log.info(f"[{counter}/{total}] Eventbrite: '{kw}' / {city['name']}")

            res = fetch_keyword_city(kw, city)
            city_results.append({
                "city": city["name"],
                **res,
            })

            # Пауза 2–4 сек между запросами
            time.sleep(random.uniform(2.0, 4.0))

        # Агрегация по городам
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
            "source": "eventbrite",
            "events_count": total_events,
            "avg_price": avg_price,
            "currency": next(
                (r["currency"] for r in city_results if r["currency"]),
                None,
            ),
            "top_city": top_city,
            "sample_titles": json.dumps(all_titles[:5], ensure_ascii=False),
        })

    return rows
