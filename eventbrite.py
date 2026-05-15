"""
Сборщик Eventbrite v0.3 — парсинг __SERVER_DATA__ JSON-блока внутри HTML.
API Eventbrite закрыт с 2020, поэтому парсим публичные страницы.
Работает с домашнего IP. Из дата-центров (GitHub Actions, Colab) — 403.

Города: London, Berlin.
Не требует токена.

ЧТО ИЗМЕНИЛОСЬ В v0.3:
- Источник events_count теперь __SERVER_DATA__.search_data.events.pagination.object_count
  (раньше = число article-карточек, было максимум 23 на страницу)
- Промо-результаты (promoted_results) исключены: они одинаковые на все запросы
  и засоряли счёт + цены
- Цены: на странице поиска у органических событий цены нет (Eventbrite их там
  не рендерит, только у промо). avg_price = None для всех. Цены вернём
  в Волне 2 через фетч детальных страниц топ-N трендов.
- sample_titles берутся из JSON-результатов, не из HTML-карточек

ИСТОЧНИК ИСТИНЫ:
  Внутри HTML страницы лежит блок `window.__SERVER_DATA__ = {...};` с тем же
  JSON, который раньше отдавал API. В нём:
    search_data.events.pagination.object_count   — общее число событий по запросу
    search_data.events.results                   — органические события страницы 1
    search_data.events.promoted_results          — промо, игнорируем
"""

import json
import time
import random
import logging
import re

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("trend_radar.eventbrite")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

URL_TEMPLATES = {
    "London": "https://www.eventbrite.co.uk/d/united-kingdom--london/{slug}/",
    "Berlin": "https://www.eventbrite.de/d/germany--berlin/{slug}/",
}

CURRENCY_BY_CITY = {
    "London": "GBP",
    "Berlin": "EUR",
}


def _extract_server_data(html: str) -> dict | None:
    """Достаёт window.__SERVER_DATA__ = {...}; как dict.

    Не используем regex .*? потому что JSON большой и вложенный.
    Считаем фигурные скобки с учётом строк и экранирования.
    """
    m = re.search(r"window\.__SERVER_DATA__\s*=\s*", html)
    if not m:
        return None

    start = m.end()
    depth = 0
    in_str = False
    escape = False
    end = None

    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end is None:
        return None
    try:
        return json.loads(html[start:end])
    except (json.JSONDecodeError, ValueError):
        return None


def _parse_page(html: str, city_name: str) -> dict:
    """Парсит одну страницу результатов поиска Eventbrite."""
    result = {
        "events_count": 0,
        "avg_price": None,        # пока всегда None — см. модуль docstring
        "currency": CURRENCY_BY_CITY.get(city_name),
        "sample_titles": [],
    }

    data = _extract_server_data(html)
    if not data:
        # Fallback: считаем органические карточки на странице (исключая промо).
        # Это сильно занижает счёт, но лучше чем ноль.
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select('[data-testid="search-event"]')
        # промо нельзя надёжно отделить без JSON, так что считаем все карточки минус ~3
        result["events_count"] = max(0, len(cards) - 3)
        return result

    events_block = (data.get("search_data") or {}).get("events") or {}
    pagination = events_block.get("pagination") or {}
    result["events_count"] = pagination.get("object_count", 0)

    results = events_block.get("results") or []
    result["sample_titles"] = [
        r.get("name", "")[:100] for r in results[:5] if r.get("name")
    ]

    return result


def fetch_keyword_city(keyword: str, city_name: str) -> dict:
    """Парсит одну страницу поиска Eventbrite для ключевика и города."""
    template = URL_TEMPLATES.get(city_name)
    if not template:
        log.warning(f"  Нет шаблона URL для {city_name}")
        return {
            "events_count": 0,
            "avg_price": None,
            "currency": None,
            "sample_titles": [],
        }

    slug = keyword.lower().replace(" ", "-").replace("&", "and")
    url = template.format(slug=slug)

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code == 429:
            log.warning("  Rate-limit, pause 30s")
            time.sleep(30)
            r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code != 200:
            log.warning(f"  HTTP {r.status_code}: {url}")
            return {
                "events_count": 0,
                "avg_price": None,
                "currency": CURRENCY_BY_CITY.get(city_name),
                "sample_titles": [],
            }

        return _parse_page(r.text, city_name)

    except requests.RequestException as e:
        log.error(f"  Network error: {e}")
        return {
            "events_count": 0,
            "avg_price": None,
            "currency": CURRENCY_BY_CITY.get(city_name),
            "sample_titles": [],
        }


def collect(keywords_flat: list, cities: list) -> list[dict]:
    """Прогоняет EN-ключевики по западным городам.

    cities — список dict с ключом 'name', например [{'name': 'London'}, {'name': 'Berlin'}].
    Города не из URL_TEMPLATES пропускаются (это russian cities, их берёт kudago).
    """
    rows = []

    # Отфильтруем только западные города (которые мы умеем парсить)
    western_cities = [c for c in cities if c["name"] in URL_TEMPLATES]
    city_names = [c["name"] for c in western_cities]

    if not city_names:
        log.warning("Нет западных городов для Eventbrite")
        return rows

    total = len(keywords_flat) * len(city_names)
    counter = 0

    for item in keywords_flat:
        kw, cat = item["keyword"], item["category"]
        city_results = []

        for city_name in city_names:
            counter += 1
            log.info(f"[{counter}/{total}] Eventbrite: '{kw}' / {city_name}")
            res = fetch_keyword_city(kw, city_name)
            city_results.append({"city": city_name, **res})
            time.sleep(random.uniform(2.5, 5.0))

        # Агрегация по городам
        total_events = sum(r["events_count"] for r in city_results)
        # avg_price пока всегда None для всех городов, поэтому общий тоже None
        avg_price = None
        # top_city по числу событий
        top_city = max(city_results, key=lambda r: r["events_count"])["city"]
        # currency: берём первую непустую (на случай если для города None)
        currency = next((r["currency"] for r in city_results if r["currency"]), None)
        # titles
        titles = [t for r in city_results for t in r["sample_titles"]]

        rows.append({
            "keyword": kw,
            "category": cat,
            "source": "eventbrite",
            "events_count": total_events,
            "avg_price": avg_price,
            "currency": currency,
            "top_city": top_city,
            "sample_titles": json.dumps(titles[:5], ensure_ascii=False),
        })

    return rows


# Сам-тест: запустить локально на сохранённой странице
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        with open(sys.argv[1], encoding="utf-8") as f:
            html = f.read()
        print(json.dumps(_parse_page(html, "London"), indent=2, ensure_ascii=False))
    else:
        print("Usage: python eventbrite.py path/to/saved_page.html")
