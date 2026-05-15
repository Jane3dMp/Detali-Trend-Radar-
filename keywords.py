"""
Словарь ключевиков для Trend Radar «Детали» v0.2
Фокус: взрослая аудитория (девичники, корпоративы, вечера с подругами).
"""

# ── Категории ключевиков (англ — для Eventbrite London/Berlin) ──────────

KEYWORDS_EN = {
    "paint_and_sip": [
        "paint and sip",
        "sip and paint",
        "paint and prosecco",
        "wine and paint night",
        "sip and sculpt",
    ],
    "candle_perfume_soap": [
        "candle making workshop",
        "candle making class",
        "perfume making workshop",
        "soap making class",
        "scent workshop",
    ],
    "pottery_ceramics": [
        "pottery class",
        "pottery wheel class",
        "ceramics workshop",
        "pottery date night",
        "couples pottery class",
    ],
    "flowers_terrarium": [
        "flower arrangement workshop",
        "floristry class",
        "terrarium workshop",
        "moss art workshop",
    ],
    "textile_craft": [
        "embroidery workshop",
        "punch needle workshop",
        "macrame workshop",
        "tufting workshop",
        "rug tufting class",
    ],
    "leather_jewelry": [
        "leather workshop",
        "leather goods class",
        "ring making workshop",
        "silver jewelry workshop",
    ],
    "food_drink": [
        "cocktail making class",
        "mixology workshop",
        "cheese making workshop",
        "wine tasting workshop",
        "chocolate workshop",
        "pasta making class",
        "sushi making class",
    ],
    "hen_party_events": [
        "hen party activities",
        "hen do ideas",
        "bachelorette party workshop",
        "girls night out workshop",
        "ladies night activities",
    ],
    "corporate_teambuilding": [
        "team building workshop",
        "corporate craft workshop",
        "creative team building",
    ],
    "wellness_creative": [
        "sound bath workshop",
        "journaling workshop",
        "collage workshop",
        "art therapy workshop",
    ],
    "drawing_art": [
        "life drawing class",
        "life drawing hen party",
        "watercolor workshop",
        "portrait drawing class",
    ],
}

# ── Ключевики на русском (для Timepad Москва/СПб) ──────────────────────

KEYWORDS_RU = {
    "paint_and_sip": [
        "рисование с вином",
        "paint and sip",
        "рисуем и пьём",
        "арт-вечеринка",
    ],
    "candle_perfume_soap": [
        "мастер-класс свечи",
        "мастер-класс по свечам",
        "создание парфюма",
        "мастер-класс мыло",
        "ароматическая свеча",
    ],
    "pottery_ceramics": [
        "гончарный мастер-класс",
        "мастер-класс керамика",
        "гончарный круг",
        "лепка из глины",
    ],
    "flowers_terrarium": [
        "мастер-класс букет",
        "флористика мастер-класс",
        "мастер-класс флорариум",
        "террариум мастер-класс",
    ],
    "textile_craft": [
        "мастер-класс вышивка",
        "панч нидл мастер-класс",
        "макраме мастер-класс",
        "тафтинг мастер-класс",
        "ковровая вышивка",
    ],
    "leather_jewelry": [
        "мастер-класс кожа",
        "мастер-класс кольцо",
        "ювелирный мастер-класс",
        "работа с кожей",
    ],
    "food_drink": [
        "мастер-класс коктейли",
        "миксология мастер-класс",
        "мастер-класс сыр",
        "мастер-класс шоколад",
        "мастер-класс суши",
        "кулинарный мастер-класс",
        "винная дегустация",
    ],
    "hen_party_events": [
        "девичник мастер-класс",
        "мастер-класс на девичник",
        "вечеринка для подруг",
        "девичник идеи",
    ],
    "corporate_teambuilding": [
        "тимбилдинг мастер-класс",
        "корпоратив мастер-класс",
        "творческий тимбилдинг",
    ],
    "wellness_creative": [
        "арт-терапия мастер-класс",
        "журналинг мастер-класс",
        "коллаж мастер-класс",
        "звуковая медитация",
    ],
    "drawing_art": [
        "рисование обнажённой натуры",
        "мастер-класс акварель",
        "мастер-класс портрет",
        "скетчинг мастер-класс",
    ],
}

# ── Города ───────────────────────────────────────────────────────────────

CITIES_WEST = [
    {
        "name": "London",
        "country": "UK",
        "platform": "eventbrite",
        "base_url": "https://www.eventbrite.co.uk",
        "search_prefix": "united-kingdom--london",
    },
    {
        "name": "Berlin",
        "country": "DE",
        "platform": "eventbrite",
        "base_url": "https://www.eventbrite.de",
        "search_prefix": "germany--berlin",
    },
]

CITIES_RU = [
    {
        "name": "Moscow",
        "name_ru": "Москва",
        "platform": "timepad",
        "city_id": "moskva",
    },
    {
        "name": "Saint Petersburg",
        "name_ru": "Санкт-Петербург",
        "platform": "timepad",
        "city_id": "sankt-peterburg",
    },
]


# ── Утилиты ──────────────────────────────────────────────────────────────

def all_keywords_en_flat():
    """Плоский список англоязычных ключевиков с категорией."""
    return [
        {"category": cat, "keyword": kw}
        for cat, kws in KEYWORDS_EN.items()
        for kw in kws
    ]


def all_keywords_ru_flat():
    """Плоский список русскоязычных ключевиков с категорией."""
    return [
        {"category": cat, "keyword": kw}
        for cat, kws in KEYWORDS_RU.items()
        for kw in kws
    ]


if __name__ == "__main__":
    en = all_keywords_en_flat()
    ru = all_keywords_ru_flat()
    print(f"EN ключевиков: {len(en)} в {len(KEYWORDS_EN)} категориях")
    print(f"RU ключевиков: {len(ru)} в {len(KEYWORDS_RU)} категориях")
    print(f"Западных городов: {len(CITIES_WEST)}")
    print(f"Российских городов: {len(CITIES_RU)}")
