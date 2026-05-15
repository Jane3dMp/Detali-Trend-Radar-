"""
Расчёт signal_score для Trend Radar v0.2.

Особенность: у нас два типа источников с разными масштабами:
  - Eventbrite (London, Berlin): цены в GBP/EUR, чеки £30–150
  - Timepad (Москва, СПб): цены в RUB, чеки 1500–8000₽

Логика signal_score:
  1. Для каждого ключевика суммируем сигналы из ВСЕХ источников.
  2. Ключевик, который есть на Eventbrite И на Timepad — получает бонус
     (кросс-платформенная валидация).
  3. Ключевик, который есть на Eventbrite, но НЕТ на Timepad — получает
     бонус «пустота рынка» (ещё не пришло в Россию → окно возможности).

Веса (можно менять для калибровки):
"""

# ── Веса ─────────────────────────────────────────────────────────────────

# Максимум баллов по каждой оси — в сумме 100
W_WEST_EVENTS = 25       # Количество событий на западе (Eventbrite)
W_WEST_PRICE = 15        # Средняя цена на западе (платёжеспособность ниши)
W_RU_EVENTS = 15         # Количество событий в РФ (Timepad)
W_OPPORTUNITY = 25        # Окно возможности (есть на западе + мало в РФ)
W_PRICE_PREMIUM = 10     # Ценовая премия (высокий чек = маржинальная ниша)
W_DIVERSITY = 10          # Бонус за наличие в нескольких городах

# Пороги нормализации
WEST_EVENTS_MAX = 30      # 30+ событий на Eventbrite = максимум
WEST_PRICE_MAX = 120      # £120+ = максимум по цене
RU_EVENTS_MAX = 20        # 20+ событий на Timepad = максимум
RU_PRICE_MAX = 6000       # 6000₽+ = максимум по цене


def compute_signal_score(west_row: dict = None, ru_row: dict = None) -> float:
    """
    Считает signal_score 0–100 для одного ключевика.

    west_row: строка из Eventbrite-сборщика (или None если не найдено)
    ru_row: строка из Timepad-сборщика (или None если не найдено)
    """
    score = 0.0

    # ── Западные события ─────────────────────────────────────────────
    west_events = 0
    west_price = 0
    if west_row:
        west_events = west_row.get("events_count", 0) or 0
        west_price = west_row.get("avg_price", 0) or 0

    # Нормализация: 0 → 0, WEST_EVENTS_MAX+ → 1
    score += min(west_events / WEST_EVENTS_MAX, 1.0) * W_WEST_EVENTS
    score += min(west_price / WEST_PRICE_MAX, 1.0) * W_WEST_PRICE

    # ── Российские события ───────────────────────────────────────────
    ru_events = 0
    ru_price = 0
    if ru_row:
        ru_events = ru_row.get("events_count", 0) or 0
        ru_price = ru_row.get("avg_price", 0) or 0

    score += min(ru_events / RU_EVENTS_MAX, 1.0) * W_RU_EVENTS

    # ── Окно возможности ─────────────────────────────────────────────
    # Чем больше на западе И чем меньше в РФ — тем больше бонус.
    # Формула: west_ratio × (1 - ru_ratio)
    # Если на западе 30 событий, а в РФ 0 → 1.0 × 1.0 = 1.0 → 25 баллов
    # Если на западе 30 событий и в РФ 20 → 1.0 × 0.0 = 0.0 → 0 баллов
    # Если на западе 0 → 0 × anything = 0 → 0 баллов
    west_ratio = min(west_events / max(WEST_EVENTS_MAX, 1), 1.0)
    ru_ratio = min(ru_events / max(RU_EVENTS_MAX, 1), 1.0)
    opportunity = west_ratio * (1.0 - ru_ratio)
    score += opportunity * W_OPPORTUNITY

    # ── Ценовая премия ───────────────────────────────────────────────
    # Высокая цена на западе = маржинальная ниша
    score += min(west_price / WEST_PRICE_MAX, 1.0) * W_PRICE_PREMIUM

    # ── Бонус за разнообразие городов ────────────────────────────────
    # Если есть на обеих платформах — это кросс-валидация тренда
    if west_events > 0 and ru_events > 0:
        score += W_DIVERSITY * 0.5   # Есть на обеих — полбонуса
    elif west_events > 0 and ru_events == 0:
        score += W_DIVERSITY         # Только на западе — полный бонус (ещё не пришло)

    return round(min(score, 100.0), 1)


def merge_west_and_ru(west_rows: list[dict], ru_rows: list[dict]) -> list[dict]:
    """
    Сводит данные Eventbrite и Timepad по категориям.
    Маппинг EN↔RU идёт через category (одинаковый в обоих словарях).
    Возвращает список с signal_score.
    """
    # Группируем по (keyword, category)
    west_by_cat = {}
    for row in west_rows:
        cat = row["category"]
        if cat not in west_by_cat:
            west_by_cat[cat] = []
        west_by_cat[cat].append(row)

    ru_by_cat = {}
    for row in ru_rows:
        cat = row["category"]
        if cat not in ru_by_cat:
            ru_by_cat[cat] = []
        ru_by_cat[cat].append(row)

    # Для каждого западного ключевика берём лучший RU-сигнал
    # из той же категории (или None если нет)
    results = []

    for row in west_rows:
        cat = row["category"]
        # Лучший RU-аналог по числу событий
        ru_matches = ru_by_cat.get(cat, [])
        best_ru = max(ru_matches, key=lambda r: r.get("events_count", 0)) if ru_matches else None

        score = compute_signal_score(west_row=row, ru_row=best_ru)
        row["signal_score"] = score

        # Добавляем RU-контекст для отладки
        if best_ru:
            row["ru_events_count"] = best_ru.get("events_count", 0)
            row["ru_avg_price"] = best_ru.get("avg_price")
            row["ru_keyword"] = best_ru.get("keyword", "")
        else:
            row["ru_events_count"] = 0
            row["ru_avg_price"] = None
            row["ru_keyword"] = ""

        results.append(row)

    # RU-ключевики, у которых НЕТ западного аналога (чисто российский тренд) —
    # тоже записываем, но с низким score (нет западного сигнала опережения)
    west_cats_covered = {row["category"] for row in west_rows}
    for row in ru_rows:
        if row["category"] not in west_cats_covered:
            score = compute_signal_score(west_row=None, ru_row=row)
            row["signal_score"] = score
            row["ru_events_count"] = row.get("events_count", 0)
            row["ru_avg_price"] = row.get("avg_price")
            row["ru_keyword"] = row.get("keyword", "")
            results.append(row)

    results.sort(key=lambda r: r.get("signal_score", 0), reverse=True)
    return results


if __name__ == "__main__":
    # Тест на синтетических данных
    test_cases = [
        ("tufting workshop", {"events_count": 25, "avg_price": 95}, {"events_count": 2, "avg_price": 3500}),
        ("paint and sip", {"events_count": 35, "avg_price": 65}, {"events_count": 15, "avg_price": 4000}),
        ("pottery class", {"events_count": 40, "avg_price": 55}, {"events_count": 25, "avg_price": 3000}),
        ("sound bath workshop", {"events_count": 10, "avg_price": 80}, None),
        ("punch needle", {"events_count": 8, "avg_price": 70}, {"events_count": 0, "avg_price": None}),
    ]

    print(f"{'Keyword':<25} {'West ev':>8} {'West £':>8} {'RU ev':>6} {'Score':>7}")
    print("─" * 60)
    for name, west, ru in test_cases:
        s = compute_signal_score(west, ru)
        ru_ev = ru["events_count"] if ru else "—"
        print(f"{name:<25} {west['events_count']:>8} {west['avg_price']:>8} {str(ru_ev):>6} {s:>7}")
