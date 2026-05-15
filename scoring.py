"""
Расчёт signal_score для Trend Radar v0.3.

Особенность v0.3: events_count теперь полный (object_count из Eventbrite
__SERVER_DATA__), разлёт ~20–5000+. Используем log10-шкалу, чтобы редкие
темы не теряли голос.

Источники:
  - Eventbrite (London, Berlin): events_count полный, avg_price = None
    (цены вернём в Волне 2 через фетч детальных страниц топа)
  - KudaGo (Москва, СПб): events_count из API. Цены частично.

Логика signal_score (сумма 100):
  - W_WEST_SCALE (30): log10 от числа событий на западе.
  - W_RU_EMPTINESS (30): насколько мало (или нет) этой темы в РФ.
  - W_OPPORTUNITY (30): пересечение «горячо на западе» × «пусто в РФ» —
    главный сигнал, ради которого мы это делаем.
  - W_DIVERSITY (10): бонус за попадание в обе платформы (валидация).

ОТЛИЧИЯ ОТ v0.2:
  - Цены убраны из формулы (avg_price = None в v0.3 для Eventbrite).
  - Запад нормируется через log10, не линейно. WEST_EVENTS_NORM_LOG = 3.5
    (10^3.5 ≈ 3162 событий = эталонный «горячий» рынок).
  - Веса перераспределены: 25+15+15+25+10+10 → 30+30+30+10.
"""

import math

# ── Веса (сумма 100) ────────────────────────────────────────────────
W_WEST_SCALE = 30      # log10-нормированное число событий на западе
W_RU_EMPTINESS = 30    # насколько пусто в РФ
W_OPPORTUNITY = 30     # окно возможности (запад × отсутствие в РФ)
W_DIVERSITY = 10       # бонус за валидацию обеими платформами

# ── Параметры шкалы ─────────────────────────────────────────────────
# log10(events) при котором запад считается «полностью горячим»
# 10^3.5 ≈ 3162 событий, 10^4.0 = 10000
WEST_EVENTS_NORM_LOG = 3.5

# Для RU: до этого числа считаем «пусто», после — «есть»
# KudaGo обычно даёт 0–50 событий по запросу
RU_EVENTS_NORM_LOG = 1.7   # 10^1.7 ≈ 50

# Минимальный западный сигнал, ниже которого тему вообще не считаем
WEST_MIN_EVENTS = 10


def _log_norm(value: float, target_log: float) -> float:
    """Нормирует через log10 в диапазон [0, 1].

    log10(1) = 0  → 0.0
    log10(10^target_log) = target_log → 1.0
    Выше target_log клампится в 1.0.
    """
    if value <= 0:
        return 0.0
    val = math.log10(value)
    if val <= 0:
        return 0.0
    return min(val / target_log, 1.0)


def compute_signal_score(west_row: dict | None = None,
                         ru_row: dict | None = None) -> float:
    """Считает signal_score 0–100 для одного ключевика.

    west_row: строка из Eventbrite-сборщика, dict с events_count.
              None если на западе ничего не нашли.
    ru_row:   строка из KudaGo (или Timepad когда оживёт), dict с events_count.
              None если в РФ ничего не нашли.
    """
    score = 0.0

    west_events = (west_row or {}).get("events_count", 0) or 0
    ru_events = (ru_row or {}).get("events_count", 0) or 0

    # Если на западе вообще ничего — это не наш тренд (мы ловим именно
    # западные сигналы и проверяем, пришли ли в РФ).
    if west_events < WEST_MIN_EVENTS:
        # Но если RU-сторона что-то нашла — оставим минимальный балл,
        # потому что данные есть, пусть и однобокие.
        if ru_events > 0:
            return round(_log_norm(ru_events, RU_EVENTS_NORM_LOG) * 10, 1)
        return 0.0

    # ── 1. Масштаб на западе (log10) ────────────────────────────────
    west_ratio = _log_norm(west_events, WEST_EVENTS_NORM_LOG)
    score += west_ratio * W_WEST_SCALE

    # ── 2. Пустота в РФ ─────────────────────────────────────────────
    # Сколько РФ-событий нормируем тем же образом, но потом инвертируем:
    # ru_ratio = 0 (пусто) → 1.0 балла по этой оси
    # ru_ratio = 1 (полно) → 0.0 балла
    ru_ratio = _log_norm(ru_events, RU_EVENTS_NORM_LOG)
    emptiness = 1.0 - ru_ratio
    score += emptiness * W_RU_EMPTINESS

    # ── 3. Окно возможности ─────────────────────────────────────────
    # Горячо на западе × пусто в РФ. Произведение, чтобы оба условия
    # должны выполняться одновременно. Это «суть» нашего сигнала.
    opportunity = west_ratio * emptiness
    score += opportunity * W_OPPORTUNITY

    # ── 4. Бонус за кросс-валидацию ─────────────────────────────────
    # Если тренд виден на обеих платформах — данные надёжнее.
    # Если только на западе — даём полный бонус (видим, но в РФ ещё нет).
    if west_events > 0 and ru_events > 0:
        score += W_DIVERSITY * 0.5
    elif west_events > 0 and ru_events == 0:
        score += W_DIVERSITY

    return round(min(score, 100.0), 1)


def merge_west_and_ru(west_rows: list[dict],
                      ru_rows: list[dict]) -> list[dict]:
    """Сводит данные Eventbrite и KudaGo/Timepad по категориям.

    Маппинг EN↔RU идёт через category (одинаковый в обоих словарях
    keywords.py: EN_KEYWORDS и RU_KEYWORDS).

    Возвращает список строк с signal_score, отсортированный по убыванию.
    """
    # Группируем RU по категории, выбирая лучший по числу событий
    ru_by_cat: dict[str, dict] = {}
    for row in ru_rows:
        cat = row.get("category")
        if not cat:
            continue
        cur = ru_by_cat.get(cat)
        if cur is None or row.get("events_count", 0) > cur.get("events_count", 0):
            ru_by_cat[cat] = row

    results = []

    # 1. Все западные строки — основной поток
    for row in west_rows:
        cat = row.get("category")
        best_ru = ru_by_cat.get(cat)

        score = compute_signal_score(west_row=row, ru_row=best_ru)
        row["signal_score"] = score

        # Прикладываем RU-контекст для отладки и AI-слоя
        if best_ru:
            row["ru_events_count"] = best_ru.get("events_count", 0)
            row["ru_avg_price"] = best_ru.get("avg_price")
            row["ru_keyword"] = best_ru.get("keyword", "")
        else:
            row["ru_events_count"] = 0
            row["ru_avg_price"] = None
            row["ru_keyword"] = ""

        results.append(row)

    # 2. RU-ключи без западного аналога — пишем, но с минимальным score
    west_cats_covered = {row.get("category") for row in west_rows}
    for row in ru_rows:
        if row.get("category") not in west_cats_covered:
            score = compute_signal_score(west_row=None, ru_row=row)
            row["signal_score"] = score
            row["ru_events_count"] = row.get("events_count", 0)
            row["ru_avg_price"] = row.get("avg_price")
            row["ru_keyword"] = row.get("keyword", "")
            results.append(row)

    results.sort(key=lambda r: r.get("signal_score", 0), reverse=True)
    return results


if __name__ == "__main__":
    # Тест на синтетических данных, которые соответствуют новому
    # масштабу events_count (object_count из Eventbrite).
    test_cases = [
        # имя, west_events, ru_events, что ожидаем увидеть
        ("свечи (массовая, в РФ есть)", 4775, 80,
         "high west × medium RU → средний score"),
        ("tufting (горячо, в РФ почти нет)", 1200, 5,
         "high west × empty RU → ВЫСОКИЙ score"),
        ("paint & sip (массово, в РФ массово)", 3500, 200,
         "high west × high RU → средний score, окно закрыто"),
        ("punch needle (нишево, в РФ нет)", 80, 0,
         "moderate west × empty RU → высокий по emptiness, средний total"),
        ("sound bath (нишево)", 250, 10,
         "moderate × low RU → средний"),
        ("отсутствует на западе", 5, 30,
         "ниже WEST_MIN_EVENTS → только RU-сигнал"),
        ("ноль везде", 0, 0,
         "0"),
    ]

    print(f"{'Тема':<40} {'West':>6} {'RU':>6} {'Score':>7}")
    print("─" * 70)
    for name, w, r, comment in test_cases:
        west = {"events_count": w}
        ru = {"events_count": r} if r > 0 else None
        s = compute_signal_score(west, ru)
        print(f"{name:<40} {w:>6} {r:>6} {s:>7}    # {comment}")
