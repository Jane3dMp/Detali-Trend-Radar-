"""
Trend Radar v0.2 — главный оркестратор.

Запуск:
    python -m trend_radar.main              # полный прогон
    python -m trend_radar.main --test       # тестовый (3 ключевика, 1 город)
    python -m trend_radar.main --export     # только экспорт последнего прогона в CSV

Результаты:
    - SQLite: trend_radar/data/trends.db
    - CSV:    trend_radar/data/signals_YYYY-MM-DD.csv
    - Логи:   trend_radar/logs/run_YYYY-MM-DD.log
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

from trend_radar.keywords import (
    all_keywords_en_flat,
    all_keywords_ru_flat,
    CITIES_WEST,
    CITIES_RU,
)
from trend_radar.collectors import eventbrite, timepad
from trend_radar.scoring import merge_west_and_ru
from trend_radar import db

# ── Настройки ────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent
LOG_DIR = ROOT / "logs"
DATA_DIR = ROOT / "data"
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

TODAY = datetime.now().strftime("%Y-%m-%d")


def setup_logging():
    """Настройка логов: файл + консоль."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(
                LOG_DIR / f"run_{TODAY}.log", encoding="utf-8"
            ),
            logging.StreamHandler(),
        ],
    )


log = logging.getLogger("trend_radar.main")


def run_collection(test_mode: bool = False):
    """Основной пайплайн сбора данных."""

    log.info("=" * 60)
    log.info(f"Trend Radar v0.2 — старт, {TODAY}")
    log.info(f"Режим: {'ТЕСТ' if test_mode else 'ПОЛНЫЙ'}")
    log.info("=" * 60)

    # Инициализация БД
    db.init_db()

    # Подготовка ключевиков
    en_kws = all_keywords_en_flat()
    ru_kws = all_keywords_ru_flat()
    west_cities = CITIES_WEST
    ru_cities = CITIES_RU

    if test_mode:
        en_kws = en_kws[:3]
        ru_kws = ru_kws[:3]
        west_cities = CITIES_WEST[:1]  # только London
        ru_cities = CITIES_RU[:1]      # только Москва

    log.info(f"EN ключевиков: {len(en_kws)}")
    log.info(f"RU ключевиков: {len(ru_kws)}")
    log.info(f"Западные города: {[c['name'] for c in west_cities]}")
    log.info(f"Российские города: {[c.get('name_ru', c['name']) for c in ru_cities]}")

    # ── Блок 1: Eventbrite (London + Berlin) ─────────────────────────
    log.info("─── Блок 1: Eventbrite ───")
    west_rows = eventbrite.collect(en_kws, west_cities)
    log.info(f"Eventbrite: собрано {len(west_rows)} строк")

    # ── Блок 2: Timepad (Москва + СПб) ──────────────────────────────
    log.info("─── Блок 2: Timepad ───")
    ru_rows = timepad.collect(ru_kws, ru_cities)
    log.info(f"Timepad: собрано {len(ru_rows)} строк")

    # ── Блок 3: Сведение + signal_score ──────────────────────────────
    log.info("─── Блок 3: Scoring ───")
    merged = merge_west_and_ru(west_rows, ru_rows)
    log.info(f"После сведения: {len(merged)} строк")

    # ── Блок 4: Запись в SQLite ──────────────────────────────────────
    log.info("─── Блок 4: SQLite ───")
    db.insert_signals(merged, collected_at=TODAY)

    # ── Блок 5: Экспорт CSV ─────────────────────────────────────────
    csv_path = DATA_DIR / f"signals_{TODAY}.csv"
    count = db.export_latest_csv(str(csv_path))
    log.info(f"CSV экспортирован: {csv_path} ({count} строк)")

    # ── Блок 6: Отчёт ───────────────────────────────────────────────
    log.info("─── ТОП-10 ПО SIGNAL_SCORE ───")
    top = merged[:10]
    log.info(f"{'Keyword':<30} {'Cat':<20} {'Score':>6} {'West':>5} {'RU':>4} {'£':>7}")
    log.info("─" * 80)
    for row in top:
        log.info(
            f"{row['keyword']:<30} "
            f"{row['category']:<20} "
            f"{row.get('signal_score', 0):>6.1f} "
            f"{row.get('events_count', 0):>5} "
            f"{row.get('ru_events_count', 0):>4} "
            f"{row.get('avg_price', 0) or 0:>7.0f}"
        )

    log.info("=" * 60)
    log.info("Готово!")
    log.info("=" * 60)

    return merged


def export_only():
    """Только экспорт последнего прогона в CSV."""
    db.init_db()
    csv_path = DATA_DIR / f"signals_{TODAY}.csv"
    count = db.export_latest_csv(str(csv_path))
    print(f"Экспортировано: {csv_path} ({count} строк)")


def main():
    setup_logging()

    if "--export" in sys.argv:
        export_only()
    else:
        test = "--test" in sys.argv
        run_collection(test_mode=test)


if __name__ == "__main__":
    main()
