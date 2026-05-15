"""
SQLite-модуль для Trend Radar.
Одна БД trends.db, две таблицы:
  - signals: еженедельные сигналы (ключевик × источник × дата)
  - decisions: решения команды (взять в работу, статус, выручка)
"""

import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime

log = logging.getLogger("trend_radar.db")

DB_PATH = Path(__file__).parent / "data" / "trends.db"


def get_connection() -> sqlite3.Connection:
    """Создаёт/открывает БД и возвращает соединение."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Создаёт таблицы, если их нет."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,       -- дата прогона (YYYY-MM-DD)
            keyword TEXT NOT NULL,
            category TEXT NOT NULL,
            source TEXT NOT NULL,             -- 'eventbrite' | 'timepad' | 'pinterest' | 'tiktok'
            events_count INTEGER DEFAULT 0,
            avg_price REAL,
            currency TEXT,
            top_city TEXT,
            sample_titles TEXT,              -- JSON-массив строк
            signal_score REAL,
            -- AI-оценка (заполняется на Волне 2)
            ai_materials INTEGER,
            ai_readability INTEGER,
            ai_occasion INTEGER,
            ai_reels_factor INTEGER,
            ai_launch_speed INTEGER,
            ai_market_gap INTEGER,
            ai_summary TEXT,
            UNIQUE(collected_at, keyword, source)
        );

        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER REFERENCES signals(id),
            status TEXT DEFAULT 'new',        -- 'new' | 'in_progress' | 'launched' | 'rejected'
            assigned_to TEXT,
            deadline TEXT,
            launched_at TEXT,
            revenue REAL,
            beat_competitors INTEGER,          -- 1=да, 0=нет, NULL=не знаю
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_signals_date
            ON signals(collected_at);
        CREATE INDEX IF NOT EXISTS idx_signals_keyword
            ON signals(keyword);
        CREATE INDEX IF NOT EXISTS idx_signals_score
            ON signals(signal_score DESC);
    """)
    conn.commit()
    conn.close()
    log.info(f"БД инициализирована: {DB_PATH}")


def insert_signals(rows: list[dict], collected_at: str = None):
    """
    Записывает строки сигналов в БД.
    При дубликате (collected_at + keyword + source) — обновляет.
    """
    if not rows:
        return

    if collected_at is None:
        collected_at = datetime.now().strftime("%Y-%m-%d")

    conn = get_connection()
    inserted = 0
    updated = 0

    for row in rows:
        try:
            conn.execute("""
                INSERT INTO signals (
                    collected_at, keyword, category, source,
                    events_count, avg_price, currency, top_city,
                    sample_titles, signal_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(collected_at, keyword, source) DO UPDATE SET
                    events_count = excluded.events_count,
                    avg_price = excluded.avg_price,
                    currency = excluded.currency,
                    top_city = excluded.top_city,
                    sample_titles = excluded.sample_titles,
                    signal_score = excluded.signal_score
            """, (
                collected_at,
                row["keyword"],
                row["category"],
                row["source"],
                row.get("events_count", 0),
                row.get("avg_price"),
                row.get("currency"),
                row.get("top_city"),
                row.get("sample_titles", "[]"),
                row.get("signal_score"),
            ))
            inserted += 1
        except sqlite3.Error as e:
            log.error(f"  Ошибка записи '{row.get('keyword')}': {e}")
            updated += 1

    conn.commit()
    conn.close()
    log.info(f"Записано в БД: {inserted} строк")


def get_latest_signals(limit: int = 50) -> list[dict]:
    """Возвращает сигналы последнего прогона, отсортированные по signal_score."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT * FROM signals
        WHERE collected_at = (SELECT MAX(collected_at) FROM signals)
        ORDER BY signal_score DESC
        LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_keyword_history(keyword: str, weeks: int = 12) -> list[dict]:
    """Возвращает историю signal_score для ключевика за N недель."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT collected_at, source, events_count, avg_price, signal_score
        FROM signals
        WHERE keyword = ?
        ORDER BY collected_at DESC
        LIMIT ?
    """, (keyword, weeks * 2))  # *2 потому что два источника на ключевик
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def export_latest_csv(output_path: str) -> int:
    """Экспортирует последний прогон в CSV. Возвращает число строк."""
    import csv

    rows = get_latest_signals(limit=500)
    if not rows:
        return 0

    fields = [
        "collected_at", "keyword", "category", "source",
        "events_count", "avg_price", "currency", "top_city",
        "signal_score", "sample_titles",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


if __name__ == "__main__":
    init_db()
    print(f"БД создана: {DB_PATH}")
