import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory


def initialize(database):
    database.executescript("""
        CREATE TABLE events (
            event_id TEXT PRIMARY KEY,
            sensor_id TEXT NOT NULL,
            event_time TEXT NOT NULL,
            temperature_c REAL NOT NULL,
            ingested_at TEXT NOT NULL
        );
        CREATE INDEX events_by_time ON events (event_time);
        CREATE TABLE dirty_days (
            day TEXT PRIMARY KEY
        );
        CREATE TABLE daily_reports (
            day TEXT PRIMARY KEY,
            readings INTEGER NOT NULL,
            hot_readings INTEGER NOT NULL,
            revision INTEGER NOT NULL
        );
    """)


def ingest(database, event):
    event_id, sensor_id, event_time, temperature, ingested_at = event
    with database:
        inserted = database.execute("""
            INSERT INTO events VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
        """, event).rowcount
        if inserted:
            database.execute("""
                INSERT INTO dirty_days VALUES (?)
                ON CONFLICT (day) DO NOTHING
            """, (event_time[:10],))
        else:
            stored = database.execute("""
                SELECT sensor_id, event_time, temperature_c
                FROM events WHERE event_id = ?
            """, (event_id,)).fetchone()
            if stored != (sensor_id, event_time, temperature):
                raise ValueError(f"Разные измерения с одинаковым event_id: {event_id}")
    status = "записано" if inserted else "повтор"
    print(f"{event_id}: {event_time} → приём {ingested_at} → {status}")
    return bool(inserted)


def iso_utc(timestamp):
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def aggregate(database, start, end):
    return database.execute("""
        SELECT COUNT(*),
               COUNT(CASE WHEN temperature_c > 8.0 THEN 1 END)
        FROM events
        WHERE event_time >= ? AND event_time < ?
    """, (iso_utc(start), iso_utc(end))).fetchone()


def refresh_day(database, day):
    start = datetime.fromisoformat(day)
    with database:
        database.execute("BEGIN IMMEDIATE")
        metrics = aggregate(database, start, start + timedelta(days=1))
        database.execute("""
            INSERT INTO daily_reports VALUES (?, ?, ?, 1)
            ON CONFLICT (day) DO UPDATE SET
                readings = excluded.readings,
                hot_readings = excluded.hot_readings,
                revision = daily_reports.revision + 1
            WHERE daily_reports.readings <> excluded.readings
               OR daily_reports.hot_readings <> excluded.hot_readings
        """, (day, *metrics))
        database.execute("DELETE FROM dirty_days WHERE day = ?", (day,))
        result = database.execute("""
            SELECT readings, hot_readings, revision FROM daily_reports WHERE day = ?
        """, (day,)).fetchone()
    print(f"Сутки {day}: измерений {result[0]}, выше 8 °C {result[1]}, версия {result[2]}")
    return result


def refresh_published_days(database):
    days = database.execute("""
        SELECT dirty_days.day FROM dirty_days JOIN daily_reports USING (day)
        ORDER BY dirty_days.day
    """).fetchall()
    return [refresh_day(database, day) for (day,) in days]


def require(actual, expected):
    if actual != expected:
        raise AssertionError(f"Ожидалось {expected!r}, получено {actual!r}")


def demonstrate(path):
    day = "2026-11-20"
    hot = ("e-102", "cam-02", f"{day}T23:58:00Z", 9.6, f"{day}T23:58:02Z")
    next_day = ("e-103", "cam-01", "2026-11-21T00:03:00Z", 4.1, "2026-11-21T00:03:02Z")
    late = ("e-104", "cam-02", f"{day}T23:59:00Z", 10.2, "2026-11-21T08:10:00Z")
    out_of_order = ("e-101", "cam-01", f"{day}T23:54:00Z", 3.2, f"{day}T23:58:14Z")
    duplicate = (hot[0], hot[1], hot[2], hot[3], f"{day}T23:58:40Z")

    with closing(sqlite3.connect(path)) as database:
        initialize(database)
        require(ingest(database, hot), True)
        require(ingest(database, out_of_order), True)
        require(ingest(database, duplicate), False)
        require(ingest(database, next_day), True)
        clock = datetime.fromisoformat("2026-11-21T00:05:00")
        live = aggregate(database, clock - timedelta(minutes=5), clock)
        require(live, (1, 0))
        print(f"Мониторинг за 5 минут на 00:05 UTC: {live[0]} измерение, выше 8 °C {live[1]}")
        require(refresh_day(database, day), (2, 1, 1))

    print("После переподключения к сохранённой базе:")
    with closing(sqlite3.connect(path)) as database:
        require(ingest(database, late), True)
        require(refresh_published_days(database), [(3, 2, 2)])
        require(ingest(database, late), False)
        require(refresh_published_days(database), [])
        require(refresh_day(database, day), (3, 2, 2))
        require(refresh_day(database, "2026-11-21"), (1, 0, 1))
        require(database.execute("SELECT COUNT(*) FROM events").fetchone()[0], 4)
    print("Все проверки пройдены.")


if __name__ == "__main__":
    with TemporaryDirectory() as directory:
        demonstrate(Path(directory) / "telemetry.db")
