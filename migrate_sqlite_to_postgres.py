#!/usr/bin/env python3
"""
Migrate/sync Tasterist data from SQLite to Postgres.

Usage:
  python migrate_sqlite_to_postgres.py --sqlite /var/data/tasterist.db --postgres-url "$DATABASE_URL" --truncate-first
"""

import argparse
import os
import sqlite3
from typing import Iterable, Sequence

import psycopg


TABLE_ORDER = (
    "users",
    "class_sessions",
    "tasters",
    "leavers",
    "user_admin_days",
    "audit_logs",
)


def create_schema(pg):
    with pg.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT 'staff',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                password_must_change INTEGER NOT NULL DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_admin_days (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                day_name TEXT NOT NULL,
                programme TEXT NOT NULL,
                UNIQUE(user_id, day_name, programme)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                user_id INTEGER,
                username TEXT NOT NULL DEFAULT 'system',
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'ok',
                details TEXT NOT NULL DEFAULT ''
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS class_sessions (
                id INTEGER PRIMARY KEY,
                programme TEXT NOT NULL,
                location TEXT NOT NULL,
                session_date TEXT NOT NULL DEFAULT '',
                day TEXT NOT NULL,
                class_name TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL DEFAULT '',
                source_file TEXT DEFAULT '',
                UNIQUE(programme, session_date, day, class_name, start_time, end_time)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tasters (
                id INTEGER PRIMARY KEY,
                child TEXT NOT NULL,
                programme TEXT NOT NULL,
                location TEXT NOT NULL,
                session TEXT NOT NULL,
                class_name TEXT NOT NULL DEFAULT '',
                taster_date TEXT NOT NULL,
                notes TEXT,
                attended INTEGER DEFAULT 0,
                club_fees INTEGER DEFAULT 0,
                bg INTEGER DEFAULT 0,
                badge INTEGER DEFAULT 0,
                reschedule_contacted INTEGER DEFAULT 0,
                UNIQUE(child, programme, taster_date, session)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leavers (
                id INTEGER PRIMARY KEY,
                child TEXT NOT NULL,
                programme TEXT NOT NULL,
                leave_month TEXT NOT NULL,
                leave_date TEXT DEFAULT '',
                class_day TEXT DEFAULT '',
                session TEXT DEFAULT '',
                class_name TEXT DEFAULT '',
                removed_la INTEGER DEFAULT 0,
                removed_bg INTEGER DEFAULT 0,
                added_to_board INTEGER DEFAULT 0,
                reason TEXT DEFAULT '',
                email TEXT DEFAULT '',
                source TEXT DEFAULT 'import',
                UNIQUE(child, programme, leave_month)
            )
        """)
    pg.commit()


def sqlite_table_columns(conn, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def postgres_table_columns(pg, table_name: str) -> list[str]:
    with pg.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
        """, (table_name,))
        return [r[0] for r in cur.fetchall()]


def fetch_sqlite_rows(conn, table_name: str, columns: Sequence[str]):
    conn.row_factory = sqlite3.Row
    sql = f"SELECT {', '.join(columns)} FROM {table_name}"
    return conn.execute(sql).fetchall()


def truncate_tables(pg, table_names: Iterable[str]):
    with pg.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE " + ", ".join(table_names) + " RESTART IDENTITY CASCADE"
        )
    pg.commit()


def upsert_rows(pg, table_name: str, columns: Sequence[str], rows):
    if not rows:
        return 0
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c != "id"])
    sql = f"""
        INSERT INTO {table_name} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {updates}
    """
    payload = [tuple(row[c] for c in columns) for row in rows]
    with pg.cursor() as cur:
        cur.executemany(sql, payload)
    pg.commit()
    return len(payload)


def sync_sequence(pg, table_name: str):
    with pg.cursor() as cur:
        cur.execute("""
            SELECT pg_get_serial_sequence(%s, 'id')
        """, (table_name,))
        seq_row = cur.fetchone()
        if not seq_row or not seq_row[0]:
            return
        seq_name = seq_row[0]
        cur.execute(f"SELECT COALESCE(MAX(id), 1) FROM {table_name}")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))
    pg.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default="tasterist.db", help="Path to sqlite db file")
    parser.add_argument(
        "--postgres-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="Postgres connection URL (defaults to DATABASE_URL)",
    )
    parser.add_argument("--truncate-first", action="store_true", help="Clear destination tables before sync")
    args = parser.parse_args()

    if not args.postgres_url.strip():
        raise SystemExit("Missing --postgres-url and DATABASE_URL not set.")

    sqlite_conn = sqlite3.connect(args.sqlite)
    pg_conn = psycopg.connect(args.postgres_url)

    try:
        create_schema(pg_conn)

        if args.truncate_first:
            truncate_tables(pg_conn, reversed(TABLE_ORDER))

        total_rows = 0
        for table_name in TABLE_ORDER:
            src_cols = sqlite_table_columns(sqlite_conn, table_name)
            dst_cols = postgres_table_columns(pg_conn, table_name)
            cols = [c for c in src_cols if c in dst_cols]
            rows = fetch_sqlite_rows(sqlite_conn, table_name, cols)
            written = upsert_rows(pg_conn, table_name, cols, rows)
            sync_sequence(pg_conn, table_name)
            total_rows += written
            print(f"{table_name}: {written} row(s)")

        print(f"\nMigration complete. Total rows synced: {total_rows}")
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()

