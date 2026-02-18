#!/usr/bin/env python3
"""
Restore/sync Tasterist data from Postgres back into SQLite.

Usage:
  python scripts/restore_sqlite_from_postgres.py \
    --sqlite /var/data/tasterist.db \
    --postgres-url "$DATABASE_URL" \
    --truncate-first
"""

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

import psycopg


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SQLITE_PATH = BASE_DIR / "data" / "db" / "tasterist.db"

TABLE_ORDER = (
    "users",
    "class_sessions",
    "tasters",
    "leavers",
    "user_admin_days",
    "audit_logs",
)


def create_sqlite_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password_hash TEXT NOT NULL DEFAULT '',
            full_name TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'staff',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            password_must_change INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_admin_days (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            programme TEXT NOT NULL,
            UNIQUE(user_id, day_name, programme),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            user_id INTEGER,
            username TEXT NOT NULL DEFAULT 'system',
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL DEFAULT '',
            entity_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'ok',
            details TEXT NOT NULL DEFAULT ''
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS class_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tasters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS leavers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        """
    )
    conn.commit()


def sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def postgres_table_columns(pg, table_name: str) -> list[str]:
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_postgres_rows(pg, table_name: str, columns: Sequence[str]):
    with pg.cursor() as cur:
        col_list = ", ".join(columns)
        cur.execute(f"SELECT {col_list} FROM {table_name}")
        return cur.fetchall()


def truncate_sqlite(conn: sqlite3.Connection, table_names: Iterable[str]) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys=OFF")
    for name in table_names:
        cur.execute(f"DELETE FROM {name}")
    cur.execute("PRAGMA foreign_keys=ON")
    conn.commit()


def upsert_sqlite_rows(conn: sqlite3.Connection, table_name: str, columns: Sequence[str], rows) -> int:
    if not rows:
        return 0
    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(columns)
    if "id" in columns:
        updates = ", ".join([f"{c}=excluded.{c}" for c in columns if c != "id"])
        sql = (
            f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {updates}"
        )
    else:
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def sync_sqlite_sequence(conn: sqlite3.Connection, table_name: str) -> None:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    if not row:
        return
    max_id = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}").fetchone()[0]
    exists = conn.execute(
        "SELECT 1 FROM sqlite_sequence WHERE name=?", (table_name,)
    ).fetchone()
    if exists:
        conn.execute("UPDATE sqlite_sequence SET seq=? WHERE name=?", (max_id, table_name))
    else:
        conn.execute("INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)", (table_name, max_id))
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default=str(DEFAULT_SQLITE_PATH), help="Path to sqlite db file")
    parser.add_argument(
        "--postgres-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="Postgres connection URL (defaults to DATABASE_URL)",
    )
    parser.add_argument("--truncate-first", action="store_true", help="Clear destination tables before restore")
    args = parser.parse_args()

    if not args.postgres_url.strip():
        raise SystemExit("Missing --postgres-url and DATABASE_URL not set.")

    sqlite_path = Path(args.sqlite)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    sqlite_conn = sqlite3.connect(args.sqlite)
    pg_conn = psycopg.connect(args.postgres_url)

    try:
        create_sqlite_schema(sqlite_conn)

        if args.truncate_first:
            truncate_sqlite(sqlite_conn, reversed(TABLE_ORDER))

        total_rows = 0
        for table_name in TABLE_ORDER:
            src_cols = postgres_table_columns(pg_conn, table_name)
            dst_cols = sqlite_table_columns(sqlite_conn, table_name)
            cols = [c for c in src_cols if c in dst_cols]
            rows = fetch_postgres_rows(pg_conn, table_name, cols)
            written = upsert_sqlite_rows(sqlite_conn, table_name, cols, rows)
            sync_sqlite_sequence(sqlite_conn, table_name)
            total_rows += written
            print(f"{table_name}: {written} row(s)")

        print(f"\nRestore complete. Total rows synced: {total_rows}")
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
