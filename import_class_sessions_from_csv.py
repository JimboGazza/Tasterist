#!/usr/bin/env python3
"""
Import class sessions from a bookings CSV into class_sessions.

Supports both legacy and current CSV column names:
- Name/Event Name
- date/Date
- Start/Start Time
- End/End Time
- Address/Location (optional)
"""

import argparse
import csv
import sqlite3

import pandas as pd


def pick_column(columns, candidates):
    col_map = {c.strip().lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in col_map:
            return col_map[candidate.lower()]
    return None


def infer_programme(class_name, address):
    text = f"{class_name} {address}".lower()

    if "honley" in text:
        return "honley", "Honley"

    if any(
        token in text for token in (
            "mini roos", "jumping joeys", "kangaroo kids",
            "preschool", "pre-school"
        )
    ):
        return "preschool", "Preschool"

    if "lockwood" in text:
        return "lockwood", "Lockwood"

    return "lockwood", "Lockwood"


def detect_header_row(csv_path):
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for idx, row in enumerate(reader):
            lowered = {cell.strip().lower() for cell in row if cell and cell.strip()}
            has_name = "name" in lowered or "event name" in lowered
            has_date = "date" in lowered
            has_start = "start" in lowered or "start time" in lowered
            has_end = "end" in lowered or "end time" in lowered
            if has_name and has_date and has_start and has_end:
                return idx
            if idx > 30:
                break
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        default="Events_from_19_Jan_2026_to_25_Jan_2026-2.csv",
        help="Path to bookings CSV",
    )
    parser.add_argument(
        "--db",
        default="tasterist.db",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Clear class_sessions table before import",
    )
    args = parser.parse_args()

    print(f"Reading CSV: {args.csv}")
    header_row = detect_header_row(args.csv)
    df = pd.read_csv(args.csv, skiprows=header_row)
    print(f"Loaded {len(df)} rows")

    name_col = pick_column(df.columns, ["Name", "Event Name"])
    date_col = pick_column(df.columns, ["date", "Date"])
    start_col = pick_column(df.columns, ["Start", "Start Time"])
    end_col = pick_column(df.columns, ["End", "End Time"])
    addr_col = pick_column(df.columns, ["Address", "Location"])

    required = [("name", name_col), ("date", date_col), ("start", start_col), ("end", end_col)]
    missing = [label for label, col in required if not col]
    if missing:
        raise SystemExit(f"Missing required CSV columns: {', '.join(missing)}")

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS class_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            programme TEXT NOT NULL,
            location TEXT NOT NULL,
            session_date TEXT NOT NULL DEFAULT '',
            day TEXT NOT NULL,
            class_name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL DEFAULT '',
            source_file TEXT DEFAULT ''
        )
    """)
    class_cols = {row[1] for row in cur.execute("PRAGMA table_info(class_sessions)")}
    if "session_date" not in class_cols:
        cur.execute(
            "ALTER TABLE class_sessions ADD COLUMN session_date TEXT NOT NULL DEFAULT ''"
        )

    cur.execute("DROP INDEX IF EXISTS uniq_class_session")
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_class_session
        ON class_sessions (
            programme, session_date, day,
            class_name, start_time, end_time
        )
    """)

    if args.replace:
        print("Clearing existing class_sessions...")
        cur.execute("DELETE FROM class_sessions")

    inserted = 0
    for _, row in df.iterrows():
        class_name = str(row.get(name_col, "")).strip()
        if not class_name or class_name.lower() == "nan":
            continue

        date_raw = str(row.get(date_col, "")).strip()
        start_time = str(row.get(start_col, "")).strip()
        end_time = str(row.get(end_col, "")).strip()
        address = str(row.get(addr_col, "")).strip() if addr_col else ""

        if not date_raw or not start_time:
            continue

        parsed_date = pd.to_datetime(date_raw, errors="coerce")
        if pd.isna(parsed_date):
            continue

        day_name = parsed_date.strftime("%A")
        session_date = parsed_date.strftime("%Y-%m-%d")
        programme, location = infer_programme(class_name, address)

        cur.execute("""
            INSERT OR IGNORE INTO class_sessions (
                programme, location, session_date, day,
                class_name, start_time, end_time, source_file
            )
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            programme,
            location,
            session_date,
            day_name,
            class_name,
            start_time,
            end_time if end_time and end_time.lower() != "nan" else "",
            args.csv,
        ))

        if cur.rowcount == 1:
            inserted += 1

    conn.commit()

    print(f"Inserted {inserted} class sessions")
    print("By programme/day:")
    for row in cur.execute("""
        SELECT programme, day, COUNT(*)
        FROM class_sessions
        GROUP BY programme, day
        ORDER BY programme, day
    """):
        print(f"  {row[0]:9} {row[1]:10} {row[2]}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
