#!/usr/bin/env python3
"""
Apply the manually provided weekly class schedule for Honley/Lockwood.

This replaces existing Honley/Lockwood rows in class_sessions with a clean
weekly template (session_date='') so Add/Leaver screens show the intended slots.
"""

from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app


SCHEDULE = {
    "Monday": [
        ("Pennine Gymnastics Honley - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Honley - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:30", "20:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "19:00", "20:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "19:30", "21:00"),
    ],
    "Tuesday": [
        ("Pennine Gymnastics Lockwood - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Honley - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Lockwood | Parkour", "17:00", "18:30"),
        ("Pennine Gymnastics Lockwood | Parkour", "17:30", "19:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Lockwood | Parkour", "18:00", "19:30"),
        ("Pennine Gymnastics Lockwood | Parkour", "18:30", "20:00"),
        ("Display Junior Honley", "18:45", "20:15"),
        ("Pennine Gymnastics Lockwood | Parkour", "19:00", "20:30"),
        ("Display Senior Honley", "19:30", "21:00"),
        ("Pennine Gymnastics Lockwood | Parkour", "19:30", "21:00"),
    ],
    "Wednesday": [
        ("Pennine Gymnastics Honley - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Lockwood - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Honley - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Honley | Parkour", "17:00", "18:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Honley | Parkour", "17:30", "19:00"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Honley | Parkour", "18:00", "19:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "18:30", "20:00"),
        ("Pennine Gymnastics Honley | Parkour", "18:30", "20:00"),
        ("Pennine Gymnastics Honley | Parkour", "19:00", "20:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "19:00", "20:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "19:30", "21:00"),
        ("Pennine Gymnastics Honley | Parkour", "19:30", "21:00"),
    ],
    "Thursday": [
        ("Pennine Gymnastics Honley - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Lockwood - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:30", "20:00"),
        ("Display Junior Lockwood", "18:45", "20:15"),
        ("Pennine Gymnastics Honley - 1.5hr", "19:00", "20:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "19:30", "21:00"),
        ("Display Senior Lockwood", "19:30", "21:00"),
    ],
    "Friday": [
        ("Pennine Gymnastics Honley - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Lockwood - 45min", "16:00", "16:45"),
        ("Pennine Gymnastics Honley - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:15", "17:15"),
        ("Pennine Gymnastics Honley - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Lockwood - 1hr", "16:45", "17:45"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:00", "18:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "17:30", "19:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "18:00", "19:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "18:30", "20:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "18:30", "20:00"),
        ("Advanced Junior Honley", "19:00", "20:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "19:00", "20:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "19:30", "21:00"),
        ("Advanced Senior Honley", "19:30", "21:00"),
    ],
    "Saturday": [
        ("Pennine Gymnastics Honley - 45min", "09:00", "09:45"),
        ("Pennine Gymnastics Lockwood - 45min", "09:00", "09:45"),
        ("Pennine Gymnastics Honley - 1hr", "09:15", "10:15"),
        ("Pennine Gymnastics Lockwood - 1hr", "09:15", "10:15"),
        ("Pennine Gymnastics Honley - 1hr", "09:45", "10:45"),
        ("Pennine Gymnastics Lockwood - 1hr", "09:45", "10:45"),
        ("Pennine Gymnastics Honley - 1.5hr", "10:00", "11:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "10:00", "11:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "10:30", "12:00"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "10:30", "12:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "11:00", "12:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "11:00", "12:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "11:30", "13:00"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "11:30", "13:00"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "12:00", "13:30"),
        ("Pennine Gymnastics Honley - 1.5hr", "12:00", "13:30"),
        ("Pennine Gymnastics Lockwood - 1.5hr", "12:30", "14:00"),
        ("Pennine Gymnastics Honley - 1.5hr", "12:30", "14:00"),
        ("Advanced Junior Lockwood", "13:00", "14:30"),
        ("Pennine Gymnastics Honley | Parkour", "13:30", "15:00"),
        ("Advanced Senior Lockwood", "13:30", "15:00"),
        ("Pennine Gymnastics Honley | Parkour", "14:00", "15:30"),
        ("Pennine Gymnastics Honley | Parkour", "14:30", "16:00"),
        ("Pennine Gymnastics Lockwood | Additional Needs", "15:15", "16:00"),
    ],
}


def programme_for_class(class_name: str) -> str:
    lower = class_name.lower()
    if "lockwood" in lower:
        return "lockwood"
    if "honley" in lower:
        return "honley"
    raise ValueError(f"Could not infer programme from class name: {class_name}")


def location_for_programme(programme: str) -> str:
    return "Lockwood" if programme == "lockwood" else "Honley"


def hhmmss(value: str) -> str:
    text = value.strip()
    if len(text) == 5:
        return f"{text}:00"
    return text


def main():
    conn = app.open_db_connection()
    cur = conn.cursor()

    before = int(cur.execute(
        "SELECT COUNT(*) FROM class_sessions WHERE lower(programme) IN ('lockwood','honley')"
    ).fetchone()[0] or 0)

    cur.execute("DELETE FROM class_sessions WHERE lower(programme) IN ('lockwood','honley')")

    inserted = 0
    per_day = Counter()
    per_programme = Counter()
    for day_name, entries in SCHEDULE.items():
        for class_name, start_time, end_time in entries:
            programme = programme_for_class(class_name)
            location = location_for_programme(programme)
            cur.execute(
                """
                INSERT INTO class_sessions
                (programme, location, session_date, day, class_name, start_time, end_time, source_file)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    programme,
                    location,
                    "",
                    day_name,
                    class_name,
                    hhmmss(start_time),
                    hhmmss(end_time),
                    "manual_schedule_2026_02_19",
                ),
            )
            inserted += 1
            per_day[day_name] += 1
            per_programme[programme] += 1

    conn.commit()

    after = int(cur.execute(
        "SELECT COUNT(*) FROM class_sessions WHERE lower(programme) IN ('lockwood','honley')"
    ).fetchone()[0] or 0)
    conn.close()

    print(f"Backend: {app.DB_BACKEND}")
    print(f"Removed old Honley/Lockwood rows: {before}")
    print(f"Inserted new rows: {inserted}")
    print(f"Current Honley/Lockwood rows: {after}")
    print("Rows by day:", dict(sorted(per_day.items())))
    print("Rows by programme:", dict(sorted(per_programme.items())))


if __name__ == "__main__":
    main()
