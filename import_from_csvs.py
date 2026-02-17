#!/usr/bin/env python3
"""
Tasterist — CSV → SQLite importer
Authoritative import from known-good CSVs

• No Excel
• No heuristics
• Idempotent
• Boring (on purpose)
"""

import sqlite3
import argparse
from pathlib import Path
import csv


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

CSV_COLUMNS = [
    "child",
    "programme",
    "session",
    "taster_date",
    "attended",
    "fees",
    "bg",
    "badge",
    "notes",
]

DB_COLUMNS = [
    "child",
    "programme",
    "location",
    "session",
    "taster_date",
    "attended",
    "bg",
    "badge",
    "notes",
]


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def infer_location(programme: str) -> str:
    if programme == "honley":
        return "Honley"
    return "Lockwood"


def truthy(v):
    if v is None:
        return 0
    s = str(v).strip().lower()
    return 1 if s in ("yes", "y", "1", "true", "paid", "done", "✓") else 0


# --------------------------------------------------
# IMPORTER
# --------------------------------------------------

def import_csv(path: Path, conn: sqlite3.Connection) -> int:
    inserted = 0
    cur = conn.cursor()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        missing = set(CSV_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(
                f"{path.name} missing columns: {', '.join(missing)}"
            )

        for row in reader:
            child = row["child"].strip()
            programme = row["programme"].strip()
            session = row["session"].strip()
            taster_date = row["taster_date"].strip()

            if not child or not programme or not taster_date:
                continue

            cur.execute(
                """
                INSERT OR IGNORE INTO tasters (
                    child,
                    programme,
                    location,
                    session,
                    taster_date,
                    attended,
                    bg,
                    badge,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    child,
                    programme,
                    infer_location(programme),
                    session,
                    taster_date,
                    truthy(row.get("attended")),
                    truthy(row.get("bg")),
                    truthy(row.get("badge")),
                    row.get("notes", "").strip(),
                ),
            )

            if cur.rowcount == 1:
                inserted += 1

    return inserted


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--folder", required=True, help="Folder containing CSV files")
    p.add_argument("--db", default="tasterist.db")
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()

    root = Path(args.folder).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"❌ Folder not found: {root}")

    conn = sqlite3.connect(args.db)

    if args.apply:
        print("\n🔥 Clearing tasters table")
        conn.execute("DELETE FROM tasters")
        conn.commit()

    total = 0

    print(f"\n📂 Importing CSVs from:")
    print(f"   {root}\n")

    for csv_file in sorted(root.rglob("*.csv")):
        print(f"📄 {csv_file.name}")
        n = import_csv(csv_file, conn)
        print(f"   ✔ Inserted: {n}")
        total += n

    conn.commit()
    conn.close()

    print("\n🎉 CSV IMPORT COMPLETE")
    print(f"   ✔ Total tasters inserted: {total}")


if __name__ == "__main__":
    main()
