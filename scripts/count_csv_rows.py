#!/usr/bin/env python3
"""
CSV sanity checker
Counts likely taster records in exported CSVs
NO database writes
"""

from pathlib import Path
import csv
import re

BASE_DIR = Path(__file__).resolve().parents[1]


def looks_like_name(value: str) -> bool:
    # Broad filter: starts with a letter and is not a common header token.
    if not value:
        return False
    s = value.strip()
    if not s:
        return False
    if s.lower() in {
        "name", "tasters", "leavers",
        "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday"
    }:
        return False
    if re.match(r"^\d{1,2}:\d{2}", s):
        return False
    return bool(re.match(r"^[A-Za-z]", s))


def count_csv(path: Path) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if not rows:
        return 0

    header_row_idx = None
    name_cols = []
    for i, row in enumerate(rows[:20]):
        cols = [
            idx for idx, cell in enumerate(row)
            if isinstance(cell, str) and cell.strip().lower() == "name"
        ]
        if cols:
            header_row_idx = i
            name_cols = cols
            break

    if header_row_idx is None:
        return 0

    count = 0
    for row in rows[header_row_idx + 1:]:
        if any((cell or "").strip().lower() == "leavers" for cell in row):
            break

        for col in name_cols:
            name = row[col].strip() if col < len(row) and row[col] else ""
            date_val = row[col + 1].strip() if col + 1 < len(row) and row[col + 1] else ""
            if looks_like_name(name) and date_val:
                count += 1

    return count


def main():
    root = (BASE_DIR / "data" / "exports").resolve()

    if not root.exists():
        raise SystemExit(f"Folder not found: {root}")

    total = 0

    print(f"\n📂 Scanning CSVs in:")
    print(f"   {root}\n")

    for csv_file in sorted(root.rglob("*.csv")):
        try:
            n = count_csv(csv_file)
            print(f"📄 {csv_file.name:30} → {n}")
            total += n
        except OSError as e:
            print(f"⚠️ {csv_file.name:30} → skipped ({e})")

    print("\n==========================")
    print(f"✅ TOTAL CSV TASTER CANDIDATES: {total}")
    print("==========================\n")


if __name__ == "__main__":
    main()
