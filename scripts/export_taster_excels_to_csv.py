#!/usr/bin/env python3
"""
export_taster_excels_to_csv.py

Exports all month sheets from Pennine Gymnastics taster Excel files
into clean, predictable CSV files.

Why this exists:
- Excel layouts are human-friendly but parser-hostile
- CSV gives us a stable, debuggable intermediate format
- Importers should ONLY read CSV, never Excel

Output format:
<Programme>__<Month>.csv
"""

import argparse
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

MONTHS = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]

def normalise_programme(filename: str) -> str:
    f = filename.lower()
    if "preschool" in f or "pre-school" in f:
        return "preschool"
    if "honley" in f:
        return "honley"
    return "lockwood"

def export_workbook(xlsx_path: Path, out_dir: Path):
    programme = normalise_programme(xlsx_path.name)
    print(f"\n📘 Reading workbook: {xlsx_path.name}")
    print(f"Programme detected: {programme}")

    try:
        xls = pd.ExcelFile(xlsx_path)
    except Exception as e:
        print(f"❌ Failed to open {xlsx_path.name}: {e}")
        return

    for sheet in xls.sheet_names:
        if sheet not in MONTHS:
            continue

        print(f"  ↳ Exporting sheet: {sheet}")

        try:
            df = pd.read_excel(
                xlsx_path,
                sheet_name=sheet,
                header=None,
                dtype=str  # preserve raw text
            )
        except Exception as e:
            print(f"    ❌ Failed to read sheet {sheet}: {e}")
            continue

        out_name = f"{programme}__{sheet}.csv"
        out_path = out_dir / out_name

        df.to_csv(out_path, index=False, header=False)
        print(f"    ✅ Wrote {out_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Folder containing .xlsx taster sheets")
    parser.add_argument(
        "--output",
        default=str(BASE_DIR / "data" / "exports"),
        help="Output folder for CSV files",
    )
    args = parser.parse_args()

    in_dir = Path(args.input)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    xlsx_files = list(in_dir.glob("*.xlsx"))
    if not xlsx_files:
        print("❌ No .xlsx files found")
        return

    for xlsx in xlsx_files:
        export_workbook(xlsx, out_dir)

    print("\n🎉 CSV export complete")

if __name__ == "__main__":
    main()
