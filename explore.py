"""
Step 1: Explore the raw Olist CSV files.

Run this BEFORE cleaning, so you know what is actually messy.
Write down what you find: these notes go in your README.

Usage:  python explore.py
"""
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw")

csv_files = sorted(RAW_DIR.glob("*.csv"))
if not csv_files:
    raise SystemExit(f"No CSV files found in {RAW_DIR}/. Put the Olist files there first.")

for path in csv_files:
    df = pd.read_csv(path)
    print("=" * 70)
    print(path.name)
    print(f"rows: {len(df):,}   columns: {df.shape[1]}")
    print(f"fully duplicated rows: {df.duplicated().sum():,}")

    nulls = df.isna().sum()
    nulls = nulls[nulls > 0]
    if nulls.empty:
        print("missing values: none")
    else:
        print("missing values:")
        for col, count in nulls.items():
            print(f"  {col}: {count:,} ({count / len(df):.1%})")

    print("first rows:")
    print(df.head(3).to_string())
    print()
