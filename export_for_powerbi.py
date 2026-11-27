"""
Export the analysis table (sales_fact) from the SQLite database to a CSV that Power BI can load.

Run this from the olist-etl-pipeline folder, AFTER etl_pipeline.py has created output/olist.db.

Usage:  python export_for_powerbi.py
Output: output/powerbi/sales_fact.csv
"""
import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path("output/olist.db")
OUT_DIR = Path("output/powerbi")

if not DB_PATH.exists():
    raise SystemExit(f"{DB_PATH} not found. Run etl_pipeline.py first.")

QUERY = """
SELECT
    order_id,
    order_item_id,
    order_status,
    DATE(order_purchase_timestamp)        AS purchase_date,
    purchase_month,
    price,
    freight_value,
    item_total,
    product_category_name_english         AS category,
    customer_state,
    customer_city,
    seller_state,
    main_payment_type,
    delivery_days,
    delay_days,
    is_late,
    avg_review_score
FROM sales_fact
"""

conn = sqlite3.connect(DB_PATH)
try:
    df = pd.read_sql_query(QUERY, conn)
finally:
    conn.close()

OUT_DIR.mkdir(parents=True, exist_ok=True)
out_path = OUT_DIR / "sales_fact.csv"
df.to_csv(out_path, index=False)

print(f"Wrote {len(df):,} rows and {df.shape[1]} columns to {out_path}")
print(f"File size: {out_path.stat().st_size / 1_000_000:.1f} MB")
print("Columns:", ", ".join(df.columns))
