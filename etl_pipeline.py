"""
Olist ETL pipeline

Extract   read the raw Olist CSV files from data/raw/
Transform clean every table, then join them into one analysis-ready table (sales_fact)
Load      write everything into a SQLite database and write a data-quality report

Usage:  python etl_pipeline.py

Outputs:
  output/olist.db                  SQLite database with the cleaned tables + sales_fact
  output/data_quality_report.md    what was found, what was fixed, and the validation results
"""
import sqlite3
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw")
OUT_DIR = Path("output")
DB_PATH = OUT_DIR / "olist.db"
REPORT_PATH = OUT_DIR / "data_quality_report.md"

FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

ORDER_DATE_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

issues = []      # (table, what was found / fixed, number of rows or values affected)
row_counts = {}  # table -> [rows before cleaning, rows after cleaning]


def log(table, message, count):
    """Record a data-quality finding and print it."""
    issues.append((table, message, int(count)))
    print(f"[{table}] {message}: {int(count):,}")


# ---------------------------------------------------------------- EXTRACT
def extract():
    tables = {}
    for name, filename in FILES.items():
        path = RAW_DIR / filename
        if not path.exists():
            raise SystemExit(f"Missing file: {path}\nPut all Olist CSV files in {RAW_DIR}/")
        tables[name] = pd.read_csv(path)
        row_counts[name] = [len(tables[name]), None]
        print(f"Extracted {name}: {len(tables[name]):,} rows")
    return tables


# -------------------------------------------------------------- TRANSFORM
def drop_dupes(df, table, subset=None, what="duplicate rows"):
    before = len(df)
    df = df.drop_duplicates(subset=subset).copy()
    log(table, f"removed {what}", before - len(df))
    return df


def standardise(df, table, col, func):
    """Apply a text clean-up function to a column and log how many values changed."""
    before = df[col].fillna("")
    df[col] = func(df[col])
    changed = (before != df[col].fillna("")).sum()
    log(table, f"standardised text in '{col}'", changed)


def clean_customers(df):
    df = drop_dupes(df, "customers", subset=["customer_id"], what="duplicate customer_id rows")
    standardise(df, "customers", "customer_city", lambda s: s.str.strip().str.title())
    standardise(df, "customers", "customer_state", lambda s: s.str.strip().str.upper())
    return df


def clean_sellers(df):
    df = drop_dupes(df, "sellers", subset=["seller_id"], what="duplicate seller_id rows")
    standardise(df, "sellers", "seller_city", lambda s: s.str.strip().str.title())
    standardise(df, "sellers", "seller_state", lambda s: s.str.strip().str.upper())
    return df


def clean_geolocation(df):
    before = len(df)
    df = df.groupby("geolocation_zip_code_prefix", as_index=False).agg(
        geolocation_lat=("geolocation_lat", "mean"),
        geolocation_lng=("geolocation_lng", "mean"),
        geolocation_city=("geolocation_city", "first"),
        geolocation_state=("geolocation_state", "first"),
    )
    log("geolocation", "collapsed repeated zip-code rows into one row per zip prefix", before - len(df))
    standardise(df, "geolocation", "geolocation_city", lambda s: s.str.strip().str.title())
    standardise(df, "geolocation", "geolocation_state", lambda s: s.str.strip().str.upper())
    return df


def clean_orders(df):
    df = drop_dupes(df, "orders", subset=["order_id"], what="duplicate order_id rows")

    for col in ORDER_DATE_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    log("orders", "date columns converted from text to real dates", len(ORDER_DATE_COLS))

    standardise(df, "orders", "order_status", lambda s: s.str.strip().str.lower())

    missing_delivery = (
        (df["order_status"] == "delivered") & df["order_delivered_customer_date"].isna()
    ).sum()
    log("orders", "delivered orders with no delivery date (kept, date left empty)", missing_delivery)

    # Logic check: an order cannot arrive before it was placed.
    impossible = df["order_delivered_customer_date"] < df["order_purchase_timestamp"]
    log("orders", "delivery date earlier than purchase date (delivery date set to empty)", impossible.sum())
    df.loc[impossible, "order_delivered_customer_date"] = pd.NaT

    # Derived columns
    seconds_per_day = 86400
    df["delivery_days"] = (
        (df["order_delivered_customer_date"] - df["order_purchase_timestamp"]).dt.total_seconds()
        / seconds_per_day
    ).round(1)
    df["delay_days"] = (
        (df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]).dt.total_seconds()
        / seconds_per_day
    ).round(1)
    df["is_late"] = (df["delay_days"] > 0).astype("Int64").where(df["delay_days"].notna())
    df["purchase_month"] = df["order_purchase_timestamp"].dt.strftime("%Y-%m")
    return df


def clean_products(df, translation):
    df = df.rename(
        columns={
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        }
    )
    df = drop_dupes(df, "products", subset=["product_id"], what="duplicate product_id rows")

    log("products", "products with no category (labelled 'unknown')", df["product_category_name"].isna().sum())
    df["product_category_name"] = df["product_category_name"].fillna("unknown")

    df = df.merge(translation, on="product_category_name", how="left")
    untranslated = (
        df["product_category_name_english"].isna() & (df["product_category_name"] != "unknown")
    ).sum()
    log("products", "categories with no English translation (kept original name)", untranslated)
    df["product_category_name_english"] = df["product_category_name_english"].fillna(
        df["product_category_name"]
    )

    log("products", "products with missing weight (left empty)", df["product_weight_g"].isna().sum())
    return df


def clean_order_items(df):
    df = drop_dupes(df, "order_items", subset=["order_id", "order_item_id"], what="duplicate order items")
    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")

    bad_price = df["price"] <= 0
    log("order_items", "rows with zero or negative price (removed)", bad_price.sum())
    df = df[~bad_price].copy()

    df["item_total"] = (df["price"] + df["freight_value"]).round(2)
    return df


def clean_payments(df):
    df = drop_dupes(df, "payments")
    standardise(df, "payments", "payment_type", lambda s: s.str.strip().str.lower())

    undefined = df["payment_type"] == "not_defined"
    log("payments", "rows with payment type 'not_defined' (removed)", undefined.sum())
    df = df[~undefined].copy()

    no_value = df["payment_value"] <= 0
    log("payments", "rows with zero or negative payment value (removed)", no_value.sum())
    df = df[~no_value].copy()
    return df


def clean_reviews(df):
    df = drop_dupes(df, "reviews", subset=["review_id", "order_id"], what="duplicate reviews")
    for col in ["review_creation_date", "review_answer_timestamp"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    bad_score = ~df["review_score"].between(1, 5)
    log("reviews", "reviews with a score outside 1-5 (removed)", bad_score.sum())
    df = df[~bad_score].copy()
    return df


def build_sales_fact(t):
    """Join the cleaned tables into one analysis-ready table: one row per order item."""
    items = t["order_items"]

    # Main payment type per order = the type that paid the most.
    pay = t["payments"]
    top_idx = pay.groupby("order_id")["payment_value"].idxmax()
    main_payment = pay.loc[top_idx, ["order_id", "payment_type"]].rename(
        columns={"payment_type": "main_payment_type"}
    )

    avg_review = (
        t["reviews"]
        .groupby("order_id", as_index=False)["review_score"]
        .mean()
        .rename(columns={"review_score": "avg_review_score"})
    )

    fact = (
        items.merge(t["orders"], on="order_id", how="inner")
        .merge(t["customers"], on="customer_id", how="left")
        .merge(t["products"][["product_id", "product_category_name_english"]], on="product_id", how="left")
        .merge(t["sellers"][["seller_id", "seller_city", "seller_state"]], on="seller_id", how="left")
        .merge(main_payment, on="order_id", how="left")
        .merge(avg_review, on="order_id", how="left")
    )
    fact["avg_review_score"] = fact["avg_review_score"].round(2)

    log("sales_fact", "order items with no matching order (dropped by the join)", len(items) - len(fact))
    return fact


def validate(fact):
    """Simple data-quality checks on the final table."""
    checks = {
        "no empty order_id": bool(fact["order_id"].notna().all()),
        "no duplicate order items (order_id + order_item_id)": not fact.duplicated(
            ["order_id", "order_item_id"]
        ).any(),
        "all prices greater than 0": bool((fact["price"] > 0).all()),
        "every row has a purchase date": bool(fact["order_purchase_timestamp"].notna().all()),
        "every row has a customer state": bool(fact["customer_state"].notna().all()),
    }
    for name, ok in checks.items():
        print(f"[validation] {'PASS' if ok else 'FAIL'}: {name}")
    return checks


# ------------------------------------------------------------------- LOAD
def load(tables, fact):
    OUT_DIR.mkdir(exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        for name, df in tables.items():
            df.to_sql(name, conn, index=False)
        fact.to_sql("sales_fact", conn, index=False)

        conn.execute("CREATE INDEX idx_fact_order ON sales_fact(order_id)")
        conn.execute("CREATE INDEX idx_fact_month ON sales_fact(purchase_month)")
        conn.execute("CREATE INDEX idx_fact_category ON sales_fact(product_category_name_english)")
        conn.commit()
    finally:
        conn.close()
    print(f"Loaded {len(tables) + 1} tables into {DB_PATH}")


def write_report(checks):
    lines = [
        "# Data quality report",
        "",
        "Generated by `etl_pipeline.py`.",
        "",
        "## Row counts (before and after cleaning)",
        "",
        "| Table | Before | After |",
        "|---|---:|---:|",
    ]
    for table, (before, after) in row_counts.items():
        before_text = f"{before:,}" if before is not None else "-"
        after_text = f"{after:,}" if after is not None else "-"
        lines.append(f"| {table} | {before_text} | {after_text} |")

    lines += ["", "## Issues found and fixed", "", "| Table | What | Count |", "|---|---|---:|"]
    for table, message, count in issues:
        lines.append(f"| {table} | {message} | {count:,} |")

    lines += ["", "## Validation checks on sales_fact", "", "| Check | Result |", "|---|---|"]
    for name, ok in checks.items():
        lines.append(f"| {name} | {'PASS' if ok else 'FAIL'} |")

    OUT_DIR.mkdir(exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")


def main():
    raw = extract()

    cleaned = {
        "customers": clean_customers(raw["customers"]),
        "sellers": clean_sellers(raw["sellers"]),
        "geolocation": clean_geolocation(raw["geolocation"]),
        "orders": clean_orders(raw["orders"]),
        "products": clean_products(raw["products"], raw["category_translation"]),
        "order_items": clean_order_items(raw["order_items"]),
        "payments": clean_payments(raw["payments"]),
        "reviews": clean_reviews(raw["reviews"]),
    }
    for name, df in cleaned.items():
        row_counts[name][1] = len(df)
    row_counts["category_translation"][1] = len(raw["category_translation"])

    fact = build_sales_fact(cleaned)
    row_counts["sales_fact"] = [None, len(fact)]

    checks = validate(fact)
    load(cleaned, fact)
    write_report(checks)


if __name__ == "__main__":
    main()
