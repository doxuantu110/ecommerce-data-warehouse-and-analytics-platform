"""
generate_dim_date.py
--------------------
Sinh bảng dim_date bằng pandas và load vào PostgreSQL.

NOTE: create_dw.sql đã populate dim_date bằng GENERATE_SERIES khi
container khởi động. File này là bản Python backup — dùng khi:
  1. Cần extend range (thêm năm 2019+)
  2. Cần thêm column mới vào dim_date
  3. Chạy ETL mà không dùng Docker initdb
"""

import os
import logging

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
DATE_START = "2016-01-01"
DATE_END   = "2018-12-31"


# ── Generate ──────────────────────────────────────────────────────────────────
def generate_dim_date(start: str = DATE_START, end: str = DATE_END) -> pd.DataFrame:
    """
    Sinh DataFrame dim_date với dải ngày từ start đến end.

    Returns
    -------
    pd.DataFrame với các cột:
        date_key, full_date, year, quarter, month, month_name,
        week_of_year, day_of_month, day_of_week, day_name, is_weekend
    """
    dates = pd.date_range(start=start, end=end, freq="D")

    dim_date = pd.DataFrame({"full_date": dates})

    dim_date["date_key"]     = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"]         = dim_date["full_date"].dt.year
    dim_date["quarter"]      = dim_date["full_date"].dt.quarter
    dim_date["month"]        = dim_date["full_date"].dt.month
    dim_date["month_name"]   = dim_date["full_date"].dt.strftime("%B")   # January..December
    dim_date["week_of_year"] = dim_date["full_date"].dt.isocalendar().week.astype(int)
    dim_date["day_of_month"] = dim_date["full_date"].dt.day
    dim_date["day_of_week"]  = dim_date["full_date"].dt.isocalendar().day.astype(int)  # 1=Mon..7=Sun
    dim_date["day_name"]     = dim_date["full_date"].dt.strftime("%A")   # Monday..Sunday
    dim_date["is_weekend"]   = dim_date["day_of_week"].isin([6, 7])

    # Reorder columns để khớp với create_dw.sql
    dim_date = dim_date[[
        "date_key", "full_date", "year", "quarter", "month", "month_name",
        "week_of_year", "day_of_month", "day_of_week", "day_name", "is_weekend",
    ]]

    log.info(
        f"[dim_date] Generated {len(dim_date):,} rows  "
        f"({start} → {end})"
    )
    return dim_date


# ── Load ──────────────────────────────────────────────────────────────────────
def load_dim_date(dim_date: pd.DataFrame) -> None:
    """
    Load dim_date vào PostgreSQL.
    Dùng ON CONFLICT DO NOTHING — idempotent, chạy lại không lỗi.
    """
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB")

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

    with engine.begin() as conn:
        records = dim_date.to_dict(orient="records")

        sql = text("""
            INSERT INTO dw.dim_date (
                date_key, full_date, year, quarter, month, month_name,
                week_of_year, day_of_month, day_of_week, day_name, is_weekend
            )
            VALUES (
                :date_key, :full_date, :year, :quarter, :month, :month_name,
                :week_of_year, :day_of_month, :day_of_week, :day_name, :is_weekend
            )
            ON CONFLICT (date_key) DO NOTHING
        """)

        conn.execute(sql, records)
        log.info(f"[dim_date] Upserted {len(records):,} rows into dw.dim_date")

    log.info("[dim_date] Done.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    dim_date = generate_dim_date()

    # Preview
    print(dim_date.head(3).to_string(index=False))
    print("...")
    print(dim_date.tail(3).to_string(index=False))
    print(f"\nTotal rows: {len(dim_date):,}")

    # Load vào DB
    load_dim_date(dim_date)
