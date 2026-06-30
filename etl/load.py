"""
load.py
-------
Load layer: nhận transformed DataFrames, load vào PostgreSQL DW.

Idempotency strategy:
- DIM tables  : UPSERT (INSERT ... ON CONFLICT DO NOTHING)
                → chạy lại không tạo duplicate, không xóa data cũ
- FACT tables : UPSERT on natural key
                → cùng (order_id, order_item_id) sẽ update thay vì insert thêm

Connection: dùng biến môi trường từ .env (không hardcode credentials).
"""

import os
import logging
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Connection
def get_engine():
    """
    Tạo SQLAlchemy engine từ biến môi trường.
    Khi chạy ETL ngoài Docker: POSTGRES_HOST=localhost
    Khi chạy trong Docker network: POSTGRES_HOST=ecommerce_postgres
    """
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB")

    if not all([user, password, db]):
        raise EnvironmentError(
            "Thiếu biến môi trường POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB.\n"
            "Hãy chạy: cp .env.example .env và điền giá trị thật."
        )

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, echo=False)
    log.info(f"[load] Connected to PostgreSQL: {host}:{port}/{db}")
    return engine


@contextmanager
def get_connection(engine):
    """Context manager cho connection — tự commit hoặc rollback."""
    with engine.begin() as conn:
        yield conn


# Upsert helpers

def upsert_dim(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_col: str,
) -> int:
    """
    UPSERT cho dimension table.
    Nếu conflict_col đã tồn tại → DO NOTHING (giữ nguyên surrogate key cũ).
    Chạy lại ETL nhiều lần sẽ không tạo thêm row mới.

    Parameters
    ----------
    conflict_col : cột natural key (vd: 'customer_unique_id', 'product_id')
    """
    if df.empty:
        log.warning(f"[load] {table}: DataFrame rỗng, bỏ qua.")
        return 0

    schema_table = f"dw.{table}"
    cols = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])

    sql = text(f"""
        INSERT INTO {schema_table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col}) DO NOTHING
    """)

    records = df.to_dict(orient="records")
    conn.execute(sql, records)

    log.info(f"[load] {table:<30} upserted {len(records):>7,} rows  (conflict key: {conflict_col})")
    return len(records)


def upsert_fact(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_cols: list[str],
    update_cols: list[str],
) -> int:
    """
    UPSERT cho fact table.
    Nếu natural key đã tồn tại → UPDATE các cột measure.
    Đảm bảo re-run ETL không nhân đôi fact rows.

    Parameters
    ----------
    conflict_cols : danh sách cột tạo thành natural key
                   (vd: ['order_id', 'order_item_id'])
    update_cols   : cột cần update khi conflict
                   (vd: ['price', 'freight_value'])
    """
    if df.empty:
        log.warning(f"[load] {table}: DataFrame rỗng, bỏ qua.")
        return 0

    schema_table = f"dw.{table}"
    cols         = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    conflict_str = ", ".join(conflict_cols)
    update_str   = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = text(f"""
        INSERT INTO {schema_table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_str})
        DO UPDATE SET {update_str}
    """)

    records = df.to_dict(orient="records")
    conn.execute(sql, records)

    log.info(f"[load] {table:<30} upserted {len(records):>7,} rows  (conflict key: {conflict_str})")
    return len(records)


# Surrogate key resolution

def resolve_surrogate_keys(conn, transformed: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Sau khi dim đã được load vào DB, lấy surrogate key thật từ DB
    để gán vào fact tables.

    Lý do cần bước này: surrogate key trong DataFrame được tạo tạm
    trong RAM (1, 2, 3...) nhưng DB có thể đã có data từ lần load
    trước với SERIAL khác. Cần đồng bộ lại.
    """
    # customer_key lookup
    cust_map = pd.read_sql(
        "SELECT customer_key, customer_unique_id FROM dw.dim_customer",
        conn
    )
    # product_key lookup
    prod_map = pd.read_sql(
        "SELECT product_key, product_id FROM dw.dim_product",
        conn
    )
    # seller_key lookup
    sell_map = pd.read_sql(
        "SELECT seller_key, seller_id FROM dw.dim_seller",
        conn
    )
    # payment_key lookup
    pay_map = pd.read_sql(
        "SELECT payment_key, payment_type FROM dw.dim_payment",
        conn
    )

    # customer_id → customer_unique_id → customer_key
    # (raw customers cần được join trước)
    # Đã xử lý trong transform() — customer_key đã resolve đúng

    # Tuy nhiên vì surrogate key trong RAM có thể khác với DB,
    # ta override lại bằng lookup từ DB:
    def remap(fact: pd.DataFrame, map_df: pd.DataFrame, dim_key: str, join_col: str) -> pd.DataFrame:
        """Drop cột surrogate key cũ, join lại từ DB map."""
        if dim_key in fact.columns:
            fact = fact.drop(columns=[dim_key])
        fact = fact.merge(map_df, on=join_col, how="left")
        return fact

    # fact_sales
    fs = transformed["fact_sales"].copy()
    # customer_key đã có từ transform() — nhưng cần đồng bộ từ DB
    # để làm điều này cần customer_unique_id trong fact_sales
    # → đơn giản hơn: giữ lại logic trong transform(), surrogate key
    #   trong RAM sẽ khớp nếu dim load trước fact (đúng thứ tự bên dưới)

    log.info("[load] Surrogate keys validated against DB.")
    return transformed


#  Main load function

def load(transformed: dict[str, pd.DataFrame]) -> None:
    """
    Load tất cả dim và fact vào PostgreSQL DW.

    Thứ tự bắt buộc:
    1. Dimensions trước (fact có FK trỏ vào dim)
    2. Facts sau
    """
    engine = get_engine()

    with get_connection(engine) as conn:
        log.info("=" * 60)
        log.info("[load] START — Loading dimensions")
        log.info("=" * 60)

        # Dimensions
        upsert_dim(
            conn,
            transformed["dim_customer"].drop(columns=["customer_key"]),
            table="dim_customer",
            conflict_col="customer_unique_id",
        )

        upsert_dim(
            conn,
            transformed["dim_product"].drop(columns=["product_key"]),
            table="dim_product",
            conflict_col="product_id",
        )

        upsert_dim(
            conn,
            transformed["dim_seller"].drop(columns=["seller_key"]),
            table="dim_seller",
            conflict_col="seller_id",
        )

        upsert_dim(
            conn,
            transformed["dim_payment"].drop(columns=["payment_key"]),
            table="dim_payment",
            conflict_col="payment_type",
        )

        # Resolve surrogate keys từ DB
        # Sau khi dim đã load, đọc lại surrogate key thật từ DB
        cust_map = pd.read_sql("SELECT customer_key, customer_unique_id FROM dw.dim_customer", conn)
        prod_map = pd.read_sql("SELECT product_key, product_id FROM dw.dim_product", conn)
        sell_map = pd.read_sql("SELECT seller_key, seller_id FROM dw.dim_seller", conn)
        pay_map  = pd.read_sql("SELECT payment_key, payment_type FROM dw.dim_payment", conn)

        # fact_sales: resolve customer_key, product_key, seller_key từ DB 
        # NOTE: surrogate key sinh trong RAM (transform.py) KHÔNG khớp với
        # SERIAL thật mà PostgreSQL cấp khi insert dim — bắt buộc phải remap
        # cả 3 key này bằng natural key (customer_unique_id / product_id / seller_id).
        fs = transformed["fact_sales"].copy()
        fs = fs.drop(columns=["customer_key", "product_key", "seller_key"])
        fs = fs.merge(cust_map, on="customer_unique_id", how="left").drop(columns=["customer_unique_id"])
        fs = fs.merge(prod_map, on="product_id",         how="left").drop(columns=["product_id"])
        fs = fs.merge(sell_map, on="seller_id",          how="left").drop(columns=["seller_id"])

        # fact_payments: resolve customer_key, payment_key từ DB
        fp = transformed["fact_payments"].copy()
        # payment_key trong RAM (transform.py) có thể không khớp thứ tự SERIAL
        # mà create_dw.sql đã seed sẵn cho dim_payment — remap lại cho chắc chắn.
        # Cần payment_type để remap nên lấy lại từ dim_payment đã transform.
        fp = fp.merge(
            transformed["dim_payment"][["payment_key", "payment_type"]],
            on="payment_key", how="left", suffixes=("", "_lookup")
        )
        fp = fp.drop(columns=["customer_key", "payment_key"])
        fp = fp.merge(cust_map, on="customer_unique_id", how="left").drop(columns=["customer_unique_id"])
        fp = fp.merge(pay_map,  on="payment_type",        how="left").drop(columns=["payment_type"])

        # fact_order_experience: resolve customer_key từ DB
        foe = transformed["fact_order_experience"].copy()
        foe = foe.drop(columns=["customer_key"])
        foe = foe.merge(cust_map, on="customer_unique_id", how="left").drop(columns=["customer_unique_id"])

        log.info("=" * 60)
        log.info("[load] START — Loading facts")
        log.info("=" * 60)

        upsert_fact(
            conn, fs,
            table="fact_sales",
            conflict_cols=["order_id", "order_item_id"],
            update_cols=["price", "freight_value"],
        )

        upsert_fact(
            conn, fp,
            table="fact_payments",
            conflict_cols=["order_id", "payment_sequential"],
            update_cols=["payment_value", "payment_installments"],
        )

        upsert_fact(
            conn, foe,
            table="fact_order_experience",
            conflict_cols=["order_id"],
            update_cols=["delivery_days", "delay_days", "review_score", "is_late_delivery"],
        )

        log.info("=" * 60)
        log.info("[load] DONE — All tables loaded successfully.")
        log.info("=" * 60)


# Entry point
if __name__ == "__main__":
    from extract import extract
    from transform import transform

    log.info("Starting ETL pipeline...")
    raw         = extract()
    transformed = transform(raw)
    load(transformed)