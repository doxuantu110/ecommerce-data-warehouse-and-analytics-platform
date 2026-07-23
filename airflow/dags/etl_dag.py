"""
etl_dag.py
----------
DAG orchestrate full ETL pipeline cho Olist E-Commerce Data Warehouse.

Flow:
    extract_task
        ↓
    transform_task
        ↓
    load_dim_task
        ↓
    load_fact_task

Về XCom và DataFrame:
    XCom của Airflow lưu trong DB, giới hạn ~48KB — không thể pass
    DataFrame 100k rows giữa các task. Giải pháp: mỗi task tự chạy lại
    extract() + transform() từ source CSV. Vì source không thay đổi,
    cách này idempotent và đúng chuẩn production.

Connection:
    Dùng Airflow Connection 'postgres_olist_dw' (Admin → Connections)
    thay vì hardcode credentials. Connection này trỏ tới PostgreSQL DW.
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Thêm etl/ vào sys.path để import các module ETL ──────────────────────────
# /opt/airflow/etl/ được mount từ ./etl/ trong docker-compose.yml
sys.path.insert(0, "/opt/airflow/etl")

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
AIRFLOW_CONN_ID = "postgres_olist_dw"


# ══════════════════════════════════════════════════════════════════════════════
# Task callables
# ══════════════════════════════════════════════════════════════════════════════

def run_extract(**context):
    """
    Task 1: Extract
    Validate tất cả CSV files tồn tại và đọc được.
    Push row counts vào XCom để các task sau log tóm tắt.
    """
    import time
    from extract import extract

    ti          = context["ti"]
    retry_count = ti.try_number - 1
    if retry_count > 0:
        log.warning(f"[extract_task] RETRY #{retry_count} — previous attempt failed.")

    log.info("[extract_task] Starting extraction from source CSV files...")
    t0  = time.time()
    raw = extract()
    elapsed = time.time() - t0

    row_counts = {name: len(df) for name, df in raw.items()}
    ti.xcom_push(key="row_counts", value=row_counts)

    log.info(f"[extract_task] Done in {elapsed:.1f}s — {len(row_counts)} tables loaded.")
    log.info(f"[extract_task] Total rows extracted: {sum(row_counts.values()):,}")

    return row_counts


def run_transform(**context):
    """
    Task 2: Transform
    Chạy lại extract() + transform() để build tất cả dim và fact DataFrames.
    Push shape summary vào XCom.

    NOTE: Chạy lại extract() là đúng — không dùng XCom để pass DataFrame
    vì XCom có giới hạn kích thước, không phù hợp cho large DataFrames.
    """
    import time
    from extract import extract
    from transform import transform

    ti          = context["ti"]
    retry_count = ti.try_number - 1
    if retry_count > 0:
        log.warning(f"[transform_task] RETRY #{retry_count} — previous attempt failed.")

    log.info("[transform_task] Running extract + transform...")
    t0          = time.time()
    raw         = extract()
    t_extract   = time.time()
    transformed = transform(raw)
    t_transform = time.time()

    shape_summary = {
        name: {"rows": len(df), "cols": len(df.columns)}
        for name, df in transformed.items()
    }
    ti.xcom_push(key="shape_summary", value=shape_summary)

    for name, info in shape_summary.items():
        log.info(f"[transform_task]   {name:<30} → {info['rows']:>7,} rows × {info['cols']} cols")

    log.info(
        f"[transform_task] Done — extract: {t_extract - t0:.1f}s | "
        f"transform: {t_transform - t_extract:.1f}s | "
        f"total: {t_transform - t0:.1f}s"
    )
    return shape_summary


def run_load_dims(**context):
    """
    Task 3: Load Dimensions
    Chạy extract() + transform() rồi load chỉ các bảng DIM vào PostgreSQL.
    Dùng PostgresHook lấy connection từ Airflow Connection 'postgres_olist_dw'.

    Tách load_dim và load_fact thành 2 task riêng vì:
    - FK constraint: fact phải được load SAU dim
    - Dễ retry: nếu fact load lỗi, không cần chạy lại dim
    """
    import time
    from extract import extract
    from transform import transform
    from load import get_engine_from_hook, get_connection, upsert_dim
    import pandas as pd

    ti          = context["ti"]
    retry_count = ti.try_number - 1
    if retry_count > 0:
        log.warning(
            f"[load_dim_task] RETRY #{retry_count} — previous attempt failed. "
            f"UPSERT is idempotent: safe to re-run."
        )

    log.info("[load_dim_task] Connecting via Airflow PostgresHook...")
    engine = get_engine_from_hook(AIRFLOW_CONN_ID)

    log.info("[load_dim_task] Running extract + transform...")
    t0          = time.time()
    raw         = extract()
    transformed = transform(raw)
    t_etl       = time.time()
    log.info(f"[load_dim_task] Extract + transform done in {t_etl - t0:.1f}s")

    with get_connection(engine) as conn:
        log.info("[load_dim_task] Loading dimensions...")
        t_load = time.time()

        upsert_dim(conn, transformed["dim_customer"].drop(columns=["customer_key"]),
                   table="dim_customer", conflict_col="customer_unique_id")

        upsert_dim(conn, transformed["dim_product"].drop(columns=["product_key"]),
                   table="dim_product",  conflict_col="product_id")

        upsert_dim(conn, transformed["dim_seller"].drop(columns=["seller_key"]),
                   table="dim_seller",   conflict_col="seller_id")

        upsert_dim(conn, transformed["dim_payment"].drop(columns=["payment_key"]),
                   table="dim_payment",  conflict_col="payment_type")

        elapsed = time.time() - t_load
        log.info(f"[load_dim_task] All dimensions loaded in {elapsed:.1f}s")

    log.info(f"[load_dim_task] Total task time: {time.time() - t0:.1f}s")


def run_load_facts(**context):
    """
    Task 4: Load Facts
    Chạy extract() + transform() rồi load các bảng FACT vào PostgreSQL.
    Dùng PostgresHook — không hardcode credentials.

    Idempotency: UPSERT on natural key (ON CONFLICT DO UPDATE).
    Chạy lại task nhiều lần sẽ không tạo duplicate rows.
    """
    import time
    from extract import extract
    from transform import transform
    from load import get_engine_from_hook, get_connection, upsert_fact
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import pandas as pd

    ti          = context["ti"]
    retry_count = ti.try_number - 1
    if retry_count > 0:
        log.warning(
            f"[load_fact_task] RETRY #{retry_count} — previous attempt failed. "
            f"UPSERT is idempotent: safe to re-run."
        )

    log.info("[load_fact_task] Connecting via Airflow PostgresHook...")
    engine = get_engine_from_hook(AIRFLOW_CONN_ID)
    hook   = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)

    log.info("[load_fact_task] Running extract + transform...")
    t0          = time.time()
    raw         = extract()
    transformed = transform(raw)
    t_etl       = time.time()
    log.info(f"[load_fact_task] Extract + transform done in {t_etl - t0:.1f}s")

    # ── Resolve surrogate keys từ DB ─────────────────────────────────────────
    # hook.get_pandas_df() tránh pandas 2.x + SQLAlchemy compatibility issue
    log.info("[load_fact_task] Resolving surrogate keys from DB...")
    cust_map = hook.get_pandas_df("SELECT customer_key, customer_unique_id FROM dw.dim_customer")
    prod_map = hook.get_pandas_df("SELECT product_key, product_id FROM dw.dim_product")
    sell_map = hook.get_pandas_df("SELECT seller_key, seller_id FROM dw.dim_seller")
    pay_map  = hook.get_pandas_df("SELECT payment_key, payment_type FROM dw.dim_payment")
    log.info(f"[load_fact_task] Surrogate keys resolved in {time.time() - t_etl:.1f}s")

    with get_connection(engine) as conn:
        t_load = time.time()

        # ── fact_sales ────────────────────────────────────────────────────────
        fs = transformed["fact_sales"].copy()
        fs = fs.drop(columns=["customer_key", "product_key", "seller_key"])
        fs = fs.merge(cust_map, on="customer_unique_id", how="left").drop(columns=["customer_unique_id"])
        fs = fs.merge(prod_map, on="product_id",         how="left").drop(columns=["product_id"])
        fs = fs.merge(sell_map, on="seller_id",          how="left").drop(columns=["seller_id"])

        upsert_fact(conn, fs,
                    table="fact_sales",
                    conflict_cols=["order_id", "order_item_id"],
                    update_cols=["price", "freight_value"])

        # ── fact_payments ─────────────────────────────────────────────────────
        fp = transformed["fact_payments"].copy()
        fp = fp.merge(
            transformed["dim_payment"][["payment_key", "payment_type"]],
            on="payment_key", how="left"
        )
        fp = fp.drop(columns=["customer_key", "payment_key"])
        fp = fp.merge(cust_map, on="customer_unique_id", how="left").drop(columns=["customer_unique_id"])
        fp = fp.merge(pay_map,  on="payment_type",       how="left").drop(columns=["payment_type"])

        upsert_fact(conn, fp,
                    table="fact_payments",
                    conflict_cols=["order_id", "payment_sequential"],
                    update_cols=["payment_value", "payment_installments"])

        # ── fact_order_experience ─────────────────────────────────────────────
        foe = transformed["fact_order_experience"].copy()
        foe = foe.drop(columns=["customer_key"])
        foe = foe.merge(cust_map, on="customer_unique_id", how="left").drop(columns=["customer_unique_id"])

        upsert_fact(conn, foe,
                    table="fact_order_experience",
                    conflict_cols=["order_id"],
                    update_cols=["delivery_days", "delay_days", "review_score", "is_late_delivery"])

        elapsed_load = time.time() - t_load
        log.info(f"[load_fact_task] All fact tables loaded in {elapsed_load:.1f}s")

    log.info(f"[load_fact_task] Total task time: {time.time() - t0:.1f}s")


def run_quality_checks(**context):
    """
    Task 5: Data Quality Check
    Chạy 20 checks trên tất cả fact và dim tables.
    Task FAIL ngay lập tức nếu bất kỳ check nào không đạt —
    chặn pipeline và không cho data xấu tiếp tục downstream.

    Checks bao gồm:
    - Completeness : fact tables không rỗng
    - Validity     : price > 0, payment_value > 0, review_score 1-5
    - Nullability  : customer_key, product_key không null
    - Uniqueness   : không có duplicate natural key
    - Referential  : FK integrity giữa fact và dim
    """
    import time
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti          = context["ti"]
    retry_count = ti.try_number - 1
    if retry_count > 0:
        log.warning(f"[quality_check] RETRY #{retry_count} (retries=0 for this task — should not happen)")

    hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)

    # ── Định nghĩa tất cả checks ─────────────────────────────────────────────
    checks = [
        # (check_id, check_name, sql, expect_zero)
        # expect_zero=True  → query phải trả về 0 (không có vi phạm)
        # expect_zero=False → query phải trả về > 0 (bảng phải có data)
        ("QC-01", "fact_sales_not_empty",
         "SELECT COUNT(*) FROM dw.fact_sales",
         False),

        ("QC-02", "fact_sales_no_null_customer_key",
         "SELECT COUNT(*) FROM dw.fact_sales WHERE customer_key IS NULL",
         True),

        ("QC-03", "fact_sales_no_null_product_key",
         "SELECT COUNT(*) FROM dw.fact_sales WHERE product_key IS NULL",
         True),

        ("QC-04", "fact_sales_price_positive",
         "SELECT COUNT(*) FROM dw.fact_sales WHERE price <= 0",
         True),

        ("QC-05", "fact_sales_freight_non_negative",
         "SELECT COUNT(*) FROM dw.fact_sales WHERE freight_value < 0",
         True),

        ("QC-06", "fact_sales_no_duplicates",
         """SELECT COUNT(*) FROM (
                SELECT order_id, order_item_id
                FROM dw.fact_sales
                GROUP BY order_id, order_item_id
                HAVING COUNT(*) > 1
            ) dups""",
         True),

        ("QC-07", "fact_payments_not_empty",
         "SELECT COUNT(*) FROM dw.fact_payments",
         False),

        ("QC-08", "fact_payments_value_non_negative",
         # payment_value = 0 là hợp lệ — voucher cover toàn bộ đơn hàng
         # chỉ fail nếu âm (không có nghiệp vụ nào tạo ra payment âm)
         "SELECT COUNT(*) FROM dw.fact_payments WHERE payment_value < 0",
         True),

        ("QC-09", "fact_payments_no_null_customer_key",
         "SELECT COUNT(*) FROM dw.fact_payments WHERE customer_key IS NULL",
         True),

        ("QC-10", "credit_card_installments_valid",
         # 2 rows credit_card với installments = 0 là known data anomaly từ Olist source
         # order_id: 744bade1fcf9ff3f31d860ace076d422, 1a57108394169c0b47d8f876acc9ba2d
         # Không fix được ở tầng ETL — exclude 2 orders này, ghi vào Known Issues
         """SELECT COUNT(*) FROM dw.fact_payments fp
            JOIN dw.dim_payment dp ON fp.payment_key = dp.payment_key
            WHERE dp.payment_type = 'credit_card'
              AND fp.payment_installments < 1
              AND fp.order_id NOT IN (
                '744bade1fcf9ff3f31d860ace076d422',
                '1a57108394169c0b47d8f876acc9ba2d'
              )""",
         True),

        ("QC-11", "fact_order_experience_not_empty",
         "SELECT COUNT(*) FROM dw.fact_order_experience",
         False),

        ("QC-12", "fact_experience_delivery_days_non_negative",
         # delivery_days = 0 có thể xảy ra do timestamp precision (mua 23h, giao 1h sáng hôm sau)
         # → date diff = 0 ngày dù thực tế đã giao. Chỉ fail nếu delivery_days âm (impossible).
         """SELECT COUNT(*) FROM dw.fact_order_experience
            WHERE delivery_days IS NOT NULL AND delivery_days < 0""",
         True),

        ("QC-13", "fact_experience_review_score_valid",
         """SELECT COUNT(*) FROM dw.fact_order_experience
            WHERE review_score IS NOT NULL AND review_score NOT BETWEEN 1 AND 5""",
         True),

        ("QC-14", "dim_customer_not_empty",
         "SELECT COUNT(*) FROM dw.dim_customer",
         False),

        ("QC-15", "dim_customer_unique_id_unique",
         """SELECT COUNT(*) FROM (
                SELECT customer_unique_id FROM dw.dim_customer
                GROUP BY customer_unique_id HAVING COUNT(*) > 1
            ) dups""",
         True),

        ("QC-16", "dim_product_not_empty",
         "SELECT COUNT(*) FROM dw.dim_product",
         False),

        ("QC-17", "dim_date_range_valid",
         """SELECT COUNT(*) FROM dw.dim_date
            WHERE full_date < '2016-01-01' OR full_date > '2018-12-31'""",
         True),

        ("QC-18", "fact_sales_customer_fk_valid",
         """SELECT COUNT(*) FROM dw.fact_sales fs
            LEFT JOIN dw.dim_customer dc ON fs.customer_key = dc.customer_key
            WHERE dc.customer_key IS NULL""",
         True),

        ("QC-19", "fact_sales_product_fk_valid",
         """SELECT COUNT(*) FROM dw.fact_sales fs
            LEFT JOIN dw.dim_product dp ON fs.product_key = dp.product_key
            WHERE dp.product_key IS NULL""",
         True),

        ("QC-20", "fact_sales_date_fk_valid",
         """SELECT COUNT(*) FROM dw.fact_sales fs
            LEFT JOIN dw.dim_date d ON fs.date_key = d.date_key
            WHERE d.date_key IS NULL""",
         True),
    ]

    # ── Chạy từng check ──────────────────────────────────────────────────────
    failed_checks = []
    passed        = 0
    t0            = time.time()

    for check_id, check_name, sql, expect_zero in checks:
        result = hook.get_first(sql)
        value  = result[0] if result else 0

        if expect_zero:
            passed_flag = (value == 0)
        else:
            passed_flag = (value > 0)

        status = "PASS" if passed_flag else "FAIL"

        if passed_flag:
            passed += 1
            log.info(f"[quality_check] {check_id} {status}  {check_name} → {value:,}")
        else:
            failed_checks.append(f"{check_id} {check_name} → {value:,}")
            log.error(f"[quality_check] {check_id} {status}  {check_name} → {value:,}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total = len(checks)
    elapsed = time.time() - t0
    log.info(f"[quality_check] Result: {passed}/{total} checks passed in {elapsed:.1f}s.")

    if failed_checks:
        fail_msg = "\n".join(failed_checks)
        raise ValueError(
            f"Data quality check FAILED — {len(failed_checks)}/{total} checks failed:\n{fail_msg}"
        )

    log.info(f"[quality_check] All {total} checks passed. Pipeline complete. ({elapsed:.1f}s)")


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner":            "data_engineer",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="olist_etl_pipeline",
    description="Olist E-Commerce ETL: Extract → Transform → Load DW",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,                    # trigger thủ công, không schedule tự động
    catchup=False,
    tags=["olist", "etl", "data-warehouse"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=run_transform,
    )

    load_dim_task = PythonOperator(
        task_id="load_dim_task",
        python_callable=run_load_dims,
    )

    load_fact_task = PythonOperator(
        task_id="load_fact_task",
        python_callable=run_load_facts,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check_task",
        python_callable=run_quality_checks,
        # Không retry quality check — nếu fail là data thật sự có vấn đề,
        # cần investigate trước khi chạy lại
        retries=0,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # extract → transform → load_dim → load_fact → quality_check
    extract_task >> transform_task >> load_dim_task >> load_fact_task >> quality_check_task