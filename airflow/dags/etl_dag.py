"""
etl_dag.py  —  SKELETON
----------------------------------
Mục đích: verify Airflow nhận diện và chạy được DAG trước khi gắn logic thật.
Tất cả task dùng EmptyOperator — không làm gì cả, chỉ để test flow.

Sau khi confirm DAG chạy OK trên UI → chuyển sang Ngày 11:
thay EmptyOperator bằng PythonOperator với logic ETL thật.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Default args 
default_args = {
    "owner":            "data_engineer",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          0,              # skeleton: không retry
    "retry_delay":      timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id="olist_etl_pipeline",
    description="[SKELETON] Olist ETL: Extract → Transform → Load",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,                      # trigger thủ công
    catchup=False,
    tags=["olist", "etl", "skeleton"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_task = EmptyOperator(task_id="extract_task")

    transform_task = EmptyOperator(task_id="transform_task")

    load_dim_task = EmptyOperator(task_id="load_dim_task")

    load_fact_task = EmptyOperator(task_id="load_fact_task")

    end = EmptyOperator(task_id="end")

    # Flow
    start >> extract_task >> transform_task >> load_dim_task >> load_fact_task >> end