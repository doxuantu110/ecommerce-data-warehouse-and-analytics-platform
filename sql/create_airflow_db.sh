#!/bin/bash
# Tạo database riêng cho Airflow metadata khi container khởi động lần đầu.
# File .sh trong docker-entrypoint-initdb.d/ được chạy qua bash,
# cho phép dùng psql CLI với \gexec hoặc IF NOT EXISTS logic.
set -e
 
psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname   "$POSTGRES_DB" \
     <<-EOSQL
        SELECT 'CREATE DATABASE airflow_db'
        WHERE NOT EXISTS (
            SELECT FROM pg_database WHERE datname = 'airflow_db'
        )\gexec
EOSQL
 
echo "airflow_db created (or already exists)."