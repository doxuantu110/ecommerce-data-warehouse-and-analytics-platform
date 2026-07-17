# Airflow Local Setup Guide

## Stack

| Component | Version | Role |
|---|---|---|
| Apache Airflow | 2.10.4 | Orchestration |
| PostgreSQL | 15 | DW + Airflow metadata |
| Docker Compose | v2 | Container management |
| Executor | LocalExecutor | Task execution |

---

## 1. Prerequisites

Đảm bảo các biến sau đã có trong file `.env` trước khi khởi động:

```env
# PostgreSQL DW
POSTGRES_USER=olist_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=olist_dw
POSTGRES_PORT=5432
POSTGRES_HOST=localhost

# Airflow
AIRFLOW_DB=airflow_db
AIRFLOW_FERNET_KEY=<generate bên dưới>
AIRFLOW_SECRET_KEY=<generate bên dưới>
```

**Tạo Fernet Key và Secret Key:**

```bash
# Fernet Key (dùng để encrypt Connections trong DB)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret Key (dùng để ký session cookie UI)
python -c "import secrets; print(secrets.token_hex(32))"
```

Dán 2 giá trị vào `.env`. **Không commit file `.env` lên GitHub.**

---

## 2. Khởi động

### Lần đầu tiên (fresh start)

```bash
# Build image Airflow với custom requirements
docker compose build

# Khởi động tất cả services
docker compose up -d
```

`airflow-init` sẽ tự động:
1. Chạy `airflow db migrate` — tạo schema Airflow trong `airflow_db`
2. Tạo user admin (`admin / admin`)
3. Thoát với exit code 0 (service_completed_successfully)

Sau đó `airflow-webserver` và `airflow-scheduler` mới khởi động.

### Theo dõi quá trình khởi động

```bash
# Xem log airflow-init (quan trọng nhất, chạy đầu tiên)
docker logs -f ecommerce_airflow_init

# Xem trạng thái tất cả container
docker compose ps
```

**Trạng thái mong đợi sau ~2 phút:**

```
NAME                        STATUS
ecommerce_postgres          Up (healthy)
ecommerce_airflow_init      Exited (0)       ← đúng, không phải lỗi
ecommerce_airflow_webserver Up (healthy)
ecommerce_airflow_scheduler Up
```

> `airflow-init` exit 0 là **bình thường** — nó là một init container,
> chạy xong nhiệm vụ thì thoát, không giống các service chạy liên tục.

---

## 3. Truy cập Airflow UI

Mở trình duyệt: **http://localhost:8080**

```
Username : admin
Password : admin
```

> ⚠️ Đổi mật khẩu sau lần đăng nhập đầu tiên:
> Admin → Security → Reset My Password

### Các khu vực chính trong UI

| Menu | Dùng để |
|---|---|
| **DAGs** | Xem danh sách DAG, trigger thủ công, xem trạng thái run |
| **Grid View** | Xem lịch sử các run theo dạng lưới |
| **Graph View** | Xem dependency giữa các task trong DAG |
| **Admin → Connections** | Quản lý kết nối tới DB, API... |
| **Admin → Variables** | Lưu biến dùng chung giữa các DAG |
| **Browse → Task Instances** | Debug từng task run |

---

## 4. Tạo PostgreSQL Connection

Connection này cho phép dùng `PostgresHook` trong ETL task thay vì hardcode
credentials — chuẩn production.

### Cách 1: Qua Airflow UI (khuyến nghị cho lần đầu)

1. Vào **Admin → Connections**
2. Click **+** (Add a new record)
3. Điền các field:

| Field | Giá trị |
|---|---|
| **Connection Id** | `postgres_olist_dw` |
| **Connection Type** | `Postgres` |
| **Host** | `postgres` ← tên service trong docker-compose, không phải `localhost` |
| **Schema** | `dw` |
| **Login** | `olist_user` |
| **Password** | `your_password` (giá trị thật trong `.env`) |
| **Port** | `5432` |

4. Click **Test** → phải hiện `Connection successfully tested`
5. Click **Save**

> **Lưu ý quan trọng:** Host phải là `postgres` (tên Docker service),
> không phải `localhost`. Các container giao tiếp với nhau qua tên service
> trong cùng Docker network `ecommerce_network`.

### Cách 2: Qua Environment Variable (không cần UI, tự động)

Thêm vào `docker-compose.yml` phần `environment` của webserver và scheduler:

```yaml
AIRFLOW_CONN_POSTGRES_OLIST_DW: postgresql://olist_user:your_password@postgres:5432/olist_dw
```

Connection sẽ tự động xuất hiện khi container khởi động — không cần tạo tay.
Format: `AIRFLOW_CONN_<CONN_ID_UPPERCASE>`.

---

## 5. Kiểm tra Connection từ CLI

```bash
# Test connection trực tiếp qua CLI (không cần mở UI)
docker exec -it ecommerce_airflow_scheduler \
  airflow connections test postgres_olist_dw
```

Kết quả mong đợi:
```
Connection successfully tested
```

---

## 6. Dùng Connection trong ETL Task

Trong `etl_dag.py`, thay vì hardcode:

```python
# ❌ Không dùng — hardcode credentials
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    user="olist_user",
    password="hardcoded_password",
    dbname="olist_dw"
)
```

Dùng `PostgresHook` với `conn_id`:

```python
# ✅ Đúng chuẩn Airflow
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_task():
    hook = PostgresHook(postgres_conn_id="postgres_olist_dw")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dw.fact_sales")
    print(cursor.fetchone())
```

---

## 7. Các lệnh Docker thường dùng

```bash
# Khởi động services
docker compose up -d

# Dừng (giữ nguyên data)
docker compose down

# Dừng và xóa toàn bộ volume (reset hoàn toàn)
docker compose down -v

# Xem log realtime
docker logs -f ecommerce_airflow_webserver
docker logs -f ecommerce_airflow_scheduler

# Mở shell vào container
docker exec -it ecommerce_airflow_scheduler bash

# Restart một service cụ thể
docker compose restart airflow-webserver

# Xem resource usage
docker stats
```

---

## 8. Troubleshooting

### airflow-init exit với lỗi (exit code khác 0)

```bash
docker logs ecommerce_airflow_init
```

Nguyên nhân thường gặp:
- `postgres` chưa healthy khi `airflow-init` chạy → tăng `start_period` trong healthcheck
- `AIRFLOW_DB` chưa được tạo → kiểm tra log postgres: `docker logs ecommerce_postgres`
- Fernet key sai format → tạo lại bằng lệnh ở mục 1

### Webserver không truy cập được (http://localhost:8080)

```bash
# Kiểm tra webserver có đang chạy không
docker compose ps airflow-webserver

# Xem log
docker logs ecommerce_airflow_webserver

# Đợi thêm — webserver mất 60-90 giây để khởi động hoàn toàn
```

### Scheduler không pick up DAG mới

```bash
# DAG mới cần ~30 giây để scheduler detect
# Kiểm tra syntax DAG trước
docker exec -it ecommerce_airflow_scheduler \
  airflow dags list

# Xem lỗi import DAG
docker exec -it ecommerce_airflow_scheduler \
  airflow dags list-import-errors
```

### Connection test thất bại

Kiểm tra host — phổ biến nhất là nhập `localhost` thay vì `postgres`:

```
# Sai: localhost (chỉ đúng khi chạy Python ngoài Docker)
Host: localhost

# Đúng: tên Docker service
Host: postgres
```

---

## 9. Cấu trúc thư mục Airflow

```
airflow/
├── Dockerfile          ← build image với custom requirements
├── requirements.txt    ← dependencies cho ETL tasks
├── dags/               ← DAG files (mount vào container)
│   └── etl_dag.py
├── logs/               ← Airflow task logs (managed by Docker volume)
└── plugins/            ← Custom operators, hooks (để trống nếu không cần)
```

> **Lưu ý:** `airflow/logs/` được quản lý bởi Docker volume `airflow_logs`,
> không cần commit thư mục này lên GitHub.
> Thêm `airflow/logs/` vào `.gitignore`.
