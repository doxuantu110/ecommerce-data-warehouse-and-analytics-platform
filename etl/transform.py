"""
transform.py
------------
Transform layer: nhận raw DataFrames từ extract.py,
trả về dict các DataFrame đã sẵn sàng để load vào DW.

Output tables:
  Dimensions : dim_customer, dim_product, dim_seller, dim_payment
  Facts      : fact_sales, fact_payments, fact_order_experience

NOTE: dim_date được sinh riêng trong generate_dim_date.py
      và đã được populate bởi create_dw.sql khi container khởi động.
"""

import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def _add_surrogate_key(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Thêm cột surrogate key tăng dần từ 1 (Kimball convention)."""
    df = df.reset_index(drop=True)
    df.insert(0, col_name, range(1, len(df) + 1))
    return df


def _to_date_key(series: pd.Series) -> pd.Series:
    """Chuyển datetime series thành date_key dạng YYYYMMDD (int)."""
    return series.dt.strftime("%Y%m%d").astype("Int64")


def _installment_group(n: int) -> str:
    """Phân nhóm số kỳ trả góp thành label cho DIM_PAYMENT."""
    if n <= 1:
        return "1 installment"
    elif n <= 3:
        return "2-3"
    elif n <= 6:
        return "4-6"
    else:
        return "7+"


# ── Dimensions ────────────────────────────────────────────────────────────────

def transform_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    """
    DIM_CUSTOMER
    ------------
    - Deduplicate theo customer_unique_id (giữ record cuối — địa chỉ mới nhất)
    - customer_id là per-order surrogate, KHÔNG dùng làm PK
    """
    dim = (
        customers[["customer_unique_id", "customer_state", "customer_city"]]
        .drop_duplicates(subset=["customer_unique_id"], keep="last")
        .copy()
    )

    dim["customer_state"] = dim["customer_state"].str.strip().str.upper()
    dim["customer_city"]  = dim["customer_city"].str.strip().str.title()

    dim = _add_surrogate_key(dim, "customer_key")

    print(f"[transform] dim_customer       → {len(dim):>7,} rows")
    return dim


def transform_dim_product(
    products: pd.DataFrame,
    category_translation: pd.DataFrame,
) -> pd.DataFrame:
    """
    DIM_PRODUCT
    -----------
    - Join với category_translation để lấy tên tiếng Anh
    - Fill null cho các cột text/số
    """
    dim = products.merge(
        category_translation,
        on="product_category_name",
        how="left",
    )

    dim = dim[[
        "product_id",
        "product_category_name_english",
        "product_name_lenght",        # typo in Olist CSV (missing 'h') — do not fix here
        "product_description_lenght", # typo in Olist CSV (missing 'h') — do not fix here
        "product_photos_qty",
        "product_weight_g",
    ]].copy()

    dim.rename(columns={
        "product_category_name_english": "category_name_english",
        "product_name_lenght":           "product_name_length",        # fix typo on rename
        "product_description_lenght":    "product_description_length", # fix typo on rename
        "product_photos_qty":            "photos_qty",
        "product_weight_g":              "weight_g",
    }, inplace=True)

    dim["category_name_english"] = (
        dim["category_name_english"].fillna("unknown").str.strip().str.lower()
    )
    dim["product_name_length"]        = dim["product_name_length"].fillna(0).astype(int)
    dim["product_description_length"] = dim["product_description_length"].fillna(0).astype(int)
    dim["photos_qty"]                 = dim["photos_qty"].fillna(0).astype(int)
    dim["weight_g"]                   = dim["weight_g"].fillna(0).astype(float)

    dim = _add_surrogate_key(dim, "product_key")

    print(f"[transform] dim_product        → {len(dim):>7,} rows")
    return dim


def transform_dim_seller(sellers: pd.DataFrame) -> pd.DataFrame:
    """
    DIM_SELLER
    ----------
    - Clean state/city text
    """
    dim = sellers[["seller_id", "seller_state", "seller_city"]].copy()

    dim["seller_state"] = dim["seller_state"].str.strip().str.upper()
    dim["seller_city"]  = dim["seller_city"].str.strip().str.title()

    dim = _add_surrogate_key(dim, "seller_key")

    print(f"[transform] dim_seller         → {len(dim):>7,} rows")
    return dim


def transform_dim_payment(payments: pd.DataFrame) -> pd.DataFrame:
    """
    DIM_PAYMENT
    -----------
    - Lấy distinct payment_type
    - Gán installment_group dựa trên payment_installments trung bình per type
    NOTE: DIM_PAYMENT đã được seed trong create_dw.sql.
          Hàm này chỉ cần thiết nếu muốn build lookup table trong ETL.
    """
    dim = (
        payments[["payment_type"]]
        .drop_duplicates()
        .copy()
    )
    # installment_group mặc định cho non-credit_card là "1 installment"
    dim["installment_group"] = "1 installment"
    dim = _add_surrogate_key(dim, "payment_key")

    print(f"[transform] dim_payment        → {len(dim):>7,} rows")
    return dim


# ── Facts ─────────────────────────────────────────────────────────────────────

def transform_fact_sales(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_seller: pd.DataFrame,
) -> pd.DataFrame:
    """
    FACT_SALES
    ----------
    Grain: 1 product line item in 1 order
    Measures: price, freight_value

    Steps:
    1. Filter orders to 'delivered' only
    2. Join order_items → orders → customers (để lấy customer_unique_id)
    3. Resolve surrogate keys (customer_key, product_key, seller_key, date_key)
    """
    # Step 1: chỉ lấy đơn đã giao
    orders_parsed = orders.copy()
    orders_parsed["order_purchase_timestamp"] = pd.to_datetime(
        orders_parsed["order_purchase_timestamp"]
    )
    orders_delivered = orders_parsed[
        orders_parsed["order_status"] == "delivered"
    ][["order_id", "customer_id", "order_purchase_timestamp"]]

    # Step 2: join order_items → orders
    fact = order_items.merge(orders_delivered, on="order_id", how="inner")

    # Step 3: lấy customer_unique_id từ raw customers
    # (dim_customer chỉ có customer_unique_id, không có customer_id)
    # cần raw customers để lookup
    # → truyền thêm customers_raw vào hàm này
    # (xem transform() bên dưới để hiểu flow)

    # Resolve customer_key
    customer_lookup = dim_customer[["customer_key", "customer_unique_id"]]
    fact = (
        fact
        .merge(
            # customers_raw được truyền vào qua wrapper transform()
            fact[["customer_id"]],  # placeholder — xem transform() wrapper
            on="customer_id",
            how="left",
        )
    )

    # Resolve product_key
    product_lookup = dim_product[["product_key", "product_id"]]
    fact = fact.merge(product_lookup, on="product_id", how="left")

    # Resolve seller_key
    seller_lookup = dim_seller[["seller_key", "seller_id"]]
    fact = fact.merge(seller_lookup, on="seller_id", how="left")

    # date_key từ order_purchase_timestamp
    fact["date_key"] = _to_date_key(fact["order_purchase_timestamp"])

    fact = fact[[
        "order_id", "order_item_id",
        "customer_key", "product_key", "seller_key", "date_key",
        "price", "freight_value",
    ]].copy()

    fact["price"]         = fact["price"].round(2)
    fact["freight_value"] = fact["freight_value"].round(2)

    print(f"[transform] fact_sales         → {len(fact):>7,} rows")
    return fact


def transform_fact_payments(
    orders: pd.DataFrame,
    payments: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_payment: pd.DataFrame,
) -> pd.DataFrame:
    """
    FACT_PAYMENTS
    -------------
    Grain: 1 payment record (1 order có thể có nhiều rows — split payment)
    Measures: payment_value, payment_installments
    """
    orders_parsed = orders.copy()
    orders_parsed["order_purchase_timestamp"] = pd.to_datetime(
        orders_parsed["order_purchase_timestamp"]
    )
    orders_delivered = orders_parsed[
        orders_parsed["order_status"] == "delivered"
    ][["order_id", "customer_id", "order_purchase_timestamp"]]

    fact = payments.merge(orders_delivered, on="order_id", how="inner")

    # Resolve customer_key (xem transform() wrapper)
    # Resolve payment_key
    payment_lookup = dim_payment[["payment_key", "payment_type"]]
    fact = fact.merge(payment_lookup, on="payment_type", how="left")

    fact["date_key"] = _to_date_key(fact["order_purchase_timestamp"])

    fact = fact[[
        "order_id", "payment_sequential",
        "customer_key",   # resolve trong transform() wrapper
        "date_key", "payment_key",
        "payment_value", "payment_installments",
    ]].copy()

    fact["payment_value"] = fact["payment_value"].round(2)
    fact["payment_installments"] = fact["payment_installments"].fillna(1).astype(int)

    print(f"[transform] fact_payments      → {len(fact):>7,} rows")
    return fact


def transform_fact_order_experience(
    orders: pd.DataFrame,
    reviews: pd.DataFrame,
    dim_customer: pd.DataFrame,
) -> pd.DataFrame:
    """
    FACT_ORDER_EXPERIENCE
    ---------------------
    Grain: 1 delivered order
    Measures: delivery_days, delay_days, review_score
    Attribute: is_late_delivery
    """
    orders_parsed = orders.copy()
    for col in [
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        orders_parsed[col] = pd.to_datetime(orders_parsed[col])

    orders_delivered = orders_parsed[
        (orders_parsed["order_status"] == "delivered")
        & orders_parsed["order_delivered_customer_date"].notna()
    ].copy()

    # Derived measures
    orders_delivered["delivery_days"] = (
        orders_delivered["order_delivered_customer_date"]
        - orders_delivered["order_purchase_timestamp"]
    ).dt.days

    orders_delivered["delay_days"] = (
        orders_delivered["order_delivered_customer_date"]
        - orders_delivered["order_estimated_delivery_date"]
    ).dt.days

    orders_delivered["is_late_delivery"] = orders_delivered["delay_days"] > 0

    orders_delivered["date_key"] = _to_date_key(
        orders_delivered["order_purchase_timestamp"]
    )

    # Join reviews (left — không phải đơn nào cũng có review)
    reviews_clean = (
        reviews[["order_id", "review_score"]]
        .drop_duplicates(subset=["order_id"], keep="last")
    )
    fact = orders_delivered.merge(reviews_clean, on="order_id", how="left")

    fact = fact[[
        "order_id",
        "customer_id",   # resolve → customer_key trong transform() wrapper
        "date_key",
        "delivery_days", "delay_days",
        "review_score",
        "is_late_delivery",
    ]].copy()

    fact["review_score"] = fact["review_score"].where(
        fact["review_score"].notna(), other=None
    )

    print(f"[transform] fact_order_exp     → {len(fact):>7,} rows")
    return fact


# ── Main wrapper ──────────────────────────────────────────────────────────────

def transform(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Wrapper chính: nhận output của extract(), trả về dict DataFrames
    đã sẵn sàng để load vào DW.

    Surrogate key lookup được thực hiện tại đây sau khi tất cả
    dim đã được build — tránh circular dependency.
    """

    # ── 1. Build dimensions ──────────────────────────────────────────────────
    dim_customer = transform_dim_customer(raw["customers"])
    dim_product  = transform_dim_product(raw["products"], raw["category_translation"])
    dim_seller   = transform_dim_seller(raw["sellers"])
    dim_payment  = transform_dim_payment(raw["payments"])

    # ── 2. Lookup helper: customer_id → customer_key ─────────────────────────
    # Olist: customer_id (per-order) → customer_unique_id (per-person) → customer_key
    customer_id_map = (
        raw["customers"][["customer_id", "customer_unique_id"]]
        .merge(
            dim_customer[["customer_unique_id", "customer_key"]],
            on="customer_unique_id",
            how="left",
        )
        [["customer_id", "customer_key"]]
    )

    # ── 3. Build FACT_SALES ──────────────────────────────────────────────────
    orders_parsed = raw["orders"].copy()
    orders_parsed["order_purchase_timestamp"] = pd.to_datetime(
        orders_parsed["order_purchase_timestamp"]
    )
    orders_delivered_base = orders_parsed[
        orders_parsed["order_status"] == "delivered"
    ][["order_id", "customer_id", "order_purchase_timestamp"]]

    # fact_sales
    fs = raw["order_items"].merge(orders_delivered_base, on="order_id", how="inner")
    fs = fs.merge(customer_id_map, on="customer_id", how="left")
    fs = fs.merge(dim_product[["product_key", "product_id"]], on="product_id", how="left")
    fs = fs.merge(dim_seller[["seller_key",  "seller_id"]],  on="seller_id",  how="left")
    fs["date_key"] = _to_date_key(fs["order_purchase_timestamp"])
    fact_sales = fs[[
        "order_id", "order_item_id",
        "customer_key", "product_key", "seller_key", "date_key",
        "price", "freight_value",
    ]].copy()
    fact_sales["price"]         = fact_sales["price"].round(2)
    fact_sales["freight_value"] = fact_sales["freight_value"].round(2)
    print(f"[transform] fact_sales         → {len(fact_sales):>7,} rows")

    # ── 4. Build FACT_PAYMENTS ───────────────────────────────────────────────
    fp = raw["payments"].merge(orders_delivered_base, on="order_id", how="inner")
    fp = fp.merge(customer_id_map, on="customer_id", how="left")
    fp = fp.merge(dim_payment[["payment_key", "payment_type"]], on="payment_type", how="left")
    fp["date_key"] = _to_date_key(fp["order_purchase_timestamp"])
    fact_payments = fp[[
        "order_id", "payment_sequential",
        "customer_key", "date_key", "payment_key",
        "payment_value", "payment_installments",
    ]].copy()
    fact_payments["payment_value"]        = fact_payments["payment_value"].round(2)
    fact_payments["payment_installments"] = fact_payments["payment_installments"].fillna(1).astype(int)
    print(f"[transform] fact_payments      → {len(fact_payments):>7,} rows")

    # ── 5. Build FACT_ORDER_EXPERIENCE ───────────────────────────────────────
    for col in [
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        orders_parsed[col] = pd.to_datetime(orders_parsed[col])

    od = orders_parsed[
        (orders_parsed["order_status"] == "delivered")
        & orders_parsed["order_delivered_customer_date"].notna()
    ].copy()
    od["delivery_days"]    = (od["order_delivered_customer_date"] - od["order_purchase_timestamp"]).dt.days
    od["delay_days"]       = (od["order_delivered_customer_date"] - od["order_estimated_delivery_date"]).dt.days
    od["is_late_delivery"] = od["delay_days"] > 0
    od["date_key"]         = _to_date_key(od["order_purchase_timestamp"])

    reviews_clean = (
        raw["reviews"][["order_id", "review_score"]]
        .drop_duplicates(subset=["order_id"], keep="last")
    )
    foe = od.merge(reviews_clean, on="order_id", how="left")
    foe = foe.merge(customer_id_map, on="customer_id", how="left")

    fact_order_experience = foe[[
        "order_id", "customer_key", "date_key",
        "delivery_days", "delay_days", "review_score", "is_late_delivery",
    ]].copy()
    print(f"[transform] fact_order_exp     → {len(fact_order_experience):>7,} rows")

    # ── 6. Return all ────────────────────────────────────────────────────────
    print(f"\n[transform] Done.\n")

    return {
        # Dimensions
        "dim_customer":          dim_customer,
        "dim_product":           dim_product,
        "dim_seller":            dim_seller,
        "dim_payment":           dim_payment,
        # Facts
        "fact_sales":            fact_sales,
        "fact_payments":         fact_payments,
        "fact_order_experience": fact_order_experience,
    }


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from extract import extract
    raw = extract()
    transformed = transform(raw)
    for name, df in transformed.items():
        print(f"{name:<30} {df.shape[0]:>7,} rows × {df.shape[1]} cols")
