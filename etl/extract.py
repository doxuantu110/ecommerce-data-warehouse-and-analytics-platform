"""
extract.py
----------
Extract layer: đọc toàn bộ CSV Olist vào dict of DataFrames.
Không thực hiện bất kỳ transformation nào ở bước này —
raw data được giữ nguyên để transform.py xử lý riêng.
"""

import os
import pandas as pd


# Config
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

CSV_FILES = {
    "customers":           "olist_customers_dataset.csv",
    "orders":              "olist_orders_dataset.csv",
    "order_items":         "olist_order_items_dataset.csv",
    "payments":            "olist_order_payments_dataset.csv",
    "reviews":             "olist_order_reviews_dataset.csv",
    "products":            "olist_products_dataset.csv",
    "sellers":             "olist_sellers_dataset.csv",
    "geolocation":         "olist_geolocation_dataset.csv",
    "category_translation":"product_category_name_translation.csv",
}


# Main function 
def extract() -> dict[str, pd.DataFrame]:
    """
    Đọc toàn bộ file CSV Olist từ DATA_PATH.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key = tên bảng logic, Value = raw DataFrame chưa transform.
    """
    dataframes = {}

    for name, filename in CSV_FILES.items():
        filepath = os.path.join(DATA_PATH, filename)

        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"[extract] File not found: {filepath}\n"
                f"Hãy tải Olist dataset từ Kaggle và đặt vào thư mục data/raw/"
            )

        df = pd.read_csv(filepath)
        dataframes[name] = df
        print(f"[extract] {name:<25} → {df.shape[0]:>7,} rows × {df.shape[1]} cols")

    print(f"\n[extract] Done — {len(dataframes)} tables loaded.\n")
    return dataframes


# Entry point
if __name__ == "__main__":
    raw = extract()
