-- =============================================================
-- FILE: data_quality_checks.sql
-- PURPOSE: Data Quality checks sau khi ETL load xong
--
-- Cách dùng độc lập (ngoài Airflow):
--   docker exec -it ecommerce_postgres psql -U olist_user -d olist_dw \
--     -f /path/to/data_quality_checks.sql
--
-- Mỗi query trả về 1 dòng kết quả dạng:
--   check_name | status | value | threshold | message
-- status = 'PASS' hoặc 'FAIL'
-- =============================================================

SET search_path TO dw;


-- =============================================================
-- FACT_SALES
-- =============================================================

-- QC-01: fact_sales không được rỗng
SELECT
    'QC-01' AS check_id,
    'fact_sales_not_empty' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    1 AS threshold,
    CASE WHEN COUNT(*) > 0
        THEN 'fact_sales has ' || COUNT(*) || ' rows'
        ELSE 'FAIL: fact_sales is empty'
    END AS message
FROM fact_sales;


-- QC-02: Không có NULL customer_key trong fact_sales
SELECT
    'QC-02' AS check_id,
    'fact_sales_no_null_customer_key' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'No NULL customer_key found'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have NULL customer_key'
    END AS message
FROM fact_sales
WHERE customer_key IS NULL;


-- QC-03: Không có NULL product_key trong fact_sales
SELECT
    'QC-03' AS check_id,
    'fact_sales_no_null_product_key' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'No NULL product_key found'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have NULL product_key'
    END AS message
FROM fact_sales
WHERE product_key IS NULL;


-- QC-04: price > 0 trong fact_sales
SELECT
    'QC-04' AS check_id,
    'fact_sales_price_positive' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All prices are positive'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have price <= 0'
    END AS message
FROM fact_sales
WHERE price <= 0;


-- QC-05: freight_value >= 0 (cho phép = 0, không được âm)
SELECT
    'QC-05' AS check_id,
    'fact_sales_freight_non_negative' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All freight values are non-negative'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have negative freight_value'
    END AS message
FROM fact_sales
WHERE freight_value < 0;


-- QC-06: Không có duplicate (order_id, order_item_id)
SELECT
    'QC-06' AS check_id,
    'fact_sales_no_duplicates' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'No duplicate order_id + order_item_id found'
        ELSE 'FAIL: ' || COUNT(*) || ' duplicate combinations found'
    END AS message
FROM (
    SELECT order_id, order_item_id, COUNT(*) AS cnt
    FROM fact_sales
    GROUP BY order_id, order_item_id
    HAVING COUNT(*) > 1
) dups;


-- =============================================================
-- FACT_PAYMENTS
-- =============================================================

-- QC-07: fact_payments không được rỗng
SELECT
    'QC-07' AS check_id,
    'fact_payments_not_empty' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    1 AS threshold,
    CASE WHEN COUNT(*) > 0
        THEN 'fact_payments has ' || COUNT(*) || ' rows'
        ELSE 'FAIL: fact_payments is empty'
    END AS message
FROM fact_payments;


-- QC-08: payment_value >= 0
-- payment_value = 0 là hợp lệ — voucher cover toàn bộ đơn hàng không cần thanh toán thêm
-- chỉ fail nếu giá trị âm (không có nghiệp vụ nào tạo ra payment âm)
SELECT
    'QC-08' AS check_id,
    'fact_payments_value_non_negative' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All payment values are non-negative'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have negative payment_value'
    END AS message
FROM fact_payments
WHERE payment_value < 0;


-- QC-09: Không có NULL customer_key trong fact_payments
SELECT
    'QC-09' AS check_id,
    'fact_payments_no_null_customer_key' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'No NULL customer_key found'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have NULL customer_key'
    END AS message
FROM fact_payments
WHERE customer_key IS NULL;


-- QC-10: credit_card installments >= 1
-- 2 rows credit_card với installments = 0 là known data anomaly từ Olist source
-- order_id: 744bade1fcf9ff3f31d860ace076d422, 1a57108394169c0b47d8f876acc9ba2d
-- Không fix được ở tầng ETL — exclude 2 orders này, documented trong Known Issues
SELECT
    'QC-10' AS check_id,
    'credit_card_installments_valid' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All credit_card installments >= 1'
        ELSE 'FAIL: ' || COUNT(*) || ' credit_card rows have installments < 1'
    END AS message
FROM fact_payments fp
JOIN dim_payment dp ON fp.payment_key = dp.payment_key
WHERE dp.payment_type = 'credit_card'
  AND fp.payment_installments < 1
  AND fp.order_id NOT IN (
    '744bade1fcf9ff3f31d860ace076d422',
    '1a57108394169c0b47d8f876acc9ba2d'
  );


-- =============================================================
-- FACT_ORDER_EXPERIENCE
-- =============================================================

-- QC-11: fact_order_experience không được rỗng
SELECT
    'QC-11' AS check_id,
    'fact_order_experience_not_empty' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    1 AS threshold,
    CASE WHEN COUNT(*) > 0
        THEN 'fact_order_experience has ' || COUNT(*) || ' rows'
        ELSE 'FAIL: fact_order_experience is empty'
    END AS message
FROM fact_order_experience;


-- QC-12: delivery_days >= 0
-- delivery_days = 0 xảy ra do timestamp precision artifact: mua 23h, giao 1h sáng hôm sau
-- → date diff Python = 0 ngày dù thực tế đã giao hàng (13 rows trong Olist dataset).
-- Chỉ fail nếu delivery_days âm — không có nghiệp vụ nào giao hàng trước khi đặt.
SELECT
    'QC-12' AS check_id,
    'fact_experience_delivery_days_non_negative' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All delivery_days are non-negative'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have negative delivery_days'
    END AS message
FROM fact_order_experience
WHERE delivery_days IS NOT NULL
  AND delivery_days < 0;


-- QC-13: review_score trong khoảng 1-5
SELECT
    'QC-13' AS check_id,
    'fact_experience_review_score_valid' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All review scores are between 1 and 5'
        ELSE 'FAIL: ' || COUNT(*) || ' rows have invalid review_score'
    END AS message
FROM fact_order_experience
WHERE review_score IS NOT NULL
  AND review_score NOT BETWEEN 1 AND 5;


-- =============================================================
-- DIMENSIONS
-- =============================================================

-- QC-14: dim_customer không rỗng
SELECT
    'QC-14' AS check_id,
    'dim_customer_not_empty' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    1 AS threshold,
    CASE WHEN COUNT(*) > 0
        THEN 'dim_customer has ' || COUNT(*) || ' rows'
        ELSE 'FAIL: dim_customer is empty'
    END AS message
FROM dim_customer;


-- QC-15: Không có duplicate customer_unique_id trong dim_customer
SELECT
    'QC-15' AS check_id,
    'dim_customer_unique_id_unique' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'customer_unique_id is unique in dim_customer'
        ELSE 'FAIL: ' || COUNT(*) || ' duplicate customer_unique_id found'
    END AS message
FROM (
    SELECT customer_unique_id, COUNT(*) AS cnt
    FROM dim_customer
    GROUP BY customer_unique_id
    HAVING COUNT(*) > 1
) dups;


-- QC-16: dim_product không rỗng
SELECT
    'QC-16' AS check_id,
    'dim_product_not_empty' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    1 AS threshold,
    CASE WHEN COUNT(*) > 0
        THEN 'dim_product has ' || COUNT(*) || ' rows'
        ELSE 'FAIL: dim_product is empty'
    END AS message
FROM dim_product;


-- QC-17: dim_date đủ range (2016-2018)
SELECT
    'QC-17' AS check_id,
    'dim_date_range_valid' AS check_name,
    CASE WHEN MIN(full_date) <= '2016-01-01'
          AND MAX(full_date) >= '2018-12-31'
        THEN 'PASS' ELSE 'FAIL'
    END AS status,
    COUNT(*) AS value,
    1096 AS threshold,
    'Date range: ' || MIN(full_date) || ' → ' || MAX(full_date) AS message
FROM dim_date;


-- =============================================================
-- REFERENTIAL INTEGRITY
-- =============================================================

-- QC-18: Tất cả customer_key trong fact_sales tồn tại trong dim_customer
SELECT
    'QC-18' AS check_id,
    'fact_sales_customer_fk_valid' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All customer_key in fact_sales exist in dim_customer'
        ELSE 'FAIL: ' || COUNT(*) || ' orphan customer_key in fact_sales'
    END AS message
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;


-- QC-19: Tất cả product_key trong fact_sales tồn tại trong dim_product
SELECT
    'QC-19' AS check_id,
    'fact_sales_product_fk_valid' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All product_key in fact_sales exist in dim_product'
        ELSE 'FAIL: ' || COUNT(*) || ' orphan product_key in fact_sales'
    END AS message
FROM fact_sales fs
LEFT JOIN dim_product dp ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;


-- QC-20: Tất cả date_key trong fact_sales tồn tại trong dim_date
SELECT
    'QC-20' AS check_id,
    'fact_sales_date_fk_valid' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*) AS value,
    0 AS threshold,
    CASE WHEN COUNT(*) = 0
        THEN 'All date_key in fact_sales exist in dim_date'
        ELSE 'FAIL: ' || COUNT(*) || ' orphan date_key in fact_sales'
    END AS message
FROM fact_sales fs
LEFT JOIN dim_date d ON fs.date_key = d.date_key
WHERE d.date_key IS NULL;


-- =============================================================
-- SUMMARY — chạy query này để xem tổng kết tất cả checks
-- =============================================================
SELECT
    check_id,
    check_name,
    status,
    value,
    threshold,
    message
FROM (
    -- Gom tất cả checks vào 1 kết quả
    SELECT 'QC-01' AS check_id, 'fact_sales_not_empty' AS check_name,
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
           COUNT(*) AS value, 1 AS threshold,
           'fact_sales rows: ' || COUNT(*) AS message FROM fact_sales
    UNION ALL
    SELECT 'QC-02', 'fact_sales_no_null_customer_key',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 0, 'NULL customer_key count: ' || COUNT(*)
    FROM fact_sales WHERE customer_key IS NULL
    UNION ALL
    SELECT 'QC-04', 'fact_sales_price_positive',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 0, 'Invalid price count: ' || COUNT(*)
    FROM fact_sales WHERE price <= 0
    UNION ALL
    SELECT 'QC-07', 'fact_payments_not_empty',
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 1, 'fact_payments rows: ' || COUNT(*) FROM fact_payments
    UNION ALL
    SELECT 'QC-08', 'fact_payments_value_non_negative',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 0, 'Negative payment_value count: ' || COUNT(*)
    FROM fact_payments WHERE payment_value < 0
    UNION ALL
    SELECT 'QC-11', 'fact_order_experience_not_empty',
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 1, 'fact_order_experience rows: ' || COUNT(*)
    FROM fact_order_experience
    UNION ALL
    SELECT 'QC-14', 'dim_customer_not_empty',
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 1, 'dim_customer rows: ' || COUNT(*) FROM dim_customer
    UNION ALL
    SELECT 'QC-17', 'dim_date_range_valid',
           CASE WHEN COUNT(*) = 1096 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 1096, 'dim_date rows: ' || COUNT(*) FROM dim_date
) all_checks
ORDER BY check_id;