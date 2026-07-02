-- =============================================================
-- FILE: analytics_queries.sql
-- PROJECT: E-commerce Data Warehouse (Olist Brazilian Dataset)
-- PURPOSE: Analytics queries covering all 17 Business Questions
--
-- Schema: dw
-- Fact tables  : fact_sales, fact_payments, fact_order_experience
-- Dim tables   : dim_customer, dim_product, dim_seller, dim_date, dim_payment
-- =============================================================

SET search_path TO dw;


-- =============================================================
-- I. REVENUE & SALES ANALYTICS
-- =============================================================

-- -------------------------------------------------------------
-- BQ #1: How has revenue evolved over time?
-- Grain: monthly & quarterly revenue trend
-- Source: fact_payments → dim_date
-- -------------------------------------------------------------
-- Monthly Revenue Trend
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS total_revenue,
    COUNT(DISTINCT fp.order_id)                AS total_orders,
    ROUND(AVG(fp.payment_value)::NUMERIC, 2)   AS avg_order_value,
    ROUND(
        (SUM(fp.payment_value) - LAG(SUM(fp.payment_value))
            OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(fp.payment_value))
            OVER (ORDER BY d.year, d.month), 0) * 100
    , 2)                                        AS mom_growth_pct
FROM fact_payments fp
JOIN dim_date d ON fp.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- Quarterly Revenue Trend
SELECT
    d.year,
    d.quarter,
    CONCAT('Q', d.quarter, '-', d.year)        AS quarter_label,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS total_revenue,
    COUNT(DISTINCT fp.order_id)                AS total_orders
FROM fact_payments fp
JOIN dim_date d ON fp.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;


-- -------------------------------------------------------------
-- BQ #2: Which product categories generate the highest revenue
--        — and which are growing or declining?
-- Source: fact_sales → dim_product, dim_date
-- NOTE: dung fact_sales.price (khong phai payment_value) vi
--       chi fact_sales moi co product_key de join dim_product
-- -------------------------------------------------------------
-- Category Revenue Ranking
SELECT
    dp.category_name_english,
    ROUND(SUM(fs.price)::NUMERIC, 2)           AS total_revenue,
    COUNT(DISTINCT fs.order_id)                AS total_orders,
    SUM(1)                                     AS units_sold,
    ROUND(
        SUM(fs.price) * 100.0
        / SUM(SUM(fs.price)) OVER ()
    , 2)                                        AS revenue_share_pct
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.category_name_english
ORDER BY total_revenue DESC
LIMIT 20;


-- Category YoY Growth (2017 vs 2018)
SELECT
    dp.category_name_english,
    ROUND(SUM(CASE WHEN d.year = 2017 THEN fs.price ELSE 0 END)::NUMERIC, 2) AS revenue_2017,
    ROUND(SUM(CASE WHEN d.year = 2018 THEN fs.price ELSE 0 END)::NUMERIC, 2) AS revenue_2018,
    ROUND(
        (SUM(CASE WHEN d.year = 2018 THEN fs.price ELSE 0 END)
        - SUM(CASE WHEN d.year = 2017 THEN fs.price ELSE 0 END))
        / NULLIF(SUM(CASE WHEN d.year = 2017 THEN fs.price ELSE 0 END), 0) * 100
    , 2)                                                                       AS yoy_growth_pct
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date    d  ON fs.date_key    = d.date_key
GROUP BY dp.category_name_english
HAVING SUM(CASE WHEN d.year = 2017 THEN fs.price ELSE 0 END) > 0
ORDER BY yoy_growth_pct DESC;


-- -------------------------------------------------------------
-- BQ #3: What is the Average Order Value (AOV),
--        and how does it vary by region?
-- Source: fact_payments → dim_customer
-- -------------------------------------------------------------
SELECT
    dc.customer_state,
    COUNT(DISTINCT fp.order_id)                AS total_orders,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS total_revenue,
    ROUND(AVG(fp.payment_value)::NUMERIC, 2)   AS avg_order_value
FROM fact_payments fp
JOIN dim_customer dc ON fp.customer_key = dc.customer_key
GROUP BY dc.customer_state
ORDER BY avg_order_value DESC;


-- =============================================================
-- II. CUSTOMER ANALYTICS
-- =============================================================

-- -------------------------------------------------------------
-- BQ #4: Who are the most valuable customers
--        based on lifetime spending?
-- Source: fact_payments → dim_customer
-- NOTE: customer_unique_id is the true person-level identifier
-- -------------------------------------------------------------
SELECT
    dc.customer_unique_id,
    dc.customer_state,
    dc.customer_city,
    COUNT(DISTINCT fp.order_id)                AS total_orders,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS lifetime_value,
    ROUND(AVG(fp.payment_value)::NUMERIC, 2)   AS avg_order_value
FROM fact_payments fp
JOIN dim_customer dc ON fp.customer_key = dc.customer_key
GROUP BY dc.customer_unique_id, dc.customer_state, dc.customer_city
ORDER BY lifetime_value DESC
LIMIT 50;


-- -------------------------------------------------------------
-- BQ #5: What is the repeat purchase rate,
--        and how long does it take customers to place a second order?
-- Source: fact_payments → dim_customer
-- -------------------------------------------------------------
-- Repeat Purchase Rate
WITH customer_orders AS (
    SELECT
        fp.customer_key,
        COUNT(DISTINCT fp.order_id) AS order_count
    FROM fact_payments fp
    GROUP BY fp.customer_key
)
SELECT
    COUNT(*)                                                             AS total_customers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END)                   AS repeat_customers,
    ROUND(
        SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                                                 AS repeat_rate_pct,
    ROUND(AVG(order_count), 2)                                          AS avg_orders_per_customer
FROM customer_orders;


-- Days Between First and Second Order (for repeat customers)
WITH customer_order_dates AS (
    SELECT
        fp.customer_key,
        d.full_date AS order_date,
        ROW_NUMBER() OVER (PARTITION BY fp.customer_key ORDER BY d.full_date) AS order_rank
    FROM fact_payments fp
    JOIN dim_date d ON fp.date_key = d.date_key
)
SELECT
    ROUND(AVG(second_order.order_date - first_order.order_date), 0) AS avg_days_to_second_order,
    MIN(second_order.order_date - first_order.order_date)           AS min_days,
    MAX(second_order.order_date - first_order.order_date)           AS max_days
FROM customer_order_dates first_order
JOIN customer_order_dates second_order
    ON  first_order.customer_key = second_order.customer_key
    AND first_order.order_rank   = 1
    AND second_order.order_rank  = 2;


-- -------------------------------------------------------------
-- BQ #6: What customer segments can be identified using RFM?
-- R: days since last purchase | F: order count | M: total spend
-- Source: fact_payments → dim_date
-- -------------------------------------------------------------
WITH snapshot_date AS (
    SELECT MAX(d.full_date) AS snap
    FROM dim_date d
    JOIN fact_payments fp ON fp.date_key = d.date_key
),
rfm_raw AS (
    SELECT
        fp.customer_key,
        (SELECT snap FROM snapshot_date) - MAX(d.full_date) AS recency_days,
        COUNT(DISTINCT fp.order_id)                         AS frequency,
        ROUND(SUM(fp.payment_value)::NUMERIC, 2)            AS monetary
    FROM fact_payments fp
    JOIN dim_date d ON fp.date_key = d.date_key
    GROUP BY fp.customer_key
),
rfm_scored AS (
    SELECT
        customer_key, recency_days, frequency, monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
    FROM rfm_raw
),
rfm_segment AS (
    SELECT
        customer_key, recency_days, frequency, monetary,
        r_score, f_score, m_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent Customers'
            WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
            WHEN r_score = 1  AND f_score = 1  THEN 'Lost'
            ELSE 'Potential Loyalists'
        END AS segment
    FROM rfm_scored
)
SELECT
    segment,
    COUNT(*)                            AS customer_count,
    ROUND(AVG(recency_days), 0)         AS avg_recency_days,
    ROUND(AVG(frequency), 2)            AS avg_frequency,
    ROUND(AVG(monetary)::NUMERIC, 2)    AS avg_monetary
FROM rfm_segment
GROUP BY segment
ORDER BY customer_count DESC;


-- =============================================================
-- III. SELLER & MARKETPLACE ANALYTICS
-- =============================================================

-- -------------------------------------------------------------
-- BQ #7: Which sellers perform best on revenue + satisfaction?
-- Source: fact_sales → dim_seller, fact_order_experience
-- -------------------------------------------------------------
SELECT
    ds.seller_id,
    ds.seller_state,
    ds.seller_city,
    COUNT(DISTINCT fs.order_id)             AS total_orders,
    ROUND(SUM(fs.price)::NUMERIC, 2)        AS total_revenue,
    ROUND(AVG(foe.review_score)::NUMERIC, 2) AS avg_review_score
FROM fact_sales fs
JOIN dim_seller            ds  ON fs.seller_key = ds.seller_key
JOIN fact_order_experience foe ON fs.order_id   = foe.order_id
WHERE foe.review_score IS NOT NULL
GROUP BY ds.seller_id, ds.seller_state, ds.seller_city
HAVING COUNT(DISTINCT fs.order_id) >= 10
ORDER BY avg_review_score DESC, total_revenue DESC
LIMIT 20;


-- -------------------------------------------------------------
-- BQ #8: Which sellers experience the highest delivery delays?
-- Source: fact_sales → dim_seller, fact_order_experience
-- -------------------------------------------------------------
SELECT
    ds.seller_id,
    ds.seller_state,
    COUNT(DISTINCT fs.order_id)                                          AS total_orders,
    SUM(CASE WHEN foe.is_late_delivery THEN 1 ELSE 0 END)               AS late_orders,
    ROUND(
        SUM(CASE WHEN foe.is_late_delivery THEN 1 ELSE 0 END) * 100.0
        / COUNT(DISTINCT fs.order_id)
    , 2)                                                                  AS late_delivery_rate_pct,
    ROUND(AVG(foe.delay_days)::NUMERIC, 1)                               AS avg_delay_days
FROM fact_sales fs
JOIN dim_seller            ds  ON fs.seller_key = ds.seller_key
JOIN fact_order_experience foe ON fs.order_id   = foe.order_id
GROUP BY ds.seller_id, ds.seller_state
HAVING COUNT(DISTINCT fs.order_id) >= 10
ORDER BY late_delivery_rate_pct DESC
LIMIT 20;


-- -------------------------------------------------------------
-- BQ #9: Which customer states generate the highest revenue?
-- Source: fact_payments → dim_customer
-- -------------------------------------------------------------
SELECT
    dc.customer_state,
    COUNT(DISTINCT fp.customer_key)            AS unique_customers,
    COUNT(DISTINCT fp.order_id)                AS total_orders,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS total_revenue,
    ROUND(AVG(fp.payment_value)::NUMERIC, 2)   AS avg_order_value
FROM fact_payments fp
JOIN dim_customer dc ON fp.customer_key = dc.customer_key
GROUP BY dc.customer_state
ORDER BY total_revenue DESC;


-- =============================================================
-- IV. PAYMENT ANALYTICS
-- =============================================================

-- -------------------------------------------------------------
-- BQ #10: Payment method distribution and AOV by payment type
-- Source: fact_payments → dim_payment
-- -------------------------------------------------------------
SELECT
    dp.payment_type,
    COUNT(DISTINCT fp.order_id)                AS total_orders,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS total_revenue,
    ROUND(AVG(fp.payment_value)::NUMERIC, 2)   AS avg_order_value,
    ROUND(
        COUNT(DISTINCT fp.order_id) * 100.0
        / SUM(COUNT(DISTINCT fp.order_id)) OVER ()
    , 2)                                        AS order_share_pct
FROM fact_payments fp
JOIN dim_payment dp ON fp.payment_key = dp.payment_key
GROUP BY dp.payment_type
ORDER BY total_orders DESC;


-- -------------------------------------------------------------
-- BQ #11: Does paying in installments lead to larger purchases?
-- Source: fact_payments (credit_card only)
-- -------------------------------------------------------------
SELECT
    fp.payment_installments,
    COUNT(DISTINCT fp.order_id)                AS total_orders,
    ROUND(AVG(fp.payment_value)::NUMERIC, 2)   AS avg_order_value,
    ROUND(SUM(fp.payment_value)::NUMERIC, 2)   AS total_revenue
FROM fact_payments fp
JOIN dim_payment dp ON fp.payment_key = dp.payment_key
WHERE dp.payment_type = 'credit_card'
GROUP BY fp.payment_installments
ORDER BY fp.payment_installments;


-- =============================================================
-- V. LOGISTICS & CUSTOMER EXPERIENCE ANALYTICS
-- =============================================================

-- -------------------------------------------------------------
-- BQ #12: Average delivery time across regions
-- Source: fact_order_experience → dim_customer
-- -------------------------------------------------------------
SELECT
    dc.customer_state,
    COUNT(*)                                   AS delivered_orders,
    ROUND(AVG(foe.delivery_days)::NUMERIC, 1)  AS avg_delivery_days,
    MIN(foe.delivery_days)                     AS min_delivery_days,
    MAX(foe.delivery_days)                     AS max_delivery_days
FROM fact_order_experience foe
JOIN dim_customer dc ON foe.customer_key = dc.customer_key
WHERE foe.delivery_days IS NOT NULL
GROUP BY dc.customer_state
ORDER BY avg_delivery_days DESC;


-- -------------------------------------------------------------
-- BQ #13: Late delivery rate (KPI)
-- Source: fact_order_experience
-- -------------------------------------------------------------
SELECT
    COUNT(*)                                                         AS total_delivered,
    SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)               AS late_orders,
    SUM(CASE WHEN NOT is_late_delivery THEN 1 ELSE 0 END)           AS on_time_orders,
    ROUND(
        SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                                             AS late_delivery_rate_pct,
    ROUND(
        SUM(CASE WHEN NOT is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                                             AS on_time_rate_pct
FROM fact_order_experience;


-- -------------------------------------------------------------
-- BQ #14: Which states experience the longest delivery delays?
-- Source: fact_order_experience → dim_customer
-- -------------------------------------------------------------
SELECT
    dc.customer_state,
    COUNT(*)                                                                   AS total_orders,
    SUM(CASE WHEN foe.is_late_delivery THEN 1 ELSE 0 END)                     AS late_orders,
    ROUND(
        SUM(CASE WHEN foe.is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                                                        AS late_rate_pct,
    ROUND(
        AVG(CASE WHEN foe.delay_days > 0 THEN foe.delay_days END)::NUMERIC, 1
    )                                                                           AS avg_delay_days_when_late
FROM fact_order_experience foe
JOIN dim_customer dc ON foe.customer_key = dc.customer_key
GROUP BY dc.customer_state
ORDER BY late_rate_pct DESC;


-- -------------------------------------------------------------
-- BQ #15: How does delivery performance affect review scores?
-- Source: fact_order_experience
-- -------------------------------------------------------------
SELECT
    CASE
        WHEN delivery_days <= 7  THEN '1. <= 7 days'
        WHEN delivery_days <= 14 THEN '2. 8-14 days'
        WHEN delivery_days <= 21 THEN '3. 15-21 days'
        WHEN delivery_days <= 30 THEN '4. 22-30 days'
        ELSE                          '5. > 30 days'
    END                                        AS delivery_bucket,
    COUNT(*)                                   AS order_count,
    ROUND(AVG(review_score)::NUMERIC, 2)       AS avg_review_score
FROM fact_order_experience
WHERE delivery_days IS NOT NULL
  AND review_score  IS NOT NULL
GROUP BY delivery_bucket
ORDER BY delivery_bucket;


-- -------------------------------------------------------------
-- BQ #16: Are delayed orders more likely to receive negative reviews?
-- Source: fact_order_experience
-- -------------------------------------------------------------
SELECT
    CASE WHEN is_late_delivery THEN 'Late' ELSE 'On Time' END      AS delivery_status,
    COUNT(*)                                                        AS total_orders,
    ROUND(AVG(review_score)::NUMERIC, 2)                           AS avg_review_score,
    SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END)             AS negative_reviews,
    ROUND(
        SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                                            AS negative_review_rate_pct,
    SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END)              AS five_star_reviews,
    ROUND(
        SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                                            AS five_star_rate_pct
FROM fact_order_experience
WHERE review_score IS NOT NULL
GROUP BY is_late_delivery
ORDER BY is_late_delivery;


-- -------------------------------------------------------------
-- BQ #17: Which product categories receive highest/lowest ratings?
-- Source: fact_sales → dim_product, fact_order_experience
-- -------------------------------------------------------------
SELECT
    dp.category_name_english,
    COUNT(*)                                   AS total_reviews,
    ROUND(AVG(foe.review_score)::NUMERIC, 2)   AS avg_review_score,
    SUM(CASE WHEN foe.review_score = 5 THEN 1 ELSE 0 END) AS five_star,
    SUM(CASE WHEN foe.review_score = 1 THEN 1 ELSE 0 END) AS one_star,
    ROUND(
        SUM(CASE WHEN foe.review_score >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    , 2)                                        AS positive_rate_pct
FROM fact_sales fs
JOIN dim_product           dp  ON fs.product_key = dp.product_key
JOIN fact_order_experience foe ON fs.order_id    = foe.order_id
WHERE foe.review_score IS NOT NULL
GROUP BY dp.category_name_english
HAVING COUNT(*) >= 50
ORDER BY avg_review_score DESC;


-- =============================================================
-- BONUS: Top Products & Sellers  (Ngan 7 deliverables)
-- =============================================================

-- Top 20 Products by Revenue
SELECT
    dp.product_id,
    dp.category_name_english,
    COUNT(DISTINCT fs.order_id)                AS orders,
    SUM(1)                                     AS units_sold,
    ROUND(SUM(fs.price)::NUMERIC, 2)           AS total_revenue,
    ROUND(AVG(fs.price)::NUMERIC, 2)           AS avg_price
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_id, dp.category_name_english
ORDER BY total_revenue DESC
LIMIT 20;


-- Top 20 Sellers by Revenue
SELECT
    ds.seller_id,
    ds.seller_state,
    COUNT(DISTINCT fs.order_id)                AS total_orders,
    SUM(1)                                     AS units_sold,
    ROUND(SUM(fs.price)::NUMERIC, 2)           AS total_revenue
FROM fact_sales fs
JOIN dim_seller ds ON fs.seller_key = ds.seller_key
GROUP BY ds.seller_id, ds.seller_state
ORDER BY total_revenue DESC
LIMIT 20;
