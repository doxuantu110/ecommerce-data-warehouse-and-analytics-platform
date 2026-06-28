-- =============================================================
-- FILE: create_dw.sql
-- PROJECT: E-commerce Data Warehouse (Olist Brazilian Dataset)
-- SCHEMA: Star Schema — Kimball Methodology
--
-- DIMENSIONS : DIM_CUSTOMER, DIM_PRODUCT, DIM_SELLER,
--              DIM_DATE, DIM_PAYMENT
-- FACT TABLES: FACT_SALES, FACT_PAYMENTS, FACT_ORDER_EXPERIENCE
--
-- NOTE: This file is mounted to /docker-entrypoint-initdb.d/
--       and runs automatically on first container startup.
-- =============================================================


-- =============================================================
-- SCHEMA
-- =============================================================
CREATE SCHEMA IF NOT EXISTS dw;

SET search_path TO dw;


-- =============================================================
-- DIMENSIONS
-- =============================================================

-- -------------------------------------------------------------
-- DIM_CUSTOMER
-- Source  : olist_customers_dataset.csv
-- Key note: customer_unique_id is the true person identifier.
--           customer_id is a per-order surrogate in Olist and
--           must NOT be used as the primary key here.
-- Supports: BQ #3, #4, #5, #6, #9, #12, #14
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        SERIAL          PRIMARY KEY,
    customer_unique_id  VARCHAR(50)     NOT NULL UNIQUE,
    customer_state      CHAR(2)         NOT NULL,
    customer_city       VARCHAR(100)    NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_unique_id ON dim_customer (customer_unique_id);
CREATE INDEX idx_dim_customer_state     ON dim_customer (customer_state);


-- -------------------------------------------------------------
-- DIM_PRODUCT
-- Source  : olist_products_dataset.csv
--           + product_category_name_translation.csv
-- Supports: BQ #2, #17
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_key             SERIAL          PRIMARY KEY,
    product_id              VARCHAR(50)     NOT NULL UNIQUE,
    category_name_english   VARCHAR(100),
    product_name_length     INT,
    product_description_length INT,
    photos_qty              INT,
    weight_g                NUMERIC(10, 2),
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_id       ON dim_product (product_id);
CREATE INDEX idx_dim_product_category ON dim_product (category_name_english);


-- -------------------------------------------------------------
-- DIM_SELLER
-- Source  : olist_sellers_dataset.csv
-- Supports: BQ #7, #8
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_key      SERIAL          PRIMARY KEY,
    seller_id       VARCHAR(50)     NOT NULL UNIQUE,
    seller_state    CHAR(2)         NOT NULL,
    seller_city     VARCHAR(100)    NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_seller_id    ON dim_seller (seller_id);
CREATE INDEX idx_dim_seller_state ON dim_seller (seller_state);


-- -------------------------------------------------------------
-- DIM_DATE
-- Source  : generated (not from Olist CSV)
--           Covers the full Olist date range: 2016-01-01 → 2018-12-31
-- Supports: BQ #1, #2, #6
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT             PRIMARY KEY,  -- format: YYYYMMDD
    full_date       DATE            NOT NULL UNIQUE,
    year            INT             NOT NULL,
    quarter         INT             NOT NULL,     -- 1..4
    month           INT             NOT NULL,     -- 1..12
    month_name      VARCHAR(10)     NOT NULL,     -- January..December
    week_of_year    INT             NOT NULL,     -- 1..53
    day_of_month    INT             NOT NULL,     -- 1..31
    day_of_week     INT             NOT NULL,     -- 1=Monday..7=Sunday
    day_name        VARCHAR(10)     NOT NULL,     -- Monday..Sunday
    is_weekend      BOOLEAN         NOT NULL
);

CREATE INDEX idx_dim_date_full_date ON dim_date (full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date (year, month);

-- Populate dim_date for range 2016-01-01 → 2018-12-31
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week_of_year, day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                 AS date_key,
    d::DATE                                      AS full_date,
    EXTRACT(YEAR    FROM d)::INT                 AS year,
    EXTRACT(QUARTER FROM d)::INT                 AS quarter,
    EXTRACT(MONTH   FROM d)::INT                 AS month,
    TO_CHAR(d, 'Month')                          AS month_name,
    EXTRACT(WEEK    FROM d)::INT                 AS week_of_year,
    EXTRACT(DAY     FROM d)::INT                 AS day_of_month,
    EXTRACT(ISODOW  FROM d)::INT                 AS day_of_week,
    TO_CHAR(d, 'Day')                            AS day_name,
    EXTRACT(ISODOW  FROM d) IN (6, 7)           AS is_weekend
FROM GENERATE_SERIES(
    '2016-01-01'::DATE,
    '2018-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT DO NOTHING;


-- -------------------------------------------------------------
-- DIM_PAYMENT
-- Source  : olist_order_payments_dataset.csv (distinct payment_type)
-- Supports: BQ #10, #11
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_key         SERIAL          PRIMARY KEY,
    payment_type        VARCHAR(30)     NOT NULL UNIQUE,
    installment_group   VARCHAR(20)     NOT NULL,
    -- '1 installment' | '2-3' | '4-6' | '7+'
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- Seed known payment types from Olist
INSERT INTO dim_payment (payment_type, installment_group) VALUES
    ('credit_card', '1 installment'),
    ('boleto',      '1 installment'),
    ('voucher',     '1 installment'),
    ('debit_card',  '1 installment'),
    ('not_defined', '1 installment')
ON CONFLICT DO NOTHING;


-- =============================================================
-- FACT TABLES
-- =============================================================

-- -------------------------------------------------------------
-- FACT_SALES
-- Grain   : 1 product line item in 1 order
-- Measures: price, freight_value
-- Supports: BQ #2, #4 (via customer_key), #7, #8, #17
--
-- NOTE: payment_value is NOT stored here to avoid fan-out join.
--       An order with N items would duplicate payment_value N times,
--       causing SUM(payment_value) to double-count. See FACT_PAYMENTS.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key       SERIAL          PRIMARY KEY,

    -- Degenerate dimensions (traceability to source)
    order_id        VARCHAR(50)     NOT NULL,
    order_item_id   INT             NOT NULL,

    -- Foreign keys
    customer_key    INT             NOT NULL REFERENCES dim_customer (customer_key),
    product_key     INT             NOT NULL REFERENCES dim_product  (product_key),
    seller_key      INT             NOT NULL REFERENCES dim_seller   (seller_key),
    date_key        INT             NOT NULL REFERENCES dim_date     (date_key),

    -- Measures
    price           NUMERIC(10, 2)  NOT NULL,
    freight_value   NUMERIC(10, 2)  NOT NULL,

    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_fact_sales UNIQUE (order_id, order_item_id)
);

CREATE INDEX idx_fact_sales_order_id     ON fact_sales (order_id);
CREATE INDEX idx_fact_sales_customer_key ON fact_sales (customer_key);
CREATE INDEX idx_fact_sales_product_key  ON fact_sales (product_key);
CREATE INDEX idx_fact_sales_seller_key   ON fact_sales (seller_key);
CREATE INDEX idx_fact_sales_date_key     ON fact_sales (date_key);


-- -------------------------------------------------------------
-- FACT_PAYMENTS
-- Grain   : 1 payment record per order
--           (1 order can have multiple rows — split payment)
-- Measures: payment_value, payment_installments
-- Supports: BQ #1, #3, #4, #5, #6, #9, #10, #11
--
-- NOTE: customer_key is included here (conformed dimension) so
--       CLV and RFM queries can go directly to FACT_PAYMENTS
--       without bridging through FACT_SALES.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_payments (
    payment_fact_key        SERIAL          PRIMARY KEY,

    -- Degenerate dimension
    order_id                VARCHAR(50)     NOT NULL,
    payment_sequential      INT             NOT NULL,

    -- Foreign keys
    customer_key            INT             NOT NULL REFERENCES dim_customer (customer_key),
    date_key                INT             NOT NULL REFERENCES dim_date     (date_key),
    payment_key             INT             NOT NULL REFERENCES dim_payment  (payment_key),

    -- Measures
    payment_value           NUMERIC(10, 2)  NOT NULL,
    payment_installments    INT             NOT NULL DEFAULT 1,

    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_fact_payments UNIQUE (order_id, payment_sequential)
);

CREATE INDEX idx_fact_payments_order_id      ON fact_payments (order_id);
CREATE INDEX idx_fact_payments_customer_key  ON fact_payments (customer_key);
CREATE INDEX idx_fact_payments_date_key      ON fact_payments (date_key);
CREATE INDEX idx_fact_payments_payment_key   ON fact_payments (payment_key);


-- -------------------------------------------------------------
-- FACT_ORDER_EXPERIENCE
-- Grain   : 1 order (after delivery + review)
-- Measures: delivery_days, delay_days, review_score
-- Supports: BQ #8, #12, #13, #14, #15, #16
--
-- NOTE: Only delivered orders are loaded (order_status = 'delivered').
--       review_score can be NULL when customer did not leave a review.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_order_experience (
    experience_fact_key     SERIAL          PRIMARY KEY,

    -- Degenerate dimension
    order_id                VARCHAR(50)     NOT NULL UNIQUE,

    -- Foreign keys
    customer_key            INT             NOT NULL REFERENCES dim_customer (customer_key),
    date_key                INT             NOT NULL REFERENCES dim_date     (date_key),

    -- Measures
    delivery_days           INT,            -- actual days from purchase to delivery
    delay_days              INT,            -- positive = late, negative = early
    review_score            SMALLINT        CHECK (review_score BETWEEN 1 AND 5),

    -- Attributes
    is_late_delivery        BOOLEAN         NOT NULL DEFAULT FALSE,

    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_experience_order_id     ON fact_order_experience (order_id);
CREATE INDEX idx_fact_experience_customer_key ON fact_order_experience (customer_key);
CREATE INDEX idx_fact_experience_date_key     ON fact_order_experience (date_key);
CREATE INDEX idx_fact_experience_is_late      ON fact_order_experience (is_late_delivery);