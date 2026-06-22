# Business Questions

## Business Objective

The objective of this project is to build a Data Warehouse and Business Intelligence platform for the Olist Brazilian E-Commerce dataset. The system aims to provide insights into sales performance, customer behavior, seller effectiveness, payment preferences, logistics operations, and customer satisfaction.

The following business questions will guide the data modeling process, ETL pipeline design, KPI definition, and dashboard development.

---

# I. Revenue & Sales Analytics

### 1. How has revenue evolved over time?

**Business Goal:**
Analyze monthly and quarterly revenue trends to identify seasonality, growth patterns, and business performance over time.

**Key Metrics:**

* Total Revenue
* Monthly Revenue
* Quarterly Revenue
* Revenue Growth Rate

**Related Tables:**

* orders
* order_items
* order_payments

---

### 2. Which product categories generate the highest revenue — and which are growing or declining?

**Business Goal:**
Identify the most profitable product categories and evaluate category performance over time.

**Key Metrics:**

* Revenue by Category
* Category Revenue Growth
* Category Revenue Contribution (%)

**Related Tables:**

* products
* category_translation
* order_items
* orders

---

### 3. What is the Average Order Value (AOV), and how does it vary by region?

**Business Goal:**
Understand customer spending behavior and compare purchasing power across geographic regions.

**Key Metrics:**

* Average Order Value (AOV)
* Revenue by State
* Orders by State

**Related Tables:**

* customers
* orders
* order_items

---

# II. Customer Analytics

### 4. Who are the most valuable customers based on lifetime spending?

**Business Goal:**
Identify high-value customers who contribute the most revenue to the marketplace.

**Key Metrics:**

* Customer Lifetime Value (CLV)
* Total Customer Revenue
* Number of Orders per Customer

**Related Tables:**

* customers
* orders
* order_items

---

### 5. What is the repeat purchase rate, and how long does it take customers to place a second order?

**Business Goal:**
Measure customer loyalty and understand purchasing frequency.

**Key Metrics:**

* Repeat Purchase Rate
* Average Days Between Purchases
* Customer Retention Rate

**Related Tables:**

* customers
* orders

---

### 6. What customer segments can be identified using RFM analysis?

**Business Goal:**
Segment customers based on Recency, Frequency, and Monetary value to support targeted marketing strategies.

**Key Metrics:**

* Recency
* Frequency
* Monetary Value
* RFM Score

**Related Tables:**

* customers
* orders
* order_items

---

# III. Seller & Marketplace Analytics

### 7. Which sellers consistently perform best on both revenue and customer satisfaction?

**Business Goal:**
Evaluate seller performance using both financial and customer experience metrics.

**Key Metrics:**

* Seller Revenue
* Average Review Score
* Number of Orders

**Related Tables:**

* sellers
* order_items
* orders
* order_reviews

---

### 8. Which sellers experience the highest delivery delays?

**Business Goal:**
Identify operational issues that may impact customer satisfaction and delivery performance.

**Key Metrics:**

* Average Delivery Delay
* Late Delivery Rate
* Orders Delivered Late

**Related Tables:**

* sellers
* order_items
* orders

---

### 9. Which customer states generate the highest revenue?

**Business Goal:**
Identify key geographic markets and revenue-driving regions.

**Key Metrics:**

* Revenue by State
* Orders by State
* Customers by State

**Related Tables:**

* customers
* orders
* order_items

---

# IV. Payment Analytics

### 10. What payment methods are most commonly used, and how do they affect order value?

**Business Goal:**
Understand customer payment preferences and evaluate the relationship between payment methods and purchasing behavior.

**Key Metrics:**

* Payment Method Distribution
* Revenue by Payment Type
* Average Order Value by Payment Type

**Related Tables:**

* order_payments
* orders

---

### 11. Does paying in installments lead to larger purchases?

**Business Goal:**
Determine whether installment payments encourage customers to spend more.

**Key Metrics:**

* Average Order Value by Installment Count
* Revenue by Installment Group
* Payment Installment Distribution

**Related Tables:**

* order_payments
* orders

---

# V. Logistics & Customer Experience Analytics

### 12. What is the average delivery time across different regions/states?

**Business Goal:**
Measure logistics efficiency and compare delivery performance across locations.

**Key Metrics:**

* Average Delivery Time
* Delivery Time by State

**Related Tables:**

* orders
* customers

---

### 13. What percentage of orders are delivered later than the estimated date?

**Business Goal:**
Monitor logistics performance and service-level compliance.

**Key Metrics:**

* Late Delivery Rate
* On-Time Delivery Rate

**Related Tables:**

* orders

---

### 14. Which states experience the longest delivery delays?

**Business Goal:**
Identify geographic regions with logistics challenges.

**Key Metrics:**

* Average Delivery Delay by State
* Late Orders by State

**Related Tables:**

* orders
* customers

---

### 15. How does delivery performance affect customer review scores?

**Business Goal:**
Evaluate the impact of logistics quality on customer satisfaction.

**Key Metrics:**

* Average Review Score
* Average Delivery Time
* Delivery Delay vs Review Score

**Related Tables:**

* orders
* order_reviews

---

### 16. Are delayed orders more likely to receive negative reviews?

**Business Goal:**
Determine whether delivery delays contribute to poor customer experiences.

**Key Metrics:**

* Negative Review Rate
* Late Delivery Rate
* Review Score Distribution

**Related Tables:**

* orders
* order_reviews

---

### 17. Which product categories receive the highest and lowest ratings?

**Business Goal:**
Identify product categories that consistently satisfy or disappoint customers.

**Key Metrics:**

* Average Review Score by Category
* Rating Distribution
* Top-Rated Categories
* Lowest-Rated Categories

**Related Tables:**

* products
* category_translation
* order_items
* order_reviews

---

# Expected Business Outcomes

By answering these business questions, the Data Warehouse will support:

* Revenue and sales performance analysis
* Customer segmentation and retention analysis
* Seller performance evaluation
* Payment behavior analysis
* Logistics and delivery monitoring
* Customer satisfaction measurement
* Executive-level decision making through Power BI dashboards
