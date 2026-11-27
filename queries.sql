-- Run these in DB Browser for SQLite (free) or: sqlite3 output/olist.db < queries.sql
-- Part A checks the data after loading. Part B answers business questions.

-- ============ PART A: DATA CHECKS ============

-- A1. Row counts per table
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'sales_fact', COUNT(*) FROM sales_fact;

-- A2. Duplicate order items in the final table (should return no rows)
SELECT order_id, order_item_id, COUNT(*) AS copies
FROM sales_fact
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-- A3. Missing values in key columns
SELECT
    SUM(order_purchase_timestamp IS NULL)          AS missing_purchase_date,
    SUM(order_delivered_customer_date IS NULL)     AS missing_delivery_date,
    SUM(product_category_name_english IS NULL)     AS missing_category,
    SUM(customer_state IS NULL)                    AS missing_state
FROM sales_fact;

-- A4. Order statuses
SELECT order_status, COUNT(DISTINCT order_id) AS orders
FROM sales_fact
GROUP BY order_status
ORDER BY orders DESC;

-- ============ PART B: BUSINESS QUESTIONS ============

-- B1. Revenue and orders per month (delivered orders only)
SELECT
    purchase_month,
    ROUND(SUM(price), 2)         AS revenue,
    COUNT(DISTINCT order_id)     AS orders
FROM sales_fact
WHERE order_status = 'delivered'
GROUP BY purchase_month
ORDER BY purchase_month;

-- B2. Top 10 product categories by revenue
SELECT
    product_category_name_english AS category,
    ROUND(SUM(price), 2)          AS revenue,
    COUNT(*)                      AS items_sold
FROM sales_fact
WHERE order_status = 'delivered'
GROUP BY category
ORDER BY revenue DESC
LIMIT 10;

-- B3. Average delivery time and late-delivery rate by customer state
SELECT
    customer_state,
    ROUND(AVG(delivery_days), 1)       AS avg_delivery_days,
    ROUND(100.0 * AVG(is_late), 1)     AS late_percent,
    COUNT(DISTINCT order_id)           AS orders
FROM sales_fact
WHERE order_status = 'delivered'
GROUP BY customer_state
ORDER BY avg_delivery_days DESC;

-- B4. Average review score by delivery outcome
SELECT
    CASE WHEN is_late = 1 THEN 'late' ELSE 'on time' END AS delivery,
    ROUND(AVG(avg_review_score), 2)                        AS avg_review,
    COUNT(DISTINCT order_id)                               AS orders
FROM sales_fact
WHERE order_status = 'delivered' AND is_late IS NOT NULL
GROUP BY delivery;
