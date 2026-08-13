-- ============================================================
-- sql/analytics.sql
-- AdventureWorks Data Engineering Pipeline — Step 2
--
-- OLAP Operation Examples:
--   1. Roll-up   : Daily -> Monthly -> Yearly
--   2. Drill-down: Year -> Quarter -> Month -> Day
--   3. Slice     : Filter one dimension (one product / color)
--   4. Dice      : Filter multiple dimensions (product + customer + date range)
--
-- Also includes:
--   - Top 10 products by sales
--   - KPI summary query
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- OLAP OPERATION 1: ROLL-UP
-- Aggregate from a detailed level up to a coarser level.
-- Pattern: Daily -> Monthly -> Yearly
-- ──────────────────────────────────────────────────────────────

-- Roll-up: Daily sales
SELECT
    d.full_date                 AS sale_date,
    SUM(f.line_total)           AS daily_total_sales,
    SUM(f.order_qty)            AS daily_total_qty,
    COUNT(DISTINCT f.sales_order_id) AS daily_order_count
FROM olap.fact_sales f
JOIN olap.dim_date   d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;


-- Roll-up: Monthly sales (aggregating daily -> monthly)
SELECT
    d.year                      AS year,
    d.month_number              AS month,
    d.month_name                AS month_name,
    d.year_month                AS year_month,
    SUM(f.line_total)           AS monthly_total_sales,
    SUM(f.order_qty)            AS monthly_total_qty,
    COUNT(DISTINCT f.sales_order_id) AS monthly_order_count
FROM olap.fact_sales f
JOIN olap.dim_date   d ON f.date_key = d.date_key
GROUP BY d.year, d.month_number, d.month_name, d.year_month
ORDER BY d.year, d.month_number;


-- Roll-up: Yearly sales (most coarse)
SELECT
    d.year                      AS year,
    SUM(f.line_total)           AS yearly_total_sales,
    SUM(f.order_qty)            AS yearly_total_qty,
    COUNT(DISTINCT f.sales_order_id) AS yearly_order_count,
    COUNT(DISTINCT f.customer_key)   AS unique_customers
FROM olap.fact_sales f
JOIN olap.dim_date   d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;


-- ──────────────────────────────────────────────────────────────
-- OLAP OPERATION 2: DRILL-DOWN
-- Start at the highest level (year) and step down to finer detail.
-- Pattern: Year -> Quarter -> Month -> Day
-- ──────────────────────────────────────────────────────────────

-- Drill-down: Year -> Quarter
SELECT
    d.year,
    d.quarter,
    d.year_quarter,
    SUM(f.line_total)  AS total_sales,
    SUM(f.order_qty)   AS total_qty
FROM olap.fact_sales f
JOIN olap.dim_date   d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.year_quarter
ORDER BY d.year, d.quarter;


-- Drill-down: Year -> Quarter -> Month
SELECT
    d.year,
    d.quarter,
    d.month_number,
    d.month_name,
    d.year_month,
    SUM(f.line_total)  AS total_sales,
    SUM(f.order_qty)   AS total_qty
FROM olap.fact_sales f
JOIN olap.dim_date   d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month_number, d.month_name, d.year_month
ORDER BY d.year, d.quarter, d.month_number;


-- Drill-down: Full hierarchy Year -> Quarter -> Month -> Day (sample 30 days)
SELECT
    d.year,
    d.quarter,
    d.month_number,
    d.full_date,
    SUM(f.line_total)  AS daily_sales,
    SUM(f.order_qty)   AS daily_qty
FROM olap.fact_sales f
JOIN olap.dim_date   d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month_number, d.full_date
ORDER BY d.full_date
LIMIT 30;


-- ──────────────────────────────────────────────────────────────
-- OLAP OPERATION 3: SLICE
-- Fix one dimension to a single value and view the rest.
-- Example: Sales for the color 'Black' only.
-- ──────────────────────────────────────────────────────────────

-- Slice by product color = 'Black'
SELECT
    d.year_month,
    SUM(f.line_total)           AS total_sales,
    SUM(f.order_qty)            AS total_qty,
    COUNT(DISTINCT f.sales_order_id) AS order_count
FROM olap.fact_sales  f
JOIN olap.dim_product  p ON f.product_key  = p.product_key
JOIN olap.dim_date     d ON f.date_key     = d.date_key
WHERE p.color = 'Black'   -- <-- SLICE on one dimension value
GROUP BY d.year_month
ORDER BY d.year_month;


-- Slice by specific product (Mountain-200 Black, 38)
SELECT
    d.year,
    d.month_name,
    SUM(f.line_total)  AS total_sales,
    SUM(f.order_qty)   AS total_qty
FROM olap.fact_sales  f
JOIN olap.dim_product  p ON f.product_key = p.product_key
JOIN olap.dim_date     d ON f.date_key    = d.date_key
WHERE p.name = 'Mountain-200 Black, 38'
GROUP BY d.year, d.month_name, d.month_number
ORDER BY d.year, d.month_number;


-- ──────────────────────────────────────────────────────────────
-- OLAP OPERATION 4: DICE
-- Filter multiple dimensions simultaneously.
-- Example: Sales for Black or Silver products,
--          in a specific date range,
--          for online orders only.
-- ──────────────────────────────────────────────────────────────

SELECT
    p.name              AS product_name,
    p.color             AS color,
    d.year_month        AS month,
    c.account_number    AS customer_account,
    SUM(f.line_total)   AS total_sales,
    SUM(f.order_qty)    AS total_qty
FROM olap.fact_sales   f
JOIN olap.dim_product  p ON f.product_key  = p.product_key
JOIN olap.dim_customer c ON f.customer_key = c.customer_key
JOIN olap.dim_date     d ON f.date_key     = d.date_key
WHERE
    p.color IN ('Black', 'Silver')          -- Dimension 1: product color
    AND d.full_date BETWEEN '2022-01-01' AND '2023-12-31'  -- Dimension 2: date range
    AND f.online_order_flag = 1             -- Dimension 3: online orders
GROUP BY p.name, p.color, d.year_month, c.account_number
ORDER BY total_sales DESC
LIMIT 20;


-- ──────────────────────────────────────────────────────────────
-- TOP 10 PRODUCTS BY SALES
-- ──────────────────────────────────────────────────────────────
SELECT
    p.product_id,
    p.name                          AS product_name,
    p.color,
    p.product_line,
    SUM(f.line_total)               AS total_sales,
    SUM(f.order_qty)                AS total_qty,
    COUNT(DISTINCT f.sales_order_id) AS order_count,
    ROUND(AVG(f.unit_price)::NUMERIC, 2) AS avg_unit_price
FROM olap.fact_sales  f
JOIN olap.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.name, p.color, p.product_line
ORDER BY total_sales DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────────────
-- KPI SUMMARY
-- ──────────────────────────────────────────────────────────────
SELECT
    (SELECT COUNT(*) FROM olap.dim_product)                     AS total_products,
    (SELECT COUNT(*) FROM olap.dim_customer)                    AS total_customers,
    (SELECT COUNT(DISTINCT sales_order_id) FROM olap.fact_sales) AS total_orders,
    (SELECT ROUND(SUM(line_total)::NUMERIC, 2) FROM olap.fact_sales) AS total_sales,
    (SELECT SUM(order_qty)    FROM olap.fact_sales)             AS total_qty_sold,
    (SELECT ROUND(AVG(list_price)::NUMERIC, 2) FROM olap.dim_product WHERE list_price > 0) AS avg_list_price,
    (SELECT ROUND(AVG(total_due)::NUMERIC, 2) FROM oltp.salesorderheader)  AS avg_order_value;
