-- ============================================================
-- sql/star_schema.sql
-- AdventureWorks Data Engineering Pipeline — Step 2
--
-- OLAP Star Schema
--
-- Purpose:
--   Represents the ANALYTICAL data model — a denormalized schema
--   designed for fast aggregations and reporting.
--
-- Characteristics of Star Schema:
--   - One central FACT table surrounded by DIMENSION tables
--   - Denormalized — dimensions may repeat data for query speed
--   - Optimized for SELECT / GROUP BY / aggregations
--   - Fewer joins than OLTP schema
--
-- Structure:
--
--       DimProduct ──┐
--                    │
--       DimCustomer ─┤── FactSales ──── DimDate
--
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- Create schema
-- ──────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS olap;


-- ──────────────────────────────────────────────────────────────
-- DROP existing tables (full refresh)
-- ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS olap.fact_sales    CASCADE;
DROP TABLE IF EXISTS olap.dim_product   CASCADE;
DROP TABLE IF EXISTS olap.dim_customer  CASCADE;
DROP TABLE IF EXISTS olap.dim_date      CASCADE;


-- ──────────────────────────────────────────────────────────────
-- DIMENSION: olap.dim_date
-- A pre-populated date dimension covering the order date range.
-- Stores pre-computed date attributes to enable fast roll-up /
-- drill-down operations without string parsing at query time.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE olap.dim_date (
    date_key        INTEGER         NOT NULL,   -- YYYYMMDD surrogate key
    full_date       DATE            NOT NULL,   -- Actual date

    -- Hierarchy levels for OLAP operations
    day_of_month    SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,   -- 1=Sun, 7=Sat
    day_name        VARCHAR(10)     NOT NULL,   -- 'Monday', 'Tuesday', etc.
    week_of_year    SMALLINT        NOT NULL,
    month_number    SMALLINT        NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    quarter         SMALLINT        NOT NULL,   -- 1..4
    year            SMALLINT        NOT NULL,
    year_month      CHAR(7)         NOT NULL,   -- 'YYYY-MM'
    year_quarter    CHAR(7)         NOT NULL,   -- 'YYYY-Qn'
    is_weekend      BOOLEAN         NOT NULL DEFAULT FALSE,
    is_weekday      BOOLEAN         NOT NULL DEFAULT TRUE,

    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE UNIQUE INDEX idx_dim_date_full ON olap.dim_date (full_date);


-- ──────────────────────────────────────────────────────────────
-- DIMENSION: olap.dim_product
-- Denormalized product attributes — no joins to subcategory or
-- model tables needed at query time.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE olap.dim_product (
    -- Surrogate key (used in FactSales)
    product_key             SERIAL          NOT NULL,

    -- Business / natural key (from source)
    product_id              INTEGER         NOT NULL,

    -- Descriptive attributes
    name                    VARCHAR(200)    NOT NULL,
    product_number          VARCHAR(50),
    color                   VARCHAR(50),
    product_line            CHAR(2),        -- R, M, T, S
    class                   CHAR(2),        -- H, M, L
    style                   CHAR(2),        -- U, M, W
    size                    VARCHAR(10),

    -- Financials
    standard_cost           NUMERIC(18, 4),
    list_price              NUMERIC(18, 4),

    -- Lifecycle
    sell_start_date         DATE,
    sell_end_date           DATE,
    is_currently_sold       BOOLEAN         DEFAULT TRUE,

    -- Data quality
    is_outlier              BOOLEAN         DEFAULT FALSE,

    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_product    PRIMARY KEY (product_key),
    CONSTRAINT uq_dim_product_id UNIQUE      (product_id)
);

CREATE INDEX idx_dim_product_color ON olap.dim_product (color);
CREATE INDEX idx_dim_product_line  ON olap.dim_product (product_line);


-- ──────────────────────────────────────────────────────────────
-- DIMENSION: olap.dim_customer
-- One row per customer with denormalized account information.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE olap.dim_customer (
    -- Surrogate key
    customer_key    SERIAL          NOT NULL,

    -- Business / natural key
    customer_id     INTEGER         NOT NULL,

    -- Attributes
    account_number  VARCHAR(20),
    territory_id    INTEGER,
    store_id        INTEGER,

    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_customer    PRIMARY KEY (customer_key),
    CONSTRAINT uq_dim_customer_id UNIQUE      (customer_id)
);

CREATE INDEX idx_dim_customer_territory ON olap.dim_customer (territory_id);


-- ──────────────────────────────────────────────────────────────
-- FACT TABLE: olap.fact_sales
-- One row per sales order line item.
-- Contains:
--   - Foreign keys to all three dimensions (for joins)
--   - Measures (numeric values for aggregation)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE olap.fact_sales (
    -- Surrogate key for the fact row
    fact_id                 BIGSERIAL       NOT NULL,

    -- Foreign Keys (point to dimension surrogate keys)
    product_key             INTEGER         NOT NULL,
    customer_key            INTEGER         NOT NULL,
    date_key                INTEGER         NOT NULL,   -- references dim_date

    -- Degenerate dimensions (order identifiers — no dimension table needed)
    sales_order_id          INTEGER         NOT NULL,
    sales_order_detail_id   INTEGER         NOT NULL,

    -- Additive Measures (can be summed across any dimension)
    order_qty               SMALLINT        NOT NULL,
    unit_price              NUMERIC(18, 4)  NOT NULL,
    unit_price_discount     NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    line_total              NUMERIC(18, 4)  NOT NULL,
    sub_total               NUMERIC(18, 4),
    tax_amt                 NUMERIC(18, 4),
    freight                 NUMERIC(18, 4),
    total_due               NUMERIC(18, 4),

    -- Semi-additive (context-dependent aggregation)
    online_order_flag       SMALLINT        DEFAULT 0,
    territory_id            INTEGER,

    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_sales       PRIMARY KEY (fact_id),
    CONSTRAINT fk_fact_product     FOREIGN KEY (product_key)
        REFERENCES olap.dim_product  (product_key),
    CONSTRAINT fk_fact_customer    FOREIGN KEY (customer_key)
        REFERENCES olap.dim_customer (customer_key),
    CONSTRAINT fk_fact_date        FOREIGN KEY (date_key)
        REFERENCES olap.dim_date     (date_key),
    CONSTRAINT uq_fact_order_line  UNIQUE (sales_order_id, sales_order_detail_id)
);

-- Indexes for common analytical queries
CREATE INDEX idx_fact_product   ON olap.fact_sales (product_key);
CREATE INDEX idx_fact_customer  ON olap.fact_sales (customer_key);
CREATE INDEX idx_fact_date      ON olap.fact_sales (date_key);
CREATE INDEX idx_fact_order     ON olap.fact_sales (sales_order_id);
CREATE INDEX idx_fact_territory ON olap.fact_sales (territory_id);


-- ──────────────────────────────────────────────────────────────
-- COMMENTS
-- ──────────────────────────────────────────────────────────────
COMMENT ON TABLE olap.dim_date     IS 'Date dimension — pre-computed calendar attributes for OLAP';
COMMENT ON TABLE olap.dim_product  IS 'Product dimension — denormalized product attributes';
COMMENT ON TABLE olap.dim_customer IS 'Customer dimension — denormalized customer attributes';
COMMENT ON TABLE olap.fact_sales   IS 'Fact table — one row per order line item with measures';
