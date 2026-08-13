-- ============================================================
-- sql/oltp_schema.sql
-- AdventureWorks Data Engineering Pipeline — Step 2
--
-- OLTP (Online Transaction Processing) Schema
--
-- Purpose:
--   Represents the OPERATIONAL data model — normalized tables
--   that reflect how data is captured in a live business system.
--
-- Characteristics of OLTP design:
--   - Normalized (3NF) — minimizes data redundancy
--   - Optimized for INSERT / UPDATE / DELETE operations
--   - Row-oriented storage
--   - Many tables with foreign key relationships
--
-- Tables created:
--   product           (504 rows)
--   customer          (19,820 rows)
--   salesorderheader  (31,465 rows)
--   salesorderdetail  (121,317 rows)
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- Create schema if not exists
-- ──────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS oltp;


-- ──────────────────────────────────────────────────────────────
-- DROP existing tables (full refresh — order matters due to FKs)
-- ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS oltp.salesorderdetail CASCADE;
DROP TABLE IF EXISTS oltp.salesorderheader  CASCADE;
DROP TABLE IF EXISTS oltp.customer          CASCADE;
DROP TABLE IF EXISTS oltp.product           CASCADE;


-- ──────────────────────────────────────────────────────────────
-- TABLE: oltp.product
-- ──────────────────────────────────────────────────────────────
CREATE TABLE oltp.product (
    -- Primary Key
    product_id              INTEGER         NOT NULL,

    -- Product Identity
    name                    VARCHAR(200)    NOT NULL,
    product_number          VARCHAR(50)     NOT NULL,

    -- Flags
    make_flag               SMALLINT        NOT NULL DEFAULT 0,     -- 1 = manufactured in-house
    finished_goods_flag     SMALLINT        NOT NULL DEFAULT 0,     -- 1 = sold to customers

    -- Attributes
    color                   VARCHAR(50),
    safety_stock_level      INTEGER,
    reorder_point           INTEGER,
    standard_cost           NUMERIC(18, 4),
    list_price              NUMERIC(18, 4),
    size                    VARCHAR(10),
    size_unit_measure_code  CHAR(3),
    weight_unit_measure_code CHAR(3),
    weight                  NUMERIC(10, 2),
    days_to_manufacture     INTEGER,

    -- Classification
    product_line            CHAR(2),        -- R=Road, M=Mountain, T=Touring, S=Standard
    class                   CHAR(2),        -- H=High, M=Medium, L=Low
    style                   CHAR(2),        -- U=Universal, M=Mens, W=Womens

    -- Foreign Keys to other OLTP tables (not in our 4-file scope)
    product_subcategory_id  INTEGER,
    product_model_id        INTEGER,

    -- Dates
    sell_start_date         DATE            NOT NULL,
    sell_end_date           DATE,
    discontinued_date       DATE,

    -- Metadata
    rowguid                 UUID,
    modified_date           TIMESTAMP       NOT NULL,

    -- Data quality flags
    is_invalid_price        BOOLEAN         DEFAULT FALSE,
    is_outlier              BOOLEAN         DEFAULT FALSE,

    -- Pipeline metadata
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_product PRIMARY KEY (product_id)
);

-- Indexes for common queries
CREATE INDEX idx_product_name   ON oltp.product (name);
CREATE INDEX idx_product_color  ON oltp.product (color);
CREATE INDEX idx_product_line   ON oltp.product (product_line);


-- ──────────────────────────────────────────────────────────────
-- TABLE: oltp.customer
-- ──────────────────────────────────────────────────────────────
CREATE TABLE oltp.customer (
    -- Primary Key
    customer_id     INTEGER         NOT NULL,

    -- Optional links to Person and Store (not in our 4-file scope)
    person_id       INTEGER,
    store_id        INTEGER,

    -- Sales Territory
    territory_id    INTEGER,

    -- Business Key
    account_number  VARCHAR(20)     NOT NULL,

    -- Metadata
    rowguid         UUID,
    modified_date   TIMESTAMP       NOT NULL,

    -- Pipeline metadata
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)
);

CREATE INDEX idx_customer_account ON oltp.customer (account_number);
CREATE INDEX idx_customer_territory ON oltp.customer (territory_id);


-- ──────────────────────────────────────────────────────────────
-- TABLE: oltp.salesorderheader
-- One row per sales order (the "header" of the order)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE oltp.salesorderheader (
    -- Primary Key
    sales_order_id          INTEGER         NOT NULL,

    -- Order tracking
    revision_number         SMALLINT,
    order_date              TIMESTAMP       NOT NULL,
    due_date                TIMESTAMP,
    ship_date               TIMESTAMP,
    status                  SMALLINT,
    online_order_flag       SMALLINT        DEFAULT 0,

    -- Order numbers
    sales_order_number      VARCHAR(25),
    purchase_order_number   VARCHAR(25),
    account_number          VARCHAR(20),

    -- References
    customer_id             INTEGER         NOT NULL,
    sales_person_id         INTEGER,
    territory_id            INTEGER,
    bill_to_address_id      INTEGER,
    ship_to_address_id      INTEGER,
    ship_method_id          INTEGER,
    credit_card_id          INTEGER,
    credit_card_approval_code VARCHAR(50),
    currency_rate_id        INTEGER,

    -- Financials
    sub_total               NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    tax_amt                 NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    freight                 NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    total_due               NUMERIC(18, 4)  NOT NULL DEFAULT 0,
    comment                 TEXT,

    -- Metadata
    rowguid                 UUID,
    modified_date           TIMESTAMP,
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_salesorderheader PRIMARY KEY (sales_order_id),
    CONSTRAINT fk_soh_customer     FOREIGN KEY (customer_id)
        REFERENCES oltp.customer (customer_id)
        ON DELETE RESTRICT
);

CREATE INDEX idx_soh_customer   ON oltp.salesorderheader (customer_id);
CREATE INDEX idx_soh_order_date ON oltp.salesorderheader (order_date);
CREATE INDEX idx_soh_territory  ON oltp.salesorderheader (territory_id);


-- ──────────────────────────────────────────────────────────────
-- TABLE: oltp.salesorderdetail
-- One row per line item within an order (the "lines")
-- ──────────────────────────────────────────────────────────────
CREATE TABLE oltp.salesorderdetail (
    -- Composite Primary Key
    sales_order_id          INTEGER         NOT NULL,
    sales_order_detail_id   INTEGER         NOT NULL,

    -- Line item details
    carrier_tracking_number VARCHAR(25),
    order_qty               SMALLINT        NOT NULL,
    product_id              INTEGER         NOT NULL,
    special_offer_id        INTEGER,
    unit_price              NUMERIC(18, 4)  NOT NULL,
    unit_price_discount     NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    line_total              NUMERIC(18, 4)  NOT NULL,

    -- Metadata
    rowguid                 UUID,
    modified_date           TIMESTAMP,
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_salesorderdetail PRIMARY KEY (sales_order_id, sales_order_detail_id),
    CONSTRAINT fk_sod_header       FOREIGN KEY (sales_order_id)
        REFERENCES oltp.salesorderheader (sales_order_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_sod_product      FOREIGN KEY (product_id)
        REFERENCES oltp.product (product_id)
        ON DELETE RESTRICT
);

CREATE INDEX idx_sod_product ON oltp.salesorderdetail (product_id);
CREATE INDEX idx_sod_order   ON oltp.salesorderdetail (sales_order_id);


-- ──────────────────────────────────────────────────────────────
-- COMMENTS — document what each table is for
-- ──────────────────────────────────────────────────────────────
COMMENT ON TABLE oltp.product          IS 'AdventureWorks product catalog — OLTP normalized table';
COMMENT ON TABLE oltp.customer         IS 'AdventureWorks customers — OLTP normalized table';
COMMENT ON TABLE oltp.salesorderheader IS 'Sales order header — one row per order';
COMMENT ON TABLE oltp.salesorderdetail IS 'Sales order detail — one row per order line item';
