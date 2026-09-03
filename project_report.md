# AdventureWorks Data Engineering Project Report

**Institution:** Data Engineering Laboratory  
**Project Title:** End-to-End Resilient Data Engineering Pipeline & Analytics Dashboard  
**Dataset:** AdventureWorks (Microsoft)  
**Platform:** Local Windows Machine — Python, PostgreSQL, Flask  
**Development Assistant:** Antigravity (Google DeepMind)  
**Date:** August 2026  

---

## Table of Contents

1. [Aim](#1-aim)
2. [Scope](#2-scope)
3. [Tools & Technologies Used](#3-tools--technologies-used)
4. [Dataset Description](#4-dataset-description)
5. [Project Structure](#5-project-structure)
6. [Procedure — Pipeline Architecture](#6-procedure--pipeline-architecture)
7. [Exercise 1: Data Preprocessing & Feature Engineering](#7-exercise-1-data-preprocessing--feature-engineering)
8. [Exercise 2: Building Core Data Pipeline (ETL)](#8-exercise-2-building-core-data-pipeline-etl)
9. [Exercise 3: Data Architecture & Schema Design](#9-exercise-3-data-architecture--schema-design)
10. [Exercise 4: Python-Based Batch Pipeline](#10-exercise-4-python-based-batch-pipeline)
11. [Exercise 5: Resilient & Production-Ready Pipelines](#11-exercise-5-resilient--production-ready-pipelines)
12. [Analytics Dashboard (Flask Web Application)](#12-analytics-dashboard-flask-web-application)
13. [Functional Requirements](#13-functional-requirements)
14. [Non-Functional Requirements](#14-non-functional-requirements)
15. [Production Resilience Features](#15-production-resilience-features)
16. [Results & Outcomes](#16-results--outcomes)
17. [Conclusion](#17-conclusion)

---

## 1. Aim

The primary aim of this project is to build a **complete, end-to-end, resilient Data Engineering pipeline and analytics dashboard** based on the AdventureWorks dataset. The system is designed to:

- **Ingest** raw operational data from flat CSV files
- **Validate** data quality using a rules-based engine
- **Transform** the data into structured analytical models (Star Schema / OLAP)
- **Load** data into a PostgreSQL relational database in both OLTP (3NF) and OLAP (Star Schema) formats
- **Present** actionable business intelligence through a modern, web-based interactive analytics dashboard

The project also demonstrates the **4 core pillars of production-ready Data Engineering**: Staging & Validation, Idempotency, Atomicity, and Error Handling with Replay/Backfill.

---

## 2. Scope

### In-Scope
- Extracting raw tab-separated CSV data from 4 source files
- Data validation and quarantine mechanisms (null checks, schema validation, outlier detection)
- Relational OLTP schema design (3rd Normal Form)
- Dimensional OLAP modeling (Star Schema — Fact and Dimension tables)
- Automated pipeline orchestration (Full Refresh mode)
- Change Data Capture (CDC) detection between pipeline runs
- A web-based Flask interface for CRUD operations, pipeline management, and analytics visualization
- Audit logging for all manual data mutations
- Production resilience: idempotency, atomicity, error handling, and replay

### Out-of-Scope
- Real-time or streaming data ingestion
- Cloud infrastructure deployment (AWS, Azure, GCP)
- Multi-node distributed processing (PySpark cluster mode)
- External API data sources

---

## 3. Tools & Technologies Used

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Language** | Python 3.10+ | Core pipeline logic |
| **Data Processing** | Pandas >= 2.0 | DataFrame manipulation, transformation |
| **Distributed Processing** | PySpark >= 3.4 (local mode) | Batch processing support |
| **Database Driver** | Psycopg2-binary >= 2.9 | PostgreSQL connectivity |
| **ORM / Abstraction** | SQLAlchemy >= 2.0 | Database schema management & query execution |
| **Database** | PostgreSQL | OLTP and OLAP data storage |
| **File Format** | Apache Parquet (PyArrow) | Efficient columnar staging storage |
| **Web Framework** | Flask | Dashboard web server and REST API routes |
| **Notebooks** | Jupyter Notebook >= 7.0 | Interactive educational exercises |
| **Visualization** | Plotly >= 5.15, Matplotlib, Seaborn | Interactive and static charts |
| **Delta Lake** | delta-spark >= 2.4 | Delta Lake (local mode, no Docker) |
| **Utilities** | Colorama, Tabulate, TQDM, python-dotenv | Logging, CLI display, progress bars |
| **Frontend** | HTML5, Vanilla CSS (Glassmorphism), JavaScript | Dashboard UI |
| **JS Visualization** | Plotly.js | Client-side interactive charts |

---

## 4. Dataset Description

The AdventureWorks dataset represents a fictional bicycle manufacturer's operational data. All source files are **tab-separated (`\t`)**, have **no header row**, and are **UTF-8 encoded**.

| File | Table | Records | Columns | Size | Description |
|------|-------|---------|---------|------|-------------|
| `Product.csv` | `product` | **504** | 25 | ~86.5 KB | Products with cost, list price, category, color, size, weight |
| `Customer.csv` | `customer` | **19,820** | 7 | ~1.6 MB | Customer identities with territory and account references |
| `SalesOrderHeader.csv` | `salesorderheader` | **31,465** | 26 | ~7.5 MB | Order-level information (dates, status, financials) |
| `SalesOrderDetail.csv` | `salesorderdetail` | **121,317** | 11 | ~13.1 MB | Line-item details per order (product, qty, price) |
| **TOTAL** | | **173,106** | | **~22.8 MB** | |

### Key Data Facts Discovered
- **Delimiter:** Tab (`\t`), not comma
- **Headers:** None — column names assigned positionally via `config.py`
- **Encoding:** UTF-8
- **Nullable Columns:** `Color`, `Size`, `Weight`, `ProductLine`, `Class`, `Style` have high null rates (expected — not all products have these attributes)
- **Pre-calculated Column:** `SalesOrderDetail.LineTotal = OrderQty x UnitPrice x (1 - UnitPriceDiscount)`
- **Raw Parquet Sizes:** `raw_product.parquet` (52.6 KB), `raw_customer.parquet` (1.1 MB), `raw_salesorderheader.parquet` (3.0 MB), `raw_salesorderdetail.parquet` (5.7 MB) — total **9.9 MB** compressed

---

## 5. Project Structure

```
AdventureWorks_DataEngineering/
|
+-- data/
|   +-- AdventureWorks/
|       +-- Product.csv             (504 products)
|       +-- Customer.csv            (19,820 customers)
|       +-- SalesOrderHeader.csv    (31,465 orders)
|       +-- SalesOrderDetail.csv    (121,317 line items)
|
+-- src/                            <- Python pipeline modules
|   +-- __init__.py
|   +-- config.py                   <- Central config (paths, column defs, DB string)
|   +-- ingestion.py                <- Extract from CSV + raw staging
|   +-- validation.py               <- Schema + null + duplicate + outlier checks
|   +-- transformation.py           <- Type casting, cleaning, joins
|   +-- oltp.py                     <- Load to PostgreSQL OLTP schema (3NF)
|   +-- olap.py                     <- Build Star Schema (OLAP)
|   +-- analytics.py                <- Generate dashboard-ready Parquet files
|   +-- data_quality.py             <- DQ rules engine + quarantine routing
|   +-- pipeline.py                 <- Standard batch pipeline orchestrator
|   +-- resilient_pipeline.py       <- Production-grade pipeline with all 4 resilience pillars
|
+-- notebooks/
|   +-- 01_core_etl.ipynb           <- Exercise 1: Extract & Stage
|   +-- 02_schema_design.ipynb      <- Exercise 2/3: OLTP + Star Schema
|   +-- 03_batch_pipeline.ipynb     <- Exercise 4: Batch Pipeline
|   +-- 04_resilient_pipeline.ipynb <- Exercise 5: Production-Ready Pipeline
|
+-- sql/
|   +-- oltp_schema.sql             <- OLTP (3NF) DDL
|   +-- star_schema.sql             <- OLAP Star Schema DDL
|   +-- analytics.sql               <- Analytical query templates
|
+-- staging/
|   +-- raw/          <- Raw extracted data (Parquet -- immutable backups)
|   +-- valid/        <- Records passing all DQ rules
|   +-- invalid/      <- Records failing schema validation
|   +-- duplicates/   <- Detected duplicate records
|   +-- outliers/     <- Statistical outliers (IQR method)
|
+-- dashboard/        <- Chart-ready Parquet datasets for Flask UI
+-- logs/             <- Pipeline logs + Flask audit JSON
+-- templates/        <- Jinja2 HTML templates for Flask
+-- static/           <- CSS, JS, and image assets
|
+-- app.py                          <- Flask web application (CRUD + Dashboard)
+-- run_pipeline.py                 <- One-command pipeline runner
+-- run_resilient_pipeline.py       <- Production-grade pipeline runner
+-- requirements.txt
+-- README.md
```

---

## 6. Procedure — Pipeline Architecture

The data architecture follows a structured **multi-hop Full Refresh pipeline**:

```
SOURCE CSVs (4 files -- Product, Customer, SalesOrderHeader, SalesOrderDetail)
        |
        v
 [Step 1] EXTRACTION  (src/ingestion.py)
          - Reads tab-separated CSVs with positional column names
          - Adds _source_file and _ingested_at lineage metadata columns
        |
        v
 [Step 2] RAW STAGING  (staging/raw/*.parquet)
          - Immutable Parquet backup -- never modified after write
          - Enables replay without re-reading source files
        |
        v
 [Step 3] DATA QUALITY & VALIDATION  (src/validation.py, src/data_quality.py)
          - Schema compliance checks (expected columns, data types)
          - Null key detection (PK columns must not be null)
          - Duplicate record detection (exact row deduplication)
          - Statistical outlier detection (IQR x 1.5 method on numeric fields)
          - Invalid --> staging/outliers/, staging/invalid/, staging/duplicates/
          - Valid --> staging/valid/*.parquet
        |
        v
 [Step 4] TRANSFORMATION  (src/transformation.py)
          - Type casting (strings --> integers, decimals, dates)
          - Null value handling (fill or flag)
          - Column standardization and renaming
          - Table joins (SalesOrderHeader + SalesOrderDetail merged)
        |
        v
 [Step 5] OLTP LOAD  (src/oltp.py --> PostgreSQL oltp schema)
          - Truncates existing tables (idempotency)
          - Bulk-inserts validated records with full FK constraints
        |
        v
 [Step 6] OLAP / STAR SCHEMA BUILD  (src/olap.py --> PostgreSQL olap schema)
          - Builds: DimProduct, DimCustomer, DimDate, FactSales
          - Optimized for aggregation, roll-up, drill-down, slicing
        |
        v
 [Step 7] ANALYTICS  (src/analytics.py)
          - Generates dashboard-ready Parquet files in dashboard/
          - KPI calculations: Total Sales, Total Orders, AOV, etc.
          - Chart datasets: Sales Trends, Top Products, Color Distribution
        |
        v
 [Step 8] DASHBOARD DATA (Flask app.py)
          - Flask reads from dashboard/ Parquet files
          - Serves KPIs and Plotly charts to web UI
          - Exposes CRUD endpoints for Products and Customers
```

**Full Refresh Design Principle:** Every run starts from source CSVs and rebuilds all layers from scratch. No dependency on previous runs. Adding, modifying, or deleting a row in a CSV and re-running the pipeline propagates the change to all downstream layers automatically.

---

## 7. Exercise 1: Data Preprocessing & Feature Engineering

### Aim
To collect data from diverse sources, perform data preprocessing, cleaning, feature engineering, and exploratory data analysis (EDA).

### Procedure
1. **Data Collection:** Identified 4 tab-separated CSV files representing core business entities of AdventureWorks.
2. **Exploratory Data Analysis (EDA):** Performed schema inspection (column names, dtypes, null counts) and statistical summaries on all 4 tables using Pandas.
3. **Data Preprocessing & Cleaning:** Detected missing values in nullable columns (`Color`, `Size`, `Weight`). Identified malformed rows using `on_bad_lines='warn'`.
4. **Feature Engineering:** Added lineage metadata columns (`_source_file`, `_ingested_at`) to each DataFrame for traceability.
5. **Data Normalization:** Column names were positionally assigned from `config.py` since the CSVs have no header row.
6. **Interpretation:** Record counts, memory usage, and sample rows displayed and summarized per table.

### Key Deliverables
| Deliverable | Description |
|------------|-------------|
| `src/config.py` | Central configuration: paths, column definitions, DB connection string |
| `src/ingestion.py` | CSV extraction module: `extract_all()`, `stage_raw()`, `display_summary()` |
| `notebooks/01_core_etl.ipynb` | 8-section educational ETL notebook |
| `staging/raw/*.parquet` | 4 raw Parquet staging files (9.9 MB total) |

### Notebook Sections (01_core_etl.ipynb)
| Section | Content |
|---------|---------|
| 1 | Configuration & Setup |
| 2 | Source File Check (verify CSVs exist, file sizes) |
| 3 | Data Extraction (`read_source_csv()` helper) |
| 4 | Schema Display (column names, dtypes, null counts) |
| 5 | Record Counts (rows + memory usage) |
| 6 | Sample Records (first 5 rows + `.describe()` stats) |
| 7 | Raw Staging (Parquet write + read-back verification) |
| 8 | Step 1 Summary |

### Public Functions in `src/ingestion.py`
| Function | Returns |
|----------|---------|
| `extract_product()` | Product DataFrame (504 rows, 25 columns) |
| `extract_customer()` | Customer DataFrame (19,820 rows, 7 columns) |
| `extract_salesorderheader()` | SalesOrderHeader DataFrame (31,465 rows, 26 columns) |
| `extract_salesorderdetail()` | SalesOrderDetail DataFrame (121,317 rows, 11 columns) |
| `extract_all()` | `dict` of all 4 DataFrames |
| `stage_raw(raw)` | Writes Parquet to `staging/raw/`, returns paths |
| `display_summary(raw)` | Prints schema + records + samples for all tables |

---

## 8. Exercise 2: Building Core Data Pipeline (ETL)

### Aim
To build a core ETL data pipeline that extracts data from diverse sources, applies data transformation techniques, implements data loading strategies, and integrates Change Data Capture (CDC) concepts.

### Procedure
1. **Data Extraction:** `src/ingestion.py` extracts all 4 tables from flat files using `pandas.read_csv()` with tab delimiter and positional column mapping.
2. **Data Transformation:** `src/transformation.py` applies type casting, null filling, column renaming to snake_case, and SalesOrderHeader + SalesOrderDetail join.
3. **Data Loading Strategies:** Implemented **Full Load** (Full Refresh): all rows from source are re-loaded every run. Contrasted with **Incremental Load** concept in notebooks.
4. **Change Data Capture (CDC):** Demonstrated in `03_batch_pipeline.ipynb` — detecting new, updated, deleted, and unchanged records by comparing current vs. previous run snapshots.
5. **Performance Evaluation:** Parquet format used for staging to minimize I/O; pipeline timing logged per step.

### Key Transformation Operations in `src/transformation.py`
| Function | Purpose |
|----------|---------|
| `transform_product()` | Cast price/cost to `float`, dates to `datetime`, flags to `bool` |
| `transform_customer()` | Cast IDs to `int`, parse `modified_date` |
| `transform_salesorderheader()` | Parse order dates, cast financial columns to `float` |
| `transform_salesorderdetail()` | Cast qty, price, compute validated `line_total` |

---

## 9. Exercise 3: Data Architecture & Schema Design

### Aim
To design operational (OLTP) and analytical (OLAP) schemas, build dimensional models (Star/Snowflake Schema), and design data cubes for multidimensional analysis.

### Procedure
1. **OLTP/OLAP Identification:** Identified 4 OLTP tables mapped to `oltp` PostgreSQL schema. Identified DimProduct, DimCustomer, DimDate, FactSales for the `olap` schema.
2. **Relational Schema Design:** Designed a 3NF normalized schema with proper PKs, FKs, and constraints.
3. **Dimensional Modeling:** Built a Star Schema centered on `FactSales` with surrounding dimension tables.
4. **Data Cube Design:** Combined FactSales with DimDate, DimProduct, DimCustomer for OLAP cube construction.
5. **Analytical Queries:** Implemented Roll-up, Drill-down, Slice, and Dice queries in `sql/analytics.sql`.

### OLTP Schema (3NF) — `sql/oltp_schema.sql`
| Table | PK | Records | Key Columns |
|-------|----|---------|-------------|
| `oltp.product` | `product_id` | 504 | name, list_price, standard_cost, color, product_line |
| `oltp.customer` | `customer_id` | 19,820 | account_number, territory_id, person_id |
| `oltp.salesorderheader` | `sales_order_id` | 31,465 | customer_id (FK), order_date, total_due |
| `oltp.salesorderdetail` | `(sales_order_id, sales_order_detail_id)` | 121,317 | product_id (FK), order_qty, unit_price, line_total |

### OLAP Star Schema — `sql/star_schema.sql`
| Table | Type | Key Dimensions |
|-------|------|---------------|
| `olap.fact_sales` | Fact Table | Links to all 4 dims; line_total, order_qty, unit_price |
| `olap.dim_product` | Dimension | product_id, name, color, list_price, product_line |
| `olap.dim_customer` | Dimension | customer_id, account_number, territory_id |
| `olap.dim_date` | Dimension | date_key, year, quarter, month, day, day_of_week |

### OLAP Operations Demonstrated
| Operation | Example |
|-----------|---------|
| **Roll-up** | Monthly Sales -> Quarterly -> Annual |
| **Drill-down** | Annual -> Monthly -> Daily Sales |
| **Slice** | Filter FactSales by `year = 2013` |
| **Dice** | Filter by `year = 2013 AND product_line = 'R'` |

---

## 10. Exercise 4: Python-Based Batch Pipeline

### Aim
To implement an end-to-end, basic Python-based batch pipeline using Pandas to extract data, transform it, and load it into a target database.

### Procedure
1. **Data Extraction:** `src/ingestion.py` extracts all 4 CSVs and stages raw Parquet files.
2. **Data Cleaning & Transformation:** `src/transformation.py` performs type casting, null handling, and joins.
3. **Data Loading:** `src/oltp.py` and `src/olap.py` use SQLAlchemy to TRUNCATE + INSERT into PostgreSQL.
4. **Pipeline Functionality:** `run_pipeline.py` and `src/pipeline.py` orchestrate all steps in sequence with timing and step-level logging.
5. **Error Handling & Verification:** Step-level try/except blocks; pipeline log written to `logs/pipeline.log`; idempotency verified by re-running the pipeline and confirming stable row counts.

### Pipeline Orchestration Flow — `src/pipeline.py`
```
run_pipeline()
    +-- Step 1: extract_all()        -> raw DataFrames
    +-- Step 2: stage_raw()          -> staging/raw/*.parquet
    +-- Step 3: validate_all()       -> staging/valid/, staging/outliers/
    +-- Step 4: transform_all()      -> clean DataFrames
    +-- Step 5: load_oltp()          -> oltp.* tables in PostgreSQL
    +-- Step 6: load_olap()          -> olap.* tables in PostgreSQL
    +-- Step 7: generate_analytics() -> dashboard/*.parquet
    +-- Step 8: pipeline_report()    -> summary log
```

---

## 11. Exercise 5: Resilient & Production-Ready Pipelines

### Aim
To design resilient, production-ready data pipelines featuring secure staging areas, data quality checks, idempotency, atomicity, and robust error handling strategies.

### Procedure
1. **Staging & Validation:** Multi-zone staging architecture (Raw, Valid, Invalid, Duplicates, Outliers) with a rules-based DQ engine.
2. **Idempotency:** All Parquet writes use overwrite mode; all DB loads use TRUNCATE/INSERT pattern.
3. **Atomicity:** Python `try/except` context managers wrap each load step; DB rollbacks on failure.
4. **Error Handling (Retries):** Exponential backoff retries on transient DB connection failures.
5. **Backfilling & Replay:** Audit log + Flask **Replay Fix** feature for historical data corrections.

### Key Module — `src/resilient_pipeline.py` (36 KB)
| Pillar | Implementation |
|--------|---------------|
| **Staging & Validation** | `staging/raw/` -> `staging/valid/` or `staging/outliers/` routing via DQ rules engine |
| **Idempotency** | `TRUNCATE / INSERT` + Parquet `overwrite` mode |
| **Atomicity** | `try/except` blocks with `engine.begin()` transaction context managers |
| **Error Handling & Replay** | Audit JSON log + `replay_fix()` backfill endpoint in Flask |

### Data Quality Rules Engine — `src/data_quality.py` (21 KB)
| Rule Category | Check | Action on Failure |
|--------------|-------|------------------|
| Schema Validation | Expected columns present, types match | FATAL -- pipeline halts |
| Null Key Check | Primary key columns must not be null | FATAL -- records quarantined |
| Duplicate Check | Row-level deduplication on PK | WARNING -- duplicates staged |
| Outlier Detection | IQR x 1.5 on ListPrice, StandardCost, LineTotal | WARNING -- outliers staged |
| Simulation Rule | `sim_product_too_few` -- expects >= 1,000 products | Intentional FAILED status to demonstrate halt behavior |

---

## 12. Analytics Dashboard (Flask Web Application)

### Overview
A local Flask web application (`app.py`, 50 KB) serves as the complete UI for the data engineering system, accessible at `http://localhost:5000`.

**Design Theme:** Premium dark-mode "Green & Black" glassmorphic aesthetic with CSS variables, Inter/JetBrains Mono typography, and smooth transitions.

### Dashboard Pages

| URL | Page | Features |
|-----|------|---------|
| `/` | **Dashboard Overview** | 8 KPI cards, annual sales bar chart, recent activity log |
| `/products` | **Products Directory** | Paginated table, client-side search, Edit/Delete actions |
| `/products/add` | **Add Product** | Validated form, triggers full pipeline on submit |
| `/products/edit/<id>` | **Edit Product** | Pre-populated form, read-only PK, pipeline trigger |
| `/customers` | **Customer Directory** | Paginated table, dual server+client search |
| `/customers/add` | **Add Customer** | UUID auto-generation, FK relationship fields |
| `/customers/edit/<id>` | **Edit Customer** | Current record diff panel, Danger Zone delete section |
| `/pipeline` | **Pipeline Control** | On-demand run trigger, idempotency test, run history |
| `/data-quality` | **DQ Rules Engine** | Physical zone architecture, rule results, quarantine stats |
| `/analytics` | **OLAP Analytics** | Full suite of Plotly charts (Sales Trends, Top Products, Color Distribution) |
| `/audit` | **Audit Log** | All CRUD operations logged; Replay Fix feature per event |

### KPI Cards Displayed on Homepage
| KPI | Source |
|-----|--------|
| Total Products | `staging/valid/valid_product.parquet` |
| Total Customers | `staging/valid/valid_customer.parquet` |
| Total Orders | `dashboard/sales_by_month.parquet` |
| Total Sales (Revenue) | `dashboard/top_products.parquet` |
| Total Quantity Sold | `dashboard/top_products.parquet` |
| Invalid Records | DQ engine output |
| Duplicate Records | `staging/duplicates/` |
| Outlier Records | `staging/outliers/` |

### CRUD Safety Features
| Feature | Description |
|---------|-------------|
| **Atomic Backup & Rollback** | CSV backed up before write; restored on pipeline failure |
| **Referential Integrity Check** | Deletion of Product/Customer blocked if referenced in SalesOrderDetail/SalesOrderHeader |
| **Audit Trail** | All mutations written to `logs/flask_audit.json` (append-only) |
| **Pipeline Lock** | `threading.Lock()` prevents concurrent pipeline runs |
| **Custom Error Pages** | Themed 404 and 500 error pages matching the dashboard aesthetic |

---

## 13. Functional Requirements

| # | Requirement |
|---|-------------|
| FR-1 | The system must provide a UI to view, add, edit, and delete Product and Customer records. |
| FR-2 | The pipeline must execute on-demand via the dashboard and rebuild all data models. |
| FR-3 | The system must apply data quality rules and block execution on fatal schema violations. |
| FR-4 | The system must track all manual data modifications in a persistent Audit Log. |
| FR-5 | The dashboard must display KPIs: Total Sales, Total Orders, Average Order Value, DQ counts. |
| FR-6 | The dashboard must render interactive Plotly charts (Sales Trends, Color Distribution, Top Products). |
| FR-7 | The system must enforce referential integrity during delete operations. |
| FR-8 | The pipeline must support a Replay/Backfill feature for historical data corrections. |

---

## 14. Non-Functional Requirements

| # | Category | Requirement |
|---|---------|-------------|
| NFR-1 | **Performance** | Intermediate staging uses `.parquet` format to minimize disk I/O |
| NFR-2 | **Usability** | Web UI provides clear flash messages for all user actions (Success/Error/Warning) |
| NFR-3 | **Aesthetics** | Premium dark-mode glassmorphic UI with consistent "Green & Black" theme |
| NFR-4 | **Data Integrity** | Referential integrity enforced before destructive operations |
| NFR-5 | **Idempotency** | Pipeline reruns are safe -- no data duplication on multiple executions |
| NFR-6 | **Atomicity** | Failed loads roll back completely -- no partial corrupted data states |
| NFR-7 | **Auditability** | All CRUD mutations are permanently recorded in a JSON audit log |
| NFR-8 | **Portability** | Runs 100% locally on Windows -- no Docker, no cloud, no external services required |

---

## 15. Production Resilience Features

The system is built around **4 core pillars of resilient data engineering**:

### Pillar 1 — Staging & Validation
The architecture implements secure multi-zone staging areas:

```
staging/
  +-- raw/        <- Immutable raw backup -- always preserved
  +-- valid/      <- Clean records that pass all DQ rules
  +-- invalid/    <- Records failing schema/type checks
  +-- duplicates/ <- Exact row duplicates detected
  +-- outliers/   <- Statistical outliers (IQR x 1.5)
```

The DQ rules engine (`src/data_quality.py`) enforces:
- **Schema validation** — column presence and type compliance
- **Null key checks** — primary keys must not be null
- **Duplicate detection** — row-level deduplication
- **Outlier detection** — IQR x 1.5 on `list_price`, `standard_cost`, `line_total`

### Pillar 2 — Idempotency
- **Parquet writes:** Always use overwrite mode — re-running never appends duplicate records
- **Database loads:** TRUNCATE table before INSERT — stable row counts across reruns
- **Verified:** Running the pipeline multiple times produces identical row counts and KPIs

### Pillar 3 — Atomicity
- All database writes wrapped in `engine.begin()` context manager
- On any DQ failure or exception mid-load: transaction rolls back automatically
- CSV mutations: backup written before change, restored on pipeline failure
- Guarantees: either all data is loaded correctly or nothing is loaded

### Pillar 4 — Error Handling & Replay
- **Audit Log:** `logs/flask_audit.json` records every CRUD operation (timestamp, action, old/new values)
- **Replay Fix:** Each audit log entry exposes a "Replay Fix" button — reruns downstream pipeline from a specific historical point
- **Exponential backoff:** Transient DB connection failures retried with increasing delays
- **Custom error pages:** 404 and 500 pages maintain dashboard aesthetic instead of raw browser errors

---

## 16. Results & Outcomes

### Data Volume Processed
| Metric | Value |
|--------|-------|
| Total Source Records | 173,106 rows |
| Total Source Size | ~22.8 MB (CSV) |
| Raw Parquet Stage Size | ~9.9 MB (56% compression) |
| Products Loaded (OLTP + OLAP) | 504 |
| Customers Loaded (OLTP + OLAP) | 19,820 |
| Sales Orders Loaded | 31,465 |
| Sales Line Items Loaded | 121,317 |

### Pipeline Steps Implemented
| Step | Module | Status |
|------|--------|--------|
| Extraction | `src/ingestion.py` | Complete |
| Raw Staging | `staging/raw/` (Parquet) | Complete |
| Data Quality Validation | `src/validation.py`, `src/data_quality.py` | Complete |
| Transformation | `src/transformation.py` | Complete |
| OLTP Load | `src/oltp.py` | Complete |
| OLAP / Star Schema Build | `src/olap.py` | Complete |
| Analytics Generation | `src/analytics.py` | Complete |
| Production Resilient Pipeline | `src/resilient_pipeline.py` | Complete |
| Flask Dashboard & CRUD | `app.py` + all templates | Complete |

### Notebooks Completed
| Notebook | Exercise | Status |
|----------|----------|--------|
| `01_core_etl.ipynb` | Exercise 1 — Core ETL & EDA | Complete |
| `02_schema_design.ipynb` | Exercise 2/3 — OLTP + Star Schema | Complete |
| `03_batch_pipeline.ipynb` | Exercise 4 — Batch Pipeline + CDC | Complete |
| `04_resilient_pipeline.ipynb` | Exercise 5 — Resilient Pipeline | Complete |

### Dashboard Pages Verified
| Page | Status |
|------|--------|
| Dashboard Overview (KPIs + Charts) | Verified |
| Products CRUD (list, add, edit, delete) | Verified |
| Customers CRUD (list, add, edit, delete) | Verified |
| Pipeline Control (trigger, history, idempotency test) | Verified |
| Data Quality Results (zones, rules, quarantine) | Verified |
| Analytics (Plotly OLAP charts) | Verified |
| Audit Log (log view + Replay Fix) | Verified |

---

## 17. Conclusion

This project successfully implemented a **complete, production-grade, end-to-end Data Engineering pipeline** on the AdventureWorks dataset, running entirely on a local Windows machine without any cloud infrastructure or containerization.

### Key Achievements
1. **Full ETL Pipeline:** Designed and implemented a multi-hop pipeline covering extraction, validation, transformation, OLTP loading, OLAP modeling, and analytics generation — all orchestrated from a single `run_pipeline()` call.
2. **Data Quality Engine:** Built a configurable, rules-based DQ engine that classifies, quarantines, and routes records across 5 staging zones, providing full traceability of data lineage.
3. **Dimensional Modeling:** Designed both a normalized 3NF OLTP schema and an analytical Star Schema with DimProduct, DimCustomer, DimDate, and FactSales tables, enabling OLAP operations.
4. **Production Resilience:** The pipeline demonstrates all 4 pillars of resilient data engineering — Staging, Idempotency, Atomicity, and Error Handling with Replay/Backfill — making it suitable as a foundation for real-world batch pipeline systems.
5. **Interactive Dashboard:** Built a full-stack Flask web application with a premium dark-mode glassmorphic UI that exposes all pipeline functionality, CRUD operations, analytics, and audit controls in a single, unified interface.

### Learning Outcomes
- **Data Engineering Fundamentals:** Config-driven design, multi-hop staging, Parquet format, lineage metadata
- **Schema Design:** 3NF normalization, Star Schema dimensional modeling, OLAP cube construction
- **Pipeline Reliability:** Idempotency patterns, transaction management, atomic file operations
- **Full-Stack Data Application:** Integrating Python data pipelines with a web UI for operational control and analytics visualization

---

*Report generated by Antigravity (Google DeepMind) -- AdventureWorks Data Engineering Project, August 2026*
