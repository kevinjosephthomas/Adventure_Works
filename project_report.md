# AdventureWorks Data Engineering Project Report

## 1. Aim
The primary aim of this project is to build an end-to-end, resilient Data Engineering pipeline and orchestration dashboard based on the AdventureWorks dataset. The system is designed to ingest raw operational data, enforce strict data quality rules, transform the data into optimized analytical models (Star Schema), and present actionable insights through a modern, web-based analytics dashboard.

## 2. Scope
The scope of this project covers the full data lifecycle for key domains including **Products**, **Customers**, **Sales Orders**, and **Sales Order Details**. 
- **In-Scope**: Extracting raw CSV data, data validation and quarantine mechanisms, relational (OLTP) and dimensional (OLAP) modeling, automated pipeline orchestration, and a web interface for CRUD operations and pipeline management.
- **Out-of-Scope**: Real-time streaming data ingestion and cloud infrastructure deployment (the system is designed to run locally).

## 3. Tools Used
- **Backend & Data Processing:** Python, Pandas (Data Manipulation), PyArrow (Parquet file storage)
- **Database Interaction:** SQLAlchemy, Psycopg2 (PostgreSQL integration)
- **Orchestration & Web UI:** Flask (Web Framework)
- **Frontend Technologies:** HTML5, Vanilla CSS (Custom Green/Black Theme), JavaScript
- **Data Visualization:** Plotly.js

## 4. Procedure
The data architecture follows a structured, multi-hop pipeline approach:
1. **Extraction:** Raw data is extracted from source CSV files.
2. **Raw Staging:** Data is securely backed up as immutable Parquet files in the `staging/raw/` directory.
3. **Data Quality & Validation:** The rules engine checks for schema compliance, null keys, and statistical outliers. Invalid records are routed to `staging/outliers/`.
4. **Transformation:** Data is cleansed, standardized, and joined (e.g., merging headers and details) and stored in `staging/valid/`.
5. **OLTP Load:** Validated data is loaded into a simulated 3NF relational database structure.
6. **OLAP Build:** Data is transformed into a dimensional Star Schema (Fact and Dimension tables) optimized for fast querying.
7. **Analytics:** Final datasets are generated and cached for the web dashboard to visualize KPIs and trends.

## 5. Functional Requirements
- The system must provide a user interface to view, add, edit, and delete Product and Customer records.
- The pipeline must execute on-demand via the dashboard and rebuild the data models.
- The system must apply data quality rules and block execution if fatal schema violations occur.
- The system must track all manual data modifications in a persistent Audit Log.
- The dashboard must display calculated KPIs including Total Sales, Total Orders, Average Order Value, and Data Quality counts.
- The dashboard must render interactive charts (Sales Trends, Color Distributions, etc.).

## 6. Non-Functional Requirements
- **Performance:** Intermediate staging must use the `.parquet` file format to minimize disk I/O and increase read/write speeds.
- **Usability:** The web application must be responsive and provide clear flash messages for user actions (Success/Error).
- **Aesthetics:** The UI must adhere to a premium, dark-mode "Green & Black" glassmorphic aesthetic to ensure a modern user experience.
- **Data Integrity:** The system must enforce referential integrity (e.g., preventing the deletion of a Product if it is referenced in historical sales records).

## 7. Production Resilience Features
The system is built around 4 core pillars of resilient data engineering:
* **Staging and Validation:** The architecture implements secure staging areas separating Raw, Valid, and Quarantined data. It utilizes a rules engine that actively enforces schema validation, missing value checks, and outlier detection before downstream loading.
* **Idempotency:** The pipeline is designed for safe reruns. It utilizes `TRUNCATE / INSERT` patterns and safe Parquet file overwrites, guaranteeing that executing the pipeline multiple times does not result in data duplication or unintended side effects.
* **Atomicity:** The system enforces "all-or-nothing" transaction blocks. Using Python `try...except` contexts and database transaction managers, it ensures that if a data quality rule fails midway, the entire load is rolled back to prevent partial, corrupted data states.
* **Error Handling & Replay:** An integrated Audit Log tracks all operational changes. The dashboard provides a distinct **"Replay Fix"** feature, allowing data engineers to explicitly backfill data and trigger historical pipeline runs from specific points in time to resolve historical anomalies.
