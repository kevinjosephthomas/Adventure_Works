# AdventureWorks End-to-End Data Engineering and Analytics Pipeline

> **Development Assistant:** Antigravity  
> **Platform:** Local Windows Machine — No Docker, No Cloud, No Databricks

---

## Project Overview

A complete, educational Data Engineering project built on the AdventureWorks dataset.  
The pipeline runs **100% locally** on Windows using Python, Pandas, PySpark, PostgreSQL, and Jupyter Notebooks.

---

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Data Processing | Pandas, PySpark (local mode) |
| Database | PostgreSQL + SQLAlchemy |
| Notebooks | Jupyter Notebook |
| Visualization | Plotly, Matplotlib |
| File Format | Parquet (staging), CSV (source) |

---

## Project Structure

```
AdventureWorks_DataEngineering/
│
├── data/
│   └── AdventureWorks/
│       ├── Product.csv
│       ├── Customer.csv
│       ├── SalesOrderHeader.csv
│       └── SalesOrderDetail.csv
│
├── src/
│   ├── config.py           ← CHANGE DATA_PATH HERE
│   ├── ingestion.py
│   ├── validation.py
│   ├── transformation.py
│   ├── oltp.py
│   ├── olap.py
│   ├── analytics.py
│   └── pipeline.py
│
├── notebooks/
│   ├── 01_core_etl.ipynb           ← Step 1: Extract & Stage
│   ├── 02_schema_design.ipynb      ← Step 2: OLTP + Star Schema
│   ├── 03_batch_pipeline.ipynb     ← Step 3: Batch Pipeline
│   ├── 04_resilient_pipeline.ipynb ← Step 4: Production-Ready
│   └── 05_analytics_dashboard.ipynb← Step 5: Analytics & Charts
│
├── sql/
│   ├── oltp_schema.sql
│   ├── star_schema.sql
│   └── analytics.sql
│
├── staging/
│   ├── raw/          ← Raw extracted data (Parquet)
│   ├── valid/        ← Records passing validation
│   ├── invalid/      ← Records failing validation
│   ├── duplicates/   ← Detected duplicate records
│   └── outliers/     ← Statistical outliers
│
├── logs/
│   └── pipeline.log
│
├── dashboard/        ← Chart-ready datasets
│
├── requirements.txt
├── README.md
└── run_pipeline.py   ← One command to run everything
```

---

## Quick Start

### 1. Install Dependencies

```powershell
cd f:\DE_CAT_1\AdventureWorks_DataEngineering
pip install -r requirements.txt
```

### 2. Configure Your Data Path

Open `src/config.py` and set `DATA_PATH`:

```python
# Option 1: Use the bundled data/ folder (default)
DATA_PATH = PROJECT_ROOT / "data" / "AdventureWorks"

# Option 2: Point to any local folder
DATA_PATH = Path(r"C:\MyData\AdventureWorks")
```

### 3. Verify Configuration

```powershell
python src/config.py
```

### 4. Run Step 1 Notebook

```powershell
jupyter notebook notebooks/01_core_etl.ipynb
```

### 5. Run the Full Pipeline (after all steps are implemented)

```powershell
python run_pipeline.py
```

---

## Pipeline Architecture — Full Refresh

```
SOURCE CSVs
    ↓
EXTRACTION  (src/ingestion.py)
    ↓
RAW STAGING  (staging/raw/)
    ↓
VALIDATION   (src/validation.py)
    ↓
TRANSFORMATION  (src/transformation.py)
    ↓
OLTP LOAD  (src/oltp.py → PostgreSQL)
    ↓
OLAP / STAR SCHEMA  (src/olap.py → PostgreSQL)
    ↓
ANALYTICS  (src/analytics.py)
    ↓
DASHBOARD DATA  (dashboard/)
    ↓
PIPELINE REPORT
```

**Full Refresh means:**
- Every pipeline run starts from source CSVs
- All staging and database tables are rebuilt from scratch
- If you add/modify/delete a row in a CSV and re-run → the change appears everywhere
- No dependency on previous runs

---

## Notebooks

| Notebook | Exercise | Status |
|----------|----------|--------|
| `01_core_etl.ipynb` | Exercise 1 — Core ETL | ✅ Complete |
| `02_schema_design.ipynb` | Exercise 2 — Schema Design | 🔲 Coming (Step 2) |
| `03_batch_pipeline.ipynb` | Exercise 3 — Batch Pipeline | 🔲 Coming (Step 3) |
| `04_resilient_pipeline.ipynb` | Exercise 4 — Resilient Pipeline | 🔲 Coming (Step 4) |
| `05_analytics_dashboard.ipynb` | Exercise 5 — Analytics Dashboard | 🔲 Coming (Step 5) |

---

## AI Assistant — Natural Language Questions

The project prepares clean datasets that an AI assistant can query.  
Example questions and where to find the answers:

| Question | Answer Source |
|----------|--------------|
| "What are the top 10 products by sales?" | `dashboard/top_products.parquet` |
| "Which product has the highest ListPrice?" | `staging/valid/valid_product.parquet` |
| "Which customer generated the highest sales?" | `dashboard/customer_sales.parquet` |
| "What is the monthly sales trend?" | `dashboard/sales_by_month.parquet` |
| "How many invalid records were detected?" | `logs/pipeline.log` + pipeline report |
| "How many duplicate records were found?" | `staging/duplicates/` |
| "What changed during the latest CDC run?" | `dashboard/cdc_report.parquet` |

---

## Source Data

| File | Table | Description |
|------|-------|-------------|
| `Product.csv` | Product | 504 products with cost, price, category |
| `Customer.csv` | Customer | 19,820 customers |
| `SalesOrderHeader.csv` | SalesOrderHeader | Order-level information |
| `SalesOrderDetail.csv` | SalesOrderDetail | Line-item detail per order |

**Format:** Tab-separated (`\t`), no header row, UTF-8 encoding.

---

*Developed incrementally. Each step is confirmed by the user before the next is built.*
