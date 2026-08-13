# AdventureWorks Data Engineering — Step 1 Completion Summary

> **Assistant:** Antigravity  
> **Date:** 2026-08-12  
> **Status:** ✅ Step 1 Complete — Awaiting confirmation to proceed to Step 2

---

## What Was Built in Step 1

Step 1 covers: project scaffolding, central configuration, data extraction from the 4 source CSVs, schema inspection, record counts, sample previews, and raw staging.

---

## Project Location

```
f:\DE_CAT_1\AdventureWorks_DataEngineering\
```

---

## Folder Structure Created

```
AdventureWorks_DataEngineering/
│
├── data/
│   └── AdventureWorks/
│       ├── Product.csv             (86.5 KB  — 504 products)
│       ├── Customer.csv            (1.6 MB   — 19,820 customers)
│       ├── SalesOrderHeader.csv    (7.5 MB   — 31,465 orders)
│       └── SalesOrderDetail.csv    (13.1 MB  — 121,317 line items)
│
├── src/
│   ├── __init__.py                 (makes src a Python package)
│   ├── config.py                   ← CENTRAL CONFIG — change DATA_PATH here
│   └── ingestion.py                ← Extraction + raw staging logic
│
├── notebooks/
│   └── 01_core_etl.ipynb           ← Educational ETL notebook (8 sections)
│
├── staging/
│   ├── raw/
│   │   ├── raw_product.parquet          (52.6 KB)
│   │   ├── raw_customer.parquet         (1.1 MB)
│   │   ├── raw_salesorderheader.parquet (3.0 MB)
│   │   └── raw_salesorderdetail.parquet (5.7 MB)
│   ├── valid/        (empty — populated in Step 2)
│   ├── invalid/      (empty — populated in Step 2)
│   ├── duplicates/   (empty — populated in Step 2)
│   └── outliers/     (empty — populated in Step 2)
│
├── sql/              (empty — populated in Step 2)
├── logs/             (empty — populated in Step 2)
├── dashboard/        (empty — populated in Step 5)
│
├── requirements.txt
└── README.md
```

---

## Files Created

### 1. `requirements.txt`
**Path:** [requirements.txt](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/requirements.txt)

Lists all Python dependencies for the full project:

| Package | Purpose |
|---------|---------|
| `pandas >= 2.0` | Core data processing |
| `numpy >= 1.24` | Numeric operations |
| `pyspark >= 3.4` | Distributed processing (local mode) |
| `psycopg2-binary` | PostgreSQL driver |
| `sqlalchemy >= 2.0` | ORM + database abstraction |
| `jupyter`, `notebook` | Notebook environment |
| `plotly >= 5.15` | Interactive visualizations |
| `matplotlib`, `seaborn` | Static charts |
| `delta-spark >= 2.4` | Delta Lake (local, no Docker) |
| `python-dotenv` | Environment variable loading |
| `pyarrow` | Parquet read/write |
| `tabulate`, `tqdm`, `colorama` | Utilities |

---

### 2. `src/config.py`
**Path:** [src/config.py](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/src/config.py)

The **single source of truth** for all project configuration. No paths are hard-coded anywhere else.

**Key settings:**

```python
# ← CHANGE THIS to point to your CSVs
DATA_PATH = PROJECT_ROOT / "data" / "AdventureWorks"
```

| Setting | Value |
|---------|-------|
| `DATA_PATH` | `f:\DE_CAT_1\AdventureWorks_DataEngineering\data\AdventureWorks` |
| `CSV_SEPARATOR` | `\t` (tab) |
| `CSV_HAS_HEADER` | `False` (files have no header row) |
| `STAGING_RAW` | `f:\...\staging\raw\` |
| `PIPELINE_MODE` | `FULL_REFRESH` |
| `OUTLIER_IQR_MULTIPLIER` | `1.5` |
| `DB_CONNECTION_STRING` | `postgresql+psycopg2://postgres:postgres@localhost:5432/adventureworks_de` |

**Column definitions** for all 4 tables are defined here positionally (since CSVs have no header):

- `PRODUCT_COLUMNS` — 25 columns
- `CUSTOMER_COLUMNS` — 7 columns
- `SALESORDERHEADER_COLUMNS` — 26 columns
- `SALESORDERDETAIL_COLUMNS` — 11 columns

**Self-test:** `python src/config.py` — verifies all 4 source files are found.

---

### 3. `src/ingestion.py`
**Path:** [src/ingestion.py](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/src/ingestion.py)

Handles all data extraction and raw staging. Fully tested — self-test passes.

**Public functions:**

| Function | Returns |
|----------|---------|
| `extract_product()` | Product DataFrame |
| `extract_customer()` | Customer DataFrame |
| `extract_salesorderheader()` | SalesOrderHeader DataFrame |
| `extract_salesorderdetail()` | SalesOrderDetail DataFrame |
| `extract_all()` | `dict` of all 4 DataFrames |
| `stage_raw(raw)` | Writes Parquet to `staging/raw/`, returns paths |
| `display_summary(raw)` | Prints schema + records + samples for all tables |

**Design decisions:**
- `header=None` + positional `names=` — handles files with no header row
- `sep='\t'` — correct tab delimiter for AdventureWorks exports
- `on_bad_lines='warn'` — logs malformed rows without crashing
- `_source_file` and `_ingested_at` metadata columns added for lineage tracking
- **Full Refresh**: `stage_raw()` always overwrites; never appends old data

---

### 4. `notebooks/01_core_etl.ipynb`
**Path:** [notebooks/01_core_etl.ipynb](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/notebooks/01_core_etl.ipynb)

Educational Jupyter Notebook with 8 clearly separated sections. Each section has:
- **Objective** — what and why
- **Code** — clean, annotated
- **Output** — visible in notebook
- **Explanation** — plain English explanation of the data engineering concept

| Section | Content |
|---------|---------|
| 1 — Configuration & Setup | Import libraries, load config, verify project root |
| 2 — Source File Check | Verify all 4 CSVs exist, show file sizes |
| 3 — Data Extraction | `read_source_csv()` helper, extract all 4 tables |
| 4 — Schema Display | `show_schema()` — column names, dtypes, null counts |
| 5 — Record Counts | Summary table with row counts + memory usage |
| 6 — Sample Records | `show_sample()` — first 5 rows, plus `.describe()` stats |
| 7 — Raw Staging | Write Parquet to `staging/raw/`, verify with read-back check |
| 8 — Step 1 Summary | Final confirmation table + concepts learned |

**Concepts taught:** ETL, config-driven design, TSV format, schema inspection, null detection, raw staging, Parquet, Full Refresh, lineage metadata, data quality gates.

---

### 5. `README.md`
**Path:** [README.md](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/README.md)

Project overview document covering:
- Technology stack
- Full folder structure
- Quick start instructions
- Pipeline architecture diagram (Full Refresh flow)
- Notebook index with completion status
- AI assistant natural language query examples
- Source data description

---

## Data Extracted — Record Counts

| Table | Records | Columns | Raw Staging File | Size |
|-------|---------|---------|-----------------|------|
| Product | **504** | 25 | `raw_product.parquet` | 52.6 KB |
| Customer | **19,820** | 7 | `raw_customer.parquet` | 1.1 MB |
| SalesOrderHeader | **31,465** | 26 | `raw_salesorderheader.parquet` | 3.0 MB |
| SalesOrderDetail | **121,317** | 11 | `raw_salesorderdetail.parquet` | 5.7 MB |
| **TOTAL** | **173,106** | | | **9.9 MB** |

---

## Source Data Facts Discovered

- **Delimiter:** Tab (`\t`) — not comma-separated
- **Header:** None — column names assigned positionally from `config.py`
- **Encoding:** UTF-8
- **Numeric columns in Product** still read as `object` (string) at raw stage — type conversion happens in Step 2
- **Nullable columns:** `Color`, `Size`, `Weight`, `ProductLine`, `Class`, `Style` have high null rates (expected — not all products have these attributes)
- **SalesOrderDetail.LineTotal** is pre-calculated in the source: `OrderQty × UnitPrice × (1 - UnitPriceDiscount)`

---

## Self-Tests Run

```powershell
python src/config.py     # PASSED - all 4 source files found
python src/ingestion.py  # PASSED - all tables extracted + staged
```

---

## How to Open the Notebook

```powershell
cd f:\DE_CAT_1\AdventureWorks_DataEngineering
jupyter notebook notebooks/01_core_etl.ipynb
```

---

## How to Change the Data Path

Open [src/config.py](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/src/config.py), find line ~35, and change:

```python
# Option 1: bundled data folder (current default)
DATA_PATH = PROJECT_ROOT / "data" / "AdventureWorks"

# Option 2: any folder on your machine
DATA_PATH = Path(r"C:\YourFolder\AdventureWorks")
```

No other file needs to be changed.

---

## What Is NOT Yet Implemented (coming in later steps)

| Feature | Planned Step |
|---------|-------------|
| Data transformation (type casting, deduplication, NULL handling) | Step 2 |
| Schema validation, null/duplicate/outlier detection | Step 2 |
| OLTP schema + PostgreSQL tables | Step 2 |
| Star Schema (DimProduct, DimCustomer, DimDate, FactSales) | Step 2 |
| OLAP operations (roll-up, drill-down, slice, dice) | Step 2 |
| Batch pipeline (`run_pipeline.py`) | Step 3 |
| CDC demonstration | Step 3 |
| Incremental load demonstration | Step 3 |
| Error handling, atomicity, replay, backfill | Step 4 |
| Analytics dashboard (Plotly charts, KPIs) | Step 5 |

---

## Next Step

> **Confirm when ready and Step 2 will be built:**  
> Data Transformation, Validation, OLTP Schema, and Star Schema Design.
