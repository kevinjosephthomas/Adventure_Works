"""
Regenerates notebooks/03_batch_pipeline.ipynb as valid JSON.
Run: python generate_nb03.py
"""
import json, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

def md(source: str, cell_id: str) -> dict:
    return {"cell_type":"markdown","id":cell_id,"metadata":{},"source":[source]}

def code(source: str, cell_id: str) -> dict:
    return {"cell_type":"code","execution_count":None,"id":cell_id,
            "metadata":{},"outputs":[],"source":[source]}

cells = [

# ── TITLE ──────────────────────────────────────────────────────────────────────
md("""# AdventureWorks — Exercise 3: Python Batch Pipeline

**Course:** Data Engineering  
**Notebook:** 03 — Batch Pipeline, Incremental Load & CDC  
**Assistant:** Antigravity  

---

## What This Notebook Covers

| # | Section | Purpose |
|---|---------|---------|
| 1 | Setup | Import modules |
| 2 | Full Refresh Pipeline | Run `run_pipeline()` — the complete pipeline |
| 3 | Pipeline Report | Understand every field of the execution report |
| 4 | Batch Execution Log | Read the log file |
| 5 | Incremental Load Demo | Show how only new/changed records could be loaded |
| 6 | CDC Demo | Detect NEW / UPDATED / UNCHANGED product records |
| 7 | Full Refresh vs Incremental | Side-by-side comparison |
| 8 | Summary | What was accomplished |

> **Important:** The main pipeline (`run_pipeline()`) always uses **Full Refresh**.  
> Sections 5 and 6 are standalone *demonstrations* — they do NOT replace the main pipeline.
""", "title"),

# ── SECTION 1 ──────────────────────────────────────────────────────────────────
md("---\n## Section 1 — Setup\n", "s1-header"),

code("""import sys
import re
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

NOTEBOOK_DIR = Path().resolve()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from config import (
    LOGS_DIR, PIPELINE_LOG_FILE, STAGING_VALID,
    STAGING_OUTLIERS, DASHBOARD_DIR, ensure_directories,
)
from ingestion      import extract_all, stage_raw
from validation     import validate_all
from transformation import transform_all, transform_product
from pipeline       import run_pipeline, print_pipeline_report
from analytics      import run_analytics, compute_kpis

pd.set_option('display.max_columns', 15)
pd.set_option('display.float_format', '{:,.2f}'.format)
ensure_directories()

print('All modules loaded.')
print(f'Project Root : {PROJECT_ROOT}')
print(f'Log File     : {PIPELINE_LOG_FILE}')""", "s1-code"),

# ── SECTION 2 ──────────────────────────────────────────────────────────────────
md("""---
## Section 2 — Run the Full Refresh Pipeline

**Objective:**  
Call `run_pipeline()` to execute the complete 11-stage pipeline.

**What happens internally:**
```
Source CSVs
    ↓ [1] Extraction
    ↓ [2] Raw Staging  → staging/raw/
    ↓ [3-6] Validation (schema, nulls, dupes, outliers) → staging/valid/|invalid/|duplicates/|outliers/
    ↓ [7] Transformation
    ↓ [8] OLTP Load  → PostgreSQL (optional)
    ↓ [9] Star Schema Build
    ↓ [10] Analytics & Dashboard Data → dashboard/
    ↓ [11] Final Validation + Report
```

> **Full Refresh Guarantee:**  
> Add/modify/delete a row in a source CSV → call `run_pipeline()` → change appears everywhere.
""", "s2-header"),

code("""# Run the complete pipeline
# db_load=False = file-only mode (PostgreSQL not required)
# Change to db_load=True if you have PostgreSQL configured in src/config.py

print('Starting Full Refresh Pipeline ...\\n')
pipeline_report = run_pipeline(db_load=False)""", "s2-run"),

md("""**Explanation:**  
- `run_pipeline()` is defined in `src/pipeline.py`.  
- Each run gets a unique **Run ID** (8-character hex) for traceability.  
- The pipeline is **idempotent**: run it 10 times, same result (given same source data).  
- Command line equivalent: `python run_pipeline.py --no-db`
""", "s2-explain"),

# ── SECTION 3 ──────────────────────────────────────────────────────────────────
md("""---
## Section 3 — Understanding the Pipeline Report

**Objective:**  
Inspect every field of the pipeline report and understand what each means.
""", "s3-header"),

code("""print('Pipeline Report — Field Breakdown\\n')
print(f'  Run ID    : {pipeline_report[\"run_id\"]}')
print(f'             -> Unique identifier for this run (hex from UUID4)')
print()
print(f'  Start     : {pipeline_report[\"start_time\"]}')
print(f'  End       : {pipeline_report[\"end_time\"]}')
print(f'  Duration  : {pipeline_report[\"duration_sec\"]} seconds')
print()
print('  SOURCE RECORDS (read from CSV files):')
print(f'    Product          : {pipeline_report[\"product_source_records\"]:>8,}')
print(f'    Customer         : {pipeline_report[\"customer_source_records\"]:>8,}')
print(f'    SalesOrderHeader : {pipeline_report[\"header_source_records\"]:>8,}')
print(f'    SalesOrderDetail : {pipeline_report[\"detail_source_records\"]:>8,}')
print()
print('  VALIDATION METRICS:')
print(f'    Valid Records     : {pipeline_report[\"valid_records\"]:>8,}')
print(f'    Invalid Records   : {pipeline_report[\"invalid_records\"]:>8,}')
print(f'    Duplicate Records : {pipeline_report[\"duplicate_records\"]:>8,}')
print(f'    Outlier Records   : {pipeline_report[\"outlier_records\"]:>8,}')
print()
print('  OUTPUT:')
print(f'    OLTP Records      : {pipeline_report[\"oltp_records\"]}')
print(f'    FactSales Records : {pipeline_report[\"factsales_records\"]:>8,}')
print()
print(f'  STATUS: {pipeline_report[\"status\"]}')
print()
print('  Stages Completed:')
for i, stage in enumerate(pipeline_report['stages_completed'], 1):
    print(f'    {i}. {stage}')""", "s3-fields"),

code("""# Show KPIs from this pipeline run
kpi_path = DASHBOARD_DIR / 'kpi_summary.parquet'
if kpi_path.exists():
    kpi_df = pd.read_parquet(kpi_path)
    print('KPI Summary (from this pipeline run):\\n')
    for col in kpi_df.columns:
        val = kpi_df[col].iloc[0]
        if isinstance(val, float):
            print(f'  {col:<25} : ${val:>15,.2f}')
        elif isinstance(val, (int, np.integer)):
            print(f'  {col:<25} : {val:>15,}')
        else:
            print(f'  {col:<25} : {val}')""", "s3-kpis"),

md("""**Explanation:**  
- The pipeline report is the \"receipt\" of every run — answers: *what happened, when, how many records?*  
- `valid_records` = sum of records passing null checks across all 4 tables.  
- `outlier_records` = flagged but still processed (not discarded).  
- `factsales_records` should match `detail_source_records` (unless FK filtering removes rows).  
- KPIs are saved to `dashboard/kpi_summary.parquet` — used by the dashboard notebook in Step 5.
""", "s3-explain"),

# ── SECTION 4 ──────────────────────────────────────────────────────────────────
md("""---
## Section 4 — Pipeline Log

**Objective:**  
Read the structured log file written by every pipeline run.  
Every run appends to `logs/pipeline.log`.
""", "s4-header"),

code("""log_file = PIPELINE_LOG_FILE

if log_file.exists():
    all_lines = log_file.read_text(encoding='utf-8').splitlines()
    print(f'Log file     : {log_file}')
    print(f'Total lines  : {len(all_lines)}')
    print()
    print('--- Most recent 25 log lines ---')
    for line in all_lines[-25:]:
        print(line)
else:
    print('Log file not found — run the pipeline first.')""", "s4-log"),

code("""# Parse run history from the log
if log_file.exists():
    all_lines = log_file.read_text(encoding='utf-8').splitlines()
    starts = [l for l in all_lines if 'PIPELINE START' in l]
    ends   = [l for l in all_lines if 'Status:' in l and 'Duration:' in l]

    print(f'Total pipeline runs recorded in log: {len(starts)}')
    print()
    for s, e in zip(starts, ends):
        run_match = re.search(r'Run ID: ([A-Z0-9]+)', s)
        run_id    = run_match.group(1) if run_match else 'unknown'
        status    = 'SUCCESS' if 'SUCCESS' in e else 'FAILED'
        dur_match = re.search(r'Duration: (\\S+)s', e)
        duration  = dur_match.group(1) if dur_match else 'N/A'
        ts        = s[1:20] if len(s) > 20 else ''
        print(f'  Run: {run_id:<12}  Status: {status:<8}  Duration: {duration}s  [{ts}]')""", "s4-history"),

md("""**Explanation:**  
- The log uses Python's `logging` module — no external dependencies.  
- Each entry includes: `[timestamp] [LEVEL] [run_id] message`.  
- Runs **append** to the log — full history is preserved.  
- In production, this log would feed into a monitoring system (Grafana, Datadog, etc.).
""", "s4-explain"),

# ── SECTION 5 ──────────────────────────────────────────────────────────────────
md("""---
## Section 5 — Incremental Load Demonstration

**Objective:**  
Show how only *new or modified* records could be processed.

> **This is a DEMONSTRATION only.**  
> The main `run_pipeline()` always uses Full Refresh.  
> This shows the concept of incremental loading separately.

| Strategy | Trigger Column | Logic |
|----------|---------------|-------|
| Product  | `ModifiedDate` | Load only products modified after a watermark |
| Sales    | `OrderDate`    | Load only orders placed after a watermark |

**Watermark** = timestamp of last successful pipeline run.
""", "s5-header"),

code("""# Load source data for incremental demo
raw = extract_all()
df_product = raw['product'].copy()
df_product['ModifiedDate'] = pd.to_datetime(df_product['ModifiedDate'], errors='coerce')

print('Product ModifiedDate range:')
print(f'  Earliest : {df_product[\"ModifiedDate\"].min()}')
print(f'  Latest   : {df_product[\"ModifiedDate\"].max()}')
print(f'  Total    : {len(df_product):,} records')""", "s5-setup"),

code("""def incremental_load_product(df, watermark_date):
    \"\"\"Return only product records modified AFTER the watermark.\"\"\"
    watermark = pd.Timestamp(watermark_date)
    df_ts = df.copy()
    df_ts['ModifiedDate'] = pd.to_datetime(df_ts['ModifiedDate'], errors='coerce')
    df_ts['ModifiedDate'] = df_ts['ModifiedDate'].dt.tz_localize(None)
    return df_ts[df_ts['ModifiedDate'] > watermark].copy()


watermark = '2024-01-01'
inc_product = incremental_load_product(df_product, watermark)

print('INCREMENTAL LOAD — Product')
print(f'  Watermark           : {watermark}')
print(f'  Full refresh count  : {len(df_product):,}')
print(f'  Incremental count   : {len(inc_product):,}')
pct = (1 - len(inc_product)/len(df_product))*100 if len(df_product) > 0 else 0
print(f'  Reduction           : {pct:.1f}% fewer records to process')
if not inc_product.empty:
    print(f'\\n  Sample (modified after {watermark}):')
    display(inc_product[['ProductID','Name','ModifiedDate']].head(5))""", "s5-product"),

code("""def incremental_load_sales(df_header, df_detail, watermark_date):
    \"\"\"Return only order detail rows for orders placed AFTER the watermark.\"\"\"
    hdr = df_header.copy()
    hdr['OrderDate'] = pd.to_datetime(hdr['OrderDate'], errors='coerce').dt.tz_localize(None)
    wm  = pd.Timestamp(watermark_date)
    new_order_ids = set(hdr[hdr['OrderDate'] > wm]['SalesOrderID'].astype(str))

    dtl = df_detail.copy()
    dtl['SalesOrderID'] = dtl['SalesOrderID'].astype(str)
    return dtl[dtl['SalesOrderID'].isin(new_order_ids)].copy()


watermark = '2024-06-01'
inc_sales = incremental_load_sales(raw['salesorderheader'], raw['salesorderdetail'], watermark)

print('INCREMENTAL LOAD — Sales')
print(f'  Watermark            : {watermark}')
print(f'  Full detail count    : {len(raw[\"salesorderdetail\"]):,}')
print(f'  Incremental count    : {len(inc_sales):,}')
pct = (1 - len(inc_sales)/len(raw[\"salesorderdetail\"]))*100
print(f'  Reduction            : {pct:.1f}% fewer records')""", "s5-sales"),

md("""**Explanation:**  
- **Watermark** is saved after each successful run. Next run loads only records newer than it.  
- **For Product**: `ModifiedDate` indicates when a product row last changed in the source.  
- **For Sales**: we filter `SalesOrderHeader.OrderDate` then join to `SalesOrderDetail`.  
- **Why Full Refresh for this project?** Incremental requires careful state management.  
  Full Refresh is always correct and runs in < 2 seconds for this dataset size.
""", "s5-explain"),

# ── SECTION 6 ──────────────────────────────────────────────────────────────────
md("""---
## Section 6 — CDC (Change Data Capture) Demonstration

**Objective:**  
Compare source vs. a previous snapshot and classify each record.

| Status | Meaning |
|--------|---------|
| **NEW** | In source, not in target |
| **UPDATED** | In both, but attribute values differ |
| **UNCHANGED** | In both, same values |
| **DELETED** | In target, not in source |

> **This is a DEMONSTRATION only.**  
> CDC is not the default pipeline mode.
""", "s6-header"),

code("""# Build current source product
val_result = validate_all(raw)
df_current = transform_product(val_result['valid']['product'])

# Simulate a previous snapshot (modify some records)
df_snapshot = df_current.copy()

# Simulate UPDATED: change ListPrice of 2 products
modified_ids = df_snapshot['ProductID'].iloc[10:12].tolist()
df_snapshot.loc[df_snapshot['ProductID'].isin(modified_ids), 'ListPrice'] *= 0.5
df_snapshot.loc[df_snapshot['ProductID'].isin(modified_ids), 'Color'] = 'OldColor'

# Simulate DELETED (in snapshot but removed from source)
fake_old = pd.DataFrame({
    'ProductID': [9901, 9902],
    'Name': ['Old Product Alpha', 'Old Product Beta'],
    'ListPrice': [99.99, 199.99],
    'StandardCost': [50.0, 100.0],
    'Color': ['Red', 'Blue'],
})
df_snapshot = pd.concat([df_snapshot, fake_old], ignore_index=True)

print(f'Current source count     : {len(df_current)}')
print(f'Simulated snapshot count : {len(df_snapshot)}')
print(f'Modified ProductIDs      : {modified_ids}')
print(f'Extra (deleted) IDs      : [9901, 9902]')""", "s6-setup"),

code("""def run_cdc(df_source, df_target, key_col='ProductID', compare_cols=None):
    \"\"\"
    CDC: classify each record as NEW / UPDATED / UNCHANGED / DELETED.
    \"\"\"
    if compare_cols is None:
        compare_cols = ['Name', 'ListPrice', 'StandardCost', 'Color']

    src = df_source[[key_col] + [c for c in compare_cols if c in df_source.columns]].copy()
    tgt = df_target[[key_col] + [c for c in compare_cols if c in df_target.columns]].copy()
    src[key_col] = pd.to_numeric(src[key_col], errors='coerce')
    tgt[key_col] = pd.to_numeric(tgt[key_col], errors='coerce')

    src_ids     = set(src[key_col].dropna())
    tgt_ids     = set(tgt[key_col].dropna())
    new_ids     = src_ids - tgt_ids
    deleted_ids = tgt_ids - src_ids
    common_ids  = src_ids & tgt_ids

    src_c = src[src[key_col].isin(common_ids)].set_index(key_col)
    tgt_c = tgt[tgt[key_col].isin(common_ids)].set_index(key_col)
    cols  = [c for c in compare_cols if c in src_c.columns and c in tgt_c.columns]
    idx   = src_c.index.intersection(tgt_c.index)

    changed_mask  = (src_c.loc[idx, cols].fillna('').astype(str) !=
                     tgt_c.loc[idx, cols].fillna('').astype(str)).any(axis=1)
    changed_ids   = set(idx[changed_mask])
    unchanged_ids = common_ids - changed_ids - deleted_ids

    rows = (
        [{'ProductID': p, 'cdc_status': 'NEW'}       for p in sorted(new_ids)] +
        [{'ProductID': p, 'cdc_status': 'UPDATED'}    for p in sorted(changed_ids)] +
        [{'ProductID': p, 'cdc_status': 'DELETED'}    for p in sorted(deleted_ids)] +
        [{'ProductID': p, 'cdc_status': 'UNCHANGED'}  for p in sorted(list(unchanged_ids)[:5])]
    )
    summary = {
        'new': len(new_ids), 'updated': len(changed_ids),
        'deleted': len(deleted_ids), 'unchanged': len(unchanged_ids),
    }
    return pd.DataFrame(rows), summary

print('CDC function defined.')""", "s6-logic"),

code("""cdc_df, cdc_summary = run_cdc(
    df_source=df_current, df_target=df_snapshot,
    compare_cols=['Name','ListPrice','StandardCost','Color'],
)

print('CDC REPORT')
print('=' * 45)
print(f'  NEW        : {cdc_summary[\"new\"]:>5,}   (in source, not in target)')
print(f'  UPDATED    : {cdc_summary[\"updated\"]:>5,}   (exists in both, values changed)')
print(f'  DELETED    : {cdc_summary[\"deleted\"]:>5,}   (in target, not in source)')
print(f'  UNCHANGED  : {cdc_summary[\"unchanged\"]:>5,}   (exists in both, no change)')
print('=' * 45)
print()
print('Sample CDC records:')
display(cdc_df.sort_values('cdc_status'))""", "s6-run"),

code("""# Equivalent PostgreSQL UPSERT logic
print('Equivalent PostgreSQL UPSERT:')
print('''
  -- Handle NEW + UPDATED in one statement (PostgreSQL 9.5+):
  INSERT INTO oltp.product (product_id, name, list_price, color, ...)
  SELECT product_id, name, list_price, color, ...
  FROM   staging_product
  ON CONFLICT (product_id)
  DO UPDATE SET
    name          = EXCLUDED.name,
    list_price    = EXCLUDED.list_price,
    color         = EXCLUDED.color;

  -- Handle DELETED:
  DELETE FROM oltp.product
  WHERE product_id NOT IN (SELECT product_id FROM staging_product);
''')""", "s6-upsert"),

md("""**Explanation:**  
- CDC detects changes by comparing current source data to a previous snapshot.  
- Set operations (`src_ids - tgt_ids`) find NEW and DELETED records.  
- Column comparison finds UPDATED records.  
- **UPSERT** (`ON CONFLICT DO UPDATE`) handles NEW + UPDATED in one SQL statement.  
- **Why not use CDC as the default?** It requires maintaining the previous snapshot,  
  and edge cases (missing snapshots, schema changes) add complexity.  
  Full Refresh eliminates all of that.
""", "s6-explain"),

# ── SECTION 7 ──────────────────────────────────────────────────────────────────
md("""---
## Section 7 — Full Refresh vs Incremental: Side-by-Side
""", "s7-header"),

code("""comparison = pd.DataFrame([
    ['Approach',         'Full Refresh (our main pipeline)', 'Incremental / CDC'],
    ['Mechanism',        'Truncate + reload everything',     'Load only changed records'],
    ['Handles deletes?', 'Yes — table fully rebuilt',        'Needs explicit delete logic'],
    ['Handles updates?', 'Yes — always uses latest value',   'Needs UPDATED detection'],
    ['Complexity',       'Low',                              'High (watermark, CDC)'],
    ['Speed (small)',    'Fast (<2s for 120K rows)',          'Faster per run'],
    ['Speed (large)',    'Slow (scans all rows)',             'Fast (scans only changes)'],
    ['Correctness risk', 'Very low',                         'Higher (bug risk in change logic)'],
    ['Idempotent?',      'Yes',                              'Only with careful design'],
    ['Best for',         'This project',                     'Massive datasets, frequent runs'],
], columns=['Dimension','Full Refresh','Incremental'])

display(comparison.set_index('Dimension'))""", "s7-comparison"),

code("""# Idempotency test — run twice, check results match
print('Idempotency Test: run pipeline twice, compare results\\n')

report1 = run_pipeline(db_load=False)
report2 = run_pipeline(db_load=False)

print('\\nRun 1 vs Run 2:')
fields = ['product_source_records','customer_source_records',
          'detail_source_records','factsales_records','status']
all_match = True
for f in fields:
    v1, v2 = report1[f], report2[f]
    match  = v1 == v2
    all_match = all_match and match
    tag = '[OK]' if match else '[MISMATCH]'
    print(f'  {tag} {f:<32}: {v1} vs {v2}')

print()
if all_match:
    print('All metrics match — pipeline is IDEMPOTENT.')
else:
    print('MISMATCH detected — investigate before using in production.')""", "s7-idempotency"),

md("""**Explanation:**  
- **Idempotency** = same input → same output, regardless of how many times you run it.  
- Our pipeline achieves this by TRUNCATING target tables before loading.  
- Run IDs differ (new UUID each time) — but the **data** is identical.  
- This is a critical production property: safe to retry on failure without creating duplicates.
""", "s7-explain"),

# ── SECTION 8 ──────────────────────────────────────────────────────────────────
md("---\n## Section 8 — Step 3 Summary\n", "s8-header"),

code("""print('=' * 60)
print('  STEP 3 COMPLETE — BATCH PIPELINE SUMMARY')
print('=' * 60)
print()
print('  Files Created:')
print('    src/analytics.py     - KPI + chart dataset builder')
print('    src/pipeline.py      - run_pipeline() orchestrator')
print('    run_pipeline.py      - CLI entry point')
print()
print('  Run Command:')
print('    python run_pipeline.py          # with DB')
print('    python run_pipeline.py --no-db  # file-only mode')
print()
print('  Dashboard Output Files:')
dashboard_files = sorted(DASHBOARD_DIR.glob('*.parquet'))
for f in dashboard_files:
    size_kb = round(f.stat().st_size / 1024, 1)
    print(f'    {f.name:<40} ({size_kb:>6.1f} KB)')
print()
log_lines = len(PIPELINE_LOG_FILE.read_text(encoding='utf-8').splitlines()) if PIPELINE_LOG_FILE.exists() else 0
print(f'  Pipeline log: {PIPELINE_LOG_FILE}')
print(f'  Log lines   : {log_lines}')
print()
print('  Next: Step 4 - Resilient and Production-Ready Pipeline')
print('=' * 60)""", "s8-summary"),

md("""---
## Concepts Learned in This Notebook

| Concept | Definition |
|---------|------------|
| **Batch Pipeline** | Runs on a schedule to process a batch of data at once |
| **Full Refresh** | Every run rebuilds all data from source |
| **Idempotency** | Running multiple times produces the same result |
| **Run ID** | Unique identifier per execution — used for traceability |
| **Pipeline Log** | Structured text file recording every event per run |
| **Watermark** | Timestamp of last successful run — filters incremental records |
| **Incremental Load** | Process only records modified since the last watermark |
| **CDC** | Change Data Capture — classify records as NEW/UPDATED/DELETED/UNCHANGED |
| **UPSERT** | INSERT if new, UPDATE if exists (PostgreSQL: ON CONFLICT DO UPDATE) |
| **KPI** | Key Performance Indicator — single number summarizing business health |

---
*Next notebook:* `04_resilient_pipeline.ipynb` — Error handling, atomicity, replay, backfill  
*Developed with:* **Antigravity** — AI Coding Assistant
""", "s8-concepts"),
]

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10.0"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = PROJECT_ROOT / "notebooks" / "03_batch_pipeline.ipynb"
out.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Written: {out}")

# Validate
try:
    json.loads(out.read_text(encoding="utf-8"))
    print("JSON validation: PASSED")
except json.JSONDecodeError as e:
    print(f"JSON validation: FAILED — {e}")
    sys.exit(1)
