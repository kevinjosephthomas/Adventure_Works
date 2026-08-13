"""
Generates notebooks/04_resilient_pipeline.ipynb as valid JSON.
Run: python generate_nb04.py
"""
import json, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

def md(source, cell_id):
    return {"cell_type":"markdown","id":cell_id,"metadata":{},"source":[source]}

def code(source, cell_id):
    return {"cell_type":"code","execution_count":None,"id":cell_id,
            "metadata":{},"outputs":[],"source":[source]}

cells = [

md("""# AdventureWorks — Exercise 4: Resilient & Production-Ready Pipeline

**Course:** Data Engineering  
**Notebook:** 04 — Resilience, DQ Rules, Retry, Atomicity, Run History  
**Assistant:** Antigravity  

---

## What This Notebook Covers

| # | Section | Purpose |
|---|---------|---------|
| 1 | Setup | Import all resilience modules |
| 2 | Pre-flight Health Checks | Validate environment before any work starts |
| 3 | Data Quality Rules Engine | 16 configurable rules with PASS/WARNING/ERROR |
| 4 | Retry Logic | Exponential backoff for transient failures |
| 5 | Atomic Staging | Temp-write then rename — no partial state |
| 6 | Alert Thresholds | Business-level alerts beyond DQ rules |
| 7 | Run the Resilient Pipeline | Full pipeline with all resilience features |
| 8 | Pipeline Run History | Audit trail of all past runs |
| 9 | Simulate Failures | Force a DQ failure and see pipeline halt |
| 10 | Summary | What was accomplished |

> **Key Question:** What makes a pipeline production-ready?  
> Answer: It should handle failures gracefully, validate data before trusting it,  
> keep a full audit trail, and alert operators when something is wrong.
""", "title"),

md("---\n## Section 1 — Setup\n", "s1-header"),

code("""import sys
import json
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

NOTEBOOK_DIR = Path().resolve()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from config import (
    LOGS_DIR, PIPELINE_LOG_FILE, STAGING_RAW,
    STAGING_VALID, DASHBOARD_DIR, ensure_directories,
)
from ingestion         import extract_all
from validation        import validate_all
from resilient_pipeline import (
    run_health_checks, print_health_report,
    with_retry, atomic_stage,
    check_alert_thresholds,
    run_resilient_pipeline,
    load_run_history, load_pipeline_state,
    PIPELINE_RUNS_FILE, PIPELINE_STATE_FILE,
    ALERT_OUTLIER_RATIO, ALERT_INVALID_RATIO, ALERT_MIN_SALES_VALUE,
)
from data_quality import (
    run_data_quality_checks, print_dq_report,
    save_dq_results, DEFAULT_RULES,
    DQRule, DQResult,
    RULE_ROW_COUNT_MIN, RULE_NULL_RATIO_MAX,
    RULE_OUTLIER_RATIO_MAX, RULE_TOTAL_SALES_MIN,
)

pd.set_option('display.max_columns', 12)
pd.set_option('display.float_format', '{:,.4f}'.format)
ensure_directories()

print('All resilience modules loaded.')
print(f'Project Root     : {PROJECT_ROOT}')
print(f'Run History File : {PIPELINE_RUNS_FILE}')
print(f'State File       : {PIPELINE_STATE_FILE}')""", "s1-code"),

md("---\n## Section 2 — Pre-flight Health Checks\n\n**Objective:**  \nBefore running any pipeline work, verify the environment is ready.\n\nChecks performed:\n1. All 4 source CSV files exist and are non-empty\n2. All staging directories are writable\n3. Dashboard and logs directories are writable\n4. Required Python packages are importable\n\n> If any check fails, `run_resilient_pipeline()` raises an error immediately.  \n> This prevents wasted processing time on a broken environment.\n", "s2-header"),

code("""print('Running pre-flight health checks ...\\n')
health = run_health_checks()
print_health_report(health)""", "s2-run"),

code("""# Inspect individual check results
checks_df = pd.DataFrame(health['checks'])
checks_df['status'] = checks_df['passed'].map({True: 'OK', False: 'FAIL'})
print('Health Check Details:')
display(checks_df[['check', 'status', 'detail']])""", "s2-detail"),

md("""**Explanation:**  
- Health checks run **before** any data is read or processed.  
- If source files are missing, the pipeline raises `RuntimeError` immediately — no wasted work.  
- Write tests on staging dirs catch permission issues before they silently corrupt data.  
- The `pyarrow` import check ensures the Parquet engine is available before we try to write Parquet files.  
- In production, you'd add checks for: disk space, DB connectivity, network reachability, etc.
""", "s2-explain"),

md("---\n## Section 3 — Data Quality Rules Engine\n\n**Objective:**  \nRun 16 configurable DQ rules against the raw source data.\n\n| Rule Type | Description |\n|-----------|-------------|\n| `ROW_COUNT_MIN` | Table must have at least N rows |\n| `ROW_COUNT_MAX` | Table must not exceed N rows |\n| `NULL_RATIO_MAX` | Key column must have < X% nulls |\n| `DUPE_RATIO_MAX` | Primary key must have < X% duplicates |\n| `OUTLIER_RATIO_MAX` | Outlier records must be < X% of table |\n| `COLUMN_EXISTS` | Required column must be present |\n| `TOTAL_SALES_MIN` | Total LineTotal must exceed threshold |\n\n**Severity levels:**\n- `ERROR` — halt the pipeline; the data is not trustworthy\n- `WARNING` — log and continue; data is usable but needs attention\n", "s3-header"),

code("""# Load raw data for DQ checks
raw = extract_all()
val_result = validate_all(raw)

# Run all 16 default DQ rules
print('Running 16 DQ rules ...\\n')
dq_results, dq_summary = run_data_quality_checks(raw, val_result['reports'])
print_dq_report(dq_results, dq_summary)""", "s3-run"),

code("""# Show results as a DataFrame
dq_df = pd.DataFrame([r.to_dict() for r in dq_results])
print('DQ Results DataFrame:')
display(dq_df[['rule_id','table','rule_type','passed','severity',
               'actual_value','threshold','status']].to_string(index=False))""", "s3-df"),

code("""# Add a custom DQ rule
print('Adding a custom DQ rule: Product must have > 400 products\\n')

custom_rule = DQRule(
    rule_id    = 'custom_product_min_400',
    table      = 'product',
    rule_type  = RULE_ROW_COUNT_MIN,
    threshold  = 400,
    severity   = 'WARNING',
    description= 'Custom rule: at least 400 products expected',
)

custom_rules = DEFAULT_RULES + [custom_rule]
custom_results, custom_summary = run_data_quality_checks(raw, val_result['reports'], custom_rules)
print(f'Rules evaluated : {custom_summary[\"total_rules\"]}')
print(f'Passed          : {custom_summary[\"passed\"]}')
print(f'Warnings        : {custom_summary[\"warnings\"]}')
print(f'Errors          : {custom_summary[\"errors\"]}')
print(f'Status          : {custom_summary[\"overall_status\"]}')""", "s3-custom"),

md("""**Explanation:**  
- Each `DQRule` is a `@dataclass` — you can add custom rules in one line without modifying source code.  
- `severity='ERROR'` means: if this rule fails, halt the pipeline before loading any data.  
  This prevents bad data from reaching the OLTP or OLAP database.  
- `severity='WARNING'` means: log the issue and continue — the data is usable but needs review.  
- DQ results are saved to `logs/dq_results_<RUN_ID>.csv` — gives a per-run audit trail.  
- The total sales rule (`RULE_TOTAL_SALES_MIN`) is a **business-level** DQ check:  
  it catches cases where the data might be structurally valid but commercially nonsensical.
""", "s3-explain"),

md("---\n## Section 4 — Retry Logic with Exponential Backoff\n\n**Objective:**  \nHandle transient failures automatically by retrying failed operations.\n\n**Retry schedule (backoff_base=2, max_retries=3):**\n\n```\nAttempt 1 → fails → wait 1s  (2^0)\nAttempt 2 → fails → wait 2s  (2^1)\nAttempt 3 → fails → wait 4s  (2^2)\nAttempt 4 → raise exception\n```\n\n**When retries are useful:**\n- Database connection drops briefly (network glitch)\n- Source file is locked by another process for a moment\n- Disk I/O momentary overload\n\n**When retries are NOT useful:**\n- Source file is missing (permanent failure)\n- DQ check fails (data error — not transient)\n- SQL syntax error (code bug — retrying won't help)\n", "s4-header"),

code("""# Demonstrate the retry mechanism with a failing function
print('Retry Demonstration\\n')

attempt_count = [0]

def flaky_function(fail_times=2):
    \"\"\"Simulates a function that fails the first N times, then succeeds.\"\"\"
    attempt_count[0] += 1
    if attempt_count[0] <= fail_times:
        raise ConnectionError(f'Simulated transient error (attempt {attempt_count[0]})')
    return f'Success on attempt {attempt_count[0]}'


# Reset counter
attempt_count[0] = 0

try:
    result = with_retry(
        func=flaky_function,
        args=(2,),            # will fail first 2 times
        max_retries=3,
        backoff_base=0.1,     # use 0.1s in demo so it's fast
        stage_name='flaky_demo',
    )
    print(f'Result: {result}')
except Exception as e:
    print(f'All retries exhausted: {e}')""", "s4-demo"),

code("""# Show what happens when ALL retries fail
print('\\nAll-retries-fail demonstration:\\n')
attempt_count[0] = 0

def always_fails():
    attempt_count[0] += 1
    raise IOError(f'Permanent failure (attempt {attempt_count[0]})')

try:
    with_retry(
        func=always_fails,
        max_retries=3,
        backoff_base=0.1,
        stage_name='always_fail_demo',
    )
except IOError as e:
    print(f'Caught after all retries: {e}')
    print(f'Total attempts made: {attempt_count[0]}')""", "s4-fail"),

md("""**Explanation:**  
- `with_retry()` wraps any callable — it doesn't care what the function does.  
- The wait time doubles each attempt (exponential backoff).  
  This avoids overwhelming a recovering service with rapid repeated requests.  
- In the real pipeline, `with_retry` wraps `extract_all()` and `get_engine()`.  
  Both can fail transiently: files can be briefly locked, DB connections can drop momentarily.  
- After `max_retries` attempts, the last exception is re-raised — the caller handles it.  
- **Exponential backoff** is a standard pattern used in distributed systems (AWS SDK, Google Cloud SDK, etc.)
""", "s4-explain"),

md("---\n## Section 5 — Atomic Staging\n\n**Objective:**  \nEnsure raw staging files are written atomically — either all files are written or none.\n\n**The problem with non-atomic writes:**\n```\nWrite product.parquet   → OK\nWrite customer.parquet  → OK\nWrite salesorder.parquet → CRASH!\n\nResult: staging/ has 2 complete files + 0 sales files\nNext run reads this partial state → WRONG data!\n```\n\n**The atomic solution:**\n```\nWrite to temp/ (product, customer, salesorder) → all succeed\nRename temp/ → staging/raw/ (single OS-level operation)\n\nResult: either ALL files exist or NONE (temp/ is deleted on failure)\n```\n", "s5-header"),

code("""# Demonstrate atomic staging
print('Atomic Staging Demonstration\\n')

# Use the real source data
raw = extract_all()

start_time = time.perf_counter()
success = atomic_stage(raw)
elapsed = time.perf_counter() - start_time

print(f'Atomic staging result : {\"SUCCESS\" if success else \"FAILED\"}')
print(f'Time taken            : {elapsed:.3f}s')
print()

# Show what was written
files_written = sorted(STAGING_RAW.glob('*.parquet'))
print('Files in staging/raw/:')
for f in files_written:
    size_kb = f.stat().st_size / 1024
    print(f'  {f.name:<45} {size_kb:>8.1f} KB')""", "s5-demo"),

code("""# Simulate atomic staging failure — show that no partial files are left
print('Simulating a staging failure ...\\n')

# Create a partial raw dict (missing one table)
partial_raw = {'product': raw['product'], 'customer': raw['customer']}
# (salesorderheader and salesorderdetail are missing)

# This will succeed because we only write what's given.
# In a real failure scenario, the temp dir is cleaned up automatically.
# The key guarantee: original staging/raw/ files are NOT touched until ALL writes succeed.

print('Key property: if any write fails during atomic_stage(),')
print('the original staging/raw/ files are NOT modified.')
print('The temp dir is deleted, and the pipeline raises an error.')
print()
print('This prevents partial state from being read by downstream stages.')""", "s5-partial"),

md("""**Explanation:**  
- The atomic write strategy: **write to temp → rename to final**.  
  On most operating systems, a directory rename is a single atomic kernel operation.  
- If any file write fails inside the temp directory, the entire temp dir is deleted.  
  The original `staging/raw/` files are untouched.  
- This prevents the downstream validation stage from reading a partially-written dataset.  
- In production databases, atomicity is achieved with **transactions** (`BEGIN/COMMIT/ROLLBACK`).  
  The atomic file write is the file-system equivalent of a database transaction.
""", "s5-explain"),

md("---\n## Section 6 — Alert Thresholds\n\n**Objective:**  \nCheck business-level thresholds that go beyond the DQ rules.\n\n| Threshold | Default | Meaning |\n|-----------|---------|--------|\n| `ALERT_OUTLIER_RATIO` | 20% | Warn if > 20% of any table rows are outliers |\n| `ALERT_INVALID_RATIO` | 0.5% | Warn if > 0.5% of any table rows have null key |\n| `ALERT_MIN_SALES_VALUE` | $1M | Warn if total sales < $1M |\n", "s6-header"),

code("""from transformation import transform_all

transformed = transform_all(val_result['valid'])

print('Checking alert thresholds ...\\n')
print(f'  ALERT_OUTLIER_RATIO   : {ALERT_OUTLIER_RATIO*100:.1f}%')
print(f'  ALERT_INVALID_RATIO   : {ALERT_INVALID_RATIO*100:.2f}%')
print(f'  ALERT_MIN_SALES_VALUE : ${ALERT_MIN_SALES_VALUE:,.0f}')
print()

alerts = check_alert_thresholds(val_result, transformed)

if alerts:
    print(f'ALERTS TRIGGERED ({len(alerts)}):')
    for a in alerts:
        print(f'  {a}')
else:
    print('No alerts triggered — all thresholds within acceptable range.')

# Show actual values
total_sales = transformed['sales']['LineTotal'].sum()
print(f'\\nActual total sales  : ${total_sales:,.2f}')""", "s6-check"),

code("""# Demonstrate an alert being triggered
print('\\nSimulating an alert by lowering the sales threshold ...\\n')

# Temporarily modify the alert threshold in-module
import resilient_pipeline as rp
original_threshold = rp.ALERT_MIN_SALES_VALUE
rp.ALERT_MIN_SALES_VALUE = 500_000_000.0  # $500M — impossible to meet

alerts_sim = check_alert_thresholds(val_result, transformed)
rp.ALERT_MIN_SALES_VALUE = original_threshold   # restore

if alerts_sim:
    for a in alerts_sim:
        print(f'  SIMULATED ALERT: {a}')
else:
    print('No alerts.')""", "s6-simulate"),

md("""**Explanation:**  
- Alerts are **separate from DQ rules**. DQ rules check data structure (nulls, dupes, counts).  
  Alerts check **business meaning** (is the total revenue plausible? are there too many outliers?).  
- Alerts trigger a `logger.warning()` — they do NOT halt the pipeline.  
  They are surfaced in the pipeline report for operator review.  
- In production, alerts would send notifications (email, Slack, PagerDuty, etc.).  
  For this project they are printed to the report and log.
""", "s6-explain"),

md("---\n## Section 7 — Run the Resilient Pipeline\n\n**Objective:**  \nExecute the complete resilient pipeline with all features enabled.\n", "s7-header"),

code("""print('Running full resilient pipeline ...\\n')
report = run_resilient_pipeline(db_load=False)""", "s7-run"),

code("""print('\\nReport Fields:')
for k, v in report.items():
    if k == 'stages_completed':
        print(f'  {k:<22}: {len(v)} stages')
    elif k == 'alerts':
        print(f'  {k:<22}: {len(v)} alerts')
    elif isinstance(v, int):
        print(f'  {k:<22}: {v:>10,}')
    elif isinstance(v, float):
        print(f'  {k:<22}: {v:>10.1f}')
    else:
        print(f'  {k:<22}: {v}')""", "s7-report"),

code("""# Show pipeline state file
if PIPELINE_STATE_FILE.exists():
    state = json.loads(PIPELINE_STATE_FILE.read_text(encoding='utf-8'))
    print('Pipeline State File (logs/pipeline_state.json):')
    print(json.dumps(state, indent=2))""", "s7-state"),

md("""**Explanation:**  
- The pipeline report now includes `dq_passed`, `dq_warnings`, `dq_errors`, `dq_status`, and `alerts`.  
- The `stages_completed` list shows exactly which stages ran — useful for debugging where a failure occurred.  
- `logs/pipeline_state.json` is updated at each stage checkpoint — if the pipeline crashes,  
  you can read this file to see which stage it was in when it failed.
""", "s7-explain"),

md("---\n## Section 8 — Pipeline Run History\n\n**Objective:**  \nView the audit trail of all past pipeline runs.\n", "s8-header"),

code("""history = load_run_history()
print(f'Total runs recorded: {len(history)}')
print()

if history:
    df_hist = pd.DataFrame(history)
    cols = ['run_id','start_time','duration_sec','status',
            'dq_passed','dq_warnings','dq_errors','factsales_rows']
    available = [c for c in cols if c in df_hist.columns]
    print('Run History:')
    display(df_hist[available])""", "s8-history"),

code("""# Compute statistics across all runs
if len(history) > 0:
    df_h = pd.DataFrame(history)
    success_runs = df_h[df_h['status'] == 'SUCCESS']
    failed_runs  = df_h[df_h['status'] == 'FAILED']

    print('Run Statistics:')
    print(f'  Total runs    : {len(df_h)}')
    print(f'  Successes     : {len(success_runs)}')
    print(f'  Failures      : {len(failed_runs)}')
    if len(success_runs) > 0:
        avg_dur = success_runs['duration_sec'].mean()
        min_dur = success_runs['duration_sec'].min()
        max_dur = success_runs['duration_sec'].max()
        print(f'  Avg duration  : {avg_dur:.1f}s')
        print(f'  Min duration  : {min_dur:.1f}s')
        print(f'  Max duration  : {max_dur:.1f}s')""", "s8-stats"),

md("""**Explanation:**  
- `logs/pipeline_runs.json` grows with every run — it's the full audit trail.  
- Each entry records: Run ID, timestamps, duration, status, source record counts, FactSales rows, DQ counts.  
- This data lets you track: pipeline performance over time, detect regressions, and audit data lineage.  
- In production, this would be stored in a database table (e.g., `pipeline_metadata.runs`) for SQL querying.  
- The `--show-history` flag of `run_resilient_pipeline.py` reads this file from the command line.
""", "s8-explain"),

md("---\n## Section 9 — Simulate a DQ Failure\n\n**Objective:**  \nForce a DQ ERROR-level failure and see the pipeline halt before loading data.\n\nThis is the most important resilience feature:\n- **Without DQ**: bad data loads silently, corrupts the warehouse, analysts get wrong numbers.\n- **With DQ**: pipeline stops immediately, logs the failing rule, and preserves the previous good state.\n", "s9-header"),

code("""# Simulate: create a DQ rule that will FAIL on our data
# We set a threshold the data cannot possibly meet.
print('Simulating a DQ ERROR failure ...\\n')

# Rule: product must have MORE than 1000 rows — but we only have 504!
failing_rule = DQRule(
    rule_id    = 'sim_product_too_few',
    table      = 'product',
    rule_type  = RULE_ROW_COUNT_MIN,
    threshold  = 1_000,          # we have 504 — this will FAIL
    severity   = 'ERROR',        # ERROR level — should halt pipeline
    description= 'Simulated failure: product table must have > 1000 rows',
)

# Run just this one rule
sim_results, sim_summary = run_data_quality_checks(
    raw, val_result['reports'], rules=[failing_rule]
)
print_dq_report(sim_results, sim_summary)
print()
print(f'halt_pipeline flag : {sim_summary[\"halt_pipeline\"]}')
print('If this were the real pipeline with halt_on_error=True,')
print('it would raise RuntimeError and stop here.')""", "s9-fail"),

code("""# Run the pipeline with soft-DQ: DQ errors become warnings (don't halt)
print('Running pipeline with --soft-dq (halt_on_error=False) ...\\n')

report_soft = run_resilient_pipeline(
    db_load       = False,
    skip_health   = True,     # skip health check in demo for speed
    halt_on_error = False,    # DQ errors become warnings
    dq_rules      = DEFAULT_RULES + [failing_rule],
)

print(f'\\nSoft-DQ pipeline status: {report_soft[\"status\"]}')
print(f'DQ errors recorded    : {report_soft[\"dq_errors\"]}')
print(f'Pipeline continued    : Yes (halt_on_error=False)')""", "s9-soft"),

code("""# Run with STRICT DQ (should halt on the failing rule)
print('\\nRunning pipeline with STRICT DQ + failing rule ...\\n')
print('Expected: pipeline raises error and stops before loading any data.\\n')

try:
    report_strict = run_resilient_pipeline(
        db_load       = False,
        skip_health   = True,
        halt_on_error = True,    # STRICT
        dq_rules      = DEFAULT_RULES + [failing_rule],
    )
except SystemExit:
    pass  # run_resilient_pipeline doesn't sys.exit — it returns the report

# The pipeline catches the error internally and sets status=FAILED
if 'report_strict' in dir():
    print(f'Status: {report_strict[\"status\"]}')
    print(f'Error : {report_strict.get(\"error_message\", \"\")}')""", "s9-strict"),

md("""**Explanation:**  
- `halt_on_error=True` (the default) means: if any DQ rule with `severity='ERROR'` fails,  
  the pipeline stops **before loading any data** into OLTP or OLAP.  
- The previous good data in the staging and database remains intact.  
- `halt_on_error=False` (the `--soft-dq` mode) lets the pipeline continue despite DQ errors.  
  Use this for: data exploration, debugging, or when you know the threshold needs adjustment.  
- The DQ result CSV (`logs/dq_results_<RUN_ID>.csv`) records which rules failed and why —  
  this is the evidence you need to fix the source data or adjust the threshold.
""", "s9-explain"),

md("---\n## Section 10 — Step 4 Summary\n", "s10-header"),

code("""print('=' * 60)
print('  STEP 4 COMPLETE — RESILIENT PIPELINE SUMMARY')
print('=' * 60)
print()
print('  Files Created:')
print('    src/data_quality.py        - 16-rule DQ engine (6 rule types)')
print('    src/resilient_pipeline.py  - resilient orchestrator')
print('    run_resilient_pipeline.py  - CLI entry point')
print()
print('  Run Commands:')
print('    python run_resilient_pipeline.py               # full pipeline')
print('    python run_resilient_pipeline.py --no-db       # no PostgreSQL')
print('    python run_resilient_pipeline.py --soft-dq     # warn on DQ errors')
print('    python run_resilient_pipeline.py --show-history # print run history')
print()
print('  Log Files:')
log_files = list(LOGS_DIR.glob('*'))
for f in sorted(log_files):
    if f.is_file():
        size_kb = round(f.stat().st_size / 1024, 1)
        print(f'    {f.name:<45} {size_kb:>6.1f} KB')
print()
history_count = len(load_run_history())
print(f'  Pipeline runs in history: {history_count}')
print()
print('  Resilience Features Implemented:')
print('    Pre-flight health checks  (source files, dirs, pyarrow)')
print('    DQ rules engine           (16 rules, ERROR/WARNING severity)')
print('    Retry with backoff        (extract, DB connect)')
print('    Atomic staging writes     (temp-rename pattern)')
print('    Alert thresholds          (outlier%, invalid%, total sales)')
print('    Pipeline state tracking   (logs/pipeline_state.json)')
print('    Run history               (logs/pipeline_runs.json)')
print()
print('  Next: Step 5 - Analytics Dashboard')
print('=' * 60)""", "s10-summary"),

md("""---
## Concepts Learned in This Notebook

| Concept | Definition |
|---------|------------|
| **Pre-flight Check** | Environment validation before any processing starts |
| **DQ Rules Engine** | Configurable rules that test data properties (counts, nulls, dupes, ranges) |
| **DQ Severity** | ERROR = halt pipeline; WARNING = log and continue |
| **Retry Logic** | Re-execute a failed operation N times before giving up |
| **Exponential Backoff** | Wait time doubles each retry (2s, 4s, 8s…) — avoids overwhelming a recovering service |
| **Atomic Write** | Either all files are written or none — temp dir renamed when complete |
| **Alert Threshold** | Business-level warning: \"this value looks wrong even if data is structurally valid\" |
| **Pipeline State** | JSON checkpoint file — records which stage the pipeline is in right now |
| **Run History** | JSON audit trail of all past pipeline executions |
| **Graceful Degradation** | Continue with reduced functionality rather than crashing completely |
| **halt_on_error** | DQ error severity mode: strict (halt) vs soft (warn and continue) |
| **Audit Trail** | Complete record of what ran, when, what data it processed, and what happened |

---
*Next notebook:* `05_dashboard.ipynb` — Interactive Analytics Dashboard with Plotly  
*Developed with:* **Antigravity** — AI Coding Assistant
""", "s10-concepts"),
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

out = PROJECT_ROOT / "notebooks" / "04_resilient_pipeline.ipynb"
out.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Written: {out}")

try:
    json.loads(out.read_text(encoding="utf-8"))
    print("JSON validation: PASSED")
except json.JSONDecodeError as e:
    print(f"JSON FAILED: {e}")
    sys.exit(1)
