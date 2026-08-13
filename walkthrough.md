# Step 5 — All Phases Complete: Walkthrough

## What Was Implemented

All remaining missing templates have been created. The Flask Dashboard is now **fully functional** with every page working.

---

## Phases Summary

| Phase | Status | What Was Built |
|-------|--------|---------------|
| Phase 1 — Core Dashboard | ✅ Done (prior session) | `app.py`, `base.html`, `index.html`, `style.css`, `dashboard.js` |
| Phase 2 — Products CRUD | ✅ Done (prior session) | `products.html`, `add_product.html`, `edit_product.html` |
| Phase 2 — Customers CRUD | ✅ **Done this session** | `customers.html`, `add_customer.html`, `edit_customer.html` |
| Support Pages | ✅ Done (prior session) | `pipeline.html`, `data_quality.html`, `analytics.html`, `audit.html` |

---

## Pages Verified — All 7/7 ✅

| URL | Page | Status |
|-----|------|--------|
| `http://localhost:5000/` | Dashboard Overview + KPIs | ✅ OK |
| `http://localhost:5000/customers` | Customer Directory + CRUD | ✅ OK |
| `http://localhost:5000/customers/add` | Add Customer Form | ✅ OK |
| `http://localhost:5000/pipeline` | Pipeline Control + History | ✅ OK |
| `http://localhost:5000/data-quality` | DQ Rules Engine Results | ✅ OK |
| `http://localhost:5000/analytics` | OLAP Plotly Charts | ✅ OK |
| `http://localhost:5000/audit` | Operational Audit Log | ✅ OK |

> Products CRUD routes (`/products`, `/products/add`, `/products/edit/<id>`) were already verified working in prior session.

---

## Files Created This Session

### [customers.html](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/customers.html)
- Stats strip (4 KPI mini-cards: total customers, page, showing, source file)  
- Server-side + client-side dual search
- Data table: CustomerID, AccountNumber, PersonID, StoreID, TerritoryID, rowguid, ModifiedDate
- Edit / Delete actions per row with confirmation modal
- Paginator with numbered page buttons
- Architecture note panel explaining the CSV → pipeline flow

### [add_customer.html](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/add_customer.html)
- **Customer Identity** panel: CustomerID (required, validated), AccountNumber
- **Relationships** panel: PersonID (FK → Person), StoreID (FK → Store), TerritoryID
- **System Fields** panel: rowguid (UUID, auto-generated if blank), ModifiedDate (auto-set)
- Data Engineering info box explaining the 5-step pipeline flow on submit
- Submit button disables + shows spinner during pipeline run

### [edit_customer.html](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/edit_customer.html)
- Identity banner showing current CustomerID, AccountNumber, Territory, PersonID
- **Editable fields**: PersonID, StoreID, TerritoryID, AccountNumber
- **Read-only fields**: CustomerID (PK), rowguid, ModifiedDate
- Current record state `<pre>` diff panel
- Danger Zone section with Delete button + confirmation modal
- Submit button disables during pipeline run

---

## Architecture Reminder

```
User Action (customers.html)
        │
        ▼
  Customer.csv modified
        │
        ▼
  run_pipeline() triggered
        │
        ├── Ingestion
        ├── Validation (DQ rules)
        ├── Transformation
        ├── OLTP load (PostgreSQL)
        ├── OLAP load (Star Schema)
        └── Dashboard Parquet regenerated
                │
                ▼
        Page reloads with fresh data
```

Flask **never** touches PostgreSQL directly. All mutations flow through source CSV → pipeline.

---

## DQ Note

The pipeline currently shows a `FAILED` status because the `sim_product_too_few` simulation rule expects ≥ 1,000 products but the dataset has 504. This is an intentional simulation rule from Step 4 — it demonstrates the DQ engine's error-halting behaviour. All real DQ rules (null checks, duplicate checks, outlier detection) pass correctly.

---

## How to Start

```bash
cd f:\DE_CAT_1\AdventureWorks_DataEngineering
python app.py
# → http://localhost:5000
```

### Step 5: Dashboard and UI (Flask Application)
We created a local Flask web application (`app.py`) to provide a UI to the Data Engineering pipeline.
- The UI exposes the underlying CSV datasets for Products and Customers.
- It includes features to Add, Edit, and Delete records safely, ensuring that referential integrity is preserved (e.g. aborting deletion if a `ProductID` is found in `SalesOrderDetail`).
- An **Audit Log** tracks all manual changes made in the dashboard.
- Plotly-based visualizations directly hook into the Parquet files produced by the pipeline in `dashboard/` allowing analysts to visualize insights.
- The UI includes modern styling using CSS Variables to create a beautiful, rich green-black glassmorphic theme.

### Step 6: Production Resilience Features
We augmented the Flask dashboard to explicitly demonstrate the 4 pillars of Resilient Data Engineering pipelines:
- **Staging & Validation**: The *Data Quality* page now details the physical layer architecture, explicitly showing the paths for the Raw Zone, Valid Zone, and Quarantine paths, mapping the rules engine to Schema, Null, and Outlier detection.
- **Idempotency**: The *Pipeline* page has a dedicated "Idempotency Run" section that allows you to safely test triggering full pipeline reloads. You can verify that row counts remain completely stable because the pipeline is designed to safely overwrite Parquet files without duplication.
- **Atomicity (All-or-Nothing)**: The *Pipeline* page explains how Python `try...except` blocks and transaction engines are utilized to guarantee that if a Data Quality rule throws a fatal error, the pipeline safely aborts before writing bad data to the target.
- **Error Handling (Replay & Backfill)**: The *Audit Trail* now includes a **Replay** feature for every historical data fix. This allows you to simulate backfilling data by replaying the execution logic downstream after an audit event is recorded.

---
**Run the App**:
```bash
python app.py
```
Open your browser to `http://localhost:5000` to interact with the full system.
