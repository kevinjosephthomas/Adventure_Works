# AdventureWorks Data Engineering Dashboard — Step 5 (Phase 1)
## Progress Summary

We have initiated the development of the Flask-based web application that acts as the user interface for the complete end-to-end data engineering and analytics pipeline.

---

### 1. Architectural Integrity & Constraints
- **Uncoupled Database Updates:** The Flask application does not update the OLTP or OLAP database tables directly.
- **Data Flow:** All modifications follow the correct path:
  ```
  User Action (Flask UI) ➔ Source CSVs ➔ run_pipeline() ➔ Staging/Validation ➔ OLTP ➔ OLAP ➔ Analytics ➔ UI
  ```
- **Execution Guard:** A pipeline lock prevents concurrent runs.
- **Single Source of Truth:** Reuses `run_pipeline()` and `analytics.py` outputs.

---

### 2. Files Created & Configured

#### Backend Controller
*   **[app.py](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/app.py)**:
    - Sets up the Flask server (port 5000) and registers endpoints.
    - Implements CSV read/write functions that handle tab-separated source files safely.
    - Configures a thread lock `_pipeline_lock` to block multiple concurrent executions.
    - Sets up an append-only JSON audit log (`logs/flask_audit.json`) for data mutations.
    - Integrates a count verification check to ensure source record sizes align with OLTP/OLAP counts.
    - Implements a custom Jinja2 filter (`format_number`) for cleaning up numeric displays.

#### Frontend Layout & Base Styling
*   **[templates/base.html](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/base.html)**:
    - Master layout using Bootstrap Icons and Plotly.
    - Premium responsive sidebar navigation.
    - Displays live clock and real-time pipeline status indicator (idle vs. running).
    - Contains a JavaScript poller that automatically reloads the page once a running pipeline completes.
    - Embeds a confirmation modal dialog for delete operations.
*   **[templates/index.html](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/index.html)**:
    - Homepage displaying 8 KPI cards:
      *   Total Products
      *   Total Customers
      *   Total Orders
      *   Total Sales
      *   Total Quantity Sold
      *   Invalid Records
      *   Duplicate Records
      *   Outlier Records
    - Visualizes annual sales with a modern dark bar chart using Plotly.
    - Displays recent activity logs and last execution status.
*   **[static/css/style.css](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/static/css/style.css)**:
    - A premium dark industrial dashboard style featuring clean typography (Inter & JetBrains Mono), glassmorphism hints, colored card accents, responsive tables, badge statuses, form inputs, and modal dialogs.
*   **[static/js/dashboard.js](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/static/js/dashboard.js)**:
    - Standardizes Plotly dark theme properties, layouts, and colors.
    - Implements helper functions for numbers formatting and client-side table searching.
    - Handles automatic dismissals of warning/success alerts.

---

### 3. Current Live Status
- The homepage loads successfully at `http://localhost:5000/` with actual stats.
- All KPI counts, charts, and activity audit lists are successfully wired to the staging outputs of Exercise 3 and 4.
- A background process is currently running Flask.

---

### 4. Next Implementation Steps
We will now proceed to implement the remaining templates to enable full CRUD operations and visual verification page integrations:
1.  `products.html`, `add_product.html`, `edit_product.html`
2.  `customers.html`, `add_customer.html`, `edit_customer.html`
3.  `pipeline.html` (with execution triggers)
4.  `data_quality.html` (visual rules execution status and historical records)
5.  `analytics.html` (full suite of Plotly visuals)
