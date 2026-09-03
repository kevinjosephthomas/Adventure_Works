# Walkthrough: Robust Error Handling Added

We have added comprehensive, production-grade operational error handling across the web app.

---

## 1. Customer CRUD Operational Safeties

Prior to this update, Customer CRUD actions wrote directly to the source `Customer.csv` file without backups or rollbacks. If a pipeline validation rule halted the process, the source CSV remained corrupted with invalid data, and deletion was not checked for reference constraints.

### Added Protections
* **Backup & Rollback on Pipeline Fails (Atomicity):** `add_customer()`, `edit_customer()`, and `delete_customer()` now automatically copy `Customer.csv` to `Customer.csv.bak` before writing changes. If the pipeline run fails or raises an error, the backup file is immediately restored to preserve source consistency.
* **Referential Integrity Constraints:** `delete_customer()` now executes a pre-check against `SalesOrderHeader.csv` to ensure the Customer is not associated with historical orders. If linked, deletion is blocked to prevent broken foreign keys in OLAP Star Schema tables.
* **Catch-All Exception Guards:** Every customer and product CRUD route now has catch-all `except Exception as e:` blocks. Unexpected file permissions or DB issues will be logged and flashed safely without crashing the server.

---
 
## 2. Custom Dashboard Error Handler Views

Rather than showing default browser warnings or exposing raw tracebacks, Flask now handles HTTP errors with beautiful themed templates that match the rest of the dark dashboard layout.

* **[404 Not Found Page](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/errors/404.html)** — Displays if a user navigates to an invalid path, offering navigation back to the Dashboard.
* **[500 Server Error Page](file:///f:/DE_CAT_1/AdventureWorks_DataEngineering/templates/errors/500.html)** — Displays on runtime failures, outputting exception tracebacks to the server log while showing a user-friendly diagnostic alert.
