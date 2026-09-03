# Contributing to AdventureWorks Data Engineering Pipeline

Thank you for contributing! This document explains our **Git branching strategy**, commit conventions, and CI/CD expectations so that every change is safe, reviewable, and automatically tested.

---

## Table of Contents
1. [Branching Strategy](#branching-strategy)
2. [Branch Naming Conventions](#branch-naming-conventions)
3. [Commit Message Guidelines](#commit-message-guidelines)
4. [Pull Request Workflow](#pull-request-workflow)
5. [CI/CD Pipeline](#cicd-pipeline)
6. [Running Tests Locally](#running-tests-locally)
7. [Code Style](#code-style)

---

## Branching Strategy

We follow a **GitFlow-inspired** model adapted for data engineering teams:

```
main          ← production-ready, always deployable, protected branch
│
develop       ← integration branch; all features merge here first
│
├── feature/add-incremental-load
├── feature/spark-transformation-layer
├── fix/null-customer-id-handling
└── hotfix/pipeline-crash-on-empty-csv
```

### Branch Descriptions

| Branch | Purpose | Who merges | Protected? |
|---|---|---|---|
| `main` | Production-ready code. Every commit represents a deployable pipeline version. | Squash merge from `develop` only | ✅ Yes — requires PR + CI pass |
| `develop` | Integration branch. All finished features land here first for integration testing. | Merge from `feature/*` branches | ✅ Yes — requires PR + CI pass |
| `feature/<name>` | One feature or enhancement per branch. Short-lived (days, not weeks). | Developer opens PR → `develop` | ❌ No |
| `fix/<name>` | Bug fix for a non-critical issue discovered during development. | Developer opens PR → `develop` | ❌ No |
| `hotfix/<name>` | Critical production fix. Branches off `main`, merges into **both** `main` and `develop`. | Senior engineer PR → `main` + `develop` | ❌ No |
| `release/<version>` | Release stabilisation branch (optional for major versions). | PR → `main` + back-merge to `develop` | ❌ No |

### Data Engineering–Specific Rules
- **Never commit data files** (`.csv`, `.parquet`, `.pickle`) to the repository.
- **Never commit secrets** (DB passwords, API keys, `.env` files).
- Staging output (`staging/`) and logs (`logs/`) are generated at runtime — excluded by `.gitignore`.
- Schema changes to `src/config.py` or `sql/` **always** require a paired test update.

---

## Branch Naming Conventions

```
feature/<short-kebab-description>   → feature/add-delta-lake-sink
fix/<short-kebab-description>       → fix/null-product-id-drop
hotfix/<short-kebab-description>    → hotfix/oltp-connection-timeout
release/<semver>                    → release/1.2.0
```

**Rules:**
- Use lowercase with hyphens only (no underscores, no spaces)
- Keep names under 50 characters
- Include a ticket/issue number where applicable: `feature/DE-42-add-incremental-load`

---

## Commit Message Guidelines

Follow the **Conventional Commits** specification:

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

### Types

| Type | When to use |
|---|---|
| `feat` | A new feature or pipeline step |
| `fix` | A bug fix |
| `test` | Adding or updating tests |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `docs` | Documentation only changes |
| `ci` | Changes to CI/CD workflows (`.github/workflows/`) |
| `chore` | Maintenance tasks (dependency updates, config tweaks) |
| `perf` | A code change that improves performance |

### Examples

```bash
feat(transformation): add margin calculation to product aggregation
fix(ingestion): handle tab-separated CSVs with BOM encoding
test(transformation): add unit tests for null ProductID dropping
ci: add code coverage upload to ci.yml workflow
docs: update CONTRIBUTING.md with hotfix procedure
```

---

## Pull Request Workflow

### Opening a PR
1. **Branch from `develop`** for features/fixes, from `main` for hotfixes
2. **Push your branch** and open a PR against `develop` (or `main` for hotfixes)
3. **Fill in the PR template** — describe what changed and why
4. **Link related issues** in the PR description: `Closes #42`

### PR Requirements (enforced by `pr_validation.yml`)
- PR title must follow: `<type>(<scope>): <description>`
- All CI checks must pass (tests, lint, coverage)
- At least one reviewer approval required
- No direct pushes to `main` or `develop`

### Merge Strategy
| Target | Strategy | Reason |
|---|---|---|
| `develop` | **Squash merge** | Keeps history clean; feature commits squashed into one |
| `main` from `develop` | **Merge commit** | Preserves the release boundary with a clear marker |
| `main` from `hotfix/*` | **Squash merge** | Single atomic fix commit in history |

---

## CI/CD Pipeline

Every push and PR runs **three automated workflows**:

### 1. `ci.yml` — Continuous Integration
Triggered on: every `push` and `pull_request` to `main`/`develop`

| Step | What it does |
|---|---|
| Setup Python 3.11 | Installs the exact Python version used in production |
| Install dependencies | `pip install -r requirements.txt` (cached) |
| Lint | `flake8 src/ tests/ --max-line-length=120` |
| Unit Tests | `pytest tests/ -v --cov=src --cov-report=xml` |
| Upload Coverage | Coverage XML uploaded as workflow artifact |

### 2. `cd.yml` — Continuous Deployment
Triggered on: push to `main` only (after CI passes)

| Step | What it does |
|---|---|
| Gate: Full test suite | Runs all tests one final time |
| Config validation | Dry-run import of `src/config.py` |
| Package artifacts | Zips `src/` + `requirements.txt` as `pipeline_artifact_<sha>.zip` |
| GitHub Release | Creates a versioned release with the package attached |

### 3. `pr_validation.yml` — PR Quality Gate
Triggered on: PR open/update

- Validates PR title follows `type(scope): description` format
- Warns if new `src/*.py` files don't have matching `tests/test_*.py` files

---

## Running Tests Locally

### Setup
```bash
cd AdventureWorks_DataEngineering

# Create a virtual environment (recommended)
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS/Linux

# Install all dependencies including test tools
pip install -r requirements.txt
```

### Run All Tests
```bash
pytest tests/ -v
```

### Run with Coverage Report
```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

### Run a Specific Test File
```bash
pytest tests/test_transformation.py -v
```

### Run a Specific Test
```bash
pytest tests/test_transformation.py::test_transform_product_drops_null_id -v
```

### Run Linting
```bash
flake8 src/ tests/ --max-line-length=120
```

---

## Code Style

- **Line length**: max 120 characters (configured in `flake8`)
- **Type hints**: use them on all public functions (e.g., `def transform_product(df: pd.DataFrame) -> pd.DataFrame:`)
- **Docstrings**: every public function must have a docstring
- **No bare `except`**: always catch specific exception types
- **Constants in `config.py`**: add all path, column, and threshold constants there — never hardcode paths in source files

---

*Last updated by the AdventureWorks Data Engineering team.*
