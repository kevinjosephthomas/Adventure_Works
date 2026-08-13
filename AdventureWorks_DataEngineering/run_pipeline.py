#!/usr/bin/env python
# ============================================================
# run_pipeline.py
# AdventureWorks Data Engineering Pipeline — Step 3
#
# CLI ENTRY POINT
# ---------------
# Run the complete Full-Refresh pipeline with one command:
#
#   python run_pipeline.py
#
# Optional flags:
#   python run_pipeline.py --no-db      (skip PostgreSQL, file-only mode)
#   python run_pipeline.py --help       (show usage)
#
# The pipeline always performs a FULL REFRESH:
#   - Reads current state of all source CSV files
#   - Rebuilds every layer from scratch
#   - Final output always reflects the current source state
# ============================================================

import sys
import argparse
from pathlib import Path

# ── Add src/ to Python path ──────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR      = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "AdventureWorks End-to-End Data Engineering Pipeline\n"
            "Architecture: Full Refresh — rebuilds everything from source CSVs.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py              # Full pipeline (with DB if available)
  python run_pipeline.py --no-db      # File-only mode (no PostgreSQL needed)
        """,
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        default=False,
        help="Skip PostgreSQL loading. Data is saved to staging/ and dashboard/ only.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print()
    print("=" * 60)
    print("  AdventureWorks Data Engineering Pipeline")
    print("  Architecture : FULL REFRESH")
    print(f"  DB Load      : {'ENABLED' if not args.no_db else 'DISABLED (--no-db flag)'}")
    print("=" * 60)
    print()

    # Import pipeline (after sys.path is set)
    from pipeline import run_pipeline

    report = run_pipeline(db_load=not args.no_db)

    # Exit with non-zero code if pipeline failed (useful for CI/CD)
    if report.get("status") != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
