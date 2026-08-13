#!/usr/bin/env python
# ============================================================
# run_resilient_pipeline.py
# AdventureWorks Data Engineering Pipeline — Step 4
#
# CLI ENTRY POINT — Production-ready resilient pipeline
#
# Usage:
#   python run_resilient_pipeline.py               # full pipeline
#   python run_resilient_pipeline.py --no-db       # skip PostgreSQL
#   python run_resilient_pipeline.py --skip-health # skip health checks
#   python run_resilient_pipeline.py --soft-dq     # warn on DQ errors, don't halt
#   python run_resilient_pipeline.py --show-history # print run history and exit
#   python run_resilient_pipeline.py --help
# ============================================================

import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR      = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "AdventureWorks — Resilient Data Engineering Pipeline\n"
            "Adds: health checks, DQ rules, retry, atomic writes, run history.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_resilient_pipeline.py                  # Full resilient pipeline
  python run_resilient_pipeline.py --no-db          # No PostgreSQL
  python run_resilient_pipeline.py --soft-dq        # DQ errors become warnings
  python run_resilient_pipeline.py --show-history   # Show all past runs
        """,
    )
    parser.add_argument("--no-db",        action="store_true", help="Skip PostgreSQL loading")
    parser.add_argument("--skip-health",  action="store_true", help="Skip pre-flight health checks")
    parser.add_argument("--soft-dq",      action="store_true", help="DQ errors become warnings (don't halt)")
    parser.add_argument("--show-history", action="store_true", help="Print run history and exit")
    return parser.parse_args()


def show_history():
    """Load and display run history from logs/pipeline_runs.json."""
    from resilient_pipeline import load_run_history
    history = load_run_history()
    if not history:
        print("No pipeline run history found.")
        return

    print()
    print("=" * 70)
    print("  PIPELINE RUN HISTORY")
    print("=" * 70)
    print(f"  {'Run ID':<12} {'Status':<10} {'Duration':>8}s  {'DQ':>6}  {'FactSales':>10}  Time")
    print("  " + "-" * 66)
    for r in history[-20:]:   # show last 20 runs
        dq_str = f"{r.get('dq_passed', 0)}/{r.get('dq_passed', 0) + r.get('dq_warnings', 0) + r.get('dq_errors', 0)}"
        print(
            f"  {r.get('run_id', 'N/A'):<12} "
            f"{r.get('status', 'N/A'):<10} "
            f"{r.get('duration_sec', 0):>8.1f}   "
            f"{dq_str:>6}  "
            f"{r.get('factsales_rows', 0):>10,}  "
            f"{r.get('start_time', 'N/A')}"
        )
    print("=" * 70)
    print(f"  Total runs: {len(history)}")
    print()


def main():
    args = parse_args()

    if args.show_history:
        show_history()
        return

    print()
    print("=" * 60)
    print("  AdventureWorks — Resilient Data Engineering Pipeline")
    print("  Step 4: Error Handling, DQ Rules, Retry, Atomicity")
    print("=" * 60)
    print(f"  DB Load      : {'ENABLED' if not args.no_db else 'DISABLED'}")
    print(f"  Health Check : {'SKIP' if args.skip_health else 'ENABLED'}")
    print(f"  DQ Mode      : {'SOFT (warn only)' if args.soft_dq else 'STRICT (halt on error)'}")
    print()

    from resilient_pipeline import run_resilient_pipeline

    report = run_resilient_pipeline(
        db_load       = not args.no_db,
        skip_health   = args.skip_health,
        halt_on_error = not args.soft_dq,
    )

    if report.get("status") != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
