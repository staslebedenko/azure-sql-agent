#!/usr/bin/env python
"""
Demo Loop — Continuous Data Quality Monitoring  (Azure SQL Edition)
═══════════════════════════════════════════════════════════════════
Runs both agents in a loop, polling every CYCLE_SECONDS.

Self-contained workflow (no SQL scripts needed):
  1. python run_demo.py          (creates tables, seeds data, scans)
  2. python run_demo.py --inject (inject anomalies + scan)
  3. Watch agents detect anomalies, flag rows, send notifications
  4. Ctrl+C to stop (tears down tables)

Tables are created in Azure SQL at startup and dropped when
the demo ends.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

from agents.config import settings
from agents.data_store import (
    get_connection,
    inject_anomalies,
    reset_data,
    teardown,
    count_dirty_rows,
    count_findings,
    get_all_findings,
    clear_findings,
    count_kpi_rows,
)
from agents.platform_monitor import run_monitor_analysis
from agents.report_validator import run_report_validation

# Table names (display only)
_KPI = settings.fq_kpi_table
_FINDINGS = settings.fq_findings_table


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

SEVERITY_ICON = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "WARNING": "🟡",
    "INFO": "🔵",
}


def print_header(cycle: int, timestamp: str) -> None:
    print("\n" + "═" * 72)
    print(f"  CYCLE {cycle}  │  {timestamp}")
    print("═" * 72)


def print_all_clear() -> None:
    print("\n  ✅  All clear — no anomalies detected.")
    print("      Both agents confirmed data quality is healthy.\n")


def print_anomaly_dashboard(findings: list[dict]) -> None:
    print(f"\n  🚨  {len(findings)} findings recorded by agents:\n")

    for i, a in enumerate(findings, 1):
        icon = SEVERITY_ICON.get(a.get("Severity", ""), "⚪")
        run_date = a.get("RunDate", "?")
        print(f"  ── Finding {i} ───────────────────────────────────────────")
        print(f"  {icon} [{a.get('Severity', '?')}] {a.get('AnomalyType', '?')}")
        print(f"     Date:     {run_date}")
        print(f"     Pipeline: {a.get('PipelineName', '?')}")
        print(f"     KPI:      {a.get('KPIName', '?')} ({a.get('AggregationType', '?')})")
        if a.get("Description"):
            print(f"     Issue:    {a['Description']}")
        if a.get("Hypothesis"):
            print(f"     Cause:    {a['Hypothesis']}")

        if a.get("NotifiedTeam") or a.get("NotifiedUsers"):
            print(f"     ┌─ 📨 Notification sent:")
            if a.get("NotifiedTeam"):
                print(f"     │  Team:  {a['NotifiedTeam']}")
            if a.get("NotifiedUsers"):
                print(f"     │  Users: {a['NotifiedUsers']}")
            if a.get("NotificationMsg"):
                msg = a["NotificationMsg"]
                if len(msg) > 200:
                    msg = msg[:200] + "…"
                print(f"     │  Message: {msg}")
            print(f"     └──")
        print()


def print_waiting(seconds: int) -> None:
    print(f"  ⏳ Next scan in {seconds}s — press Ctrl+C to stop\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demo loop — self-contained Azure SQL data quality agents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Demo workflow:\n"
            "  1. python run_demo.py          (creates tables + seeds data)\n"
            "  2. python run_demo.py --inject  (inject anomalies + scan)\n"
            "  3. python run_demo.py --reset   (reset to clean + exit)\n"
            "  4. Ctrl+C to stop (tables dropped on exit)"
        ),
    )
    parser.add_argument(
        "--interval", type=int, default=30,
        help="Seconds between scan cycles (default 30)",
    )
    parser.add_argument(
        "--days", type=int, default=7,
        help="Agent lookback window in days (default 7)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single cycle then exit",
    )
    parser.add_argument(
        "--inject", action="store_true",
        help="Inject anomalies into the data before scanning",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Reset data to clean state and exit",
    )
    parser.add_argument(
        "--no-teardown", action="store_true",
        help="Don't drop tables on exit (inspect them in SSMS afterwards)",
    )
    args = parser.parse_args()

    # ── Initialise: connect + create tables + seed ──
    print("\n" + "╔" + "═" * 70 + "╗")
    print("║  🤖  Data Quality AI Agents — Live Demo  (Azure SQL)              ║")
    print("║  Scanning every {:>3}s  │  Lookback: {} days{} ║".format(
        args.interval, args.days, " " * (27 - len(str(args.days)))))
    print("╚" + "═" * 70 + "╝")

    print(f"\n  Server  : {settings.sql_server}")
    print(f"  Database: {settings.sql_database}")
    print(f"  KPI tbl : {_KPI}")
    print(f"  Findings: {_FINDINGS}\n")

    # First call triggers table creation + seeding
    get_connection()
    total = count_kpi_rows()
    print(f"  📊 {total} rows in {_KPI}")

    # Handle --reset
    if args.reset:
        reset_data()
        print("  Done. Exiting.")
        return

    # Handle --inject
    if args.inject:
        dirty = inject_anomalies()
        print(f"  💉 Injected anomalies — {dirty} anomalous rows now in table\n")

    cycle = 0

    try:
        while True:
            cycle += 1
            now = datetime.now().strftime("%H:%M:%S")
            print_header(cycle, now)

            # ── Step 1: Check data state ──
            dirty = count_dirty_rows()
            existing = count_findings()

            if dirty == 0 and existing == 0:
                print("\n  📡 Scanning database… no anomalous data found.")
                print_all_clear()

                if args.once:
                    break
                print_waiting(args.interval)
                time.sleep(args.interval)
                continue

            # ── Step 2: Clear previous findings ──
            if existing > 0:
                clear_findings()
                print(f"\n  🧹 Cleared {existing} previous findings for fresh scan.")

            print(f"\n  📡 Detected {dirty} anomalous rows — running agents…\n")

            # ── Step 3: Agent 1 — Platform Monitor ──
            print("  ─── Agent 1: Platform Monitor ───────────────────────────────")
            try:
                monitor_result = run_monitor_analysis(days=args.days)
                summary = monitor_result[:500]
                if len(monitor_result) > 500:
                    summary += "\n  … (truncated)"
                print(f"  {summary}\n")
            except Exception as e:
                print(f"  ❌ Agent 1 error: {e}\n")

            # ── Step 4: Agent 2 — Report Validator ──
            print("  ─── Agent 2: Report Validator ──────────────────────────────")
            try:
                validator_result = run_report_validation(days=args.days)
                summary = validator_result[:500]
                if len(validator_result) > 500:
                    summary += "\n  … (truncated)"
                print(f"  {summary}\n")
            except Exception as e:
                print(f"  ❌ Agent 2 error: {e}\n")

            # ── Step 5: Dashboard ──
            findings = get_all_findings()
            if findings:
                print("  ─── 📊 Findings Dashboard ───────────────────────────────────")
                print_anomaly_dashboard(findings)
            else:
                print("  ⚠️  Agents ran but did not record any findings.\n")

            if args.once:
                break

            print_waiting(args.interval)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\n  👋 Demo stopped.")
        final = get_all_findings()
        if final:
            print(f"     {len(final)} findings in {_FINDINGS}.")
    finally:
        if not args.no_teardown:
            teardown()
        else:
            print("  📌 Tables left in place (--no-teardown). Clean up manually:")
            print(f"     DROP TABLE IF EXISTS {_KPI};")
            print(f"     DROP TABLE IF EXISTS {_FINDINGS};")

    sys.exit(0)


if __name__ == "__main__":
    main()
