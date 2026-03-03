#!/usr/bin/env python
"""Run Agent 2 — Report Validator (Azure SQL) once."""

from __future__ import annotations

import argparse
from dotenv import load_dotenv

load_dotenv()

from agents.data_store import get_connection, teardown
from agents.report_validator import run_report_validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Report Validator Agent (Azure SQL)")
    parser.add_argument("--days", type=int, default=None, help="Lookback days")
    parser.add_argument("--prompt", type=str, default=None, help="Custom prompt")
    parser.add_argument("--no-teardown", action="store_true", help="Keep tables after run")
    args = parser.parse_args()

    # Create tables + seed data on first connection
    get_connection()

    print("🤖  Running Report Validator Agent (Azure SQL)…\n")
    try:
        result = run_report_validation(days=args.days, user_prompt=args.prompt)
        print(result)
    finally:
        if not args.no_teardown:
            teardown()


if __name__ == "__main__":
    main()
