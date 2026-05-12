#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPTS = [
    ("script_1_validation.py", "Script 1 Validation"),
    ("script_2_phase2_emi_creation.py", "Script 2 Phase2 EMI Creation"),
    ("script_3_backfill_collection.py", "Script 3 Collection Backfill"),
    ("script_4_settlement_creation.py", "Script 4 Settlement Creation"),
    ("script_5_collection_status_update.py", "Script 5 Finalization"),
]


def run_script(script_dir: Path, script_name: str, passthrough: list[str]) -> int:
    cmd = [sys.executable, str(script_dir / script_name), *passthrough]
    return subprocess.call(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Master runner for CLOSED_LOANS_DETAILS backfill")
    parser.add_argument("--mode", choices=["full", "single", "resume"], default="full")
    parser.add_argument("--script", default="script_1_validation.py")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def summary_from_code(code: int) -> tuple[int, int, int]:
    if code == 0:
        return (1, 1, 0)
    if code == 2:
        return (1, 0, 1)
    return (1, 0, 0)


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    passthrough: list[str] = []
    if args.execute:
        passthrough.append("--execute")
    if args.dry_run:
        passthrough.append("--dry-run")
    if args.resume or args.mode == "resume":
        passthrough.append("--resume")

    if args.mode == "single":
        return run_script(script_dir, args.script, passthrough)

    for script_name, label in SCRIPTS:
        code = run_script(script_dir, script_name, passthrough)
        total, success, fail = summary_from_code(code)
        print(f"\n{label} summary")
        print(f"total processed: {total}")
        print(f"success count: {success}")
        print(f"failure count: {fail}")
        print("skipped count: check generated XLSX for exact number")
        if code not in (0, 2):
            print("Script failed unexpectedly, stopping pipeline.")
            return code
        ans = input("Proceed to next script? (Y/N): ").strip().upper()
        if ans != "Y":
            print("Pipeline stopped by user.")
            return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
