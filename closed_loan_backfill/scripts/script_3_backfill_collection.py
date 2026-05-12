#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from common import (
    CheckpointStore,
    mandatory_failure_fields,
    parse_common_args,
    read_excel,
    runtime_from_args,
    setup_logger,
    to_records,
    write_audit_xlsx,
)

STAGE = "script_3_backfill_collection"


def load_existing_backfill_module(repo_root: Path):
    target = repo_root / "backfill_collection.py"
    spec = importlib.util.spec_from_file_location("existing_backfill_collection", target)
    if not spec or not spec.loader:
        raise RuntimeError(f"unable to load {target}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run() -> int:
    parser = parse_common_args("Script 3 - Collection backfill orchestration")
    parser.add_argument("--input", default="CLOSED_LOANS_DETAILS.xlsx")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    cp = CheckpointStore(STAGE, paths)
    repo_root = paths.root.parent

    tracker = to_records(read_excel(paths.source_sheets / args.input, 0))
    db_mapping = to_records(read_excel(paths.source_sheets / "DB_SHEET.xlsx", 0))
    db_by_app = {str(row.get("Application No.") or row.get("application_no")): row for row in db_mapping}
    completed = cp.completed() if runtime.resume else set()

    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    manual: list[dict[str, Any]] = []

    for row in tracker:
        app = str(row.get("Application No.") or "")
        loan_id = row.get("loan_id")
        key = str(loan_id)
        if key in completed:
            skipped.append({"loan_id": loan_id, "application_id": row.get("application_id"), "reason": "already_processed"})
            continue
        map_row = db_by_app.get(app)
        if not map_row:
            failed.append(mandatory_failure_fields(loan_id, row.get("application_id"), "app mapping missing", STAGE))
            continue
        db_app = str(map_row.get("Application No.") or map_row.get("application_no") or "")
        if db_app != app:
            failed.append(mandatory_failure_fields(loan_id, row.get("application_id"), "application mapping mismatch", STAGE))
            continue
        success.append(row)
        cp.mark_completed(key)

    # Create a filtered input for existing script and execute it.
    filtered_input = paths.generated_sheets / "script_3" / "script_3_mapping_validated_input.xlsx"
    if success:
        import pandas as pd

        pd.DataFrame(success).to_excel(filtered_input, index=False)
        sys.path.insert(0, str(repo_root))
        module = load_existing_backfill_module(repo_root)
        result_code = module.run_backfill(execute=(runtime.execute and not runtime.dry_run), logger=module.setup_logging("script_3_backfill"))
        if result_code != 0:
            manual.append(mandatory_failure_fields("", "", "existing backfill returned non-zero", STAGE))

    out = write_audit_xlsx(
        paths.generated_sheets / "script_3",
        "script_3_backfill",
        {"success": success, "failed": failed, "skipped": skipped, "manual_intervention": manual},
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
