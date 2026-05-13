#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

import pandas as pd

from common import (
    CheckpointStore,
    DbClient,
    load_env_file,
    log_stage_summary,
    mandatory_failure_fields,
    parse_common_args,
    read_excel,
    runtime_from_args,
    setup_logger,
    to_records,
    write_audit_xlsx,
)

STAGE = "script_5_collection_status_update"


def run() -> int:
    parser = parse_common_args("Script 5 - Final collection status update")
    parser.add_argument("--input", default="CLOSED_LOANS_DETAILS.xlsx")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)
    cp = CheckpointStore(STAGE, paths)

    rows = to_records(read_excel(paths.source_sheets / args.input, 0))
    completed = cp.completed() if runtime.resume else set()
    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    db = DbClient(logger)
    try:
        for row in rows:
            loan_id = row.get("loan_id")
            app_id = row.get("application_id")
            key = str(loan_id)
            if key in completed:
                skipped.append({"loan_id": loan_id, "application_id": app_id, "reason": "already_processed"})
                continue
            closed_date = pd.to_datetime(row.get("CLOSED_DATE"), errors="coerce")
            if pd.isna(closed_date):
                failed.append(mandatory_failure_fields(loan_id, app_id, "missing CLOSED_DATE", STAGE))
                continue
            if runtime.execute and not runtime.dry_run:
                with db.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE collections
                        SET status='DONE'
                        WHERE loan_id=%s AND is_active=true AND due_date <= %s AND status <> 'DONE'
                        """,
                        (loan_id, closed_date.to_pydatetime()),
                    )
            success.append({"loan_id": loan_id, "application_id": app_id, "closed_date": str(closed_date), "updated": runtime.execute and not runtime.dry_run})
            cp.mark_completed(key)
    finally:
        db.close()

    out = write_audit_xlsx(
        paths.generated_sheets / "script_5",
        "script_5_final_status",
        {"success": success, "failed": failed, "skipped": skipped},
    )
    log_stage_summary(
        logger,
        "script_5_collection_status_update",
        loaded=len(rows),
        buckets={
            "success": success,
            "failed": failed,
            "skipped": skipped,
        },
        reason_fields_by_bucket={
            "failed": ("failure_reason",),
            "skipped": ("reason",),
        },
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
