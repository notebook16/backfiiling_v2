#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from psycopg2.extras import RealDictCursor

from common import (
    CheckpointStore,
    DbClient,
    mandatory_failure_fields,
    parse_common_args,
    read_excel,
    runtime_from_args,
    setup_logger,
    to_records,
    write_audit_xlsx,
)

STAGE = "script_2_phase2_emi_creation"


def process_row(db: DbClient, row: dict[str, Any], execute: bool) -> tuple[str, dict[str, Any]]:
    loan_id = row.get("loan_id")
    app_id = row.get("application_id")
    with db.conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT collection_id, collection_subtype, status
                FROM collections
                WHERE loan_id = %s AND is_active = true
                ORDER BY collection_id
                FOR UPDATE
                """,
                (loan_id,),
            )
            coll = cur.fetchall()
            if len(coll) == 0:
                return "failed", mandatory_failure_fields(loan_id, app_id, "no collections found", STAGE)
            if len(coll) > 1:
                return "multi_conflict", mandatory_failure_fields(loan_id, app_id, "multiple collections exist", STAGE)
            item = coll[0]
            if (item["collection_subtype"] or "").upper() != "DP":
                return "skipped", mandatory_failure_fields(loan_id, app_id, "single collection is not DP", STAGE)
            if execute:
                cur.execute(
                    "UPDATE collections SET status='DONE' WHERE collection_id=%s AND status='PENDING'",
                    (item["collection_id"],),
                )
            return "updated", {"loan_id": loan_id, "application_id": app_id, "collection_id": item["collection_id"]}


def run() -> int:
    parser = parse_common_args("Script 2 - Phase2 EMI creation prep")
    parser.add_argument("--input", default="script_1_validation_success_latest.xlsx")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    cp = CheckpointStore(STAGE, paths)
    input_file = paths.generated_sheets / "script_1" / args.input
    if not input_file.exists():
        # fallback to source if user copied CLOSED_LOANS_DETAILS there directly
        input_file = paths.source_sheets / "CLOSED_LOANS_DETAILS.xlsx"
    rows = to_records(read_excel(input_file, 0))
    completed = cp.completed() if runtime.resume else set()

    updated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []

    db = DbClient(logger, maxconn=max(runtime.max_workers, 2))
    try:
        with ThreadPoolExecutor(max_workers=runtime.max_workers) as ex:
            future_map = {}
            for row in rows:
                key = str(row.get("loan_id"))
                if key in completed:
                    skipped.append({"loan_id": row.get("loan_id"), "application_id": row.get("application_id"), "reason": "already_processed"})
                    continue
                future_map[ex.submit(process_row, db, row, runtime.execute and not runtime.dry_run)] = (key, row)
            for fut in as_completed(future_map):
                key, row = future_map[fut]
                bucket, payload = fut.result()
                if bucket == "updated":
                    updated.append(payload)
                    cp.mark_completed(key)
                elif bucket == "skipped":
                    skipped.append(payload)
                elif bucket == "failed":
                    failed.append(payload)
                else:
                    conflicts.append(payload)
    finally:
        db.close()

    out = write_audit_xlsx(
        paths.generated_sheets / "script_2",
        "script_2_phase2",
        {"updated": updated, "skipped": skipped, "failed": failed, "multi_collection_conflict": conflicts},
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
