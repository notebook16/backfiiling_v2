#!/usr/bin/env python3
from __future__ import annotations

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

STAGE = "script_4_settlement_creation"


def run() -> int:
    parser = parse_common_args("Script 4 - Settlement collection creation")
    parser.add_argument("--input", default="script_3_backfill_success_latest.xlsx")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    cp = CheckpointStore(STAGE, paths)
    input_file = paths.generated_sheets / "script_3" / args.input
    if not input_file.exists():
        input_file = paths.source_sheets / "CLOSED_LOANS_DETAILS.xlsx"
    rows = to_records(read_excel(input_file, 0))
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
            settlement = row.get("settlement_amount") or row.get("Settlement Amount") or 0
            with db.conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT close_status FROM loan_applications WHERE application_id=%s FOR UPDATE", (loan_id,))
                    app = cur.fetchone()
                    if not app:
                        failed.append(mandatory_failure_fields(loan_id, app_id, "loan missing in loan_applications", STAGE))
                        continue
                    if runtime.execute and not runtime.dry_run:
                        cur.execute(
                            """
                            INSERT INTO collections (
                                center_id, org_id, loan_id, sale_id, emi_installment_no, is_active,
                                collection_type, collection_subtype, due_amount, due_date, status, created_by, loan_term_id
                            ) VALUES (%s,%s,%s,0,0,true,'SETTLEMENT','SETTLEMENT',%s,NOW(),'DONE',1,0)
                            RETURNING collection_id
                            """,
                            (row.get("center_id") or 1, row.get("org_id") or 1, loan_id, settlement),
                        )
                        coll_id = cur.fetchone()["collection_id"]
                        cur.execute(
                            """
                            INSERT INTO collection_trans (
                                org_id, center_id, collection_id, is_active, trans_type, trans_subtype, mode,
                                amount, recorded_by, scrap_type_id, recon_status, is_settled, from_user,
                                from_customer, to_user, created_by
                            ) VALUES (%s,%s,%s,true,'CREDIT','RECORD_PAYMENT','SETTLEMENT',%s,1,0,'ACCEPTED',true,0,%s,1,1)
                            """,
                            (row.get("org_id") or 1, row.get("center_id") or 1, coll_id, settlement, row.get("customer_id") or 0),
                        )
                        cur.execute(
                            "UPDATE loan_applications SET close_status='CLOSED', closed_at=NOW() WHERE application_id=%s",
                            (loan_id,),
                        )
                    validation = app.get("close_status") != "PENDING"
                    success.append(
                        {
                            "loan_id": loan_id,
                            "collection_id": "" if runtime.dry_run else locals().get("coll_id", ""),
                            "settlement_amount": settlement,
                            "close_trigger_applied": runtime.execute and not runtime.dry_run,
                            "db_validation_result": validation,
                        }
                    )
                    cp.mark_completed(key)
    finally:
        db.close()

    out = write_audit_xlsx(paths.generated_sheets / "script_4", "script_4_settlement", {"success": success, "failed": failed, "skipped": skipped})
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
