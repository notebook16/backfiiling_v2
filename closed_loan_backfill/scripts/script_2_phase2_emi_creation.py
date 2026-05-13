#!/usr/bin/env python3
from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from psycopg2.extras import RealDictCursor

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

STAGE = "script_2_phase2_emi_creation"
DONE_UPDATE_DELAY_SECONDS = 5.0


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _normalize_token(value: Any) -> str:
    return "" if _is_blank(value) else str(value).strip().upper()


def _normalize_identifier(value: Any) -> str:
    if _is_blank(value):
        return ""
    text = str(value).strip().replace(",", "")
    try:
        decimal_value = Decimal(text)
    except Exception:  # noqa: BLE001
        return str(value).strip()
    if decimal_value == decimal_value.to_integral_value():
        return str(int(decimal_value))
    return str(decimal_value)


def _failure_payload(row: dict[str, Any], reason: str) -> dict[str, Any]:
    payload = mandatory_failure_fields(row.get("loan_id"), row.get("application_id"), reason, STAGE)
    payload["application_no"] = row.get("application_no")
    payload["phase"] = row.get("phase")
    payload["dp_collection_id"] = row.get("dp_collection_id")
    return payload


def _skip_payload(row: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "loan_id": row.get("loan_id"),
        "application_id": row.get("application_id"),
        "application_no": row.get("application_no"),
        "phase": row.get("phase"),
        "dp_collection_id": row.get("dp_collection_id"),
        "reason": reason,
    }


def _fetch_collection(cur, collection_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT collection_id, loan_id, collection_type, collection_subtype, status, is_active
        FROM collections
        WHERE collection_id = %s
        FOR UPDATE
        """,
        (collection_id,),
    )
    return cur.fetchone()


def run() -> int:
    parser = parse_common_args("Script 2 - Phase2 EMI creation prep")
    parser.add_argument("--input", default="script_1_validation_success_latest.xlsx")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)
    cp = CheckpointStore(STAGE, paths)
    input_file = paths.generated_sheets / "script_1" / args.input
    if not input_file.exists():
        input_file = paths.source_sheets / "CLOSED_LOANS_DETAILS.xlsx"
    rows = to_records(read_excel(input_file, 0))
    completed = cp.completed() if runtime.resume else set()

    updated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    prepared_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    db = DbClient(logger, maxconn=max(runtime.max_workers, 2))
    try:
        with db.conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for row in rows:
                    logger.info(
                        "processing input row: raw_loan_id=%s raw_application_id=%s raw_dp_collection_id=%s raw_phase=%s",
                        row.get("loan_id"),
                        row.get("application_id"),
                        row.get("dp_collection_id"),
                        row.get("phase"),
                    )
                    phase = _normalize_token(row.get("phase"))
                    if phase != "PHASE_2":
                        skipped.append(_skip_payload(row, "non_phase_2"))
                        continue

                    collection_key = _normalize_identifier(row.get("dp_collection_id"))
                    loan_id = _normalize_identifier(row.get("loan_id"))
                    app_id = _normalize_identifier(row.get("application_id"))
                    row["phase"] = phase
                    row["loan_id"] = loan_id
                    row["application_id"] = app_id
                    row["dp_collection_id"] = collection_key

                    key = collection_key or loan_id or app_id
                    if not key:
                        failed.append(_failure_payload(row, "missing dp_collection_id"))
                        continue
                    if key in completed:
                        skipped.append(_skip_payload(row, "already_processed"))
                        continue
                    if key in seen_keys:
                        conflicts.append(_failure_payload(row, "duplicate dp_collection_id in script_1 output"))
                        continue
                    seen_keys.add(key)

                    if not collection_key:
                        failed.append(_failure_payload(row, "missing dp_collection_id"))
                        continue

                    collection = _fetch_collection(cur, int(collection_key))
                    if not collection:
                        failed.append(_failure_payload(row, "dp_collection_id not found"))
                        continue
                    if not collection.get("is_active"):
                        failed.append(_failure_payload(row, "dp_collection_id is inactive"))
                        continue
                    if _normalize_token(collection.get("collection_type")) != "DP":
                        failed.append(_failure_payload(row, "dp_collection_id collection_type is not DP"))
                        continue
                    db_loan_id = _normalize_identifier(collection.get("loan_id"))
                    if loan_id and db_loan_id and loan_id != db_loan_id:
                        conflicts.append(_failure_payload(row, "dp_collection_id loan_id mismatch"))
                        continue

                    logger.info(
                        "row prepared for phase_2: loan_id=%s application_id=%s dp_collection_id=%s current_status=%s",
                        loan_id,
                        app_id,
                        collection_key,
                        collection.get("status"),
                    )

                    prepared_rows.append(
                        {
                            **row,
                            "current_status": collection.get("status"),
                        }
                    )

        if prepared_rows and runtime.execute and not runtime.dry_run:
            collection_ids = [int(row["dp_collection_id"]) for row in prepared_rows]
            logger.info(
                "updating collections to PENDING for %s records: collection_ids=%s",
                len(collection_ids),
                collection_ids,
            )
            with db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE collections SET status='PENDING' WHERE collection_id = ANY(%s)",
                        (collection_ids,),
                    )
            logger.info("set %s phase_2 dp collections to PENDING", len(prepared_rows))

        for index, row in enumerate(prepared_rows):
            payload = {
                "loan_id": row.get("loan_id"),
                "application_id": row.get("application_id"),
                "application_no": row.get("application_no"),
                "phase": row.get("phase"),
                "dp_collection_id": row.get("dp_collection_id"),
                "status_transition": "PENDING->DONE",
            }
            if runtime.execute and not runtime.dry_run:
                if index > 0:
                    time.sleep(DONE_UPDATE_DELAY_SECONDS)
                logger.info(
                    "updating collection to DONE (PENDING->DONE): index=%s loan_id=%s application_id=%s dp_collection_id=%s",
                    index,
                    row.get("loan_id"),
                    row.get("application_id"),
                    row.get("dp_collection_id"),
                )
                with db.conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE collections SET status='DONE' WHERE collection_id=%s AND status='PENDING'",
                            (int(row["dp_collection_id"]),),
                        )
                payload["executed"] = True
            else:
                payload["executed"] = False
            updated.append(payload)
            cp.mark_completed(row["dp_collection_id"])
    finally:
        db.close()

    out = write_audit_xlsx(
        paths.generated_sheets / "script_2",
        "script_2_phase2",
        {"updated": updated, "skipped": skipped, "failed": failed, "multi_collection_conflict": conflicts},
    )
    log_stage_summary(
        logger,
        "script_2_phase2_emi_creation",
        loaded=len(rows),
        buckets={
            "updated": updated,
            "skipped": skipped,
            "failed": failed,
            "multi_collection_conflict": conflicts,
        },
        reason_fields_by_bucket={
            "skipped": ("reason",),
            "failed": ("failure_reason",),
            "multi_collection_conflict": ("failure_reason",),
        },
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
