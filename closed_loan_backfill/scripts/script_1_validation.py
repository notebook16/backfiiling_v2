#!/usr/bin/env python3
from __future__ import annotations

from contextlib import nullcontext
import time
from datetime import timedelta
from decimal import Decimal
from typing import Any, Callable

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

STAGE = "script_1_validation"
DETAILS_TARGET_STATUSES = {"TARGET"}
DETAILS_TARGET_PHASES = {"PHASE_2", "PHASE_3"}
EXPECTED_TARGET_COUNT = 49
CLOSE_STATUS_UPDATE_DELAY_SECONDS = 0.5


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if not _is_blank(value):
            return value
    return None


def _clean_key(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _normalize_token(value: Any) -> str:
    return _clean_key(value).upper()


def _is_true_flag(value: Any) -> bool:
    token = _normalize_token(value)
    return token in {"TRUE", "1", "1.0", "YES", "Y"}


def _to_decimal(value: Any) -> Decimal | None:
    if _is_blank(value):
        return None
    text = str(value).strip().replace(",", "")
    if text.lower() in {"nan", "none", "return", "recovered"}:
        return None
    try:
        return Decimal(text)
    except Exception:  # noqa: BLE001
        return None


def _to_int(value: Any) -> int | None:
    if _is_blank(value):
        return None
    try:
        return int(Decimal(str(value).strip().replace(",", "")))
    except Exception:  # noqa: BLE001
        return None


def _normalize_identifier(value: Any) -> str:
    if _is_blank(value):
        return ""
    decimal_value = _to_decimal(value)
    if decimal_value is not None and decimal_value == decimal_value.to_integral_value():
        return str(int(decimal_value))
    return _clean_key(value)


def _to_timestamp(value: Any) -> pd.Timestamp:
    if _is_blank(value):
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value

    text = str(value).strip().replace(",", "")
    decimal_value = _to_decimal(text)
    if decimal_value is not None:
        try:
            numeric = float(decimal_value)
            if numeric > 20000:
                return pd.Timestamp("1899-12-30") + pd.to_timedelta(numeric, unit="D")
        except Exception:  # noqa: BLE001
            pass
    return pd.to_datetime(value, errors="coerce")


def _index_by(
    records: list[dict[str, Any]],
    key: str,
    *,
    normalizer: Callable[[Any], str] | None = None,
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    norm = normalizer or _clean_key
    for row in records:
        row_key = norm(row.get(key))
        if row_key and row_key not in indexed:
            indexed[row_key] = row
    return indexed


def _extract_target_details(details_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    target_details = [
        row
        for row in details_records
        if _normalize_token(row.get("implimentation_status")) in DETAILS_TARGET_STATUSES
        and _normalize_token(row.get("phase")) in DETAILS_TARGET_PHASES
    ]
    if len(target_details) != EXPECTED_TARGET_COUNT:
        raise ValueError(
            "unexpected TARGET record count in CLOSED_LOANS_DETAILS: "
            f"expected {EXPECTED_TARGET_COUNT}, found {len(target_details)}"
        )
    return target_details


def _db_lookup_ids_from_details(target_details: list[dict[str, Any]]) -> list[int]:
    lookup_ids: set[int] = set()
    for row in target_details:
        app_id = _to_int(row.get("application_id"))
        loan_id = _to_int(row.get("loan_id"))
        if app_id is not None:
            lookup_ids.add(app_id)
        if loan_id is not None:
            lookup_ids.add(loan_id)
    return sorted(lookup_ids)


def _generate_db_details_sheet(logger, paths, target_details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lookup_ids = _db_lookup_ids_from_details(target_details)
    db_details_path = paths.source_sheets / "CLOSED_LOAN_DB_DETAILS.xlsx"
    if not lookup_ids:
        pd.DataFrame([]).to_excel(db_details_path, index=False)
        logger.info("generated DB details sheet: %s (0 rows)", db_details_path)
        return []

    load_env_file(paths.root.parent / ".env", logger)
    db = DbClient(logger)
    try:
        with db.conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        c.loan_id,
                        la.installation_id,
                        la.close_status,
                        CASE WHEN c.collection_type = 'DP' THEN lt.dp_amt ELSE 0 END AS dp_amt,
                        lt.emi_amt,
                        ins.installation_date,
                        CASE WHEN c.collection_type = 'DP' THEN 1 ELSE 0 END AS dp_count,
                        (
                            SELECT COUNT(*)
                            FROM collections c2
                            WHERE c2.loan_id = c.loan_id
                              AND c2.collection_type = 'EMI'
                              AND c2.is_active = true
                        ) AS emi_count,
                        lt.emi_tenure AS tenure,
                        c.collection_id AS dp_collection_id
                    FROM collections c
                    LEFT JOIN loan_terms lt
                        ON c.loan_term_id = lt.loan_term_id
                    LEFT JOIN loan_applications la
                        ON c.loan_id = la.application_id
                    LEFT JOIN installations ins
                        ON la.installation_id = ins.installation_id
                    WHERE c.loan_id = ANY(%s)
                      AND c.is_active = true
                      AND c.collection_type = 'DP'
                    ORDER BY c.loan_id, c.collection_id
                    """,
                    (lookup_ids,),
                )
                headers = [col.name for col in cur.description]
                rows = [dict(zip(headers, db_row, strict=False)) for db_row in cur.fetchall()]
    finally:
        db.close()

    pd.DataFrame(rows).to_excel(db_details_path, index=False)
    logger.info("generated DB details sheet: %s (%s rows)", db_details_path, len(rows))
    return rows


def _load_records(paths, logger) -> list[dict[str, Any]]:
    tracker_path = paths.source_sheets / "CLOSED_LOAN_TRACKER.xlsx"
    details_path = paths.source_sheets / "CLOSED_LOANS_DETAILS.xlsx"

    emi_records = to_records(read_excel(tracker_path, "EMI"))
    dp_records = to_records(read_excel(tracker_path, "DP"))
    details_records = to_records(read_excel(details_path, 0))
    target_details = _extract_target_details(details_records)
    db_detail_records = _generate_db_details_sheet(logger, paths, target_details)

    emi_by_app = _index_by(emi_records, "Application No.")
    dp_by_app = _index_by(dp_records, "Application No.")
    db_by_loan_id = _index_by(
        db_detail_records,
        "loan_id",
        normalizer=_normalize_identifier,
    )

    normalized: list[dict[str, Any]] = []
    for details_row in target_details:
        app_no = _clean_key(details_row.get("Application No."))
        emi_row = emi_by_app.get(app_no, {})
        dp_row = dp_by_app.get(app_no, {})
        application_id = _normalize_identifier(details_row.get("application_id"))
        installation_id = _normalize_identifier(details_row.get("installation_id"))
        db_row = db_by_loan_id.get(application_id, {})

        normalized.append(
            {
                **emi_row,
                "application_no": app_no,
                "application_id": application_id,
                "loan_term_id": details_row.get("loan_term_id"),
                "customer_id": details_row.get("customer_id"),
                "phase": _normalize_token(details_row.get("phase")),
                "implimentation_status": _normalize_token(details_row.get("implimentation_status")),
                "installation_id": _first_non_empty(installation_id, _normalize_identifier(db_row.get("installation_id"))),
                "loan_id": db_row.get("loan_id"),
                "close_status": _normalize_token(db_row.get("close_status")),
                "db_dp_amt": db_row.get("dp_amt"),
                "db_emi_amt": db_row.get("emi_amt"),
                "db_installation_date": db_row.get("installation_date"),
                "db_dp_count": db_row.get("dp_count"),
                "db_emi_count": db_row.get("emi_count"),
                "db_tenure": db_row.get("tenure"),
                "dp_collection_id": db_row.get("dp_collection_id"),
                "tracker_dp": dp_row.get("DP"),
                "tracker_installation_date": _first_non_empty(
                    emi_row.get("Installation Date"),
                    details_row.get("Installation Date TRACKER"),
                ),
                "tracker_emi_1_date": emi_row.get("EMI - 1"),
                "tracker_close_date": emi_row.get("CLOSE_DATE"),
            }
        )

    return normalized


def _failure_payload(row: dict[str, Any], reason: str) -> dict[str, Any]:
    payload = mandatory_failure_fields(row.get("loan_id"), row.get("application_id"), reason, STAGE)
    payload["application_no"] = row.get("application_no")
    payload["phase"] = row.get("phase")
    payload["installation_id"] = row.get("installation_id")
    return payload


def _write_latest_success(base_dir, rows: list[dict[str, Any]], columns: list[str]) -> None:
    output = base_dir / "script_1_validation_success_latest.xlsx"
    pd.DataFrame(rows, columns=columns).to_excel(output, index=False)


def run() -> int:
    parser = parse_common_args("Script 1 - Validation and eligibility checks")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    cp = CheckpointStore(STAGE, paths)

    records = _load_records(paths, logger)
    logger.info("loaded TARGET validation records: %s", len(records))
    completed = cp.completed() if runtime.resume else set()

    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    manual: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    execute_mode = runtime.execute and not runtime.dry_run
    db = None
    if execute_mode:
        load_env_file(paths.root.parent / ".env", logger)
        db = DbClient(logger, maxconn=max(runtime.max_workers, 2))

    close_status_update_count = 0
    close_status_already_pending_count = 0
    close_status_not_found_count = 0
    last_close_status_update_ts: float | None = None

    conn_cm = db.conn() if db is not None else nullcontext(None)
    try:
        with conn_cm as conn:
            cur = conn.cursor() if conn is not None else None
            for row in records:
                loan_id = row.get("loan_id")
                app_id = row.get("application_id")
                app_no = row.get("application_no")
                phase = row.get("phase")
                key = _clean_key(_first_non_empty(app_no, app_id, loan_id))
                if not key:
                    failed.append(_failure_payload(row, "missing identifiers"))
                    continue
                if key in completed:
                    skipped.append(
                        {
                            "loan_id": loan_id,
                            "application_id": app_id,
                            "application_no": app_no,
                            "phase": phase,
                            "reason": "already_processed",
                        }
                    )
                    continue
                if key in seen_keys:
                    failed.append(_failure_payload(row, "duplicate application"))
                    continue
                seen_keys.add(key)

                if _is_blank(app_id):
                    failed.append(_failure_payload(row, "application missing in CLOSED_LOANS_DETAILS"))
                    continue
                if _is_blank(app_no):
                    failed.append(_failure_payload(row, "application missing in CLOSED_LOAN_TRACKER EMI"))
                    continue
                if _normalize_token(row.get("implimentation_status")) not in DETAILS_TARGET_STATUSES:
                    skipped.append(
                        {
                            "loan_id": loan_id,
                            "application_id": app_id,
                            "application_no": app_no,
                            "phase": phase,
                            "reason": "non_target_status",
                        }
                    )
                    continue
                if phase not in DETAILS_TARGET_PHASES:
                    manual.append(_failure_payload(row, f"unsupported phase {phase or 'BLANK'}"))
                    continue
                if _is_blank(row.get("EMI")) or _is_blank(row.get("Tenure")):
                    manual.append(_failure_payload(row, "missing EMI or tenure in CLOSED_LOAN_TRACKER EMI"))
                    continue
                if _is_blank(loan_id):
                    manual.append(_failure_payload(row, "missing match in CLOSED_LOAN_DB_DETAILS via application_id/loan_id"))
                    continue

                tracker_tenure = _to_int(row.get("Tenure"))
                db_tenure = _to_int(row.get("db_tenure"))
                if tracker_tenure is None or db_tenure is None:
                    manual.append(_failure_payload(row, "missing tenure for validation"))
                    continue
                if tracker_tenure != db_tenure:
                    failed.append(_failure_payload(row, "tenure mismatch"))
                    continue

                tracker_emi = _to_decimal(row.get("EMI"))
                db_emi = _to_decimal(row.get("db_emi_amt"))
                if tracker_emi is None or db_emi is None:
                    manual.append(_failure_payload(row, "missing EMI for validation"))
                    continue
                if tracker_emi != db_emi:
                    failed.append(_failure_payload(row, "EMI mismatch"))
                    continue

                db_installation_date = _to_timestamp(row.get("db_installation_date"))
                if pd.isna(db_installation_date):
                    manual.append(_failure_payload(row, "missing installation date for validation"))
                    continue

                emi1 = _to_timestamp(row.get("tracker_emi_1_date"))
                if not pd.isna(emi1):
                    expected_emi1 = (
                        db_installation_date + timedelta(days=1) + pd.DateOffset(months=1)
                    ).normalize()
                    if emi1.normalize() != expected_emi1:
                        failed.append(_failure_payload(row, "EMI-1 validation failed"))
                        continue
                else:
                    tracker_installation_date = _to_timestamp(row.get("tracker_installation_date"))
                    installation_date_matches = (
                        not pd.isna(tracker_installation_date)
                        and tracker_installation_date.normalize() == db_installation_date.normalize()
                    )
                    if not installation_date_matches:
                        if pd.isna(tracker_installation_date):
                            manual.append(_failure_payload(row, "missing tracker installation date"))
                        else:
                            failed.append(_failure_payload(row, "Installation Date validation failed"))
                        continue

                tracker_dp = _to_decimal(row.get("tracker_dp"))
                db_dp = _to_decimal(row.get("db_dp_amt"))
                if tracker_dp is None or db_dp is None:
                    manual.append(_failure_payload(row, "missing DP for validation"))
                    continue
                if tracker_dp != db_dp:
                    failed.append(_failure_payload(row, "DP mismatch"))
                    continue

                if phase in DETAILS_TARGET_PHASES and _normalize_token(row.get("close_status")) != "PENDING":
                    logger.info(
                        "close_status must be PENDING before validation pass: application_id=%s loan_id=%s current_close_status=%s",
                        app_id,
                        loan_id,
                        row.get("close_status"),
                    )
                    if cur is None:
                        failed.append(_failure_payload(row, f"{phase} close_status must be PENDING"))
                        continue

                    if last_close_status_update_ts is not None:
                        elapsed = time.time() - last_close_status_update_ts
                        if elapsed < CLOSE_STATUS_UPDATE_DELAY_SECONDS:
                            time.sleep(CLOSE_STATUS_UPDATE_DELAY_SECONDS - elapsed)

                    application_id_value = _normalize_identifier(app_id)
                    loan_id_value = _normalize_identifier(loan_id)
                    logger.info(
                        "close_status update start: application_id=%s loan_id=%s",
                        application_id_value,
                        loan_id_value,
                    )

                    updated_this_record = 0
                    if application_id_value:
                        cur.execute(
                            """
                            UPDATE loan_applications
                            SET close_status = 'PENDING'
                            WHERE application_id = %s
                              AND COALESCE(UPPER(close_status), '') <> 'PENDING'
                            RETURNING application_id
                            """,
                            (int(application_id_value),),
                        )
                        updated_this_record += cur.rowcount

                    if updated_this_record == 0 and loan_id_value:
                        cur.execute(
                            """
                            UPDATE loan_applications
                            SET close_status = 'PENDING'
                            WHERE loan_id = %s
                              AND COALESCE(UPPER(close_status), '') <> 'PENDING'
                            RETURNING application_id
                            """,
                            (int(loan_id_value),),
                        )
                        updated_this_record += cur.rowcount

                    last_close_status_update_ts = time.time()

                    if updated_this_record > 0:
                        close_status_update_count += updated_this_record
                        row["close_status"] = "PENDING"
                        logger.info(
                            "close_status update success: application_id=%s loan_id=%s updated_rows=%s",
                            application_id_value,
                            loan_id_value,
                            updated_this_record,
                        )
                    else:
                        existing_found = False
                        if application_id_value:
                            cur.execute("SELECT 1 FROM loan_applications WHERE application_id=%s LIMIT 1", (int(application_id_value),))
                            existing_found = cur.fetchone() is not None
                        if not existing_found and loan_id_value:
                            cur.execute("SELECT 1 FROM loan_applications WHERE loan_id=%s LIMIT 1", (int(loan_id_value),))
                            existing_found = cur.fetchone() is not None

                        if existing_found:
                            close_status_already_pending_count += 1
                            row["close_status"] = "PENDING"
                            logger.info(
                                "close_status already pending: application_id=%s loan_id=%s",
                                application_id_value,
                                loan_id_value,
                            )
                        else:
                            close_status_not_found_count += 1
                            failed.append(_failure_payload(row, "loan_applications row missing for close_status update"))
                            logger.info(
                                "close_status target not found: application_id=%s loan_id=%s",
                                application_id_value,
                                loan_id_value,
                            )
                            continue

                if phase in DETAILS_TARGET_PHASES and _normalize_token(row.get("close_status")) != "PENDING":
                    failed.append(_failure_payload(row, f"{phase} close_status must be PENDING"))
                    continue

                if phase == "PHASE_2":
                    emi_count = _to_int(row.get("db_emi_count"))
                    if emi_count is None:
                        manual.append(_failure_payload(row, "missing emi_count for PHASE_2 validation"))
                        continue
                    if emi_count != 0:
                        failed.append(_failure_payload(row, "PHASE_2 emi_count must be 0"))
                        continue
                elif phase == "PHASE_3":
                    dp_count = _to_int(row.get("db_dp_count"))
                    emi_count = _to_int(row.get("db_emi_count"))
                    if dp_count is None or emi_count is None:
                        manual.append(_failure_payload(row, "missing DP/EMI counts for PHASE_3 validation"))
                        continue
                    if dp_count <= 0 or emi_count <= 0:
                        failed.append(_failure_payload(row, "PHASE_3 requires dp_count > 0 and emi_count > 0"))
                        continue

                row["backfill_stage"] = "validated"
                success.append(row)
                cp.mark_completed(key)
    finally:
        if db is not None:
            db.close()

    output_dir = paths.generated_sheets / "script_1"
    success_columns = list(records[0].keys()) + ["backfill_stage"] if records else ["backfill_stage"]
    out = write_audit_xlsx(
        output_dir,
        "script_1_validation",
        {"success": success, "failed": failed, "skipped": skipped, "manual_intervention": manual},
    )
    log_stage_summary(
        logger,
        "script_1_validation",
        loaded=len(records),
        buckets={
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "manual_intervention": manual,
        },
        reason_fields_by_bucket={
            "failed": ("failure_reason",),
            "skipped": ("reason",),
            "manual_intervention": ("failure_reason",),
        },
    )
    logger.info(
        "close_status inline update summary: updated_rows=%s already_pending=%s not_found=%s",
        close_status_update_count,
        close_status_already_pending_count,
        close_status_not_found_count,
    )
    _write_latest_success(output_dir, success, success_columns)
    logger.info("audit files: %s", out)
    logger.info("latest success file: %s", output_dir / "script_1_validation_success_latest.xlsx")
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
