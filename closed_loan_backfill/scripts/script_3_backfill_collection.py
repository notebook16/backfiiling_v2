#!/usr/bin/env python3
from __future__ import annotations

import csv
from contextlib import nullcontext
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

import pandas as pd
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

STAGE = "script_3_backfill_collection"
DEFAULT_INPUT = "script_1_validation_success_latest.xlsx"
DB_SHEET_NAME = "DB_SHEET.xlsx"
LOS_DATA_NAME = "LOS _Data.xlsx"
TARGET_PHASES = {"PHASE_2", "PHASE_3"}
STATUS_PENDING = "PENDING"
STATUS_DONE = "DONE"
PHASE2_DIR_NAME = "produced_sheets_phase_2"

CENTER_MANAGER_TO_CENTER_IDS = {
    1072: [2],
    1060: [1, 3, 7, 8, 15],
    1084: [4, 11],
    1071: [5],
    427: [9],
    1489: [10],
    2368: [13],
    2017: [12],
    3326: [14],
    3583: [6],
    3819: [16],
    3911: [17],
    4421: [18],
    4428: [19],
}


def _run_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _clean_key(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if not _is_blank(value):
            return value
    return None


def _normalize_token(value: Any) -> str:
    return _clean_key(value).upper()


def _normalize_app_key(value: Any) -> str:
    text = _normalize_token(value)
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        text = text.replace(dash, "-")
    return text.replace(" ", "")


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
    return text


def _to_decimal(value: Any) -> Decimal | None:
    if _is_blank(value):
        return None
    text = str(value).strip().replace(",", "")
    if text.lower() in {"nan", "none", "return", "recovered", "closed", "end", "nil"}:
        return None
    try:
        return Decimal(text)
    except Exception:  # noqa: BLE001
        return None


def _to_int(value: Any) -> int | None:
    decimal_value = _to_decimal(value)
    if decimal_value is None:
        return None
    try:
        return int(decimal_value)
    except Exception:  # noqa: BLE001
        return None


def _index_by(
    records: list[dict[str, Any]],
    key: str,
    *,
    normalizer: Callable[[Any], str],
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in records:
        row_key = normalizer(row.get(key))
        if row_key and row_key not in indexed:
            indexed[row_key] = row
    return indexed


def _group_by(
    records: list[dict[str, Any]],
    key: str,
    *,
    normalizer: Callable[[Any], str],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in records:
        row_key = normalizer(row.get(key))
        if row_key:
            grouped.setdefault(row_key, []).append(row)
    return grouped


def _excel_safe_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.tz_localize(None)
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value
    return value


def _excel_safe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: _excel_safe_value(value) for key, value in row.items()} for row in records]


def _resolve_input_file(paths, input_name: str) -> Path:
    candidate = Path(input_name)
    if candidate.is_absolute():
        resolved = candidate
    else:
        resolved = (paths.generated_sheets / "script_1" / input_name).resolve()

    if resolved.exists():
        return resolved

    raise FileNotFoundError(
        "script_3 requires the validated script_1 output as input; "
        f"expected file not found: {resolved}"
    )


def _extract_loan_ids(rows: list[dict[str, Any]]) -> list[int]:
    normalized: set[int] = set()
    for row in rows:
        loan_id = _to_int(row.get("loan_id"))
        if loan_id is not None:
            normalized.add(loan_id)
    return sorted(normalized)


def _fetch_db_sheet_rows(logger, loan_ids: list[int]) -> list[dict[str, Any]]:
    if not loan_ids:
        return []

    db = DbClient(logger)
    try:
        with db.conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        c.collection_id,
                        c.emi_installment_no,
                        c.loan_id,
                        c.status,
                        c.center_id,
                        c.due_date,
                        c.due_amount,
                        c.collection_type,
                        c.collection_subtype,
                        c.loan_term_id,
                        c.expire_reason,
                        lt.emi_amt AS monthly_payble,
                        lt.emi_tenure
                    FROM collections c
                    LEFT JOIN loan_terms lt
                        ON c.loan_term_id = lt.loan_term_id
                    WHERE c.loan_id = ANY(%s)
                    AND c.is_active = true
                    ORDER BY c.loan_id, c.collection_id
                    """,
                    (loan_ids,),
                )
                return [dict(row) for row in cur.fetchall()]
    finally:
        db.close()


def _find_los_row(
    tracker_row: dict[str, Any],
    *,
    los_by_app: dict[str, dict[str, Any]],
    los_by_application_id: dict[str, dict[str, Any]],
    los_by_loan_term_id: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    tracker_app = _normalize_app_key(tracker_row.get("Application No.") or tracker_row.get("application_no"))
    tracker_application_id = _normalize_identifier(tracker_row.get("application_id"))
    tracker_loan_term_id = _normalize_identifier(tracker_row.get("loan_term_id"))

    if tracker_app and tracker_app in los_by_app:
        return los_by_app[tracker_app]
    if tracker_application_id and tracker_application_id in los_by_application_id:
        return los_by_application_id[tracker_application_id]
    if tracker_loan_term_id and tracker_loan_term_id in los_by_loan_term_id:
        return los_by_loan_term_id[tracker_loan_term_id]
    return None


def _tracker_emi_value(row: dict[str, Any], base_column: str, emi_no: int) -> Any:
    suffix = "" if emi_no <= 1 else f".{emi_no - 1}"
    return row.get(f"{base_column}{suffix}")


def _is_valid_paid_on(value: Any) -> bool:
    if _is_blank(value):
        return False
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return True
    if isinstance(value, time):
        return True
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
            return 25000 <= numeric <= 80000
        except Exception:  # noqa: BLE001
            return False
    text = _clean_key(value)
    if not text:
        return False
    if text.lower() in {"recovered", "return", "closed", "end"}:
        return False
    if text.replace(".", "", 1).isdigit():
        return False
    return True


def _paid_on_to_iso_timestamp(value: Any) -> str:
    if _is_blank(value):
        return ""
    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()
    elif isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.min)
    elif isinstance(value, (int, float)):
        numeric = float(value)
        if not 25000 <= numeric <= 80000:
            return ""
        dt = pd.Timestamp("1899-12-30") + pd.to_timedelta(numeric, unit="D")
        dt = dt.to_pydatetime()
    else:
        text = _clean_key(value)
        if not text or text.replace(".", "", 1).isdigit():
            return ""
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return ""
        dt = parsed.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _is_part_subtype(value: Any) -> bool:
    subtype = _normalize_token(value).replace("-", "_").replace(" ", "")
    return subtype in {"PART1", "PART2", "PART_1", "PART_2", "P1", "P2"}


def _build_center_to_manager_map() -> dict[int, int]:
    center_to_manager: dict[int, int] = {}
    for manager_id, center_ids in CENTER_MANAGER_TO_CENTER_IDS.items():
        for center_id in center_ids:
            center_to_manager[center_id] = manager_id
    return center_to_manager


def _failure_payload(row: dict[str, Any], reason: str) -> dict[str, Any]:
    payload = mandatory_failure_fields(row.get("loan_id"), row.get("application_id"), reason, STAGE)
    payload["application_no"] = row.get("Application No.") or row.get("application_no")
    payload["phase"] = row.get("phase")
    return payload


def _skip_payload(row: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "loan_id": row.get("loan_id"),
        "application_id": row.get("application_id"),
        "application_no": row.get("Application No.") or row.get("application_no"),
        "phase": row.get("phase"),
        "reason": reason,
    }


def _write_csv(path: Path, headers: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def _write_latest_success(base_dir: Path, rows: list[dict[str, Any]]) -> Path:
    output = base_dir / "script_3_backfill_success_latest.xlsx"
    pd.DataFrame(rows).to_excel(output, index=False)
    return output


def _note_cannot_update(
    blocked: dict[str, dict[str, str]],
    *,
    collection_id: str,
    loan_id: str,
    application_no: str,
    reason: str,
) -> None:
    entry = blocked.get(collection_id)
    if entry is None:
        blocked[collection_id] = {
            "loan_id": loan_id,
            "application_no": application_no,
            "reason": reason,
        }
        return
    existing_reasons = {item.strip() for item in entry["reason"].split(";") if item.strip()}
    if reason not in existing_reasons:
        entry["reason"] = "; ".join([entry["reason"], reason]).strip("; ").strip()


def run() -> int:
    parser = parse_common_args("Script 3 - Collection backfill orchestration")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    cp = CheckpointStore(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)

    output_dir = paths.generated_sheets / "script_3"
    phase2_dir = output_dir / PHASE2_DIR_NAME
    run_ts = _run_ts()

    input_file = _resolve_input_file(paths, args.input)
    tracker_rows = to_records(read_excel(input_file, 0))
    logger.info("loaded %s validated tracker rows from %s", len(tracker_rows), input_file)

    loan_ids = _extract_loan_ids(tracker_rows)
    db_rows = _fetch_db_sheet_rows(logger, loan_ids)
    db_sheet_path = paths.source_sheets / DB_SHEET_NAME
    pd.DataFrame(_excel_safe_records(db_rows)).to_excel(db_sheet_path, index=False)
    logger.info("generated DB sheet: %s (%s rows)", db_sheet_path, len(db_rows))

    los_rows = to_records(read_excel(paths.source_sheets / LOS_DATA_NAME, 0))
    logger.info("loaded %s LOS rows from %s", len(los_rows), paths.source_sheets / LOS_DATA_NAME)

    db_by_loan_id = _group_by(db_rows, "loan_id", normalizer=_normalize_identifier)
    los_by_app = _index_by(los_rows, "Application Number", normalizer=_normalize_app_key)
    los_by_application_id = _index_by(los_rows, "application_id", normalizer=_normalize_identifier)
    los_by_loan_term_id = _index_by(los_rows, "loan_term_id", normalizer=_normalize_identifier)
    center_to_manager = _build_center_to_manager_map()
    completed = cp.completed() if runtime.resume else set()

    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    manual: list[dict[str, Any]] = []
    updated_collection_rows: list[dict[str, Any]] = []
    amount_adjustment_rows: list[dict[str, Any]] = []
    phase2_trans_rows: list[tuple[Any, ...]] = []
    phase2_comment_rows: list[tuple[Any, ...]] = []
    blocked_by_collection: dict[str, dict[str, str]] = {}
    completed_collection_keys: list[str] = []
    next_trans_id = 15000
    next_comment_id = 15000

    db = DbClient(logger)
    execute_mode = runtime.execute and not runtime.dry_run
    conn_cm = db.conn() if execute_mode else nullcontext(None)
    try:
        with conn_cm as conn:
            db_cursor = conn.cursor() if conn is not None else None
            for row in tracker_rows:
                phase = _normalize_token(row.get("phase"))
                if phase and phase not in TARGET_PHASES:
                    skipped.append(_skip_payload(row, "non_script_3_phase"))
                    continue

                loan_id = _normalize_identifier(row.get("loan_id"))
                application_id = _normalize_identifier(row.get("application_id"))
                application_no = _clean_key(row.get("Application No.") or row.get("application_no"))
                loan_term_id = _normalize_identifier(row.get("loan_term_id"))
                if not (loan_id or application_id or application_no):
                    failed.append(_failure_payload(row, "missing identifiers"))
                    continue
                if not loan_id:
                    failed.append(_failure_payload(row, "missing loan_id in script_1 output"))
                    continue

                db_matches = db_by_loan_id.get(loan_id, [])
                if not db_matches:
                    failed.append(_failure_payload(row, "loan_id missing in generated DB_SHEET"))
                    continue

                los_row = _find_los_row(
                    row,
                    los_by_app=los_by_app,
                    los_by_application_id=los_by_application_id,
                    los_by_loan_term_id=los_by_loan_term_id,
                )
                if not los_row:
                    failed.append(_failure_payload(row, "LOS mapping missing"))
                    continue

                los_app = _clean_key(los_row.get("Application Number"))
                los_application_id = _normalize_identifier(los_row.get("application_id"))
                los_loan_term_id = _normalize_identifier(los_row.get("loan_term_id"))

                resolved_application_id = _normalize_identifier(_first_non_empty(application_id, los_application_id))
                resolved_loan_term_id = _normalize_identifier(_first_non_empty(loan_term_id, los_loan_term_id))
                resolved_customer_id = _normalize_identifier(
                    _first_non_empty(row.get("customer_id"), los_row.get("customer_id"))
                )

                if application_no and los_app and _normalize_app_key(application_no) != _normalize_app_key(los_app):
                    failed.append(_failure_payload(row, "tracker and LOS application number mismatch"))
                    continue
                if application_id and los_application_id and application_id != los_application_id:
                    failed.append(_failure_payload(row, "tracker and LOS application_id mismatch"))
                    continue
                if loan_term_id and los_loan_term_id and loan_term_id != los_loan_term_id:
                    failed.append(_failure_payload(row, "tracker and LOS loan_term_id mismatch"))
                    continue

                db_loan_term_ids = {
                    _normalize_identifier(db_row.get("loan_term_id"))
                    for db_row in db_matches
                    if _normalize_identifier(db_row.get("loan_term_id"))
                }
                if resolved_loan_term_id and db_loan_term_ids and resolved_loan_term_id not in db_loan_term_ids:
                    failed.append(_failure_payload(row, "tracker and DB_SHEET loan_term_id mismatch"))
                    continue

                row_updated_ids: list[str] = []
                row_processed_ids: list[str] = []
                row_blocked_ids: list[str] = []
                row_block_reasons: list[str] = []
                row_already_processed_ids: list[str] = []

                for db_row in db_matches:
                    emi_no = _to_int(db_row.get("emi_installment_no"))
                    collection_id = _normalize_identifier(db_row.get("collection_id"))
                    if not collection_id or emi_no is None or emi_no <= 0:
                        continue

                    row_processed_ids.append(collection_id)
                    collection_key = f"collection:{collection_id}"
                    if collection_key in completed:
                        row_already_processed_ids.append(collection_id)
                        continue

                    status = _normalize_token(db_row.get("status"))
                    # Out of scope: script_3 only handles PENDING -> DONE.
                    # Non-PENDING rows are ignored and not treated as cannot-update blockers.
                    if status != STATUS_PENDING:
                        continue

                    if _is_part_subtype(db_row.get("collection_subtype")):
                        _note_cannot_update(
                            blocked_by_collection,
                            collection_id=collection_id,
                            loan_id=loan_id,
                            application_no=los_app or application_no,
                            reason="part_payment_not_handled",
                        )
                        row_blocked_ids.append(collection_id)
                        row_block_reasons.append("part_payment_not_handled")
                        continue

                    due_amount = _to_decimal(db_row.get("due_amount"))
                    tracker_paid_on = _tracker_emi_value(row, "Paid On", emi_no)
                    tracker_cash = _to_decimal(_tracker_emi_value(row, "Cash Amount", emi_no))
                    tracker_online = _to_decimal(_tracker_emi_value(row, "Online Amount", emi_no))
                    tracker_total = _to_decimal(_tracker_emi_value(row, "Total Amount", emi_no))
                    tracker_comment = _clean_key(_tracker_emi_value(row, "Comments", emi_no))
                    tracker_due_date = _tracker_emi_value(row, f"EMI - {emi_no}", 1)

                    if not _is_valid_paid_on(tracker_paid_on):
                        _note_cannot_update(
                            blocked_by_collection,
                            collection_id=collection_id,
                            loan_id=loan_id,
                            application_no=los_app or application_no,
                            reason="tracker_paid_on_missing_or_invalid",
                        )
                        row_blocked_ids.append(collection_id)
                        row_block_reasons.append("tracker_paid_on_missing_or_invalid")
                        continue

                    if due_amount is None or tracker_total is None:
                        _note_cannot_update(
                            blocked_by_collection,
                            collection_id=collection_id,
                            loan_id=loan_id,
                            application_no=los_app or application_no,
                            reason="tracker_total_or_due_amount_missing",
                        )
                        row_blocked_ids.append(collection_id)
                        row_block_reasons.append("tracker_total_or_due_amount_missing")
                        continue

                    if tracker_total != due_amount:
                        _note_cannot_update(
                            blocked_by_collection,
                            collection_id=collection_id,
                            loan_id=loan_id,
                            application_no=los_app or application_no,
                            reason="amount_mismatch_exact_required",
                        )
                        row_blocked_ids.append(collection_id)
                        row_block_reasons.append("amount_mismatch_exact_required")
                        amount_adjustment_rows.append(
                            {
                                "collection_id": collection_id,
                                "loan_id": loan_id,
                                "application_id": resolved_application_id,
                                "application_no": los_app or application_no,
                                "emi_installment_no": emi_no,
                                "tracker_total_amount": str(tracker_total),
                                "due_amount": str(due_amount),
                                "difference": str(tracker_total - due_amount),
                            }
                        )
                        continue

                    center_id = _to_int(db_row.get("center_id"))
                    manager_id = center_to_manager.get(center_id or -1)
                    if manager_id is None:
                        _note_cannot_update(
                            blocked_by_collection,
                            collection_id=collection_id,
                            loan_id=loan_id,
                            application_no=los_app or application_no,
                            reason="center_id_manager_mapping_missing",
                        )
                        row_blocked_ids.append(collection_id)
                        row_block_reasons.append("center_id_manager_mapping_missing")
                        continue

                    if db_cursor is not None:
                        db_cursor.execute(
                            """
                            UPDATE collections
                            SET status=%s, created_by=%s, collector_id=%s, fine_amount=%s, discount_amount=%s
                            WHERE collection_id=%s AND is_active=true AND UPPER(status)=%s
                            """,
                            (
                                STATUS_DONE,
                                manager_id,
                                manager_id,
                                Decimal("0"),
                                Decimal("0"),
                                int(collection_id),
                                STATUS_PENDING,
                            ),
                        )
                        if db_cursor.rowcount != 1:
                            _note_cannot_update(
                                blocked_by_collection,
                                collection_id=collection_id,
                                loan_id=loan_id,
                                application_no=los_app or application_no,
                                reason="update_rowcount_not_1",
                            )
                            row_blocked_ids.append(collection_id)
                            row_block_reasons.append("update_rowcount_not_1")
                            continue

                    row_updated_ids.append(collection_id)
                    completed_collection_keys.append(collection_key)
                    updated_collection_rows.append(
                        {
                            "loan_id": loan_id,
                            "application_id": resolved_application_id,
                            "application_no": los_app or application_no,
                            "customer_id": resolved_customer_id,
                            "loan_term_id": resolved_loan_term_id,
                            "collection_id": collection_id,
                            "emi_installment_no": emi_no,
                            "status_after": STATUS_DONE if execute_mode else "WOULD_UPDATE_TO_DONE",
                            "collection_subtype": db_row.get("collection_subtype"),
                            "due_amount": str(due_amount),
                            "tracker_total_amount": str(tracker_total),
                            "tracker_cash_amount": "" if tracker_cash is None else str(tracker_cash),
                            "tracker_online_amount": "" if tracker_online is None else str(tracker_online),
                            "tracker_paid_on": _paid_on_to_iso_timestamp(tracker_paid_on),
                            "tracker_due_date": "" if _is_blank(tracker_due_date) else str(tracker_due_date),
                            "tracker_comment": tracker_comment,
                            "center_id": center_id,
                            "manager_id": manager_id,
                            "db_sheet_path": str(db_sheet_path),
                        }
                    )

                    created_at = _paid_on_to_iso_timestamp(tracker_paid_on)
                    actual_created_at = datetime.now(timezone.utc).isoformat()
                    from_customer = resolved_customer_id or ""
                    if tracker_cash is not None and tracker_cash > 0:
                        phase2_trans_rows.append(
                            (
                                next_trans_id,
                                True,
                                1,
                                center_id,
                                int(collection_id),
                                True,
                                "",
                                "CREDIT",
                                "RECORD_PAYMENT",
                                "CASH",
                                str(tracker_cash),
                                manager_id,
                                0,
                                "ACCEPTED",
                                manager_id,
                                created_at,
                                "",
                                True,
                                0,
                                from_customer,
                                manager_id,
                                manager_id,
                                created_at,
                                los_app or application_no,
                                "",
                                "",
                                "",
                                actual_created_at,
                            )
                        )
                        next_trans_id += 1

                    if tracker_online is not None and tracker_online > 0:
                        phase2_trans_rows.append(
                            (
                                next_trans_id,
                                True,
                                1,
                                center_id,
                                int(collection_id),
                                True,
                                "DUMMYBC2",
                                "CREDIT",
                                "RECORD_PAYMENT",
                                "UPI",
                                str(tracker_online),
                                manager_id,
                                0,
                                "ACCEPTED",
                                manager_id,
                                created_at,
                                "",
                                True,
                                0,
                                from_customer,
                                manager_id,
                                manager_id,
                                created_at,
                                los_app or application_no,
                                "",
                                "",
                                "",
                                actual_created_at,
                            )
                        )
                        next_trans_id += 1

                    if tracker_comment:
                        comment_prefix = f"Application NO. {los_app or application_no} || " if (los_app or application_no) else ""
                        phase2_comment_rows.append(
                            (
                                next_comment_id,
                                1,
                                int(collection_id),
                                False,
                                f"{comment_prefix}{tracker_comment}",
                                False,
                                manager_id,
                                actual_created_at,
                            )
                        )
                        next_comment_id += 1

                success.append(
                    {
                        **row,
                        "phase": phase,
                        "loan_id": loan_id,
                        "application_id": resolved_application_id,
                        "loan_term_id": resolved_loan_term_id,
                        "customer_id": resolved_customer_id,
                        "los_application_number": los_app,
                        "los_customer_id": _normalize_identifier(los_row.get("customer_id")),
                        "los_application_id": los_application_id,
                        "los_sale_id": los_row.get("sale_id"),
                        "db_collection_count": len(db_matches),
                        "db_pending_collection_count": sum(
                            1 for db_row in db_matches if _normalize_token(db_row.get("status")) == STATUS_PENDING
                        ),
                        "db_collection_ids": ",".join(
                            _normalize_identifier(db_row.get("collection_id"))
                            for db_row in db_matches
                            if _normalize_identifier(db_row.get("collection_id"))
                        ),
                        "updated_collection_count": len(row_updated_ids),
                        "updated_collection_ids": ",".join(row_updated_ids),
                        "already_processed_collection_count": len(row_already_processed_ids),
                        "already_processed_collection_ids": ",".join(row_already_processed_ids),
                        "blocked_collection_count": len(set(row_blocked_ids)),
                        "blocked_collection_ids": ",".join(sorted(set(row_blocked_ids))),
                        "blocked_reasons": "; ".join(sorted(set(row_block_reasons))),
                        "processed_collection_count": len(set(row_processed_ids)),
                        "backfill_result": (
                            "updated"
                            if execute_mode and row_updated_ids
                            else "would_update"
                            if row_updated_ids
                            else "already_processed_only"
                            if row_already_processed_ids and not row_blocked_ids
                            else "validated_no_eligible_collection"
                        ),
                        "db_sheet_path": str(db_sheet_path),
                        "backfill_stage": "mapping_validated_and_backfill_processed",
                    }
                )
    finally:
        db.close()

    if execute_mode:
        for key in completed_collection_keys:
            cp.mark_completed(key)

    mapping_validated_input = output_dir / "script_3_mapping_validated_input.xlsx"
    pd.DataFrame(success).to_excel(mapping_validated_input, index=False)
    latest_success = _write_latest_success(output_dir, success)
    logger.info("mapping-validated input: %s", mapping_validated_input)
    logger.info("latest success file: %s", latest_success)

    updated_collections_path = output_dir / f"script_3_updated_collections_{run_ts}.xlsx"
    pd.DataFrame(updated_collection_rows).to_excel(updated_collections_path, index=False)
    logger.info("updated collections sheet: %s", updated_collections_path)

    amount_adjustment_path = output_dir / f"script_3_amount_mismatches_{run_ts}.xlsx"
    pd.DataFrame(amount_adjustment_rows).to_excel(amount_adjustment_path, index=False)
    logger.info("amount mismatch sheet: %s", amount_adjustment_path)

    cannot_update_rows = [
        (
            collection_id,
            payload["loan_id"],
            payload["application_no"],
            payload["reason"],
        )
        for collection_id, payload in sorted(blocked_by_collection.items(), key=lambda item: int(item[0]))
    ]
    cannot_update_path = output_dir / f"script_3_cannot_update_collections_{run_ts}.csv"
    _write_csv(
        cannot_update_path,
        ["collection_id", "loan_id", "application_no", "reason"],
        cannot_update_rows,
    )
    logger.info("cannot-update report: %s", cannot_update_path)

    phase2_trans_path = phase2_dir / f"collection_trans_{run_ts}.csv"
    _write_csv(
        phase2_trans_path,
        [
            "trans_id",
            "is_aggr_trans",
            "org_id",
            "center_id",
            "collection_id",
            "is_active",
            "transaction_cd",
            "trans_type",
            "trans_subtype",
            "mode",
            "amount",
            "recorded_by",
            "scrap_type_id",
            "recon_status",
            "recon_by",
            "recon_at",
            "recon_comment",
            "is_settled",
            "from_user",
            "from_customer",
            "to_user",
            "created_by",
            "created_at",
            "trans_comments",
            "parent_trans_id",
            "request_id",
            "deposit_trans_ids",
            "actual_created_at",
        ],
        phase2_trans_rows,
    )
    logger.info("PHASE 2 collection_trans: %s (%s rows)", phase2_trans_path, len(phase2_trans_rows))

    phase2_comments_path = phase2_dir / f"collection_comments_{run_ts}.csv"
    _write_csv(
        phase2_comments_path,
        [
            "collection_comment_id",
            "org_id",
            "collection_id",
            "is_called",
            "comment",
            "is_latest",
            "created_by",
            "created_at",
        ],
        phase2_comment_rows,
    )
    logger.info("PHASE 2 collection_comments: %s (%s rows)", phase2_comments_path, len(phase2_comment_rows))

    out = write_audit_xlsx(
        output_dir,
        "script_3_backfill",
        {"success": success, "failed": failed, "skipped": skipped, "manual_intervention": manual},
    )
    log_stage_summary(
        logger,
        "script_3_backfill_collection",
        loaded=len(tracker_rows),
        buckets={
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "manual_intervention": manual,
            "updated_collections": updated_collection_rows,
            "phase2_collection_trans": [{"mode": row[9]} for row in phase2_trans_rows],
            "phase2_collection_comments": [{"generated": True} for _ in phase2_comment_rows],
        },
        reason_fields_by_bucket={
            "failed": ("failure_reason",),
            "skipped": ("reason",),
            "manual_intervention": ("failure_reason",),
            "success": ("backfill_result",),
            "updated_collections": ("status_after",),
            "phase2_collection_trans": ("mode",),
        },
    )
    logger.info("audit files: %s", out)
    logger.info(
        "script_3 summary: success_rows=%s failed_rows=%s skipped_rows=%s updated_collections=%s phase2_trans=%s phase2_comments=%s",
        len(success),
        len(failed),
        len(skipped),
        len(updated_collection_rows),
        len(phase2_trans_rows),
        len(phase2_comment_rows),
    )
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
