#!/usr/bin/env python3
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

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

STAGE = "script_1_validation"
LEGACY_COLUMNS = {"loan_id", "application_id", "dp", "EMI", "tenure", "DP Date", "EMI-1 date"}


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


def _index_by(records: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in records:
        row_key = _clean_key(row.get(key))
        if row_key and row_key not in indexed:
            indexed[row_key] = row
    return indexed


def _group_by(records: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in records:
        row_key = _clean_key(row.get(key))
        if row_key:
            grouped.setdefault(row_key, []).append(row)
    return grouped


def _resolve_tracker_sheet(tracker_path, requested_sheet: str) -> str:
    sheet_names = pd.ExcelFile(tracker_path).sheet_names
    if requested_sheet in sheet_names:
        return requested_sheet
    if requested_sheet == "CLOSED_LOAN_TRACKER" and "EMI" in sheet_names:
        return "EMI"
    raise ValueError(f"Worksheet named '{requested_sheet}' not found. Available sheets: {sheet_names}")


def _pick_db_row(rows: list[dict[str, Any]], emi: Any, tenure: Any) -> dict[str, Any] | None:
    if not rows:
        return None
    target_emi = _to_decimal(emi)
    target_tenure = _to_int(tenure)
    for row in rows:
        row_emi = _to_decimal(_first_non_empty(row.get("monthly_payble"), row.get("monthly_payable")))
        row_tenure = _to_int(_first_non_empty(row.get("emi_tenure"), row.get("tenure")))
        if row_emi == target_emi and row_tenure == target_tenure:
            return row
    return rows[0]


def _load_records(paths, requested_sheet: str) -> tuple[list[dict[str, Any]], str]:
    tracker_path = paths.source_sheets / "CLOSED_LOAN_TRACKER.xlsx"
    resolved_sheet = _resolve_tracker_sheet(tracker_path, requested_sheet)
    tracker_df = read_excel(tracker_path, resolved_sheet)
    if LEGACY_COLUMNS.issubset(set(tracker_df.columns)):
        return to_records(tracker_df), resolved_sheet

    if resolved_sheet != "EMI":
        raise ValueError(
            f"Sheet '{resolved_sheet}' uses the new split layout. Use sheet 'EMI' so the script can merge EMI and DP data."
        )

    details_df = read_excel(paths.source_sheets / "CLOSED_LOANS_DETAILS.xlsx", 0)
    db_df = read_excel(paths.source_sheets / "DB_SHEET.xlsx", 0)

    emi_records = to_records(tracker_df)
    details_by_app = _index_by(to_records(details_df), "Application No.")
    db_rows_by_app = _group_by(to_records(db_df), "expire_reason")

    normalized: list[dict[str, Any]] = []
    for emi_row in emi_records:
        app_no = _clean_key(emi_row.get("Application No."))
        if not app_no:
            continue

        details_row = details_by_app.get(app_no, {})
        db_rows = db_rows_by_app.get(app_no, [])
        db_row = _pick_db_row(db_rows, emi_row.get("EMI"), emi_row.get("Tenure")) or {}

        normalized.append(
            {
                **emi_row,
                "application_no": app_no,
                "loan_id": _first_non_empty(details_row.get("loan_id"), db_row.get("loan_id")),
                "application_id": details_row.get("application_id"),
                "loan_term_id": _first_non_empty(details_row.get("loan_term_id"), db_row.get("loan_term_id")),
                "customer_id": details_row.get("customer_id"),
                "tenure": _first_non_empty(emi_row.get("Tenure"), details_row.get("Tenure")),
                "EMI": _first_non_empty(emi_row.get("EMI"), details_row.get("EMI")),
                "Installation Date": details_row.get("Installation Date"),
                "EMI-1 date": emi_row.get("EMI - 1"),
                "CLOSED_DATE": _first_non_empty(emi_row.get("CLOSE_DATE"), details_row.get("CLOSED DATE")),
                "details_tenure": details_row.get("Tenure"),
                "details_emi": details_row.get("EMI"),
                "db_emi_tenure": _first_non_empty(db_row.get("emi_tenure"), db_row.get("tenure")),
                "db_monthly_payable": _first_non_empty(db_row.get("monthly_payble"), db_row.get("monthly_payable")),
                "db_match_count": len(db_rows),
            }
        )

    return normalized, resolved_sheet


def run() -> int:
    parser = parse_common_args("Script 1 - Validation and eligibility checks")
    parser.add_argument("--sheet", default="EMI")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    cp = CheckpointStore(STAGE, paths)

    records, resolved_sheet = _load_records(paths, args.sheet)
    logger.info("using tracker sheet: %s", resolved_sheet)
    completed = cp.completed() if runtime.resume else set()

    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    manual: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for row in records:
        loan_id = row.get("loan_id")
        app_id = row.get("application_id")
        app_no = row.get("application_no") or row.get("Application No.")
        key = _clean_key(_first_non_empty(loan_id, app_id, app_no))

        if not key:
            failed.append(mandatory_failure_fields(loan_id, app_id, "missing identifiers", STAGE))
            continue
        if key in completed:
            skipped.append({"loan_id": loan_id, "application_id": app_id, "reason": "already_processed"})
            continue
        if key in seen_keys:
            failed.append(mandatory_failure_fields(loan_id, app_id, "duplicate loan/application", STAGE))
            continue
        seen_keys.add(key)

        if _is_blank(app_id):
            failed.append(mandatory_failure_fields(loan_id, app_id, "loan missing in CLOSED_LOANS_DETAILS", STAGE))
            continue
        if _is_blank(loan_id):
            failed.append(mandatory_failure_fields(loan_id, app_id, "loan missing in DB_SHEET", STAGE))
            continue

        tracker_tenure = _to_int(row.get("tenure"))
        details_tenure = _to_int(row.get("details_tenure"))
        if tracker_tenure is None or details_tenure is None:
            manual.append(mandatory_failure_fields(loan_id, app_id, "missing tenure for validation", STAGE))
            continue
        if tracker_tenure != details_tenure:
            failed.append(mandatory_failure_fields(loan_id, app_id, "tenure mismatch", STAGE))
            continue

        tracker_emi = _to_decimal(row.get("EMI"))
        details_emi = _to_decimal(row.get("details_emi"))
        if tracker_emi is None or details_emi is None:
            manual.append(mandatory_failure_fields(loan_id, app_id, "missing EMI for validation", STAGE))
            continue
        if tracker_emi != details_emi:
            failed.append(mandatory_failure_fields(loan_id, app_id, "EMI mismatch", STAGE))
            continue

        db_tenure = _to_int(row.get("db_emi_tenure"))
        if db_tenure not in (None, 0) and tracker_tenure != db_tenure:
            failed.append(mandatory_failure_fields(loan_id, app_id, "tenure mismatch in DB_SHEET", STAGE))
            continue

        db_emi = _to_decimal(row.get("db_monthly_payable"))
        if db_emi not in (None, Decimal("0")) and tracker_emi != db_emi:
            failed.append(mandatory_failure_fields(loan_id, app_id, "EMI mismatch in DB_SHEET", STAGE))
            continue

        installation_date = pd.to_datetime(row.get("Installation Date"), errors="coerce")
        emi1 = pd.to_datetime(row.get("EMI-1 date"), errors="coerce")
        if pd.isna(installation_date) or pd.isna(emi1):
            manual.append(mandatory_failure_fields(loan_id, app_id, "missing installation/EMI-1 date", STAGE))
            continue
        expected_emi1 = (installation_date + pd.DateOffset(months=1) + timedelta(days=1)).normalize()
        if emi1.normalize() != expected_emi1:
            failed.append(mandatory_failure_fields(loan_id, app_id, "invalid EMI-1 date", STAGE))
            continue

        row["backfill_stage"] = "validated"
        success.append(row)
        cp.mark_completed(key)

    out = write_audit_xlsx(
        paths.generated_sheets / "script_1",
        "script_1_validation",
        {"success": success, "failed": failed, "skipped": skipped, "manual_intervention": manual},
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
