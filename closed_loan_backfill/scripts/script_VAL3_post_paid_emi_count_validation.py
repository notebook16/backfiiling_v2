#!/usr/bin/env python3
"""Post-pipeline validation: paid EMI count in DB (DONE) vs tracker Paid On rows (excl. settlement)."""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from common import (
    DbClient,
    load_env_file,
    log_stage_summary,
    parse_common_args,
    read_excel,
    runtime_from_args,
    setup_logger,
    to_records,
    ts_label,
    write_audit_xlsx,
)

STAGE = "script_VAL3_post_paid_emi_count_validation"
DEFAULT_INPUT_GLOB = "script_5_final_status_success_*.xlsx"
TRACKER_PATH = "CLOSED_LOAN_TRACKER.xlsx"
TRACKER_EMI_SHEET = "EMI"
LOS_DATA_NAME = "LOS _Data.xlsx"
EXPECTED_PIPELINE_SUCCESS_COUNT = 49

DB_PAID_EMI_COUNT_SQL = """
SELECT
    loan_id,
    COUNT(*)::int AS paid_emi_count
FROM collections
WHERE loan_id = ANY(%s)
  AND is_active = true
  AND collection_type = 'EMI'
  AND status = 'DONE'
  AND emi_installment_no > 0
GROUP BY loan_id
"""


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


def _normalize_app_key(value: Any) -> str:
    text = _clean_key(value).upper()
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


def _to_int(value: Any) -> int | None:
    if _is_blank(value):
        return None
    text = str(value).strip().replace(",", "")
    try:
        return int(float(text))
    except Exception:  # noqa: BLE001
        return None


def _tracker_emi_value(row: dict[str, Any], base_column: str, emi_no: int) -> Any:
    suffix = "" if emi_no <= 1 else f".{emi_no - 1}"
    return row.get(f"{base_column}{suffix}")


def _is_valid_paid_on(value: Any) -> bool:
    """Same rules as script_3: a paid EMI has a real Paid On date/value in the tracker."""
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


def _count_tracker_paid_emis(tracker_row: dict[str, Any]) -> int | None:
    tenure = _to_int(tracker_row.get("Tenure"))
    if tenure is None or tenure <= 0:
        return None
    paid = 0
    for emi_no in range(1, tenure + 1):
        if _is_valid_paid_on(_tracker_emi_value(tracker_row, "Paid On", emi_no)):
            paid += 1
    return paid


def _index_by_app_no(tracker_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in tracker_rows:
        key = _normalize_app_key(row.get("Application No."))
        if key and key not in out:
            out[key] = row
    return out


def _index_los_by_application_id(los_rows: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in los_rows:
        app_id = _normalize_identifier(row.get("application_id"))
        app_no = _clean_key(row.get("Application Number"))
        if app_id and app_no and app_id not in out:
            out[app_id] = app_no
    return out


def _resolve_script5_success_path(paths: Any, input_arg: str | None) -> Path:
    script5_dir = paths.generated_sheets / "script_5"
    if input_arg:
        candidate = Path(input_arg)
        if not candidate.is_absolute():
            candidate = script5_dir / candidate
        if not candidate.exists():
            raise FileNotFoundError(f"script_VAL3 input not found: {candidate}")
        return candidate.resolve()

    latest = script5_dir / "script_5_final_status_success_latest.xlsx"
    if latest.exists():
        return latest.resolve()

    matches = sorted(
        script5_dir.glob(DEFAULT_INPUT_GLOB),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not matches:
        raise FileNotFoundError(
            f"script_VAL3 requires script_5 success output under {script5_dir}. Run script_5 first."
        )
    return matches[0].resolve()


def _fetch_db_paid_emi_counts(loan_ids: list[int], logger) -> dict[int, int]:
    if not loan_ids:
        return {}

    db = DbClient(logger)
    counts: dict[int, int] = {}
    try:
        with db.conn() as conn:
            with conn.cursor() as cur:
                cur.execute(DB_PAID_EMI_COUNT_SQL, (loan_ids,))
                for loan_id, paid_emi_count in cur.fetchall():
                    counts[int(loan_id)] = int(paid_emi_count)
    finally:
        db.close()
    return counts


def run() -> int:
    parser = parse_common_args(
        "Script VAL3 - paid EMI count (DB DONE vs tracker), excluding settlement"
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Script 5 success workbook (default: latest script_5_final_status_success_*.xlsx)",
    )
    args = parser.parse_args()
    _runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)

    input_path = _resolve_script5_success_path(paths, args.input)
    pipeline_rows = to_records(read_excel(input_path, 0))
    logger.info("loaded %s pipeline-success rows from %s", len(pipeline_rows), input_path)

    if len(pipeline_rows) != EXPECTED_PIPELINE_SUCCESS_COUNT:
        logger.warning(
            "expected %s script_5 success rows, found %s",
            EXPECTED_PIPELINE_SUCCESS_COUNT,
            len(pipeline_rows),
        )

    los_rows = to_records(read_excel(paths.source_sheets / LOS_DATA_NAME, 0))
    tracker_rows = to_records(read_excel(paths.source_sheets / TRACKER_PATH, TRACKER_EMI_SHEET))
    los_app_by_id = _index_los_by_application_id(los_rows)
    tracker_by_app = _index_by_app_no(tracker_rows)

    loan_ids = sorted(
        {
            loan_id
            for row in pipeline_rows
            if (loan_id := _to_int(row.get("loan_id"))) is not None
        }
    )
    db_counts = _fetch_db_paid_emi_counts(loan_ids, logger)

    report_rows: list[dict[str, Any]] = []
    matched: list[dict[str, Any]] = []
    mismatched: list[dict[str, Any]] = []
    mapping_failed: list[dict[str, Any]] = []

    for row in pipeline_rows:
        loan_id = _to_int(row.get("loan_id"))
        application_id = _normalize_identifier(row.get("application_id"))
        los_app_no = los_app_by_id.get(application_id, "")
        tracker_row = tracker_by_app.get(_normalize_app_key(los_app_no)) if los_app_no else None

        if loan_id is None:
            fail = {
                "application_no": los_app_no,
                "loan_id": None,
                "application_id": application_id,
                "db_paid_emi_count": None,
                "tracker_paid_emi_count": None,
                "count_difference": None,
                "counts_match": False,
                "detail": "missing loan_id on script_5 success row",
            }
            mapping_failed.append(fail)
            report_rows.append(fail)
            continue

        if not los_app_no:
            fail = {
                "application_no": "",
                "loan_id": loan_id,
                "application_id": application_id,
                "db_paid_emi_count": db_counts.get(loan_id, 0),
                "tracker_paid_emi_count": None,
                "count_difference": None,
                "counts_match": False,
                "detail": "application_id not found in LOS _Data.xlsx",
            }
            mapping_failed.append(fail)
            report_rows.append(fail)
            continue

        if tracker_row is None:
            fail = {
                "application_no": los_app_no,
                "loan_id": loan_id,
                "application_id": application_id,
                "db_paid_emi_count": db_counts.get(loan_id, 0),
                "tracker_paid_emi_count": None,
                "count_difference": None,
                "counts_match": False,
                "detail": "application_no not found in CLOSED_LOAN_TRACKER EMI sheet",
            }
            mapping_failed.append(fail)
            report_rows.append(fail)
            continue

        db_count = db_counts.get(loan_id, 0)
        tracker_count = _count_tracker_paid_emis(tracker_row)
        if tracker_count is None:
            fail = {
                "application_no": los_app_no,
                "loan_id": loan_id,
                "application_id": application_id,
                "db_paid_emi_count": db_count,
                "tracker_paid_emi_count": None,
                "count_difference": None,
                "counts_match": False,
                "detail": "Tenure missing or invalid in CLOSED_LOAN_TRACKER",
            }
            mapping_failed.append(fail)
            report_rows.append(fail)
            continue

        is_match = db_count == tracker_count
        out = {
            "application_no": los_app_no,
            "loan_id": loan_id,
            "application_id": application_id,
            "db_paid_emi_count": db_count,
            "tracker_paid_emi_count": tracker_count,
            "count_difference": db_count - tracker_count,
            "counts_match": is_match,
            "detail": "" if is_match else "db DONE EMI count != tracker paid EMI count",
        }
        report_rows.append(out)
        if is_match:
            matched.append(out)
        else:
            mismatched.append(out)

    output_dir = paths.generated_sheets / "script_VAL3"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_ts = ts_label()

    report_path = output_dir / f"script_VAL3_paid_emi_count_validation_{run_ts}.xlsx"
    latest_path = output_dir / "script_VAL3_paid_emi_count_validation_latest.xlsx"
    pd.DataFrame(report_rows).to_excel(report_path, index=False)
    pd.DataFrame(report_rows).to_excel(latest_path, index=False)

    audit = write_audit_xlsx(
        output_dir,
        "script_VAL3_paid_emi_count_validation",
        {
            "matched": matched,
            "mismatch": mismatched,
            "mapping_failed": mapping_failed,
        },
    )

    log_stage_summary(
        logger,
        STAGE,
        loaded=len(pipeline_rows),
        buckets={
            "matched": matched,
            "mismatch": mismatched,
            "mapping_failed": mapping_failed,
        },
        reason_fields_by_bucket={
            "mismatch": ("detail",),
            "mapping_failed": ("detail",),
        },
    )
    logger.info(
        "DB rule: collection_type='EMI', status='DONE', emi_installment_no>0 (settlement excluded)"
    )
    logger.info("Tracker rule: count EMI slots 1..Tenure with valid Paid On (same as script_3)")
    logger.info("validation report: %s", report_path)
    logger.info("latest validation report: %s", latest_path)
    logger.info("audit files: %s", audit)

    hard_failures = mismatched + mapping_failed
    return 0 if not hard_failures else 2


if __name__ == "__main__":
    raise SystemExit(run())
