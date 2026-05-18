#!/usr/bin/env python3
"""Post-pipeline validation: DB EMI collection totals vs CLOSED_LOAN_TRACKER EMIs Total."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

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

STAGE = "script_VAL1_post_emi_total_validation"
DEFAULT_INPUT_GLOB = "script_5_final_status_success_*.xlsx"
TRACKER_PATH = "CLOSED_LOAN_TRACKER.xlsx"
TRACKER_EMI_SHEET = "EMI"
LOS_DATA_NAME = "LOS _Data.xlsx"
EXPECTED_PIPELINE_SUCCESS_COUNT = 49

DB_EMI_TOTALS_SQL = """
SELECT
    loan_id,
    SUM(due_amount + fine_amount) AS db_emi_total
FROM collections
WHERE loan_id = ANY(%s)
  AND (
      (status = 'DONE' AND collection_type = 'EMI')
      OR (
          emi_installment_no = -1
          AND status IN ('DONE_CLOSE', 'DONE_RECOVER', 'DONE_RETURN')
      )
  )
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
    dec = _to_decimal(value)
    if dec is None:
        return None
    try:
        return int(dec)
    except Exception:  # noqa: BLE001
        return None


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


def _resolve_script5_success_path(paths: Any, input_arg: str | None) -> Path:
    script5_dir = paths.generated_sheets / "script_5"
    if input_arg:
        candidate = Path(input_arg)
        if not candidate.is_absolute():
            candidate = script5_dir / candidate
        if not candidate.exists():
            raise FileNotFoundError(f"script_VAL1 input not found: {candidate}")
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
            f"script_VAL1 requires script_5 success output under {script5_dir}. Run script_5 first."
        )
    return matches[0].resolve()


def _fetch_db_emi_totals(loan_ids: list[int], logger) -> dict[int, Decimal]:
    if not loan_ids:
        return {}

    db = DbClient(logger)
    totals: dict[int, Decimal] = {}
    try:
        with db.conn() as conn:
            with conn.cursor() as cur:
                cur.execute(DB_EMI_TOTALS_SQL, (loan_ids,))
                for loan_id, db_total in cur.fetchall():
                    totals[int(loan_id)] = _to_decimal(db_total) or Decimal("0")
    finally:
        db.close()
    return totals


def _comparison_row(
    *,
    loan_id: int | None,
    application_id: str,
    application_no: str,
    tracker_emis_total: Decimal | None,
    db_emi_total: Decimal | None,
    status: str,
    detail: str = "",
) -> dict[str, Any]:
    difference: str | None = None
    if tracker_emis_total is not None and db_emi_total is not None:
        difference = str(db_emi_total - tracker_emis_total)
    return {
        "loan_id": loan_id,
        "application_id": application_id,
        "application_no": application_no,
        "tracker_emis_total": None if tracker_emis_total is None else str(tracker_emis_total),
        "db_emi_total": None if db_emi_total is None else str(db_emi_total),
        "difference": difference,
        "validation_status": status,
        "detail": detail,
    }


def run() -> int:
    parser = parse_common_args("Script VAL1 - Post-pipeline EMI total vs tracker validation")
    parser.add_argument(
        "--input",
        default=None,
        help=(
            "Script 5 success workbook (default: latest script_5_final_status_success_*.xlsx "
            "or script_5_final_status_success_latest.xlsx)"
        ),
    )
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
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
    los_by_application_id = _index_by(los_rows, "application_id", normalizer=_normalize_identifier)
    los_by_app = _index_by(los_rows, "Application Number", normalizer=_normalize_app_key)
    tracker_by_app = _index_by(tracker_rows, "Application No.", normalizer=_normalize_app_key)

    loan_ids: list[int] = []
    prepared: list[dict[str, Any]] = []
    mapping_failed: list[dict[str, Any]] = []

    for row in pipeline_rows:
        loan_id = _to_int(row.get("loan_id"))
        application_id = _normalize_identifier(row.get("application_id"))
        if loan_id is None:
            mapping_failed.append(
                _comparison_row(
                    loan_id=None,
                    application_id=application_id,
                    application_no="",
                    tracker_emis_total=None,
                    db_emi_total=None,
                    status="mapping_failed",
                    detail="missing loan_id on script_5 success row",
                )
            )
            continue

        los_row = los_by_application_id.get(application_id)
        if los_row is None and application_id:
            los_row = los_by_app.get(_normalize_app_key(application_id))

        if los_row is None:
            mapping_failed.append(
                _comparison_row(
                    loan_id=loan_id,
                    application_id=application_id,
                    application_no="",
                    tracker_emis_total=None,
                    db_emi_total=None,
                    status="mapping_failed",
                    detail="application_id not found in LOS _Data.xlsx",
                )
            )
            continue

        application_no = _clean_key(los_row.get("Application Number"))
        app_key = _normalize_app_key(application_no)
        tracker_row = tracker_by_app.get(app_key)
        if tracker_row is None:
            mapping_failed.append(
                _comparison_row(
                    loan_id=loan_id,
                    application_id=application_id,
                    application_no=application_no,
                    tracker_emis_total=None,
                    db_emi_total=None,
                    status="mapping_failed",
                    detail="application_no not found in CLOSED_LOAN_TRACKER EMI sheet",
                )
            )
            continue

        tracker_emis_total = _to_decimal(tracker_row.get("EMIs Total"))
        if tracker_emis_total is None:
            mapping_failed.append(
                _comparison_row(
                    loan_id=loan_id,
                    application_id=application_id,
                    application_no=application_no,
                    tracker_emis_total=None,
                    db_emi_total=None,
                    status="mapping_failed",
                    detail="EMIs Total missing or invalid in CLOSED_LOAN_TRACKER",
                )
            )
            continue

        loan_ids.append(loan_id)
        prepared.append(
            {
                "loan_id": loan_id,
                "application_id": application_id,
                "application_no": application_no,
                "tracker_emis_total": tracker_emis_total,
            }
        )

    db_totals = _fetch_db_emi_totals(sorted(set(loan_ids)), logger)

    matched: list[dict[str, Any]] = []
    mismatched: list[dict[str, Any]] = []

    for item in prepared:
        loan_id = item["loan_id"]
        tracker_total: Decimal = item["tracker_emis_total"]
        db_total = db_totals.get(loan_id)
        if db_total is None:
            mismatched.append(
                _comparison_row(
                    loan_id=loan_id,
                    application_id=item["application_id"],
                    application_no=item["application_no"],
                    tracker_emis_total=tracker_total,
                    db_emi_total=None,
                    status="mismatch",
                    detail="no qualifying EMI collections in database for loan_id",
                )
            )
            continue

        if db_total == tracker_total:
            matched.append(
                _comparison_row(
                    loan_id=loan_id,
                    application_id=item["application_id"],
                    application_no=item["application_no"],
                    tracker_emis_total=tracker_total,
                    db_emi_total=db_total,
                    status="matched",
                )
            )
        else:
            mismatched.append(
                _comparison_row(
                    loan_id=loan_id,
                    application_id=item["application_id"],
                    application_no=item["application_no"],
                    tracker_emis_total=tracker_total,
                    db_emi_total=db_total,
                    status="mismatch",
                    detail="db_emi_total != tracker EMIs Total",
                )
            )

    output_dir = paths.generated_sheets / "script_VAL1"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_ts = ts_label()

    mismatch_path = output_dir / f"script_VAL1_emi_total_mismatch_{run_ts}.xlsx"
    pd.DataFrame(mismatched + mapping_failed).to_excel(mismatch_path, index=False)

    latest_mismatch = output_dir / "script_VAL1_emi_total_mismatch_latest.xlsx"
    pd.DataFrame(mismatched + mapping_failed).to_excel(latest_mismatch, index=False)

    audit = write_audit_xlsx(
        output_dir,
        "script_VAL1_emi_total_validation",
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
    logger.info("mismatch report (mismatches + mapping failures): %s", mismatch_path)
    logger.info("latest mismatch report: %s", latest_mismatch)
    logger.info("audit files: %s", audit)

    hard_failures = mismatched + mapping_failed
    return 0 if not hard_failures else 2


if __name__ == "__main__":
    raise SystemExit(run())
