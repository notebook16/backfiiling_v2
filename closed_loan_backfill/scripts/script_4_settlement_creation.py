#!/usr/bin/env python3
from __future__ import annotations

import math
import re
import shutil
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import numbers
import pandas as pd
from psycopg2.extras import RealDictCursor

from common import (
    CheckpointStore,
    DbClient,
    load_env_file,
    log_stage_summary,
    mandatory_failure_fields,
    parse_common_args,
    prompt_db_import,
    read_excel,
    runtime_from_args,
    setup_logger,
    to_records,
    ts_label,
    write_audit_xlsx,
)

STAGE = "script_4_settlement_creation"
DEFAULT_INPUT = "script_3_backfill_success_latest.xlsx"
COLLECTION_START_ID = 45000
TRANS_START_ID = 45000

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


def _to_decimal(value: Any) -> Decimal | None:
    if _is_blank(value):
        return None
    text = str(value).strip().replace(",", "")
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


def _normalize_identifier(value: Any) -> str:
    decimal_value = _to_decimal(value)
    if decimal_value is None:
        return _clean_key(value)
    if decimal_value == decimal_value.to_integral_value():
        return str(int(decimal_value))
    return str(decimal_value)


def _build_center_to_manager_map() -> dict[int, int]:
    center_to_manager: dict[int, int] = {}
    for manager_id, center_ids in CENTER_MANAGER_TO_CENTER_IDS.items():
        for center_id in center_ids:
            center_to_manager[center_id] = manager_id
    return center_to_manager


def _resolve_info_request_type(final_close_type: Any) -> str | None:
    token = _clean_key(final_close_type).upper()
    if token == "RECOVERED":
        return "Application_Close_Recover_Asset"
    if token == "FORECLOSE":
        return "Application_Close_Foreclose"
    if token in {"RETURN_CREDIT", "RETURN_DEBIT"}:
        return "Application_Close_Return_Asset"
    return None


def _settlement_amount_is_negative(value: Any) -> bool:
    if _is_blank(value):
        return False
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        decimal_value = _to_decimal(value)
        return decimal_value is not None and decimal_value < 0
    text = str(value).strip()
    if text.startswith("-"):
        return True
    return bool(re.search(r"-\s*[\d,]", text))


def _parse_settlement_amount(value: Any) -> Decimal:
    """Signed CASH / online amount for collections.due_amount: '-' in cell → negative; else positive; blank → 0."""
    if _is_blank(value):
        return Decimal("0")
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        decimal_value = _to_decimal(value)
        return decimal_value if decimal_value is not None else Decimal("0")

    text = str(value).strip()
    cleaned = re.sub(r"[^0-9\-,.]", "", text).replace(",", "")
    decimal_value = _to_decimal(cleaned)
    if decimal_value is None:
        return Decimal("0")
    magnitude = abs(decimal_value)
    if _settlement_amount_is_negative(value):
        return -magnitude
    return magnitude


def _settlement_cash_and_online_amounts(closed_row: dict[str, Any]) -> tuple[Decimal, Decimal]:
    cash_amount = _parse_settlement_amount(closed_row.get("CASH"))
    online_amount = Decimal("0")
    for key in ("UPI", "ONLINE", "Online", "online"):
        raw = closed_row.get(key)
        if not _is_blank(raw):
            online_amount = _parse_settlement_amount(raw)
            break
    return cash_amount, online_amount


def _settlement_payment_slices(closed_row: dict[str, Any]) -> list[tuple[str, Decimal]]:
    """Non-zero CASH / UPI from CLOSED_LOANS_DETAILS → collection_trans (amount always positive)."""
    slices: list[tuple[str, Decimal]] = []
    cash_amount, online_amount = _settlement_cash_and_online_amounts(closed_row)
    if cash_amount != 0:
        slices.append(("CASH", abs(cash_amount)))
    if online_amount != 0:
        slices.append(("UPI", abs(online_amount)))
    return slices


def _settlement_trans_transaction_cd(mode: str) -> str:
    if mode == "UPI":
        return "DUMMYBC2A"
    return ""


def _build_settlement_trans_record(
    *,
    trans_id: int,
    center_id: int,
    collection_id: int,
    mode: str,
    amount: Decimal,
    manager_id: int,
    customer_id: int,
    app_no: str,
    collection_created_at: datetime,
    trans_actual_created_at: datetime,
) -> dict[str, Any]:
    return {
        "trans_id": trans_id,
        "is_aggr_trans": False,
        "org_id": 1,
        "center_id": center_id,
        "collection_id": collection_id,
        "is_active": True,
        "transaction_cd": _settlement_trans_transaction_cd(mode),
        "trans_type": "CREDIT",
        "trans_subtype": "RECORD_PAYMENT",
        "mode": mode,
        "amount": amount,
        "recorded_by": manager_id,
        "scrap_type_id": 0,
        "recon_status": "ACCEPTED",
        "recon_by": manager_id,
        "recon_at": collection_created_at,
        "recon_comment": app_no,
        "is_settled": True,
        "from_user": 0,
        "from_customer": customer_id,
        "to_user": manager_id,
        "created_by": manager_id,
        "created_at": collection_created_at,
        "trans_comments": app_no,
        "parent_trans_id": None,
        "request_id": None,
        "deposit_trans_ids": None,
        "actual_created_at": trans_actual_created_at,
    }


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


def _nan_like_to_none(value: Any) -> Any:
    """Excel read often yields NaN for empty cells; PostgreSQL rejects NaN for timestamptz / integer NULLs."""
    if value is None:
        return None
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        try:
            if math.isnan(float(value)):
                return None
        except (TypeError, ValueError, OverflowError):
            pass
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _sanitize_db_import_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _nan_like_to_none(val) for key, val in row.items()}


def _resolve_input_file(paths, input_name: str) -> Path:
    candidate = Path(input_name)
    if candidate.is_absolute():
        resolved = candidate
    else:
        resolved = (paths.generated_sheets / "script_3" / input_name).resolve()
    if not resolved.exists():
        raise FileNotFoundError(
            "script_4 requires script_3 success output; "
            f"file not found: {resolved}"
        )
    return resolved


def _csv_blank_to_none(value: Any) -> Any:
    """With keep_default_na=False, empty cells become '' — coerce to None for SQL NULL (e.g. bigint columns)."""
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _normalize_script4_csv_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: _csv_blank_to_none(val) for key, val in row.items()} for row in records]


def _read_tabular_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        # keep_default_na=False: otherwise collection_subtype "NA" is parsed as NaN → NULL on insert.
        raw = to_records(pd.read_csv(path, keep_default_na=False))
        return _normalize_script4_csv_records(raw)
    return to_records(read_excel(path, 0))


def _script4_import_settlement_rows(
    cur: Any,
    logger: Any,
    *,
    success: list[dict[str, Any]],
    collections_sheet: Path,
    trans_sheet: Path,
    cp: CheckpointStore,
    paths: Any,
) -> None:
    collection_rows_for_import = _read_tabular_records(collections_sheet)
    trans_rows_for_import = _read_tabular_records(trans_sheet)
    loan_ids = [int(row["loan_id"]) for row in success]
    status_rows: dict[int, str] = {}
    if loan_ids:
        cur.execute(
            """
            SELECT application_id, close_status
            FROM loan_applications
            WHERE application_id = ANY(%s)
            """,
            (loan_ids,),
        )
        status_rows = {int(row["application_id"]): _clean_key(row["close_status"]) for row in cur.fetchall()}

    non_pending_loan_ids = [loan_id for loan_id in loan_ids if status_rows.get(loan_id, "").upper() != "PENDING"]
    if non_pending_loan_ids:
        raise RuntimeError(
            "loan_applications.close_status must be PENDING before script_4 import; "
            f"non_pending_loan_ids={sorted(set(non_pending_loan_ids))}"
        )

    for coll in collection_rows_for_import:
        cur.execute(
            """
            INSERT INTO collections (
                collection_id, center_id, org_id, loan_id, sale_id, emi_installment_no, is_active,
                invoice_doc_id, collection_type, collection_subtype, due_amount, fine_amount,
                follow_up_date, last_call, due_date, status, expire_reason, info, parent_id,
                collector_id, caller_id, created_by, created_at, loan_term_id, receipt_doc_id,
                is_last_collection, discount_amount, discount_reason
            ) VALUES (
                %(collection_id)s, %(center_id)s, %(org_id)s, %(loan_id)s, %(sale_id)s,
                %(emi_installment_no)s, %(is_active)s, %(invoice_doc_id)s, %(collection_type)s,
                %(collection_subtype)s, %(due_amount)s, %(fine_amount)s, %(follow_up_date)s,
                %(last_call)s, %(due_date)s, %(status)s, %(expire_reason)s, %(info)s::jsonb,
                %(parent_id)s, %(collector_id)s, %(caller_id)s, %(created_by)s, %(created_at)s,
                %(loan_term_id)s, %(receipt_doc_id)s, %(is_last_collection)s, %(discount_amount)s,
                %(discount_reason)s
            )
            """,
            {**_sanitize_db_import_row(coll), "info": None},
        )

    for trans in trans_rows_for_import:
        trans_row = {**_sanitize_db_import_row(trans), "actual_created_at": datetime.now(timezone.utc)}
        cur.execute(
            """
            INSERT INTO collection_trans (
                trans_id, is_aggr_trans, org_id, center_id, collection_id, is_active,
                transaction_cd, trans_type, trans_subtype, mode, amount, recorded_by,
                scrap_type_id, recon_status, recon_by, recon_at, recon_comment, is_settled,
                from_user, from_customer, to_user, created_by, created_at, trans_comments,
                parent_trans_id, request_id, actual_created_at
            ) VALUES (
                %(trans_id)s, %(is_aggr_trans)s, %(org_id)s, %(center_id)s, %(collection_id)s,
                %(is_active)s, %(transaction_cd)s, %(trans_type)s, %(trans_subtype)s, %(mode)s,
                %(amount)s, %(recorded_by)s, %(scrap_type_id)s, %(recon_status)s, %(recon_by)s,
                %(recon_at)s, %(recon_comment)s, %(is_settled)s, %(from_user)s, %(from_customer)s,
                %(to_user)s, %(created_by)s, %(created_at)s, %(trans_comments)s, %(parent_trans_id)s,
                %(request_id)s, %(actual_created_at)s
            )
            """,
            trans_row,
        )

    applied_loan_ids: list[int] = []
    for row in success:
        close_status = status_rows.get(int(row["loan_id"]), "")
        row["trigger_applied"] = close_status.upper() == "PENDING"
        row["close_status_after_done"] = close_status
        row["reason"] = "imported_close_status_pending"
        applied_loan_ids.append(int(row["loan_id"]))
        cp.mark_completed(str(row["loan_id"]))

    next_script_file = paths.generated_sheets / "script_4" / "script_4_next_script_loan_ids_latest.xlsx"
    pd.DataFrame([{"loan_id": loan_id} for loan_id in sorted(set(applied_loan_ids))]).to_excel(
        next_script_file, index=False
    )
    logger.info("loan ids for next script: %s", next_script_file)


def run() -> int:
    parser = parse_common_args("Script 4 - Settlement collection creation")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)
    cp = CheckpointStore(STAGE, paths)
    explicit_dry_run = bool(args.dry_run)

    input_file = _resolve_input_file(paths, args.input)
    tracker_rows = to_records(read_excel(input_file, 0))
    closed_rows = to_records(read_excel(paths.source_sheets / "CLOSED_LOANS_DETAILS.xlsx", 0))
    completed = cp.completed() if runtime.resume else set()

    closed_by_loan_id = {
        _normalize_identifier(row.get("loan_id")): row for row in closed_rows if _normalize_identifier(row.get("loan_id"))
    }
    closed_by_app_id = {
        _normalize_identifier(row.get("application_id")): row
        for row in closed_rows
        if _normalize_identifier(row.get("application_id"))
    }
    closed_by_app_no = {
        _clean_key(row.get("Application No.") or row.get("Application_No.")): row
        for row in closed_rows
        if _clean_key(row.get("Application No.") or row.get("Application_No."))
    }
    center_to_manager = _build_center_to_manager_map()

    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    staged_collections: list[dict[str, Any]] = []
    staged_trans: list[dict[str, Any]] = []
    output_dir = paths.generated_sheets / "script_4"
    run_id = ts_label()
    db_bundle_dir = paths.generated_db_sheets / "script_4" / run_id
    db_bundle_dir.mkdir(parents=True, exist_ok=True)
    collections_staging = db_bundle_dir / "settlement_collections.csv"
    trans_staging = db_bundle_dir / "settlement_collection_trans.csv"
    collections_sheet = output_dir / "script_4_generated_collections_latest.csv"
    trans_sheet = output_dir / "script_4_generated_collection_trans_latest.csv"
    applied_sheet = output_dir / "script_4_settlement_apply_status_latest.xlsx"

    next_collection_id = COLLECTION_START_ID
    next_trans_id = TRANS_START_ID

    db = DbClient(logger)
    try:
        with db.conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for tracker_row in tracker_rows:
                    loan_id = _to_int(tracker_row.get("loan_id"))
                    app_id = _normalize_identifier(tracker_row.get("application_id"))
                    key = str(loan_id) if loan_id is not None else ""

                    if not key:
                        failed.append(mandatory_failure_fields(None, app_id, "missing loan_id in script_3 output", STAGE))
                        continue

                    if key in completed:
                        skipped.append({"loan_id": loan_id, "application_id": app_id, "reason": "already_processed"})
                        continue

                    closed_row = closed_by_loan_id.get(key)
                    if not closed_row and app_id:
                        closed_row = closed_by_app_id.get(app_id)
                    if not closed_row:
                        app_no = _clean_key(tracker_row.get("Application No.") or tracker_row.get("application_no"))
                        if app_no:
                            closed_row = closed_by_app_no.get(app_no)
                    if not closed_row:
                        failed.append(mandatory_failure_fields(loan_id, app_id, "loan missing in CLOSED_LOANS_DETAILS", STAGE))
                        continue

                    loan_term_id = _to_int(closed_row.get("LOAN_TERM_ID") or closed_row.get("loan_term_id"))
                    if loan_term_id is None:
                        failed.append(mandatory_failure_fields(loan_id, app_id, "loan_term_id missing in CLOSED_LOANS_DETAILS", STAGE))
                        continue

                    cash_amount, upi_amount = _settlement_cash_and_online_amounts(closed_row)
                    settlement_amount = cash_amount + upi_amount  # signed sum → collections.due_amount
                    payment_slices = _settlement_payment_slices(closed_row)  # trans amounts are positive only

                    request_type = _resolve_info_request_type(
                        closed_row.get("FINAL_CLOSE_TYPE") or closed_row.get("FINAL CLOSE TYPE")
                    )
                    if request_type is None:
                        failed.append(
                            mandatory_failure_fields(
                                loan_id,
                                app_id,
                                "FINAL_CLOSE_TYPE must be RECOVERED/FORECLOSE/RETURN_CREDIT/RETURN_DEBIT",
                                STAGE,
                            )
                        )
                        continue

                    customer_id = _to_int(tracker_row.get("customer_id"))
                    if customer_id is None:
                        failed.append(mandatory_failure_fields(loan_id, app_id, "customer_id missing in script_3 output", STAGE))
                        continue

                    cur.execute(
                        """
                        SELECT center_id
                        FROM collections
                        WHERE loan_id=%s AND is_active=true
                        ORDER BY collection_id DESC
                        LIMIT 1
                        """,
                        (loan_id,),
                    )
                    center_row = cur.fetchone()
                    if not center_row:
                        failed.append(mandatory_failure_fields(loan_id, app_id, "center_id resolution failed", STAGE))
                        continue
                    center_id = _to_int(center_row.get("center_id"))
                    if center_id is None:
                        failed.append(mandatory_failure_fields(loan_id, app_id, "invalid center_id resolved", STAGE))
                        continue
                    manager_id = center_to_manager.get(center_id)
                    if manager_id is None:
                        failed.append(
                            mandatory_failure_fields(
                                loan_id,
                                app_id,
                                f"collector/created_by mapping missing for center_id={center_id}",
                                STAGE,
                            )
                        )
                        continue

                    app_no = _clean_key(closed_row.get("Application_No.") or closed_row.get("Application No."))
                    closed_date_raw = pd.to_datetime(
                        closed_row.get("CLOSED_DATE") or closed_row.get("CLOSED DATE"),
                        errors="coerce",
                    )
                    if pd.isna(closed_date_raw):
                        failed.append(
                            mandatory_failure_fields(
                                loan_id,
                                app_id,
                                "CLOSED DATE missing or invalid in CLOSED_LOANS_DETAILS",
                                STAGE,
                            )
                        )
                        continue
                    collection_created_at = closed_date_raw.to_pydatetime()
                    if collection_created_at.tzinfo is None:
                        collection_created_at = collection_created_at.replace(tzinfo=timezone.utc)

                    coll_record = {
                        "collection_id": next_collection_id,
                        "center_id": center_id,
                        "org_id": 1,
                        "loan_id": loan_id,
                        "sale_id": 0,
                        "emi_installment_no": -1,
                        "is_active": True,
                        "invoice_doc_id": None,
                        "collection_type": "SETTLEMENT",
                        "collection_subtype": "NA",
                        "due_amount": settlement_amount,
                        "fine_amount": Decimal("0"),
                        "follow_up_date": None,
                        "last_call": None,
                        "due_date": datetime(5000, 1, 1, 5, 30, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))),
                        "status": "DONE",
                        "expire_reason": app_no,
                        "info": None,
                        "parent_id": None,
                        "collector_id": manager_id,
                        "caller_id": None,
                        "created_by": manager_id,
                        "created_at": collection_created_at,
                        "loan_term_id": loan_term_id,
                        "receipt_doc_id": None,
                        "is_last_collection": False,
                        "discount_amount": None,
                        "discount_reason": None,
                    }
                    staged_collections.append(coll_record)

                    trans_actual_created_at = datetime.now(timezone.utc)
                    for mode, slice_amount in payment_slices:
                        staged_trans.append(
                            _build_settlement_trans_record(
                                trans_id=next_trans_id,
                                center_id=center_id,
                                collection_id=next_collection_id,
                                mode=mode,
                                amount=slice_amount,
                                manager_id=manager_id,
                                customer_id=customer_id,
                                app_no=app_no,
                                collection_created_at=collection_created_at,
                                trans_actual_created_at=trans_actual_created_at,
                            )
                        )
                        next_trans_id += 1

                    success.append(
                        {
                            "loan_id": loan_id,
                            "application_id": app_id,
                            "collection_id": next_collection_id,
                            "settlement_cash": str(cash_amount),
                            "settlement_upi": str(upi_amount),
                            "settlement_amount": str(settlement_amount),
                            "collection_status_initial": "DONE",
                            "trigger_applied": False,
                            "close_status_after_done": "",
                            "collection_trans_created": len(payment_slices) > 0,
                            "collection_trans_count": len(payment_slices),
                            "reason": "staged",
                        }
                    )
                    next_collection_id += 1

    except Exception as exc:  # noqa: BLE001
        logger.exception("script_4 failed: %s", exc)
        if not failed:
            failed.append(mandatory_failure_fields(None, None, str(exc), STAGE))
    finally:
        db.close()

    if staged_collections:
        pd.DataFrame(_excel_safe_records(staged_collections)).to_csv(collections_staging, index=False)
        pd.DataFrame(_excel_safe_records(staged_trans)).to_csv(trans_staging, index=False)
        shutil.copy2(collections_staging, collections_sheet)
        shutil.copy2(trans_staging, trans_sheet)
        logger.info("script_4 DB staging bundle: %s", db_bundle_dir)
        logger.info("settlement collections (staging): %s", collections_staging)
        logger.info("settlement collection_trans (staging): %s", trans_staging)
        logger.info("copied latest sheets to: %s and %s", collections_sheet, trans_sheet)

    import_performed = False
    if (
        not explicit_dry_run
        and staged_collections
        and prompt_db_import(
            logger,
            stage_label="script_4_settlement",
            files=[
                ("settlement_collections", collections_staging),
                ("settlement_collection_trans", trans_staging),
            ],
        )
    ):
        db_import = DbClient(logger)
        try:
            with db_import.conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    _script4_import_settlement_rows(
                        cur,
                        logger,
                        success=success,
                        collections_sheet=collections_sheet,
                        trans_sheet=trans_sheet,
                        cp=cp,
                        paths=paths,
                    )
            import_performed = True
        finally:
            db_import.close()

    if not import_performed:
        for row in success:
            if row.get("reason") == "staged":
                row["reason"] = "staged_not_imported"

    pd.DataFrame(_excel_safe_records(success)).to_excel(applied_sheet, index=False)
    logger.info("settlement apply status sheet: %s", applied_sheet)

    out = write_audit_xlsx(
        output_dir,
        "script_4_settlement",
        {"success": success, "failed": failed, "skipped": skipped},
    )
    log_stage_summary(
        logger,
        "script_4_settlement_creation",
        loaded=len(tracker_rows),
        buckets={
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "collections_generated": staged_collections,
            "collection_trans_generated": staged_trans,
        },
        reason_fields_by_bucket={
            "failed": ("failure_reason",),
            "skipped": ("reason",),
            "success": ("reason",),
        },
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
