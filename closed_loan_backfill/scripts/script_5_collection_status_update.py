#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

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

STAGE = "script_5_collection_status_update"
DEFAULT_INPUT = "script_4_settlement_apply_status_latest.xlsx"
CLOSED_LOANS_DETAILS = "CLOSED_LOANS_DETAILS.xlsx"
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


def _clean_key(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _normalize_token(value: Any) -> str:
    return _clean_key(value).upper()


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


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if _is_blank(value):
            continue
        return value
    return None


def _loan_application_int(row: dict[str, Any]) -> int | None:
    return _to_int(row.get("loan_id") or row.get("LOAN_ID") or row.get("application_id"))


def _resolve_script5_input_path(paths: Any, input_name: str) -> Path:
    candidate = Path(input_name)
    if candidate.is_absolute():
        return candidate.resolve()
    if candidate.name == DEFAULT_INPUT:
        return (paths.generated_sheets / "script_4" / DEFAULT_INPUT).resolve()
    return (paths.source_sheets / input_name).resolve()


def _script4_closed_merged_row(script4_row: dict[str, Any], closed: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = dict(closed)
    li = _to_int(script4_row.get("loan_id"))
    ai = _normalize_identifier(script4_row.get("application_id"))
    out["loan_id"] = li if li is not None else _to_int(closed.get("application_id"))
    out["application_id"] = ai or _normalize_identifier(closed.get("application_id"))
    out["CLOSED_DATE"] = _first_non_empty(closed.get("CLOSED DATE"), closed.get("CLOSED_DATE"))
    out["CLOSE_TYPE"] = closed.get("close_type") or closed.get("CLOSE_TYPE")
    final_ct = _first_non_empty(closed.get("FINAL CLOSE TYPE"), closed.get("FINAL_CLOSE_TYPE"))
    out["FINAL CLOSE TYPE"] = final_ct
    out["FINAL_CLOSE_TYPE"] = final_ct
    out["SETTLEMENT_AMOUNT"] = closed.get("SETTLEMENT_AMOUNT")
    return out


def _merge_script4_with_closed(
    script4_rows: list[dict[str, Any]],
    closed_rows: list[dict[str, Any]],
    logger: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    closed_by_app: dict[str, dict[str, Any]] = {}
    for cr in closed_rows:
        aid = _normalize_identifier(cr.get("application_id"))
        if aid:
            closed_by_app[aid] = cr

    work: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for r in script4_rows:
        key_int = _loan_application_int(r)
        aid = _normalize_identifier(key_int) if key_int is not None else ""
        closed = closed_by_app.get(aid) if aid else None
        if not closed:
            failures.append(
                mandatory_failure_fields(
                    key_int,
                    _normalize_identifier(r.get("application_id")),
                    "no CLOSED_LOANS_DETAILS row for this script_4 loan/application id",
                    STAGE,
                )
            )
            continue
        work.append(_script4_closed_merged_row(r, closed))
    logger.info(
        "script_5 merged script_4 output with closed-loan sheet: script_4_rows=%s merged_ok=%s merge_failed=%s",
        len(script4_rows),
        len(work),
        len(failures),
    )
    return work, failures


def _coerce_full_closed_sheet_row(row: dict[str, Any]) -> dict[str, Any]:
    """CLOSED_LOANS_DETAILS uses application_id; script_5 DB logic expects loan_id key."""
    out = dict(row)
    if _loan_application_int(out) is None and not _is_blank(out.get("application_id")):
        out["loan_id"] = out.get("application_id")
    out["CLOSED_DATE"] = _first_non_empty(out.get("CLOSED_DATE"), out.get("CLOSED DATE"))
    out["CLOSE_TYPE"] = out.get("CLOSE_TYPE") or out.get("close_type")
    final_ct = _first_non_empty(out.get("FINAL_CLOSE_TYPE"), out.get("FINAL CLOSE TYPE"))
    out["FINAL_CLOSE_TYPE"] = final_ct
    out["FINAL CLOSE TYPE"] = final_ct
    return out


def _month_tuple(dt: datetime) -> tuple[int, int]:
    return (dt.year, dt.month)


RETURN_FINAL_CLOSE_TYPES = frozenset({"RETURN_CREDIT", "RETURN_DEBIT"})


def _final_close_type_token(row: dict[str, Any]) -> str:
    return _normalize_token(row.get("FINAL_CLOSE_TYPE") or row.get("FINAL CLOSE TYPE"))


def _is_return_final_close_type(final_close_type: str) -> bool:
    return final_close_type in RETURN_FINAL_CLOSE_TYPES


def _resolve_behavior_from_final_close_type(final_close_type_raw: Any) -> str:
    """EMI matrix key from FINAL_CLOSE_TYPE. RETURN_CREDIT and RETURN_DEBIT both map to RETURN."""
    token = _normalize_token(final_close_type_raw)
    if not token:
        return "RECOVERY"
    if token in {"RECOVER", "RECOVERY", "RECOVERED"}:
        return "RECOVERY"
    if _is_return_final_close_type(token):
        return "RETURN"
    if token in {"FORECLOSE", "FORECLOSURE", "CLOSED", "CLOSE"}:
        return "BULK_CLOSE"
    if token in {"LAST EMI PAID", "LAST EMI", "EMI PAID"}:
        return "LAST_EMI_PAID"
    return "RECOVERY"


def _expected_settlement_due_amount(row: dict[str, Any]) -> Decimal:
    """Signed CASH + online total — must match script_4 collections.due_amount (not SETTLEMENT_AMOUNT column)."""
    from script_4_settlement_creation import _settlement_cash_and_online_amounts

    cash, online = _settlement_cash_and_online_amounts(row)
    return cash + online


def _past_month_status_for_return_final_close_type(final_close_type: str) -> str:
    if final_close_type == "RETURN_CREDIT":
        return "DEFAULT_CREDIT"
    if final_close_type == "RETURN_DEBIT":
        return "DEFAULT_DEBIT"
    raise ValueError(
        f"past-month return status requires RETURN_CREDIT or RETURN_DEBIT; got {final_close_type!r}"
    )


def _resolve_close_by_user_id(app_row: dict[str, Any]) -> int:
    """Prefer BACKFILL_CLOSE_BY env (user id); else loan_applications.created_by."""
    raw = os.environ.get("BACKFILL_CLOSE_BY", "").strip()
    if raw:
        parsed = _to_int(raw)
        if parsed is None:
            raise ValueError(f"BACKFILL_CLOSE_BY must be an integer user id, got {raw!r}")
        return parsed
    created_by = _to_int(app_row.get("created_by"))
    if created_by is None:
        raise ValueError("loan_applications.created_by is null; set BACKFILL_CLOSE_BY or fix data")
    return created_by


def _map_loan_close_status_for_db(canonical: str) -> str:
    """loan_applications.close_status uses RECOVER/CLOSE/RETURN (not sheet labels RECOVERED/FORECLOSE/CLOSED)."""
    t = _normalize_token(canonical)
    if t in {"RECOVERED", "RECOVER"}:
        return "RECOVER"
    if t in {"FORECLOSE", "FORECLOSURE", "CLOSED", "CLOSE"}:
        return "CLOSE"
    if t in RETURN_FINAL_CLOSE_TYPES:
        return "RETURN"
    if t == "PENDING":
        return "PENDING"
    return t


def _resolve_loan_close_status(row: dict[str, Any], behavior: str) -> str:
    final_close_type = _final_close_type_token(row)
    if final_close_type in {"CLOSED", "RECOVERED", "FORECLOSE", "FORECLOSURE"}:
        return _map_loan_close_status_for_db(final_close_type)
    if _is_return_final_close_type(final_close_type):
        return _map_loan_close_status_for_db("RETURN")
    if behavior == "RECOVERY":
        return _map_loan_close_status_for_db("RECOVERED")
    if behavior == "RETURN":
        return _map_loan_close_status_for_db("RETURN")
    if behavior == "BULK_CLOSE":
        return _map_loan_close_status_for_db("FORECLOSE")
    if behavior == "LAST_EMI_PAID":
        return _map_loan_close_status_for_db("CLOSED")
    return _map_loan_close_status_for_db("CLOSED")


def _settlement_collection_status_from_final_close_type(row: dict[str, Any]) -> str:
    """Settlement row (emi_installment_no=-1): status from CLOSED_LOANS_DETAILS FINAL_CLOSE_TYPE."""
    final_close_type = _final_close_type_token(row)
    if _is_return_final_close_type(final_close_type):
        return "DONE_RETURN"
    mapping = {
        "RECOVERED": "DONE_RECOVER",
        "FORECLOSE": "DONE_CLOSE",
        "FORECLOSURE": "DONE_CLOSE",
        "CLOSED": "DONE_CLOSE",
    }
    if final_close_type in mapping:
        return mapping[final_close_type]
    raise ValueError(
        "FINAL_CLOSE_TYPE must be RECOVERED/RETURN_CREDIT/RETURN_DEBIT/FORECLOSE/CLOSED "
        f"for settlement collection; got {final_close_type!r}"
    )


def _status_for_collection(category: str, behavior: str) -> str | None:
    matrix: dict[str, dict[str, str | None]] = {
        "PAST_MONTH": {
            "RECOVERY": "DEFAULT",
            "BULK_CLOSE": "PAID_BULK",
            "LAST_EMI_PAID": "DONE",
        },
        "CLOSURE_MONTH": {
            "RECOVERY": "RECOVER",
            "RETURN": "RETURN",
            "BULK_CLOSE": "CLOSE",
            "LAST_EMI_PAID": "DONE",
        },
        "FUTURE_MONTH": {
            "RECOVERY": "RECOVER",
            "RETURN": "RETURN",
            "BULK_CLOSE": "CLOSE",
            "LAST_EMI_PAID": None,
        },
    }
    return matrix.get(category, {}).get(behavior)


def run() -> int:
    parser = parse_common_args("Script 5 - Final collection status update")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=(
            "Default: script_4 settlement apply sheet under generated_sheets/script_4. "
            "Pass CLOSED_LOANS_DETAILS.xlsx to process the full source sheet."
        ),
    )
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)
    cp = CheckpointStore(STAGE, paths)
    execute_mode = runtime.execute and not runtime.dry_run

    input_path = _resolve_script5_input_path(paths, args.input)
    if not input_path.exists():
        raise FileNotFoundError(
            f"script_5 input not found: {input_path}. Run script_4 first or pass --input explicitly."
        )
    input_rows = to_records(read_excel(input_path, 0))
    input_row_count = len(input_rows)
    logger.info("loaded %s rows from %s", input_row_count, input_path)

    merge_failed: list[dict[str, Any]] = []
    if input_path.name == DEFAULT_INPUT:
        closed_path = paths.source_sheets / CLOSED_LOANS_DETAILS
        if not closed_path.exists():
            raise FileNotFoundError(f"script_5 requires {closed_path} to enrich script_4 output rows")
        closed_raw = to_records(read_excel(closed_path, 0))
        rows, merge_failed = _merge_script4_with_closed(input_rows, closed_raw, logger)
    else:
        rows = [_coerce_full_closed_sheet_row(dict(r)) for r in input_rows]

    completed = cp.completed() if runtime.resume else set()
    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = list(merge_failed)
    skipped: list[dict[str, Any]] = []

    db = DbClient(logger)
    last_close_status_update_ts: float | None = None
    try:
        with db.conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for row in rows:
                    loan_id = _loan_application_int(row)
                    app_id = _normalize_identifier(row.get("application_id") or row.get("Application No."))
                    key = str(loan_id) if loan_id is not None else ""
                    if not key:
                        failed.append(mandatory_failure_fields(None, app_id, "missing loan_id", STAGE))
                        continue
                    if key in completed:
                        skipped.append({"loan_id": loan_id, "application_id": app_id, "reason": "already_processed"})
                        continue

                    closed_date = pd.to_datetime(
                        _first_non_empty(row.get("CLOSED_DATE"), row.get("CLOSED DATE")),
                        errors="coerce",
                    )
                    if pd.isna(closed_date):
                        failed.append(mandatory_failure_fields(loan_id, app_id, "missing CLOSED_DATE", STAGE))
                        continue
                    closed_month = _month_tuple(closed_date.to_pydatetime())
                    final_close_type = _final_close_type_token(row)
                    behavior = _resolve_behavior_from_final_close_type(final_close_type)
                    new_close_status = _resolve_loan_close_status(row, behavior)
                    expected_settlement_due = _expected_settlement_due_amount(row)

                    try:
                        cur.execute("SAVEPOINT sp_script5_loan")
                        cur.execute(
                            """
                            SELECT application_id, close_status, created_by
                            FROM loan_applications
                            WHERE application_id=%s
                            FOR UPDATE
                            """,
                            (loan_id,),
                        )
                        app_row = cur.fetchone()
                        if not app_row:
                            raise ValueError("loan_applications row not found")
                        close_by_user_id = _resolve_close_by_user_id(dict(app_row))
                        old_close_status = _normalize_token(app_row.get("close_status"))
                        if old_close_status and old_close_status != "PENDING":
                            skipped.append(
                                {
                                    "loan_id": loan_id,
                                    "application_id": app_id,
                                    "reason": f"loan_already_closed:{old_close_status}",
                                }
                            )
                            continue

                        cur.execute(
                            """
                            SELECT collection_id, emi_installment_no, due_date, status, due_amount
                            FROM collections
                            WHERE loan_id=%s AND is_active=true
                            ORDER BY collection_id
                            """,
                            (loan_id,),
                        )
                        collections = [dict(item) for item in cur.fetchall()]
                        if not collections:
                            raise ValueError("no active collections found")

                        updates: list[tuple[int, str, str, str]] = []
                        validation_failures: list[str] = []
                        status_counts: dict[str, int] = {}
                        has_settlement_collection = False
                        for coll in collections:
                            installment_no = _to_int(coll.get("emi_installment_no")) or 0
                            coll_due_date = pd.to_datetime(coll.get("due_date"), errors="coerce")
                            if pd.isna(coll_due_date):
                                raise ValueError(f"invalid due_date for collection_id={coll['collection_id']}")
                            if installment_no == -1:
                                category = "SETTLEMENT"
                                has_settlement_collection = True
                                target = _settlement_collection_status_from_final_close_type(row)
                            else:
                                if _normalize_token(coll.get("status")) == "DONE":
                                    continue
                                due_month = _month_tuple(coll_due_date.to_pydatetime())
                                if due_month < closed_month:
                                    category = "PAST_MONTH"
                                elif due_month == closed_month:
                                    category = "CLOSURE_MONTH"
                                else:
                                    category = "FUTURE_MONTH"
                                if category == "PAST_MONTH" and _is_return_final_close_type(final_close_type):
                                    target = _past_month_status_for_return_final_close_type(final_close_type)
                                else:
                                    target = _status_for_collection(category, behavior)
                                if target is None:
                                    continue
                            updates.append((int(coll["collection_id"]), target, category, _clean_key(coll.get("status"))))
                            status_counts[target] = status_counts.get(target, 0) + 1
                            if _to_decimal(coll.get("due_amount")) is None:
                                validation_failures.append(f"collection_id={coll['collection_id']} due_amount_missing")
                            if installment_no == -1 and expected_settlement_due != 0:
                                due_amt = _to_decimal(coll.get("due_amount")) or Decimal("0")
                                if due_amt != expected_settlement_due:
                                    validation_failures.append(
                                        f"collection_id={coll['collection_id']} settlement_amount_mismatch"
                                    )

                        if expected_settlement_due != 0 and not has_settlement_collection:
                            raise ValueError("settlement amount present but settlement collection (installment=-1) missing")
                        if not updates:
                            raise ValueError("no collection status transitions derived")
                        if validation_failures:
                            raise ValueError("validation failed: " + "; ".join(validation_failures[:5]))

                        if execute_mode:
                            if last_close_status_update_ts is not None:
                                elapsed = time.time() - last_close_status_update_ts
                                if elapsed < CLOSE_STATUS_UPDATE_DELAY_SECONDS:
                                    time.sleep(CLOSE_STATUS_UPDATE_DELAY_SECONDS - elapsed)
                            cur.execute(
                                """
                                UPDATE loan_applications
                                SET close_status=%s, closed_at=%s, close_by=%s, updated_at=NOW()
                                WHERE application_id=%s
                                """,
                                (new_close_status, closed_date.to_pydatetime(), close_by_user_id, loan_id),
                            )
                            if cur.rowcount != 1:
                                raise ValueError("loan_applications update rowcount not 1")
                            last_close_status_update_ts = time.time()

                        updated_rows = 0
                        if execute_mode:
                            for collection_id, target, _, _ in updates:
                                cur.execute(
                                    """
                                    UPDATE collections
                                    SET status=%s
                                    WHERE collection_id=%s AND is_active=true AND COALESCE(status,'') <> %s
                                    """,
                                    (target, collection_id, target),
                                )
                                updated_rows += cur.rowcount

                        cur.execute(
                            """
                            SELECT COUNT(*) AS pending_count
                            FROM collections
                            WHERE loan_id=%s
                              AND is_active=true
                              AND emi_installment_no > 0
                              AND UPPER(COALESCE(status, '')) = 'PENDING'
                            """,
                            (loan_id,),
                        )
                        pending_count = int(cur.fetchone()["pending_count"])
                        if pending_count > 0:
                            raise ValueError(f"pending EMI remains after update: {pending_count}")

                        success.append(
                            {
                                "loan_id": loan_id,
                                "application_id": app_id,
                                "old_close_status": old_close_status or "PENDING",
                                "new_close_status": new_close_status,
                                "close_by": close_by_user_id,
                                "closed_at": closed_date.to_pydatetime(),
                                "collection_update_summary": "; ".join(
                                    f"{status}:{count}" for status, count in sorted(status_counts.items())
                                ),
                                "collection_rows_considered": len(collections),
                                "collection_rows_targeted": len(updates),
                                "collection_rows_updated": updated_rows,
                                "execute_mode": execute_mode,
                                "timestamp": datetime.now().isoformat(),
                            }
                        )
                        if execute_mode:
                            cp.mark_completed(key)
                    except Exception as exc:  # noqa: BLE001
                        cur.execute("ROLLBACK TO SAVEPOINT sp_script5_loan")
                        failed_row = mandatory_failure_fields(loan_id, app_id, str(exc), STAGE)
                        failed_row["old_close_status"] = _normalize_token(row.get("close_status")) or "UNKNOWN"
                        failed_row["new_close_status"] = new_close_status
                        failed_row["closed_at"] = None if pd.isna(closed_date) else closed_date.to_pydatetime()
                        failed_row["collection_update_summary"] = ""
                        failed_row["failed_validations"] = str(exc)
                        failed_row["skipped_reason"] = "validation_failed_or_transaction_error"
                        failed.append(failed_row)
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
        loaded=input_row_count,
        buckets={
            "success": success,
            "failed": failed,
            "skipped": skipped,
        },
        reason_fields_by_bucket={
            "failed": ("failure_reason", "failed_validations"),
            "skipped": ("reason",),
        },
    )
    logger.info("audit files: %s", out)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(run())
