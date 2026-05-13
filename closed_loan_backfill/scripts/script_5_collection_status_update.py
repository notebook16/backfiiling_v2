#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
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


def _month_tuple(dt: datetime) -> tuple[int, int]:
    return (dt.year, dt.month)


def _resolve_behavior(close_type_raw: Any) -> str:
    token = _normalize_token(close_type_raw).replace("_", " ").replace("-", " ")
    token = " ".join(token.split())
    if not token:
        return "RECOVERY"
    if token in {"RECOVER", "RECOVERY", "RECOVERED"}:
        return "RECOVERY"
    if token in {"RETURN + PAYMENT", "RETURN PAYMENT", "RETURN+PAYMENT"}:
        return "RETURN_PAYMENT"
    if token in {"RETURN TO CUSTOMER", "RETURN", "RETURN ASSET"}:
        return "RETURN_TO_CUSTOMER"
    if token in {"BULK CLOSE", "CLOSE", "CLOSED", "FORECLOSE", "FORECLOSURE"}:
        return "BULK_CLOSE"
    if token in {"LAST EMI PAID", "LAST EMI", "EMI PAID"}:
        return "LAST_EMI_PAID"
    return "RECOVERY"


def _resolve_loan_close_status(row: dict[str, Any], behavior: str) -> str:
    close_type = _normalize_token(row.get("CLOSE_TYPE"))
    final_close_type = _normalize_token(row.get("FINAL_CLOSE_TYPE") or row.get("FINAL CLOSE TYPE"))
    if final_close_type in {"CLOSED", "RECOVERED", "RETURN", "FORECLOSE"}:
        return final_close_type
    if close_type in {"CLOSED", "RECOVERED", "RETURN", "FORECLOSE"}:
        return close_type
    if behavior == "RECOVERY":
        return "RECOVERED"
    if behavior in {"RETURN_PAYMENT", "RETURN_TO_CUSTOMER"}:
        return "RETURN"
    if behavior == "BULK_CLOSE":
        return "FORECLOSE"
    if behavior == "LAST_EMI_PAID":
        return "CLOSED"
    return "CLOSED"


def _status_for_collection(category: str, behavior: str) -> str | None:
    matrix: dict[str, dict[str, str | None]] = {
        "PAST_MONTH": {
            "RECOVERY": "DEFAULT",
            "RETURN_PAYMENT": "DEFAULT_CREDIT",
            "RETURN_TO_CUSTOMER": "DEFAULT_DEBIT",
            "BULK_CLOSE": "PAID_BULK",
            "LAST_EMI_PAID": "DONE",
        },
        "CLOSURE_MONTH": {
            "RECOVERY": "RECOVER",
            "RETURN_PAYMENT": "RETURN",
            "RETURN_TO_CUSTOMER": "RETURN",
            "BULK_CLOSE": "CLOSE",
            "LAST_EMI_PAID": "DONE",
        },
        "FUTURE_MONTH": {
            "RECOVERY": "RECOVER",
            "RETURN_PAYMENT": "RETURN",
            "RETURN_TO_CUSTOMER": "RETURN",
            "BULK_CLOSE": "CLOSE",
            "LAST_EMI_PAID": None,
        },
        "SETTLEMENT": {
            "RECOVERY": "DONE_RECOVER",
            "RETURN_PAYMENT": "DONE_RETURN",
            "RETURN_TO_CUSTOMER": "DONE_RETURN",
            "BULK_CLOSE": "DONE_CLOSE",
            "LAST_EMI_PAID": None,
        },
    }
    return matrix.get(category, {}).get(behavior)


def run() -> int:
    parser = parse_common_args("Script 5 - Final collection status update")
    parser.add_argument("--input", default="CLOSED_LOANS_DETAILS.xlsx")
    args = parser.parse_args()
    runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)
    cp = CheckpointStore(STAGE, paths)
    execute_mode = runtime.execute and not runtime.dry_run

    rows = to_records(read_excel(paths.source_sheets / args.input, 0))
    completed = cp.completed() if runtime.resume else set()
    success: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    db = DbClient(logger)
    try:
        with db.conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for row in rows:
                    loan_id = _to_int(row.get("loan_id"))
                    app_id = _normalize_identifier(row.get("application_id") or row.get("Application No."))
                    key = str(loan_id) if loan_id is not None else ""
                    if not key:
                        failed.append(mandatory_failure_fields(None, app_id, "missing loan_id", STAGE))
                        continue
                    if key in completed:
                        skipped.append({"loan_id": loan_id, "application_id": app_id, "reason": "already_processed"})
                        continue

                    closed_date = pd.to_datetime(row.get("CLOSED_DATE"), errors="coerce")
                    if pd.isna(closed_date):
                        failed.append(mandatory_failure_fields(loan_id, app_id, "missing CLOSED_DATE", STAGE))
                        continue
                    closed_month = _month_tuple(closed_date.to_pydatetime())
                    behavior = _resolve_behavior(row.get("CLOSE_TYPE"))
                    new_close_status = _resolve_loan_close_status(row, behavior)
                    settlement_amount = _to_decimal(
                        row.get("SETTLEMENT_AMOUNT") or row.get("Settlement Amount") or 0
                    ) or Decimal("0")

                    try:
                        cur.execute("SAVEPOINT sp_script5_loan")
                        cur.execute(
                            """
                            SELECT application_id, close_status
                            FROM loan_applications
                            WHERE application_id=%s
                            FOR UPDATE
                            """,
                            (loan_id,),
                        )
                        app_row = cur.fetchone()
                        if not app_row:
                            raise ValueError("loan_applications row not found")
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
                            else:
                                due_month = _month_tuple(coll_due_date.to_pydatetime())
                                if due_month < closed_month:
                                    category = "PAST_MONTH"
                                elif due_month == closed_month:
                                    category = "CLOSURE_MONTH"
                                else:
                                    category = "FUTURE_MONTH"
                            target = _status_for_collection(category, behavior)
                            if target is None:
                                continue
                            updates.append((int(coll["collection_id"]), target, category, _clean_key(coll.get("status"))))
                            status_counts[target] = status_counts.get(target, 0) + 1
                            if _to_decimal(coll.get("due_amount")) is None:
                                validation_failures.append(f"collection_id={coll['collection_id']} due_amount_missing")
                            if installment_no == -1 and settlement_amount > 0:
                                due_amt = _to_decimal(coll.get("due_amount")) or Decimal("0")
                                if due_amt != settlement_amount:
                                    validation_failures.append(
                                        f"collection_id={coll['collection_id']} settlement_amount_mismatch"
                                    )

                        if settlement_amount > 0 and not has_settlement_collection:
                            raise ValueError("settlement amount present but settlement collection (installment=-1) missing")
                        if not updates:
                            raise ValueError("no collection status transitions derived")

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
                        if validation_failures:
                            raise ValueError("validation failed: " + "; ".join(validation_failures[:5]))

                        if execute_mode:
                            cur.execute(
                                """
                                UPDATE loan_applications
                                SET close_status=%s, closed_at=%s, updated_at=NOW()
                                WHERE application_id=%s
                                """,
                                (new_close_status, closed_date.to_pydatetime(), loan_id),
                            )
                            if cur.rowcount != 1:
                                raise ValueError("loan_applications update rowcount not 1")

                        success.append(
                            {
                                "loan_id": loan_id,
                                "application_id": app_id,
                                "old_close_status": old_close_status or "PENDING",
                                "new_close_status": new_close_status,
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
        loaded=len(rows),
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
