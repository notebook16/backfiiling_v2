#!/usr/bin/env python3
"""Post-pipeline validation: collection_trans amounts vs collections due_amount + fine_amount."""
from __future__ import annotations

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

STAGE = "script_VAL2_collection_collection_trans_validation"
LOS_DATA_NAME = "LOS _Data.xlsx"
SCRIPT3_UPDATED_GLOB = "script_3_updated_collections_*.xlsx"
SCRIPT3_TRANS_GLOB = "collection_trans_*.csv"
SCRIPT4_COLLECTIONS_NAME = "script_4_generated_collections_latest.csv"
SCRIPT4_TRANS_NAME = "script_4_generated_collection_trans_latest.csv"


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


def _to_decimal(value: Any) -> Decimal:
    if _is_blank(value):
        return Decimal("0")
    text = str(value).strip().replace(",", "")
    try:
        return Decimal(text)
    except Exception:  # noqa: BLE001
        return Decimal("0")


def _to_int(value: Any) -> int | None:
    dec = _to_decimal(value)
    try:
        return int(dec)
    except Exception:  # noqa: BLE001
        return None


def _index_los_by_application_id(los_rows: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in los_rows:
        app_id = _normalize_identifier(row.get("application_id"))
        app_no = _clean_key(row.get("Application Number"))
        if app_id and app_no and app_id not in out:
            out[app_id] = app_no
    return out


def _resolve_latest_file(directory: Path, glob_pattern: str) -> Path:
    matches = sorted(directory.glob(glob_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"no files matching {glob_pattern} under {directory}")
    return matches[0].resolve()


def _resolve_latest_script3_trans_csv(paths: Any) -> Path:
    script3_db_dir = paths.generated_db_sheets / "script_3"
    if not script3_db_dir.is_dir():
        raise FileNotFoundError(f"script_VAL2 requires {script3_db_dir} for script_3 collection_trans staging")
    run_dirs = sorted(
        (p for p in script3_db_dir.iterdir() if p.is_dir()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for run_dir in run_dirs:
        matches = sorted(run_dir.glob(SCRIPT3_TRANS_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
        if matches:
            return matches[0].resolve()
    raise FileNotFoundError(f"no {SCRIPT3_TRANS_GLOB} found under {script3_db_dir}")


def _load_allowed_trans_by_collection(paths: Any, logger) -> dict[int, set[int]]:
    """trans_ids staged by script_3 / script_4 (the only ones that should exist on these collections)."""
    allowed: dict[int, set[int]] = {}

    script3_trans_path = _resolve_latest_script3_trans_csv(paths)
    script3_trans = to_records(pd.read_csv(script3_trans_path))
    logger.info("loaded %s script_3 staged collection_trans from %s", len(script3_trans), script3_trans_path)

    script4_trans_path = paths.generated_sheets / "script_4" / SCRIPT4_TRANS_NAME
    if not script4_trans_path.exists():
        raise FileNotFoundError(f"script_VAL2 requires {script4_trans_path}")
    script4_trans = to_records(pd.read_csv(script4_trans_path))
    logger.info("loaded %s script_4 staged collection_trans from %s", len(script4_trans), script4_trans_path)

    for row in script3_trans + script4_trans:
        collection_id = _to_int(row.get("collection_id"))
        trans_id = _to_int(row.get("trans_id"))
        if collection_id is None or trans_id is None:
            continue
        allowed.setdefault(collection_id, set()).add(trans_id)

    return allowed


def _amounts_match(due_amount: Decimal, fine_amount: Decimal, trans_amount: Decimal) -> bool:
    expected = due_amount + fine_amount
    if trans_amount == expected:
        return True
    # script_4 settlement: collections.due_amount is signed; trans rows use positive slice amounts
    if expected < 0 and trans_amount == -expected:
        return True
    return False


def _reconciled_collection_total(due_amount: Decimal, fine_amount: Decimal) -> Decimal:
    """Batch-side amount aligned with per-collection match (trans uses positive slices)."""
    expected = due_amount + fine_amount
    if expected < 0:
        return -expected
    return expected


def _compute_batch_summary(
    db_by_collection: dict[int, dict[str, Decimal]],
) -> dict[str, Any]:
    collections_total_raw = Decimal("0")
    collections_total_reconciled = Decimal("0")
    collection_trans_total = Decimal("0")
    negative_due_adjustment = Decimal("0")
    negative_due_sum = Decimal("0")

    for row in db_by_collection.values():
        due_amount = row["due_amount"]
        fine_amount = row["fine_amount"]
        due_plus_fine = due_amount + fine_amount
        trans_amount = row["collection_trans_amount"]
        collections_total_raw += due_plus_fine
        collections_total_reconciled += _reconciled_collection_total(due_amount, fine_amount)
        collection_trans_total += trans_amount
        if due_plus_fine < 0:
            negative_due_sum += due_plus_fine
            negative_due_adjustment += _reconciled_collection_total(due_amount, fine_amount) - due_plus_fine

    return {
        "collection_count": len(db_by_collection),
        "collections_total_raw": str(collections_total_raw),
        "sum_of_negative_due_plus_fine": str(negative_due_sum),
        "negative_due_batch_adjustment": str(negative_due_adjustment),
        "collections_total_reconciled": str(collections_total_reconciled),
        "collection_trans_total": str(collection_trans_total),
        "batch_totals_match": collections_total_reconciled == collection_trans_total,
        "batch_totals_note": (
            "Per-collection rows use abs(due+fine) when due+fine<0 (script_4 settlements). "
            "Raw SUM(due+fine) in pgAdmin subtracts negative dues; add negative_due_batch_adjustment "
            "(2×abs per negative settlement) to compare with SUM(trans.amount)."
        ),
    }


def _load_scope_collections(paths: Any) -> list[dict[str, Any]]:
    script3_dir = paths.generated_sheets / "script_3"
    script4_dir = paths.generated_sheets / "script_4"

    script3_path = _resolve_latest_file(script3_dir, SCRIPT3_UPDATED_GLOB)
    script3_rows = to_records(read_excel(script3_path, 0))

    script4_path = script4_dir / SCRIPT4_COLLECTIONS_NAME
    if not script4_path.exists():
        raise FileNotFoundError(f"script_VAL2 requires {script4_path}")
    script4_rows = to_records(pd.read_csv(script4_path))

    scoped: dict[int, dict[str, Any]] = {}

    for row in script3_rows:
        collection_id = _to_int(row.get("collection_id"))
        if collection_id is None:
            continue
        scoped[collection_id] = {
            "collection_id": collection_id,
            "loan_id": _to_int(row.get("loan_id")),
            "application_id": _normalize_identifier(row.get("application_id")),
            "application_no": _clean_key(row.get("application_no")),
            "source_script": "script_3",
        }

    for row in script4_rows:
        collection_id = _to_int(row.get("collection_id"))
        if collection_id is None:
            continue
        scoped[collection_id] = {
            "collection_id": collection_id,
            "loan_id": _to_int(row.get("loan_id")),
            "application_id": _normalize_identifier(row.get("application_id")),
            "application_no": "",
            "source_script": "script_4",
        }

    return list(scoped.values())


def _fetch_db_collection_data(
    collection_ids: list[int],
    logger,
) -> tuple[dict[int, dict[str, Decimal]], dict[int, list[int]]]:
    if not collection_ids:
        return {}, {}

    collections_sql = """
        SELECT collection_id, due_amount, fine_amount
        FROM collections
        WHERE collection_id = ANY(%s)
          AND is_active = true
    """
    trans_sql = """
        SELECT collection_id, trans_id, amount
        FROM collection_trans
        WHERE collection_id = ANY(%s)
          AND is_active = true
        ORDER BY collection_id, trans_id
    """

    db = DbClient(logger)
    totals: dict[int, dict[str, Decimal]] = {}
    trans_ids_by_collection: dict[int, list[int]] = {}
    try:
        with db.conn() as conn:
            with conn.cursor() as cur:
                cur.execute(collections_sql, (collection_ids,))
                for collection_id, due_amount, fine_amount in cur.fetchall():
                    cid = int(collection_id)
                    totals[cid] = {
                        "due_amount": _to_decimal(due_amount),
                        "fine_amount": _to_decimal(fine_amount),
                        "collection_trans_amount": Decimal("0"),
                    }
                    trans_ids_by_collection[cid] = []

                cur.execute(trans_sql, (collection_ids,))
                for collection_id, trans_id, amount in cur.fetchall():
                    cid = int(collection_id)
                    tid = int(trans_id)
                    trans_ids_by_collection.setdefault(cid, []).append(tid)
                    if cid in totals:
                        totals[cid]["collection_trans_amount"] += _to_decimal(amount)
    finally:
        db.close()
    return totals, trans_ids_by_collection


def run() -> int:
    parser = parse_common_args(
        "Script VAL2 - collection due_amount+fine_amount vs collection_trans amount"
    )
    args = parser.parse_args()
    _runtime, paths = runtime_from_args(args)
    logger = setup_logger(STAGE, paths)
    load_env_file(paths.root.parent / ".env", logger)

    scoped = _load_scope_collections(paths)
    logger.info("loaded %s collections from script_3 updated + script_4 generated outputs", len(scoped))

    allowed_trans_by_collection = _load_allowed_trans_by_collection(paths, logger)

    los_rows = to_records(read_excel(paths.source_sheets / LOS_DATA_NAME, 0))
    los_app_by_id = _index_los_by_application_id(los_rows)

    collection_ids = [item["collection_id"] for item in scoped]
    db_by_collection, db_trans_ids_by_collection = _fetch_db_collection_data(collection_ids, logger)

    report_rows: list[dict[str, Any]] = []
    matched: list[dict[str, Any]] = []
    mismatched: list[dict[str, Any]] = []
    missing_in_db: list[dict[str, Any]] = []
    extra_trans: list[dict[str, Any]] = []

    for item in scoped:
        collection_id = item["collection_id"]
        db_row = db_by_collection.get(collection_id)
        application_no = item["application_no"] or los_app_by_id.get(item["application_id"], "")
        loan_id = item["loan_id"]

        allowed_trans_ids = allowed_trans_by_collection.get(collection_id, set())
        db_trans_ids = db_trans_ids_by_collection.get(collection_id, [])
        unexpected_trans_ids = sorted(set(db_trans_ids) - allowed_trans_ids)
        no_extra_trans = len(unexpected_trans_ids) == 0

        if db_row is None:
            row = {
                "application_no": application_no,
                "loan_id": loan_id,
                "collection_id": collection_id,
                "source_script": item["source_script"],
                "due_amount": None,
                "fine_amount": None,
                "collection_due_plus_fine_sum": None,
                "collection_trans_amount": None,
                "amounts_match": False,
                "expected_trans_count": len(allowed_trans_ids),
                "db_trans_count": len(db_trans_ids),
                "unexpected_trans_ids": ",".join(str(t) for t in unexpected_trans_ids),
                "no_extra_collection_trans": no_extra_trans,
                "detail": "collection not found in database",
            }
            missing_in_db.append(row)
            report_rows.append(row)
            continue

        due_amount = db_row["due_amount"]
        fine_amount = db_row["fine_amount"]
        due_plus_fine = due_amount + fine_amount
        trans_amount = db_row["collection_trans_amount"]
        is_match = _amounts_match(due_amount, fine_amount, trans_amount)

        details: list[str] = []
        if not is_match:
            details.append("collection_trans sum != due_amount + fine_amount")
        if not no_extra_trans:
            details.append(
                "unexpected collection_trans not created by script_3/script_4: "
                + ",".join(str(t) for t in unexpected_trans_ids)
            )

        row = {
            "application_no": application_no,
            "loan_id": loan_id,
            "collection_id": collection_id,
            "source_script": item["source_script"],
            "due_amount": str(due_amount),
            "fine_amount": str(fine_amount),
            "collection_due_plus_fine_sum": str(due_plus_fine),
            "collection_trans_amount": str(trans_amount),
            "amounts_match": is_match,
            "expected_trans_count": len(allowed_trans_ids),
            "db_trans_count": len(db_trans_ids),
            "unexpected_trans_ids": ",".join(str(t) for t in unexpected_trans_ids),
            "no_extra_collection_trans": no_extra_trans,
            "detail": "; ".join(details),
        }
        report_rows.append(row)
        if not no_extra_trans:
            extra_trans.append(row)
        if is_match and no_extra_trans:
            matched.append(row)
        elif not is_match or not no_extra_trans:
            mismatched.append(row)

    batch_summary = _compute_batch_summary(db_by_collection)
    logger.info("=== VAL2 batch totals (same %s collection ids as pgAdmin list) ===", len(collection_ids))
    logger.info("collections_total_raw (SUM due+fine as stored): %s", batch_summary["collections_total_raw"])
    logger.info("sum_of_negative_due_plus_fine: %s", batch_summary["sum_of_negative_due_plus_fine"])
    logger.info("negative_due_batch_adjustment (add to raw for pgAdmin reconcile): %s", batch_summary["negative_due_batch_adjustment"])
    logger.info("collections_total_reconciled: %s", batch_summary["collections_total_reconciled"])
    logger.info("collection_trans_total: %s", batch_summary["collection_trans_total"])
    logger.info("batch_totals_match: %s", batch_summary["batch_totals_match"])

    output_dir = paths.generated_sheets / "script_VAL2"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_ts = ts_label()

    report_path = output_dir / f"script_VAL2_collection_trans_amount_validation_{run_ts}.xlsx"
    latest_path = output_dir / "script_VAL2_collection_trans_amount_validation_latest.xlsx"
    batch_summary_rows = [batch_summary]
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        pd.DataFrame(report_rows).to_excel(writer, sheet_name="per_collection", index=False)
        pd.DataFrame(batch_summary_rows).to_excel(writer, sheet_name="batch_summary", index=False)
    with pd.ExcelWriter(latest_path, engine="openpyxl") as writer:
        pd.DataFrame(report_rows).to_excel(writer, sheet_name="per_collection", index=False)
        pd.DataFrame(batch_summary_rows).to_excel(writer, sheet_name="batch_summary", index=False)

    audit = write_audit_xlsx(
        output_dir,
        "script_VAL2_collection_trans_validation",
        {
            "matched": matched,
            "mismatch": mismatched,
            "extra_collection_trans": extra_trans,
            "missing_in_db": missing_in_db,
        },
    )

    log_stage_summary(
        logger,
        STAGE,
        loaded=len(scoped),
        buckets={
            "matched": matched,
            "mismatch": mismatched,
            "extra_collection_trans": extra_trans,
            "missing_in_db": missing_in_db,
        },
        reason_fields_by_bucket={
            "mismatch": ("detail",),
            "extra_collection_trans": ("detail",),
            "missing_in_db": ("detail",),
        },
    )
    logger.info("validation report: %s", report_path)
    logger.info("latest validation report: %s", latest_path)
    logger.info("audit files: %s", audit)

    hard_failures = mismatched + missing_in_db
    return 0 if not hard_failures else 2


if __name__ == "__main__":
    raise SystemExit(run())
