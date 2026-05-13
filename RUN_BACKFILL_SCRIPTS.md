# Backfill Script Commands

This file is formatted for direct copy-paste into the terminal.

Use the command lines below as-is. Do not paste Markdown fences like ```bash.

## Repository Root

cd /home/prince/Desktop/BACKFILL

## Virtualenv

Activate the virtualenv:

source /home/prince/Desktop/BACKFILL/.venv/bin/activate

Do not run `.venv/bin/activate` directly. It must be sourced.

If you do not want to activate the virtualenv, use `.venv/bin/python` directly in the commands below.

## Individual Scripts

### Dry Run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_1_validation.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_2_phase2_emi_creation.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_3_backfill_collection.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_4_settlement_creation.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_5_collection_status_update.py --dry-run

### Execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_1_validation.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_2_phase2_emi_creation.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_3_backfill_collection.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_4_settlement_creation.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/script_5_collection_status_update.py --execute

## Master Runner

### Full Pipeline Dry Run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --dry-run

### Full Pipeline Execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --execute

## Run One Script Through Master

### Dry Run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_1_validation.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_2_phase2_emi_creation.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_3_backfill_collection.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_4_settlement_creation.py --dry-run

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_5_collection_status_update.py --dry-run

### Execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_1_validation.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_2_phase2_emi_creation.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_3_backfill_collection.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_4_settlement_creation.py --execute

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_5_collection_status_update.py --execute

## Resume Commands

### Full Pipeline Resume

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode resume --dry-run

### Single Script Resume

cd /home/prince/Desktop/BACKFILL && .venv/bin/python closed_loan_backfill/scripts/master_runner.py --mode single --script script_3_backfill_collection.py --resume --dry-run

## Recommended Order

Run scripts in this order if you are executing them individually:

1. script_1_validation.py
2. script_2_phase2_emi_creation.py
3. script_3_backfill_collection.py
4. script_4_settlement_creation.py
5. script_5_collection_status_update.py

## Notes

- `script_3_backfill_collection.py` expects the validated output from `script_1_validation.py`.
- `master_runner.py` asks for confirmation before moving to the next script in full mode.
- `script_3`, `script_4`, and `script_5` are the stages where execute mode matters most for DB changes.
