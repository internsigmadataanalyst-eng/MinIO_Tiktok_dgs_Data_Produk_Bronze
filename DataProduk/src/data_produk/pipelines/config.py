# src/data_produk/pipelines/config.py
"""Shared constants and mutable pipeline state for the Data Produk ETL."""

import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/data_produk.json"

# Sheets allowed to drift AHEAD of their watermark without aborting the run
# (Gate 2 pages on-call instead). `ian` is frozen on the pre-AFFILIASI export,
# so the MINIO bucket watermark sits at its last data date and the sheet never
# stops changing from the sheet side. Comma-separated sheet names.
WHITELIST_SHEETS = set(
    s.strip() for s in os.getenv("WHITELIST_SHEETS", "ian").split(",") if s.strip()
)

# Quarantine entries younger than this are "pending recovery" and hold the
# pre-flight gate open (PATH A still expects to re-admit the repaired rows);
# older entries are treated as stuck and abort the run again.
QUARANTINE_RECOVERY_MAX_AGE_DAYS = int(
    os.getenv("QUARANTINE_RECOVERY_MAX_AGE_DAYS", "14")
)

SRC_GSHEET = {"system": "Google_Sheets", "entity": "Data Produk"}
SRC_MINIO = {"system": "MinIO", "entity": WATERMARK_PATH}
TGT_MINIO = {"system": "MinIO", "entity": "data/produk"}
TGT_MINIO_QUARANTINE = {"system": "MinIO", "entity": "quarantine"}

# BigQuery table ids -- single source of truth. Previously these table ids were
# duplicated as string literals in ~13 places in run_daily_etl.py (load, reads,
# MERGE, cleanup, logs) where they could drift apart.
# Production is the default. The Testing tables remain reachable by env override:
#   BQ_BRONZE_TABLE=Testing.bronze_produk
#   BQ_SILVER_TABLE=Testing.silver_tt_produk
BQ_BRONZE_TABLE = os.getenv("BQ_BRONZE_TABLE", "BRONZE_DB.bronze_produk")
BQ_SILVER_TABLE = os.getenv("BQ_SILVER_TABLE", "SILVER_DB.silver_tt_produk")

TGT_BQ_BRONZE = {"system": "BigQuery", "entity": f"{PROJECT_ID}.{BQ_BRONZE_TABLE}"}
TGT_BQ_SILVER = {"system": "BigQuery", "entity": f"{PROJECT_ID}.{BQ_SILVER_TABLE}"}

BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
]

_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
}


def bq_full_load(rows: int) -> list[dict]:
    """BQ-update summary for a run that appended rows to bronze + merged silver."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "append", "rows": rows},
        {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
    ]


def bq_noop() -> list[dict]:
    """BQ-update summary for a successful run with no new rows to load."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "no change", "rows": 0},
        {"table": TGT_BQ_SILVER["entity"], "action": "no change"},
    ]


def get_credentials():
    from google.oauth2 import service_account

    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)
