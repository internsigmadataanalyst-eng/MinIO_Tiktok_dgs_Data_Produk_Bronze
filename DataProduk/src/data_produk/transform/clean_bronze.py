# src/data_produk/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.data_produk.utils.transform_utils import (
    parse_mixed_dates,
    to_snake_case,
    normalize_status,
)
from src.data_produk.utils.minio_client import filter_by_sheet_watermark
from src.data_produk.utils.log import emit


def _canon(x):
    import pandas as pd

    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_produk(
    tiktok_produk_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per (creds,sheet_name,toko) berdasarkan watermark (sheet_watermarks).
    Watermark grain is (creds, sheet_name, toko) — toko verbatim, blank toko already quarantined upstream.
    Output: (df siap di-load ke BRONZE_DB.bronze_live, sheet_max_dates)
    """

    df = tiktok_produk_raw.copy()
    # parse tanggal
    df["Tanggal"] = parse_mixed_dates(
        df["Tanggal"], return_date=False
    )

    # snake_case
    df.columns = df.columns.map(to_snake_case)

    # defensive: drop empty-header columns (unmapped data) -> prevents BQ "Field missing name" (400)
    # e.g. trailing empty GSheet columns arrive as "" and survive to_snake_case as "".
    df = df.loc[:, df.columns.astype(str).str.strip() != ""]
    # also drop duplicate names that collide after snake_case (e.g. "a-b" vs "a/b" -> "ab")
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    # buang baris tanpa id
    df = df[df["id"].astype(str).str.strip() != ""]

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["tanggal","toko","id","gmv","produk_terjual"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

     # Filter incremental per (creds,sheet_name,toko) — triple grain verbatim
    if "creds" in df.columns and "sheet_name" in df.columns and "toko" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "sheet_name", "toko", "tanggal", sheet_watermarks or {}
        )
    elif "creds" in df.columns and "toko" in df.columns:
        # fallback without sheet_name (should not happen after quarantine)
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "sheet_name", "toko", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # status: unify the Indonesian and English vocabularies to English so
    # silver's single column is comparable across sheets. Done AFTER the
    # watermark filter so the unmapped count covers only rows actually entering
    # bronze -- a frozen sheet with a legacy vocabulary (ian) loads nothing and
    # must not raise a permanent false warning.
    if "status" in df.columns:
        df["status"], n_unmapped = normalize_status(df["status"])
        if n_unmapped:
            emit(
                phase="transform",
                component="clean_bronze",
                message=f"{n_unmapped} status value(s) outside STATUS_VALUE_MAP (kept, uppercased)",
                level="WARNING",
                metrics={"n_unmapped_status": n_unmapped},
            )
            print(f"[STATUS] {n_unmapped} nilai di luar STATUS_VALUE_MAP (dipertahankan, huruf besar).")

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates