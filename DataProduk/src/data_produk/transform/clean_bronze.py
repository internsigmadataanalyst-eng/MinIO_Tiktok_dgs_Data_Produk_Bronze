# src/data_produk/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.data_produk.utils.transform_utils import (
    clean_numeric_columns,
    parse_mixed_dates,
    to_snake_case,
)
from src.data_produk.utils.minio_client import filter_by_sheet_watermark


NUMERIC_COLS = [
    "GMV",
    "Produk terjual",
    "Pesanan",
    "GMV dari Shop Tab",
    "Produk terjual dari Tab Shop",
    "Impresi daftar produk shop tab",
    "Tayangan halaman shop tab",
    "Tayangan halaman unik shop tab",
    "Pembeli produk unik shop tab",
    "GMV dari LIVE",
    "Produk terjual dari LIVE",
    "Impresi dari LIVE",
    "Tayangan halaman dari LIVE",
    "Tayangan halaman unik dari LIVE",
    "Pembeli produk unik dari LIVE",
    "GMV dari video",
    "Produk terjual dari video",
    "Impresi dari video",
    "Tayangan halaman dari video",
    "Tayangan halaman unik dari video",
    "Pembeli produk unik dari video",
    "GMV dari kartu produk",
    "Produk terjual dari kartu produk",
    "Impresi dari kartu produk",
    "Tayangan halaman dari kartu produk",
    "Tayangan halaman unik dari kartu produk",
    "Pembeli unik dari kartu produk",
]


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
    Filter incremental per sheet_name berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke BRONZE_DB.bronze_live, sheet_max_dates)
    """
    # numeric cleaning
    tiktok_produk_clean1 = clean_numeric_columns(
        tiktok_produk_raw, NUMERIC_COLS, fillna_value=0
    )

    # parse tanggal
    tiktok_produk_clean1["Tanggal"] = parse_mixed_dates(
        tiktok_produk_clean1["Tanggal"], return_date=False
    )

    # copy & snake_case
    df = tiktok_produk_clean1.copy()
    df.columns = df.columns.map(to_snake_case)

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

    # Filter incremental per sheet (creds-keyed) berdasarkan watermark
    if "creds" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates