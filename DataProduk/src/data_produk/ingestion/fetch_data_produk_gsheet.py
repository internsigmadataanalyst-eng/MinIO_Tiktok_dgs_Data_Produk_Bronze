# src/data_produk/ingestion/fetch_data_produk_gsheet.py
import os

import gspread
import pandas as pd

from src.data_produk.utils.transform_utils import (
    DIM_COLS,
    NUMERIC_COLS,
    PERCENT_COLS,
    to_snake_case,
)
from src.data_produk.utils.log import emit


SHEET_REGISTRY = {
    "matz": "SH_KEY_MATZ",
    "ian": "SH_KEY_IAN",
    "deni": "SH_KEY_DENI",
    "riwa": "SH_KEY_RIWA",
    "imam": "SH_KEY_IMAM",
}

# Columns the transform and the silver SQL require. Checked against each
# sheet's OWN header (values[2]) before the frame exists -- once sheets are
# concatenated the column union masks a sheet that lost a column, and it only
# surfaces much later as a NULL column in bronze / an Unrecognized-name error
# in silver, neither of which names the sheet at fault.
EXPECTED_HEADER = {
    to_snake_case(c) for c in NUMERIC_COLS + PERCENT_COLS + DIM_COLS
}

# Sheets allowed to keep a partial header. `ian` is frozen on the pre-AFFILIASI
# export (no rentang_gmv, no affiliasi block) and never regenerates, so its
# header would never satisfy the check. Its own watermark (2026-02-07) is at its
# last data date and filter_by_sheet_watermark uses strict '>', so it emits zero
# rows anyway -- its missing columns would only appear as NULL if the watermark
# were ever reset, which is correct for "not reported".
HEADER_CHECK_EXEMPT = {"ian"}


def _check_header(sheet_name: str, header) -> None:
    """Fail fast when a required column is missing from one sheet's header.

    Missing column -> ValueError (stop the run: a half-schema bronze table is
    unrecoverable by append). Unknown column -> WARN only, so a renamed or
    newly-added metric reaches bronze and quarantine instead of halting the
    pipeline.
    """
    if sheet_name in HEADER_CHECK_EXEMPT:
        return

    have = {to_snake_case(str(c)) for c in header if str(c).strip()}
    missing = sorted(EXPECTED_HEADER - have)
    unknown = sorted(have - EXPECTED_HEADER)

    if unknown:
        print(f"[HEADER] {sheet_name}: kolom tak dikenal {unknown} (tetap diproses)")
        emit(
            phase="ingestion",
            component="fetch_data_produk_gsheet",
            message=f"{sheet_name}: unknown header column(s): {unknown}",
            level="WARNING",
            metrics={"n_unknown_header": len(unknown), "unknown_header": unknown},
            source={"sheet_name": sheet_name},
        )

    if missing:
        raise ValueError(
            f"[HEADER] {sheet_name}: header kehilangan kolom wajib {missing}. "
            f"Source format sudah berubah -- cek header GSheet (values[2]) dan "
            f"sinkronkan NUMERIC_COLS/PERCENT_COLS/DIM_COLS sebelum run berikutnya."
        )


def fetch_tiktok_data_produk(gc: gspread.Client) -> pd.DataFrame:
    """
    Ambil data produk dari GSheet yang terdaftar di SHEET_REGISTRY,
    tag tiap sheet dengan kolom 'sheet_name', lalu concat jadi satu
    DataFrame raw (belum dibersihkan).
    """
    frames = []
    for sheet_name, env_key in SHEET_REGISTRY.items():
        sh = gc.open_by_key(os.getenv(env_key))
        ws = sh.worksheet("Data Produk")
        values = ws.get_all_values()
        _check_header(sheet_name, values[2])
        df_sheet = pd.DataFrame(values[3:], columns=values[2])
        df_sheet = df_sheet.loc[:, ~df_sheet.columns.duplicated()]
        df_sheet["creds"] = os.getenv(env_key)
        df_sheet["sheet_name"] = sheet_name
        frames.append(df_sheet)
        print(f"[INGEST] {sheet_name}: {len(df_sheet)} rows")

    tiktok_produk = pd.concat(frames, ignore_index=True)
    return tiktok_produk
