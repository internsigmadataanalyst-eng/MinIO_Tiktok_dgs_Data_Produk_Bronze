# src/data_produk/utils/watermark_monitor.py
"""Pre-flight watermark monitor for the Data Produk ETL.

Ported from Pesanan_Affiliasi and parameterized for Data Produk (sheet "Data
Produk", header at spreadsheet row 3, data from row 4, watermark grain
(creds, sheet_name, toko)). Runs at the very start of run_daily_etl so a
sheet whose data moved ahead of its MinIO watermark aborts the run instead of
silently dropping rows (Gate 1) or, for whitelisted sheets, pages on-call
(Gate 2).

Two gates:

    Gate 1 (abort) -- every non-whitelisted sheet is caught here. Any toko
    whose max source date is AHEAD of the stored watermark means the last run
    never advanced that watermark, so this run would re-ingest from the old
    point and DROP the rows in between. Abort + email before any ingestion.

    Gate 2 (page) -- for whitelisted sheets (config WHITELIST_SHEETS, default
    "ian") the drift is expected (frozen export) or tolerated for other team
    reasons. We do not abort; we page so the drift is reviewed and the
    whitelist kept honest.

Drift = max_tanggal_toko - stored watermark. Only rows that would actually
reach bronze are considered: blank-ID rows are dropped by clean_bronze, so
they can't inflate a toko's max date, and unparsable dates are skipped. A
sheet with no usable data (empty / header-only) counts as NO_DATA and never
blocks.
"""
import os

import gspread
import pandas as pd

from src.data_produk.pipelines.config import (
    WHITELIST_SHEETS,
    WATERMARK_PATH,
)
from src.data_produk.utils.gsheet_client import get_gspread_client
from src.data_produk.utils.minio_client import get_minio_client, get_sheet_watermarks
from src.data_produk.utils.transform_utils import parse_mixed_dates, to_snake_case

TokoGroup = str
STATUS_NEEDS_UPDATE = "NEEDS_UPDATE"
STATUS_OK = "OK"
STATUS_NO_DATA = "NO_DATA"

# Only rows with these columns populated are real data rows in bronze terms.
# blank-ID rows are dropped by clean_bronze, so they must not advance the max
# date (that would let a watermark advance past rows that were dropped).
REQUIRED_COLS = ["id"]

# Data Produk sheets carry their header on spreadsheet row 3 (values[2]) and
# data from row 4 (values[3:]) -- see fetch_data_produk_gsheet.fetch_tiktok_data_produk.
HEADER_ROW = 2  # 0-based; actual row = HEADER_ROW + 1 = 3
DATA_START_ROW = 3  # 0-based; actual first data row = DATA_START_ROW + 1 = 4

_SHEET_CACHE = {}


def _open_spreadsheet(gc: gspread.Client, env_key: str):
    """Open the spreadsheet named by SHEET_REGISTRY's env var, cached."""
    key = os.getenv(env_key)
    if not key:
        raise RuntimeError(f"Env {env_key} belum di-set")
    if key not in _SHEET_CACHE:
        _SHEET_CACHE[key] = gc.open_by_key(key)
    return _SHEET_CACHE[key]


def col_index_to_letter(index: int) -> str:
    letters = ""
    while index >= 0:
        letters = chr(ord("A") + index % 26) + letters
        index = index // 26 - 1
    return letters


def with_retry(func, *args, max_retries: int = 4, **kwargs):
    """Retry a gspread call with linear backoff on rate-limit errors."""
    delay = 15
    attempts = 0
    while True:
        try:
            return func(*args, **kwargs)
        except (gspread.exceptions.APIError, gspread.exceptions.RateLimitError) as e:
            attempts += 1
            if attempts > max_retries:
                raise RuntimeError(
                    f"[RETRY] {func.__name__} gagal setelah {max_retries + 1} "
                    f"percobaan (rate limit): {e!r}"
                )
            headers = getattr(getattr(e, "response", None), "headers", {}) or {}
            retry_after = headers.get("Retry-After")
            if retry_after is not None:
                try:
                    delay = max(int(retry_after), delay)
                except (TypeError, ValueError):
                    pass
            print(
                f"[RETRY] {func.__name__} rate-limited (attempt {attempts}/{max_retries}), "
                f"retry in {delay}s ..."
            )
            import time

            time.sleep(delay)


def get_df_minimal(
    sh, sheet_name: str, min_cols=None, header_row=HEADER_ROW, data_start_row=DATA_START_ROW
) -> pd.DataFrame:
    """Read only the date/toko/ID columns of a Data Produk worksheet."""
    ws = sh.worksheet(sheet_name)
    header_raw = with_retry(ws.row_values, header_row + 1)
    header = [to_snake_case(h)[:128] or f"col_{i}" for i, h in enumerate(header_raw)]
    have = set(header)

    cols = list({"tanggal", "toko"} | set(min_cols or REQUIRED_COLS))
    cols = [c for c in cols if c in have]

    letters = [col_index_to_letter(header.index(c)) for c in cols]
    ranges = [f"{L}{data_start_row + 1}:{L}" for L in letters]
    columns = with_retry(ws.batch_get, ranges)

    n = max((len(c) for c in columns), default=0)
    rows = []
    for r in range(n):
        rows.append([c[r][0] if len(c) > r else "" for c in columns])

    return pd.DataFrame(rows, columns=cols)


def get_max_tanggal_by_toko(df: pd.DataFrame) -> dict:
    """Max parsable date per toko, using only rows that reach bronze.

    Blank-ID rows are dropped (clean_bronze drops them), blank toko dropped,
    unparsable dates dropped.
    """
    result: dict[str, str] = {}
    if df.empty or "tanggal" not in df.columns or "toko" not in df.columns:
        return result

    df = df.copy()
    if "id" in df.columns:
        df = df[df["id"].astype(str).str.strip().ne("")]
    df["toko"] = df["toko"].astype(str).str.strip()
    df = df[df["toko"].ne("")]
    df["tanggal"] = parse_mixed_dates(df["tanggal"], return_date=False)
    df = df[df["tanggal"].notna()]
    if df.empty:
        return result

    grouped = df.groupby("toko")["tanggal"].max()
    for toko, dt in grouped.items():
        result[toko] = pd.Timestamp(dt).strftime("%Y-%m-%d")
    return result


def get_watermark_map(minio_client, bucket: str) -> dict:
    """{(creds, sheet_name, toko): last_processed_date}."""
    records = get_sheet_watermarks(minio_client, bucket, path=WATERMARK_PATH)
    wm = {}
    for rec in records or []:
        creds = str(rec.get("creds") or "")
        sheet = str(rec.get("sheet_name") or "")
        toko = str(rec.get("toko") or "")
        date_val = str(rec.get("last_processed_date") or rec.get("last_update") or "")
        wm[(creds, sheet, toko)] = date_val
    return wm


def compare_watermark_vs_sheet(
    minio_client, bucket: str, sh, creds: str, sheet_name: str, wm_map: dict
) -> dict:
    """Per-toko drift for one sheet vs the stored watermark."""
    drift = {}
    try:
        df = get_df_minimal(sh, sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        raise RuntimeError(
            f"Worksheet '{sheet_name}' tidak ditemukan (sheet id {creds}) -- "
            f"cek SHEET_REGISTRY."
        )
    except Exception as e:
        raise RuntimeError(f"Gagal membaca worksheet '{sheet_name}': {e!r}")

    max_by_toko = get_max_tanggal_by_toko(df)
    if not max_by_toko:
        return {}  # empty/header-only sheet is NO_DATA, never blocks

    for toko, max_date in max_by_toko.items():
        wm = wm_map.get((creds, sheet_name, toko), "")
        try:
            sheet_dt = pd.Timestamp(max_date)
            wm_dt = pd.Timestamp(wm) if wm else pd.Timestamp(None)
        except Exception:
            sheet_dt, wm_dt = pd.Timestamp(None), pd.Timestamp(None)

        if pd.isna(wm_dt):
            status, diff_days = STATUS_NEEDS_UPDATE, None
        elif sheet_dt > wm_dt:
            status, diff_days = STATUS_NEEDS_UPDATE, (sheet_dt - wm_dt).days
        else:
            status, diff_days = STATUS_OK, (sheet_dt - wm_dt).days

        drift[toko] = {
            "sheet_max_date": max_date if pd.notna(sheet_dt) else None,
            "watermark": wm or None,
            "diff_days": diff_days,
            "status": status,
        }
    return drift


def data_produk_watermark_check(
    minio_client=None,
    bucket: str = None,
    whitelist_sheets: list[str] = None,
) -> dict:
    """Pre-flight watermark check over every registered sheet.

    Non-whitelisted sheets that drifted AHEAD of their watermark produce
    ``stale_creds_sheet_toko`` rows (Gate 1: run_daily_etl aborts). Whitelisted
    sheets that drifted produce ``whitelisted_drift`` rows (reported, never
    abort). A sheet that fails to open/read produces an ``error: ...`` status
    row in status_report so run_daily_etl can emit a Gate-1 access-error abort
    instead of an unhandled exception.

    Returns:
      stale_creds_sheet_toko : [(creds, sheet_name, toko, drift)] Gate-1 rows
      whitelisted_drift      : same shape but for whitelisted sheets (reported)
      status_report          : {sheet_name: {creds, whitelisted, stale,
                                            drift_by_toko}}
      summary                : {n_tokos, n_stale, n_whitelisted_drift}
    """
    if whitelist_sheets is None:
        whitelist_sheets = [s.strip() for s in WHITELIST_SHEETS.split(",") if s.strip()]

    from src.data_produk.ingestion.fetch_data_produk_gsheet import SHEET_REGISTRY

    if minio_client is None or bucket is None:
        minio_client, bucket = get_minio_client()

    gc = get_gspread_client()
    wm_map = get_watermark_map(minio_client, bucket)

    stale_creds_sheet_toko = []
    whitelisted_drift = []
    status_report = {}

    for sheet_name, env_key in SHEET_REGISTRY.items():
        creds = os.getenv(env_key) or env_key
        whitelisted = sheet_name in whitelist_sheets
        try:
            sh = _open_spreadsheet(gc, env_key)
            drift_by_toko = compare_watermark_vs_sheet(
                minio_client, bucket, sh, creds, sheet_name, wm_map
            )
        except Exception as e:
            status_report[sheet_name] = {
                "creds": creds,
                "whitelisted": whitelisted,
                "stale": False,
                "error": str(e),
                "drift_by_toko": {},
            }
            continue

        is_stale = False
        for toko, drift in drift_by_toko.items():
            if drift["status"] != STATUS_NEEDS_UPDATE:
                continue
            if whitelisted:
                whitelisted_drift.append((creds, sheet_name, toko, drift))
            else:
                stale_creds_sheet_toko.append((creds, sheet_name, toko, drift))
                is_stale = True

        status_report[sheet_name] = {
            "creds": creds,
            "whitelisted": whitelisted,
            "stale": is_stale,
            "error": None,
            "drift_by_toko": drift_by_toko,
        }

    return {
        "stale_creds_sheet_toko": stale_creds_sheet_toko,
        "whitelisted_drift": whitelisted_drift,
        "status_report": status_report,
        "summary": {
            "n_tokos": sum(
                len(r["drift_by_toko"]) for r in status_report.values()
            ),
            "n_stale": len(stale_creds_sheet_toko),
            "n_whitelisted_drift": len(whitelisted_drift),
        },
    }