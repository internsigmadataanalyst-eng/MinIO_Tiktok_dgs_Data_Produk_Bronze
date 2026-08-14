import re
from pathlib import Path
import duckdb
import pandas as pd


def _transpile_bq_to_duckdb(sql_content: str) -> str:
    """Membaca file SQL DuckDB yang sudah siap pakai."""
    # Menghapus prefix database & backtick jika ada
    sql_executable = (
        sql_content.replace("`database-sigma.", "")
        .replace("database-sigma.", "")
        .replace("`", "")
    )
    return sql_executable


def test_merge_to_silver_duckdb(df_bronze: pd.DataFrame):
    """Menjalankan simulasi MERGE Bronze -> Silver Produk di DuckDB In-Memory."""
    con = duckdb.connect(":memory:")

    # Setup Schema & Tabel Bronze
    con.sql("CREATE SCHEMA IF NOT EXISTS BRONZE_DB;")
    con.sql("CREATE TABLE BRONZE_DB.bronze_produk AS SELECT * FROM df_bronze")
    
    # Setup Schema & Tabel Silver
    con.sql("CREATE SCHEMA IF NOT EXISTS SILVER_DB;")
    con.sql("""
        CREATE TABLE IF NOT EXISTS SILVER_DB.silver_tt_produk (
            tanggal DATE, toko VARCHAR, id VARCHAR, produk VARCHAR, status VARCHAR, kanal VARCHAR,
            gmv BIGINT, qty BIGINT, impresi BIGINT, views BIGINT, unique_views BIGINT, unique_buyers BIGINT,
            ctr DOUBLE, cr DOUBLE, gmv_total BIGINT, qty_total BIGINT, pesanan_total BIGINT,
            snapshot_ts VARCHAR, snapshot_date DATE, run_id VARCHAR, row_hash_raw VARCHAR, row_hash_clean VARCHAR
        );
    """)

    # Read File SQL
    root_dir = Path(__file__).resolve().parents[3]
    sql_path = root_dir / "sql" / "silver_merge_tt_produk.sql"

    sql_content = sql_path.read_text(encoding="utf-8")
    sql_executable = _transpile_bq_to_duckdb(sql_content)

    # Eksekusi MERGE
    try:
        con.sql(sql_executable)
        print("✅ MERGE SQL Execution Success!")
    except Exception as e:
        print(f"❌ Error saat eksekusi SQL: {e}")

    con.sql("SELECT * FROM SILVER_DB.silver_tt_produk LIMIT 3").show()
    con.close()