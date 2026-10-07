-- =====================================================================
-- Silver MERGE: bronze_produk -> silver_tt_produk (long per kanal)
-- Kanal: SHOP (irisan, non-additive), LIVE, VIDEO, AFFILIASI, CARD
-- =====================================================================

-- Batasi scan Bronze. NULL = scan semua. Contoh: DATE '2026-10-01'
DECLARE since_date DATE DEFAULT NULL;

-- 1) DDL: kolom baru di Silver (aman dijalankan berulang)
ALTER TABLE `database-sigma.SILVER_DB.silver_tt_produk`
  ADD COLUMN IF NOT EXISTS rentang_gmv STRING,
  ADD COLUMN IF NOT EXISTS is_additive BOOL;

-- 2) MERGE
MERGE `database-sigma.SILVER_DB.silver_tt_produk` T
USING (
  WITH latest_raw AS (
    -- snapshot terbaru per (id_norm, business_date)
    SELECT * EXCEPT(rn) FROM (
      SELECT
        b.*,
        UPPER(TRIM(b.id)) AS id_norm,
        COALESCE(SAFE_CAST(DATE(b.tanggal) AS DATE), b.snapshot_date) AS business_date,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(b.id)), COALESCE(SAFE_CAST(DATE(b.tanggal) AS DATE), b.snapshot_date)
          ORDER BY b.snapshot_ts DESC, b.run_id DESC
        ) AS rn
      FROM `database-sigma.BRONZE_DB.bronze_produk` b
      WHERE since_date IS NULL OR b.snapshot_date >= since_date
    )
    WHERE rn = 1
  ),

  long_raw AS (
    SELECT
      r.business_date                 AS tanggal,
      UPPER(TRIM(r.toko))             AS toko,
      r.id_norm                       AS id,
      UPPER(TRIM(r.produk))           AS produk,
      UPPER(TRIM(r.status))           AS status,
      NULLIF(TRIM(r.rentang_gmv), '-')   AS rentang_gmv,
      x.kanal, x.is_additive,
      x.gmv, x.qty, x.impresi, x.views, x.unique_views, x.unique_buyers,
      x.ctr, x.cr,
      SAFE_CAST(r.gmv AS INT64)            AS gmv_total,
      SAFE_CAST(r.produk_terjual AS INT64) AS qty_total,
      SAFE_CAST(r.pesanan AS INT64)        AS pesanan_total,
      r.snapshot_ts, r.snapshot_date, r.run_id, r.row_hash_raw
    FROM latest_raw r,
    UNNEST([
      -- SHOP: irisan titik masuk (tab Toko), BUKAN komponen penjumlah GMV
      STRUCT(
        'SHOP' AS kanal, FALSE AS is_additive,
        SAFE_CAST(r.gmv_dari_shop_tab AS INT64)                AS gmv,
        SAFE_CAST(r.produk_terjual_dari_tab_shop AS INT64)     AS qty,
        SAFE_CAST(r.impresi_daftar_produk_shop_tab AS INT64)   AS impresi,
        SAFE_CAST(r.tayangan_halaman_shop_tab AS INT64)        AS views,
        SAFE_CAST(r.tayangan_halaman_unik_shop_tab AS INT64)   AS unique_views,
        SAFE_CAST(r.pembeli_produk_unik_shop_tab AS INT64)     AS unique_buyers,
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.rasio_kliktayang_shop_tab, r'[%\s]', ''), ',', '.') AS FLOAT64)/100 AS ctr,
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.persentase_konversi_shop_tab, r'[%\s]', ''), ',', '.') AS FLOAT64)/100 AS cr
      ),
      -- LIVE
      STRUCT(
        'LIVE', TRUE,
        SAFE_CAST(r.gmv_dari_live AS INT64),
        SAFE_CAST(r.produk_terjual_dari_live AS INT64),
        SAFE_CAST(r.impresi_dari_live AS INT64),
        SAFE_CAST(r.tayangan_halaman_dari_live AS INT64),
        SAFE_CAST(r.tayangan_halaman_unik_dari_live AS INT64),
        SAFE_CAST(r.pembeli_produk_unik_dari_live AS INT64),
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.rasio_kliktayang_dari_live, r'[%\s]', ''), ',', '.') AS FLOAT64)/100,
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.persentase_konversi_dari_live, r'[%\s]', ''), ',', '.') AS FLOAT64)/100
      ),
      -- VIDEO
      STRUCT(
        'VIDEO', TRUE,
        SAFE_CAST(r.gmv_dari_video AS INT64),
        SAFE_CAST(r.produk_terjual_dari_video AS INT64),
        SAFE_CAST(r.impresi_dari_video AS INT64),
        SAFE_CAST(r.tayangan_halaman_dari_video AS INT64),
        SAFE_CAST(r.tayangan_halaman_unik_dari_video AS INT64),
        SAFE_CAST(r.pembeli_produk_unik_dari_video AS INT64),
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.rasio_kliktayang_dari_video, r'[%\s]', ''), ',', '.') AS FLOAT64)/100,
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.persentase_konversi_dari_video, r'[%\s]', ''), ',', '.') AS FLOAT64)/100
      ),
      -- AFFILIASI
      STRUCT(
        'AFFILIASI', TRUE,
        SAFE_CAST(r.gmv_dari_affiliasi AS INT64),
        SAFE_CAST(r.produk_terjual_dari_affiliasi AS INT64),
        SAFE_CAST(r.impresi_dari_affiliasi AS INT64),
        SAFE_CAST(r.tayangan_halaman_dari_affiliasi AS INT64),
        SAFE_CAST(r.tayangan_halaman_unik_dari_affiliasi AS INT64),
        SAFE_CAST(r.pembeli_unik_dari_affiliasi AS INT64),
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.rasio_kliktayang_dari_affiliasi, r'[%\s]', ''), ',', '.') AS FLOAT64)/100,
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.persentase_konversi_dari_affiliasi, r'[%\s]', ''), ',', '.') AS FLOAT64)/100
      ),
      -- CARD
      STRUCT(
        'CARD', TRUE,
        SAFE_CAST(r.gmv_dari_kartu_produk AS INT64),
        SAFE_CAST(r.produk_terjual_dari_kartu_produk AS INT64),
        SAFE_CAST(r.impresi_dari_kartu_produk AS INT64),
        SAFE_CAST(r.tayangan_halaman_dari_kartu_produk AS INT64),
        SAFE_CAST(r.tayangan_halaman_unik_dari_kartu_produk AS INT64),
        SAFE_CAST(r.pembeli_unik_dari_kartu_produk AS INT64),
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.rasio_kliktayang_dari_kartu_produk, r'[%\s]', ''), ',', '.') AS FLOAT64)/100,
        SAFE_CAST(REPLACE(REGEXP_REPLACE(r.persentase_konversi_dari_kartu_produk, r'[%\s]', ''), ',', '.') AS FLOAT64)/100
      )
    ]) AS x
  )

  SELECT
    *,
    TO_HEX(SHA256(ARRAY_TO_STRING([
      FORMAT_DATE('%F', tanggal), toko, id, produk, status, rentang_gmv, kanal,
      CAST(gmv AS STRING), CAST(qty AS STRING), CAST(impresi AS STRING),
      CAST(views AS STRING), CAST(unique_views AS STRING), CAST(unique_buyers AS STRING),
      CAST(ctr AS STRING), CAST(cr AS STRING),
      CAST(gmv_total AS STRING), CAST(qty_total AS STRING), CAST(pesanan_total AS STRING)
    ], '||', ''))) AS row_hash_clean
  FROM long_raw
) S
ON  T.tanggal = S.tanggal
AND T.id      = S.id
AND T.kanal   = S.kanal

WHEN MATCHED AND (
       T.row_hash_clean IS DISTINCT FROM S.row_hash_clean
    OR T.is_additive    IS DISTINCT FROM S.is_additive   -- backfill baris lama
) THEN
  UPDATE SET
    toko = S.toko, produk = S.produk, status = S.status,
    rentang_gmv = S.rentang_gmv, is_additive = S.is_additive,
    gmv = S.gmv, qty = S.qty, impresi = S.impresi, views = S.views,
    unique_views = S.unique_views, unique_buyers = S.unique_buyers,
    ctr = S.ctr, cr = S.cr,
    gmv_total = S.gmv_total, qty_total = S.qty_total, pesanan_total = S.pesanan_total,
    snapshot_ts = S.snapshot_ts, snapshot_date = S.snapshot_date, run_id = S.run_id,
    row_hash_raw = S.row_hash_raw, row_hash_clean = S.row_hash_clean

WHEN NOT MATCHED THEN
  INSERT (tanggal, id, kanal,
          toko, produk, status, rentang_gmv, is_additive,
          gmv, qty, impresi, views, unique_views, unique_buyers, ctr, cr,
          gmv_total, qty_total, pesanan_total,
          snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean)
  VALUES (S.tanggal, S.id, S.kanal,
          S.toko, S.produk, S.status, S.rentang_gmv, S.is_additive,
          S.gmv, S.qty, S.impresi, S.views, S.unique_views, S.unique_buyers, S.ctr, S.cr,
          S.gmv_total, S.qty_total, S.pesanan_total,
          S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean);