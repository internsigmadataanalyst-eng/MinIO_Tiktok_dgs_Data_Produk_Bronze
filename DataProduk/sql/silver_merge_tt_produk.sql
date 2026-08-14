-- MERGE `database-sigma.SILVER_DB.silver_tt_produk` T
-- USING (
--   WITH latest_raw AS (
--     SELECT * EXCEPT(rn)
--     FROM (
--       SELECT
--         b.*,
--         UPPER(TRIM(b.id)) AS id_norm,
--         COALESCE(SAFE_CAST(DATE(b.tanggal) AS DATE), b.snapshot_date) AS business_date,
--         ROW_NUMBER() OVER (
--           PARTITION BY UPPER(TRIM(b.id)), COALESCE(SAFE_CAST(DATE(b.tanggal) AS DATE), b.snapshot_date)
--           ORDER BY b.snapshot_ts DESC, b.run_id DESC
--         ) AS rn
--       FROM `database-sigma.BRONZE_DB.bronze_produk` b
--     )
--     WHERE rn = 1
--   ),
--   base AS (
--     SELECT
--       business_date                          AS tanggal,
--       UPPER(TRIM(toko))                      AS toko,
--       id_norm                                AS id,
--       UPPER(TRIM(produk))                    AS produk,
--       UPPER(TRIM(status))                    AS status,

--       snapshot_ts, snapshot_date, run_id, row_hash_raw,

--       SAFE_CAST(gmv AS INT64)                AS gmv_total,
--       SAFE_CAST(produk_terjual AS INT64)     AS qty_total,
--       SAFE_CAST(pesanan AS INT64)            AS pesanan_total,

--       STRUCT(
--         SAFE_CAST(gmv_dari_shop_tab AS INT64)                                   AS gmv,
--         SAFE_CAST(produk_terjual_dari_tab_shop AS INT64)                        AS qty,
--         SAFE_CAST(impresi_daftar_produk_shop_tab AS INT64)                      AS impresi,
--         SAFE_CAST(tayangan_halaman_shop_tab AS INT64)                            AS views,
--         SAFE_CAST(tayangan_halaman_unik_shop_tab AS INT64)                      AS unique_views,
--         SAFE_CAST(pembeli_produk_unik_shop_tab AS INT64)                        AS unique_buyers,
--         SAFE_CAST(REGEXP_REPLACE(rasio_kliktayang_shop_tab, r'[%\s]', '') AS FLOAT64)/100 AS ctr,
--         SAFE_CAST(REGEXP_REPLACE(persentase_konversi_shop_tab, r'[%\s]', '') AS FLOAT64)/100 AS cr
--       ) AS shop,

--       STRUCT(
--         SAFE_CAST(gmv_dari_live AS INT64)                                       AS gmv,
--         SAFE_CAST(produk_terjual_dari_live AS INT64)                            AS qty,
--         SAFE_CAST(impresi_dari_live AS INT64)                                   AS impresi,
--         SAFE_CAST(tayangan_halaman_dari_live AS INT64)                           AS views,
--         SAFE_CAST(tayangan_halaman_unik_dari_live AS INT64)                     AS unique_views,
--         SAFE_CAST(pembeli_produk_unik_dari_live AS INT64)                       AS unique_buyers,
--         SAFE_CAST(REGEXP_REPLACE(rasio_kliktayang_dari_live, r'[%\s]', '') AS FLOAT64)/100 AS ctr,
--         SAFE_CAST(REGEXP_REPLACE(persentase_konversi_dari_live, r'[%\s]', '') AS FLOAT64)/100 AS cr
--       ) AS live,

--       STRUCT(
--         SAFE_CAST(gmv_dari_video AS INT64)                                      AS gmv,
--         SAFE_CAST(produk_terjual_dari_video AS INT64)                           AS qty,
--         SAFE_CAST(impresi_dari_video AS INT64)                                  AS impresi,
--         SAFE_CAST(tayangan_halaman_dari_video AS INT64)                          AS views,
--         SAFE_CAST(tayangan_halaman_unik_dari_video AS INT64)                    AS unique_views,
--         SAFE_CAST(pembeli_produk_unik_dari_video AS INT64)                      AS unique_buyers,
--         SAFE_CAST(REGEXP_REPLACE(rasio_kliktayang_dari_video, r'[%\s]', '') AS FLOAT64)/100 AS ctr,
--         SAFE_CAST(REGEXP_REPLACE(persentase_konversi_dari_video, r'[%\s]', '') AS FLOAT64)/100 AS cr
--       ) AS video,

--       STRUCT(
--         SAFE_CAST(gmv_dari_kartu_produk AS INT64)                                AS gmv,
--         SAFE_CAST(produk_terjual_dari_kartu_produk AS INT64)                    AS qty,
--         SAFE_CAST(impresi_dari_kartu_produk AS INT64)                           AS impresi,
--         SAFE_CAST(tayangan_halaman_dari_kartu_produk AS INT64)                   AS views,
--         SAFE_CAST(tayangan_halaman_unik_dari_kartu_produk AS INT64)             AS unique_views,
--         SAFE_CAST(pembeli_unik_dari_kartu_produk AS INT64)                      AS unique_buyers,
--         SAFE_CAST(REGEXP_REPLACE(rasio_kliktayang_dari_kartu_produk, r'[%\s]', '') AS FLOAT64)/100 AS ctr,
--         SAFE_CAST(REGEXP_REPLACE(persentase_konversi_dari_kartu_produk, r'[%\s]', '') AS FLOAT64)/100 AS cr
--       ) AS card
--     FROM latest_raw
--   ),

--   longed AS (
--     SELECT
--       tanggal, toko, id, produk, status,
--       x.kanal,
--       x.gmv, x.qty, x.impresi, x.views, x.unique_views, x.unique_buyers, x.ctr, x.cr,
--       gmv_total, qty_total, pesanan_total,
--       snapshot_ts, snapshot_date, run_id, row_hash_raw,
--       TO_HEX(SHA256(ARRAY_TO_STRING([
--         FORMAT_DATE('%F', tanggal), toko, id, produk, status, x.kanal,
--         CAST(x.gmv AS STRING), CAST(x.qty AS STRING), CAST(x.impresi AS STRING),
--         CAST(x.views AS STRING), CAST(x.unique_views AS STRING), CAST(x.unique_buyers AS STRING),
--         CAST(x.ctr AS STRING), CAST(x.cr AS STRING),
--         CAST(gmv_total AS STRING), CAST(qty_total AS STRING), CAST(pesanan_total AS STRING)
--       ], '||'))) AS row_hash_clean
--     FROM base,
--     UNNEST([
--       STRUCT('SHOP'  AS kanal, shop.gmv  AS gmv, shop.qty  AS qty, shop.impresi  AS impresi, shop.views  AS views, shop.unique_views  AS unique_views, shop.unique_buyers  AS unique_buyers, shop.ctr  AS ctr, shop.cr  AS cr),
--       STRUCT('LIVE'  AS kanal, live.gmv  AS gmv, live.qty  AS qty, live.impresi  AS impresi, live.views  AS views, live.unique_views  AS unique_views, live.unique_buyers  AS unique_buyers, live.ctr  AS ctr, live.cr  AS cr),
--       STRUCT('VIDEO' AS kanal, video.gmv AS gmv, video.qty AS qty, video.impresi AS impresi, video.views AS views, video.unique_views AS unique_views, video.unique_buyers AS unique_buyers, video.ctr AS ctr, video.cr AS cr),
--       STRUCT('CARD'  AS kanal, card.gmv  AS gmv, card.qty  AS qty, card.impresi  AS impresi, card.views  AS views, card.unique_views  AS unique_views, card.unique_buyers  AS unique_buyers, card.ctr  AS ctr, card.cr  AS cr)
--     ]) AS x
--   )
--   SELECT * FROM longed
-- ) S
-- ON  T.tanggal = S.tanggal
-- AND T.id      = S.id
-- AND T.kanal   = S.kanal
-- WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
--   UPDATE SET
--     toko = S.toko, 
--     produk = S.produk, 
--     status = S.status,
--     gmv = S.gmv, 
--     qty = S.qty, 
--     impresi = S.impresi, 
--     views = S.views,
--     unique_views = S.unique_views, 
--     unique_buyers = S.unique_buyers,
--     ctr = S.ctr, 
--     cr = S.cr,
--     gmv_total = S.gmv_total, 
--     qty_total = S.qty_total, 
--     pesanan_total = S.pesanan_total,
--     snapshot_ts = S.snapshot_ts, 
--     snapshot_date = S.snapshot_date, 
--     run_id = S.run_id,
--     row_hash_raw = S.row_hash_raw, 
--     row_hash_clean = S.row_hash_clean
-- WHEN NOT MATCHED THEN
--   INSERT (
--     tanggal, toko, id, produk, status, kanal,
--     gmv, qty, impresi, views, unique_views, unique_buyers, ctr, cr,
--     gmv_total, qty_total, pesanan_total,
--     snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
--   )
--   VALUES (
--     S.tanggal, S.toko, S.id, S.produk, S.status, S.kanal,
--     S.gmv, S.qty, S.impresi, S.views, S.unique_views, S.unique_buyers, S.ctr, S.cr,
--     S.gmv_total, S.qty_total, S.pesanan_total,
--     S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
--   );





MERGE INTO SILVER_DB.silver_tt_produk T
USING (
  WITH latest_raw AS (
    SELECT * EXCLUDE (rn)
    FROM (
      SELECT
        b.*,
        UPPER(TRIM(b.id)) AS id_norm,
        COALESCE(TRY_CAST(TRY_CAST(b.tanggal AS DATE) AS DATE), b.snapshot_date) AS business_date,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(b.id)), COALESCE(TRY_CAST(TRY_CAST(b.tanggal AS DATE) AS DATE), b.snapshot_date)
          ORDER BY b.snapshot_ts DESC, b.run_id DESC
        ) AS rn
      FROM BRONZE_DB.bronze_produk b
    )
    WHERE rn = 1
  ),
  base AS (
    SELECT
      business_date                          AS tanggal,
      UPPER(TRIM(toko))                      AS toko,
      id_norm                                AS id,
      UPPER(TRIM(produk))                    AS produk,
      UPPER(TRIM(status))                    AS status,

      snapshot_ts, snapshot_date, run_id, row_hash_raw,

      TRY_CAST(gmv AS BIGINT)                AS gmv_total,
      TRY_CAST(produk_terjual AS BIGINT)     AS qty_total,
      TRY_CAST(pesanan AS BIGINT)            AS pesanan_total,

      {
        'gmv': TRY_CAST(gmv_dari_shop_tab AS BIGINT),
        'qty': TRY_CAST(produk_terjual_dari_tab_shop AS BIGINT),
        'impresi': TRY_CAST(impresi_daftar_produk_shop_tab AS BIGINT),
        'views': TRY_CAST(tayangan_halaman_shop_tab AS BIGINT),
        'unique_views': TRY_CAST(tayangan_halaman_unik_shop_tab AS BIGINT),
        'unique_buyers': TRY_CAST(pembeli_produk_unik_shop_tab AS BIGINT),
        'ctr': TRY_CAST(REGEXP_REPLACE(rasio_kliktayang_shop_tab, '[%\s]', '') AS DOUBLE)/100,
        'cr': TRY_CAST(REGEXP_REPLACE(persentase_konversi_shop_tab, '[%\s]', '') AS DOUBLE)/100
      } AS shop,

      {
        'gmv': TRY_CAST(gmv_dari_live AS BIGINT),
        'qty': TRY_CAST(produk_terjual_dari_live AS BIGINT),
        'impresi': TRY_CAST(impresi_dari_live AS BIGINT),
        'views': TRY_CAST(tayangan_halaman_dari_live AS BIGINT),
        'unique_views': TRY_CAST(tayangan_halaman_unik_dari_live AS BIGINT),
        'unique_buyers': TRY_CAST(pembeli_produk_unik_dari_live AS BIGINT),
        'ctr': TRY_CAST(REGEXP_REPLACE(rasio_kliktayang_dari_live, '[%\s]', '') AS DOUBLE)/100,
        'cr': TRY_CAST(REGEXP_REPLACE(persentase_konversi_dari_live, '[%\s]', '') AS DOUBLE)/100
      } AS live,

      {
        'gmv': TRY_CAST(gmv_dari_video AS BIGINT),
        'qty': TRY_CAST(produk_terjual_dari_video AS BIGINT),
        'impresi': TRY_CAST(impresi_dari_video AS BIGINT),
        'views': TRY_CAST(tayangan_halaman_dari_video AS BIGINT),
        'unique_views': TRY_CAST(tayangan_halaman_unik_dari_video AS BIGINT),
        'unique_buyers': TRY_CAST(pembeli_produk_unik_dari_video AS BIGINT),
        'ctr': TRY_CAST(REGEXP_REPLACE(rasio_kliktayang_dari_video, '[%\s]', '') AS DOUBLE)/100,
        'cr': TRY_CAST(REGEXP_REPLACE(persentase_konversi_dari_video, '[%\s]', '') AS DOUBLE)/100
      } AS video,

      {
        'gmv': TRY_CAST(gmv_dari_kartu_produk AS BIGINT),
        'qty': TRY_CAST(produk_terjual_dari_kartu_produk AS BIGINT),
        'impresi': TRY_CAST(impresi_dari_kartu_produk AS BIGINT),
        'views': TRY_CAST(tayangan_halaman_dari_kartu_produk AS BIGINT),
        'unique_views': TRY_CAST(tayangan_halaman_unik_dari_kartu_produk AS BIGINT),
        'unique_buyers': TRY_CAST(pembeli_unik_dari_kartu_produk AS BIGINT),
        'ctr': TRY_CAST(REGEXP_REPLACE(rasio_kliktayang_dari_kartu_produk, '[%\s]', '') AS DOUBLE)/100,
        'cr': TRY_CAST(REGEXP_REPLACE(persentase_konversi_dari_kartu_produk, '[%\s]', '') AS DOUBLE)/100
      } AS card
    FROM latest_raw
  ),

  longed AS (
    SELECT
      tanggal, toko, id, produk, status,
      x.u.kanal          AS kanal,
      x.u.gmv            AS gmv,
      x.u.qty            AS qty,
      x.u.impresi        AS impresi,
      x.u.views          AS views,
      x.u.unique_views   AS unique_views,
      x.u.unique_buyers  AS unique_buyers,
      x.u.ctr            AS ctr,
      x.u.cr             AS cr,
      gmv_total, qty_total, pesanan_total,
      snapshot_ts, snapshot_date, run_id, row_hash_raw,
      md5(concat_ws('||',
        STRFTIME(tanggal, '%Y-%m-%d'), toko, id, produk, status, x.u.kanal,
        CAST(x.u.gmv AS VARCHAR), CAST(x.u.qty AS VARCHAR), CAST(x.u.impresi AS VARCHAR),
        CAST(x.u.views AS VARCHAR), CAST(x.u.unique_views AS VARCHAR), CAST(x.u.unique_buyers AS VARCHAR),
        CAST(x.u.ctr AS VARCHAR), CAST(x.u.cr AS VARCHAR),
        CAST(gmv_total AS VARCHAR), CAST(qty_total AS VARCHAR), CAST(pesanan_total AS VARCHAR)
      )) AS row_hash_clean
    FROM base,
    (
      SELECT UNNEST([
        struct_pack(kanal := 'SHOP',  gmv := shop.gmv,  qty := shop.qty,  impresi := shop.impresi,  views := shop.views,  unique_views := shop.unique_views,  unique_buyers := shop.unique_buyers,  ctr := shop.ctr,  cr := shop.cr),
        struct_pack(kanal := 'LIVE',  gmv := live.gmv,  qty := live.qty,  impresi := live.impresi,  views := live.views,  unique_views := live.unique_views,  unique_buyers := live.unique_buyers,  ctr := live.ctr,  cr := live.cr),
        struct_pack(kanal := 'VIDEO', gmv := video.gmv, qty := video.qty, impresi := video.impresi, views := video.views, unique_views := video.unique_views, unique_buyers := video.unique_buyers, ctr := video.ctr, cr := video.cr),
        struct_pack(kanal := 'CARD',  gmv := card.gmv,  qty := card.qty,  impresi := card.impresi,  views := card.views,  unique_views := card.unique_views,  unique_buyers := card.unique_buyers,  ctr := card.ctr,  cr := card.cr)
      ]) AS u
    ) AS x
  )
  SELECT * FROM longed
) S
ON  T.tanggal = S.tanggal
AND T.id      = S.id
AND T.kanal   = S.kanal
WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    toko = S.toko, 
    produk = S.produk, 
    status = S.status,
    gmv = S.gmv, 
    qty = S.qty, 
    impresi = S.impresi, 
    views = S.views,
    unique_views = S.unique_views, 
    unique_buyers = S.unique_buyers,
    ctr = S.ctr, 
    cr = S.cr,
    gmv_total = S.gmv_total, 
    qty_total = S.qty_total, 
    pesanan_total = S.pesanan_total,
    snapshot_ts = S.snapshot_ts, 
    snapshot_date = S.snapshot_date, 
    run_id = S.run_id,
    row_hash_raw = S.row_hash_raw, 
    row_hash_clean = S.row_hash_clean
WHEN NOT MATCHED THEN
  INSERT (
    tanggal, toko, id, produk, status, kanal,
    gmv, qty, impresi, views, unique_views, unique_buyers, ctr, cr,
    gmv_total, qty_total, pesanan_total,
    snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
  )
  VALUES (
    S.tanggal, S.toko, S.id, S.produk, S.status, S.kanal,
    S.gmv, S.qty, S.impresi, S.views, S.unique_views, S.unique_buyers, S.ctr, S.cr,
    S.gmv_total, S.qty_total, S.pesanan_total,
    S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
  );