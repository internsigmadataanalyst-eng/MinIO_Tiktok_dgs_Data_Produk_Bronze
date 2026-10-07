-- =====================================================================
-- One-time schema reconciliation (idempotent, safe to run repeatedly).
-- Run manually BEFORE a pipeline run. Not part of the daily ETL.
--
-- Why: the daily loader uses to_gbq(if_exists="append"), which adds columns
-- the target table is missing but silently keeps columns the source has
-- dropped (new rows land as NULL there, with no error anywhere). These
-- ALTERs close the known gaps up front so silver never references a column
-- bronze does not have.
--
-- Observed failure without this step: the loader raised
-- "InvalidSchema ... Cannot add fields (field: <name>)" and the run aborted
-- before any rows were written, so this cannot be deferred to the loader.
-- =====================================================================

-- Production bronze: needed at cutover. The silver SQL reads rentang_gmv and
-- the 8 affiliasi columns; against a bronze table lacking them it fails with
-- "Unrecognized name" rather than producing NULLs.
ALTER TABLE `database-sigma.BRONZE_DB.bronze_produk`
  ADD COLUMN IF NOT EXISTS rentang_gmv STRING,
  ADD COLUMN IF NOT EXISTS gmv_dari_affiliasi INTEGER,
  ADD COLUMN IF NOT EXISTS produk_terjual_dari_affiliasi INTEGER,
  ADD COLUMN IF NOT EXISTS pembeli_unik_dari_affiliasi INTEGER,
  ADD COLUMN IF NOT EXISTS impresi_dari_affiliasi INTEGER,
  ADD COLUMN IF NOT EXISTS tayangan_halaman_dari_affiliasi INTEGER,
  ADD COLUMN IF NOT EXISTS rasio_kliktayang_dari_affiliasi STRING,
  ADD COLUMN IF NOT EXISTS tayangan_halaman_unik_dari_affiliasi INTEGER,
  ADD COLUMN IF NOT EXISTS persentase_konversi_dari_affiliasi STRING;
