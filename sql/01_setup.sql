-- ============================================================
-- STEP 1: Create Parquet file format
-- ============================================================
CREATE OR REPLACE FILE FORMAT my_parquet_ff
    TYPE = PARQUET;

-- ============================================================
-- STEP 2: Create internal stage for Parquet source files
-- Expected files:
--   usage_events.parquet
--   profile_installation.parquet
--   rate_card.parquet
--   sim_card_plan_history.parquet
-- ============================================================
CREATE OR REPLACE STAGE my_parquet_stage
    FILE_FORMAT = my_parquet_ff;

-- Verify stage contents after uploading files
LIST @my_parquet_stage;
