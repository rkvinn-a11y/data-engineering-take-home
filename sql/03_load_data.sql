-- ============================================================
-- STEP 3: Load data from Parquet stage into tables
--
-- Column mapping (Parquet field -> table column):
--   usage_events   : sid->sid, pid->pid, tech->tech  (direct match)
--   profile_install: pid->profile, asset_id->sim, beg_dttm->start_ts,
--                    end_dttm->end_ts, src_cd->src, crt_dttm->crtd_dttm
--   sim_card_plan  : direct match for all columns
--   rate_card      : direct match for all columns
--
-- evt_dttm / ld_dttm / beg_dttm / end_dttm / eff_dttm / x_dttm /
-- upd_dttm / crt_dttm in the Parquet files are Unix epoch nanoseconds.
-- Dividing by 1,000,000,000 converts them to seconds before TIMESTAMP cast.
-- ============================================================


-- ------------------------------------------------------------
-- Load RATE_CARD
-- ------------------------------------------------------------
COPY INTO RATE_CARD (
    bundle_id, cc1, cc2, tech_cd, beg_dttm, end_dttm,
    rt_amt, curr_cd, prio_nbr
)
FROM (
    SELECT
        $1:bundle_id::NUMBER,
        $1:cc1::VARCHAR,
        $1:cc2::VARCHAR,
        $1:tech_cd::VARCHAR,
        TO_DATE(TO_TIMESTAMP($1:beg_dttm::NUMBER / 1000000000)),
        TO_DATE(TO_TIMESTAMP($1:end_dttm::NUMBER / 1000000000)),
        $1:rt_amt::REAL,
        $1:curr_cd::VARCHAR,
        $1:prio_nbr::NUMBER
    FROM @my_parquet_stage/rate_card.parquet
)
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_ff');

SELECT COUNT(*) AS rate_card_rows FROM RATE_CARD;


-- ------------------------------------------------------------
-- Load SIM_CARD_PLAN_HISTORY
-- ------------------------------------------------------------
COPY INTO SIM_CARD_PLAN_HISTORY (
    asset_id, bundle_id, eff_dttm, x_dttm, why_cd, upd_dttm
)
FROM (
    SELECT
        $1:asset_id::VARCHAR,
        $1:bundle_id::NUMBER,
        TO_TIMESTAMP($1:eff_dttm::NUMBER  / 1000000000),
        TO_TIMESTAMP($1:x_dttm::NUMBER    / 1000000000),
        $1:why_cd::VARCHAR,
        TO_TIMESTAMP($1:upd_dttm::NUMBER  / 1000000000)
    FROM @my_parquet_stage/sim_card_plan_history.parquet
)
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_ff');

SELECT COUNT(*) AS sim_plan_rows FROM SIM_CARD_PLAN_HISTORY;


-- ------------------------------------------------------------
-- Load PROFILE_INSTALLATION
-- Parquet field -> table column:
--   pid        -> profile
--   asset_id   -> sim
--   beg_dttm   -> start_ts
--   end_dttm   -> end_ts
--   src_cd     -> src
--   crt_dttm   -> crtd_dttm  (row_created_at removed)
-- ------------------------------------------------------------
COPY INTO PROFILE_INSTALLATION (
    profile, sim, start_ts, end_ts, src, crtd_dttm
)
FROM (
    SELECT
        $1:pid::VARCHAR,
        $1:asset_id::VARCHAR,
        TO_TIMESTAMP($1:beg_dttm::NUMBER / 1000000000),
        TO_TIMESTAMP($1:end_dttm::NUMBER / 1000000000),
        $1:src_cd::VARCHAR,
        TO_TIMESTAMP($1:crt_dttm::NUMBER / 1000000000)
    FROM @my_parquet_stage/profile_installation.parquet
)
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_ff');

SELECT COUNT(*) AS profile_rows FROM PROFILE_INSTALLATION;


-- ------------------------------------------------------------
-- Load USAGE_EVENTS
-- (event_id is AUTOINCREMENT -- omit from column list)
-- Parquet field -> table column: direct match on all columns
-- ------------------------------------------------------------
COPY INTO USAGE_EVENTS (
    sid, pid, evt_dttm, mb, cc1, cc2, tech, apn_nm, src_nm, ld_dttm
)
FROM (
    SELECT
        $1:sid::VARCHAR,
        $1:pid::VARCHAR,
        TO_TIMESTAMP($1:evt_dttm::NUMBER / 1000000000),
        $1:mb::REAL,
        $1:cc1::VARCHAR,
        $1:cc2::VARCHAR,
        $1:tech::VARCHAR,
        $1:apn_nm::VARCHAR,
        $1:src_nm::VARCHAR,
        TO_TIMESTAMP($1:ld_dttm::NUMBER  / 1000000000)
    FROM @my_parquet_stage/usage_events.parquet
)
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_ff');

SELECT COUNT(*) AS usage_event_rows FROM USAGE_EVENTS;
