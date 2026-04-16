-- ============================================================
-- STEP 4: Analytical Queries
-- All column names match the ERD diagram exactly.
-- ============================================================


-- ------------------------------------------------------------
-- Q1: Top SIM card by total data usage (MB)
-- Column: sid (SIM identifier in usage_events per ERD)
-- ------------------------------------------------------------
SELECT
    sid,
    SUM(mb) AS total_mb
FROM USAGE_EVENTS
GROUP BY sid
ORDER BY total_mb DESC
LIMIT 1;


-- ------------------------------------------------------------
-- Q2: Total number of 3G events
-- Column: tech (ERD column name in usage_events, not tech_cd)
-- LOWER() guard against mixed-case source values.
-- ------------------------------------------------------------
SELECT
    COUNT(*) AS total_3g_events
FROM USAGE_EVENTS
WHERE LOWER(tech) = '3g';


-- ------------------------------------------------------------
-- Q3: Duplicate records
-- Natural duplicate key: (sid, evt_dttm, mb)
-- Returns each duplicate group and how many times it appears.
-- ------------------------------------------------------------
SELECT
    sid,
    evt_dttm,
    mb,
    COUNT(*) AS duplicate_count
FROM USAGE_EVENTS
GROUP BY
    sid,
    evt_dttm,
    mb
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Summary: total excess rows caused by duplicates
SELECT
    SUM(duplicate_count - 1) AS total_duplicate_rows,
    COUNT(*)                 AS distinct_duplicate_groups
FROM (
    SELECT sid, evt_dttm, mb, COUNT(*) AS duplicate_count
    FROM USAGE_EVENTS
    GROUP BY sid, evt_dttm, mb
    HAVING COUNT(*) > 1
) dupes;


-- ------------------------------------------------------------
-- Q4: Total cost of all usage
--
-- Join path (following ERD relationships):
--   usage_events.pid
--     -> profile_installation.profile
--     -> profile_installation.sim = sim_card_plan_history.asset_id
--     -> sim_card_plan_history.bundle_id = rate_card.bundle_id
--
-- Temporal filter: usage event must fall within the active plan window
--   (evt_dttm BETWEEN eff_dttm AND x_dttm)
-- ------------------------------------------------------------
SELECT
    SUM(u.mb * r.rt_amt) AS total_cost
FROM USAGE_EVENTS u
JOIN PROFILE_INSTALLATION pi
    ON  u.pid       = pi.profile
JOIN SIM_CARD_PLAN_HISTORY s
    ON  pi.sim      = s.asset_id
    AND u.evt_dttm BETWEEN s.eff_dttm AND s.x_dttm
JOIN RATE_CARD r
    ON  s.bundle_id = r.bundle_id;


-- ------------------------------------------------------------
-- Bonus: Cost breakdown by currency and technology
-- ------------------------------------------------------------
SELECT
    r.curr_cd,
    u.tech,
    COUNT(*)             AS event_count,
    SUM(u.mb)            AS total_mb,
    SUM(u.mb * r.rt_amt) AS total_cost
FROM USAGE_EVENTS u
JOIN PROFILE_INSTALLATION pi
    ON  u.pid       = pi.profile
JOIN SIM_CARD_PLAN_HISTORY s
    ON  pi.sim      = s.asset_id
    AND u.evt_dttm BETWEEN s.eff_dttm AND s.x_dttm
JOIN RATE_CARD r
    ON  s.bundle_id = r.bundle_id
GROUP BY r.curr_cd, u.tech
ORDER BY total_cost DESC;
