-- ============================================================
-- STEP 2: Create tables using column names from the ERD
--
-- Changes applied on top of the original ERD:
--   - Explicit PRIMARY KEY on all tables
--   - Foreign key constraints declared
--   - event_id surrogate PK added to usage_events (no natural PK in ERD)
--   - row_created_at removed from profile_installation;
--     replaced by crtd_dttm / upd_dttm (added consistently to all tables)
--   - asset_id stored as VARCHAR (not int as shown in ERD) to support
--     the join to profile_installation.sim which is str — an int/str
--     join would fail at runtime
-- ============================================================


-- ------------------------------------------------------------
-- RATE_CARD
-- PK: bundle_id
-- ERD columns: bundle_id, cc1, cc2, tech_cd, beg_dttm, end_dttm,
--              rt_amt, curr_cd, prio_nbr
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE RATE_CARD (
    bundle_id   NUMBER(38, 0)   NOT NULL,
    cc1         VARCHAR,
    cc2         VARCHAR,
    tech_cd     VARCHAR,
    beg_dttm    DATE,
    end_dttm    DATE,
    rt_amt      REAL,
    curr_cd     VARCHAR,
    prio_nbr    NUMBER(38, 0),
    crtd_dttm   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    upd_dttm    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_rate_card PRIMARY KEY (bundle_id)
);


-- ------------------------------------------------------------
-- SIM_CARD_PLAN_HISTORY
-- PK: asset_id
-- FK: bundle_id -> RATE_CARD.bundle_id
-- ERD columns: asset_id, bundle_id, eff_dttm, x_dttm, why_cd, upd_dttm
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE SIM_CARD_PLAN_HISTORY (
    asset_id    VARCHAR         NOT NULL,
    bundle_id   NUMBER(38, 0)   NOT NULL,
    eff_dttm    TIMESTAMP,
    x_dttm      TIMESTAMP,
    why_cd      VARCHAR,
    upd_dttm    TIMESTAMP,
    crtd_dttm   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_sim_plan       PRIMARY KEY (asset_id),
    CONSTRAINT fk_sim_rate_card  FOREIGN KEY (bundle_id) REFERENCES RATE_CARD (bundle_id)
);


-- ------------------------------------------------------------
-- PROFILE_INSTALLATION
-- PK: profile
-- FK: sim -> SIM_CARD_PLAN_HISTORY.asset_id
-- ERD columns: profile, sim, start_ts, end_ts, src, row_created_at
--   row_created_at removed; replaced by crtd_dttm / upd_dttm
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE PROFILE_INSTALLATION (
    profile     VARCHAR         NOT NULL,
    sim         VARCHAR,
    start_ts    TIMESTAMP,
    end_ts      TIMESTAMP,
    src         VARCHAR,
    crtd_dttm   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    upd_dttm    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_profile_inst   PRIMARY KEY (profile),
    CONSTRAINT fk_profile_sim    FOREIGN KEY (sim) REFERENCES SIM_CARD_PLAN_HISTORY (asset_id)
);


-- ------------------------------------------------------------
-- USAGE_EVENTS
-- PK: event_id (surrogate -- no natural PK in ERD)
-- FK: pid -> PROFILE_INSTALLATION.profile
-- ERD columns: sid, pid, evt_dttm, mb, cc1, cc2, tech, apn_nm, src_nm, ld_dttm
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE USAGE_EVENTS (
    event_id    NUMBER(38, 0)   NOT NULL AUTOINCREMENT,
    sid         VARCHAR,
    pid         VARCHAR,
    evt_dttm    TIMESTAMP,
    mb          REAL,
    cc1         VARCHAR,
    cc2         VARCHAR,
    tech        VARCHAR,
    apn_nm      VARCHAR,
    src_nm      VARCHAR,
    ld_dttm     TIMESTAMP,
    crtd_dttm   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    upd_dttm    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_usage_events   PRIMARY KEY (event_id),
    CONSTRAINT fk_ue_profile     FOREIGN KEY (pid) REFERENCES PROFILE_INSTALLATION (profile)
);


-- ============================================================
-- Recommended indices
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_ue_pid        ON USAGE_EVENTS (pid);
CREATE INDEX IF NOT EXISTS idx_ue_sid        ON USAGE_EVENTS (sid);
CREATE INDEX IF NOT EXISTS idx_ue_evt_dttm   ON USAGE_EVENTS (evt_dttm);
CREATE INDEX IF NOT EXISTS idx_ue_tech       ON USAGE_EVENTS (tech);

CREATE INDEX IF NOT EXISTS idx_sim_bundle    ON SIM_CARD_PLAN_HISTORY (bundle_id);
CREATE INDEX IF NOT EXISTS idx_sim_eff       ON SIM_CARD_PLAN_HISTORY (eff_dttm, x_dttm);

CREATE INDEX IF NOT EXISTS idx_pi_sim        ON PROFILE_INSTALLATION (sim);
CREATE INDEX IF NOT EXISTS idx_pi_dates      ON PROFILE_INSTALLATION (start_ts, end_ts);

CREATE INDEX IF NOT EXISTS idx_rc_dates      ON RATE_CARD (beg_dttm, end_dttm);
