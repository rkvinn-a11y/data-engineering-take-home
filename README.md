# Telecom Usage Data Engineering Assessment

## Project Overview

End-to-end analysis of telecom usage data stored as Parquet files in a Snowflake internal stage.  
All column names in the scripts match the ERD diagram exactly.

---

## Repository Structure

```
telecom-analysis/
├── README.md
├── sql/
│   ├── 01_setup.sql          # File format and stage creation
│   ├── 02_create_tables.sql  # Table schemas with PKs, FKs, indices
│   ├── 03_load_data.sql      # COPY INTO from Parquet stage
│   └── 04_analysis.sql       # Four analytical questions
├── python/
│   └── line_chart.py         # Daily total usage (MB) line chart
└── docs/
    └── erd_analysis.md       # ERD review, redesign, data quality notes
```

---

## Setup & Reproduction

### Prerequisites
- Snowflake account with a database and schema
- Parquet source files uploaded to a Snowflake internal stage:
  - `usage_events.parquet`
  - `profile_installation.parquet`
  - `rate_card.parquet`
  - `sim_card_plan_history.parquet`
- Python 3.8+ with `snowflake-snowpark-python`, `pandas`, `matplotlib`

### Steps

```bash
# Run SQL scripts in order in a Snowflake worksheet or SnowSQL
snowsql -f sql/01_setup.sql
snowsql -f sql/02_create_tables.sql
snowsql -f sql/03_load_data.sql
snowsql -f sql/04_analysis.sql

# Generate the line chart (run inside a Snowflake Notebook or Snowpark env)
python python/line_chart.py
```

> Update the database, schema, and stage path in each script to match your environment.

---

## ERD Column Names Used

| Table | Key Columns (ERD names) |
|---|---|
| `usage_events` | `sid`, `pid`, `evt_dttm`, `mb`, `cc1`, `cc2`, `tech`, `apn_nm`, `src_nm`, `ld_dttm` |
| `profile_installation` | `profile` (PK), `sim` (FK), `start_ts`, `end_ts`, `src` |
| `sim_card_plan_history` | `asset_id` (PK), `bundle_id` (FK), `eff_dttm`, `x_dttm`, `why_cd`, `upd_dttm` |
| `rate_card` | `bundle_id` (PK), `cc1`, `cc2`, `tech_cd`, `beg_dttm`, `end_dttm`, `rt_amt`, `curr_cd`, `prio_nbr` |

**Additions on top of the ERD:**
- `event_id` surrogate PK added to `usage_events` (no natural PK existed)
- `row_created_at` removed from `profile_installation`; replaced by `crtd_dttm` / `upd_dttm`
- `crtd_dttm` / `upd_dttm` added to all four tables

---

## Data Questions — Answers

### Q1 — Top SIM Card by Usage
```sql
SELECT sid, SUM(mb) AS total_mb
FROM USAGE_EVENTS
GROUP BY sid
ORDER BY total_mb DESC LIMIT 1;
```

### Q2 — Total 3G Events
```sql
SELECT COUNT(*) AS total_3g_events
FROM USAGE_EVENTS
WHERE LOWER(tech) = '3g';
```

### Q3 — Duplicate Records
```sql
SELECT sid, evt_dttm, mb, COUNT(*) AS duplicate_count
FROM USAGE_EVENTS
GROUP BY sid, evt_dttm, mb
HAVING COUNT(*) > 1;
```

### Q4 — Total Cost
```sql
SELECT SUM(u.mb * r.rt_amt) AS total_cost
FROM USAGE_EVENTS u
JOIN PROFILE_INSTALLATION pi ON u.pid = pi.profile
JOIN SIM_CARD_PLAN_HISTORY s ON pi.sim = s.asset_id
    AND u.evt_dttm BETWEEN s.eff_dttm AND s.x_dttm
JOIN RATE_CARD r ON s.bundle_id = r.bundle_id;
```

---

## Key Assumptions

1. `evt_dttm`, `ld_dttm`, and all other timestamp fields in the Parquet files are Unix epoch **nanoseconds** → divided by `1,000,000,000` before `TO_TIMESTAMP()`.
2. Join: `usage_events.pid = profile_installation.profile`
3. Join: `profile_installation.sim = sim_card_plan_history.asset_id`
4. `sid` / `pid` stored as VARCHAR in the table despite ERD showing int, to allow joins to str-typed parent columns.
5. `tech` values are lowercase (`3g`, `4g`, etc.) — `LOWER()` applied defensively.
6. Duplicate detection key: `(sid, evt_dttm, mb)`.

See [`docs/erd_analysis.md`](docs/erd_analysis.md) for full ERD review, redesign proposal, risks, indices, and data quality issues.
