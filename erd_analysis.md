# ERD Review, Redesign Proposal & Data Quality Notes

> All column names in this document match the ERD diagram exactly.

---

## 1. Issues Identified in the Original ERD

### 1.1 Missing Primary Keys
No table had an explicitly labelled primary key in the original diagram.

| Table | Likely PK | Resolution |
|---|---|---|
| `usage_events` | None clear | Added `event_id` autoincrement surrogate PK |
| `rate_card` | `bundle_id` | Designated as PK |
| `sim_card_plan_history` | `asset_id` | Designated as PK |
| `profile_installation` | `profile` | Designated as PK |

### 1.2 Missing Foreign Key Labels
Crow's foot lines were drawn without labelling which columns participate in each join, making the join logic ambiguous.

| Relationship | Join Columns |
|---|---|
| `usage_events` → `profile_installation` | `usage_events.pid = profile_installation.profile` |
| `profile_installation` → `sim_card_plan_history` | `profile_installation.sim = sim_card_plan_history.asset_id` |
| `sim_card_plan_history` → `rate_card` | `sim_card_plan_history.bundle_id = rate_card.bundle_id` |

### 1.3 Data Type Mismatch Blocking Joins
| Table | Column | ERD Type | Required Type | Reason |
|---|---|---|---|---|
| `usage_events` | `sid` | int | VARCHAR | Must join to `asset_id` (str) via profile chain |
| `usage_events` | `pid` | int | VARCHAR | Must join to `profile_installation.profile` (str) |
| `sim_card_plan_history` | `asset_id` | int | VARCHAR | Must join to `profile_installation.sim` (str) |

Without these corrections the join `pid = profile` and `sim = asset_id` would produce no rows.

### 1.4 Inconsistent Audit Columns
`profile_installation` had `row_created_at` while other tables used no audit column at all.  
**Fix:** `row_created_at` removed and `crtd_dttm` / `upd_dttm` added to all four tables.

### 1.5 No Temporal Constraint Enforcement
`sim_card_plan_history` uses an SCD2 pattern (`eff_dttm` / `x_dttm`), but nothing prevents overlapping plan windows for the same `asset_id`, which would silently double-count costs in Q4.

---

## 2. Redesigned Schema (ERD Column Names)

```
RATE_CARD (bundle_id PK)
    |  1
    |< (one rate_card bundle -> many sim_card_plan_history records)
    ∞
SIM_CARD_PLAN_HISTORY (asset_id PK, bundle_id FK)
    |  1
    |< (one SIM asset -> many profile_installation records)
    ∞
PROFILE_INSTALLATION (profile PK, sim FK)
    |  1
    |< (one profile -> many usage_events)
    ∞
USAGE_EVENTS (event_id PK, pid FK)
```

### Table Definitions

#### RATE_CARD
| Column | Type | Constraint |
|---|---|---|
| bundle_id | NUMBER | PK |
| cc1 | VARCHAR | |
| cc2 | VARCHAR | |
| tech_cd | VARCHAR | |
| beg_dttm | DATE | |
| end_dttm | DATE | |
| rt_amt | REAL | |
| curr_cd | VARCHAR | |
| prio_nbr | NUMBER | |
| crtd_dttm | TIMESTAMP | DEFAULT NOW() |
| upd_dttm | TIMESTAMP | DEFAULT NOW() |

#### SIM_CARD_PLAN_HISTORY
| Column | Type | Constraint | Note |
|---|---|---|---|
| asset_id | VARCHAR | PK | ERD shows int; VARCHAR required for join to profile_installation.sim |
| bundle_id | NUMBER | FK -> RATE_CARD | |
| eff_dttm | TIMESTAMP | | SCD2 start |
| x_dttm | TIMESTAMP | | SCD2 end |
| why_cd | VARCHAR | | |
| upd_dttm | TIMESTAMP | | Existed in ERD |
| crtd_dttm | TIMESTAMP | DEFAULT NOW() | New |

#### PROFILE_INSTALLATION
| Column | Type | Constraint | Note |
|---|---|---|---|
| profile | VARCHAR | PK | ERD column name |
| sim | VARCHAR | FK -> SIM_CARD_PLAN_HISTORY.asset_id | ERD column name |
| start_ts | TIMESTAMP | | |
| end_ts | TIMESTAMP | | |
| src | VARCHAR | | |
| crtd_dttm | TIMESTAMP | DEFAULT NOW() | Replaces row_created_at |
| upd_dttm | TIMESTAMP | DEFAULT NOW() | New |

#### USAGE_EVENTS
| Column | Type | Constraint | Note |
|---|---|---|---|
| event_id | NUMBER | PK, AUTOINCREMENT | New surrogate key |
| sid | VARCHAR | | ERD: int; VARCHAR for join consistency |
| pid | VARCHAR | FK -> PROFILE_INSTALLATION.profile | ERD: int; VARCHAR for join |
| evt_dttm | TIMESTAMP | | |
| mb | REAL | | |
| cc1 | VARCHAR | | |
| cc2 | VARCHAR | | |
| tech | VARCHAR | | ERD column name (not tech_cd) |
| apn_nm | VARCHAR | | |
| src_nm | VARCHAR | | |
| ld_dttm | TIMESTAMP | | |
| crtd_dttm | TIMESTAMP | DEFAULT NOW() | New |
| upd_dttm | TIMESTAMP | DEFAULT NOW() | New |

---

## 3. Recommended Indices

| Table | Index Columns | Reason |
|---|---|---|
| `usage_events` | `pid` | Join to profile_installation |
| `usage_events` | `sid` | SIM-level aggregations (Q1) |
| `usage_events` | `evt_dttm` | Temporal range filtering |
| `usage_events` | `tech` | Filter by technology type (Q2) |
| `sim_card_plan_history` | `bundle_id` | Join to rate_card |
| `sim_card_plan_history` | `(eff_dttm, x_dttm)` | Temporal window lookups |
| `profile_installation` | `sim` | Join to sim_card_plan_history |
| `profile_installation` | `(start_ts, end_ts)` | Validity window lookups |
| `rate_card` | `(beg_dttm, end_dttm)` | Rate validity lookups |

---

## 4. Risks & Trade-offs

| Decision | Risk | Trade-off |
|---|---|---|
| `event_id` AUTOINCREMENT surrogate PK | Sequence dependency during parallel loads | Enables fast single-column PK; avoids composite key complexity |
| `asset_id` as VARCHAR (ERD shows int) | Slight storage overhead vs pure int | Required for join to `profile_installation.sim` (str); int/str join would fail at runtime |
| `pid` / `sid` as VARCHAR (ERD shows int) | Same as above | Required to join to `profile_installation.profile` (str) |
| SCD2 pattern without overlap constraint | Overlapping windows double-count cost | Snowflake has no native SCD2 non-overlap check; needs application-layer guard |
| No deduplication on load | Inflates all aggregations | Duplicates should be removed before loading or filtered in queries |

---

## 5. Data Quality Issues Found

1. **Type mismatches blocking all joins** — `pid`/`sid` in `usage_events` are int in the ERD but must be VARCHAR to join to `profile_installation.profile` and `sim_card_plan_history.asset_id` (both str). No join would succeed without this correction.
2. **Epoch timestamp encoding** — `evt_dttm`, `ld_dttm`, `eff_dttm`, `x_dttm`, `upd_dttm`, `beg_dttm`, `end_dttm`, `crt_dttm` in the Parquet files are Unix epoch **nanoseconds** (not seconds). Requires `÷ 1,000,000,000` before `TO_TIMESTAMP()`.
3. **Inconsistent audit columns** — `profile_installation` used `row_created_at`; removed and replaced with `crtd_dttm` / `upd_dttm` for consistency across all tables.
4. **Duplicate usage events** — `(sid, evt_dttm, mb)` combinations can appear multiple times, inflating aggregations in Q1, Q2, and Q4.
5. **No non-overlap guarantee on SCD2** — Multiple active records for the same `asset_id` within the same time window would silently inflate cost calculations in Q4.
6. **Nullable FK columns** — `pid` in `usage_events` could be NULL, producing events that can't be attributed to a profile or costed.
7. **`src_nm` provenance** — Column origin is documented as Parquet metadata but its exact meaning is undefined in the original model.

---

## 6. Clarifying Questions & Assumptions

### Questions
1. Should `sid` and `pid` in `usage_events` remain int, or will they always be string-valued? The ERD shows int but the parent tables store strings.
2. Can one `profile` be active on more than one `sim` at the same time?
3. Can one `asset_id` (SIM) have overlapping plan windows in `sim_card_plan_history`? `prio_nbr` in `rate_card` suggests this may be intentional.
4. Should duplicates in `usage_events` be removed on load or retained for audit?
5. What does `src_nm` represent — source system name, Parquet filename, or something else?

### Assumptions Made
1. Epoch values in Parquet are **nanoseconds** → divide by `1,000,000,000` for seconds.
2. `pid = profile` is the correct join between `usage_events` and `profile_installation`.
3. `sim = asset_id` is the correct join between `profile_installation` and `sim_card_plan_history`.
4. Cost join uses the temporal window: `evt_dttm BETWEEN eff_dttm AND x_dttm`.
5. Duplicate detection key is `(sid, evt_dttm, mb)`.
6. `tech` values are lowercase (e.g. `3g`) — `LOWER()` applied defensively in Q2.
