"""
line_chart.py
-------------
Generates a line chart of total data usage (MB) per day.
Reads usage_events.parquet directly via Snowpark.
Column names match the ERD: evt_dttm (timestamp), mb (float).
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from snowflake.snowpark.context import get_active_session


# ── Session ───────────────────────────────────────────────────────────
session = get_active_session()
session.sql("USE DATABASE util_db").collect()
session.sql("USE SCHEMA public").collect()

STAGE_PATH = "@util_db.public.my_parquet_stage/"


# ── Load ──────────────────────────────────────────────────────────────
usage = (
    session.read
    .parquet(STAGE_PATH + "usage_events.parquet")
    .to_pandas()
)

# evt_dttm is stored as epoch nanoseconds in the Parquet source.
# Divide by 1e9 to convert to seconds before parsing.
usage["evt_dttm"] = pd.to_datetime(
    usage["evt_dttm"].astype(float) / 1_000_000_000, unit="s", utc=True
)
usage["event_date"] = usage["evt_dttm"].dt.date


# ── Aggregate by day ──────────────────────────────────────────────────
daily_usage = (
    usage
    .groupby("event_date")["mb"]   # mb = ERD column name
    .sum()
    .reset_index()
    .rename(columns={"mb": "total_mb"})
    .sort_values("event_date")
)


# ── Plot ──────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 5))

ax.plot(
    daily_usage["event_date"],
    daily_usage["total_mb"],
    linewidth=1.8,
    color="#2E75B6",
    marker="o",
    markersize=3,
    label="Total MB"
)
ax.fill_between(
    daily_usage["event_date"],
    daily_usage["total_mb"],
    alpha=0.12,
    color="#2E75B6"
)

ax.set_title("Total Data Usage (MB) per Day", fontsize=14, fontweight="bold", pad=14)
ax.set_xlabel("Date", fontsize=11)
ax.set_ylabel("Total Usage (MB)", fontsize=11)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
ax.xaxis.set_major_locator(mdates.AutoDateLocator())
plt.xticks(rotation=45, ha="right", fontsize=9)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.legend(fontsize=10)
plt.tight_layout()

plt.savefig("total_usage_per_day.png", dpi=150, bbox_inches="tight")
plt.show()
print("Chart saved to total_usage_per_day.png")
