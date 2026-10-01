"""build_training_data.py - Day 4: join our own collected data into training_data.csv

Steps (exactly the Day 4 task list):
  1. Read every NO2 row for NL10240 from sensor_readings
  2. Read every NDW CSV from the bucket and pivot: one row per hour, one intensity column per site
  3. Join the two on the hour
  4. total_intensity_veh_per_hr = sum of the four sites
  5. hour_of_day = 0-23 (UTC, the same clock the dashboard uses when predicting)
  6. Write training_data.csv

Run (on the VM, where both the database and the bucket are reachable):
  docker run --rm --env-file .env -v ~/airbreda/out:/app/out airbreda-trainer python build_training_data.py
"""
import os
import sys

import pandas as pd

from common import get_db_conn, log_event
from storage import SITES, list_keys, read_rows

SERVICE = "build_training_data"
STATION_ID = os.environ.get("LMN_STATION", "NL10240")
OUT_DIR = os.environ.get("OUT_DIR", "out")


def load_no2(conn) -> pd.DataFrame:
    """NO2 per hour. Flagged rows (stale/null) are LEFT OUT of training: we keep them in the
    database for honesty, but we don't want the model to learn from values we suspect are wrong."""
    with conn.cursor() as cur:
        cur.execute("""SELECT timestamp, value FROM sensor_readings
                       WHERE station_id = %s AND component = 'NO2'
                         AND NOT is_flagged AND value IS NOT NULL
                       ORDER BY timestamp""", (STATION_ID,))
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["hour", "no2_ug_m3"])
    df["hour"] = pd.to_datetime(df["hour"], utc=True).dt.round("h")
    return df


def load_traffic() -> pd.DataFrame:
    """One row per hour, one column per site (intensity_hrl, intensity_hrr, ...)."""
    records = []
    for key in list_keys("ndw/"):
        for row in read_rows(key):
            value = row.get("intensity_veh_per_hr")
            if value in (None, ""):
                continue  # this site had no valid flow that hour
            records.append({"hour": row["hour"], "site": row["site"], "intensity": float(value)})
    if not records:
        return pd.DataFrame(columns=["hour"] + [f"intensity_{s}" for s in SITES])
    df = pd.DataFrame(records)
    df["hour"] = pd.to_datetime(df["hour"], utc=True).dt.round("h")
    wide = df.pivot_table(index="hour", columns="site", values="intensity", aggfunc="mean")
    wide.columns = [f"intensity_{c}" for c in wide.columns]
    return wide.reset_index()


def build(no2: pd.DataFrame, traffic: pd.DataFrame) -> pd.DataFrame:
    """Inner join on the hour, then add the two model features."""
    df = no2.merge(traffic, on="hour", how="inner")
    site_cols = [f"intensity_{s}" for s in SITES]
    for col in site_cols:
        if col not in df:
            df[col] = float("nan")
    # Only keep hours where ALL four sites reported, so 'total' means the same thing in every row.
    df = df.dropna(subset=site_cols)
    df["total_intensity_veh_per_hr"] = df[site_cols].sum(axis=1)
    df["hour_of_day"] = df["hour"].dt.hour
    return df[["hour", "no2_ug_m3", *site_cols, "total_intensity_veh_per_hr", "hour_of_day"]]


def main() -> int:
    conn = get_db_conn()
    try:
        no2 = load_no2(conn)
    finally:
        conn.close()
    traffic = load_traffic()
    df = build(no2, traffic)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "training_data.csv")
    df.to_csv(path, index=False)
    log_event("info", SERVICE, "training_data_built", no2_hours=len(no2),
              traffic_hours=len(traffic), joined_rows=len(df), path=path)
    if len(df) < 24:
        log_event("warning", SERVICE, "small_training_set", joined_rows=len(df),
                  note="fewer than 24 rows: state this as a limitation in ADR-006")
    return 0 if len(df) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
