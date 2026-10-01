"""ingest_air.py - container 1 of 3 (image: airbreda-air)

What it does, once per run (cron starts it every hour):
  1. FETCH   the latest readings for station NL10240 from the Luchtmeetnet API
             (same call as the course repository's extract.py / getNO2readings.py)
  2. FILTER  to NO2 only (filter_no2_readings, Day 1 Lab 1)
  3. CHECK   each reading: null value, or unchanged for 3+ consecutive hours -> flag it (Day 2)
  4. WRITE   every reading to sensor_readings (flagged ones with is_flagged = TRUE);
             duplicates are skipped by the primary key + ON CONFLICT DO NOTHING (Day 1 Lab 2)
  5. RECORD  the run's result in ingestion_runs (read later by the dashboard's /health)
  6. LOG     everything as structured JSON (Day 2), including the most recent reading

REQUIRED COMMENT (Day 1): CAP trade-off and null handling
  The Luchtmeetnet sensor network is a distributed system: stations measure locally and
  publish to a central service later. When a station or its link is down, Luchtmeetnet keeps
  answering with the data it has (older or missing hours) instead of refusing to answer.
  That is the AVAILABILITY side of the CAP trade-off: you always get a response, but it may
  be stale or incomplete, so consistency (always the newest true value) is given up.
  In a production pipeline a missing or null value must therefore NOT be silently dropped
  (a hidden gap looks like "no pollution") and must NOT be filled in with a guess. We store
  it with is_flagged = TRUE and log a structured warning, so the gap stays visible,
  countable, and excludable from model training.

Error handling: if the API is briefly unreachable (timeout, HTTP error), the run logs a
structured error, records a failed run in ingestion_runs, and exits with code 1. Nothing is
lost: the next hourly run fetches the last ~50 hours again and fills the gap.

Polling interval (Day 2): Luchtmeetnet publishes ONE value per hour, so polling hourly is
enough. Polling every minute would re-download the same 50 readings 60 times an hour: no
new data, 60x the load on a free public API (fair use: 100 requests per 5 minutes), and
60x the log noise.
"""
import json
import os
import sys

import pandas as pd
import requests

from common import (BAD_DATA_THRESHOLD, get_db_conn, health_payload, log_event,
                    publish_reading, record_run)

SERVICE = "ingest_air"
SOURCE = "luchtmeetnet"
STATION_ID = os.environ.get("LMN_STATION", "NL10240")
API_URL = "https://api.luchtmeetnet.nl/open_api/stations/{station}/measurements"
STALE_RUN_LENGTH = 3  # "unchanged for 3+ consecutive hourly timestamps" (Day 2)


def fetch_measurements(station_id: str = STATION_ID, formula: str = "NO2") -> pd.DataFrame:
    """Call the Luchtmeetnet API and return a DataFrame with columns component, value, timestamp.

    Real response shape (checked against the live API on 30 Sep 2026):
      {"pagination": {...}, "data": [
         {"value": 26.07, "timestamp_measured": "2026-09-30T13:00:00+00:00", "formula": "NO2", ...},
         ... ~50 hourly readings, newest first ... ]}
    """
    resp = requests.get(API_URL.format(station=station_id),
                        params={"formula": formula}, timeout=30)
    resp.raise_for_status()  # turns HTTP errors (500, 404...) into an exception we log
    data = resp.json().get("data", [])
    return pd.DataFrame({
        "component": [item.get("formula") for item in data],
        "value": [item.get("value") for item in data],
        "timestamp": [item.get("timestamp_measured") for item in data],
    })


def filter_no2_readings(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only NO2 rows (Day 1 Lab 1). Rows with a null value are KEPT, not dropped:
    they get flagged later, see the CAP comment at the top."""
    return df[df["component"] == "NO2"].reset_index(drop=True)


def detect_quality(df: pd.DataFrame, station_id: str = STATION_ID) -> list[dict]:
    """Turn NO2 rows into database rows and decide is_flagged for each.

    Rules (Day 2):
      - value is null/missing                              -> flagged, reason "null"
      - same value as the previous 2 hours (3 in a row)    -> flagged, reason "stale"
        (the first two of such a run are not flagged: at that point they looked normal)
    """
    rows = []
    for rec in df.to_dict("records"):
        if not rec.get("timestamp"):
            continue
        value = rec.get("value")
        value = None if value is None or pd.isna(value) else float(value)
        rows.append({"station_id": station_id, "timestamp": rec["timestamp"],
                     "component": "NO2", "value": value})
    rows.sort(key=lambda r: r["timestamp"])  # oldest first, so "previous hours" is easy

    for i, row in enumerate(rows):
        row["is_flagged"], row["reason"] = False, None
        if row["value"] is None:
            row["is_flagged"], row["reason"] = True, "null"
        elif i >= STALE_RUN_LENGTH - 1 and all(
            rows[j]["value"] == row["value"] for j in range(i - STALE_RUN_LENGTH + 1, i)
        ):
            row["is_flagged"], row["reason"] = True, "stale"
    return rows


def write_readings(conn, rows: list[dict]) -> list[dict]:
    """Insert rows; return only the rows that were NEW (not already in the table).

    ON CONFLICT DO NOTHING makes re-sending the same reading harmless (idempotent).
    We check rowcount per row so a skipped duplicate is visible in the logs instead of
    silently disappearing (the Day 3 checklist warns about exactly that).
    """
    inserted = []
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (station_id, timestamp, component) DO NOTHING""",
                (row["station_id"], row["timestamp"], row["component"], row["value"],
                 row["is_flagged"]),
            )
            if cur.rowcount == 1:
                inserted.append(row)
    conn.commit()
    return inserted


def get_latest_no2(rows: list[dict]) -> dict | None:
    """Most recent non-null NO2 reading (named after the course helper getNO2readings.py)."""
    valid = [r for r in rows if r["value"] is not None]
    return valid[-1] if valid else None


def run() -> int:
    """One full ingestion run. Returns an exit code (0 = success, 1 = failure)."""
    conn = None
    try:
        conn = get_db_conn()
        rows = detect_quality(filter_no2_readings(fetch_measurements()))
        new_rows = write_readings(conn, rows)

        # Count bad data only among NEW rows. Every call re-sends ~50 hours, so counting
        # all of them would re-count the same stale reading every hour and trip the alarm.
        luchtmeetnet_bad_data_count = 0
        for row in new_rows:
            if row["is_flagged"]:
                luchtmeetnet_bad_data_count += 1
                log_event("warning", SERVICE, "DATA_QUALITY_ERROR", source="Luchtmeetnet",
                          station_id=row["station_id"], field="NO2", reason=row["reason"],
                          value=row["value"], timestamp=row["timestamp"], action="kept_flagged")
            else:
                log_event("info", SERVICE, "fetch_success", source="Luchtmeetnet",
                          station_id=row["station_id"], value=row["value"],
                          timestamp=row["timestamp"])
            # Day 2 message format; only active when REDIS_HOST is set (local Day 2 setup)
            publish_reading({"station_id": row["station_id"], "timestamp": row["timestamp"],
                             "component": "NO2", "value": row["value"]}, SERVICE)

        if luchtmeetnet_bad_data_count > BAD_DATA_THRESHOLD:
            log_event("error", SERVICE, "BAD_DATA_THRESHOLD_EXCEEDED", source="Luchtmeetnet",
                      count=luchtmeetnet_bad_data_count)

        latest = get_latest_no2(rows)
        if latest:
            log_event("info", SERVICE, "latest_reading", source="Luchtmeetnet",
                      station_id=STATION_ID, value=latest["value"], timestamp=latest["timestamp"])

        record_run(conn, SOURCE, success=True, rows_written=len(new_rows), bad_data_count=luchtmeetnet_bad_data_count)
        log_event("info", SERVICE, "run_complete", source="Luchtmeetnet", fetched=len(rows),
                  inserted=len(new_rows), skipped_existing=len(rows) - len(new_rows),
                  bad_data_count=luchtmeetnet_bad_data_count)
        return 0

    except Exception as exc:  # any failure: log it as structured JSON, record it, exit non-zero
        log_event("error", SERVICE, "fetch_failed", source="Luchtmeetnet",
                  error_type=type(exc).__name__, error=str(exc))
        if conn is not None:
            try:
                conn.rollback()
                record_run(conn, SOURCE, success=False, error=f"{type(exc).__name__}: {exc}")
            except Exception:
                pass  # the DB itself may be what failed; the log line above still exists
        return 1
    finally:
        if conn is not None:
            conn.close()


def health() -> int:
    """`python ingest_air.py health` prints this container's Day 2 health JSON."""
    conn = get_db_conn()
    try:
        print(json.dumps(health_payload(conn, SOURCE, "Luchtmeetnet")))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(health() if sys.argv[1:] == ["health"] else run())
