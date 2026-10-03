"""ingest_traffic.py - container 2 of 3 (image: airbreda-traffic)

What it does, once per run (cron starts it every hour). Structure follows the course's reference
extraction script / getTrafficReading.py (Day 1 Lab 2): download_and_decompress ->
build_index_map -> extract_measurements.
  1. DOWNLOAD  the two NDW feeds named in the Day 1 lab (DATEX II v3):
               snelheden_en_intensiteiten_configuratie_meetlocaties.xml.gz  (site configuration)
               snelheden_en_intensiteiten_meetgegevens.xml.gz               (live measured values)
  2. INDEX     from the configuration: for each of our 4 sites, which measurement index is a
               traffic FLOW and which is a SPEED, and for which vehicle type (build_index_map)
  3. EXTRACT   the live values for our 4 A27 sites (hrl, hrr, vwd, vwa) (extract_measurements)
  4. CLEAN     speed = -1 values: log a structured WARNING, count them, EXCLUDE them (Day 2)
  5. SAVE      one small CSV per site, named after the MEASUREMENT time (not today's date):
               ndw/YYYY-MM-DD/HH-{site}.csv, uploaded to the bucket,
               AND the untouched raw measurement file: raw/ndw/YYYY-MM-DD/HH.xml.gz
  6. RECORD    the run in ingestion_runs (so the dashboard's /health can see it)

REQUIRED COMMENT (Day 1): why we need BOTH the database and the bucket
  The DATABASE (PostgreSQL sensor_readings) gives fast, indexed answers to questions like
  "latest NO2 for NL10240" or "all hours between x and y", plus joins and a primary key that
  blocks duplicates. It holds clean, structured, small rows. It is bad at storing large raw files.
  The BUCKET (S3) is cheap, durable storage for whole files: the untouched raw NDW XML and the
  per-site CSVs. It can't answer queries or enforce uniqueness, but it keeps the ORIGINAL data.
  Retraining the ML model six months from now: we rebuild training_data.csv from the bucket's
  files plus the database. If we discover a parsing bug (say, in the speed=-1 rule) we can
  re-parse the raw XML with fixed code and retrain on corrected data. With only the database,
  anything our old code got wrong would be wrong forever, because the original is gone.

Why stream-parse? The unzipped measurement XML is ~50 MB and the VM has 1 GB of RAM.
iterparse() reads one site at a time and discards it, so memory stays small (measured: <40 MB).

speed = -1 means "no speed could be computed" (typically no cars passed that lane in that
minute). It is a sentinel value, not a real speed, so it must never reach our stored data.

Known limitation: the live feed only contains the CURRENT minute. One hourly run stores a
one-minute snapshot, not an hourly average, which is noisier than the NO2 hourly average.
"""
import csv
import gzip
import io
import json
import os
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests

from common import (BAD_DATA_THRESHOLD, get_db_conn, health_payload, log_event,
                    publish_reading, record_run)

SERVICE = "ingest_traffic"
SOURCE = "ndw"
NDW_BASE = os.environ.get("NDW_BASE_URL", "https://opendata.ndw.nu")
CONFIG_FILE = "snelheden_en_intensiteiten_configuratie_meetlocaties.xml.gz"
MEASUREMENT_FILE = "snelheden_en_intensiteiten_meetgegevens.xml.gz"

# Site label -> NDW measurement site ID. All four are at the same point on the A27 just north of
# Breda (about 51.592 N, 4.829 E); NDW's own configuration labels them mainCarriageway (hrl, hrr),
# entrySlipRoad (vwd) and exitSlipRoad (vwa). I matched them by road position (0063).
# Override with NDW_SITES="hrl=...,hrr=...,vwd=...,vwa=..."
DEFAULT_SITES = {
    "hrl": "RWS01_MONIBAS_0271hrl0063ra",
    "hrr": "RWS01_MONIBAS_0271hrr0063ra",
    "vwd": "RWS01_MONIBAS_0270vwd0063ra",
    "vwa": "RWS01_MONIBAS_0270vwa0063ra",
}

CSV_COLUMNS = ["site", "ndw_site_id", "measurement_time", "hour",
               "intensity_veh_per_hr", "avg_speed_kmh", "lanes_with_valid_speed",
               "excluded_speed_values"]


def load_sites() -> dict[str, str]:
    raw = os.environ.get("NDW_SITES", "").strip()
    if not raw:
        return dict(DEFAULT_SITES)
    return dict(pair.split("=", 1) for pair in raw.split(","))


def _local(tag: str) -> str:
    """'{http://datex2.eu/schema/3/roadTrafficData}siteMeasurements' -> 'siteMeasurements'."""
    return tag.rsplit("}", 1)[-1]


def download_and_decompress(url: str) -> tuple[str, gzip.GzipFile]:
    """Stream the .xml.gz to a temp file on disk (not into RAM). Returns (path, decompressing
    stream). Keeping the path lets us upload the untouched original to the bucket."""
    tmp = tempfile.NamedTemporaryFile(suffix=".xml.gz", delete=False)
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=1 << 16):
            tmp.write(chunk)
    tmp.close()
    return tmp.name, gzip.open(tmp.name, "rb")


def build_index_map(config_stream, wanted_ids: set[str]) -> dict[str, dict[int, tuple[str, str]]]:
    """From the site configuration: {site_id: {index: (value_type, vehicle_type)}}.
    value_type is 'trafficFlow' or 'trafficSpeed'; vehicle_type is e.g. 'anyVehicle'."""
    index_map = {}
    for _event, elem in ET.iterparse(config_stream, events=("end",)):
        if _local(elem.tag) != "measurementSite":
            continue
        site_id = elem.get("id")
        if site_id in wanted_ids:
            entries = {}
            for msc in elem:
                if _local(msc.tag) != "measurementSpecificCharacteristics" or msc.get("index") is None:
                    continue
                value_type = vehicle_type = None
                for node in msc.iter():
                    if _local(node.tag) == "specificMeasurementValueType":
                        value_type = node.text
                    elif _local(node.tag) == "vehicleType":
                        vehicle_type = node.text
                entries[int(msc.get("index"))] = (value_type, vehicle_type)
            index_map[site_id] = entries
        elem.clear()
        if len(index_map) == len(wanted_ids):
            break
    return index_map


def extract_measurements(meas_stream, wanted_ids: set[str],
                         index_map: dict[str, dict[int, tuple[str, str]]]) -> dict[str, dict]:
    """Read the live values one <siteMeasurements> at a time; keep only our sites.

    Uses index_map so a value is classified by what the CONFIGURATION says it is. Only
    'anyVehicle' values are kept: some NDW sites also publish per-vehicle-class values, and
    summing those as well would double-count traffic.
    Returns {site_id: {"measurement_time": str, "flows": [(index, v)], "speeds": [(index, v)]}}
    """
    found = {}
    for _event, elem in ET.iterparse(meas_stream, events=("end",)):
        if _local(elem.tag) != "siteMeasurements":
            continue
        site_id, measurement_time = None, None
        for child in elem:
            name = _local(child.tag)
            if name == "measurementSiteReference":
                site_id = child.get("id")
            elif name == "measurementTimeDefault":
                measurement_time = next((n.text for n in child.iter() if _local(n.tag) == "timeValue"),
                                        child.text)
        if site_id in wanted_ids:
            flows, speeds, config = [], [], index_map.get(site_id, {})
            for pq in elem:
                if _local(pq.tag) != "physicalQuantity" or pq.get("index") is None:
                    continue
                index = int(pq.get("index"))
                value_type, vehicle_type = config.get(index, (None, None))
                if vehicle_type not in (None, "anyVehicle"):
                    continue
                for node in pq.iter():
                    tag = _local(node.tag)
                    if tag == "vehicleFlowRate" and value_type in (None, "trafficFlow"):
                        flows.append((index, float(node.text)))
                    elif tag == "speed" and value_type in (None, "trafficSpeed"):
                        speeds.append((index, float(node.text)))
            found[site_id] = {"measurement_time": measurement_time, "flows": flows, "speeds": speeds}
        elem.clear()
        if len(found) == len(wanted_ids):
            break
    return found


def nearest_hour(iso_ts: str) -> datetime:
    """Round a timestamp to the nearest hour, e.g. 13:59 -> 14:00.

    Luchtmeetnet's timestamp 14:00 means "the hour 13:00-14:00". A traffic snapshot taken by
    the 14:00 cron run is from ~13:59, so rounding puts both on the same hour for the Day 4 join.
    """
    ts = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return (ts + timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)


def clean_site(site: str, ndw_id: str, parsed: dict) -> tuple[dict, list[dict]]:
    """Apply the bad-data rule to one site. Pure function: returns (csv_row, bad_events).

    Rule: any speed < 0 (NDW's sentinel is -1) is logged and EXCLUDED. We drop that single
    speed value, not the whole site-hour: empty lanes produce -1 very often (e.g. at night),
    and dropping the whole row would throw away the valid flow and the other lane's speed.
    Negative flows are treated the same way (never observed, but also impossible values).
    """
    bad_events = []
    valid_flows = []
    for index, value in parsed["flows"]:
        if value < 0:
            bad_events.append({"location": ndw_id, "site": site, "field": "vehicleFlowRate",
                               "index": index, "value": value})
        else:
            valid_flows.append(value)
    valid_speeds = []
    for index, value in parsed["speeds"]:
        if value < 0:
            bad_events.append({"location": ndw_id, "site": site, "field": "speed",
                               "index": index, "value": value})
        else:
            valid_speeds.append(value)

    hour = nearest_hour(parsed["measurement_time"])
    row = {
        "site": site,
        "ndw_site_id": ndw_id,
        "measurement_time": parsed["measurement_time"],
        "hour": hour.isoformat().replace("+00:00", "Z"),
        "intensity_veh_per_hr": int(sum(valid_flows)) if valid_flows else None,
        "avg_speed_kmh": round(sum(valid_speeds) / len(valid_speeds), 1) if valid_speeds else None,
        "lanes_with_valid_speed": len(valid_speeds),
        "excluded_speed_values": sum(1 for e in bad_events if e["field"] == "speed"),
    }
    return row, bad_events


def object_key(row: dict) -> str:
    """ndw/YYYY-MM-DD/HH-{site}.csv  (the naming scheme required by Day 3/4)."""
    hour = datetime.fromisoformat(row["hour"].replace("Z", "+00:00"))
    return f"ndw/{hour:%Y-%m-%d}/{hour:%H}-{row['site']}.csv"


def to_csv(row: dict) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerow(row)
    return buf.getvalue()


def save(key: str, body, content_type: str = "text/csv") -> str:
    """Save to S3 if S3_BUCKET is set, otherwise to a local folder (for laptop testing).
    body may be text (CSV) or bytes (the raw .xml.gz).

    On the VM no keys are needed: boto3 automatically uses the VM's IAM role.
    """
    data = body.encode("utf-8") if isinstance(body, str) else body
    bucket = os.environ.get("S3_BUCKET")
    if bucket:
        import boto3
        boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-north-1")).put_object(
            Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        return f"s3://{bucket}/{key}"
    path = os.path.join(os.environ.get("LOCAL_OUTPUT_DIR", "data"), key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    return path


def run() -> int:
    use_db = bool(os.environ.get("DB_HOST"))
    conn, tmp_paths = None, []
    try:
        conn = get_db_conn() if use_db else None
        sites = load_sites()
        wanted = set(sites.values())
        config_path, config_stream = download_and_decompress(f"{NDW_BASE}/{CONFIG_FILE}")
        tmp_paths.append(config_path)
        with config_stream:
            index_map = build_index_map(config_stream, wanted)
        tmp_path, meas_stream = download_and_decompress(f"{NDW_BASE}/{MEASUREMENT_FILE}")
        tmp_paths.append(tmp_path)
        with meas_stream:
            parsed = extract_measurements(meas_stream, wanted, index_map)

        ndw_bad_data_count, saved = 0, 0
        for site, ndw_id in sites.items():
            if ndw_id not in parsed:
                log_event("warning", SERVICE, "site_missing", source="NDW", site=site, location=ndw_id)
                continue
            row, bad_events = clean_site(site, ndw_id, parsed[ndw_id])
            for ev in bad_events:
                ndw_bad_data_count += 1
                log_event("warning", SERVICE, "DATA_QUALITY_ERROR", source="NDW",
                          action="excluded", **ev)
            where = save(object_key(row), to_csv(row))
            publish_reading({"site_id": site, "location": ndw_id, "timestamp": row["hour"],
                             "component": "intensity_veh_per_hr",
                             "value": row["intensity_veh_per_hr"]}, SERVICE)
            saved += 1
            log_event("info", SERVICE, "fetch_success", source="NDW", site=site, location=ndw_id,
                      intensity_veh_per_hr=row["intensity_veh_per_hr"],
                      avg_speed_kmh=row["avg_speed_kmh"],
                      measurement_time=row["measurement_time"], saved_to=where)

        # Keep the untouched original (~1.2 MB) for audit and re-processing.
        if parsed:
            first = next(iter(parsed.values()))
            hour = nearest_hour(first["measurement_time"])
            with open(tmp_path, "rb") as f:
                raw_where = save(f"raw/ndw/{hour:%Y-%m-%d}/{hour:%H}.xml.gz", f.read(),
                                 content_type="application/gzip")
            log_event("info", SERVICE, "raw_saved", source="NDW", saved_to=raw_where)

        if ndw_bad_data_count > BAD_DATA_THRESHOLD:
            log_event("error", SERVICE, "BAD_DATA_THRESHOLD_EXCEEDED", source="NDW", count=ndw_bad_data_count)

        success = saved > 0
        if conn is not None:
            record_run(conn, SOURCE, success=success, rows_written=saved, bad_data_count=ndw_bad_data_count,
                       error=None if success else "no configured site found in NDW feed")
        log_event("info" if success else "error", SERVICE, "run_complete", source="NDW",
                  sites_saved=saved, bad_data_count=ndw_bad_data_count)
        return 0 if success else 1

    except Exception as exc:
        log_event("error", SERVICE, "fetch_failed", source="NDW",
                  error_type=type(exc).__name__, error=str(exc))
        if conn is not None:
            try:
                conn.rollback()
                record_run(conn, SOURCE, success=False, error=f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
        return 1
    finally:
        for path in tmp_paths:
            if os.path.exists(path):
                os.remove(path)
        if conn is not None:
            conn.close()


def health() -> int:
    """`python ingest_traffic.py health` prints this container's Day 2 health JSON."""
    conn = get_db_conn()
    try:
        print(json.dumps(health_payload(conn, SOURCE, "NDW")))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(health() if sys.argv[1:] == ["health"] else run())
