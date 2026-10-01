"""Read the NDW CSV files that ingest_traffic.py saved.

Works in two modes, chosen by the S3_BUCKET environment variable:
  - S3_BUCKET set   -> read from the S3 bucket (on the VM; uses the VM's IAM role, no keys)
  - S3_BUCKET empty -> read from a local folder (LOCAL_OUTPUT_DIR, default ./data) for testing

Used by build_training_data.py (reads ALL files) and dashboard.py (reads the LATEST file per site).
"""
import csv
import io
import os
from datetime import datetime, timedelta, timezone

SITES = ["hrl", "hrr", "vwd", "vwa"]


def _bucket():
    return os.environ.get("S3_BUCKET", "").strip()


def _s3():
    import boto3
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-north-1"))


def _local_root():
    return os.environ.get("LOCAL_OUTPUT_DIR", "data")


def list_keys(prefix: str = "ndw/") -> list[str]:
    """All object keys under a prefix, e.g. 'ndw/2026-10-01/'. Sorted, so newest is last."""
    if _bucket():
        keys, token = [], None
        while True:
            kwargs = {"Bucket": _bucket(), "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = _s3().list_objects_v2(**kwargs)
            keys += [obj["Key"] for obj in resp.get("Contents", [])]
            if not resp.get("IsTruncated"):
                return sorted(keys)
            token = resp["NextContinuationToken"]
    root = _local_root()
    keys = []
    base = os.path.join(root, prefix)
    for dirpath, _dirs, files in os.walk(base):
        for name in files:
            full = os.path.join(dirpath, name)
            keys.append(os.path.relpath(full, root).replace(os.sep, "/"))
    return sorted(keys)


def read_rows(key: str) -> list[dict]:
    """Read one CSV file and return its rows as dictionaries."""
    if _bucket():
        body = _s3().get_object(Bucket=_bucket(), Key=key)["Body"].read().decode("utf-8")
    else:
        with open(os.path.join(_local_root(), key), encoding="utf-8") as f:
            body = f.read()
    return list(csv.DictReader(io.StringIO(body)))


def latest_row(site: str, days_back: int = 2) -> dict | None:
    """Most recent CSV row for one site. Only looks at today and the previous day(s),
    so the dashboard doesn't list the whole bucket on every request."""
    today = datetime.now(timezone.utc).date()
    for offset in range(days_back + 1):
        day = today - timedelta(days=offset)
        keys = [k for k in list_keys(f"ndw/{day:%Y-%m-%d}/") if k.endswith(f"-{site}.csv")]
        if keys:
            rows = read_rows(keys[-1])  # keys sort by hour, so the last one is the newest
            return rows[0] if rows else None
    return None
