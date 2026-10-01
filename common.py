"""Shared helpers for the AirBreda ingestion containers.

Three jobs:
  1. log_event()   -> print one structured JSON log line (Day 2 requirement)
  2. get_db_conn() -> open a PostgreSQL connection using environment variables
  3. record_run()  -> save the result of one ingestion run in `ingestion_runs`
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Plain "%(message)s" format: every line we print is already a complete JSON object.
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_logger = logging.getLogger("airbreda")

# If more than this many bad readings appear in one run (= one hour, because
# the containers run hourly), we log a single ERROR event. Day 2 requirement.
BAD_DATA_THRESHOLD = 10


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log_event(level: str, service: str, event: str, **fields) -> dict:
    """Emit one structured JSON log line and return it (returning makes it testable).

    Example output:
      {"ts": "2026-09-30T14:00:05Z", "level": "WARNING", "service": "ingest_traffic",
       "event": "DATA_QUALITY_ERROR", "source": "NDW", "field": "speed", "value": -1}
    """
    record = {"ts": utc_now_iso(), "level": level.upper(), "service": service, "event": event}
    record.update(fields)
    _logger.log(getattr(logging, level.upper()), json.dumps(record, default=str))
    return record


def get_db_conn():
    """Connect to PostgreSQL. All settings come from environment variables (the .env file),
    never from the code, so no password ends up in Git."""
    import psycopg2  # imported here so scripts can still run parts of their logic without a DB

    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "airbreda"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode=os.environ.get("DB_SSLMODE", "prefer"),
        connect_timeout=10,
    )


def record_run(conn, source: str, success: bool, rows_written: int = 0,
               bad_data_count: int = 0, error: str | None = None) -> None:
    """Write one row to ingestion_runs so the dashboard's /health can report on this run."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO ingestion_runs (source, success, rows_written, bad_data_count, error)
               VALUES (%s, %s, %s, %s, %s)""",
            (source, success, rows_written, bad_data_count, error),
        )
    conn.commit()


# ---------------------------------------------------------------- Day 2: Redis queue
def publish_reading(message: dict, service: str) -> bool:
    """Day 2 Lab: push one reading onto the Redis list 'readings' (only if REDIS_HOST is set).

    On the VM REDIS_HOST is NOT set: Day 3 drops the queue (one VM, hourly ingestion, no second
    consumer, so a broker adds operational surface without benefit; see ADR-002/ADR-004).
    If the broker is down, we log it and carry on: the reading still reaches the database, so
    a broker outage loses nothing here (this answers ADR-002's "what if the broker goes down").
    """
    host = os.environ.get("REDIS_HOST")
    if not host:
        return False
    try:
        import redis
        redis.Redis(host=host, port=int(os.environ.get("REDIS_PORT", "6379")),
                    socket_timeout=3).rpush("readings", json.dumps(message, default=str))
        return True
    except Exception as exc:
        log_event("warning", service, "queue_publish_failed", error_type=type(exc).__name__,
                  error=str(exc))
        return False


# ---------------------------------------------------------------- Day 2: per-container /health
def health_payload(conn, source: str, label: str) -> dict:
    """The Day 2 per-container health document: {"last_successful_fetch", "bad_data_count", "source"}.

    Why this is read from the database and not from memory: our containers are started by cron,
    fetch once and exit (Day 3), so there is no long-running process to keep a counter in RAM or
    to answer HTTP requests. Each run stores its result in ingestion_runs; this reads it back.
    bad_data_count = the last hour, matching the Day 2 "more than 10 within an hour" rule.
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT max(run_at) FILTER (WHERE success),
                              coalesce(sum(bad_data_count)
                                FILTER (WHERE run_at > now() - interval '1 hour'), 0)
                       FROM ingestion_runs WHERE source = %s""", (source,))
        last_ok, bad = cur.fetchone()
    from datetime import timezone as _tz
    ts = last_ok.astimezone(_tz.utc).isoformat(timespec="seconds").replace("+00:00", "Z") if last_ok else None
    return {"last_successful_fetch": ts,
            "bad_data_count": int(bad), "source": label}
