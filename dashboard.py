"""dashboard.py - container 3 of 3 (image: airbreda-dashboard). Runs all the time on port 8000.

Routes:
  GET /site/{site_id}  -> JSON for hrl, hrr, vwd or vwa
  GET /health          -> last successful fetch + bad data count for BOTH ingestion sources
  GET /                -> HTML page; its JavaScript calls /site/{id} four times (the page is
                          just another client of our own API, so dashboard and API can't disagree)

WHERE EACH FIELD OF /site/{id} COMES FROM (required comment, Day 4):
  site_id               -> the URL
  no2_ug_m3             -> PostgreSQL sensor_readings: newest non-null value, component 'NO2', NL10240.
                           One air-quality station serves all four sites (they are ~100 m apart).
  timestamp             -> that NO2 reading's timestamp (end of the measured hour, UTC)
  intensity_veh_per_hr  -> S3 bucket: this site's newest ndw/YYYY-MM-DD/HH-{site}.csv
  no2_ug_m3_predicted   -> predict.py (model.pkl baked into this image)
  no2_exceedance_risk   -> predict.py (sigmoid around the threshold)
  traffic_timestamp     -> measurement time of the traffic snapshot (lets you spot stale traffic)
  The model was trained on the TOTAL of all four sites, so predict() receives that total, not this
  site's intensity. Feeding it one site's value would be training-serving skew. Consequence: all
  four sites show the same prediction, because one NO2 station can't tell the sites apart.

IF predict() RAISES (required comment, Day 4):
  We DEGRADE instead of failing: the response still returns the real NO2 and real intensity,
  with no2_ug_m3_predicted and no2_exceedance_risk set to null and a "prediction_error" field.
  Reason: the measured values are real and useful on their own; a broken model shouldn't hide
  them. A 500 error would make the endpoint look fully down when only one part is broken.
"""
import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from common import get_db_conn, log_event
from predict import predict
from storage import SITES, latest_row

SERVICE = "dashboard"
STATION_ID = os.environ.get("LMN_STATION", "NL10240")

app = FastAPI(title="AirBreda")


def _iso(dt) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def latest_no2() -> tuple[float | None, datetime | None]:
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT value, timestamp FROM sensor_readings
                           WHERE station_id = %s AND component = 'NO2' AND value IS NOT NULL
                           ORDER BY timestamp DESC LIMIT 1""", (STATION_ID,))
            row = cur.fetchone()
    finally:
        conn.close()
    return (float(row[0]), row[1]) if row else (None, None)


def site_intensity(site: str) -> tuple[float | None, str | None]:
    row = latest_row(site)
    if not row or row.get("intensity_veh_per_hr") in (None, ""):
        return None, None
    return float(row["intensity_veh_per_hr"]), row.get("measurement_time")


@app.get("/site/{site_id}")
def get_site(site_id: str):
    if site_id not in SITES:
        raise HTTPException(status_code=404, detail=f"unknown site '{site_id}', use one of {SITES}")
    try:
        no2, no2_ts = latest_no2()
        intensities = {s: site_intensity(s) for s in SITES}
    except Exception as exc:
        log_event("error", SERVICE, "data_read_failed", site=site_id,
                  error_type=type(exc).__name__, error=str(exc))
        raise HTTPException(status_code=503, detail="database or bucket unavailable")

    intensity, traffic_ts = intensities[site_id]
    response = {
        "site_id": site_id,
        "no2_ug_m3": no2,
        "intensity_veh_per_hr": intensity,
        "no2_exceedance_risk": None,
        "timestamp": _iso(no2_ts),
        "no2_ug_m3_predicted": None,
        "traffic_timestamp": traffic_ts,
    }

    values = [v for v, _ in intensities.values()]
    if any(v is None for v in values):
        response["prediction_error"] = "traffic missing for at least one site; total unknown"
    else:
        try:
            # UTC hour of the traffic snapshot = the same clock used in training
            ts = datetime.fromisoformat(traffic_ts.replace("Z", "+00:00"))
            hour = (ts + timedelta(minutes=30)).astimezone(timezone.utc).hour
            response.update(predict(sum(values), hour))
        except Exception as exc:
            response["prediction_error"] = f"{type(exc).__name__}: {exc}"
            log_event("error", SERVICE, "prediction_failed", site=site_id,
                      error_type=type(exc).__name__, error=str(exc))

    log_event("info", SERVICE, "site_served", site=site_id,
              no2=response["no2_ug_m3"], intensity=intensity,
              risk=response["no2_exceedance_risk"])
    return response


@app.get("/health")
def health():
    """Reads ingestion_runs, which each ingestion container writes at the end of every run.
    bad_data_count = sum over the last 24 hours."""
    try:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                out = {"status": "ok"}
                for source in ("luchtmeetnet", "ndw"):
                    cur.execute("""SELECT max(run_at) FILTER (WHERE success),
                                          coalesce(sum(bad_data_count)
                                            FILTER (WHERE run_at > now() - interval '24 hours'), 0)
                                   FROM ingestion_runs WHERE source = %s""", (source,))
                    last_ok, bad = cur.fetchone()
                    out[source] = {"last_successful_fetch": _iso(last_ok), "bad_data_count": int(bad)}
        finally:
            conn.close()
    except Exception as exc:
        log_event("error", SERVICE, "health_check_failed", error_type=type(exc).__name__, error=str(exc))
        return {"status": "degraded", "error": "database unavailable",
                "luchtmeetnet": None, "ndw": None}

    # 'ok' only if both sources succeeded in the last 2 hours (ingestion runs hourly)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    for source in ("luchtmeetnet", "ndw"):
        ts = out[source]["last_successful_fetch"]
        if ts is None or datetime.fromisoformat(ts.replace("Z", "+00:00")) < cutoff:
            out["status"] = "degraded"
    return out


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda</title>
<style>
 body{font-family:system-ui,sans-serif;max-width:900px;margin:24px auto;padding:0 16px;color:#1d2433;background:#f6f7f9}
 h1{margin:0 0 4px} .sub{color:#5b6475;margin:0 0 20px}
 .tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:20px}
 .tile{background:#fff;border:1px solid #dde1e8;border-radius:10px;padding:14px}
 .tile .label{font-size:13px;color:#5b6475} .tile .value{font-size:30px;font-weight:600;margin-top:4px}
 table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #dde1e8;border-radius:10px;overflow:hidden}
 th,td{padding:10px;text-align:left;border-bottom:1px solid #eef0f4} th{font-size:13px;color:#5b6475;font-weight:600}
 .meta{font-size:13px;color:#5b6475;margin-top:12px} .warn{color:#b3261e}
</style></head><body>
<h1>AirBreda</h1>
<p class="sub">A27 interchange, Breda: NO₂ station NL10240 (Luchtmeetnet) and 4 NDW traffic sites</p>
<div class="tiles">
 <div class="tile"><div class="label">Actual NO₂ (µg/m³)</div><div class="value" id="actual">…</div></div>
 <div class="tile"><div class="label">Predicted NO₂ (µg/m³)</div><div class="value" id="pred">…</div></div>
 <div class="tile"><div class="label">Exceedance risk</div><div class="value" id="risk">…</div></div>
 <div class="tile"><div class="label">Total traffic (vehicles/hour)</div><div class="value" id="total">…</div></div>
</div>
<table><thead><tr><th>Site</th><th>Vehicles/hour</th><th>Predicted NO₂</th><th>Risk</th></tr></thead>
<tbody id="rows"></tbody></table>
<p class="meta" id="meta"></p>
<script>
const SITES = ["hrl","hrr","vwd","vwa"];
const fmt = (v, d=0) => (v === null || v === undefined) ? "n/a" : Number(v).toFixed(d);
async function load() {
  const data = await Promise.all(SITES.map(s => fetch("/site/" + s).then(r => r.json())));
  const first = data[0];
  document.getElementById("actual").textContent = fmt(first.no2_ug_m3, 1);
  document.getElementById("pred").textContent = fmt(first.no2_ug_m3_predicted, 1);
  document.getElementById("risk").textContent = fmt(first.no2_exceedance_risk, 2);
  const vals = data.map(d => d.intensity_veh_per_hr);
  document.getElementById("total").textContent =
    vals.some(v => v === null) ? "n/a" : fmt(vals.reduce((a, b) => a + b, 0));
  document.getElementById("rows").innerHTML = data.map(d =>
    `<tr><td>${d.site_id}</td><td>${fmt(d.intensity_veh_per_hr)}</td>` +
    `<td>${fmt(d.no2_ug_m3_predicted, 1)}</td><td>${fmt(d.no2_exceedance_risk, 2)}</td></tr>`).join("");
  const err = data.find(d => d.prediction_error);
  const age = first.timestamp ? (Date.now() - Date.parse(first.timestamp)) / 3600000 : null;
  document.getElementById("meta").innerHTML =
    `NO₂ reading from ${first.timestamp ?? "n/a"} · traffic from ${first.traffic_timestamp ?? "n/a"} (UTC)` +
    (age !== null && age > 3 ? ` <span class="warn">· data is ${age.toFixed(1)} h old</span>` : "") +
    (err ? ` <span class="warn">· prediction unavailable: ${err.prediction_error}</span>` : "") +
    ` · page refreshed ${new Date().toLocaleTimeString()}`;
}
load(); setInterval(load, 60000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
