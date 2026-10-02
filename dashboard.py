"""dashboard.py - container 3 of 3 (image: airbreda-dashboard). Runs all the time on port 8000.

Routes:
  GET /site/{site_id}  -> JSON for hrl, hrr, vwd or vwa
  GET /health          -> last successful fetch + bad data count for BOTH ingestion sources
  GET /history         -> last N hours (default 48) of actual NO2, total traffic and the model's
                          prediction for each hour; feeds the charts. Not part of the graded API.
  GET /                -> HTML page; its JavaScript calls /site/{id}, /health and /history (the
                          page is just another client of our own API, so dashboard and API can't
                          disagree)

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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from common import get_db_conn, log_event
import predict as predict_module
from predict import THRESHOLD_UG_M3, predict
from storage import SITES, latest_row, list_keys, read_rows

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


# ------------------------------------------------------------------ /history (charts)
# Reading ~200 small CSV files from S3 takes a few seconds, so the result is cached for 5 minutes.
# New data only arrives once an hour, so a 5-minute-old answer is never meaningfully stale.
HISTORY_TTL_S = 300
_history_cache: dict = {}
_history_lock = threading.Lock()


def _no2_history(hours: int) -> list[dict]:
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT timestamp, value, is_flagged FROM sensor_readings
                           WHERE station_id = %s AND component = 'NO2'
                             AND timestamp > now() - make_interval(hours => %s)
                           ORDER BY timestamp""", (STATION_ID, hours))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [{"t": _iso(ts), "value": None if v is None else float(v), "flagged": bool(f)}
            for ts, v, f in rows]


def _traffic_history(hours: int) -> tuple[list[dict], dict]:
    """Per hour: total of the four sites (only when all four reported) and the per-site values.
    Also returns the newest row per site (for the sensor cards)."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    keys, day = [], start.date()
    while day <= now.date():
        keys += [k for k in list_keys(f"ndw/{day:%Y-%m-%d}/") if k.endswith(".csv")]
        day += timedelta(days=1)

    def key_time(k: str) -> datetime:  # ndw/YYYY-MM-DD/HH-site.csv
        _, d, name = k.split("/")
        return datetime.fromisoformat(f"{d}T{name[:2]}:00:00+00:00")

    keys = [k for k in keys if key_time(k) >= start]
    with ThreadPoolExecutor(max_workers=16) as pool:
        files = list(pool.map(read_rows, keys))

    by_hour: dict[str, dict] = {}
    latest: dict[str, dict] = {}
    for rows in files:
        for row in rows:
            site = row.get("site")
            if site not in SITES:
                continue
            speed = row.get("avg_speed_kmh")
            info = {"intensity_veh_per_hr": float(row["intensity_veh_per_hr"])
                    if row.get("intensity_veh_per_hr") not in (None, "") else None,
                    "avg_speed_kmh": float(speed) if speed not in (None, "") else None,
                    "measurement_time": row.get("measurement_time"), "hour": row.get("hour")}
            by_hour.setdefault(row["hour"], {})[site] = info["intensity_veh_per_hr"]
            if site not in latest or (row.get("hour") or "") > (latest[site]["hour"] or ""):
                latest[site] = info

    out = []
    for hour in sorted(by_hour):
        sites = by_hour[hour]
        complete = all(sites.get(s) is not None for s in SITES)
        total = sum(sites[s] for s in SITES) if complete else None
        point = {"t": hour, "total": total, "sites": sites, "predicted": None}
        if complete:
            try:  # same features as training: total of 4 sites + UTC hour (no skew)
                h = datetime.fromisoformat(hour.replace("Z", "+00:00")).astimezone(timezone.utc).hour
                point["predicted"] = predict(total, h)["no2_ug_m3_predicted"]
            except Exception:
                point["predicted"] = None
        out.append(point)
    return out, latest


def _model_info() -> dict | None:
    try:
        model = predict_module._load()
        return {"intercept": round(float(model.intercept_), 3),
                "coef_traffic": float(model.coef_[0]), "coef_hour": float(model.coef_[1])}
    except Exception:
        return None


@app.get("/history")
def history(hours: int = Query(48, ge=6, le=72)):
    with _history_lock:
        hit = _history_cache.get(hours)
        if hit and time.time() - hit[0] < HISTORY_TTL_S:
            return hit[1]
        try:
            no2 = _no2_history(hours)
            traffic, latest = _traffic_history(hours)
        except Exception as exc:
            log_event("error", SERVICE, "history_failed", error_type=type(exc).__name__, error=str(exc))
            raise HTTPException(status_code=503, detail="database or bucket unavailable")
        data = {"hours": hours, "threshold_ug_m3": THRESHOLD_UG_M3, "no2": no2,
                "traffic": traffic, "sites_latest": latest, "model": _model_info(),
                "generated_at": _iso(datetime.now(timezone.utc))}
        _history_cache[hours] = (time.time(), data)
        log_event("info", SERVICE, "history_served", hours=hours, no2_points=len(no2),
                  traffic_points=len(traffic))
        return data


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda: is the A27 making Breda's air dirtier?</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;0,6..72,600;1,6..72,400&family=Atkinson+Hyperlegible:wght@400;700&display=swap" rel="stylesheet">
<style>
:root{
 color-scheme:light;
 --paper:#f2f0e8; --paper-2:#e9e6db; --rule:#d6d2c5; --ink:#1c2230; --ink-2:#4d5361; --ink-3:#7b7f88;
 --actual:#1f4e99; --pred:#b7791f; --traffic:#278a6a; --limit:#b3261e; --good:#1d7a3a;
 --road:#c9c4b5; --road-edge:#b5af9e; --lane:#f2f0e8;
}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])){
 color-scheme:dark;
 --paper:#15181e; --paper-2:#1d2129; --rule:#2c313b; --ink:#ece8dd; --ink-2:#b4b0a6; --ink-3:#858892;
 --actual:#4a86d8; --pred:#bd7f1f; --traffic:#2f9a74; --limit:#e0605a; --good:#4cb26a;
 --road:#2a2f39; --road-edge:#363c47; --lane:#15181e;
}}
:root[data-theme="dark"]{
 color-scheme:dark;
 --paper:#15181e; --paper-2:#1d2129; --rule:#2c313b; --ink:#ece8dd; --ink-2:#b4b0a6; --ink-3:#858892;
 --actual:#4a86d8; --pred:#bd7f1f; --traffic:#2f9a74; --limit:#e0605a; --good:#4cb26a;
 --road:#2a2f39; --road-edge:#363c47; --lane:#15181e;
}
*{box-sizing:border-box}
html{background:var(--paper)}
body{margin:0;background:var(--paper);color:var(--ink);font:400 16px/1.6 "Atkinson Hyperlegible",system-ui,sans-serif;-webkit-font-smoothing:antialiased}
.page{max-width:1080px;margin:0 auto;padding:0 24px 64px}
.measure{max-width:700px}
a{color:var(--actual)}
button{font:inherit;color:inherit}
/* masthead */
.mast{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:18px 0;border-bottom:1px solid var(--rule)}
.word{font:600 21px/1 Newsreader,Georgia,serif;letter-spacing:-.01em}
.word span{color:var(--ink-3);font-weight:400}
.live{display:flex;align-items:center;gap:10px;font-size:14px;color:var(--ink-2)}
.pulse{width:9px;height:9px;border-radius:50%;background:var(--good);position:relative}
.pulse::after{content:"";position:absolute;inset:-4px;border-radius:50%;border:2px solid var(--good);opacity:0;animation:ring 2.4s infinite}
.pulse.off{background:var(--limit)}.pulse.off::after{display:none}
@keyframes ring{0%{opacity:.6;transform:scale(.6)}100%{opacity:0;transform:scale(1.6)}}
.toggle{border:1px solid var(--rule);background:transparent;border-radius:999px;padding:5px 12px;cursor:pointer;font-size:13px;color:var(--ink-2)}
.toggle:hover{color:var(--ink);border-color:var(--ink-3)}
/* headline */
h1{font:500 clamp(40px,6.4vw,76px)/1.02 Newsreader,Georgia,serif;letter-spacing:-.025em;margin:56px 0 22px;max-width:900px;font-variation-settings:"opsz" 72}
.stand{font:400 clamp(19px,2.1vw,23px)/1.5 Newsreader,Georgia,serif;color:var(--ink-2);margin:0 0 40px}
.stand .em{color:var(--ink)}
/* figures */
.figs{display:grid;grid-template-columns:1.2fr 1fr 1fr;border-top:2px solid var(--ink);border-bottom:1px solid var(--rule)}
.fig{padding:20px 24px 22px 0}
.fig+.fig{padding-left:24px;border-left:1px solid var(--rule)}
.fig .v{font:500 clamp(44px,5vw,60px)/1 Newsreader,Georgia,serif;letter-spacing:-.02em;font-variant-numeric:lining-nums tabular-nums}
.fig .u{font-size:15px;color:var(--ink-2);margin-left:6px}
.fig p{margin:10px 0 0;font-size:14.5px;color:var(--ink-2);line-height:1.45}
.fig p .em,.stand .em{font-weight:400}
.fig p .em{color:var(--ink)}
.key{display:inline-block;width:9px;height:9px;border-radius:50%;vertical-align:middle;margin-right:7px;position:relative;top:-1px}
.key.dash{background:none!important;border:2px solid}
.lim{margin-top:16px;position:relative;height:26px;max-width:320px}
.lim .bar{position:absolute;left:0;right:0;top:9px;height:4px;background:var(--rule);border-radius:2px}
.lim .fill{position:absolute;left:0;top:9px;height:4px;border-radius:2px;background:var(--actual);transition:width .9s cubic-bezier(.2,.8,.2,1)}
.lim .tick{position:absolute;top:2px;width:2px;height:18px;background:var(--limit)}
.lim .tl{position:absolute;top:0;font-size:12px;color:var(--limit);transform:translateX(6px)}
.verdict{display:inline-flex;align-items:center;gap:7px;margin-top:8px;font-size:14px}
/* sections */
section{margin-top:72px;position:relative}
h2{font:500 clamp(28px,3.2vw,36px)/1.15 Newsreader,Georgia,serif;letter-spacing:-.015em;margin:0 0 10px}
.dek{color:var(--ink-2);margin:0 0 20px;font-size:16.5px}
.dek .sw{white-space:nowrap}
.controls{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;margin-bottom:6px}
.range{display:inline-flex;gap:2px;border-bottom:1px solid var(--rule)}
.range button{background:none;border:0;border-bottom:2px solid transparent;padding:6px 10px;margin-bottom:-1px;cursor:pointer;color:var(--ink-2);font-size:14px}
.range button[aria-pressed="true"]{color:var(--ink);border-bottom-color:var(--ink)}
.range button:focus-visible,.toggle:focus-visible{outline:2px solid var(--actual);outline-offset:2px}
.chart{position:relative}
.chart svg{display:block;width:100%;overflow:visible}
.axis text{fill:var(--ink-3);font:12px "Atkinson Hyperlegible",sans-serif}
.lbl{font:400 12.5px "Atkinson Hyperlegible",sans-serif}
.sub-h{font-size:14px;color:var(--ink-2);margin:22px 0 4px}
.tip{position:absolute;pointer-events:none;background:var(--paper);border:1px solid var(--ink);padding:10px 12px;font-size:13.5px;min-width:190px;opacity:0;transition:opacity .1s;z-index:5;box-shadow:4px 4px 0 var(--rule)}
.tip .tt{font:600 15px Newsreader,Georgia,serif;margin-bottom:4px}
.tip .row{display:flex;justify-content:space-between;gap:16px;color:var(--ink-2)}
.tip .row .em{color:var(--ink)}
.empty{padding:48px 0;color:var(--ink-3);font-style:italic;font-family:Newsreader,Georgia,serif;font-size:18px}
/* junction */
.junction{display:grid;grid-template-columns:1.7fr 1fr;gap:32px;align-items:start}
.map{border-top:2px solid var(--ink);padding-top:14px}
.map svg{width:100%;display:block}
.flow{stroke-dasharray:3 14;stroke-linecap:round;animation:drive linear infinite}
.flow.rev{animation-direction:reverse}
@keyframes drive{to{stroke-dashoffset:-170}}
.sensor{cursor:pointer}
.sensor:focus{outline:none}
.sensor:focus-visible circle.ring{stroke:var(--actual);stroke-width:3}
.sensor circle.ring{transition:r .2s}
.sensor[aria-pressed="true"] circle.ring{stroke:var(--ink);stroke-width:2.5}
.maptext{font:400 13px "Atkinson Hyperlegible",sans-serif;fill:var(--ink)}
.mapsub{font:12px "Atkinson Hyperlegible",sans-serif;fill:var(--ink-2)}
.side{border-top:2px solid var(--ink);padding-top:14px}
.side h3{font:500 26px/1.15 Newsreader,Georgia,serif;margin:0 0 4px}
.side .role{color:var(--ink-2);font-size:14.5px;margin:0 0 16px}
.side dl{display:grid;grid-template-columns:auto 1fr;gap:8px 16px;margin:0}
.side dt{color:var(--ink-2);font-size:14px}
.side dd{margin:0;color:var(--ink);text-align:right;font-variant-numeric:tabular-nums}
.side .hint{font-size:13.5px;color:var(--ink-3);margin-top:16px}
.note-small{font-size:13px;color:var(--ink-3);margin-top:8px}
/* model + health */
.two{display:grid;grid-template-columns:1.3fr 1fr;gap:48px}
.prose p{margin:0 0 14px;font:400 19px/1.55 Newsreader,Georgia,serif}
.prose p .em{color:var(--ink)}
.eq{font-size:15px;color:var(--ink-2);border-left:3px solid var(--pred);padding:4px 0 4px 14px;margin:6px 0 16px}
.hrow{display:grid;grid-template-columns:22px 1fr auto;gap:10px;align-items:start;padding:14px 0;border-top:1px solid var(--rule)}
.hrow:last-child{border-bottom:1px solid var(--rule)}
.hrow .n{color:var(--ink)}
.hrow .w{font-size:14px;color:var(--ink-2)}
.hrow .r{text-align:right;font-size:14px;color:var(--ink-2)}
.hrow .r .em{display:block;color:var(--ink)}
footer{margin-top:72px;padding-top:0;display:flex;justify-content:space-between;gap:24px;flex-wrap:wrap;font-size:13.5px;color:var(--ink-2)}
footer p{margin:0;max-width:560px}
.sk{color:transparent!important;background:var(--paper-2);border-radius:4px}
@media (max-width:860px){.figs{grid-template-columns:1fr}.fig,.fig+.fig{padding:18px 0;border-left:0}.fig+.fig{border-top:1px solid var(--rule)}.junction,.two{grid-template-columns:1fr;gap:28px}h1{margin-top:36px}}
@media (max-width:480px){.word span{display:none}.page{padding:0 16px 48px}section{margin-top:56px}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head>
<body>
<div class="page">
<header class="mast">
 <div class="word">AirBreda <span>A27 and the air on Tilburgseweg</span></div>
 <div class="live"><span class="pulse" id="pulse"></span><span id="livetxt">Connecting</span><button class="toggle" id="theme" aria-label="Switch light or dark">Dark</button></div>
</header>

<h1>Is the A27 making Breda’s air dirtier?</h1>
<p class="stand measure" id="stand">Every hour this page collects the nitrogen dioxide (NO₂) measured in Breda and the traffic on four A27 sensors, then asks a model whether the traffic explains the air.</p>

<div class="figs">
 <div class="fig">
  <div><span class="v sk" id="f-actual">00.0</span><span class="u">µg/m³</span></div>
  <div class="lim" aria-hidden="true"><div class="bar"></div><div class="fill" id="limfill" style="width:0"></div><div class="tick" id="limtick"></div><div class="tl" id="limlbl">EU limit 40</div></div>
  <div class="verdict" id="verdict"></div>
  <p><span class="key" style="background:var(--actual)"></span><span class=em>Measured</span> NO₂ at station NL10240 <span id="f-actual-when"></span></p>
 </div>
 <div class="fig">
  <div><span class="v sk" id="f-pred">00.0</span><span class="u">µg/m³</span></div>
  <p><span class="key dash" style="border-color:var(--pred)"></span><span class=em>Expected</span> by the model from this hour’s traffic. Chance of a high hour: <span class=em id="f-risk">n/a</span></p>
 </div>
 <div class="fig">
  <div><span class="v sk" id="f-total">0,000</span><span class="u">vehicles/hour</span></div>
  <p><span class="key" style="background:var(--traffic)"></span><span class=em>Traffic</span> on all four sensors together <span id="f-total-when"></span></p>
 </div>
</div>
<p class="note-small measure" style="margin-top:12px">How to read these: NO₂ is an average over a whole hour, so "hour ending 10:00" means 09:00 to 10:00. Traffic is a snapshot of one minute, because NDW only publishes the current minute. Times are shown in Amsterdam time; the system stores everything in UTC, the world clock, which is 2 hours behind in summer.</p>

<section aria-labelledby="h-time">
 <h2 id="h-time">Hour by hour</h2>
 <p class="dek measure">If traffic drives the pollution, the <span class="sw"><span class="key dash" style="border-color:var(--pred)"></span>expected</span> line should follow the <span class="sw"><span class="key" style="background:var(--actual)"></span>measured</span> line. Move along the chart to compare any hour; the traffic below follows.</p>
 <div class="controls">
  <div class="range" role="group" aria-label="Time range"><button data-h="12">12 hours</button><button data-h="24">24 hours</button><button data-h="48" aria-pressed="true">48 hours</button></div>
  <span style="font-size:13px;color:var(--ink-3)">Open circles are suspicious readings: kept, not used for training</span>
 </div>
 <div class="chart" id="c-no2"></div>
 <p class="note-small" style="margin:6px 0 0">The red line is the EU annual limit of 40 µg/m³, used here to mark the hours that push the yearly average up. The EU hourly limit is 200 µg/m³, far above every hour measured here. From 2030 the annual limit drops to 20.</p>
 <div class="sub-h"><span class="key" style="background:var(--traffic)"></span>Vehicles per hour, four sensors combined</div>
 <div class="chart" id="c-traffic"></div>
 <p class="note-small measure" style="margin-top:8px">Each bar is the traffic in the minute before the hour, rounded to that hour so it can be paired with the NO₂ hour. The expected line only starts on Thursday evening because that is when traffic collection began; NO₂ goes back further because the air station sends its last 50 hours every time.</p>
 <div class="tip" id="tip"></div>
</section>

<section aria-labelledby="h-scatter">
 <h2 id="h-scatter">Traffic against NO₂</h2>
 <p class="dek measure">Each dot is one hour where we have both numbers. If more traffic meant more NO₂, the dots would rise from left to right. The dotted line is what the model learned.</p>
 <div class="chart" id="c-scatter"></div>
 <p class="note-small" id="scatter-note"></p>
</section>

<section aria-labelledby="h-junction">
 <h2 id="h-junction">The junction</h2>
 <p class="dek measure">Four road sensors at one A27 junction. The moving dashes run faster where more vehicles pass. Select a sensor to read it.</p>
 <div class="junction">
  <div class="map"><svg id="map" viewBox="0 0 640 330" role="group" aria-label="Schematic of the junction with four sensors"></svg>
   <div class="note-small">Schematic, not to scale. Sensor positions from NDW site codes.</div></div>
  <aside class="side" id="side" aria-live="polite"></aside>
 </div>
</section>

<section aria-labelledby="h-say">
 <div class="two">
  <div class="prose">
   <h2 id="h-say">What we can say so far</h2>
   <div id="model"><p>Loading the model…</p></div>
  </div>
  <div>
   <h2 style="font-size:26px">Is the system running?</h2>
   <p class="dek" style="font-size:15px">Each hourly run records its result in the database. <a href="/health" target="_blank" rel="noopener">Open the raw health check</a></p>
   <div id="health"></div>
  </div>
 </div>
</section>



<footer>
 <p>Air quality: Luchtmeetnet (RIVM), station NL10240 Breda-Tilburgseweg. Traffic: NDW, the Dutch national road traffic data portal. Collected hourly on one AWS server in Stockholm, inside the EU.</p>
 <p id="gen">Times are Amsterdam time.</p>
</footer>
</div>

<script>
(function(){
const SITES=["hrl","hrr","vwd","vwa"];
const INFO={hrl:{name:"Main road, left carriageway",short:"Main road (L)"},hrr:{name:"Main road, right carriageway",short:"Main road (R)"},vwd:{name:"On-ramp, traffic joining",short:"On-ramp"},vwa:{name:"Off-ramp, traffic leaving",short:"Off-ramp"}};
const TZ="Europe/Amsterdam";let RANGE=48,HIST=null,SEL="hrl",SD={},HEALTH=null;
const $=id=>document.getElementById(id);
const fmt=(v,d=0)=>(v===null||v===undefined||isNaN(v))?"n/a":Number(v).toLocaleString("en-GB",{minimumFractionDigits:d,maximumFractionDigits:d});
const tm=t=>new Date(t).toLocaleTimeString("en-GB",{hour:"2-digit",minute:"2-digit",timeZone:TZ});
const dtm=t=>new Date(t).toLocaleString("en-GB",{weekday:"long",hour:"2-digit",minute:"2-digit",timeZone:TZ});
const ago=t=>{if(!t)return "never";const m=Math.round((Date.now()-Date.parse(t))/60000);return m<1?"just now":m<60?m+" minutes ago":(m/60).toFixed(1)+" hours ago"};
const css=v=>getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const NS="http://www.w3.org/2000/svg";
const el=(tag,a,p)=>{const e=document.createElementNS(NS,tag);for(const k in a)e.setAttribute(k,a[k]);if(p)p.appendChild(e);return e};
const j=async u=>{const r=await fetch(u,{cache:"no-store"});if(!r.ok)throw new Error(u+" "+r.status);return r.json()};
const thr=()=>(HIST&&HIST.threshold_ug_m3)||40;

/* theme */
function themeLabel(){const d=(document.documentElement.dataset.theme||(matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light"))==="dark";$("theme").textContent=d?"Light":"Dark"}
document.documentElement.dataset.theme="dark";try{const t=localStorage.getItem("ab-theme");if(t)document.documentElement.dataset.theme=t}catch(e){}
themeLabel();
$("theme").onclick=()=>{const cur=document.documentElement.dataset.theme||(matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light");const n=cur==="dark"?"light":"dark";document.documentElement.dataset.theme=n;try{localStorage.setItem("ab-theme",n)}catch(e){}themeLabel();drawAll()};
document.querySelectorAll(".range button").forEach(b=>b.onclick=()=>{document.querySelectorAll(".range button").forEach(x=>x.setAttribute("aria-pressed","false"));b.setAttribute("aria-pressed","true");RANGE=+b.dataset.h;drawCharts()});

/* ---------- figures + standfirst ---------- */
function drawFigures(){
 const d=SD.hrl;if(!d)return;["f-actual","f-pred","f-total"].forEach(i=>$(i).classList.remove("sk"));
 const T=thr(),a=d.no2_ug_m3,p=d.no2_ug_m3_predicted,r=d.no2_exceedance_risk;
 const vals=SITES.map(s=>SD[s]&&SD[s].intensity_veh_per_hr);const tot=vals.some(v=>v==null)?null:vals.reduce((x,y)=>x+y,0);
 $("f-actual").textContent=fmt(a,1);$("f-pred").textContent=fmt(p,1);$("f-total").textContent=fmt(tot);
 $("f-risk").textContent=r==null?"n/a":(r<0.01?"under 1%":Math.round(r*100)+"%");
 $("f-actual-when").textContent=d.timestamp?"for the hour ending "+tm(d.timestamp)+".":"";
 $("f-total-when").textContent=d.traffic_timestamp?"in the minute before "+tm(new Date(Date.parse(d.traffic_timestamp)+60e3))+".":"";
 const top=T*1.5;$("limfill").style.width=Math.min(100,(a||0)/top*100)+"%";$("limtick").style.left=(T/top*100)+"%";$("limlbl").style.left=(T/top*100)+"%";
 const above=a!=null&&a>=T;
 $("verdict").innerHTML=a==null?"":above?`<svg width="16" height="16" viewBox="0 0 16 16"><path d="M8 1.5 15 14H1z" fill="var(--limit)"/><path d="M8 6v4M8 11.8v.4" stroke="#fff" stroke-width="1.8" stroke-linecap="round"/></svg><span style="color:var(--limit)">Above the EU limit</span>`:`<svg width="16" height="16" viewBox="0 0 16 16"><circle cx="8" cy="8" r="7" fill="var(--good)"/><path d="m4.8 8.2 2.2 2.2 4.2-4.4" stroke="#fff" stroke-width="1.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg><span style="color:var(--good)">Below the EU limit</span>`;
 if(a!=null){const gap=p!=null?p-a:null;
  $("stand").innerHTML=`Right now the air on Tilburgseweg holds <span class=em>${fmt(a,1)} µg/m³</span> of nitrogen dioxide, ${above?"<span class=em>above</span>":"below"} the EU limit of ${T}. `+
  (tot!=null&&p!=null?`With <span class=em>${fmt(tot)} vehicles an hour</span> passing the junction, our model expected <span class=em>${fmt(p,1)}</span>, ${Math.abs(gap)<2?"close to what was measured.":gap<0?fmt(Math.abs(gap),1)+" less than measured.":fmt(gap,1)+" more than measured."}`:"The traffic reading for this hour is not complete yet.")}
}

/* ---------- charts ---------- */
function niceStep(v){const raw=v/4;const p=Math.pow(10,Math.floor(Math.log10(raw)));const n=raw/p;return (n<=1?1:n<=2?2:n<=5?5:10)*p}
function series(){if(!HIST)return null;const end=Date.now(),start=end-RANGE*3600e3;
 const no2=HIST.no2.filter(p=>Date.parse(p.t)>=start&&p.value!==null),tr=HIST.traffic.filter(p=>Date.parse(p.t)>=start);
 const ts=[...no2,...tr].map(p=>Date.parse(p.t));return {no2,tr,x0:ts.length?Math.min(...ts):start,x1:Math.max(end-30*60e3,ts.length?Math.max(...ts):end)}}
function ticks(x0,x1,w){const hours=(x1-x0)/3600e3,fit=Math.max(1,Math.floor((w-70)/64)),step=[1,2,3,4,6,8,12,24].find(s=>hours/s<=fit)||24,out=[];const d=new Date(x0);d.setMinutes(0,0,0);for(let t=d.getTime();t<=x1;t+=3600e3){const h=+new Date(t).toLocaleString("en-GB",{hour:"2-digit",hour12:false,timeZone:TZ});if(t>=x0&&h%step===0)out.push(t)}return out}
function tlabel(t){const h=new Date(t).toLocaleString("en-GB",{hour:"2-digit",minute:"2-digit",timeZone:TZ});return h==="00:00"?new Date(t).toLocaleDateString("en-GB",{weekday:"short",timeZone:TZ}):h}

function drawNO2(){
 const box=$("c-no2");box.innerHTML="";const S=series();if(!S||!S.no2.length){box.innerHTML='<div class="empty">No measurements in this range yet.</div>';box._g=null;return}
 const W=box.clientWidth,H=Math.max(240,Math.min(340,W*.34)),m={l:34,r:W<520?12:86,t:14,b:26},T=thr();
 const svg=el("svg",{viewBox:`0 0 ${W} ${H}`,height:H,role:"img","aria-label":"Measured and expected NO2 per hour"},box);
 const preds=S.tr.filter(p=>p.predicted!==null);const hi=Math.max(T*1.12,...S.no2.map(p=>p.value),...preds.map(p=>p.predicted));const step=niceStep(hi),ymax=step*Math.ceil(hi/step);
 const X=t=>m.l+(t-S.x0)/(S.x1-S.x0||1)*(W-m.l-m.r),Y=v=>m.t+(1-v/ymax)*(H-m.t-m.b);
 const g=el("g",{class:"axis"},svg);
 for(let v=0;v<=ymax+1e-9;v+=step){el("line",{x1:m.l,x2:W-m.r,y1:Y(v),y2:Y(v),stroke:css("--rule"),"stroke-width":v===0?1.5:1},g);const t=el("text",{x:m.l-8,y:Y(v)+4,"text-anchor":"end"},g);t.textContent=fmt(v)}
 ticks(S.x0,S.x1,W).forEach(t=>{const x=el("text",{x:X(t),y:H-6,"text-anchor":"middle"},g);x.textContent=tlabel(t)});
 el("line",{x1:m.l,x2:W-m.r,y1:Y(T),y2:Y(T),stroke:css("--limit"),"stroke-width":1.5},svg);
 const lt=el("text",{x:m.l+6,y:Y(T)-7,class:"lbl",fill:css("--limit")},svg);lt.textContent="EU limit, "+T+" µg/m³";
 const path=(pts,key)=>{let d="",prev=null;pts.forEach(p=>{const t=Date.parse(p.t);d+=(d===""||(prev!==null&&t-prev>1.5*3600e3)?"M":"L")+X(t).toFixed(1)+","+Y(p[key]).toFixed(1);prev=t});return d};
 const pa=el("path",{d:path(S.no2,"value"),fill:"none",stroke:css("--actual"),"stroke-width":2.25,"stroke-linejoin":"round","stroke-linecap":"round"},svg);
 if(!drawNO2.done&&pa.getTotalLength&&!matchMedia("(prefers-reduced-motion: reduce)").matches){const L=pa.getTotalLength();pa.style.strokeDasharray=L;pa.style.strokeDashoffset=L;pa.getBoundingClientRect();pa.style.transition="stroke-dashoffset 1.4s cubic-bezier(.3,.7,.2,1)";pa.style.strokeDashoffset=0;setTimeout(()=>pa.style.strokeDasharray="",1500)}
 drawNO2.done=true;
 if(preds.length)el("path",{d:path(preds,"predicted"),fill:"none",stroke:css("--pred"),"stroke-width":2.25,"stroke-dasharray":"1 5","stroke-linecap":"round"},svg);
 S.no2.filter(p=>p.flagged).forEach(p=>el("circle",{cx:X(Date.parse(p.t)),cy:Y(p.value),r:4.5,fill:css("--paper"),stroke:css("--actual"),"stroke-width":2},svg));
 // direct labels at line ends
 const la=S.no2[S.no2.length-1],lp=preds[preds.length-1];
 el("circle",{cx:X(Date.parse(la.t)),cy:Y(la.value),r:4.5,fill:css("--actual"),stroke:css("--paper"),"stroke-width":2},svg);
 if(W>=520){let ya=Y(la.value),yp=lp?Y(lp.predicted):null;if(yp!==null&&Math.abs(ya-yp)<16){if(ya<yp){ya-=8;yp+=8}else{ya+=8;yp-=8}}
  const ta=el("text",{x:W-m.r+10,y:ya+4,class:"lbl",fill:css("--ink")},svg);ta.textContent="Measured";
  if(lp){const tp=el("text",{x:W-m.r+10,y:yp+4,class:"lbl",fill:css("--ink")},svg);tp.textContent="Expected"}}
 const cross=el("line",{y1:m.t,y2:H-m.b,stroke:css("--ink"),"stroke-width":1,opacity:0},svg);
 const ha=el("circle",{r:5,fill:css("--actual"),stroke:css("--paper"),"stroke-width":2,opacity:0},svg),hp=el("circle",{r:5,fill:css("--pred"),stroke:css("--paper"),"stroke-width":2,opacity:0},svg);
 const hit=el("rect",{x:m.l,y:0,width:W-m.l-m.r,height:H,fill:"transparent"},svg);
 box._g={X,Y,S,cross,ha,hp};bindHover(hit,svg,S,m,W);
}
function drawTraffic(){
 const box=$("c-traffic");box.innerHTML="";const S=series();if(!S||!S.tr.length){box.innerHTML='<div class="empty">No traffic in this range yet.</div>';box._g=null;return}
 const W=box.clientWidth,H=120,m={l:34,r:W<520?12:86,t:8,b:24};
 const svg=el("svg",{viewBox:`0 0 ${W} ${H}`,height:H,role:"img","aria-label":"Vehicles per hour"},box);
 const vals=S.tr.map(p=>p.total).filter(v=>v!==null),hi=Math.max(1,...vals),step=niceStep(hi*2)/1,ymax=Math.ceil(hi/step)*step||step;
 const X=t=>m.l+(t-S.x0)/(S.x1-S.x0||1)*(W-m.l-m.r),Y=v=>m.t+(1-v/ymax)*(H-m.t-m.b);
 const g=el("g",{class:"axis"},svg);
 [0,ymax].forEach(v=>{el("line",{x1:m.l,x2:W-m.r,y1:Y(v),y2:Y(v),stroke:css("--rule"),"stroke-width":v===0?1.5:1},g);const t=el("text",{x:m.l-8,y:Y(v)+4,"text-anchor":"end"},g);t.textContent=v>=1000?fmt(v/1000,1)+"k":fmt(v)});
 ticks(S.x0,S.x1,W).forEach(t=>{const x=el("text",{x:X(t),y:H-6,"text-anchor":"middle"},g);x.textContent=tlabel(t)});
 const bw=Math.max(2,Math.min(16,(W-m.l-m.r)/Math.max(RANGE,1)-2)),bars=[];
 S.tr.forEach(p=>{if(p.total===null)return;const x=X(Date.parse(p.t))-bw/2,y=Y(p.total),h=Y(0)-y,r=Math.min(3,bw/2,h);
  bars.push([Date.parse(p.t),el("path",{d:`M${x},${Y(0)}V${y+r}Q${x},${y} ${x+r},${y}H${x+bw-r}Q${x+bw},${y} ${x+bw},${y+r}V${Y(0)}Z`,fill:css("--traffic")},svg)])});
 const cross=el("line",{y1:m.t,y2:H-m.b,stroke:css("--ink"),"stroke-width":1,opacity:0},svg);
 const hit=el("rect",{x:m.l,y:0,width:W-m.l-m.r,height:H,fill:"transparent"},svg);
 box._g={X,cross,bars};bindHover(hit,svg,S,m,W);
}
function bindHover(hit,svg,S,m,W){const mv=e=>{const r=svg.getBoundingClientRect(),cx=(e.touches?e.touches[0].clientX:e.clientX)-r.left;hover(S.x0+(cx-m.l)/(W-m.l-m.r)*(S.x1-S.x0))};
 hit.addEventListener("mousemove",mv);hit.addEventListener("touchstart",mv,{passive:true});hit.addEventListener("touchmove",mv,{passive:true});hit.addEventListener("mouseleave",()=>hover(null))}
function near(arr,t){let b=null,bd=1e18;arr.forEach(p=>{const d=Math.abs(Date.parse(p.t)-t);if(d<bd){bd=d;b=p}});return bd<=45*60e3?b:null}
function hover(t){
 const a=$("c-no2")._g,b=$("c-traffic")._g,tip=$("tip");
 if(t===null||!a){if(a){a.cross.setAttribute("opacity",0);a.ha.setAttribute("opacity",0);a.hp.setAttribute("opacity",0)}if(b){b.cross.setAttribute("opacity",0);b.bars.forEach(([,p])=>p.setAttribute("opacity",1))}tip.style.opacity=0;return}
 const pn=near(a.S.no2,t),pt=near(a.S.tr,t),snap=pn?Date.parse(pn.t):pt?Date.parse(pt.t):null;if(snap===null)return;
 const x=a.X(snap);a.cross.setAttribute("x1",x);a.cross.setAttribute("x2",x);a.cross.setAttribute("opacity",.35);
 if(pn){a.ha.setAttribute("cx",x);a.ha.setAttribute("cy",a.Y(pn.value));a.ha.setAttribute("opacity",1)}else a.ha.setAttribute("opacity",0);
 if(pt&&pt.predicted!=null){a.hp.setAttribute("cx",x);a.hp.setAttribute("cy",a.Y(pt.predicted));a.hp.setAttribute("opacity",1)}else a.hp.setAttribute("opacity",0);
 if(b){const bx=b.X(snap);b.cross.setAttribute("x1",bx);b.cross.setAttribute("x2",bx);b.cross.setAttribute("opacity",.35);b.bars.forEach(([tt,p])=>p.setAttribute("opacity",Math.abs(tt-snap)<30*60e3?1:.4))}
 const diff=pn&&pt&&pt.predicted!=null?pt.predicted-pn.value:null;
 tip.innerHTML=`<div class="tt">${dtm(snap)}</div><div class="row"><span><span class="key" style="background:var(--actual)"></span>Measured</span><span class=em>${pn?fmt(pn.value,1):"n/a"}</span></div><div class="row"><span><span class="key dash" style="border-color:var(--pred)"></span>Expected</span><span class=em>${pt&&pt.predicted!=null?fmt(pt.predicted,1):"n/a"}</span></div><div class="row"><span><span class="key" style="background:var(--traffic)"></span>Vehicles/hour</span><span class=em>${pt&&pt.total!=null?fmt(pt.total):"n/a"}</span></div>`+(diff!=null?`<div class="row" style="margin-top:4px;border-top:1px solid var(--rule);padding-top:4px"><span>Model off by</span><span class=em>${diff>0?"+":""}${fmt(diff,1)}</span></div>`:"")+(pn&&pn.flagged?`<div class="row"><span>Suspicious reading, kept</span></div>`:"");
 const sec=tip.parentElement.getBoundingClientRect(),c=$("c-no2").getBoundingClientRect();let left=c.left-sec.left+x+16;if(left+210>sec.width)left=c.left-sec.left+x-226;
 tip.style.left=Math.max(0,left)+"px";tip.style.top=(c.top-sec.top+8)+"px";tip.style.opacity=1;
}

function drawScatter(){
 const box=$("c-scatter");if(!box)return;box.innerHTML="";if(!HIST){return}
 const tot={};HIST.traffic.forEach(p=>{if(p.total!=null)tot[p.t]=p.total});
 const pts=HIST.no2.filter(p=>p.value!=null&&!p.flagged&&tot[p.t]!=null).map(p=>({x:tot[p.t],y:p.value,t:p.t}));
 if(pts.length<2){box.innerHTML='<div class="empty">Not enough matching hours yet.</div>';$("scatter-note").textContent="";return}
 const W=box.clientWidth,H=Math.max(240,Math.min(320,W*.3)),m={l:40,r:16,t:12,b:40};
 const svg=el("svg",{viewBox:`0 0 ${W} ${H}`,height:H,role:"img","aria-label":"Scatter of traffic against NO2 per hour"},box);
 const xs=pts.map(p=>p.x),ys=pts.map(p=>p.y),M=HIST.model;
 let x0=Math.min(...xs),x1=Math.max(...xs);const pad=(x1-x0)*.08||100;x0=Math.max(0,x0-pad);x1+=pad;
 const yst=niceStep(Math.max(...ys,thr())),ymax=yst*Math.ceil(Math.max(...ys)*1.1/yst);
 const X=v=>m.l+(v-x0)/(x1-x0)*(W-m.l-m.r),Y=v=>m.t+(1-v/ymax)*(H-m.t-m.b);
 const g=el("g",{class:"axis"},svg);
 for(let v=0;v<=ymax+1e-9;v+=yst){el("line",{x1:m.l,x2:W-m.r,y1:Y(v),y2:Y(v),stroke:css("--rule"),"stroke-width":v===0?1.5:1},g);const t=el("text",{x:m.l-8,y:Y(v)+4,"text-anchor":"end"},g);t.textContent=fmt(v)}
 const xst=niceStep(x1-x0);for(let v=Math.ceil(x0/xst)*xst;v<=x1;v+=xst){const t=el("text",{x:X(v),y:H-22,"text-anchor":"middle"},g);t.textContent=v>=1000?fmt(v/1000,1)+"k":fmt(v)}
 const xl=el("text",{x:(m.l+W-m.r)/2,y:H-4,"text-anchor":"middle"},g);xl.textContent="Vehicles per hour, four sensors combined";
 const yl=el("text",{x:m.l,y:m.t-2,"text-anchor":"start"},g);yl.textContent="";
 if(M){const hrs=pts.map(p=>new Date(p.t).getUTCHours()),mh=hrs.reduce((a,b)=>a+b,0)/hrs.length,f=x=>M.intercept+M.coef_traffic*x+M.coef_hour*mh;
  el("line",{x1:X(x0),y1:Y(Math.max(0,f(x0))),x2:X(x1),y2:Y(Math.max(0,f(x1))),stroke:css("--pred"),"stroke-width":2.25,"stroke-dasharray":"1 5","stroke-linecap":"round"},svg)}
 pts.forEach(p=>{const c=el("circle",{cx:X(p.x),cy:Y(p.y),r:5,fill:css("--actual"),"fill-opacity":.85,stroke:css("--paper"),"stroke-width":1.5},svg);const tt=el("title",{},c);tt.textContent=`${dtm(p.t)}: ${fmt(p.x)} vehicles/hour, ${fmt(p.y,1)} µg/m³ NO₂`});
 const per=M?M.coef_traffic*1000:null;
 $("scatter-note").textContent=`${pts.length} matching hours. `+(per==null?"":`The model's line: ${per<0?"minus":"plus"} ${fmt(Math.abs(per),1)} µg/m³ for every extra 1,000 vehicles an hour. With this few hours, one or two dots can tilt the line.`);
}
function drawCharts(){drawNO2();drawTraffic();drawScatter()}

/* ---------- junction map ---------- */
function intensity(s){const L=HIST&&HIST.sites_latest&&HIST.sites_latest[s];return (SD[s]&&SD[s].intensity_veh_per_hr)??(L&&L.intensity_veh_per_hr)??null}
function drawMap(){
 const svg=$("map");svg.innerHTML="";const road=css("--road"),edge=css("--road-edge"),lane=css("--lane"),tc=css("--traffic");
 const defs=el("defs",{},svg);const rg=el("radialGradient",{id:"plume"},defs);el("stop",{offset:"0%","stop-color":css("--actual"),"stop-opacity":.35},rg);el("stop",{offset:"100%","stop-color":css("--actual"),"stop-opacity":0},rg);
 // main carriageways
 const mainL="M0 120 H640",mainR="M0 176 H640";
 // ramps on the right carriageway side
 const on="M140 300 C 220 292, 250 198, 350 180",off="M420 180 C 510 198, 540 290, 610 300";
 [on,off].forEach(d=>{el("path",{d,stroke:edge,"stroke-width":22,fill:"none","stroke-linecap":"round"},svg);el("path",{d,stroke:road,"stroke-width":18,fill:"none","stroke-linecap":"round"},svg)});
 [mainL,mainR].forEach(d=>{el("path",{d,stroke:edge,"stroke-width":40,fill:"none"},svg);el("path",{d,stroke:road,"stroke-width":36,fill:"none"},svg);el("path",{d,stroke:lane,"stroke-width":1.5,"stroke-dasharray":"14 12",fill:"none"},svg)});
 // flowing traffic (speed ~ intensity)
 const mx=Math.max(1,...SITES.map(intensity).filter(v=>v!=null));
 const flows={hrl:[mainL,true],hrr:[mainR,false],vwd:[on,false],vwa:[off,false]};
 SITES.forEach(s=>{const v=intensity(s);if(v==null||v<=0)return;const dur=Math.max(1.4,9-7*(v/mx));
  const p=el("path",{d:flows[s][0],stroke:tc,"stroke-width":s.startsWith("hr")?4:3,fill:"none",class:"flow"+(flows[s][1]?" rev":"")},svg);p.style.animationDuration=dur+"s"});
 // direction labels
 const t1=el("text",{x:12,y:96,class:"mapsub"},svg);t1.textContent="A27, carriageway L";
 const t2=el("text",{x:12,y:212,class:"mapsub"},svg);t2.textContent="A27, carriageway R";
 // air station + plume
 const a=SD.hrl&&SD.hrl.no2_ug_m3,T=thr(),pr=a==null?30:30+Math.min(60,a/T*45);
 el("circle",{cx:560,cy:46,r:pr,fill:"url(#plume)"},svg);
 el("rect",{x:551,y:37,width:18,height:18,rx:3,fill:css("--actual")},svg);
 const st=el("text",{x:540,y:44,"text-anchor":"end",class:"maptext"},svg);st.textContent="Air station NL10240";
 const st2=el("text",{x:540,y:61,"text-anchor":"end",class:"mapsub"},svg);st2.textContent=(a==null?"n/a":fmt(a,1))+" µg/m³ NO₂, Tilburgseweg";
 // sensors
 const pos={hrl:[190,120],hrr:[190,176],vwd:[262,236],vwa:[478,232]};
 SITES.forEach(s=>{const [x,y]=pos[s],v=intensity(s);
  const g=el("g",{class:"sensor",tabindex:0,role:"button","aria-pressed":String(s===SEL),"aria-label":`${INFO[s].name}: ${fmt(v)} vehicles per hour`},svg);
  el("circle",{cx:x,cy:y,r:22,fill:"transparent"},g);
  el("circle",{class:"ring",cx:x,cy:y,r:s===SEL?13:11,fill:css("--paper"),stroke:css("--ink-2"),"stroke-width":1.5},g);
  el("circle",{cx:x,cy:y,r:5,fill:css("--ink")},g);
  const lx=s==="vwd"?x-20:s==="vwa"?x+20:x,ly=s==="hrl"?y-30:s==="hrr"?y+44:y+5,anc=s==="vwd"?"end":s==="vwa"?"start":"middle";
  const tt=el("text",{x:lx,y:ly,"text-anchor":anc,class:"maptext"},g);tt.textContent=s.toUpperCase()+"  "+fmt(v);
  const sel=()=>{SEL=s;drawMap();drawSide()};g.addEventListener("click",sel);g.addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();sel();setTimeout(()=>{const n=[...$("map").querySelectorAll(".sensor")][SITES.indexOf(s)];n&&n.focus()},0)}})});
}
function drawSide(){
 const s=SEL,L=(HIST&&HIST.sites_latest&&HIST.sites_latest[s])||{},d=SD[s]||{},v=intensity(s);
 const tot=SITES.map(intensity);const share=tot.some(x=>x==null)||v==null?null:v/tot.reduce((a,b)=>a+b,0);
 $("side").innerHTML=`<h3>${s.toUpperCase()}</h3><p class="role">${INFO[s].name}</p><dl>
 <dt>Vehicles per hour</dt><dd>${fmt(v)}</dd><dt>Share of the junction</dt><dd>${share==null?"n/a":Math.round(share*100)+"%"}</dd>
 <dt>Average speed</dt><dd>${L.avg_speed_kmh!=null?fmt(L.avg_speed_kmh)+" km/h":"n/a"}</dd><dt>Measured at</dt><dd>${(L.measurement_time||d.traffic_timestamp)?tm(L.measurement_time||d.traffic_timestamp):"n/a"}</dd></dl>
 <p class="hint">A speed of −1 from NDW means no car was measured in that lane; those values are dropped, not averaged in.</p>`;
}

/* ---------- model + health ---------- */
function drawModel(){
 const M=HIST&&HIST.model;if(!M){$("model").innerHTML="<p>The model is not available right now. The measured values above still update every hour.</p>";return}
 const per1000=M.coef_traffic*1000,neg=per1000<0;
 $("model").innerHTML=`<p>The model is a straight line: it predicts NO₂ from the total traffic and the hour of the day. Right now it says that every extra 1,000 vehicles an hour ${neg?"<span class=em>lowers</span>":"<span class=em>raises</span>"} NO₂ by <span class=em>${fmt(Math.abs(per1000),1)} µg/m³</span>.</p>`+
 (neg?`<p>That direction is not believable yet. It comes from very few hours, most of them at night, when traffic falls but still air can keep pollution near the ground. A handful of odd hours is enough to flip the line.</p>`:`<p>That is the direction you would expect, but it rests on very few hours. Weather, wind and weekends are not in the model yet.</p>`)+
 `<p><span class=em>Honest answer:</span> the pipeline works end to end, but it needs weeks of data before it can answer the question.</p>
 <div class="eq">NO₂ = ${fmt(M.intercept,1)} ${M.coef_traffic<0?"−":"+"} ${fmt(Math.abs(per1000),2)} × thousand vehicles/hour ${M.coef_hour<0?"−":"+"} ${fmt(Math.abs(M.coef_hour),2)} × hour of day (UTC)</div>`;
}
function drawHealth(){
 const h=HEALTH,rows=[["luchtmeetnet","Air collector","Luchtmeetnet NO₂ into the database"],["ndw","Traffic collector","NDW traffic into file storage"]];
 $("health").innerHTML=rows.map(([k,n,w])=>{const s=h&&h[k],ok=s&&s.last_successful_fetch&&(Date.now()-Date.parse(s.last_successful_fetch))<2*3600e3;
  const ic=ok?`<svg width="18" height="18" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="var(--good)"/><path d="m4.8 8.2 2.2 2.2 4.2-4.4" stroke="#fff" stroke-width="1.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>`:`<svg width="18" height="18" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1.5 15 14H1z" fill="var(--limit)"/><path d="M8 6v4M8 11.8v.4" stroke="#fff" stroke-width="1.8" stroke-linecap="round"/></svg>`;
  return `<div class="hrow">${ic}<div><div class="n">${n}: ${ok?"running":"late"}</div><div class="w">${w}</div></div><div class="r"><span class=em>${s?ago(s.last_successful_fetch):"n/a"}</span>${s?fmt(s.bad_data_count)+(s.bad_data_count===1?" bad value":" bad values")+" in 24 h":"no answer"}</div></div>`}).join("");
 const ok=h&&h.status==="ok";$("pulse").className="pulse"+(ok?"":" off");
}

function drawAll(){drawFigures();drawCharts();drawMap();drawSide();drawModel();drawHealth()}

async function load(){
 try{const [sites,h]=await Promise.all([Promise.all(SITES.map(s=>j("/site/"+s).catch(()=>null))),j("/health").catch(()=>null)]);
  SITES.forEach((s,i)=>{if(sites[i])SD[s]=sites[i]});HEALTH=h;
  $("livetxt").textContent="Live, updated "+new Date().toLocaleTimeString("en-GB",{hour:"2-digit",minute:"2-digit",timeZone:TZ});
 }catch(e){$("livetxt").textContent="Offline, retrying every minute";$("pulse").className="pulse off"}
 drawFigures();drawMap();drawSide();drawHealth();
 try{HIST=await j("/history?hours=48");$("gen").textContent="Times are Amsterdam time. Chart data from "+tm(HIST.generated_at)+".";drawAll()}
 catch(e){$("c-no2").innerHTML='<div class="empty">The hour-by-hour history could not be loaded. The live figures above still work; this retries every minute.</div>'}
}
let rt;addEventListener("resize",()=>{clearTimeout(rt);rt=setTimeout(()=>{drawCharts();drawMap()},150)});
load();setInterval(load,60000);
})();
</script>
</body></html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
