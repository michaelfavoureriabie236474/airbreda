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
<title>AirBreda · live air and traffic</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">
<style>
:root{
 color-scheme:light;
 --bg:#f3f4f1; --card:#fcfcfb; --card-2:#f6f6f3; --line:#e4e3de; --grid:#ecebe6;
 --ink:#0b0b0b; --ink-2:#52514e; --ink-3:#8a8984;
 --s-actual:#2a78d6; --s-pred:#eb6834; --s-traffic:#1baf7a; --s-traffic-soft:#bfe9d8;
 --good:#0ca30c; --warn:#fab219; --crit:#d03b3b;
 --accent:#2a78d6; --shadow:0 1px 2px rgba(20,20,10,.04),0 8px 24px rgba(20,20,10,.06);
}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])){
 color-scheme:dark;
 --bg:#121211; --card:#1a1a19; --card-2:#212120; --line:#2e2e2c; --grid:#262624;
 --ink:#ffffff; --ink-2:#c3c2b7; --ink-3:#8f8e86;
 --s-actual:#3987e5; --s-pred:#d95926; --s-traffic:#199e70; --s-traffic-soft:#1d4a3a;
 --accent:#3987e5; --shadow:0 1px 2px rgba(0,0,0,.3),0 8px 24px rgba(0,0,0,.25);
}}
:root[data-theme="dark"]{
 color-scheme:dark;
 --bg:#121211; --card:#1a1a19; --card-2:#212120; --line:#2e2e2c; --grid:#262624;
 --ink:#ffffff; --ink-2:#c3c2b7; --ink-3:#8f8e86;
 --s-actual:#3987e5; --s-pred:#d95926; --s-traffic:#199e70; --s-traffic-soft:#1d4a3a;
 --accent:#3987e5; --shadow:0 1px 2px rgba(0,0,0,.3),0 8px 24px rgba(0,0,0,.25);
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 Inter,system-ui,-apple-system,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}
.wrap{max-width:1180px;margin:0 auto;padding:28px 20px 48px}
h1,h2,.num{font-family:"Space Grotesk",Inter,system-ui,sans-serif}
header{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:22px}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:40px;height:40px;border-radius:12px;background:linear-gradient(135deg,var(--s-actual),var(--s-traffic));display:grid;place-items:center;color:#fff;font:700 15px "Space Grotesk",sans-serif;letter-spacing:-.02em}
h1{margin:0;font-size:26px;letter-spacing:-.02em;line-height:1.1}
.sub{color:var(--ink-2);font-size:13.5px;margin-top:2px}
.hdr-right{display:flex;align-items:center;gap:10px}
.pill{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border-radius:999px;background:var(--card);border:1px solid var(--line);font-size:13px;color:var(--ink-2)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--good);box-shadow:0 0 0 0 rgba(12,163,12,.5);animation:pulse 2s infinite}
.dot.bad{background:var(--crit);animation:none}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(12,163,12,.45)}70%{box-shadow:0 0 0 8px rgba(12,163,12,0)}100%{box-shadow:0 0 0 0 rgba(12,163,12,0)}}
button{font:inherit;color:inherit}
.iconbtn{width:36px;height:36px;border-radius:10px;border:1px solid var(--line);background:var(--card);cursor:pointer;display:grid;place-items:center;color:var(--ink-2)}
.iconbtn:hover{color:var(--ink)}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;box-shadow:var(--shadow)}
.hero{display:grid;grid-template-columns:1.25fr 1fr 1fr;gap:14px;margin-bottom:14px}
.kpi{padding:20px 22px;position:relative;overflow:hidden}
.kpi .label{font-size:13px;color:var(--ink-2);font-weight:500;display:flex;align-items:center;gap:8px}
.kpi .num{font-size:52px;font-weight:600;letter-spacing:-.03em;line-height:1.05;margin-top:10px}
.kpi .num small{font:500 15px Inter,sans-serif;color:var(--ink-2);margin-left:6px;letter-spacing:0}
.kpi .foot{font-size:12.5px;color:var(--ink-3);margin-top:10px}
.swatch{width:10px;height:10px;border-radius:3px;display:inline-block}
.status{display:inline-flex;align-items:center;gap:6px;font-size:12.5px;font-weight:600;padding:4px 10px;border-radius:999px;margin-top:12px;border:1px solid var(--line);background:var(--card-2);color:var(--ink)}
.status svg{flex:none}
.scale{margin-top:14px;position:relative;height:30px}
.scale .track{position:absolute;left:0;right:0;top:8px;height:8px;border-radius:999px;background:linear-gradient(90deg,var(--good),var(--warn) 66%,var(--crit))}
.scale .marker{position:absolute;top:2px;width:4px;height:20px;border-radius:3px;background:var(--ink);box-shadow:0 0 0 2px var(--card);transform:translateX(-2px);transition:left .6s cubic-bezier(.2,.8,.2,1)}
.scale .lim{position:absolute;top:22px;font-size:11px;color:var(--ink-3);transform:translateX(-50%)}
.gauge{display:flex;align-items:center;gap:16px;margin-top:6px}
.gauge svg{flex:none}
.gauge .gnum{font:600 30px "Space Grotesk",sans-serif;letter-spacing:-.02em}
.mini{margin-top:10px;height:44px}
.section{padding:20px 22px 14px;margin-bottom:14px;position:relative}
.sec-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:6px}
h2{margin:0;font-size:18px;letter-spacing:-.01em}
.sec-sub{color:var(--ink-2);font-size:13px;margin-top:2px;max-width:640px}
.seg{display:inline-flex;background:var(--card-2);border:1px solid var(--line);border-radius:10px;padding:3px}
.seg button{border:0;background:transparent;padding:5px 12px;border-radius:7px;cursor:pointer;font-size:13px;color:var(--ink-2)}
.seg button[aria-pressed="true"]{background:var(--card);color:var(--ink);box-shadow:0 1px 2px rgba(0,0,0,.08);font-weight:600}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:12.5px;color:var(--ink-2);margin:10px 0 4px}
.legend span{display:inline-flex;align-items:center;gap:6px}
.lg-line{width:18px;height:0;border-top:2px solid}
.lg-dash{width:18px;height:0;border-top:2px dashed}
.chart{position:relative;width:100%}
.chart svg{display:block;width:100%;overflow:visible}
.tip{position:absolute;pointer-events:none;background:var(--card);border:1px solid var(--line);border-radius:10px;box-shadow:var(--shadow);padding:9px 11px;font-size:12.5px;min-width:170px;opacity:0;transition:opacity .12s;z-index:5}
.tip .tt{font-weight:600;margin-bottom:4px;color:var(--ink)}
.tip .row{display:flex;justify-content:space-between;gap:14px;color:var(--ink-2)}
.tip .row b{color:var(--ink);font-weight:600}
.tip .row i{font-style:normal;display:inline-flex;align-items:center;gap:6px}
.axis text{fill:var(--ink-3);font-size:11px}
.empty{padding:40px 0;text-align:center;color:var(--ink-3)}
.grid2{display:grid;grid-template-columns:1.4fr 1fr;gap:14px}
.sensors{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-top:12px}
.sensor{all:unset;cursor:pointer;display:block;padding:14px;border-radius:12px;border:1px solid var(--line);background:var(--card-2);transition:border-color .15s,transform .15s}
.sensor:hover{transform:translateY(-1px);border-color:var(--ink-3)}
.sensor[aria-pressed="true"]{border-color:var(--accent);box-shadow:0 0 0 3px color-mix(in srgb,var(--accent) 22%,transparent)}
.sensor:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.sensor .sid{font:600 15px "Space Grotesk",sans-serif;display:flex;justify-content:space-between;align-items:center}
.sensor .role{font-size:12px;color:var(--ink-2)}
.sensor .val{font:600 24px "Space Grotesk",sans-serif;margin-top:8px;letter-spacing:-.02em}
.sensor .val small{font:500 12px Inter,sans-serif;color:var(--ink-2);margin-left:4px}
.bar{height:6px;border-radius:999px;background:var(--grid);margin-top:8px;overflow:hidden}
.bar>div{height:100%;border-radius:999px;background:var(--s-traffic);transition:width .6s cubic-bezier(.2,.8,.2,1)}
.sensor .meta{font-size:11.5px;color:var(--ink-3);margin-top:6px}
pre{margin:12px 0 0;background:var(--card-2);border:1px solid var(--line);border-radius:12px;padding:14px;font:12.5px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;overflow:auto;color:var(--ink)}
.k{color:var(--s-actual)} .s{color:var(--s-traffic)} .n{color:var(--s-pred)} .nl{color:var(--ink-3)}
.api-row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-top:12px;flex-wrap:wrap}
.api-row code{font:12.5px ui-monospace,Menlo,monospace;background:var(--card-2);border:1px solid var(--line);padding:4px 8px;border-radius:8px}
.link{color:var(--accent);text-decoration:none;font-size:13px;font-weight:600}
.link:hover{text-decoration:underline}
.health{display:grid;gap:10px;margin-top:12px}
.hrow{display:grid;grid-template-columns:auto 1fr auto;gap:12px;align-items:center;padding:12px 14px;border:1px solid var(--line);border-radius:12px;background:var(--card-2)}
.hrow .name{font-weight:600;font-size:14px}
.hrow .what{font-size:12px;color:var(--ink-2)}
.hrow .right{text-align:right;font-size:12px;color:var(--ink-2)}
.hrow .right b{display:block;color:var(--ink);font-size:13px}
.flow{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:12px}
.step{padding:14px;border-radius:12px;background:var(--card-2);border:1px solid var(--line);font-size:13px;color:var(--ink-2)}
.step b{display:block;color:var(--ink);font:600 14px "Space Grotesk",sans-serif;margin-bottom:2px}
.step .n{font:600 12px Inter,sans-serif;color:var(--ink-3)}
.model{margin-top:12px;font-size:13px;color:var(--ink-2)}
.model b{color:var(--ink)}
.note{margin-top:10px;padding:10px 12px;border-radius:10px;border:1px dashed var(--line);font-size:12.5px;color:var(--ink-2)}
footer{color:var(--ink-3);font-size:12px;text-align:center;margin-top:20px}
.sk{background:linear-gradient(90deg,var(--grid),var(--card-2),var(--grid));background-size:200% 100%;animation:sk 1.2s infinite;border-radius:8px;color:transparent!important}
@keyframes sk{to{background-position:-200% 0}}
@media (max-width:900px){.hero{grid-template-columns:1fr}.grid2{grid-template-columns:1fr}.flow{grid-template-columns:repeat(2,1fr)}}
@media (max-width:520px){.wrap{padding:18px 16px 36px}.kpi .num{font-size:42px}.sensors{grid-template-columns:1fr}.flow{grid-template-columns:1fr}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head>
<body>
<div class="wrap">
<header>
 <div class="brand"><div class="logo">AB</div>
  <div><h1>AirBreda</h1><div class="sub">Does traffic on the A27 make Breda's air dirtier? Live answer, every hour.</div></div></div>
 <div class="hdr-right">
  <span class="pill" id="live"><span class="dot" id="livedot"></span><span id="livetxt">Connecting…</span></span>
  <button class="iconbtn" id="theme" aria-label="Toggle light or dark theme" title="Light / dark">
   <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg></button>
 </div>
</header>

<section class="hero">
 <div class="card kpi">
  <div class="label"><span class="swatch" style="background:var(--s-actual)"></span>Actual NO₂ now · station NL10240</div>
  <div class="num"><span id="actual" class="sk">00.0</span><small>µg/m³</small></div>
  <div class="scale" aria-hidden="true"><div class="track"></div><div class="marker" id="marker" style="left:0%"></div><div class="lim" id="limlbl">EU limit 40</div></div>
  <div class="status" id="status">…</div>
  <div class="foot" id="actualfoot">Measured by Luchtmeetnet (RIVM)</div>
 </div>
 <div class="card kpi">
  <div class="label"><span class="swatch" style="background:var(--s-pred)"></span>Predicted NO₂ from traffic</div>
  <div class="gauge">
   <svg width="96" height="60" viewBox="0 0 96 60" aria-hidden="true">
    <path d="M8 52 A40 40 0 0 1 88 52" fill="none" stroke="var(--grid)" stroke-width="9" stroke-linecap="round"/>
    <path id="garc" d="M8 52 A40 40 0 0 1 88 52" fill="none" stroke="var(--s-pred)" stroke-width="9" stroke-linecap="round" pathLength="100" stroke-dasharray="0 100" style="transition:stroke-dasharray .8s cubic-bezier(.2,.8,.2,1)"/>
   </svg>
   <div><div class="gnum" id="risk">–</div><div style="font-size:12.5px;color:var(--ink-2)">chance this hour is above the limit</div></div>
  </div>
  <div class="num" style="font-size:34px;margin-top:6px"><span id="pred" class="sk">00.0</span><small>µg/m³ predicted</small></div>
  <div class="foot" id="predfoot">Linear regression on total traffic + hour of day</div>
 </div>
 <div class="card kpi">
  <div class="label"><span class="swatch" style="background:var(--s-traffic)"></span>Total traffic · 4 A27 sensors</div>
  <div class="num"><span id="total" class="sk">0000</span><small>vehicles/hour</small></div>
  <div class="mini" id="spark"></div>
  <div class="foot" id="trafficfoot">From NDW (Nationaal Dataportaal Wegverkeer)</div>
 </div>
</section>

<section class="card section">
 <div class="sec-head">
  <div><h2>Actual vs predicted NO₂</h2><div class="sec-sub">If traffic drives pollution, the orange line (model) should follow the blue line (reality). Hover to compare any hour; the traffic chart below follows your cursor.</div></div>
  <div class="seg" role="group" aria-label="Time range"><button data-h="12">12 h</button><button data-h="24">24 h</button><button data-h="48" aria-pressed="true">48 h</button></div>
 </div>
 <div class="legend">
  <span><span class="lg-line" style="border-color:var(--s-actual)"></span>Actual NO₂ (µg/m³)</span>
  <span><span class="lg-dash" style="border-color:var(--s-pred)"></span>Predicted NO₂ (µg/m³)</span>
  <span><span class="lg-dash" style="border-color:var(--crit);border-top-width:1px"></span>EU limit</span>
  <span><svg width="10" height="10"><circle cx="5" cy="5" r="3.5" fill="var(--card)" stroke="var(--s-actual)" stroke-width="2"/></svg>Flagged reading (kept, not trained on)</span>
 </div>
 <div class="chart" id="c-no2"></div>
 <div style="font-size:12.5px;color:var(--ink-2);margin:8px 0 2px;display:flex;align-items:center;gap:6px"><span class="swatch" style="background:var(--s-traffic)"></span>Total traffic, vehicles/hour (all four sensors)</div>
 <div class="chart" id="c-traffic"></div>
 <div class="tip" id="tip"></div>
</section>

<div class="grid2">
 <section class="card section">
  <div class="sec-head"><div><h2>The four sensors</h2><div class="sec-sub">One junction on the A27 just north of Breda. Click a sensor to see exactly what the API returns for it.</div></div></div>
  <div class="sensors" id="sensors"></div>
  <div class="api-row"><code id="apipath">GET /site/hrl</code><a class="link" id="apilink" href="/site/hrl" target="_blank" rel="noopener">Open raw JSON ↗</a></div>
  <pre id="json">Loading…</pre>
 </section>
 <section class="card section">
  <div class="sec-head"><div><h2>System health</h2><div class="sec-sub">Each hourly run writes its result to the database; <a class="link" href="/health" target="_blank" rel="noopener">/health</a> reads it back.</div></div></div>
  <div class="health" id="health"></div>
  <h2 style="margin-top:20px">The model, honestly</h2>
  <div class="model" id="model">Loading…</div>
  <div class="note">Trained on only a few hours of data. Treat the prediction as a working pipeline, not yet as evidence. It needs weeks of data, weekends and wind to answer the question.</div>
 </section>
</div>

<section class="card section">
 <div class="sec-head"><div><h2>How a new hour arrives</h2><div class="sec-sub">One small AWS virtual machine in Stockholm (EU), no one pressing buttons.</div></div></div>
 <div class="flow">
  <div class="step"><span class="n">01</span><b>Alarm clock</b>cron starts both collectors at minute 0 of every hour.</div>
  <div class="step"><span class="n">02</span><b>Air</b>NO₂ from Luchtmeetnet into PostgreSQL. Duplicates refused, odd values flagged.</div>
  <div class="step"><span class="n">03</span><b>Traffic</b>NDW file filtered to 4 sensors, stored in S3 with the raw original. Speed −1 dropped.</div>
  <div class="step"><span class="n">04</span><b>This page</b>Reads both stores, asks the model, refreshes every 60 seconds.</div>
 </div>
</section>
<footer>AirBreda · BUas System Design &amp; Cloud Platforms · times shown in Amsterdam time · <span id="gen"></span></footer>
</div>

<script>
(function(){
const SITES=["hrl","hrr","vwd","vwa"];
const ROLE={hrl:"Main road, one direction",hrr:"Main road, other direction",vwd:"On-ramp (joining)",vwa:"Off-ramp (leaving)"};
const TZ="Europe/Amsterdam";
let RANGE=48, HIST=null, SEL="hrl", SITEDATA={}, HOVER=null;
const $=id=>document.getElementById(id);
const fmt=(v,d=0)=>(v===null||v===undefined||isNaN(v))?"–":Number(v).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const tm=t=>new Date(t).toLocaleTimeString("en-GB",{hour:"2-digit",minute:"2-digit",timeZone:TZ});
const dtm=t=>new Date(t).toLocaleString("en-GB",{weekday:"short",hour:"2-digit",minute:"2-digit",timeZone:TZ});
const ago=t=>{if(!t)return "never";const m=Math.round((Date.now()-Date.parse(t))/60000);return m<1?"just now":m<60?m+" min ago":(m/60).toFixed(1)+" h ago"};
const css=v=>getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const NS="http://www.w3.org/2000/svg";
function el(tag,attrs,parent){const e=document.createElementNS(NS,tag);for(const k in attrs)e.setAttribute(k,attrs[k]);if(parent)parent.appendChild(e);return e}
async function j(url){const r=await fetch(url,{cache:"no-store"});if(!r.ok)throw new Error(url+" "+r.status);return r.json()}

/* theme */
try{const t=localStorage.getItem("ab-theme");if(t)document.documentElement.dataset.theme=t}catch(e){}
$("theme").onclick=()=>{const cur=document.documentElement.dataset.theme||(matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light");const nxt=cur==="dark"?"light":"dark";document.documentElement.dataset.theme=nxt;try{localStorage.setItem("ab-theme",nxt)}catch(e){}drawAll()};

/* range toggle */
document.querySelectorAll(".seg button").forEach(b=>b.onclick=()=>{document.querySelectorAll(".seg button").forEach(x=>x.setAttribute("aria-pressed","false"));b.setAttribute("aria-pressed","true");RANGE=+b.dataset.h;drawAll()});

function niceStep(v){const raw=v/4;const p=Math.pow(10,Math.floor(Math.log10(raw)));const n=raw/p;return (n<=1?1:n<=2?2:n<=5?5:10)*p}
function niceMax(v){if(v<=0)return 10;const p=Math.pow(10,Math.floor(Math.log10(v)));const n=v/p;return (n<=1?1:n<=2?2:n<=2.5?2.5:n<=5?5:10)*p}

function series(){
 if(!HIST)return null;
 const end=Date.now(), start=end-RANGE*3600e3;
 const no2=HIST.no2.filter(p=>Date.parse(p.t)>=start&&p.value!==null);
 const tr=HIST.traffic.filter(p=>Date.parse(p.t)>=start);
 const times=[...no2.map(p=>Date.parse(p.t)),...tr.map(p=>Date.parse(p.t))];
 const x0=times.length?Math.min(...times):start, x1=Math.max(end-30*60e3,times.length?Math.max(...times):end);
 return {no2,tr,x0,x1};
}

function axisTicks(x0,x1,w){const hours=(x1-x0)/3600e3;const fit=Math.max(1,Math.floor((w-60)/56));const step=[1,2,3,4,6,8,12,24].find(s=>hours/s<=fit)||24;const out=[];const d=new Date(x0);d.setMinutes(0,0,0);let t=d.getTime();while(t<=x1){const h=+new Date(t).toLocaleString("en-GB",{hour:"2-digit",hour12:false,timeZone:TZ});if(t>=x0&&h%step===0)out.push(t);t+=3600e3}return out}

function drawNO2(){
 const box=$("c-no2");box.innerHTML="";const S=series();
 if(!S||(!S.no2.length&&!S.tr.length)){box.innerHTML='<div class="empty">No data in this range yet.</div>';return}
 const W=box.clientWidth,H=Math.max(220,Math.min(300,W*.32)),m={l:36,r:12,t:12,b:24};
 const svg=el("svg",{viewBox:`0 0 ${W} ${H}`,height:H,role:"img","aria-label":"Line chart of actual and predicted NO2 per hour"},box);
 const thr=HIST.threshold_ug_m3||40;
 const preds=S.tr.filter(p=>p.predicted!==null);
 const step=niceStep(Math.max(thr*1.15,...S.no2.map(p=>p.value),...preds.map(p=>p.predicted)));
 const ymax=step*Math.ceil(Math.max(thr*1.15,...S.no2.map(p=>p.value),...preds.map(p=>p.predicted))/step);
 const X=t=>m.l+(t-S.x0)/(S.x1-S.x0||1)*(W-m.l-m.r), Y=v=>m.t+(1-v/ymax)*(H-m.t-m.b);
 const g=el("g",{class:"axis"},svg);
 for(let v=0;v<=ymax+1e-9;v+=step){const y=Y(v);el("line",{x1:m.l,x2:W-m.r,y1:y,y2:y,stroke:css("--grid"),"stroke-width":1},g);const t=el("text",{x:m.l-8,y:y+4,"text-anchor":"end"},g);t.textContent=fmt(v)}
 axisTicks(S.x0,S.x1,W).forEach(t=>{const tx=el("text",{x:X(t),y:H-6,"text-anchor":"middle"},g);tx.textContent=tm(t)});
 // threshold
 el("line",{x1:m.l,x2:W-m.r,y1:Y(thr),y2:Y(thr),stroke:css("--crit"),"stroke-width":1,"stroke-dasharray":"4 4",opacity:.8},svg);
 const tl=el("text",{x:W-m.r,y:Y(thr)-6,"text-anchor":"end","font-size":11,fill:css("--ink-2")},svg);tl.textContent="EU limit "+thr+" µg/m³";
 const line=(pts,key,color,dash)=>{if(pts.length<1)return;let d="",prev=null;pts.forEach(p=>{const t=Date.parse(p.t);const gap=prev!==null&&t-prev>1.5*3600e3;d+=(d===""||gap?"M":"L")+X(t).toFixed(1)+","+Y(p[key]).toFixed(1);prev=t});
  const path=el("path",{d,fill:"none",stroke:color,"stroke-width":2,"stroke-linejoin":"round","stroke-linecap":"round"},svg);if(dash)path.setAttribute("stroke-dasharray","6 5");
  const len=path.getTotalLength?path.getTotalLength():0;if(len&&!dash&&!drawNO2.done){path.style.strokeDasharray=len;path.style.strokeDashoffset=len;path.getBoundingClientRect();path.style.transition="stroke-dashoffset 1.1s cubic-bezier(.2,.8,.2,1)";path.style.strokeDashoffset=0;setTimeout(()=>{path.style.strokeDasharray=""},1200)}};
 // soft area under actual
 if(S.no2.length>1){let d="M"+X(Date.parse(S.no2[0].t))+","+Y(0);S.no2.forEach(p=>d+="L"+X(Date.parse(p.t)).toFixed(1)+","+Y(p.value).toFixed(1));d+="L"+X(Date.parse(S.no2[S.no2.length-1].t))+","+Y(0)+"Z";
  const gid="ga";const defs=el("defs",{},svg);const lg=el("linearGradient",{id:gid,x1:0,x2:0,y1:0,y2:1},defs);el("stop",{offset:"0%","stop-color":css("--s-actual"),"stop-opacity":.18},lg);el("stop",{offset:"100%","stop-color":css("--s-actual"),"stop-opacity":0},lg);el("path",{d,fill:`url(#${gid})`},svg)}
 line(S.no2,"value",css("--s-actual"),false);
 line(preds,"predicted",css("--s-pred"),true);
 S.no2.filter(p=>p.flagged).forEach(p=>el("circle",{cx:X(Date.parse(p.t)),cy:Y(p.value),r:4,fill:css("--card"),stroke:css("--s-actual"),"stroke-width":2},svg));
 // latest point emphasis
 const last=S.no2[S.no2.length-1];if(last){el("circle",{cx:X(Date.parse(last.t)),cy:Y(last.value),r:5,fill:css("--s-actual"),stroke:css("--card"),"stroke-width":2},svg)}
 drawNO2.done=true;
 const cross=el("line",{y1:m.t,y2:H-m.b,stroke:css("--ink-3"),"stroke-width":1,opacity:0},svg);
 const ha=el("circle",{r:5,fill:css("--s-actual"),stroke:css("--card"),"stroke-width":2,opacity:0},svg);
 const hp=el("circle",{r:5,fill:css("--s-pred"),stroke:css("--card"),"stroke-width":2,opacity:0},svg);
 const hit=el("rect",{x:m.l,y:0,width:W-m.l-m.r,height:H,fill:"transparent"},svg);
 box._geo={X,Y,S,m,W,H,cross,ha,hp};
 const move=e=>{const r=svg.getBoundingClientRect();const cx=(e.touches?e.touches[0].clientX:e.clientX)-r.left;const t=S.x0+(cx-m.l)/(W-m.l-m.r)*(S.x1-S.x0);setHover(t,e)};
 hit.addEventListener("mousemove",move);hit.addEventListener("touchmove",move,{passive:true});hit.addEventListener("mouseleave",()=>setHover(null));
}

function drawTraffic(){
 const box=$("c-traffic");box.innerHTML="";const S=series();if(!S||!S.tr.length){box.innerHTML='<div class="empty">No traffic data in this range yet.</div>';return}
 const W=box.clientWidth,H=110,m={l:36,r:12,t:6,b:22};
 const svg=el("svg",{viewBox:`0 0 ${W} ${H}`,height:H,role:"img","aria-label":"Bar chart of total traffic per hour"},box);
 const vals=S.tr.map(p=>p.total).filter(v=>v!==null);const ymax=niceMax(Math.max(1,...vals));
 const X=t=>m.l+(t-S.x0)/(S.x1-S.x0||1)*(W-m.l-m.r), Y=v=>m.t+(1-v/ymax)*(H-m.t-m.b);
 const g=el("g",{class:"axis"},svg);
 [0,ymax/2,ymax].forEach(v=>{el("line",{x1:m.l,x2:W-m.r,y1:Y(v),y2:Y(v),stroke:css("--grid")},g);const t=el("text",{x:m.l-8,y:Y(v)+4,"text-anchor":"end"},g);t.textContent=v>=1000?fmt(v/1000,1)+"k":fmt(v)});
 axisTicks(S.x0,S.x1,W).forEach(t=>{const tx=el("text",{x:X(t),y:H-5,"text-anchor":"middle"},g);tx.textContent=tm(t)});
 const bw=Math.max(2,Math.min(18,(W-m.l-m.r)/Math.max(RANGE,1)-2));
 const bars=[];
 S.tr.forEach(p=>{if(p.total===null)return;const x=X(Date.parse(p.t))-bw/2,y=Y(p.total),h=Y(0)-y;const r=Math.min(4,bw/2,h);
  const d=`M${x},${Y(0)}V${y+r}Q${x},${y} ${x+r},${y}H${x+bw-r}Q${x+bw},${y} ${x+bw},${y+r}V${Y(0)}Z`;
  bars.push([Date.parse(p.t),el("path",{d,fill:css("--s-traffic")},svg)])});
 const cross=el("line",{y1:m.t,y2:H-m.b,stroke:css("--ink-3"),"stroke-width":1,opacity:0},svg);
 const hit=el("rect",{x:m.l,y:0,width:W-m.l-m.r,height:H,fill:"transparent"},svg);
 box._geo={X,cross,bars,S,m,W};
 const move=e=>{const r=svg.getBoundingClientRect();const cx=(e.touches?e.touches[0].clientX:e.clientX)-r.left;setHover(S.x0+(cx-m.l)/(W-m.l-m.r)*(S.x1-S.x0),e)};
 hit.addEventListener("mousemove",move);hit.addEventListener("touchmove",move,{passive:true});hit.addEventListener("mouseleave",()=>setHover(null));
}

function nearest(arr,t){let best=null,bd=Infinity;arr.forEach(p=>{const d=Math.abs(Date.parse(p.t)-t);if(d<bd){bd=d;best=p}});return bd<=45*60e3?best:null}

function setHover(t,e){
 const a=$("c-no2")._geo,b=$("c-traffic")._geo,tip=$("tip");
 if(t===null||!a){[a&&a.cross,a&&a.ha,a&&a.hp,b&&b.cross].forEach(x=>x&&x.setAttribute("opacity",0));if(b)b.bars.forEach(([,p])=>p.setAttribute("opacity",1));tip.style.opacity=0;return}
 const S=a.S;const pn=nearest(S.no2,t),pt=nearest(S.tr,t);const snap=pn?Date.parse(pn.t):pt?Date.parse(pt.t):null;if(snap===null)return;
 const x=a.X(snap);a.cross.setAttribute("x1",x);a.cross.setAttribute("x2",x);a.cross.setAttribute("opacity",.6);
 if(pn){a.ha.setAttribute("cx",x);a.ha.setAttribute("cy",a.Y(pn.value));a.ha.setAttribute("opacity",1)}else a.ha.setAttribute("opacity",0);
 if(pt&&pt.predicted!==null){a.hp.setAttribute("cx",x);a.hp.setAttribute("cy",a.Y(pt.predicted));a.hp.setAttribute("opacity",1)}else a.hp.setAttribute("opacity",0);
 if(b){const bx=b.X(snap);b.cross.setAttribute("x1",bx);b.cross.setAttribute("x2",bx);b.cross.setAttribute("opacity",.6);b.bars.forEach(([tt,p])=>p.setAttribute("opacity",Math.abs(tt-snap)<30*60e3?1:.45))}
 const diff=(pn&&pt&&pt.predicted!==null)?pt.predicted-pn.value:null;
 tip.innerHTML=`<div class="tt">${dtm(snap)}</div>`+
  `<div class="row"><i><span class="swatch" style="background:var(--s-actual)"></span>Actual</i><b>${pn?fmt(pn.value,1)+" µg/m³":"–"}</b></div>`+
  `<div class="row"><i><span class="swatch" style="background:var(--s-pred)"></span>Predicted</i><b>${pt&&pt.predicted!==null?fmt(pt.predicted,1)+" µg/m³":"–"}</b></div>`+
  `<div class="row"><i><span class="swatch" style="background:var(--s-traffic)"></span>Traffic</i><b>${pt&&pt.total!==null?fmt(pt.total)+" veh/h":"–"}</b></div>`+
  (diff!==null?`<div class="row" style="margin-top:4px"><i>Model error</i><b>${diff>0?"+":""}${fmt(diff,1)}</b></div>`:"")+
  (pn&&pn.flagged?`<div class="row" style="margin-top:4px"><i>⚑ flagged reading</i></div>`:"");
 const sec=tip.parentElement.getBoundingClientRect(),chart=$("c-no2").getBoundingClientRect();
 let left=chart.left-sec.left+x+14;if(left+190>sec.width)left=chart.left-sec.left+x-200;
 tip.style.left=left+"px";tip.style.top=(chart.top-sec.top+10)+"px";tip.style.opacity=1;
}

function drawSpark(){
 const box=$("spark");box.innerHTML="";if(!HIST)return;const pts=HIST.traffic.filter(p=>p.total!==null).slice(-24);if(pts.length<2)return;
 const W=box.clientWidth,H=44;const svg=el("svg",{viewBox:`0 0 ${W} ${H}`,height:H,"aria-hidden":"true"},box);
 const mx=Math.max(...pts.map(p=>p.total)),t0=Date.parse(pts[0].t),t1=Date.parse(pts[pts.length-1].t);
 const X=t=>2+(t-t0)/(t1-t0||1)*(W-8),Y=v=>4+(1-v/mx)*(H-8);
 let d="";pts.forEach((p,i)=>d+=(i?"L":"M")+X(Date.parse(p.t)).toFixed(1)+","+Y(p.total).toFixed(1));
 el("path",{d:d+`L${X(t1)},${H}L${X(t0)},${H}Z`,fill:css("--s-traffic"),opacity:.12},svg);
 el("path",{d,fill:"none",stroke:css("--s-traffic"),"stroke-width":2,"stroke-linejoin":"round"},svg);
 const l=pts[pts.length-1];el("circle",{cx:X(t1),cy:Y(l.total),r:3.5,fill:css("--s-traffic"),stroke:css("--card"),"stroke-width":2},svg);
}

function drawSensors(){
 const box=$("sensors");const lat=(HIST&&HIST.sites_latest)||{};
 const vals=SITES.map(s=>(SITEDATA[s]&&SITEDATA[s].intensity_veh_per_hr)??(lat[s]&&lat[s].intensity_veh_per_hr)??null);
 const mx=Math.max(1,...vals.filter(v=>v!==null));
 box.innerHTML=SITES.map((s,i)=>{const v=vals[i],L=lat[s]||{};return `<button class="sensor" data-s="${s}" aria-pressed="${s===SEL}">
  <div class="sid">${s.toUpperCase()}<span style="font:500 11px Inter;color:var(--ink-3)">${L.avg_speed_kmh!=null?fmt(L.avg_speed_kmh)+" km/h":""}</span></div>
  <div class="role">${ROLE[s]}</div><div class="val">${fmt(v)}<small>veh/h</small></div>
  <div class="bar"><div style="width:${v===null?0:Math.round(v/mx*100)}%"></div></div>
  <div class="meta">${L.measurement_time?"measured "+tm(L.measurement_time):""}</div></button>`}).join("");
 box.querySelectorAll(".sensor").forEach(b=>b.onclick=()=>{SEL=b.dataset.s;drawSensors();showJSON()});
}
function hl(o){return JSON.stringify(o,null,2).replace(/("(?:\\.|[^"\\])*")(\s*:)?|\b(-?\d+\.?\d*(?:e[+-]?\d+)?)\b|\bnull\b/g,(m,str,colon,num)=>str?(colon?`<span class="k">${str}</span>${colon}`:`<span class="s">${str}</span>`):num!==undefined?`<span class="n">${m}</span>`:`<span class="nl">${m}</span>`)}
function showJSON(){$("apipath").textContent="GET /site/"+SEL;$("apilink").href="/site/"+SEL;const d=SITEDATA[SEL];$("json").innerHTML=d?hl(d):"Loading…"}

function drawHealth(h){
 const box=$("health");
 const rows=[["luchtmeetnet","Air collector ①","NO₂ → PostgreSQL"],["ndw","Traffic collector ②","NDW → S3 bucket"]];
 box.innerHTML=rows.map(([k,name,what])=>{const s=h&&h[k];const ok=s&&s.last_successful_fetch&&(Date.now()-Date.parse(s.last_successful_fetch))<2*3600e3;
  const icon=ok?`<svg width="22" height="22" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="var(--good)"/><path d="M7 12.5l3 3 7-7" stroke="#fff" stroke-width="2.4" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>`:`<svg width="22" height="22" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="var(--crit)"/><path d="M12 7v6M12 16.5v.5" stroke="#fff" stroke-width="2.4" stroke-linecap="round"/></svg>`;
  return `<div class="hrow">${icon}<div><div class="name">${name} · ${ok?"Healthy":"Needs attention"}</div><div class="what">${what}</div></div>
  <div class="right"><b>${s?ago(s.last_successful_fetch):"–"}</b>${s?fmt(s.bad_data_count)+(s.bad_data_count===1?" bad value":" bad values")+", 24 h":"unavailable"}</div></div>`}).join("");
 const allok=h&&h.status==="ok";$("livedot").className="dot"+(allok?"":" bad");
}

function drawModel(){
 const M=HIST&&HIST.model;if(!M){$("model").textContent="Model unavailable; the page still shows measured values.";return}
 const n=HIST.traffic.filter(p=>p.predicted!==null).length;
 $("model").innerHTML=`Predicted NO₂ = <b>${fmt(M.intercept,1)}</b> ${M.coef_traffic<0?"−":"+"} <b>${fmt(Math.abs(M.coef_traffic)*1000,2)}</b> × (thousand vehicles/hour) ${M.coef_hour<0?"−":"+"} <b>${fmt(Math.abs(M.coef_hour),2)}</b> × (hour of day, UTC).<br>
 In plain words: every extra 1,000 vehicles per hour ${M.coef_traffic<0?"<b>lowers</b>":"<b>raises</b>"} the prediction by ${fmt(Math.abs(M.coef_traffic)*1000,1)} µg/m³.${M.coef_traffic<0?" That sign is not believable yet; it comes from too few, mostly night-time hours.":""}`;
}

function drawHero(){
 const d=SITEDATA.hrl;if(!d)return;
 const thr=(HIST&&HIST.threshold_ug_m3)||40;
 ["actual","pred","total"].forEach(id=>$(id).classList.remove("sk"));
 $("actual").textContent=fmt(d.no2_ug_m3,1);$("pred").textContent=fmt(d.no2_ug_m3_predicted,1);
 const vals=SITES.map(s=>SITEDATA[s]&&SITEDATA[s].intensity_veh_per_hr);
 $("total").textContent=vals.some(v=>v==null)?"–":fmt(vals.reduce((a,b)=>a+b,0));
 const top=thr*1.5;$("marker").style.left=Math.min(100,Math.max(0,(d.no2_ug_m3||0)/top*100))+"%";$("limlbl").style.left=(thr/top*100)+"%";
 const above=d.no2_ug_m3!=null&&d.no2_ug_m3>=thr;
 $("status").innerHTML=above?`<svg width="14" height="14" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="var(--crit)"/><path d="M12 7v6M12 16.5v.5" stroke="#fff" stroke-width="2.6" stroke-linecap="round"/></svg>Above the EU limit of ${thr}`:`<svg width="14" height="14" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="var(--good)"/><path d="M7 12.5l3 3 7-7" stroke="#fff" stroke-width="2.6" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>Below the EU limit of ${thr}`;
 $("actualfoot").textContent=d.timestamp?`Hour ending ${dtm(d.timestamp)} · Luchtmeetnet (RIVM)`:"No reading yet";
 const r=d.no2_exceedance_risk;$("risk").textContent=r==null?"–":Math.round(r*100)+"%";$("garc").setAttribute("stroke-dasharray",`${r==null?0:Math.max(1,r*100)} 100`);
 $("predfoot").textContent=d.prediction_error?("Prediction unavailable: "+d.prediction_error):"Linear regression on total traffic + hour of day";
 $("trafficfoot").textContent=d.traffic_timestamp?`Snapshot ${tm(d.traffic_timestamp)} · last 24 h shown · NDW`:"From NDW";
}

function drawAll(){drawNO2();drawTraffic();drawSpark();drawSensors();drawModel();drawHero();showJSON()}

async function load(){
 try{
  const [sites,h]=await Promise.all([Promise.all(SITES.map(s=>j("/site/"+s).catch(()=>null))),j("/health").catch(()=>null)]);
  SITES.forEach((s,i)=>{if(sites[i])SITEDATA[s]=sites[i]});
  drawHealth(h);drawHero();drawSensors();showJSON();
  $("livetxt").textContent="Live · updated "+new Date().toLocaleTimeString("en-GB",{hour:"2-digit",minute:"2-digit",timeZone:TZ});
 }catch(e){$("livetxt").textContent="Offline · retrying";$("livedot").className="dot bad"}
 try{HIST=await j("/history?hours=48");$("gen").textContent="history built "+tm(HIST.generated_at);drawAll()}
 catch(e){$("c-no2").innerHTML='<div class="empty">History unavailable right now; live values above still work.</div>'}
}
let rt;addEventListener("resize",()=>{clearTimeout(rt);rt=setTimeout(drawAll,150)});
load();setInterval(load,60000);
})();
</script>
</body></html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
