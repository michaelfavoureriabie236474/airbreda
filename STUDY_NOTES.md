# Study notes: what was built on Wednesday afternoon, and why

Read this before Thursday. Every decision below is something the evaluator can ask about.
(Delete this file before you push to GitHub, or keep it out of `docs/`.)

---

## 1. The two scripts in one sentence each

- **ingest_air.py** asks Luchtmeetnet for the latest NO₂ (nitrogen dioxide) readings at station
  NL10240, marks suspicious ones, and saves them in the `sensor_readings` table.
- **ingest_traffic.py** downloads NDW's (Nationaal Dataportaal Wegverkeer) file of all Dutch road
  sensors, picks out our 4 A27 sites, throws away impossible values like `speed = -1`, and saves
  one small CSV (Comma-Separated Values) file per site per hour to object storage.

Both run once and stop. Cron (the VM's built-in alarm clock) starts them every hour.

---

## 2. Decisions in the code you must be able to defend

### D1. Duplicates can't happen (idempotent writes)
**Simple:** Luchtmeetnet sends the last 50 hours every time we ask. Without protection, the same
reading would be saved 50 times.
**How:** `(station_id, timestamp)` is the table's PRIMARY KEY, and inserts use
`ON CONFLICT DO NOTHING`.
**Proof:** run 1 stored 50 rows; run 2 received the same 50 and stored 0.
**Evaluator phrase:** "Our fetch is at-least-once, our write is idempotent, so the result behaves
like exactly-once."

### D2. A table that remembers each run: `ingestion_runs`
**Problem:** Day 2 says keep `bad_data_count` "in memory" and give each container its own `/health`.
But Day 3 makes the containers run-once-and-exit via cron. When a container exits, its memory is
gone, and there's no running server to answer `/health`.
**Decision:** each run writes one row to `ingestion_runs` (source, time, success, bad count). The
dashboard's `/health` (built Thursday) reads it back for BOTH sources, which is exactly Day 5's format.
**This is a genuine tension in the course material. Mention it in ADR-003 or ADR-004: evaluators
like students who notice it.**

### D3. Bad Luchtmeetnet data is counted only once
Every run re-sends 50 hours. If we counted every flagged row in every response, one stale reading
would be counted again every hour and falsely trigger `BAD_DATA_THRESHOLD_EXCEEDED`.
So only NEW rows are counted.

### D4. NDW: drop the bad VALUE, not the whole site-hour
`speed = -1` means "no speed could be measured", typically because **no cars passed that lane
that minute** (the live file shows `numberOfInputValuesUsed="0"` next to it). This happens a lot at
night on quiet lanes. If we dropped the whole site-hour, we'd lose the valid flow and the other
lane's valid speed. So we drop just the `-1` value, log a WARNING, and count it.
**Alternative:** drop the whole row (simpler, but loses good data). This fits ADR-002's question:
"would you apply the same rule to both sources if you started over?"

### D5. Streaming the NDW file (memory)
The file is ~50 MB unzipped with ~20,000 sites. The VM has 1 GB of RAM. `iterparse` reads one site,
checks it, throws it away, and stops once all 4 are found. **Measured: 38 MB peak, 3.1 seconds.**

### D6. No double-counting of traffic (uses the course's build_index_map)
Some NDW sites report per-vehicle-type values AND a total (seen live: 0+900+300+0+120 = 1320).
Summing everything would double-count. NDW's site table shows OUR 4 sites only report
`anyVehicle` per lane, so `intensity = sum of lanes` is correct for these sites.

### D7. Traffic and NO₂ are matched to the same hour
Luchtmeetnet's `14:00` means the hour 13:00 to 14:00. A traffic snapshot taken by the 14:00 cron run
is from about 13:59. Rounding to the nearest hour puts both on 14:00, ready for Day 4's join.

### D8. Region: eu-north-1 (Stockholm)
The course says eu-west-1 (Ireland). Your Free plan blocked Ireland unless "advanced features"
were activated. Stockholm is also in the EU (European Union), so GDPR (General Data Protection
Regulation) protection is the same.

---

## 3. Raw files AND parsed files in the bucket (updated Thursday, after reading the Day 1 lab)

The Day 1 lab says raw NDW XML belongs in object storage as the "audit trail and future ML
retraining dataset". Day 3/4 want one parsed CSV per site per hour. We now do BOTH:
- `raw/ndw/YYYY-MM-DD/HH.xml.gz`  the untouched NDW file (~1.2 MB)
- `ndw/YYYY-MM-DD/HH-{site}.csv`   the 4 small parsed rows the model and dashboard use
Why it matters: if a parsing bug is found (e.g. in the speed=-1 rule), we can re-parse the
originals with fixed code. That's the Kappa idea from Day 1: one code path, reprocess by replay.
Cost: about 1.2 MB x 24 x 30 ≈ 0.8 GB per month (my arithmetic; check the S3 price for ADR-001).

## 3b. Table design follows the Day 1 lab exactly

`sensor_readings(station_id, timestamp, component, value)` with PRIMARY KEY
(station_id, timestamp, component), plus Day 2's `is_flagged`. We only store component = 'NO2',
but the design allows PM10 or O3 later without changing the table.

---

## 3a. Traffic script follows the course's reference structure

download_and_decompress() -> build_index_map() (from the configuration file: which index is a
flow, which is a speed, for which vehicle type) -> extract_measurements() (only 'anyVehicle'
values, so per-vehicle-class values can't be double-counted). Same function names as the
course's reference extraction script / getTrafficReading.py. Required Day 1 comment ("why both
database and bucket? retraining in six months?") is at the top of ingest_traffic.py.

## 3c. Course alignment audit (Thursday): what each Day asked for, and where it is

| Day | Requirement | Where |
|---|---|---|
| 1 | ingest_air.py: live API, NO2 filter, latest reading, error handling | ingest_air.py |
| 1 | Required comment: CAP trade-off + null handling | top of ingest_air.py |
| 1 | tests/test_ingest_air.py (course example) | tests/test_ingest_air.py |
| 1 | sensor_readings table + ON CONFLICT DO NOTHING | schema.sql, write_readings() |
| 1 | Raw NDW XML in object storage | raw/ndw/... in the bucket |
| 1 | ingest_traffic: course's 2 NDW feeds + function names, measurement-time file names | ingest_traffic.py |
| 1 | Required comment: why database AND bucket, retraining in 6 months | top of ingest_traffic.py |
| 2 | Redis queue + compose file, message format | docker-compose.day2.yml, publish_reading() |
| 2 | Polling interval comment | top of ingest_air.py |
| 2 | Structured JSON logs, DATA_QUALITY_ERROR, threshold ERROR | both ingest scripts |
| 2 | is_flagged (keep) vs speed=-1 (drop) | both ingest scripts |
| 2 | luchtmeetnet_bad_data_count / ndw_bad_data_count | both ingest scripts |
| 2 | Per-container /health | `python ingest_air.py health` (see 3d) |
| 2 | tests/test_data_quality.py | tests/test_data_quality.py |
| 3 | VM, bucket, IAM role, .env, cron, no compose/Redis on VM | DEPLOY.md |
| 4 | build_training_data, LinearRegression, R2/MAE, sigmoid risk, predict.py | Day 4 files |
| 4 | dashboard /site/{id}, /health, /; required comment; refresh; last updated | dashboard.py |
| 4 | tests/test_model.py, Dockerfile.dashboard, compose, tighten IAM | as named, DEPLOY Part 7 |
| 5 | exact JSON shapes, real data, both bad-data handlers, no passwords in git | dashboard.py, .gitignore |

Not done (optional in the course): Day 4 stretch goal (LogisticRegression comparison), CI/CD, IaC.

## 3d. A contradiction in the course you should mention (good evaluator point)

Day 2 asks for a /health endpoint inside each ingestion container and an in-memory bad_data_count.
Day 3 makes those containers run once per hour via cron and exit. A process that has exited can't
answer HTTP or keep a counter in memory. Our solution: each run writes its result to
ingestion_runs; `python ingest_air.py health` prints the per-container health from there, and the
dashboard's /health (Day 5) aggregates both. Also: when Redis is down, the scripts log
queue_publish_failed and still write to the database (tested), so no reading is lost.

## 4. Known limitations (honest, and useful for the reflection)

1. **One minute per hour.** NDW's live file only holds the current minute. Our "hourly" traffic
   value is a one-minute snapshot, while NO₂ is a full-hour average. The model will be noisier.
2. **The site mapping is inferred, not confirmed.** Check the course README.
3. **Format:** we use the two NDW feeds the Day 1 lab names (site configuration + measured
   values), which are DATEX II **v3**. NDW announced it stops the older v2.3 files on 9 Feb 2027,
   so our choice is also future-proof (good reflection point).

---

## 5. Quick self-test (answer out loud, no looking)

1. Why doesn't running `ingest_air.py` twice create duplicate rows?
2. Why can't each ingestion container have its own `/health` endpoint in our deployment?
3. It's 03:00 and hrr lane 2 reports `speed = -1`. What happens to that value, to the flow value,
   and to `bad_data_count`?
