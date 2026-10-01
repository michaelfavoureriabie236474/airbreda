# AirBreda: ingestion (Day 1 to 3 code)

Collects real NO₂ (nitrogen dioxide) and real traffic data for the A27 near Breda, every hour.

```
airbreda/
├── common.py               shared: JSON logging, DB connection, run recording
├── ingest_air.py           container 1: Luchtmeetnet NO₂ -> sensor_readings table
├── ingest_traffic.py       container 2: NDW traffic -> ndw/YYYY-MM-DD/HH-{site}.csv in S3
├── schema.sql              creates sensor_readings + ingestion_runs
├── requirements.txt
├── Dockerfile              builds airbreda-air
├── Dockerfile.traffic      builds airbreda-traffic
├── .env.example            template for your secrets file (.env is never committed)
├── .gitignore
├── docker-compose.day2.yml Day 2 lab only: both ingesters + Redis queue (local)
├── tests/
│   ├── test_ingest_air.py      Day 1 lab test (filter_no2_readings)
│   └── test_data_quality.py    Day 2 bad-data handler tests
└── docs/                   GitHub Pages: architecture document goes here (Thursday/Friday)
```

Day 4 files (added Thursday):

```
├── storage.py              reads NDW CSVs from S3 (or a local folder when testing)
├── build_training_data.py  joins sensor_readings + bucket CSVs -> out/training_data.csv
├── train_model.py          linear regression -> out/model.pkl + out/model_report.json
├── predict.py              predict(total_intensity, hour) -> predicted NO2 + 0-1 risk
├── dashboard.py            container 3: FastAPI with /site/{id}, /health, /
├── requirements-ml.txt     pinned ML/web libraries (same versions for training and serving)
├── Dockerfile.train        trainer image (runs once, writes the model, exits)
├── Dockerfile.dashboard    dashboard image (model.pkl baked in)
├── docker-compose.yml      local testing only
├── DEPLOY.md               step-by-step VM deployment
└── tests/test_model.py
```

## How data flows

```
Luchtmeetnet API ──(hourly, cron)──> ingest_air.py ──> PostgreSQL: sensor_readings
  (NO₂, station NL10240)                    │                       + ingestion_runs
                                            └─ stale/null -> kept, is_flagged = TRUE

NDW snelheden_en_intensiteiten (config + meetgegevens) ──(hourly, cron)──> ingest_traffic.py ──> S3: ndw/YYYY-MM-DD/HH-{site}.csv
  (4 A27 sites)                                     │            PostgreSQL: ingestion_runs
                                                    └─ speed = -1 -> logged, excluded
```

## NDW site mapping (CONFIRM against the course README)

| Site | NDW ID | NDW's own label | Lanes |
|---|---|---|---|
| hrl | RWS01_MONIBAS_0271hrl0063ra | mainCarriageway | 2 |
| hrr | RWS01_MONIBAS_0271hrr0063ra | mainCarriageway | 2 |
| vwd | RWS01_MONIBAS_0270vwd0063ra | entrySlipRoad | 1 |
| vwa | RWS01_MONIBAS_0270vwa0063ra | exitSlipRoad | 2 |

All four are at about 51.592 N, 4.829 E (A27, just north of Breda). Found by searching NDW's
live files on 30 Sep 2026: `hrl0063ra` is the example ID used in the course's Day 2 text, and
the other three are the only matching sites at the same road position (`0063`).

## Run locally

```bash
pip install -r requirements.txt pytest
cp .env.example .env            # then fill in real values
python ingest_air.py            # needs DB_* in the environment
python ingest_traffic.py        # without S3_BUCKET it saves CSVs to ./data
pytest -v                       # database tests are skipped unless TEST_DB_HOST is set
```

## What has been verified (30 Sep 2026)

- `ingest_air.py` against the live Luchtmeetnet API and a real PostgreSQL 16: 50 readings stored;
  a second run stored 0 new rows (duplicates correctly skipped)
- `ingest_traffic.py` against the live NDW file: 4 sites saved, 3.1 s, 38 MB peak memory
- 13/13 tests pass (with a test model), including 3 against a real PostgreSQL
- Day 4 pipeline (build -> train -> dashboard) tested end to end on SYNTHETIC test data only;
  the real model will be trained on the VM from your own collected data
- A clean Python 3.11 install from `requirements.txt` runs the scripts

**Not yet verified:** `docker build` (blocked in the build environment; test it on your laptop),
the S3 upload path (needs your bucket), and the connection to your cloud database.
