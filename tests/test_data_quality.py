"""Tests for both bad-data handlers (Day 2 requirement: tests/test_data_quality.py).

Run from the project folder:
    pytest -v

The database tests need a PostgreSQL to write into. They use these environment variables
and are SKIPPED (not failed) if TEST_DB_HOST is not set:
    TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USER, TEST_DB_PASSWORD, TEST_DB_NAME
Never point these at your real cloud database: the tests delete their own test rows.
"""
import gzip
import io
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ingest_air  # noqa: E402
import ingest_traffic  # noqa: E402

TEST_STATION = "TEST_STATION"  # never a real station, so cleanup can't touch real data


# ---------------------------------------------------------------- helpers
def lmn_item(ts: str, value):
    """One item in the same shape the real Luchtmeetnet API returns."""
    return {"value": value, "timestamp_measured": ts, "formula": "NO2"}


def hours(values):
    """Same shape ingest_air.fetch_measurements() returns: component, value, timestamp."""
    import pandas as pd
    return pd.DataFrame({"component": ["NO2"] * len(values), "value": values,
                         "timestamp": [f"2026-09-30T{h:02d}:00:00+00:00" for h in range(len(values))]})


# Same structure as the real NDW DATEX II v3 feeds named in the Day 1 lab, copied from the live
# files on 1 Oct 2026, with one lane's speed set to -1 on purpose.
NS = ('xmlns:mc="http://datex2.eu/schema/3/messageContainer" '
      'xmlns:roa="http://datex2.eu/schema/3/roadTrafficData" '
      'xmlns:com="http://datex2.eu/schema/3/common" '
      'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')

CONFIG_XML = f"""<?xml version="1.0" encoding="UTF-8"?><mc:messageContainer {NS}><mc:payload>
<roa:measurementSite id="RWS01_MONIBAS_0271hrl0063ra" version="1">
 <roa:measurementSpecificCharacteristics index="1"><roa:measurementSpecificCharacteristics><roa:specificMeasurementValueType>trafficFlow</roa:specificMeasurementValueType><roa:specificVehicleCharacteristics><com:vehicleType>anyVehicle</com:vehicleType></roa:specificVehicleCharacteristics></roa:measurementSpecificCharacteristics></roa:measurementSpecificCharacteristics>
 <roa:measurementSpecificCharacteristics index="2"><roa:measurementSpecificCharacteristics><roa:specificMeasurementValueType>trafficSpeed</roa:specificMeasurementValueType><roa:specificVehicleCharacteristics><com:vehicleType>anyVehicle</com:vehicleType></roa:specificVehicleCharacteristics></roa:measurementSpecificCharacteristics></roa:measurementSpecificCharacteristics>
 <roa:measurementSpecificCharacteristics index="3"><roa:measurementSpecificCharacteristics><roa:specificMeasurementValueType>trafficFlow</roa:specificMeasurementValueType><roa:specificVehicleCharacteristics><com:vehicleType>anyVehicle</com:vehicleType></roa:specificVehicleCharacteristics></roa:measurementSpecificCharacteristics></roa:measurementSpecificCharacteristics>
 <roa:measurementSpecificCharacteristics index="4"><roa:measurementSpecificCharacteristics><roa:specificMeasurementValueType>trafficSpeed</roa:specificMeasurementValueType><roa:specificVehicleCharacteristics><com:vehicleType>anyVehicle</com:vehicleType></roa:specificVehicleCharacteristics></roa:measurementSpecificCharacteristics></roa:measurementSpecificCharacteristics>
 <roa:measurementSpecificCharacteristics index="5"><roa:measurementSpecificCharacteristics><roa:specificMeasurementValueType>trafficFlow</roa:specificMeasurementValueType><roa:specificVehicleCharacteristics><com:vehicleType>lorry</com:vehicleType></roa:specificVehicleCharacteristics></roa:measurementSpecificCharacteristics></roa:measurementSpecificCharacteristics>
</roa:measurementSite></mc:payload></mc:messageContainer>"""

def _pq(index, kind, value):
    if kind == "flow":
        body = f'<roa:basicData xsi:type="roa:TrafficFlow"><roa:vehicleFlow><com:vehicleFlowRate>{value}</com:vehicleFlowRate></roa:vehicleFlow></roa:basicData>'
    else:
        body = f'<roa:basicData xsi:type="roa:TrafficSpeed"><roa:averageVehicleSpeed><com:speed>{value}</com:speed></roa:averageVehicleSpeed></roa:basicData>'
    return f'<roa:physicalQuantity index="{index}"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity">{body}</roa:physicalQuantity></roa:physicalQuantity>'

NDW_XML = f"""<?xml version="1.0" encoding="UTF-8"?><mc:messageContainer {NS}><mc:payload>
<roa:siteMeasurements><roa:measurementSiteReference id="OTHER_SITE"/>{_pq(1, "flow", 999)}
 <roa:measurementTimeDefault><roa:timeValue>2026-09-30T13:59:00Z</roa:timeValue></roa:measurementTimeDefault></roa:siteMeasurements>
<roa:siteMeasurements><roa:measurementSiteReference id="RWS01_MONIBAS_0271hrl0063ra"/>
 {_pq(1, "flow", 840)}{_pq(2, "speed", 108.0)}{_pq(3, "flow", 0)}{_pq(4, "speed", -1.0)}{_pq(5, "flow", 120)}
 <roa:measurementTimeDefault><roa:timeValue>2026-09-30T13:59:00Z</roa:timeValue></roa:measurementTimeDefault></roa:siteMeasurements>
</mc:payload></mc:messageContainer>"""

HRL = "RWS01_MONIBAS_0271hrl0063ra"


def _parse():
    index_map = ingest_traffic.build_index_map(io.BytesIO(CONFIG_XML.encode()), {HRL})
    return ingest_traffic.extract_measurements(io.BytesIO(NDW_XML.encode()), {HRL}, index_map)


# ------------------------------------------------ Luchtmeetnet: flag-and-keep rule
def test_null_reading_is_flagged():
    rows = ingest_air.detect_quality(hours([20.0, None, 22.0]))
    assert [r["is_flagged"] for r in rows] == [False, True, False]
    assert rows[1]["reason"] == "null"


def test_three_identical_hours_flags_the_third():
    rows = ingest_air.detect_quality(hours([20.0, 25.0, 25.0, 25.0, 25.0, 30.0]))
    assert [r["is_flagged"] for r in rows] == [False, False, False, True, True, False]
    assert rows[3]["reason"] == "stale"


def test_normal_readings_are_not_flagged():
    rows = ingest_air.detect_quality(hours([20.0, 21.0, 20.0, 21.0]))
    assert not any(r["is_flagged"] for r in rows)


# ------------------------------------------------ NDW: drop-and-log rule
def test_ndw_speed_minus_one_is_excluded_and_counted():
    parsed = _parse()
    row, bad = ingest_traffic.clean_site("hrl", HRL, parsed[HRL])
    assert len(bad) == 1 and bad[0]["field"] == "speed" and bad[0]["value"] == -1
    assert row["avg_speed_kmh"] == 108.0          # -1 did NOT drag the average down
    assert row["excluded_speed_values"] == 1
    assert row["intensity_veh_per_hr"] == 840      # 840 + 0; the lorry-only value (120) is not double-counted
    assert "-1" not in ingest_traffic.to_csv(row)  # the sentinel never reaches storage


def test_ndw_parser_ignores_other_sites():
    assert list(_parse()) == [HRL]


def test_measurement_just_before_the_hour_joins_that_hour():
    assert ingest_traffic.nearest_hour("2026-09-30T13:59:00Z").hour == 14


# ------------------------------------------------ database tests (need TEST_DB_HOST)
needs_db = pytest.mark.skipif(not os.environ.get("TEST_DB_HOST"),
                              reason="set TEST_DB_HOST to run database tests")


@pytest.fixture
def db(monkeypatch):
    for key in ("HOST", "PORT", "USER", "PASSWORD", "NAME"):
        if os.environ.get(f"TEST_DB_{key}"):
            monkeypatch.setenv(f"DB_{key}", os.environ[f"TEST_DB_{key}"])
    monkeypatch.setenv("DB_SSLMODE", "disable")
    import common
    conn = common.get_db_conn()
    yield conn
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sensor_readings WHERE station_id = %s", (TEST_STATION,))
        cur.execute("DELETE FROM ingestion_runs WHERE error = 'pytest'")
    conn.commit()
    conn.close()


@needs_db
def test_flagged_luchtmeetnet_row_is_written_not_dropped(db):
    rows = ingest_air.detect_quality(hours([20.0, None]), station_id=TEST_STATION)
    ingest_air.write_readings(db, rows)
    with db.cursor() as cur:
        cur.execute("""SELECT value, is_flagged FROM sensor_readings
                       WHERE station_id = %s ORDER BY timestamp""", (TEST_STATION,))
        assert cur.fetchall() == [(20.0, False), (None, True)]


@needs_db
def test_same_reading_twice_is_stored_once(db):
    rows = ingest_air.detect_quality(hours([20.0, 21.0]), station_id=TEST_STATION)
    assert len(ingest_air.write_readings(db, rows)) == 2
    assert len(ingest_air.write_readings(db, rows)) == 0   # second time: nothing new


@needs_db
def test_ndw_bad_value_increments_ndw_bad_data_count(db, monkeypatch, tmp_path):
    """Full run of ingest_traffic with a fake download containing speed=-1."""
    files = {}
    for name, xml in (("config", CONFIG_XML), ("meas", NDW_XML)):
        files[name] = tmp_path / f"{name}.xml.gz"
        files[name].write_bytes(gzip.compress(xml.encode()))

    def fake_download(url):
        path = str(files["config" if "configuratie" in url else "meas"])
        return path, gzip.open(path, "rb")

    monkeypatch.setattr(ingest_traffic, "download_and_decompress", fake_download)
    monkeypatch.setattr(ingest_traffic.os, "remove", lambda p: None)  # keep our fixture files
    monkeypatch.setenv("NDW_SITES", "hrl=RWS01_MONIBAS_0271hrl0063ra")
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setenv("LOCAL_OUTPUT_DIR", str(tmp_path / "out"))

    assert ingest_traffic.run() == 0
    with db.cursor() as cur:
        cur.execute("""SELECT bad_data_count FROM ingestion_runs
                       WHERE source = 'ndw' ORDER BY id DESC LIMIT 1""")
        assert cur.fetchone()[0] == 1
        cur.execute("UPDATE ingestion_runs SET error = 'pytest' WHERE id = "
                    "(SELECT max(id) FROM ingestion_runs WHERE source = 'ndw')")
    db.commit()
    saved = (tmp_path / "out" / "ndw" / "2026-09-30" / "14-hrl.csv").read_text()
    assert (tmp_path / "out" / "raw" / "ndw" / "2026-09-30" / "14.xml.gz").exists()  # raw kept
    assert ",-1" not in saved
