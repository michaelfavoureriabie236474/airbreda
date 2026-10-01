"""Day 1 Lab 1: first test (exactly the course example, adapted to our function name)."""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest_air import filter_no2_readings  # noqa: E402


def test_filter_no2_readings_handles_null_value():
    df = pd.DataFrame({
        "component": ["NO2", "NO2", "PM10"],
        "value": [18.4, None, 22.1],
        "timestamp": ["2024-01-15T08:00:00Z", "2024-01-15T09:00:00Z", "2024-01-15T08:00:00Z"],
    })
    result = filter_no2_readings(df)
    assert len(result) == 2
    assert result["value"].isnull().sum() == 1  # null NO2 rows are kept, not silently dropped
