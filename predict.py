"""predict.py - turn traffic into a predicted NO2 value and a 0-1 exceedance risk.

predict(total_intensity_veh_per_hr, hour_of_day) ->
    {"no2_ug_m3_predicted": float, "no2_exceedance_risk": float between 0 and 1}

IMPORTANT (training-serving skew): the model was trained on the TOTAL of all four sites and
on the UTC hour. The caller must pass exactly those, not one site's intensity or local time,
or the model receives numbers unlike anything it was trained on.
"""
import os

import numpy as np
import pandas as pd

MODEL_PATH = os.environ.get("MODEL_PATH", "model.pkl")

# Exceedance threshold in µg/m³ (see ADR-006).
# Default 40 = the EU annual limit value for NO2. It is an ANNUAL average applied here to
# HOURLY predictions, which must be stated as a limitation.
THRESHOLD_UG_M3 = float(os.environ.get("NO2_THRESHOLD", "40"))
STEEPNESS = 0.2  # from the Day 4 example: how fast risk rises around the threshold

_model = None


def _load():
    global _model
    if _model is None:
        import joblib
        _model = joblib.load(MODEL_PATH)
    return _model


def exceedance_risk(predicted_no2: float, threshold: float = THRESHOLD_UG_M3,
                    steepness: float = STEEPNESS) -> float:
    """Sigmoid centred on the threshold: 0.5 exactly at the threshold, near 0 well below,
    near 1 well above."""
    return float(1 / (1 + np.exp(-steepness * (predicted_no2 - threshold))))


def predict(total_intensity_veh_per_hr: float, hour_of_day: int) -> dict:
    model = _load()
    X = pd.DataFrame([[float(total_intensity_veh_per_hr), int(hour_of_day)]],
                     columns=["total_intensity_veh_per_hr", "hour_of_day"])
    predicted = float(model.predict(X)[0])
    # A straight line can predict below zero for unusual inputs; concentration can't be negative.
    predicted = max(predicted, 0.0)
    return {"no2_ug_m3_predicted": round(predicted, 2),
            "no2_exceedance_risk": round(exceedance_risk(predicted), 4)}
