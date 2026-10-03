"""train_model.py - Day 4 Lab 1: train the linear regression and report how good it is.

Input:  out/training_data.csv  (from build_training_data.py)
Output: out/model.pkl          (baked into the dashboard image afterwards)
        out/model_report.json  (rows, R2, MAE and coefficients, reported in ADR-006)

Why linear regression: we have at most a few dozen hourly rows. A more complex model
(random forest, neural network) would just memorise noise and be harder to debug. (Day 4.)
"""
import json
import os
import sys

import joblib
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from common import log_event

SERVICE = "train_model"
FEATURES = ["total_intensity_veh_per_hr", "hour_of_day"]
TARGET = "no2_ug_m3"
OUT_DIR = os.environ.get("OUT_DIR", "out")
MIN_ROWS_FOR_TEST_SPLIT = 20  # below this, a held-out test set is too small to mean anything


def train(df: pd.DataFrame) -> tuple[LinearRegression, dict]:
    X, y = df[FEATURES], df[TARGET]
    report = {"rows": int(len(df)), "features": FEATURES}

    if len(df) >= MIN_ROWS_FOR_TEST_SPLIT:
        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42)
        held_out = LinearRegression().fit(X_tr, y_tr)
        pred = held_out.predict(X_te)
        report["evaluation"] = "25% held-out test set"
        report["test_rows"] = int(len(X_te))
        report["r2_test"] = round(float(r2_score(y_te, pred)), 3)
        report["mae_test_ug_m3"] = round(float(mean_absolute_error(y_te, pred)), 2)
    else:
        report["evaluation"] = (f"no test split: only {len(df)} rows "
                                f"(need {MIN_ROWS_FOR_TEST_SPLIT}); in-sample numbers only")

    # The model we SHIP is trained on all rows: with this little data, every row counts.
    model = LinearRegression().fit(X, y)
    pred_all = model.predict(X)
    report["r2_in_sample"] = round(float(r2_score(y, pred_all)), 3) if len(df) > 1 else None
    report["mae_in_sample_ug_m3"] = round(float(mean_absolute_error(y, pred_all)), 2)
    report["coefficients"] = {f: round(float(c), 5) for f, c in zip(FEATURES, model.coef_)}
    report["intercept"] = round(float(model.intercept_), 3)
    report["traffic_coefficient_sign"] = (
        "positive (more traffic -> more NO2, as expected)" if model.coef_[0] > 0
        else "NOT positive: discuss this honestly in the reflection")
    return model, report


def main() -> int:
    df = pd.read_csv(os.path.join(OUT_DIR, "training_data.csv"))
    if len(df) < 3:
        log_event("error", SERVICE, "not_enough_data", rows=len(df))
        return 1
    model, report = train(df)
    joblib.dump(model, os.path.join(OUT_DIR, "model.pkl"))
    with open(os.path.join(OUT_DIR, "model_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    log_event("info", SERVICE, "model_trained", **report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
