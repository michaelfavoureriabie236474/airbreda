"""Day 4 model tests. Run: pytest -v

The first two tests need the real model.pkl in the project folder (copied from out/ after
training). Until you've trained, they are skipped, not failed.
"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
MODEL = os.path.join(ROOT, "model.pkl")

import predict  # noqa: E402

needs_model = pytest.mark.skipif(not os.path.exists(MODEL), reason="model.pkl not trained yet")


@needs_model
def test_model_pkl_loads():
    import joblib
    model = joblib.load(MODEL)
    assert hasattr(model, "predict")


@needs_model
def test_predict_returns_plausible_values(monkeypatch):
    monkeypatch.setattr(predict, "MODEL_PATH", MODEL)
    monkeypatch.setattr(predict, "_model", None)
    out = predict.predict(total_intensity_veh_per_hr=5000, hour_of_day=8)
    assert isinstance(out["no2_ug_m3_predicted"], float)
    assert 0 <= out["no2_ug_m3_predicted"] <= 200
    assert 0 <= out["no2_exceedance_risk"] <= 1


def test_risk_is_one_half_at_the_threshold():
    assert predict.exceedance_risk(40, threshold=40) == pytest.approx(0.5)


def test_risk_rises_with_predicted_no2():
    assert predict.exceedance_risk(10, 40) < predict.exceedance_risk(40, 40) < predict.exceedance_risk(70, 40)
