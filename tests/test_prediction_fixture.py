import csv
from pathlib import Path
import pytest


def test_fixture_contract_and_hand_calculated_metrics():
    path = Path(__file__).resolve().parents[1] / "data/samples/predictions_fixture.csv"
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 4
    assert all(r["is_fixture"] == "true" and r["model_id"] == "SYNTHETIC_NOT_FOR_REPORTING" for r in rows)
    assert all(int(r["label"]) == int(float(r["future_30m_mm"]) > float(r["threshold_mm"])) for r in rows)
    assert all(int(r["prediction"]) == int(float(r["raw_probability"]) >= float(r["decision_threshold"])) for r in rows)
    assert all(r["calibrated_probability"] == "" for r in rows)
    assert sum((float(r["raw_probability"]) - int(r["label"]))**2 for r in rows)/4 == pytest.approx(.075)
    pairs = [(int(r["label"]), int(r["prediction"])) for r in rows]
    assert [pairs.count(p) for p in [(0,0), (1,1), (0,1), (1,0)]] == [2,2,0,0]
