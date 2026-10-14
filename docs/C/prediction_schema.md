# Prediction interchange v0.2

Owner C, consumer B; proposed H2 schema awaiting team acceptance.
Primary key: `(model_id, split, station_id, prediction_time, threshold_mm)`.
CSV timestamps are ISO 8601 with explicit +08:00; blank cells mean null.

| Field | Type | Rule |
|---|---|---|
| station_id | string | Non-empty |
| prediction_time | timestamp | SGT prediction anchor |
| threshold_mm | double | Finite, non-negative rainfall threshold |
| future_30m_mm | nullable decimal(30,3) in Parquet | Exact outcome truth; never a feature |
| label | nullable double | 0/1 using strict > |
| label_available | boolean | Complete valid outcome |
| raw_probability | nullable double | Model probability of label 1 |
| calibrated_probability | nullable double | Blank if no calibration |
| prediction | nullable integer | Probability >= decision_threshold |
| decision_threshold | double | Probability cutoff, not rainfall threshold |
| probability_source | string | raw or calibrated; determines prediction |
| split | string | train/validation/calibration/test |
| model_id | string | Immutable model version |
| feature_complete | boolean | Historical feature completeness |
| is_fixture | boolean | Must be false for reported results |
| inference_mode | string | available_at_proxy for actual predictions |

Evaluate only rows with available label and a finite selected probability.
Do **not** require feature_complete=true: in the actual as-of experiment this
strict all-12-slots-plus-lag60 flag is false for every sampled origin, because
the current slot is not yet published. Missingness-aware features remain valid;
filtering on feature_complete would erase the evaluation population.
Report retained/excluded counts. Keep raw and calibrated metrics separate;
never silently substitute one for the other. Group by model/split/threshold.
Specify probability cutoff before test evaluation. PR/ROC are undefined for
single-class groups: emit null and explain, not a fabricated zero.

## Fixture for B

`data/samples/predictions_fixture.csv` is **SYNTHETIC TEST FIXTURE — NOT FOR
REPORTING**. It is hand-authored, not model output or a performance estimate.
All four observations have q=1 mm, raw probabilities [0.1,0.8,0.4,0.7], labels
[0,1,0,1], no calibrator, and cutoff 0.5. Expected raw Brier = 0.075;
TN=2, TP=2, FP=0, FN=0. Includes a target exactly equal to q (negative label).
The automatic test independently recomputes these expectations with stdlib CSV.

H2 is accepted when B independently recomputes Brier/confusion counts and A can
load the actual model. This fixture only enables interface/plot development;
the fixture alone is not H2 acceptance. Actual predictions are written as Spark
Parquet directories under `data/predictions/c_available_v1/{validation,calibration,test}`.
Each candidate has a separate validation directory; final family winners and
the serving fallback rule have test directories. Parquet timestamp display must
use an Asia/Singapore Spark session. Prediction schema v0.2 changes target truth
from double to decimal and adds inference_mode; B should preserve this precision.

`test/serving` keeps the actual model_id on each row (calendar fallback versus
primary model). Probability_source=calibrated, cutoff=0.5, is_fixture=false.
Raw probabilities remain available. Calibration predictions are calibration-fit
diagnostics, not held-out evidence. B's independent real-prediction recomputation
and A's independent model reload remain team review steps after C's handoff.
