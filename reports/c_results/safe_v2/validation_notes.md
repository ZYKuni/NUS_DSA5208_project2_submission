# v2 interface acceptance (2026-10-07)

- Targeted tests: 16 passed in 8.58s.
- Full A/B/C suite: 72 passed in 44.31s; one existing Pandas dependency deprecation warning.
- Actual CLI invocations for 2025-01-01, 12:01 non-grid time, and q=11 each exited 2 with a specific error and no probability. Unsupported inputs are rejected before Spark startup.
- Real-data check uses only S50 2023 validation rows; chosen by largest raw threshold increase, not labels. At 2023-02-20 15:45 SGT the original calibrated q=1/q=2 outputs were 0.4047619048 / 0.5000000000; v2 returns 0.4523809524 / 0.4523809524.
- Eight separately invoked thresholds [0,0.2,0.5,1,2,3,5,10] all use the same corrected curve and are non-increasing. Full records: acceptance.json.
- A's exact 0.22 S50 case was not supplied with a timestamp. We do not claim to have reproduced that particular observation. A synthetic 0.20→0.42 violation is separately covered, and a real S50 violation is verified as above.
- Frozen v1 hashes remain intact. No model weight/calibrator changes or 2024 metric re-evaluation. This establishes interface correctness, not v2 holdout accuracy.
