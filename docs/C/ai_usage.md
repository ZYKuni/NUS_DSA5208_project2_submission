# AI usage log

- 2026-10-07: Codex assisted implementation of A-review v2 interface guards,
  timezone/grid documentation, fixed-knot monotonic postprocessing, tests and
  validation-only acceptance checks. This is a post-hoc interface revision;
  original v1 holdout results are not represented as v2 performance.

- 2026-10-06: OpenAI Codex assisted C in defining Task 2 contracts, implementing
  Spark target construction and split/threshold helpers, writing artificial
  boundary tests, preparing a synthetic prediction fixture, and executing local
  environment and persistence checks. No real rainfall result is claimed.
- Subsequent assistance on 2026-10-06: OpenAI Codex inspected the merged A pipeline
  and B's independent precision review, implemented the decimal label repair,
  availability-aware feature construction, Spark LR/GBT and isotonic calibration,
  chronological sampling/selection/freeze/evaluation, saved-model inference,
  regression tests, reproducibility documentation, and result-rendering code.
  It also operated local tools to download and process the real data and execute
  experiments. Actual outcomes are evidenced by `reports/c_results`, not by this log.
- Human review remains required: A should independently load the delivered model;
  B should independently recompute real prediction metrics and review the report.
  Neither check is claimed complete merely because C's automated checks pass.
  Team members must verify and take responsibility for submitted code/results,
  and incorporate this disclosure in the final PDF report.
