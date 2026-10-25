# Checkpoint traffic analytics (A1–A3)

## 1. Why this analytics step is needed

Spark produces a window for each station with average speed, total flow, average occupancy, and the number of valid observations. The checkpoint also needs a short traffic label that a reviewer can reproduce from those metrics. A1 defines that rule; A2 checks it against known examples.

## 2. What the analytics output adds

For each Spark metrics row, keep the seven S3 fields and add `traffic_state` and `state_explanation`. The label uses `avg_speed` and `valid_count`; flow and occupancy stay visible as supporting context. The raw Kafka contract uses `speed`, `flow`, and `occupancy`, mapped from the PeMS source columns by the producer.

## 3. A1: deterministic, configurable rule

Let `v` be `avg_speed` in miles per hour and `n` be `valid_count` in the station/window group. Let `s`, `c`, and `m` be the severe, congested, and moderate speed thresholds. The current configurable defaults are `s = 20`, `c = 40`, and `m = 55` mph. The repository marks these defaults provisional; the analytics owner should confirm them before presenting them as domain-validated thresholds.

For a nonempty window with a finite, nonnegative average speed:

\[
\operatorname{state}(v)=
\begin{cases}
\text{SEVERE}, & 0 \le v < s,\\
\text{CONGESTED}, & s \le v < c,\\
\text{MODERATE}, & c \le v < m,\\
\text{FREE}, & v \ge m.
\end{cases}
\]

Thus 20 mph is CONGESTED, 40 mph is MODERATE, and 55 mph is FREE. A zero or invalid `n`, or a missing, non-finite, or negative `v`, produces no label and an explanation code. Missing observations are never labeled FREE.

`TrafficThresholds.from_env()` reads `SEVERE_SPEED_MPH`, `CONGESTED_SPEED_MPH`, and `MODERATE_SPEED_MPH` once. Pass the same threshold object to the classifier for every row in one run. The pure Python rule is in `src/freewaypulse/analytics/traffic_state.py`; `add_traffic_state()` applies the same boundaries to a Spark DataFrame.

## 4. A2: representative calculation and result

The Spark test builds two station groups in one 5-minute window. Each row below is the expected result after S3 aggregation and A1 classification.

| Example | Speeds (mph) | Flow values (vehicles/5 min) | Mean occupancy | `avg_speed` | `total_flow` | `valid_count` | State |
|---|---:|---:|---:|---:|---:|---:|---|
| Free-flow | 65, 70 | 100, 120 | 0.03 | 67.5 mph | 220 | 2 | FREE |
| Congested | 25, 30 | 60, 80 | 0.11 | 27.5 mph | 140 | 2 | CONGESTED |

These are synthetic golden cases, not observations measured from PeMS. The pure unit tests also check every boundary, invalid windows, configuration overrides, and threshold ordering. The Spark test verifies that the S3 aggregation output receives the expected state and explanation.

Run the Spark checks in the project’s Java 17 container:

```powershell
docker compose run --build --rm --no-deps worker sh -lc "pip install --quiet pytest && python -m pytest -q tests/test_streaming_transforms.py tests/test_streaming_s4_parquet.py tests/test_traffic_state_spark.py"
```

## 5. A3: checkpoint acceptance still needs integration evidence

- [ ] Run the producer against the agreed Kafka contract and record sent/failed counts.
- [ ] Show typed Spark input, validation, deduplication, and watermark behavior.
- [ ] Produce 5-minute and 15-minute metrics and show the manually checked example.
- [ ] Read the persisted Parquet metrics back after the run.
- [ ] Apply the same classification to the run output and show its explanation.
- [ ] Rehearse one clean-checkout run and distinguish verified results from synthetic examples.

S3/S4 are marked Done in the tracker and have code plus offline tests on `minh_spark`. A3 still depends on running the integrated path and the P2 handoff; this document is the acceptance checklist, not sign-off.
