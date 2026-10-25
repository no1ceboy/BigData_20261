"""Independent, hand-calculated output checks; no broker or Spark is needed."""

import csv
from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from freewaypulse.verification import VerificationError, main, verify


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def write_rows(path, rows):
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path / "part-00000.parquet")


@pytest.fixture
def replay_artifacts(tmp_path):
    source = tmp_path / "observations.csv.gz"
    # Deliberately unsorted; station 999999 is outside the replay selection.
    raw = [
        [715898, "2026-10-14T00:15:00-07:00", 60, 30, 0.4],
        [999999, "2026-10-14T00:00:00-07:00", 80, 100, 0.8],
        [715898, "2026-10-14T00:00:00-07:00", 10.005, 10, 0.12345],
        [715898, "2026-10-14T00:05:00-07:00", 40, 20, 0.3],
    ]
    with gzip.open(source, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["station_id", "timestamp", "speed_mph", "flow_veh_5min", "occupancy_fraction"])
        writer.writerows(raw)
    run = tmp_path / "runs" / "verified"
    reports = run / "reports"
    write_json(reports / "producer.json", {
        "status": "passed", "station_ids": [715898], "event_limit": 0,
        "selected_events": 3, "sent": 3, "failed": 0, "topic": "traffic-test",
    })
    write_json(reports / "runtime.json", {
        "topic": "traffic-test", "event_limit": 0,
        "speed_thresholds_mph": {
            "severe_speed_mph": 20, "congested_speed_mph": 40, "moderate_speed_mph": 55,
        },
    })
    start = datetime(2026, 10, 14, 7, tzinfo=timezone.utc)
    events = [
        {"event_id": "715898_20261014T000000", "station_id": 715898,
         "event_time": start, "speed": 10.005, "flow": 10, "occupancy": 0.12345},
        {"event_id": "715898_20261014T000500", "station_id": 715898,
         "event_time": start + timedelta(minutes=5), "speed": 40.0, "flow": 20, "occupancy": 0.3},
        {"event_id": "715898_20261014T001500", "station_id": 715898,
         "event_time": start + timedelta(minutes=15), "speed": 60.0, "flow": 30, "occupancy": 0.4},
    ]
    write_rows(run / "valid_events", events)
    (run / "quarantine").mkdir()

    def metric(minute, duration, speed, occupancy, flow, count):
        return {"station_id": 715898, "window_start": start + timedelta(minutes=minute),
                "window_end": start + timedelta(minutes=minute + duration),
                "avg_speed": speed, "avg_occupancy": occupancy,
                "total_flow": flow, "valid_count": count}

    metrics5 = [metric(0, 5, 10.01, 0.1235, 10, 1), metric(5, 5, 40.0, 0.3, 20, 1), metric(15, 5, 60.0, 0.4, 30, 1)]
    metrics15 = [metric(0, 15, 25.0, 0.2117, 30, 2), metric(15, 15, 60.0, 0.4, 30, 1)]
    write_rows(run / "traffic_metrics" / "5min", metrics5)
    write_rows(run / "traffic_metrics" / "15min", metrics15)
    labels = [
        ("SEVERE", "0 <= avg_speed < 20 mph (SEVERE)"),
        ("MODERATE", "40 <= avg_speed < 55 mph (MODERATE)"),
        ("FREE", "avg_speed >= 55 mph (FREE)"),
        ("CONGESTED", "20 <= avg_speed < 40 mph (CONGESTED)"),
        ("FREE", "avg_speed >= 55 mph (FREE)"),
    ]
    summary = [
        {**row, "window_minutes": 5 if index < 3 else 15,
         "traffic_state": labels[index][0], "state_explanation": labels[index][1]}
        for index, row in enumerate(metrics5 + metrics15)
    ]
    write_rows(run / "traffic_summary", summary)
    for name in ("ingest", "window_5", "window_15"):
        checkpoint = run / "checkpoints" / name
        (checkpoint / "offsets").mkdir(parents=True)
        (checkpoint / "metadata").write_text('{"id":"saved-query-id"}')
        (checkpoint / "offsets" / "0").write_text("saved offset evidence")
    kafka_query = {"progress": [{"numInputRows": 3, "sources": [
        {"description": "KafkaV2[Subscribe[traffic-test]]", "endOffset": {"traffic-test": {"0": 3}}},
    ]}]}
    write_json(reports / "streaming.json", {
        "status": "passed", "source": "kafka", "finite_demo": True,
        "topic": "traffic-test", "window_15_slide": None,
        "queries": {"query_ingest": kafka_query},
        "first_ingestion": {"query_ingest": kafka_query},
        "counts": {
            "aggregation_mode": "finite_complete_streaming_snapshots",
            "valid_events": 3, "quarantine": 0, "metrics_5min": 3,
            "metrics_15min": 2, "traffic_summary": 5,
            "traffic_states": {"SEVERE": 1, "MODERATE": 1, "FREE": 2, "CONGESTED": 1},
            "window_queries": {
                "metrics_5min": {"output_mode": "complete", "progress": [{"numInputRows": 3}]},
                "metrics_15min": {"output_mode": "complete", "progress": [{"numInputRows": 3}]},
            },
        },
    })
    return run, source


def test_independent_source_reference_checks_tail_rounding_and_labels(replay_artifacts):
    run, source = replay_artifacts
    report = verify(run, source)
    assert report["status"] == "passed"
    assert report["streaming_e2e_verified"] is True
    assert report["input_events"] == 3
    assert report["metrics_5min"] == 3
    assert report["metrics_15min"] == 2
    assert report["summary_rows"] == 5
    assert report["counts"] == {"valid_events": 3, "quarantine": 0, "metrics_5min": 3, "metrics_15min": 2, "traffic_summary": 5}
    assert report["coverage"]["date_event_counts"] == {"2026-10-14": 3}
    assert report["coverage"]["event_time_min_utc"] == "2026-10-14T07:00:00+00:00"
    assert len(report["source_sha256"]) == 64
    assert json.loads((run / "reports" / "verification.json").read_text())["status"] == "passed"


@pytest.mark.parametrize("corruption", ["speed", "flow", "count", "occupancy", "end", "key", "missing", "state", "explanation", "summary_metric", "duplicate_event"])
def test_corrupted_output_cannot_pass(replay_artifacts, corruption):
    run, source = replay_artifacts
    destination = run / "traffic_metrics" / "15min"
    if corruption in {"state", "explanation", "summary_metric"}:
        destination = run / "traffic_summary"
    if corruption == "duplicate_event":
        destination = run / "valid_events"
    rows = pq.ParquetFile(destination / "part-00000.parquet").read().to_pylist()
    if corruption == "speed":
        rows[0]["avg_speed"] = 25.01
    elif corruption == "flow":
        rows[0]["total_flow"] += 1
    elif corruption == "count":
        rows[0]["valid_count"] += 1
    elif corruption == "occupancy":
        rows[0]["avg_occupancy"] = 0.2118
    elif corruption == "end":
        rows[0]["window_end"] += timedelta(minutes=5)
    elif corruption == "key":
        rows[0]["window_start"] += timedelta(minutes=5)
    elif corruption == "missing":
        rows.pop()
    elif corruption == "state":
        rows[0]["traffic_state"] = "FREE"
    elif corruption == "explanation":
        rows[0]["state_explanation"] = "unrelated explanation"
    elif corruption == "summary_metric":
        rows[0]["total_flow"] += 1
    else:
        rows.append(rows[0])
    write_rows(destination, rows)
    with pytest.raises(VerificationError):
        verify(run, source)
    report = json.loads((run / "reports" / "verification.json").read_text())
    assert report["status"] == "failed"
    assert report["streaming_e2e_verified"] is False


@pytest.mark.parametrize("corruption", ["non_kafka", "no_progress", "checkpoint", "producer_failure", "window_evidence", "reported_count", "topic"])
def test_missing_pipeline_evidence_cannot_pass(replay_artifacts, corruption):
    run, source = replay_artifacts
    path = run / "reports" / "streaming.json"
    evidence = json.loads(path.read_text())
    if corruption == "non_kafka":
        evidence["source"] = "offline"
    elif corruption == "no_progress":
        evidence["first_ingestion"]["query_ingest"]["progress"] = []
    elif corruption == "checkpoint":
        (run / "checkpoints" / "ingest" / "metadata").unlink()
    elif corruption == "producer_failure":
        producer_path = run / "reports" / "producer.json"
        producer = json.loads(producer_path.read_text())
        producer["failed"] = 1
        write_json(producer_path, producer)
    elif corruption == "window_evidence":
        evidence["counts"]["window_queries"] = {}
    elif corruption == "reported_count":
        evidence["counts"]["metrics_15min"] = 3
    elif corruption == "topic":
        evidence["topic"] = "other-topic"
    write_json(path, evidence)
    with pytest.raises(VerificationError):
        verify(run, source)


def test_restart_can_reuse_original_kafka_progress(replay_artifacts):
    run, source = replay_artifacts
    path = run / "reports" / "streaming.json"
    evidence = json.loads(path.read_text())
    evidence["queries"]["query_ingest"]["progress"] = [{"numInputRows": 0, "sources": []}]
    write_json(path, evidence)
    assert verify(run, source)["streaming_e2e_verified"] is True


def test_restart_can_reuse_initial_window_progress(replay_artifacts):
    run, source = replay_artifacts
    path = run / "reports" / "streaming.json"
    evidence = json.loads(path.read_text())
    write_json(run / "reports" / "streaming-initial.json", evidence)
    for query in evidence["counts"]["window_queries"].values():
        query["progress"] = []
    write_json(path, evidence)
    assert verify(run, source)["streaming_e2e_verified"] is True


def test_restart_rejects_initial_window_progress_from_other_topic(replay_artifacts):
    run, source = replay_artifacts
    path = run / "reports" / "streaming.json"
    evidence = json.loads(path.read_text())
    initial = json.loads(path.read_text())
    initial["topic"] = "wrong-topic"
    write_json(run / "reports" / "streaming-initial.json", initial)
    for query in evidence["counts"]["window_queries"].values():
        query["progress"] = []
    write_json(path, evidence)
    with pytest.raises(VerificationError, match="Initial streaming report"):
        verify(run, source)


def test_cli_failure_sets_nonzero_status(replay_artifacts):
    run, source = replay_artifacts
    assert main(["--run-dir", str(run), "--source", str(source)]) == 0
    (run / "reports" / "streaming.json").unlink()
    assert main(["--run-dir", str(run), "--source", str(source)]) == 1
