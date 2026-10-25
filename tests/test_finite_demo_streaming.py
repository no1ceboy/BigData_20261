"""Finite replay tail windows and checkpoint restart use real file streaming."""

from dataclasses import replace
import json
from pathlib import Path

import pytest

from freewaypulse.config import Settings
from freewaypulse.streaming.cli import bind_run_configuration, parse_args
from freewaypulse.streaming.engine import (
    build_finite_demo_pipeline,
    create_spark_session,
    finalize_finite_demo,
)


def test_finite_demo_requires_bounded_trigger_and_positive_batch_limit():
    for options in (
        ["--finite-demo"],
        ["--finite-demo", "--trigger", "once"],
        ["--max-offsets-per-trigger", "0"],
    ):
        with pytest.raises(SystemExit):
            parse_args(options)
    args = parse_args(["--finite-demo", "--trigger", "available_now", "--slide-15min", "none"])
    assert args.finite_demo
    assert args.max_offsets_per_trigger == 5000


def test_checkpoint_contract_rejects_source_or_window_changes(tmp_path):
    paths = {"reports": str(tmp_path / "reports")}
    configuration = {"topic": "run-01", "finite_demo": True, "watermark": "10 minutes"}
    bind_run_configuration(paths, configuration)
    bind_run_configuration(paths, configuration)
    for change in ({"topic": "other-topic"}, {"finite_demo": False}, {"watermark": "5 minutes"}):
        with pytest.raises(ValueError, match="checkpoint contract"):
            bind_run_configuration(paths, {**configuration, **change})


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session(
        app_name="test-finite-demo-streaming", include_kafka_package=False,
        extra_configs={"spark.sql.shuffle.partitions": "2"},
    )
    yield session
    session.stop()


def test_finite_replay_closes_tail_and_restart_preserves_counts(spark, tmp_path):
    source_dir = tmp_path / "kafka-json-values"
    source_dir.mkdir()

    def event(event_id, minute, speed=60.0):
        return {
            "event_id": event_id,
            "event_time": f"2026-10-14T00:{minute:02d}:00-07:00",
            "station_id": 715898,
            "speed": speed,
            "flow": 10,
            "occupancy": 0.02,
            "observed_percent": 100.0,
        }

    records = [event("e1", 0), event("e1", 0), event("e2", 5), event("e3", 10), event("bad", 10, 150.0)]
    (source_dir / "batch-01.jsonl").write_text(
        "\n".join(json.dumps(row) for row in records) + "\n", encoding="utf-8"
    )
    settings = replace(Settings.read(), runs_dir=tmp_path / "runs")

    def drain():
        source = spark.readStream.text(str(source_dir))
        pipeline = build_finite_demo_pipeline(spark, settings, "finite-test", source_df=source)
        for name, query in pipeline.items():
            if name.startswith("query_"):
                assert query.awaitTermination(90), f"{name} did not drain"
        return pipeline["paths"]

    paths = drain()
    first = finalize_finite_demo(spark, paths)
    assert first["valid_events"] == 3
    assert first["quarantine"] == 1
    assert first["metrics_5min"] == 3
    assert first["metrics_15min"] == 1
    assert first["traffic_summary"] == 4
    assert first["traffic_states"] == {"FREE": 4}
    tail = spark.read.parquet(paths["metrics_5min"]).orderBy("window_start").collect()[-1]
    assert tail.window_start.minute == 10
    assert tail.valid_count == 1
    assert tail.total_flow == 10
    assert Path(paths["ingest_checkpoint"]).is_dir()

    # No new source files: offsets resume, snapshots remain identical.
    drain()
    restarted = finalize_finite_demo(spark, paths)
    assert {
        key: value for key, value in restarted.items() if key != "window_queries"
    } == {key: value for key, value in first.items() if key != "window_queries"}
    assert all(
        query["input_rows_this_invocation"] == 0
        for query in restarted["window_queries"].values()
    )
