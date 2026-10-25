"""
Unit tests for FreewayPulse Spark Streaming transforms (Tasks S1, S2, S3).
Validates schema compliance, validation rules, quarantine classification,
watermark deduplication, and window aggregation math.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import pytest

# Ensure Windows/Local environment compatibility
if "SPARK_LOCAL_IP" not in os.environ:
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if sys.platform == "win32":
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home or not (Path(java_home) / "bin" / "java.exe").exists():
        for candidate in [
            Path("C:/Program Files/Java/latest"),
            Path("C:/Program Files/Java/jre1.8.0_491"),
        ]:
            if (candidate / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                break

from freewaypulse.streaming.schema import (
    PARSED_TRAFFIC_EVENT_SCHEMA,
    RAW_TRAFFIC_EVENT_SCHEMA,
    WINDOWED_METRICS_SCHEMA,
)
from freewaypulse.streaming.transforms import (
    apply_watermark_and_dedup,
    compute_traffic_windows,
    validate_records,
)


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for unit tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-freewaypulse-streaming")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_schema_contracts():
    """Verify minimum required fields as defined in BigData_Checklist.xlsx Interfaces."""
    # 1. Raw event schema check
    raw_names = set(RAW_TRAFFIC_EVENT_SCHEMA.fieldNames())
    required_raw = {
        "event_id",
        "event_time",
        "station_id",
        "speed_mph",
        "flow_veh_5min",
        "occupancy_fraction",
        "observed_percent",
    }
    assert required_raw.issubset(raw_names), f"Missing fields in raw schema: {required_raw - raw_names}"

    # 2. Windowed metrics output schema check
    metrics_names = set(WINDOWED_METRICS_SCHEMA.fieldNames())
    required_metrics = {
        "window_start",
        "window_end",
        "station_id",
        "avg_speed",
        "total_flow",
        "avg_occupancy",
        "valid_count",
    }
    assert required_metrics.issubset(metrics_names), f"Missing fields in metrics schema: {required_metrics - metrics_names}"


def test_validation_and_quarantine(spark):
    """S2: Tests that invalid records are partitioned into quarantine with clear reasons."""
    data = [
        # 1. Valid record
        ("715898_20261014T000000", datetime(2026, 10, 14, 0, 0, 0), 715898, 65.0, 150, 0.03, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # 2. Invalid speed (> 120 mph)
        ("715898_20261014T000500", datetime(2026, 10, 14, 0, 5, 0), 715898, 150.0, 100, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # 3. Invalid negative flow
        ("715898_20261014T001000", datetime(2026, 10, 14, 0, 10, 0), 715898, 60.0, -5, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # 4. Invalid occupancy (> 1.0)
        ("715898_20261014T001500", datetime(2026, 10, 14, 0, 15, 0), 715898, 55.0, 80, 1.5, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # 5. Invalid null station_id
        ("null_station", datetime(2026, 10, 14, 0, 20, 0), None, 65.0, 100, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
    ]

    df = spark.createDataFrame(data, PARSED_TRAFFIC_EVENT_SCHEMA)
    valid_df, quarantine_df = validate_records(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 4

    reasons = [row["quarantine_reason"] for row in quarantine_df.collect()]
    assert "SPEED_OUT_OF_RANGE" in reasons
    assert "NEGATIVE_FLOW" in reasons
    assert "OCCUPANCY_OUT_OF_RANGE" in reasons
    assert "INVALID_STATION_ID" in reasons


def test_deduplication(spark):
    """S2: Tests that duplicate events with same event_id are discarded."""
    data = [
        ("dup_01", datetime(2026, 10, 14, 0, 0, 0), 715898, 65.0, 100, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("dup_01", datetime(2026, 10, 14, 0, 0, 0), 715898, 65.0, 100, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("unique_02", datetime(2026, 10, 14, 0, 5, 0), 715898, 70.0, 120, 0.025, 100.0, 34.0, -118.0, "5", "N", "ML"),
    ]

    df = spark.createDataFrame(data, PARSED_TRAFFIC_EVENT_SCHEMA)
    deduped = apply_watermark_and_dedup(df, watermark="10 minutes")

    assert deduped.count() == 2


def test_window_aggregations_math(spark):
    """S3: Validates 5-minute window math (avg_speed, total_flow, avg_occupancy, valid_count)."""
    data = [
        # Window 1 (00:00 - 00:05) for Station 715898: 2 records
        ("e1", datetime(2026, 10, 14, 0, 1, 0), 715898, 60.0, 100, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("e2", datetime(2026, 10, 14, 0, 3, 0), 715898, 80.0, 200, 0.04, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # Window 2 (00:05 - 00:10) for Station 715898: 1 record
        ("e3", datetime(2026, 10, 14, 0, 6, 0), 715898, 50.0, 150, 0.03, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # Window 1 (00:00 - 00:05) for Station 999999 (different station)
        ("e4", datetime(2026, 10, 14, 0, 2, 0), 999999, 40.0, 50, 0.01, 100.0, 34.0, -118.0, "5", "N", "ML"),
    ]

    df = spark.createDataFrame(data, PARSED_TRAFFIC_EVENT_SCHEMA)
    result_df = compute_traffic_windows(df, window_duration="5 minutes")

    rows = result_df.collect()
    assert len(rows) == 3

    # Check Station 715898 in Window 1 (00:00 - 00:05)
    w1_715898 = [
        r for r in rows
        if r["station_id"] == 715898 and r["window_start"] == datetime(2026, 10, 14, 0, 0, 0)
    ]
    assert len(w1_715898) == 1
    rec = w1_715898[0]
    assert rec["avg_speed"] == 70.0  # (60 + 80) / 2
    assert rec["total_flow"] == 300  # 100 + 200
    assert rec["avg_occupancy"] == 0.03  # (0.02 + 0.04) / 2
    assert rec["valid_count"] == 2
