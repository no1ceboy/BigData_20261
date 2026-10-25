"""Check A2 labels against representative Spark window outputs."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from freewaypulse.analytics.spark import add_traffic_state
from freewaypulse.analytics.traffic_state import TrafficThresholds
from freewaypulse.streaming.schema import PARSED_TRAFFIC_EVENT_SCHEMA
from freewaypulse.streaming.transforms import compute_traffic_windows


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-freewaypulse-analytics")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_known_free_and_congested_windows_are_classified(spark):
    rows = [
        # Station 1: free-flow case, mean speed 67.5 mph.
        ("f1", datetime(2026, 10, 14, 8, 1), 1, 65.0, 100, 0.02, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("f2", datetime(2026, 10, 14, 8, 3), 1, 70.0, 120, 0.04, 100.0, 34.0, -118.0, "5", "N", "ML"),
        # Station 2: congested case, mean speed 27.5 mph.
        ("c1", datetime(2026, 10, 14, 8, 2), 2, 25.0, 60, 0.10, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("c2", datetime(2026, 10, 14, 8, 4), 2, 30.0, 80, 0.12, 100.0, 34.0, -118.0, "5", "N", "ML"),
    ]
    events = spark.createDataFrame(rows, PARSED_TRAFFIC_EVENT_SCHEMA)
    metrics = compute_traffic_windows(events, window_duration="5 minutes")
    classified = add_traffic_state(metrics, TrafficThresholds())

    actual = {
        row.station_id: row.asDict()
        for row in classified.collect()
    }

    free = actual[1]
    assert free["avg_speed"] == 67.5
    assert free["total_flow"] == 220
    assert free["avg_occupancy"] == 0.03
    assert free["valid_count"] == 2
    assert free["traffic_state"] == "FREE"
    assert free["state_explanation"] == "avg_speed >= 55 mph (FREE)"

    congested = actual[2]
    assert congested["avg_speed"] == 27.5
    assert congested["total_flow"] == 140
    assert congested["avg_occupancy"] == 0.11
    assert congested["valid_count"] == 2
    assert congested["traffic_state"] == "CONGESTED"
    assert congested["state_explanation"] == "20 <= avg_speed < 40 mph (CONGESTED)"
