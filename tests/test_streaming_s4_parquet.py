"""
Tests for Task S4: Parquet Sink, Layout and Readback.
Verifies that windowed traffic metrics written to Parquet match the
Interface contract and can be read back cleanly by downstream consumers.
"""

from datetime import datetime
import os
import shutil
import tempfile
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

from freewaypulse.config import Settings
from freewaypulse.streaming.schema import (
    PARSED_TRAFFIC_EVENT_SCHEMA,
    WINDOWED_METRICS_SCHEMA,
)
from freewaypulse.streaming.transforms import compute_traffic_windows


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-freewaypulse-s4-sink")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_parquet_sink_and_readback(spark, tmp_path):
    """S4: Writes windowed metrics to Parquet and reads back to verify schema and data."""
    sample_data = [
        ("e1", datetime(2026, 10, 14, 0, 1, 0), 715898, 65.4, 180, 0.035, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("e2", datetime(2026, 10, 14, 0, 4, 0), 715898, 70.2, 220, 0.040, 100.0, 34.0, -118.0, "5", "N", "ML"),
        ("e3", datetime(2026, 10, 14, 0, 2, 0), 760063, 55.0, 120, 0.025, 100.0, 34.0, -118.0, "105", "E", "ML"),
    ]

    df = spark.createDataFrame(sample_data, PARSED_TRAFFIC_EVENT_SCHEMA)
    df_metrics = compute_traffic_windows(df, window_duration="5 minutes")

    output_dir = tmp_path / "traffic_metrics" / "5min"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Persist as Parquet
    df_metrics.write.mode("overwrite").parquet(str(output_dir))

    # Read back with Spark to verify schema preservation
    readback_df = spark.read.parquet(str(output_dir))
    assert set(readback_df.columns) == {
        "window_start",
        "window_end",
        "station_id",
        "avg_speed",
        "total_flow",
        "avg_occupancy",
        "valid_count",
    }
    assert readback_df.count() == 2

    # Verify values for station 715898
    st_715898 = readback_df.filter(readback_df.station_id == 715898).collect()[0]
    assert st_715898["total_flow"] == 400
    assert abs(st_715898["avg_speed"] - 67.8) < 0.01
    assert st_715898["valid_count"] == 2
