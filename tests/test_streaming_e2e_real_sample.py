"""
End-to-End pipeline verification on real Caltrans PeMS observations.
Simulates: Producer JSON messages -> Spark Parsing -> Validation ->
Deduplication -> 5-min & 15-min Windowing -> Parquet Persistence.
"""

from datetime import datetime
import json
import gzip
import os
from pathlib import Path
import sys
import pandas as pd
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
from freewaypulse.streaming.schema import RAW_TRAFFIC_EVENT_SCHEMA
from freewaypulse.streaming.transforms import (
    apply_watermark_and_dedup,
    compute_traffic_windows,
    validate_records,
)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-real-pems-e2e")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_real_pems_sample_pipeline(spark, tmp_path):
    settings = Settings.read()
    obs_file = settings.observations_csv

    assert obs_file.exists(), f"Source dataset {obs_file} not found"

    # 1. Read first 1,000 real observations from observations.csv.gz
    df_raw_pandas = pd.read_csv(obs_file, nrows=1000, compression="gzip")
    assert len(df_raw_pandas) == 1000

    # Ensure event_id and event_time exist according to contract
    if "event_time" not in df_raw_pandas.columns and "timestamp" in df_raw_pandas.columns:
        df_raw_pandas["event_time"] = df_raw_pandas["timestamp"]
    if "event_id" not in df_raw_pandas.columns:
        ts_clean = pd.to_datetime(df_raw_pandas["event_time"]).dt.strftime("%Y%m%dT%H%M%S")
        df_raw_pandas["event_id"] = df_raw_pandas["station_id"].astype(str) + "_" + ts_clean

    records = df_raw_pandas.to_dict(orient="records")

    # 2. Simulate Kafka messages: list of (key, json_string)
    kafka_simulated = [
        (str(r["station_id"]), json.dumps({
            k: (None if (isinstance(v, float) and pd.isna(v)) else v)
            for k, v in r.items()
        }))
        for r in records
    ]

    # Create Spark DataFrame mimicking raw Kafka value
    df_kafka_sim = spark.createDataFrame(
        [(k, v.encode("utf-8")) for k, v in kafka_simulated],
        ["key", "value"],
    )

    # 3. S1: Parse JSON
    from freewaypulse.streaming.transforms import parse_kafka_stream
    parsed_df = parse_kafka_stream(df_kafka_sim)
    assert parsed_df.count() == 1000

    # 4. S2: Validate records
    valid_df, quarantine_df = validate_records(parsed_df)
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    assert valid_count + quarantine_count == 1000
    assert valid_count > 900, "Expected vast majority of real PeMS records to be valid"

    # 5. S2: Watermark & Deduplication
    clean_df = apply_watermark_and_dedup(valid_df, watermark="10 minutes")
    clean_count = clean_df.count()
    assert clean_count <= valid_count

    # 6. S3: Window Aggregations
    m5_df = compute_traffic_windows(clean_df, window_duration="5 minutes")
    m15_df = compute_traffic_windows(clean_df, window_duration="15 minutes", slide_duration="5 minutes")

    assert m5_df.count() > 0
    assert m15_df.count() > 0

    # 7. S4: Parquet Output & Readback
    out_5min = tmp_path / "traffic_metrics" / "5min"
    m5_df.write.mode("overwrite").parquet(str(out_5min))

    from pyspark.sql.functions import col
    readback = spark.read.parquet(str(out_5min))
    assert readback.count() == m5_df.count()

    # Verify realistic traffic values on real sample
    sample_stat = readback.filter(col("avg_speed").isNotNull()).first()
    assert sample_stat is not None
    assert 0.0 <= sample_stat["avg_speed"] <= 120.0
    assert sample_stat["total_flow"] >= 0
