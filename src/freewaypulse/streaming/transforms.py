"""
Pure Spark DataFrame transformations for FreewayPulse.
Implements tasks S1, S2, S3:
- S1: Parsing raw Kafka JSON payloads into typed DataFrames
- S2: Data validation, quarantine isolation, event-time watermarking and deduplication
- S3: Tumbling and sliding window aggregations
"""

from typing import Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    from_json,
    lit,
    round,
    sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import StructType

from freewaypulse.streaming.schema import RAW_TRAFFIC_EVENT_SCHEMA


def parse_kafka_stream(
    df_kafka: DataFrame,
    raw_schema: StructType = RAW_TRAFFIC_EVENT_SCHEMA,
) -> DataFrame:
    """
    S1: Consume and parse traffic-events from Kafka DataFrame.
    Maps Kafka binary 'value' column to typed traffic columns.
    Converts event_time (ISO 8601 string) into TimestampType.
    """
    # 1. Parse JSON payload from binary value
    json_df = df_kafka.selectExpr("CAST(value AS STRING) AS json_payload")
    parsed_df = json_df.select(from_json(col("json_payload"), raw_schema).alias("data")).select("data.*")

    # 2. Convert event_time string to TimestampType (supporting ISO 8601 with offset, e.g. 2026-10-14T00:00:00-07:00)
    parsed_time_df = parsed_df.withColumn(
        "event_time",
        coalesce(
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            to_timestamp(col("event_time")),
        ),
    ).withColumn("freeway", col("freeway").cast("string"))

    return parsed_time_df


def validate_records(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    S2: Data validation & quarantine isolation.
    Validates range and integrity of observations:
    - station_id > 0 AND NOT NULL
    - event_time NOT NULL
    - event_id NOT NULL AND LENGTH > 0
    - speed: NULL OR (0.0 <= speed <= 120.0)
    - flow: NULL OR flow >= 0
    - occupancy: NULL OR (0.0 <= occupancy <= 1.0)

    Returns:
        (valid_df, quarantine_df)
    """
    cond_station = col("station_id").isNotNull() & (col("station_id") > 0)
    cond_time = col("event_time").isNotNull()
    cond_id = col("event_id").isNotNull() & (col("event_id") != "")

    cond_speed = col("speed").isNull() | ((col("speed") >= 0.0) & (col("speed") <= 120.0))
    cond_flow = col("flow").isNull() | (col("flow") >= 0)
    cond_occ = col("occupancy").isNull() | (
        (col("occupancy") >= 0.0) & (col("occupancy") <= 1.0)
    )

    is_valid = cond_station & cond_time & cond_id & cond_speed & cond_flow & cond_occ

    # Assign quarantine reason for debugging invalid events
    quarantine_reason = when(~cond_station, lit("INVALID_STATION_ID")) \
        .when(~cond_time, lit("INVALID_EVENT_TIME")) \
        .when(~cond_id, lit("INVALID_EVENT_ID")) \
        .when(~cond_speed, lit("SPEED_OUT_OF_RANGE")) \
        .when(~cond_flow, lit("NEGATIVE_FLOW")) \
        .when(~cond_occ, lit("OCCUPANCY_OUT_OF_RANGE")) \
        .otherwise(lit("UNKNOWN_VALIDATION_ERROR"))

    valid_df = df.filter(is_valid)
    quarantine_df = df.filter(~is_valid).withColumn("quarantine_reason", quarantine_reason)

    return valid_df, quarantine_df


def apply_watermark_and_dedup(
    df: DataFrame,
    watermark: str = "10 minutes",
) -> DataFrame:
    """
    S2: Event-time watermarking and deterministic deduplication.
    Watermark allows Spark to discard stale state and handle late-arriving data.
    dropDuplicates on (event_id, event_time) eliminates network retransmissions.
    """
    watermarked_df = df.withWatermark("event_time", watermark)
    deduped_df = watermarked_df.dropDuplicates(["event_id", "event_time"])
    return deduped_df


def compute_traffic_windows(
    df: DataFrame,
    window_duration: str = "5 minutes",
    slide_duration: Optional[str] = None,
) -> DataFrame:
    """
    S3: Compute windowed aggregations by station_id and event_time window.
    Supports tumbling windows (slide_duration=None) and sliding windows.
    Metrics computed:
    - avg_speed: round(avg(speed), 2)
    - total_flow: sum(flow)
    - avg_occupancy: round(avg(occupancy), 4)
    - valid_count: count(1)
    """
    if slide_duration:
        win_col = window(col("event_time"), window_duration, slide_duration)
    else:
        win_col = window(col("event_time"), window_duration)

    agg_df = df.groupBy(col("station_id"), win_col.alias("win")).agg(
        round(avg(col("speed")), 2).alias("avg_speed"),
        coalesce(sum(col("flow")), lit(0)).cast("long").alias("total_flow"),
        round(avg(col("occupancy")), 4).alias("avg_occupancy"),
        count(lit(1)).cast("long").alias("valid_count"),
    )

    result_df = agg_df.select(
        col("win.start").alias("window_start"),
        col("win.end").alias("window_end"),
        col("station_id"),
        col("avg_speed"),
        col("total_flow"),
        col("avg_occupancy"),
        col("valid_count"),
    )

    return result_df
