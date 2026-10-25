"""
Streaming Engine execution orchestration for FreewayPulse.
Implements tasks S1 and S4:
- Initializes SparkSession configured with Kafka dependencies and tuning
- Constructs the full structured streaming pipeline connecting Kafka -> Transforms -> Parquet Sinks
- Manages fault-tolerant checkpoints conforming to P1 storage layout
"""

import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional

# Ensure reliable local runtime on Windows and container environments
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

from pyspark.sql import DataFrame, SparkSession

from freewaypulse.config import Settings
from freewaypulse.analytics.spark import add_traffic_state
from freewaypulse.streaming.schema import PARSED_TRAFFIC_EVENT_SCHEMA
from freewaypulse.streaming.transforms import (
    apply_watermark_and_dedup,
    compute_traffic_windows,
    parse_kafka_stream,
    validate_records,
)


def create_spark_session(
    app_name: str = "freewaypulse-spark-streaming",
    master: str = "local[2]",
    include_kafka_package: bool = True,
    extra_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Creates and configures an optimized SparkSession for Structured Streaming.
    - Includes org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 for Kafka integration
    - Sets shuffle partitions to 3 (matching 3 topic partitions) for low streaming latency
    - Sets session timezone to America/Los_Angeles (California time)
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .config("spark.ui.enabled", "false")
    )

    if include_kafka_package:
        # Download or load cached kafka connector jar from Maven / ivy cache
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
        )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_streaming_pipeline(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    trigger_mode: str = "continuous",
    trigger_interval: str = "10 seconds",
    window_15_slide: Optional[str] = "5 minutes",
    include_quarantine_sink: bool = True,
) -> Dict[str, Any]:
    """
    Builds and starts the end-to-end Structured Streaming pipeline:
    1. Reads stream from Kafka topic `traffic-events`
    2. S1: Parses JSON payloads
    3. S2: Validates observations and isolates quarantine records
    4. S2: Applies watermarking and deduplication
    5. S3: Computes 5-min and 15-min traffic aggregations
    6. S4: Sinks to Parquet and manages checkpoints under data/runs/<run_id>/
    """
    run_paths = settings.for_run(run_id)
    paths = run_paths.as_dict()

    # Ensure output directories exist
    for p in [paths["metrics_5min"], paths["metrics_15min"], paths["quarantine"]]:
        Path(p).mkdir(parents=True, exist_ok=True)

    # 1. Read Kafka Stream
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. S1: Parse JSON
    parsed_df = parse_kafka_stream(kafka_df)

    # 3. S2: Validate & Quarantine
    valid_df, quarantine_df = validate_records(parsed_df)

    # 4. S2: Watermark & Deduplicate
    clean_df = apply_watermark_and_dedup(valid_df, watermark=settings.watermark)

    # 5. S3: Window Aggregations (5-min & 15-min)
    metrics_5min_df = compute_traffic_windows(clean_df, window_duration="5 minutes")
    metrics_15min_df = compute_traffic_windows(
        clean_df,
        window_duration="15 minutes",
        slide_duration=window_15_slide,
    )

    # 6. S4: Sinks configuration
    def apply_trigger(writer):
        if trigger_mode == "available_now":
            return writer.trigger(availableNow=True)
        elif trigger_mode == "once":
            return writer.trigger(once=True)
        else:
            return writer.trigger(processingTime=trigger_interval)

    # 5-min metrics sink
    writer_5min = (
        metrics_5min_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", paths["metrics_5min"])
        .option("checkpointLocation", paths["window_5_checkpoint"])
    )
    query_5min = apply_trigger(writer_5min).start()

    # 15-min metrics sink
    writer_15min = (
        metrics_15min_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", paths["metrics_15min"])
        .option("checkpointLocation", paths["window_15_checkpoint"])
    )
    query_15min = apply_trigger(writer_15min).start()

    queries = {
        "query_5min": query_5min,
        "query_15min": query_15min,
        "paths": paths,
    }

    # Optional quarantine sink
    if include_quarantine_sink:
        writer_quarantine = (
            quarantine_df.writeStream.format("parquet")
            .outputMode("append")
            .option("path", paths["quarantine"])
            .option("checkpointLocation", paths["quarantine_checkpoint"])
        )
        query_quarantine = apply_trigger(writer_quarantine).start()
        queries["query_quarantine"] = query_quarantine

    return queries


def process_offline_batch(
    df_raw: DataFrame,
    watermark: str = "10 minutes",
    window_15_slide: Optional[str] = "5 minutes",
) -> Dict[str, DataFrame]:
    """
    Offline/static processing helper used for unit testing and offline batch verification.
    Applies the exact same transformation logic without streaming triggers.
    """
    valid_df, quarantine_df = validate_records(df_raw)
    clean_df = apply_watermark_and_dedup(valid_df, watermark=watermark)
    metrics_5min = compute_traffic_windows(clean_df, window_duration="5 minutes")
    metrics_15min = compute_traffic_windows(
        clean_df,
        window_duration="15 minutes",
        slide_duration=window_15_slide,
    )

    return {
        "valid": valid_df,
        "quarantine": quarantine_df,
        "clean": clean_df,
        "metrics_5min": metrics_5min,
        "metrics_15min": metrics_15min,
    }


def build_finite_demo_pipeline(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    source_df: Optional[DataFrame] = None,
    max_offsets_per_trigger: int = 5000,
) -> Dict[str, Any]:
    """Drain a finite replay into checkpointed, validated event Parquet.

    The Kafka producer must finish before this availableNow query starts. A
    watermark cannot close the final historical windows without future events;
    ``finalize_finite_demo`` therefore uses complete-mode streaming window
    snapshots after ingestion finishes. Continuous S1-S4 is unchanged.
    ``source_df`` permits the same ingestion to be exercised with a file stream.
    """
    paths = settings.for_run(run_id).as_dict()
    if max_offsets_per_trigger <= 0:
        raise ValueError("max_offsets_per_trigger must be positive")
    if source_df is None:
        source_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.bootstrap_servers)
            .option("subscribe", settings.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            .load()
        )
    if not source_df.isStreaming:
        raise ValueError("Finite demo ingestion requires a streaming source")

    valid, quarantine = validate_records(parse_kafka_stream(source_df))
    clean = apply_watermark_and_dedup(valid, watermark=settings.watermark)
    queries: Dict[str, Any] = {"paths": paths}
    try:
        for name, frame, destination, checkpoint in (
            ("ingest", clean, "valid_events", "ingest_checkpoint"),
            ("quarantine", quarantine, "quarantine", "quarantine_checkpoint"),
        ):
            queries[f"query_{name}"] = (
                frame.writeStream.format("parquet")
                .outputMode("append")
                .option("path", paths[destination])
                .option("checkpointLocation", paths[checkpoint])
                .trigger(availableNow=True)
                .start()
            )
    except Exception:
        for name, query in queries.items():
            if name.startswith("query_") and query.isActive:
                query.stop()
        raise
    return queries


def finalize_finite_demo(
    spark: SparkSession,
    paths: Dict[str, str],
    window_15_slide: Optional[str] = None,
) -> Dict[str, Any]:
    """Materialize full-window metrics/analytics from a drained finite replay.

    Input Parquet and its checkpoint remain append-only. Derived snapshots are
    complete-mode Structured Streaming aggregations, replaced per micro-batch.
    Full aggregation state is retained, so this mode is limited to finite demos.
    Snapshot replacement is not atomic across the two metric sinks/analytics.
    Restarting the same completed run does not append duplicates.
    This must only run after every ingestion query has terminated successfully.
    """
    from pyspark.sql.functions import lit

    events = spark.read.schema(PARSED_TRAFFIC_EVENT_SCHEMA).parquet(paths["valid_events"])
    event_count = events.count()
    if not event_count:
        raise ValueError("Finite replay produced no valid events; refusing to publish empty snapshots")
    event_stream = spark.readStream.schema(PARSED_TRAFFIC_EVENT_SCHEMA).parquet(paths["valid_events"])
    metric_counts: Dict[str, int] = {}
    window_queries: Dict[str, Any] = {}
    for minutes, slide, checkpoint in (
        (5, None, "window_5_checkpoint"),
        (15, window_15_slide, "window_15_checkpoint"),
    ):
        metrics = compute_traffic_windows(
            event_stream, window_duration=f"{minutes} minutes", slide_duration=slide
        )
        destination = paths[f"metrics_{minutes}min"]

        def write_snapshot(batch: DataFrame, batch_id: int, output: str = destination) -> None:
            batch.write.mode("overwrite").parquet(output)

        query = (
            metrics.writeStream.outputMode("complete")
            .foreachBatch(write_snapshot)
            .option("checkpointLocation", paths[checkpoint])
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        metric_counts[f"metrics_{minutes}min"] = spark.read.parquet(destination).count()
        window_queries[f"metrics_{minutes}min"] = {
            "id": str(query.id),
            "output_mode": "complete",
            "input_rows_this_invocation": sum(
                progress.get("numInputRows", 0) for progress in query.recentProgress
            ),
            "progress": query.recentProgress,
        }

    summary = (
        add_traffic_state(spark.read.parquet(paths["metrics_5min"]))
        .withColumn("window_minutes", lit(5))
        .unionByName(
            add_traffic_state(spark.read.parquet(paths["metrics_15min"]))
            .withColumn("window_minutes", lit(15))
        )
    )
    summary.write.mode("overwrite").parquet(paths["traffic_summary"])
    persisted_summary = spark.read.parquet(paths["traffic_summary"])
    return {
        "aggregation_mode": "finite_complete_streaming_snapshots",
        "window_queries": window_queries,
        "valid_events": event_count,
        # A clean replay may produce no data files in the quarantine sink.
        "quarantine": spark.read.schema(PARSED_TRAFFIC_EVENT_SCHEMA).parquet(paths["quarantine"]).count(),
        **metric_counts,
        "traffic_summary": persisted_summary.count(),
        "traffic_states": {
            str(row.traffic_state): row["count"]
            for row in persisted_summary.groupBy("traffic_state").count().collect()
        },
    }
