"""
FreewayPulse Streaming Module (Apache Spark Structured Streaming).
Implements S1, S2, S3, S4 tasks:
- S1: Consume and parse traffic-events from Kafka
- S2: Validation, Deduplication and Event-time Watermarking
- S3: 5-minute and 15-minute traffic window aggregations
- S4: Parquet Sink and Checkpointing
"""

from freewaypulse.streaming.schema import (
    RAW_TRAFFIC_EVENT_SCHEMA,
    WINDOWED_METRICS_SCHEMA,
)
from freewaypulse.streaming.transforms import (
    parse_kafka_stream,
    validate_records,
    apply_watermark_and_dedup,
    compute_traffic_windows,
)
from freewaypulse.streaming.engine import (
    create_spark_session,
    build_streaming_pipeline,
)

__all__ = [
    "RAW_TRAFFIC_EVENT_SCHEMA",
    "WINDOWED_METRICS_SCHEMA",
    "parse_kafka_stream",
    "validate_records",
    "apply_watermark_and_dedup",
    "compute_traffic_windows",
    "create_spark_session",
    "build_streaming_pipeline",
]
