"""
Schema definitions for FreewayPulse Streaming Ingestion and Aggregations.
Strictly conforms to the Data Contracts defined in docs/kafka.md and BigData_Checklist.xlsx.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Raw JSON Event schema as published by Replay Producer into 'traffic-events' topic
RAW_TRAFFIC_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True),
    StructField("event_time", StringType(), nullable=True),
    StructField("station_id", IntegerType(), nullable=True),
    StructField("speed_mph", DoubleType(), nullable=True),
    StructField("flow_veh_5min", IntegerType(), nullable=True),
    StructField("occupancy_fraction", DoubleType(), nullable=True),
    StructField("observed_percent", DoubleType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("freeway", StringType(), nullable=True),
    StructField("direction", StringType(), nullable=True),
    StructField("lane_type", StringType(), nullable=True),
])

# Parsed Event schema with event_time converted to TimestampType
PARSED_TRAFFIC_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=True),
    StructField("station_id", IntegerType(), nullable=True),
    StructField("speed_mph", DoubleType(), nullable=True),
    StructField("flow_veh_5min", IntegerType(), nullable=True),
    StructField("occupancy_fraction", DoubleType(), nullable=True),
    StructField("observed_percent", DoubleType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("freeway", StringType(), nullable=True),
    StructField("direction", StringType(), nullable=True),
    StructField("lane_type", StringType(), nullable=True),
])

# Standardized Metrics Output Schema (Handoff to Analytics and Platform)
WINDOWED_METRICS_SCHEMA = StructType([
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("station_id", IntegerType(), nullable=False),
    StructField("avg_speed", DoubleType(), nullable=True),
    StructField("total_flow", LongType(), nullable=False),
    StructField("avg_occupancy", DoubleType(), nullable=True),
    StructField("valid_count", LongType(), nullable=False),
])
