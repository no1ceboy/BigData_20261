"""Spark expressions that add the checkpoint traffic-state labels to metrics."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, isnan, lit, when

from freewaypulse.analytics.traffic_state import TrafficThresholds


def add_traffic_state(
    metrics: DataFrame,
    thresholds: TrafficThresholds | None = None,
) -> DataFrame:
    """Add ``traffic_state`` and ``state_explanation`` to Spark window metrics.

    The labels use only ``avg_speed`` and ``valid_count``. Invalid or empty
    windows retain their metrics and receive a null state plus an explanation.
    """
    thresholds = thresholds or TrafficThresholds.from_env()
    speed = col("avg_speed")
    count = col("valid_count")
    speed_infinite = spark_abs(speed) == lit(float("inf"))

    invalid_count = count.isNull() | (count < 0)
    no_observations = count == 0
    missing_speed = speed.isNull()
    non_finite_speed = isnan(speed) | speed_infinite
    negative_speed = speed < 0

    state = (
        when(invalid_count | no_observations | missing_speed | non_finite_speed | negative_speed,
             lit(None).cast("string"))
        .when(speed < thresholds.severe_speed_mph, lit("SEVERE"))
        .when(speed < thresholds.congested_speed_mph, lit("CONGESTED"))
        .when(speed < thresholds.moderate_speed_mph, lit("MODERATE"))
        .otherwise(lit("FREE"))
    )

    explanation = (
        when(invalid_count, lit("invalid_valid_count"))
        .when(no_observations, lit("no_valid_observations"))
        .when(missing_speed, lit("missing_or_invalid_avg_speed"))
        .when(non_finite_speed, lit("non_finite_avg_speed"))
        .when(negative_speed, lit("negative_avg_speed"))
        .when(
            speed < thresholds.severe_speed_mph,
            lit(f"0 <= avg_speed < {thresholds.severe_speed_mph:g} mph (SEVERE)"),
        )
        .when(
            speed < thresholds.congested_speed_mph,
            lit(
                f"{thresholds.severe_speed_mph:g} <= avg_speed < "
                f"{thresholds.congested_speed_mph:g} mph (CONGESTED)"
            ),
        )
        .when(
            speed < thresholds.moderate_speed_mph,
            lit(
                f"{thresholds.congested_speed_mph:g} <= avg_speed < "
                f"{thresholds.moderate_speed_mph:g} mph (MODERATE)"
            ),
        )
        .otherwise(
            lit(f"avg_speed >= {thresholds.moderate_speed_mph:g} mph (FREE)")
        )
    )

    return metrics.withColumn("traffic_state", state).withColumn(
        "state_explanation", explanation
    )
