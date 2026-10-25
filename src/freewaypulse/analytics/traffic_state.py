"""Explainable checkpoint traffic-state classification."""

from dataclasses import dataclass
import math
import os
from typing import Any

from freewaypulse.config import load_env


@dataclass(frozen=True)
class TrafficThresholds:
    """Speed boundaries in miles per hour, ordered from severe to free flow."""

    severe_speed_mph: float = 20.0
    congested_speed_mph: float = 40.0
    moderate_speed_mph: float = 55.0

    def __post_init__(self) -> None:
        try:
            values = (
                float(self.severe_speed_mph),
                float(self.congested_speed_mph),
                float(self.moderate_speed_mph),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("Traffic speed thresholds must be numbers") from exc

        if not all(math.isfinite(value) for value in values):
            raise ValueError("Traffic speed thresholds must be finite")
        severe, congested, moderate = values
        if not 0 <= severe < congested < moderate:
            raise ValueError(
                "Thresholds must satisfy 0 <= severe < congested < moderate"
            )

        object.__setattr__(self, "severe_speed_mph", severe)
        object.__setattr__(self, "congested_speed_mph", congested)
        object.__setattr__(self, "moderate_speed_mph", moderate)

    @classmethod
    def from_env(cls) -> "TrafficThresholds":
        """Read the shared .env thresholds once before classifying a batch."""
        load_env()
        return cls(
            severe_speed_mph=float(os.getenv("SEVERE_SPEED_MPH", "20")),
            congested_speed_mph=float(os.getenv("CONGESTED_SPEED_MPH", "40")),
            moderate_speed_mph=float(os.getenv("MODERATE_SPEED_MPH", "55")),
        )


@dataclass(frozen=True)
class TrafficStateClassification:
    traffic_state: str | None
    explanation: str


DEFAULT_THRESHOLDS = TrafficThresholds()


def classify_traffic_state(
    avg_speed: Any,
    valid_count: Any,
    thresholds: TrafficThresholds = DEFAULT_THRESHOLDS,
) -> TrafficStateClassification:
    """Classify one Spark window using its average speed in mph.

    ``valid_count`` is the number of valid observations in that window. A
    window without valid observations or a usable average speed has no state.
    """
    if isinstance(valid_count, bool):
        return TrafficStateClassification(None, "invalid_valid_count")
    try:
        count = float(valid_count)
    except (TypeError, ValueError):
        return TrafficStateClassification(None, "invalid_valid_count")
    if not math.isfinite(count) or count < 0 or not count.is_integer():
        return TrafficStateClassification(None, "invalid_valid_count")
    if count == 0:
        return TrafficStateClassification(None, "no_valid_observations")

    if isinstance(avg_speed, bool):
        return TrafficStateClassification(None, "missing_or_invalid_avg_speed")
    try:
        speed = float(avg_speed)
    except (TypeError, ValueError):
        return TrafficStateClassification(None, "missing_or_invalid_avg_speed")
    if not math.isfinite(speed):
        return TrafficStateClassification(None, "non_finite_avg_speed")
    if speed < 0:
        return TrafficStateClassification(None, "negative_avg_speed")

    severe = thresholds.severe_speed_mph
    congested = thresholds.congested_speed_mph
    moderate = thresholds.moderate_speed_mph

    if speed < severe:
        state = "SEVERE"
        explanation = f"avg_speed {speed:g} mph < severe threshold {severe:g} mph"
    elif speed < congested:
        state = "CONGESTED"
        explanation = (
            f"{severe:g} <= avg_speed {speed:g} mph < {congested:g} mph"
        )
    elif speed < moderate:
        state = "MODERATE"
        explanation = (
            f"{congested:g} <= avg_speed {speed:g} mph < {moderate:g} mph"
        )
    else:
        state = "FREE"
        explanation = f"avg_speed {speed:g} mph >= free-flow threshold {moderate:g} mph"

    return TrafficStateClassification(state, explanation)
