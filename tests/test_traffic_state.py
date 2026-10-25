import math
import os
import unittest
from unittest.mock import patch

from freewaypulse.analytics.traffic_state import (
    TrafficThresholds,
    classify_traffic_state,
)


class TrafficStateTests(unittest.TestCase):
    def test_speed_thresholds_have_explicit_boundary_behavior(self):
        cases = [
            (19.9, "SEVERE"),
            (20, "CONGESTED"),
            (39.9, "CONGESTED"),
            (40, "MODERATE"),
            (45, "MODERATE"),
            (54.9, "MODERATE"),
            (55, "FREE"),
            (70, "FREE"),
        ]
        for avg_speed, expected_state in cases:
            with self.subTest(avg_speed=avg_speed):
                result = classify_traffic_state(avg_speed=avg_speed, valid_count=3)
                self.assertEqual(result.traffic_state, expected_state)
                self.assertTrue(result.explanation)

    def test_custom_thresholds_are_used(self):
        thresholds = TrafficThresholds(
            severe_speed_mph=10,
            congested_speed_mph=25,
            moderate_speed_mph=60,
        )

        result = classify_traffic_state(avg_speed=30, valid_count=1, thresholds=thresholds)

        self.assertEqual(result.traffic_state, "MODERATE")

    def test_thresholds_load_from_shared_environment(self):
        values = {
            "SEVERE_SPEED_MPH": "10",
            "CONGESTED_SPEED_MPH": "25",
            "MODERATE_SPEED_MPH": "60",
        }
        with patch.dict(os.environ, values):
            thresholds = TrafficThresholds.from_env()

        self.assertEqual(thresholds.severe_speed_mph, 10)
        self.assertEqual(thresholds.congested_speed_mph, 25)
        self.assertEqual(thresholds.moderate_speed_mph, 60)

    def test_empty_or_invalid_windows_are_not_classified(self):
        cases = [
            (60, 0, "no_valid_observations"),
            (None, 2, "missing_or_invalid_avg_speed"),
            (math.nan, 2, "non_finite_avg_speed"),
            (-1, 2, "negative_avg_speed"),
            (60, -1, "invalid_valid_count"),
            (60, 1.5, "invalid_valid_count"),
            (60, True, "invalid_valid_count"),
        ]
        for avg_speed, valid_count, expected_explanation in cases:
            with self.subTest(avg_speed=avg_speed, valid_count=valid_count):
                result = classify_traffic_state(
                    avg_speed=avg_speed,
                    valid_count=valid_count,
                )
                self.assertIsNone(result.traffic_state)
                self.assertEqual(result.explanation, expected_explanation)

    def test_thresholds_must_be_in_strict_order(self):
        with self.assertRaisesRegex(ValueError, "severe < congested < moderate"):
            TrafficThresholds(
                severe_speed_mph=40,
                congested_speed_mph=20,
                moderate_speed_mph=55,
            )


if __name__ == "__main__":
    unittest.main()
