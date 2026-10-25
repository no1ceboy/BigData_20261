"""
Command-line interface for running the FreewayPulse Spark Streaming pipeline.
Supports both continuous long-running streaming and single batch drain (--trigger available_now).
"""

import argparse
import json
import logging
import sys
import time
from typing import List, Optional

from freewaypulse.config import Settings
from freewaypulse.streaming.engine import (
    build_streaming_pipeline,
    create_spark_session,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("freewaypulse.streaming")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FreewayPulse Spark Structured Streaming Engine (Tasks S1-S4)",
    )
    parser.add_argument(
        "--run-id",
        default="stream-run-01",
        help="Run identifier used for isolated storage paths under data/runs/<run_id>/",
    )
    parser.add_argument(
        "--trigger",
        choices=["continuous", "available_now", "once"],
        default="continuous",
        help="Trigger mode for streaming queries (default: continuous)",
    )
    parser.add_argument(
        "--interval",
        default="5 seconds",
        help="Micro-batch interval when running in continuous mode (default: 5 seconds)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="Maximum runtime in seconds (0 = run indefinitely until interrupted)",
    )
    parser.add_argument(
        "--slide-15min",
        default="5 minutes",
        help="Slide duration for 15-minute window ('5 minutes' for sliding, 'none' for tumbling)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Override Kafka bootstrap servers address (defaults to Settings.bootstrap_servers)",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Override Kafka topic name (defaults to Settings.topic)",
    )

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    settings = Settings.read()

    # Allow CLI overrides
    if args.bootstrap_servers:
        object.__setattr__(settings, "bootstrap_servers", args.bootstrap_servers)
    if args.topic:
        object.__setattr__(settings, "topic", args.topic)

    slide_duration = None if args.slide_15min.lower() in ("none", "", "null") else args.slide_15min

    logger.info("Starting Spark Streaming Engine...")
    logger.info("Run ID: %s", args.run_id)
    logger.info("Kafka Broker: %s", settings.bootstrap_servers)
    logger.info("Topic: %s", settings.topic)
    logger.info("Watermark: %s", settings.watermark)
    logger.info("Trigger mode: %s", args.trigger)

    spark = create_spark_session()

    try:
        pipeline = build_streaming_pipeline(
            spark=spark,
            settings=settings,
            run_id=args.run_id,
            trigger_mode=args.trigger,
            trigger_interval=args.interval,
            window_15_slide=slide_duration,
        )

        paths = pipeline["paths"]
        logger.info("5-min metrics destination: %s", paths["metrics_5min"])
        logger.info("15-min metrics destination: %s", paths["metrics_15min"])
        logger.info("Streaming queries active. Waiting for data...")

        start_time = time.time()
        while True:
            # Check query status
            active = False
            for name, q in pipeline.items():
                if name.startswith("query_"):
                    if q.isActive:
                        active = True
                    if q.exception():
                        logger.error("Query %s failed with exception: %s", name, q.exception())
                        return 1

            if not active and args.trigger in ("available_now", "once"):
                logger.info("Batch trigger completed for all active queries.")
                break

            if args.timeout > 0 and (time.time() - start_time) >= args.timeout:
                logger.info("Reached timeout of %d seconds. Stopping queries gracefully...", args.timeout)
                for name, q in pipeline.items():
                    if name.startswith("query_") and q.isActive:
                        q.stop()
                break

            time.sleep(2)

        summary = {
            "status": "success",
            "run_id": args.run_id,
            "metrics_5min": paths["metrics_5min"],
            "metrics_15min": paths["metrics_15min"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down streaming queries...")
        return 0
    except Exception as e:
        logger.exception("Streaming pipeline encountered an error: %s", e)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
