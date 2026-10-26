"""
Command-line interface for running the FreewayPulse Spark Streaming pipeline.
Supports both continuous long-running streaming and single batch drain (--trigger available_now).
"""

import argparse
import json
import logging
from pathlib import Path
import sys
import time
from typing import List, Optional

from freewaypulse.config import Settings
from freewaypulse.streaming.engine import (
    build_streaming_pipeline,
    build_finite_demo_pipeline,
    create_spark_session,
    finalize_finite_demo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("freewaypulse.streaming")


def bind_run_configuration(paths: dict, configuration: dict) -> None:
    """Refuse to reuse checkpoints with a different source or query contract."""
    report_dir = Path(paths["reports"])
    report_dir.mkdir(parents=True, exist_ok=True)
    manifest = report_dir / "streaming-config.json"
    if manifest.exists():
        previous = json.loads(manifest.read_text(encoding="utf-8"))
        if previous != configuration:
            raise ValueError(
                "Run configuration differs from its checkpoint contract; "
                "resume with the original topic/broker/watermark/window mode or use a new --run-id"
            )
    else:
        manifest.write_text(json.dumps(configuration, indent=2), encoding="utf-8")


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
    parser.add_argument(
        "--finite-demo", action="store_true",
        help="Drain replay into checkpointed events, then complete-mode streaming window snapshots",
    )
    parser.add_argument(
        "--max-offsets-per-trigger", type=int, default=5000,
        help="Kafka input limit per micro-batch in finite demo mode (default: 5000)",
    )

    args = parser.parse_args(argv)
    if args.finite_demo and args.trigger != "available_now":
        parser.error("--finite-demo requires --trigger available_now")
    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger must be positive")
    return args


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

    spark = None
    try:
        configuration = {
            "source": "kafka",
            "topic": settings.topic,
            "bootstrap_servers": settings.bootstrap_servers,
            "watermark": settings.watermark,
            "window_15_slide": slide_duration,
            "finite_demo": args.finite_demo,
        }
        bind_run_configuration(settings.for_run(args.run_id).as_dict(), configuration)
        spark = create_spark_session()
        if args.finite_demo:
            pipeline = build_finite_demo_pipeline(
                spark=spark, settings=settings, run_id=args.run_id,
                max_offsets_per_trigger=args.max_offsets_per_trigger,
            )
        else:
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
                return 124

            time.sleep(2)

        summary = {
            "status": "passed",
            "run_id": args.run_id,
            **configuration,
            "metrics_5min": paths["metrics_5min"],
            "metrics_15min": paths["metrics_15min"],
            "paths": paths,
            "queries": {
                name: {
                    "id": str(query.id),
                    "input_rows_this_invocation": sum(
                        progress.get("numInputRows", 0) for progress in query.recentProgress
                    ),
                    "progress": query.recentProgress,
                }
                for name, query in pipeline.items() if name.startswith("query_")
            },
        }
        report_dir = Path(paths["reports"])
        ingestion_report = report_dir / "ingestion.json"
        if ingestion_report.exists():
            summary["first_ingestion"] = json.loads(ingestion_report.read_text(encoding="utf-8"))
        else:
            summary["first_ingestion"] = summary["queries"]
            ingestion_report.write_text(json.dumps(summary["queries"], indent=2), encoding="utf-8")
        if args.finite_demo:
            summary["counts"] = finalize_finite_demo(spark, paths, slide_duration)
            initial_windows = report_dir / "window-queries-initial.json"
            if initial_windows.exists():
                summary["first_window_queries"] = json.loads(initial_windows.read_text(encoding="utf-8"))
            else:
                summary["first_window_queries"] = summary["counts"]["window_queries"]
                initial_windows.write_text(
                    json.dumps(summary["first_window_queries"], indent=2), encoding="utf-8"
                )
        serialized = json.dumps(summary, indent=2)
        (report_dir / "streaming.json").write_text(serialized, encoding="utf-8")
        initial_report = report_dir / "streaming-initial.json"
        if not initial_report.exists():
            initial_report.write_text(serialized, encoding="utf-8")
        print(serialized)
        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down streaming queries...")
        return 130
    except Exception as e:
        logger.exception("Streaming pipeline encountered an error: %s", e)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
