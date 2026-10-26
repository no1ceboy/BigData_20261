"""Verify saved Kafka/Spark outputs against an independent CSV reference.

This module never generates pipeline outputs and does not import Spark, pandas,
or Kafka. Successful verification requires the saved Kafka ingestion evidence,
checkpoints, and every persisted event, metric, and traffic-state label.
"""

import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import gzip
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterator

import pyarrow.parquet as pq

from freewaypulse.analytics import TrafficThresholds, classify_traffic_state
from freewaypulse.config import RunPaths


class VerificationError(ValueError):
    """Saved outputs or their supporting evidence did not match the source."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def _json(path: Path) -> dict:
    _require(path.is_file(), f"Missing report: {path}")
    result = json.loads(path.read_text(encoding="utf-8-sig"))
    _require(isinstance(result, dict), f"Expected JSON object: {path}")
    return result


def _utc(value: Any, *, source: bool = False) -> datetime:
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        _require(not source, "Source event_time must include a UTC offset")
        # Spark TimestampType is persisted as UTC instants without a zone.
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decimal(value: Any) -> Decimal | None:
    if value is None or str(value).strip().lower() in {"", "null", "none", "nan"}:
        return None
    number = Decimal(str(value))
    # The producer sanitizes non-finite floating values to JSON null.
    return number if number.is_finite() else None


def _number_equal(actual: Any, expected: Any, field: str, key: Any) -> None:
    if expected is None:
        _require(actual is None, f"{key}: {field} expected null, got {actual}")
        return
    _require(actual is not None and not isinstance(actual, bool), f"{key}: missing/invalid {field}")
    try:
        matches = math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-8)
    except (ValueError, TypeError, OverflowError):
        matches = False
    _require(matches, f"{key}: {field} expected {expected}, got {actual}")


def _integer(value: Any, field: str) -> int:
    _require(not isinstance(value, bool), f"Invalid integer {field}: {value}")
    number = _decimal(value)
    _require(number is not None and number == number.to_integral_value(), f"Invalid integer {field}: {value}")
    return int(number)


def _parquet_rows(path: Path, *, allow_empty: bool = False) -> Iterator[dict]:
    _require(path.is_dir(), f"Missing output directory: {path}")
    files = sorted(
        file for file in path.rglob("*.parquet")
        if not any(part.startswith(("_", ".")) for part in file.relative_to(path).parts)
    )
    _require(bool(files) or allow_empty, f"No Parquet output: {path}")
    for file in files:
        for batch in pq.ParquetFile(file).iter_batches(batch_size=16_384):
            yield from batch.to_pylist()


def _source_events(source: Path, producer: dict) -> list[dict]:
    station_ids = producer.get("station_ids", producer.get("selected_station_ids"))
    _require(isinstance(station_ids, list) and bool(station_ids), "Producer report must identify selected station_ids")
    selected = {_integer(station, "station_id") for station in station_ids}
    limit = _integer(producer.get("event_limit", producer.get("limit", 0)) or 0, "event_limit")
    _require(limit >= 0, "Producer event_limit must be >= 0")
    opener = gzip.open if source.suffix.lower() == ".gz" else open
    events = []
    with opener(source, "rt", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        aliases = {
            "event_time": "timestamp", "speed": "speed_mph",
            "flow": "flow_veh_5min", "occupancy": "occupancy_fraction",
        }
        names = {canonical: canonical if canonical in columns else raw for canonical, raw in aliases.items()}
        _require({"station_id", *names.values()}.issubset(columns), "Source CSV lacks required observation columns")
        for row in reader:
            station = _integer(row["station_id"], "station_id")
            if station not in selected:
                continue
            clock = row[names["event_time"]]
            when = _utc(clock, source=True)
            local_id = clock[:19].replace("-", "").replace(":", "").replace(" ", "T")
            event_id = row.get("event_id", f"{station}_{local_id}")
            speed, flow, occupancy = (_decimal(row[names[name]]) for name in ("speed", "flow", "occupancy"))
            _require(station > 0 and bool(event_id), "Source contains an invalid event identity")
            _require(speed is None or 0 <= speed <= 120, f"Source {event_id}: invalid speed for clean replay")
            _require(flow is None or (flow >= 0 and flow == flow.to_integral_value()), f"Source {event_id}: invalid flow")
            _require(occupancy is None or 0 <= occupancy <= 1, f"Source {event_id}: invalid occupancy")
            events.append({
                "event_id": event_id, "event_time": when, "station_id": station,
                "speed": speed, "flow": flow, "occupancy": occupancy,
                "source_date": datetime.fromisoformat(clock.replace("Z", "+00:00")).date().isoformat(),
            })
    events.sort(key=lambda item: (item["event_time"], item["station_id"]))
    return events[:limit] if limit else events


def _reference(events: list[dict], minutes: int) -> dict:
    groups: dict[tuple, dict] = {}
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    duration = minutes * 60
    for event in events:
        start = epoch + timedelta(seconds=(int((event["event_time"] - epoch).total_seconds()) // duration) * duration)
        key = (event["station_id"], start, minutes)
        group = groups.setdefault(key, {
            "window_start": start, "window_end": start + timedelta(minutes=minutes),
            "station_id": event["station_id"], "valid_count": 0, "total_flow": Decimal(0),
            "speed_sum": Decimal(0), "speed_count": 0,
            "occupancy_sum": Decimal(0), "occupancy_count": 0,
        })
        group["valid_count"] += 1
        group["total_flow"] += event["flow"] or 0
        for name in ("speed", "occupancy"):
            if event[name] is not None:
                group[f"{name}_sum"] += event[name]
                group[f"{name}_count"] += 1
    for group in groups.values():
        for name, places in (("speed", 2), ("occupancy", 4)):
            count = group.pop(f"{name}_count")
            total = group.pop(f"{name}_sum")
            group[f"avg_{name}"] = (
                (total / count).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
                if count else None
            )
    return groups


def _compare_metric(row: dict, expected: dict, key: tuple) -> None:
    _require(_utc(row.get("window_end")) == expected["window_end"], f"{key}: incorrect window_end")
    for field in ("valid_count", "total_flow"):
        _require(_integer(row.get(field), field) == expected[field], f"{key}: incorrect {field}")
    for field in ("avg_speed", "avg_occupancy"):
        _number_equal(row.get(field), expected[field], field, key)


def _key(row: dict, minutes: int) -> tuple:
    return (_integer(row.get("station_id"), "station_id"), _utc(row.get("window_start")), minutes)


def _state_explanation(state: str | None, fallback: str, thresholds: TrafficThresholds) -> str:
    severe, congested, moderate = thresholds.severe_speed_mph, thresholds.congested_speed_mph, thresholds.moderate_speed_mph
    return {
        "SEVERE": f"0 <= avg_speed < {severe:g} mph (SEVERE)",
        "CONGESTED": f"{severe:g} <= avg_speed < {congested:g} mph (CONGESTED)",
        "MODERATE": f"{congested:g} <= avg_speed < {moderate:g} mph (MODERATE)",
        "FREE": f"avg_speed >= {moderate:g} mph (FREE)",
    }.get(state, fallback)


def _streaming_evidence(run_dir: Path, producer: dict, runtime: dict) -> dict:
    evidence = _json(run_dir / "reports" / "streaming.json")
    _require(evidence.get("status") in {"passed", "success"}, "Spark report is not successful")
    _require(evidence.get("source") == "kafka" and evidence.get("finite_demo") is True,
             "Verification requires an actual Kafka finite-demo ingestion report")
    topic = producer.get("topic")
    _require(bool(topic) and evidence.get("topic") == topic == runtime.get("topic"), "Kafka topic differs across runtime/producer/Spark reports")
    _require(evidence.get("window_15_slide") is None, "Verifier expects 15-minute tumbling windows")
    queries = evidence.get("first_ingestion", evidence.get("queries", {}))
    progress = queries.get("query_ingest", {}).get("progress", [])
    kafka_sources = [
        source for batch in progress for source in batch.get("sources", [])
        if "kafka" in str(source.get("description", "")).lower() and source.get("endOffset") is not None
    ]
    _require(bool(kafka_sources), "Missing saved Kafka source progress/offset evidence")
    counts = evidence.get("counts", {})
    _require(counts.get("aggregation_mode") == "finite_complete_streaming_snapshots", "Missing streaming aggregation evidence")
    window_queries = evidence.get("first_window_queries", counts.get("window_queries", {}))
    if not all(window_queries.get(f"metrics_{minutes}min", {}).get("progress") for minutes in (5, 15)):
        initial_path = run_dir / "reports" / "streaming-initial.json"
        if initial_path.is_file():
            initial = _json(initial_path)
            _require(initial.get("topic") == topic and initial.get("source") == "kafka"
                     and initial.get("finite_demo") is True and initial.get("status") in {"passed", "success"},
                     "Initial streaming report does not match the Kafka run")
            window_queries = initial.get("counts", {}).get("window_queries", {})
    for minutes in (5, 15):
        query = window_queries.get(f"metrics_{minutes}min", {})
        _require(query.get("output_mode") == "complete" and bool(query.get("progress")), f"Missing {minutes}-minute streaming window evidence")
    for name in ("ingest", "window_5", "window_15"):
        checkpoint = run_dir / "checkpoints" / name
        _require((checkpoint / "metadata").is_file(), f"Missing {name} checkpoint metadata")
        _require(any(file.name.isdigit() for file in (checkpoint / "offsets").glob("*")), f"Missing {name} checkpoint offsets")
    return evidence


def verify(run_dir: Path | str, source: Path | str) -> dict:
    """Save a verification report; raise VerificationError if any check fails."""
    run_dir, source = Path(run_dir).resolve(), Path(source).resolve()
    report_path = run_dir / "reports" / "verification.json"
    report = {
        "engine": "kafka_spark", "status": "failed", "streaming_e2e_verified": False,
        "run_dir": str(run_dir), "source": str(source),
        "reference_engine": "python_csv_decimal_half_up",
    }
    try:
        paths = {name: Path(value) for name, value in RunPaths(run_dir).as_dict().items()}
        _require(source.is_file(), f"Missing source CSV: {source}")
        with source.open("rb") as handle:
            report["source_sha256"] = hashlib.file_digest(handle, "sha256").hexdigest()
        producer = _json(paths["reports"] / "producer.json")
        runtime = _json(paths["reports"] / "runtime.json")
        _require(producer.get("status") == "passed" and producer.get("failed") == 0, "Producer did not confirm clean delivery")
        if "event_limit" in runtime:
            _require((runtime["event_limit"] or 0) == (producer.get("event_limit") or 0), "Runtime and producer event limits differ")
        if runtime.get("source_sha256"):
            _require(runtime["source_sha256"] == report["source_sha256"], "Source checksum differs from runtime snapshot")
        thresholds = TrafficThresholds(**runtime["speed_thresholds_mph"])
        report["speed_thresholds_mph"] = runtime["speed_thresholds_mph"]
        evidence = _streaming_evidence(run_dir, producer, runtime)
        events = _source_events(source, producer)
        expected_events = {event["event_id"]: event for event in events}
        _require(bool(events) and len(expected_events) == len(events), "Selected source is empty or contains duplicate event_id values")
        _require(len(events) == producer.get("sent") == producer.get("selected_events"), "Source selection does not match producer delivery count")
        seen = set()
        for row in _parquet_rows(paths["valid_events"]):
            identity = row.get("event_id")
            _require(identity in expected_events and identity not in seen, f"Unexpected/duplicate persisted event_id: {identity}")
            seen.add(identity)
            expected = expected_events[identity]
            _require(_integer(row.get("station_id"), "station_id") == expected["station_id"] and _utc(row.get("event_time")) == expected["event_time"], f"{identity}: incorrect event identity/time")
            for field in ("speed", "flow", "occupancy"):
                _number_equal(row.get(field), expected[field], field, identity)
        _require(len(seen) == len(events), f"Persisted events: expected {len(events)}, got {len(seen)}")
        quarantine_count = sum(1 for _ in _parquet_rows(paths["quarantine"], allow_empty=True))
        _require(quarantine_count == 0, "Clean replay unexpectedly produced quarantine records")
        metrics = {}
        counts = {"valid_events": len(seen), "quarantine": quarantine_count}
        for minutes in (5, 15):
            expected = _reference(events, minutes)
            metrics.update(expected)
            observed_keys = set()
            for row in _parquet_rows(paths[f"metrics_{minutes}min"]):
                key = _key(row, minutes)
                _require(key in expected and key not in observed_keys, f"Unexpected/duplicate metric key: {key}")
                observed_keys.add(key)
                _compare_metric(row, expected[key], key)
            _require(observed_keys == expected.keys(), f"{minutes}-minute metric keys/count differ from source reference")
            counts[f"metrics_{minutes}min"] = len(observed_keys)
        summary_keys, states = set(), Counter()
        for row in _parquet_rows(paths["traffic_summary"]):
            minutes = _integer(row.get("window_minutes"), "window_minutes")
            key = _key(row, minutes)
            _require(key in metrics and key not in summary_keys, f"Unexpected/duplicate summary key: {key}")
            summary_keys.add(key)
            expected = metrics[key]
            _compare_metric(row, expected, key)
            classification = classify_traffic_state(expected["avg_speed"], expected["valid_count"], thresholds)
            _require(row.get("traffic_state") == classification.traffic_state, f"{key}: incorrect traffic_state")
            explanation = _state_explanation(classification.traffic_state, classification.explanation, thresholds)
            _require(row.get("state_explanation") == explanation, f"{key}: incorrect state_explanation")
            states[str(classification.traffic_state)] += 1
        _require(summary_keys == metrics.keys(), "Summary window keys/count differ from metric reference")
        counts["traffic_summary"] = len(summary_keys)
        for name, count in counts.items():
            _require(evidence["counts"].get(name) == count, f"Spark reported {name} count differs from persisted outputs")
        reported_states = evidence["counts"].get("traffic_states")
        _require(reported_states == dict(states), "Spark reported state distribution differs from saved summary")
        report.update({
            "status": "passed", "streaming_e2e_verified": True, "counts": counts,
            "input_events": counts["valid_events"],
            "metrics_5min": counts["metrics_5min"], "metrics_15min": counts["metrics_15min"],
            "summary_rows": counts["traffic_summary"],
            "traffic_states": dict(sorted(states.items())),
            "coverage": {
                "station_count": len({event["station_id"] for event in events}),
                "station_event_counts": dict(sorted(Counter(str(event["station_id"]) for event in events).items())),
                "date_event_counts": dict(sorted(Counter(event["source_date"] for event in events).items())),
                "event_time_min_utc": min(event["event_time"] for event in events).isoformat(),
                "event_time_max_utc": max(event["event_time"] for event in events).isoformat(),
            },
            "checks": ["kafka_progress_and_checkpoint_evidence", "producer_delivery_and_source_selection",
                       "all_persisted_event_values_and_unique_ids", "empty_clean_replay_quarantine",
                       "all_5_and_15_minute_tumbling_window_keys_and_values",
                       "all_traffic_labels_and_explanations", "reported_counts_and_state_distribution"],
        })
    except Exception as error:
        report["error"] = str(error)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        if isinstance(error, VerificationError):
            raise
        raise VerificationError(str(error)) from error
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True, help="Original observations.csv[.gz]")
    args = parser.parse_args(argv)
    try:
        report = verify(args.run_dir, args.source)
    except VerificationError as error:
        print(json.dumps({"status": "failed", "error": str(error)}))
        return 1
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
