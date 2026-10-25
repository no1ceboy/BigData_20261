"""Replay the data team's observations as canonical Kafka traffic events."""

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import pandas as pd
from confluent_kafka import KafkaError, Producer

from freewaypulse.config import ROOT, Settings


def resolve_config() -> Dict[str, Any]:
    """Use the same environment and repository paths as the platform tools."""
    settings = Settings.read()
    return {
        "bootstrap": settings.bootstrap_servers,
        "topic": settings.topic,
        "data_path": str(settings.observations_csv),
        "rate": settings.replay_rate,
        "limit": settings.event_limit or 0,
        "station_limit": settings.station_limit,
    }


def find_input_file(configured_path: str) -> Path:
    """Resolve a repository-relative input without silently replacing it."""
    path = Path(configured_path)
    if not path.is_absolute():
        path = ROOT / path
    if not path.is_file():
        raise FileNotFoundError(f"Traffic input does not exist: {path}")
    return path.resolve()


def _selected_stations(file_path: Path, station_limit: int) -> set[int] | None:
    if station_limit == 0:
        return None
    metadata = file_path.parent / "stations.csv"
    if metadata.is_file():
        stations = pd.read_csv(metadata, usecols=["station_id"])["station_id"]
        return set(stations.dropna().astype(int).sort_values().unique()[:station_limit])

    # A standalone CSV still gets a stable selection, independent of row order.
    selected: set[int] = set()
    for chunk in pd.read_csv(file_path, usecols=["station_id"], chunksize=100_000):
        selected.update(chunk["station_id"].dropna().astype(int))
        selected = set(sorted(selected)[:station_limit])
    return selected


def load_dataset(file_path: Path, limit: int = 0, station_limit: int = 100) -> pd.DataFrame:
    """Select stations, map the shared schema and replay by event time/station.

    CSV is filtered in chunks so the seven-day, 100-station demo does not load
    all 3.9 million observations into memory. An event cap applies after sorting,
    so it does not change which stations are selected. Zero means no cap.
    """
    if limit < 0 or station_limit < 0:
        raise ValueError("Event and station limits must be >= 0")
    file_path = Path(file_path)
    if file_path.suffix == ".parquet":
        df = pd.read_parquet(file_path)
        if station_limit:
            selected = sorted(df["station_id"].dropna().astype(int).unique())[:station_limit]
            df = df[df["station_id"].isin(selected)].copy()
    else:
        selected = _selected_stations(file_path, station_limit)
        chunks = []
        for chunk in pd.read_csv(file_path, chunksize=100_000):
            chunks.append(chunk if selected is None else chunk[chunk["station_id"].isin(selected)])
        if not chunks:
            raise ValueError("Traffic input contains no observations")
        df = pd.concat(chunks, ignore_index=True)

    aliases = {
        "timestamp": "event_time",
        "flow_veh_5min": "flow",
        "speed_mph": "speed",
        "occupancy_fraction": "occupancy",
    }
    for source, canonical in aliases.items():
        if source in df.columns and canonical in df.columns:
            raise ValueError(f"Input contains both source and canonical columns: {source}, {canonical}")
        if source in df.columns:
            df = df.rename(columns={source: canonical})
    required = {
        "event_time", "station_id", "flow", "speed", "occupancy",
        "observed_percent", "freeway", "direction",
    }
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Input is missing canonical traffic-event columns: {', '.join(missing)}")
    if df.empty:
        raise ValueError("Traffic input contains no observations for the selected stations")
    if df["station_id"].isna().any() or df["event_time"].isna().any():
        raise ValueError("Traffic station_id and event_time must not be null")

    # Keep the original offset in the message, but compare actual instants when
    # sorting. IDs use the local clock, as specified in docs/kafka.md.
    df["event_time"] = df["event_time"].map(
        lambda value: value.isoformat() if hasattr(value, "isoformat") else str(value)
    )
    df["_replay_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    if "event_id" not in df.columns:
        local_clock = df["event_time"].str.slice(0, 19).str.replace(r"[-:]", "", regex=True)
        local_clock = local_clock.str.replace(" ", "T", regex=False)
        df["event_id"] = df["station_id"].astype(int).astype(str) + "_" + local_clock
    if df["event_id"].isna().any():
        raise ValueError("Traffic event_id must not be null")
    df = df.sort_values(["_replay_time", "station_id"], kind="stable")
    df = df.drop(columns="_replay_time").reset_index(drop=True)
    return df.iloc[:limit].copy() if limit else df


def sanitize_for_json(record: Dict[str, Any]) -> Dict[str, Any]:
    """Produce strict JSON, including integer flow values expected by Spark."""
    clean = {}
    for key, value in record.items():
        if pd.isna(value) or (isinstance(value, float) and not math.isfinite(value)):
            value = None
        elif hasattr(value, "item"):
            value = value.item()
        if key in {"station_id", "flow", "freeway"} and isinstance(value, (int, float)):
            if float(value).is_integer():
                value = int(value)
        clean[key] = value
    return clean


class ReplayProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.sent = 0
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.ms": 20,
            "acks": "all",
            "retries": 5,
            "delivery.timeout.ms": 30000,
        })

    def _delivery_report(self, err: Optional[KafkaError], msg: Any):
        if err is not None:
            self.failed += 1
        else:
            self.sent += 1

    def run(self, df: pd.DataFrame, rate: int) -> Tuple[int, int, float]:
        if rate < 0:
            raise ValueError("Replay rate must be >= 0")
        self.sent = self.failed = 0
        start_time = time.monotonic()
        columns = list(df.columns)
        for idx, row in enumerate(df.itertuples(index=False, name=None)):
            item = sanitize_for_json(dict(zip(columns, row)))
            key = str(item["station_id"]).encode("utf-8")
            payload = json.dumps(item, allow_nan=False).encode("utf-8")
            while True:
                try:
                    self.producer.produce(
                        topic=self.topic, key=key, value=payload,
                        on_delivery=self._delivery_report,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.1)
            if idx % 1000 == 0:
                self.producer.poll(0)
            if rate:
                delay = start_time + (idx + 1) / float(rate) - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
        # flush returns queued messages that have no delivery confirmation.
        # They must make the CLI fail instead of reporting a successful replay.
        self.failed += self.producer.flush(timeout=30)
        return self.sent, self.failed, max(time.monotonic() - start_time, 0.001)


def main(argv: Sequence[str] | None = None) -> int:
    cfg = resolve_config()
    parser = argparse.ArgumentParser(description="Replay Producer for FreewayPulse")
    parser.add_argument("--bootstrap", default=cfg["bootstrap"], help="Kafka bootstrap server")
    parser.add_argument("--topic", default=cfg["topic"], help="Target Kafka topic")
    parser.add_argument("--rate", type=int, default=cfg["rate"], help="Messages/sec (0 = max)")
    parser.add_argument("--limit", type=int, default=cfg["limit"], help="Event cap after sorting (0 = all)")
    parser.add_argument("--station-limit", type=int, default=cfg["station_limit"],
                        help="Lowest N station IDs (0 = all)")
    parser.add_argument("--input", default=cfg["data_path"], help="Observations CSV[.gz] or Parquet")
    parser.add_argument("--report", type=Path, help="Write the delivery report as JSON")
    args = parser.parse_args(argv)
    if min(args.rate, args.limit, args.station_limit) < 0:
        parser.error("--rate, --limit and --station-limit must be >= 0")
    input_file = find_input_file(args.input)
    df = load_dataset(input_file, limit=args.limit, station_limit=args.station_limit)
    replay = ReplayProducer(bootstrap_servers=args.bootstrap, topic=args.topic)
    sent, failed, duration = replay.run(df, rate=args.rate)
    result = {
        "status": "passed" if failed == 0 and sent == len(df) else "failed",
        "input": str(input_file), "selected_events": len(df),
        "station_count": int(df["station_id"].nunique()),
        "station_ids": sorted(int(value) for value in df["station_id"].unique()),
        "event_time_min": str(df.iloc[0]["event_time"]),
        "event_time_max": str(df.iloc[-1]["event_time"]),
        "station_limit": args.station_limit, "event_limit": args.limit,
        "sent": sent, "failed": failed, "seconds": round(duration, 3),
        "events_per_sec": round(sent / duration, 2), "topic": args.topic,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
