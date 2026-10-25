"""
Replay Producer for FreewayPulse.
Maps data from dataset (observations.csv.gz) to Kafka traffic-events.
"""

import argparse
import gzip
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from confluent_kafka import KafkaError, Producer

try:
    from freewaypulse.config import Settings
    _SETTINGS = Settings()
except Exception:
    _SETTINGS = None


def resolve_config() -> Dict[str, Any]:
    """Ưu tiên biến môi trường, Settings của P1, sau đó đến default."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        if _SETTINGS and hasattr(_SETTINGS, "KAFKA_BOOTSTRAP_SERVERS"):
            bootstrap = _SETTINGS.KAFKA_BOOTSTRAP_SERVERS
        else:
            bootstrap = "127.0.0.1:29092"

    topic = os.getenv("KAFKA_TOPIC")
    if not topic:
        if _SETTINGS and hasattr(_SETTINGS, "KAFKA_TOPIC"):
            topic = _SETTINGS.KAFKA_TOPIC
        else:
            topic = "traffic-events"

    # Đường dẫn chuẩn theo bàn giao của Sơn & Thanh
    default_data_path = "data/d07-1week-mainline/processed/observations.csv.gz"
    
    rate = int(os.getenv("REPLAY_EVENTS_PER_SECOND", "1000"))
    limit = int(os.getenv("DEMO_EVENT_LIMIT", "0"))

    return {
        "bootstrap": bootstrap,
        "topic": topic,
        "data_path": default_data_path,
        "rate": rate,
        "limit": limit,
    }


def find_input_file(configured_path: str) -> Path:
    """Xác định file dữ liệu thực tế."""
    p = Path(configured_path)
    if p.exists():
        return p

    # Các vị trí fallback
    fallbacks = [
        Path("data/sample/observations.csv.gz"),
        Path("data/sample/traffic_7d.parquet"),
        Path("data/sample/observations.csv"),
    ]
    for fb in fallbacks:
        if fb.exists():
            return fb

    raise FileNotFoundError(f"Không tìm thấy file dữ liệu tại '{configured_path}' hoặc trong data/sample/")


def load_dataset(file_path: Path, limit: int = 0) -> pd.DataFrame:
    """
    Đọc dữ liệu observations.csv.gz của Sơn và chuẩn hóa contract:
    - timestamp -> event_time (giữ nguyên chuỗi ISO chuẩn)
    - sinh event_id ổn định {station_id}_{timestamp} phục vụ Spark dedup
    - sắp xếp theo event_time, station_id để replay mang tính deterministic
    """
    if file_path.suffix == ".parquet":
        df = pd.read_parquet(file_path)
    else:
        df = pd.read_csv(file_path, compression="gzip" if str(file_path).endswith(".gz") else None)

    # Đổi tên timestamp -> event_time theo contract của Spark
    if "event_time" not in df.columns and "timestamp" in df.columns:
        df["event_time"] = df["timestamp"]

    # Đảm bảo event_id tồn tại
    if "event_id" not in df.columns:
        # Chuẩn hóa timestamp thành chuỗi gọn cho id
        ts_clean = pd.to_datetime(df["event_time"]).dt.strftime("%Y%m%dT%H%M%S")
        df["event_id"] = df["station_id"].astype(str) + "_" + ts_clean

    # Sắp xếp ổn định
    df = df.sort_values(by=["event_time", "station_id"], ascending=True).reset_index(drop=True)

    if limit > 0:
        df = df.iloc[:limit]

    return df


def sanitize_for_json(record: Dict[str, Any]) -> Dict[str, Any]:
    """Chuyển NaN/Inf thành None để tránh sinh chuỗi NaN không hợp lệ trong JSON."""
    clean = {}
    for k, v in record.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            clean[k] = None
        else:
            clean[k] = v
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
        })

    def _delivery_report(self, err: Optional[KafkaError], msg: Any):
        if err is not None:
            self.failed += 1
        else:
            self.sent += 1

    def run(self, df: pd.DataFrame, rate: int) -> Tuple[int, int, float]:
        start_time = time.time()
        records = df.to_dict(orient="records")

        for idx, item in enumerate(records):
            clean_item = sanitize_for_json(item)
            key = str(clean_item.get("station_id", "")).encode("utf-8")
            payload = json.dumps(clean_item).encode("utf-8")

            while True:
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=payload,
                        on_delivery=self._delivery_report,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.1)

            if idx % 1000 == 0:
                self.producer.poll(0)

            # Điều khiển tốc độ (pacing)
            if rate > 0:
                target_time = start_time + (idx + 1) / float(rate)
                now = time.time()
                if target_time > now:
                    time.sleep(target_time - now)

        self.producer.flush(timeout=30)
        duration = max(time.time() - start_time, 0.001)
        return self.sent, self.failed, duration


def main():
    cfg = resolve_config()

    parser = argparse.ArgumentParser(description="Replay Producer for FreewayPulse")
    parser.add_argument("--bootstrap", default=cfg["bootstrap"], help="Kafka bootstrap server")
    parser.add_argument("--topic", default=cfg["topic"], help="Target Kafka topic")
    parser.add_argument("--rate", type=int, default=cfg["rate"], help="Target messages/sec (0 = max)")
    parser.add_argument("--limit", type=int, default=cfg["limit"], help="Number of records to stream")
    parser.add_argument("--input", default=cfg["data_path"], help="Path to observations.csv.gz or parquet")
    args = parser.parse_args()

    input_file = find_input_file(args.input)
    df = load_dataset(input_file, limit=args.limit)

    replay = ReplayProducer(bootstrap_servers=args.bootstrap, topic=args.topic)
    sent, failed, duration = replay.run(df, rate=args.rate)

    result = {
        "sent": sent,
        "failed": failed,
        "seconds": round(duration, 3),
        "events_per_sec": round(sent / duration, 2),
        "topic": args.topic,
    }
    print(json.dumps(result))

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()