"""
Tests for Replay Producer (Contract and Smoke E2E).
"""

import json
import pytest
import pandas as pd
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition

from freewaypulse.producer import replay as replay_module
from freewaypulse.producer.replay import (
    find_input_file,
    load_dataset,
    resolve_config,
    sanitize_for_json,
    ReplayProducer,
)


@pytest.fixture(scope="session")
def app_config():
    return resolve_config()


@pytest.fixture(scope="session")
def input_path(app_config):
    return find_input_file(app_config["data_path"])


def test_load_keeps_contract(input_path):
    """Kiểm tra offline: format schema, tính toàn vẹn của event_id và xử lý null."""
    df = load_dataset(input_path, limit=100)

    # 1. Các trường dữ liệu cốt lõi
    assert "station_id" in df.columns
    assert "event_time" in df.columns
    assert "event_id" in df.columns
    assert {"speed", "flow", "occupancy"}.issubset(df.columns)
    assert not {"speed_mph", "flow_veh_5min", "occupancy_fraction"}.intersection(df.columns)

    # 2. Không chứa null ở khóa chính
    assert df["event_id"].isnull().sum() == 0
    assert df["station_id"].isnull().sum() == 0

    # 3. Format event_id chuẩn {station_id}_{timestamp}
    first_id = str(df.iloc[0]["event_id"])
    assert "_" in first_id
    assert str(df.iloc[0]["station_id"]) in first_id

    # 4. JSON dump không được sinh ra NaN string
    sample_json = json.dumps(sanitize_for_json(df.iloc[0].to_dict()))
    assert "NaN" not in sample_json


def _fetch_total_offsets(bootstrap: str, topic: str) -> int:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "test-offset-verifier",
        "enable.auto.commit": False,
    })
    try:
        meta = consumer.list_topics(topic=topic, timeout=10)
        topic_meta = meta.topics.get(topic)
        if not topic_meta:
            return 0

        total_offset = 0
        for pid in topic_meta.partitions.keys():
            tp = TopicPartition(topic, pid)
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            total_offset += high
        return total_offset
    finally:
        consumer.close()


def test_smoke_e2e(app_config, input_path):
    """Bắn 1.000 events vào Kafka thật và đối soát offset trên broker."""
    bootstrap = app_config["bootstrap"]
    topic = app_config["topic"]
    sample_limit = 1000

    try:
        initial_offset = _fetch_total_offsets(bootstrap, topic)
    except Exception as e:
        pytest.skip(f"Không thể kết nối đến broker tại {bootstrap}: {e}")

    df = load_dataset(input_path, limit=sample_limit)
    assert len(df) == sample_limit

    producer = ReplayProducer(bootstrap_servers=bootstrap, topic=topic)
    sent, failed, duration = producer.run(df, rate=0)

    assert sent == sample_limit
    assert failed == 0

    final_offset = _fetch_total_offsets(bootstrap, topic)
    assert final_offset - initial_offset == sample_limit, "Kafka broker offset không tăng đúng 1.000 messages"


@pytest.fixture
def tiny_input(tmp_path):
    path = tmp_path / "observations.csv.gz"
    pd.DataFrame([
        {"timestamp": timestamp, "station_id": station, "speed_mph": 45.0,
         "flow_veh_5min": 12, "occupancy_fraction": 0.1,
         "observed_percent": 100.0, "freeway": 5, "direction": "N"}
        for timestamp, station in [
            ("2026-10-14T00:05:00-07:00", 30),
            ("2026-10-14T00:05:00-07:00", 10),
            ("2026-10-14T00:00:00-07:00", 20),
            ("2026-10-14T00:00:00-07:00", 10),
        ]
    ]).to_csv(path, index=False)
    return path


def test_settings_use_p1_configuration(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example:19092")
    monkeypatch.setenv("KAFKA_TOPIC", "integration-test")
    monkeypatch.setenv("STATION_LIMIT", "7")
    monkeypatch.setenv("REPLAY_EVENTS_PER_SECOND", "321")
    monkeypatch.setenv("DEMO_EVENT_LIMIT", "17")
    config = resolve_config()
    assert config["bootstrap"] == "broker.example:19092"
    assert config["topic"] == "integration-test"
    assert config["station_limit"] == 7
    assert config["rate"] == 321
    assert config["limit"] == 17
    assert Path(config["data_path"]).is_absolute()


def test_station_selection_is_stable_and_event_cap_applies_after_sort(tiny_input):
    data = load_dataset(tiny_input, station_limit=2)
    assert list(data["station_id"]) == [10, 20, 10]
    assert list(data["event_id"]) == [
        "10_20261014T000000", "20_20261014T000000", "10_20261014T000500",
    ]
    assert data["event_time"].str.endswith("-07:00").all()
    capped = load_dataset(tiny_input, station_limit=2, limit=2)
    assert list(capped["event_id"]) == list(data["event_id"][:2])
    assert len(load_dataset(tiny_input, station_limit=0)) == 4


def test_source_metadata_selects_the_same_stations(tiny_input):
    pd.DataFrame({"station_id": [30, 10, 20]}).to_csv(tiny_input.parent / "stations.csv", index=False)
    assert set(load_dataset(tiny_input, station_limit=2)["station_id"]) == {10, 20}


def test_sort_uses_actual_instants_and_preserves_existing_ids(tiny_input):
    data = pd.read_csv(tiny_input).iloc[:2].copy()
    data["timestamp"] = ["2026-11-01T01:10:00-08:00", "2026-11-01T01:30:00-07:00"]
    data["event_id"] = ["existing-later", "existing-earlier"]
    data.to_csv(tiny_input, index=False)
    replayed = load_dataset(tiny_input, station_limit=0)
    assert list(replayed["event_id"]) == ["existing-earlier", "existing-later"]
    assert list(replayed["event_time"]) == list(reversed(data["timestamp"].tolist()))


def test_missing_explicit_input_never_uses_an_unrelated_sample(tmp_path):
    with pytest.raises(FileNotFoundError, match="Traffic input does not exist"):
        find_input_file(str(tmp_path / "missing.csv"))


def test_null_and_integer_flow_produce_strict_spark_json():
    clean = sanitize_for_json({"flow": 12.0, "station_id": 10.0, "speed": float("nan"),
                               "occupancy": float("inf"), "observed_percent": pd.NA})
    encoded = json.dumps(clean, allow_nan=False)
    assert json.loads(encoded) == {"flow": 12, "station_id": 10, "speed": None,
                                   "occupancy": None, "observed_percent": None}
    assert type(clean["flow"]) is int
    assert type(clean["station_id"]) is int


class FakeKafkaProducer:
    def __init__(self, config):
        self.messages = []
        self.pending = 0

    def produce(self, **message):
        self.messages.append(message)

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        confirmed = self.messages[:-self.pending] if self.pending else self.messages
        for message in confirmed:
            message["on_delivery"](None, None)
        return self.pending


def test_replay_counts_unconfirmed_flush_as_failure(monkeypatch, tiny_input):
    monkeypatch.setattr(replay_module, "Producer", FakeKafkaProducer)
    producer = ReplayProducer("fake:9092", "test")
    producer.producer.pending = 1
    sent, failed, _ = producer.run(load_dataset(tiny_input), rate=0)
    assert (sent, failed) == (3, 1)
    message = producer.producer.messages[0]
    assert message["key"] == b"10"
    assert json.loads(message["value"])["event_id"] == "10_20261014T000000"


@pytest.mark.parametrize("pending, expected_code", [(0, 0), (1, 1)])
def test_cli_writes_delivery_report_and_returns_failure(monkeypatch, tiny_input, tmp_path, pending, expected_code):
    class FakeWithPending(FakeKafkaProducer):
        def __init__(self, config):
            super().__init__(config)
            self.pending = pending

    monkeypatch.setattr(replay_module, "Producer", FakeWithPending)
    report_path = tmp_path / "reports/replay.json"
    code = replay_module.main([
        "--input", str(tiny_input), "--station-limit", "2", "--limit", "2",
        "--rate", "0", "--report", str(report_path),
    ])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert code == expected_code
    assert report["selected_events"] == 2
    assert report["station_ids"] == [10, 20]
    assert report["sent"] + report["failed"] == 2
    assert report["status"] == ("passed" if expected_code == 0 else "failed")
