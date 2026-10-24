"""
Tests for Replay Producer (Contract and Smoke E2E).
"""

import json
import pytest
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition

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