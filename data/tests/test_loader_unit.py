import sys
import os
import json
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "loader")))

from app.config import Config, load_config
from app.kafka_consumer import KafkaLoaderConsumer, InvalidPayloadError, TemporaryInfrastructureError, validate_payload
from app.neo4j_writer import Neo4jWriter
from app.main import check_non_root

class DummyKafkaRecord:
    def __init__(self, value: bytes, offset: int = 10, topic: str = "csv-rows", partition: int = 0):
        self.value = value
        self.offset = offset
        self.topic = topic
        self.partition = partition

def test_config_defaults_and_env(monkeypatch):
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USER", "custom_user")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret_pass")
    monkeypatch.setenv("NEO4J_DATABASE", "CSV_Graph_DB")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "csv-rows")
    monkeypatch.setenv("KAFKA_GROUP_ID", "test-group")

    config = load_config()
    assert config.neo4j_uri == "bolt://localhost:7687"
    assert config.neo4j_user == "custom_user"
    assert config.neo4j_password == "secret_pass"
    assert config.neo4j_database == "CSV_Graph_DB"
    assert config.kafka_bootstrap_servers == "localhost:9092"
    assert config.kafka_topic == "csv-rows"
    assert config.kafka_group_id == "test-group"

def test_non_root_check_pass(monkeypatch):
    monkeypatch.setattr(os, "getuid", lambda: 10001)
    monkeypatch.setattr(os, "geteuid", lambda: 10001)
    # Should not raise or exit
    check_non_root()

def test_non_root_check_fail(monkeypatch):
    monkeypatch.setattr(os, "getuid", lambda: 0)
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    with pytest.raises(SystemExit) as exc_info:
        check_non_root()
    assert exc_info.value.code == 1

def test_stable_dataset_id():
    raw_payload = {
        "dataset_id": "producer-dataset-xyz-123",
        "row_index": 5,
        "row_data": {"colA": "valA"}
    }
    validated = validate_payload(raw_payload)
    assert validated["dataset_id"] == "producer-dataset-xyz-123"

def test_valid_payload_commit_flow():
    config = Config(
        neo4j_uri="bolt://mock:7687",
        neo4j_user="neo4j",
        neo4j_password="password",
        neo4j_database="CSV_Graph_DB",
        kafka_bootstrap_servers="mock:9092",
        kafka_topic="csv-rows",
        kafka_group_id="test-group"
    )
    mock_neo4j = MagicMock(spec=Neo4jWriter)
    loader_consumer = KafkaLoaderConsumer(config, mock_neo4j)
    loader_consumer.commit_record_offset = MagicMock()

    valid_json = json.dumps({
        "dataset_id": "ds-1",
        "row_index": 1,
        "row_data": {"Col1": "Val1"}
    }).encode("utf-8")
    record = DummyKafkaRecord(valid_json, offset=100)

    success, err = loader_consumer.process_record(record)
    assert success is True
    assert err is None
    mock_neo4j.write_row.assert_called_once()
    loader_consumer.commit_record_offset.assert_called_once_with(record)

def test_permanent_invalid_payload_commits_offset():
    config = Config(
        neo4j_uri="bolt://mock:7687",
        neo4j_user="neo4j",
        neo4j_password="password",
        neo4j_database="CSV_Graph_DB",
        kafka_bootstrap_servers="mock:9092",
        kafka_topic="csv-rows",
        kafka_group_id="test-group"
    )
    mock_neo4j = MagicMock(spec=Neo4jWriter)
    loader_consumer = KafkaLoaderConsumer(config, mock_neo4j)
    loader_consumer.commit_record_offset = MagicMock()

    invalid_json = json.dumps({
        "dataset_id": "ds-1",
        "row_index": "NOT_AN_INT",
        "row_data": {"Col1": "Val1"}
    }).encode("utf-8")
    record = DummyKafkaRecord(invalid_json, offset=101)

    success, err = loader_consumer.process_record(record)
    assert success is False
    assert "must be an integer" in err
    mock_neo4j.write_row.assert_not_called()
    # Permanent invalid message MUST commit offset so partition is not blocked!
    loader_consumer.commit_record_offset.assert_called_once_with(record)

def test_temporary_infrastructure_error_does_not_commit_offset():
    config = Config(
        neo4j_uri="bolt://mock:7687",
        neo4j_user="neo4j",
        neo4j_password="password",
        neo4j_database="CSV_Graph_DB",
        kafka_bootstrap_servers="mock:9092",
        kafka_topic="csv-rows",
        kafka_group_id="test-group"
    )
    mock_neo4j = MagicMock(spec=Neo4jWriter)
    mock_neo4j.write_row.side_effect = Exception("Neo4j database connection lost")
    loader_consumer = KafkaLoaderConsumer(config, mock_neo4j)
    loader_consumer.commit_record_offset = MagicMock()

    valid_json = json.dumps({
        "dataset_id": "ds-1",
        "row_index": 1,
        "row_data": {"Col1": "Val1"}
    }).encode("utf-8")
    record = DummyKafkaRecord(valid_json, offset=102)

    with pytest.raises(TemporaryInfrastructureError):
        loader_consumer.process_record(record)

    mock_neo4j.write_row.assert_called_once()
    # Temporary infrastructure failure MUST NOT commit offset!
    loader_consumer.commit_record_offset.assert_not_called()
