import sys
import os
import json
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "loader")))

from app.config import Config, load_config
from app.neo4j_writer import Neo4jWriter
from app.kafka_consumer import KafkaLoaderConsumer
from app.queries import (
    CLEAN_TEST_DATASET_QUERY,
    VERIFY_DATASET_ROW_COUNTS_QUERY,
    VERIFY_RELATIONSHIP_COUNT_QUERY,
    VERIFY_DUPLICATES_QUERY
)

class DummyRecord:
    def __init__(self, value: bytes, offset: int = 0):
        self.value = value
        self.offset = offset
        self.topic = "csv-rows"
        self.partition = 0

def test_idempotency_100_row_replay():
    """
    Neo4j Integration Test:
    1. Connect to CSV_Graph_DB (skip cleanly if unavailable).
    2. Clean test dataset 'integration-test-100'.
    3. Process 100 unique row messages.
    4. Process the exact same 100 messages again.
    5. Verify counts: Datasets=1, Rows=100, Relationships=100, Duplicates=0.
    """
    base_config = load_config()
    # Fast failure detection for skipping when Neo4j container is not running locally
    config = Config(
        neo4j_uri=base_config.neo4j_uri,
        neo4j_user=base_config.neo4j_user,
        neo4j_password=base_config.neo4j_password,
        neo4j_database=base_config.neo4j_database,
        kafka_bootstrap_servers=base_config.kafka_bootstrap_servers,
        kafka_topic=base_config.kafka_topic,
        kafka_group_id=base_config.kafka_group_id,
        max_retries=2,
        retry_backoff_sec=0.1
    )
    writer = Neo4jWriter(config)

    try:
        writer.connect_and_verify()
    except Exception as e:
        pytest.skip(f"Neo4j database unavailable - skipping integration test: {e}")

    dataset_id = "integration-test-100"

    # Clean existing test data before run
    with writer.driver.session(database=config.neo4j_database) as session:
        session.run(CLEAN_TEST_DATASET_QUERY, {"dataset_id": dataset_id})

    consumer = KafkaLoaderConsumer(config, writer)
    consumer.commit_record_offset = lambda record: None  # mock offset commit for integration test runner

    # Create 100 unique messages
    messages = []
    for i in range(1, 101):
        payload = {
            "dataset_id": dataset_id,
            "row_index": i,
            "row_data": {
                "Customer": f"Customer_{i}",
                "Order": f"O_{1000 + i}",
                "Amount": 10.5 * i,
                "IsVIP": (i % 2 == 0)
            },
            "filename": "integration_100_test.csv",
            "uploaded_at": "2026-09-11T12:00:00Z"
        }
        record = DummyRecord(json.dumps(payload).encode('utf-8'), offset=i)
        messages.append(record)

    # First pass: process all 100 messages
    for rec in messages:
        success, err = consumer.process_record(rec)
        assert success is True, f"Failed processing record offset {rec.offset}: {err}"

    # Verify counts after first pass
    with writer.driver.session(database=config.neo4j_database) as session:
        r1 = session.run(VERIFY_DATASET_ROW_COUNTS_QUERY).single()
        assert r1["datasets"] == 1
        assert r1["rows"] == 100

        r2 = session.run(VERIFY_RELATIONSHIP_COUNT_QUERY).single()
        assert r2["relationships"] == 100

        dupes1 = session.run(VERIFY_DUPLICATES_QUERY).data()
        assert len(dupes1) == 0

    # Second pass: REPLAY all 100 messages again
    for rec in messages:
        success, err = consumer.process_record(rec)
        assert success is True, f"Failed replaying record offset {rec.offset}: {err}"

    # Verify counts after second pass (REPLAY adds 0 Datasets, 0 Rows, 0 Relationships)
    with writer.driver.session(database=config.neo4j_database) as session:
        r1_replay = session.run(VERIFY_DATASET_ROW_COUNTS_QUERY).single()
        assert r1_replay["datasets"] == 1, f"Expected 1 dataset, got {r1_replay['datasets']}"
        assert r1_replay["rows"] == 100, f"Expected 100 rows after replay, got {r1_replay['rows']}"

        r2_replay = session.run(VERIFY_RELATIONSHIP_COUNT_QUERY).single()
        assert r2_replay["relationships"] == 100, f"Expected 100 relationships, got {r2_replay['relationships']}"

        dupes_replay = session.run(VERIFY_DUPLICATES_QUERY).data()
        assert len(dupes_replay) == 0, f"Expected 0 duplicates, got {len(dupes_replay)}"

    # Clean up test dataset after test completion
    with writer.driver.session(database=config.neo4j_database) as session:
        session.run(CLEAN_TEST_DATASET_QUERY, {"dataset_id": dataset_id})

    writer.close()
