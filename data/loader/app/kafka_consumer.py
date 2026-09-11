import json
import logging
import time
from typing import Dict, Any, Tuple, Optional
from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable
from kafka.structs import OffsetAndMetadata
from app.config import Config
from app.neo4j_writer import Neo4jWriter

logger = logging.getLogger(__name__)

class InvalidPayloadError(Exception):
    """Exception raised for permanent invalid payloads."""
    pass

class TemporaryInfrastructureError(Exception):
    """Exception raised for temporary infrastructure/database errors."""
    pass

def validate_payload(raw_data: Any) -> Dict[str, Any]:
    """
    Validates message payload against strict schema and data type rules.
    Raises InvalidPayloadError for permanent invalid inputs.
    """
    if not isinstance(raw_data, dict):
        raise InvalidPayloadError(f"Payload must be a JSON object/map, got {type(raw_data).__name__}")

    # dataset_id validation
    if "dataset_id" not in raw_data:
        raise InvalidPayloadError("Missing required field 'dataset_id'")
    dataset_id = raw_data["dataset_id"]
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise InvalidPayloadError(f"Field 'dataset_id' must be a non-empty string, got {dataset_id!r}")

    # row_index validation
    if "row_index" not in raw_data:
        raise InvalidPayloadError("Missing required field 'row_index'")
    row_index = raw_data["row_index"]
    if isinstance(row_index, bool) or not isinstance(row_index, int):
        raise InvalidPayloadError(
            f"Field 'row_index' must be an integer (and not boolean), got {row_index!r} ({type(row_index).__name__})"
        )

    # row_data validation
    if "row_data" not in raw_data:
        raise InvalidPayloadError("Missing required field 'row_data'")
    row_data = raw_data["row_data"]
    if not isinstance(row_data, dict):
        raise InvalidPayloadError(f"Field 'row_data' must be a JSON object/map, got {type(row_data).__name__}")
    if len(row_data) == 0:
        raise InvalidPayloadError("Field 'row_data' cannot be empty object")

    return {
        "dataset_id": dataset_id,
        "row_index": row_index,
        "row_data": row_data,
        "filename": raw_data.get("filename"),
        "uploaded_at": raw_data.get("uploaded_at"),
    }

class KafkaLoaderConsumer:
    """
    Kafka Consumer for csv-rows topic with manual offset commit flow,
    retry/backoff, and permanent vs temporary failure handling.
    """
    def __init__(self, config: Config, neo4j_writer: Neo4jWriter):
        self.config = config
        self.neo4j_writer = neo4j_writer
        self.consumer: Optional[KafkaConsumer] = None

    def wait_for_kafka_and_topic(self) -> None:
        """
        Waits for Kafka broker to become available and ensures topic exists.
        """
        retries = 0
        backoff = self.config.retry_backoff_sec

        while retries < self.config.max_retries:
            try:
                admin_client = KafkaAdminClient(
                    bootstrap_servers=self.config.kafka_bootstrap_servers,
                    client_id="csv-loader-admin"
                )
                existing_topics = admin_client.list_topics()
                if self.config.kafka_topic not in existing_topics:
                    logger.info(f"Topic '{self.config.kafka_topic}' not found. Attempting creation...")
                    topic_list = [NewTopic(name=self.config.kafka_topic, num_partitions=1, replication_factor=1)]
                    try:
                        admin_client.create_topics(new_topics=topic_list, validate_only=False)
                        logger.info(f"Topic '{self.config.kafka_topic}' created successfully.")
                    except TopicAlreadyExistsError:
                        pass
                admin_client.close()
                logger.info(f"Kafka broker and topic '{self.config.kafka_topic}' ready.")
                return
            except Exception as e:
                retries += 1
                logger.warning(
                    f"Kafka startup check attempt {retries}/{self.config.max_retries} failed: {e}. Retrying in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)

        raise RuntimeError(f"Kafka or topic '{self.config.kafka_topic}' unavailable after {self.config.max_retries} retries.")

    def create_consumer(self) -> KafkaConsumer:
        """
        Creates KafkaConsumer with enable_auto_commit=False for manual offset commits.
        """
        retries = 0
        backoff = self.config.retry_backoff_sec

        while retries < self.config.max_retries:
            try:
                consumer = KafkaConsumer(
                    self.config.kafka_topic,
                    bootstrap_servers=self.config.kafka_bootstrap_servers,
                    group_id=self.config.kafka_group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    value_deserializer=lambda m: m,  # deserialize in message loop
                    consumer_timeout_ms=1000
                )
                self.consumer = consumer
                logger.info(f"Kafka consumer subscribed to topic '{self.config.kafka_topic}' in group '{self.config.kafka_group_id}'.")
                return consumer
            except Exception as e:
                retries += 1
                logger.warning(
                    f"Kafka consumer creation attempt {retries}/{self.config.max_retries} failed: {e}. Retrying in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)

        raise RuntimeError(f"Could not create Kafka consumer after {self.config.max_retries} attempts.")

    def commit_record_offset(self, record) -> None:
        """
        Manually commits offset for record after successful processing or permanent error discarding.
        """
        if not self.consumer:
            return
        tp = TopicPartition(record.topic, record.partition)
        om = OffsetAndMetadata(record.offset + 1, None)
        self.consumer.commit({tp: om})

    def process_record(self, record) -> Tuple[bool, Optional[str]]:
        """
        Processes a single Kafka record.
        Flow:
        1. Decode JSON
        2. Validate payload schema
        3. If invalid JSON/payload: log error, commit offset (so partition isn't blocked), return (False, err)
        4. If valid: write to Neo4j using MERGE
        5. If Neo4j write succeeds: commit offset, return (True, None)
        6. If Neo4j write fails (temp infra error): do NOT commit offset, raise TemporaryInfrastructureError
        """
        # 1. Decode JSON
        try:
            raw_bytes = record.value
            if isinstance(raw_bytes, (bytes, bytearray)):
                text = raw_bytes.decode('utf-8')
            elif isinstance(raw_bytes, str):
                text = raw_bytes
            else:
                text = str(raw_bytes)
            raw_json = json.loads(text)
        except Exception as e:
            msg = f"Malformed JSON in Kafka message at offset {record.offset}: {e}"
            logger.error(msg)
            # Commit offset to unblock partition for permanent bad message
            self.commit_record_offset(record)
            return False, msg

        # 2. Validate payload
        try:
            validated_payload = validate_payload(raw_json)
        except InvalidPayloadError as e:
            msg = f"Invalid payload in Kafka message at offset {record.offset}: {e}"
            logger.error(msg)
            # Commit offset to unblock partition for permanent bad message
            self.commit_record_offset(record)
            return False, msg

        # 3. Write to Neo4j & commit offset on success
        try:
            self.neo4j_writer.write_row(validated_payload)
            self.commit_record_offset(record)
            logger.debug(
                f"Successfully loaded dataset_id='{validated_payload['dataset_id']}' row_index={validated_payload['row_index']} into Neo4j"
            )
            return True, None
        except Exception as e:
            msg = f"Temporary infrastructure failure writing to Neo4j at offset {record.offset}: {e}"
            logger.error(msg)
            # DO NOT commit offset for temporary infrastructure failures!
            raise TemporaryInfrastructureError(msg) from e

    def close(self) -> None:
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None
