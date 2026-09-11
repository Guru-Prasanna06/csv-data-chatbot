import logging
import os
import signal
import sys
import time
from app.config import load_config
from app.kafka_consumer import KafkaLoaderConsumer, TemporaryInfrastructureError
from app.neo4j_writer import Neo4jWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

RUNNING = True

def signal_handler(signum, frame):
    global RUNNING
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    RUNNING = False

def check_non_root() -> None:
    """
    Verifies that application process is NOT running as root (UID 0).
    Fails explicitly if UID == 0.
    """
    uid = os.getuid() if hasattr(os, "getuid") else None
    euid = os.geteuid() if hasattr(os, "geteuid") else None
    
    if uid == 0 or euid == 0:
        logger.critical(
            f"SECURITY ERROR: Application MUST run as non-root user! Current UID: {uid}, EUID: {euid}"
        )
        sys.exit(1)
    
    logger.info(f"Verified non-root process execution. UID: {uid}, EUID: {euid}")

def main() -> None:
    global RUNNING

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting CSV -> Kafka -> Neo4j Loader service...")

    # 1. Non-root verification
    check_non_root()

    # 2. Load configuration
    config = load_config()
    logger.info(f"Configuration loaded. Target database: '{config.neo4j_database}', Kafka topic: '{config.kafka_topic}'")

    # 3. Wait for Kafka readiness & topic csv-rows
    neo4j_writer = Neo4jWriter(config)
    kafka_consumer = KafkaLoaderConsumer(config, neo4j_writer)

    logger.info("Checking Kafka readiness...")
    kafka_consumer.wait_for_kafka_and_topic()

    # 4. Wait for Neo4j Bolt & CSV_Graph_DB readiness
    logger.info("Checking Neo4j readiness...")
    neo4j_writer.connect_and_verify()

    # 5. Create Kafka consumer
    consumer = kafka_consumer.create_consumer()

    logger.info("Loader service initialization complete. Starting message consumption loop...")

    while RUNNING:
        try:
            records_dict = consumer.poll(timeout_ms=1000)
            for tp, records in records_dict.items():
                for record in records:
                    if not RUNNING:
                        break
                    
                    # Process record with retry on temporary infra failure
                    max_attempts = 5
                    attempt = 0
                    while attempt < max_attempts and RUNNING:
                        try:
                            kafka_consumer.process_record(record)
                            break
                        except TemporaryInfrastructureError as tie:
                            attempt += 1
                            logger.warning(
                                f"Temporary error processing record at offset {record.offset} (attempt {attempt}/{max_attempts}): {tie}"
                            )
                            time.sleep(2.0 * attempt)
                            # Re-verify Neo4j connection if needed
                            try:
                                neo4j_writer.connect_and_verify()
                            except Exception:
                                pass
        except Exception as e:
            if RUNNING:
                logger.error(f"Unexpected error in consumer main loop: {e}")
                time.sleep(1.0)

    logger.info("Closing Neo4j writer and Kafka consumer...")
    kafka_consumer.close()
    neo4j_writer.close()
    logger.info("Loader service stopped cleanly.")

if __name__ == "__main__":
    main()
