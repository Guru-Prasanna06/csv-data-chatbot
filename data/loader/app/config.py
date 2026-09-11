import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file if available
load_dotenv()

@dataclass(frozen=True)
class Config:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    neo4j_database: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    max_retries: int = 30
    retry_backoff_sec: float = 2.0

def load_config() -> Config:
    """
    Loads runtime configuration from environment variables with safe defaults.
    Ensures credentials are externalized and not hardcoded in source.
    """
    return Config(
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "csvgraphdb"),
        neo4j_database=os.getenv("NEO4J_DATABASE", "CSV_Graph_DB"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "csv-rows"),
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "csv-neo4j-loader"),
        max_retries=int(os.getenv("MAX_RETRIES", "30")),
        retry_backoff_sec=float(os.getenv("RETRY_BACKOFF_SEC", "2.0")),
    )
