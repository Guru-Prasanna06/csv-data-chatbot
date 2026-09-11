import logging
import time
from typing import Dict, Any, Optional
from neo4j import GraphDatabase, Driver
from app.config import Config
from app.queries import MERGE_DATASET_AND_ROW_QUERY, DB_VERIFY_QUERY

logger = logging.getLogger(__name__)

class Neo4jWriter:
    """
    Manages Neo4j database connections, startup availability verification,
    and idempotent MERGE query writes for CSV dataset rows.
    """
    def __init__(self, config: Config, driver: Optional[Driver] = None):
        self.config = config
        self.driver = driver

    def connect_and_verify(self) -> None:
        """
        Connects to Neo4j Bolt, verifies reachability, verifies configured database
        availability, and executes a test Cypher query with retry/backoff.
        """
        retries = 0
        backoff = self.config.retry_backoff_sec
        last_error = None

        while retries < self.config.max_retries:
            try:
                if self.driver is None:
                    self.driver = GraphDatabase.driver(
                        self.config.neo4j_uri,
                        auth=(self.config.neo4j_user, self.config.neo4j_password)
                    )
                
                # 1. Connectivity check
                self.driver.verify_connectivity()

                # 2. Database availability check & 3. Cypher query execution
                with self.driver.session(database=self.config.neo4j_database) as session:
                    result = session.run(DB_VERIFY_QUERY)
                    record = result.single()
                    if record and record["result"] == 1:
                        logger.info(
                            f"Successfully connected to Neo4j and verified database '{self.config.neo4j_database}'"
                        )
                        return
            except Exception as e:
                last_error = e
                retries += 1
                logger.warning(
                    f"Neo4j connection attempt {retries}/{self.config.max_retries} failed: {e}. Retrying in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)

        raise RuntimeError(
            f"Failed to connect to Neo4j and verify database '{self.config.neo4j_database}' after {self.config.max_retries} attempts. Last error: {last_error}"
        )

    def write_row(self, payload: Dict[str, Any]) -> None:
        """
        Executes idempotent MERGE for Dataset, Row, and HAS_ROW relationship.
        """
        if not self.driver:
            raise RuntimeError("Neo4j driver is not connected. Call connect_and_verify() first.")

        params = {
            "dataset_id": payload["dataset_id"],
            "row_index": payload["row_index"],
            "row_data": payload["row_data"],
            "filename": payload.get("filename"),
            "uploaded_at": payload.get("uploaded_at")
        }

        with self.driver.session(database=self.config.neo4j_database) as session:
            session.execute_write(lambda tx: tx.run(MERGE_DATASET_AND_ROW_QUERY, params))

    def close(self) -> None:
        if self.driver:
            self.driver.close()
            self.driver = None
