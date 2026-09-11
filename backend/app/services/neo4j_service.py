import asyncio
import logging
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from neo4j import AsyncGraphDatabase, AsyncDriver
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class Neo4jService:
    def __init__(self):
        self.settings = get_settings()
        self._driver: Optional[AsyncDriver] = None

    def get_driver(self) -> AsyncDriver:
        if self._driver is None:
            self._driver = AsyncGraphDatabase.driver(
                self.settings.NEO4J_URI,
                auth=(self.settings.NEO4J_USER, self.settings.NEO4J_PASSWORD),
                connection_timeout=2.0,
            )
        return self._driver

    async def check_health(self) -> bool:
        """
        Check if Neo4j instance is genuinely reachable.
        Does NOT fake connectivity.
        Uses timeout protection so health checks never hang.
        """
        try:
            # Quick probe
            parsed = urlparse(self.settings.NEO4J_URI)
            host = parsed.hostname or "localhost"
            port = parsed.port or 7687
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=1.0,
                )
                writer.close()
                await writer.wait_closed()
            except Exception:
                return False

            # Driver verification
            driver = self.get_driver()
            await asyncio.wait_for(driver.verify_connectivity(), timeout=2.0)
            return True
        except Exception as exc:
            logger.debug(f"Neo4j health check failed: {exc}")
            return False

    async def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        driver = self.get_driver()
        async with driver.session(database=self.settings.NEO4J_DATABASE) as session:
            result = await session.run(query, parameters or {})
            records = await result.data()
            return records

    async def close(self) -> None:
        if self._driver:
            await self._driver.close()
            self._driver = None


neo4j_service = Neo4jService()

