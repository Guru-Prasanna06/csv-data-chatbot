import asyncio
import json
import logging
from typing import Optional, Dict, Any, List
from aiokafka import AIOKafkaProducer
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class KafkaService:
    def __init__(self):
        self.settings = get_settings()
        self._producer: Optional[AIOKafkaProducer] = None

    async def get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                client_id=self.settings.KAFKA_CLIENT_ID,
                request_timeout_ms=5000,
            )
            await self._producer.start()
        return self._producer

    async def check_health(self) -> bool:
        """
        Check if Kafka cluster is genuinely reachable.
        Does NOT fake connectivity.
        Uses timeout protection so health checks never hang.
        """
        try:
            # 1. Quick TCP socket probe to bootstrap servers
            servers = [s.strip() for s in self.settings.KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]
            if not servers:
                return False

            first_server = servers[0]
            if ":" in first_server:
                host, port_str = first_server.split(":", 1)
                port = int(port_str)
            else:
                host, port = first_server, 9092

            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=1.0,
                )
                writer.close()
                await writer.wait_closed()
            except Exception:
                return False

            # 2. Protocol verification with short-lived producer
            producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                client_id=f"{self.settings.KAFKA_CLIENT_ID}-hc",
                request_timeout_ms=1500,
            )
            try:
                await asyncio.wait_for(producer.start(), timeout=2.0)
                return True
            finally:
                try:
                    await producer.stop()
                except Exception:
                    pass
        except Exception as exc:
            logger.debug(f"Kafka health check failed: {exc}")
            return False

    async def send_row_message(self, topic: str, payload: Dict[str, Any], key: Optional[str] = None) -> None:
        producer = await self.get_producer()
        key_bytes = key.encode("utf-8") if key else None
        await producer.send_and_wait(topic, value=payload, key=key_bytes)

    async def publish_row_messages(self, messages: List[Dict[str, Any]], topic: Optional[str] = None) -> int:
        """
        Publishes a batch of row messages sequentially to Kafka.
        Returns the number of messages successfully sent.
        """
        if not messages:
            return 0
        target_topic = topic or self.settings.KAFKA_TOPIC
        producer = await self.get_producer()
        sent_count = 0
        for msg in messages:
            key = f"{msg.get('dataset_id')}_{msg.get('row_index')}"
            await producer.send_and_wait(target_topic, value=msg, key=key.encode("utf-8"))
            sent_count += 1
        return sent_count

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None


kafka_service = KafkaService()


