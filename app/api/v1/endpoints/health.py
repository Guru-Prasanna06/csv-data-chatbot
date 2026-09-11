import asyncio
from fastapi import APIRouter
from app.schemas.health import HealthResponse
from app.services.kafka_producer import kafka_service
from app.services.neo4j_service import neo4j_service

router = APIRouter()


@router.get("/health", response_model=HealthResponse, summary="Service Health Check")
async def health_check():
    """
    Genuine health check for Kafka and Neo4j.
    - Tests actual Kafka connectivity.
    - Tests actual Neo4j connectivity.
    - If Kafka is unavailable, kafka_connected is false.
    - If Neo4j is unavailable, neo4j_connected is false.
    - Overall status is ONLY 'ok' when both services are genuinely reachable.
    """
    kafka_ok, neo4j_ok = await asyncio.gather(
        kafka_service.check_health(),
        neo4j_service.check_health(),
        return_exceptions=False,
    )

    status = "ok" if (kafka_ok and neo4j_ok) else "degraded"

    return HealthResponse(
        status=status,
        kafka_connected=kafka_ok,
        neo4j_connected=neo4j_ok,
    )

