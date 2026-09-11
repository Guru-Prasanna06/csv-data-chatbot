from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    kafka_connected: bool
    neo4j_connected: bool
