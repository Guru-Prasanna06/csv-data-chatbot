from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import get_settings
from app.api.v1.router import api_router
from app.services.kafka_producer import kafka_service
from app.services.neo4j_service import neo4j_service

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: logging and preparation
    yield
    # Shutdown: cleanly close open connections
    await kafka_service.close()
    await neo4j_service.close()


def create_application() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        description="Backend API for CSV -> Kafka -> Neo4j -> Chatbot pipeline",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS configuration for frontend integration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers (exposing /health, /status, /ingest, /chat at root level and /api/v1)
    app.include_router(api_router)
    app.include_router(api_router, prefix="/api/v1")

    return app


app = create_application()
