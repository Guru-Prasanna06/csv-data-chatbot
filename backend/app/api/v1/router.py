from fastapi import APIRouter
from app.api.v1.endpoints import health, ingest, status, chat

api_router = APIRouter()

api_router.include_router(health.router, tags=["Health"])
api_router.include_router(ingest.router, tags=["Ingestion"])
api_router.include_router(status.router, tags=["Job Status"])
api_router.include_router(chat.router, tags=["Chatbot"])
