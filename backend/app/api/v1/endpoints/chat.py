import logging
from typing import Any, Dict

from fastapi import APIRouter

from app.core.config import get_settings
from app.schemas.chat import ChatRequest, ChatResponse
from app.services import chat_service
from app.services.neo4j_service import neo4j_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat", response_model=ChatResponse, summary="Query Knowledge Graph via Chatbot")
async def chat_endpoint(request: ChatRequest):
    """
    Converts a natural-language question into a read-only Cypher query, executes it
    against Neo4j, and returns an answer grounded strictly in the actual query result.
    """
    question = (request.question or "").strip()
    if not question:
        return ChatResponse(
            answer="Please provide a question.",
            cypher="",
            result=[],
            grounded=False,
        )

    neo4j_ok = await neo4j_service.check_health()
    if not neo4j_ok:
        return ChatResponse(
            answer="I can't answer right now because the graph database is unreachable.",
            cypher="",
            result=[],
            grounded=False,
        )

    schema = await chat_service.get_graph_schema()
    settings = get_settings()

    cypher = None
    params: Dict[str, Any] = {}

    if settings.OPENAI_API_KEY:
        try:
            llm_cypher = await chat_service.generate_cypher_llm(question, schema)
            chat_service.validate_read_only_cypher(llm_cypher)
            cypher = llm_cypher
        except Exception as exc:
            logger.warning("LLM Cypher generation unavailable or unsafe, using fallback: %s", exc)
            cypher = None

    if not cypher:
        cypher, params, _reason = chat_service.build_fallback_cypher(question, schema)

    if not cypher:
        return ChatResponse(
            answer=(
                "I couldn't map that question to the available data. "
                "Try asking about a specific column value or a row count."
            ),
            cypher="",
            result=[],
            grounded=False,
        )

    try:
        chat_service.validate_read_only_cypher(cypher)
    except chat_service.UnsafeCypherError as exc:
        logger.error("Blocked unsafe generated Cypher: %s", exc)
        return ChatResponse(
            answer="I generated a query that wasn't safe to run, so I blocked it.",
            cypher=cypher,
            result=[],
            grounded=False,
        )

    try:
        result = await neo4j_service.execute_query(cypher, params)
    except Exception as exc:
        logger.error("Cypher execution failed: %s", exc)
        return ChatResponse(
            answer="I couldn't execute the query against the graph database.",
            cypher=cypher,
            result=[],
            grounded=False,
        )

    grounded = chat_service.has_supporting_data(result)
    answer = chat_service.format_answer(result, grounded)

    return ChatResponse(answer=answer, cypher=cypher, result=result, grounded=grounded)
