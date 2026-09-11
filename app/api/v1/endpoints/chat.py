from fastapi import APIRouter
from app.schemas.chat import ChatRequest, ChatResponse

router = APIRouter()


@router.post("/chat", response_model=ChatResponse, summary="Query Knowledge Graph via Chatbot")
async def chat_endpoint(request: ChatRequest):
    """
    Temporary chat endpoint adhering to the contract.
    Will be hooked to LLM Text-to-Cypher and Neo4j executor in next step.
    """
    return ChatResponse(
        answer=f"Processed query for question: '{request.question}'. Backend skeleton ready.",
        cypher="MATCH (r:Row) RETURN count(r)",
        result=[{"count(r)": 0}],
        grounded=True,
    )
