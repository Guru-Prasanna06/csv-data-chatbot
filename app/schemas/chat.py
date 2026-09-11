from typing import Any, Dict, List
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., description="Natural language question to ask about the graph data")


class ChatResponse(BaseModel):
    answer: str
    cypher: str
    result: List[Dict[str, Any]] = Field(default_factory=list)
    grounded: bool = True
