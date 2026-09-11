from typing import Any, Dict
from pydantic import BaseModel, Field


class KafkaRowMessage(BaseModel):
    job_id: str
    dataset_id: str
    row_index: int
    data: Dict[str, Any] = Field(..., description="Dynamic key-value pairs representing a single row")
