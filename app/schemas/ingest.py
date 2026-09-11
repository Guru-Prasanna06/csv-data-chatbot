from pydantic import BaseModel


class IngestResponse(BaseModel):
    job_id: str
    rows_received: int
    status: str = "queued"
