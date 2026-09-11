from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class JobStatusEnum(str, Enum):
    QUEUED = "queued"
    LOADING = "loading"
    COMPLETE = "complete"
    FAILED = "failed"


class StatusResponse(BaseModel):
    job_id: str
    status: JobStatusEnum
    rows_total: int
    rows_loaded: int
    rows_failed: int


class StatusUpdateRequest(BaseModel):
    status: Optional[JobStatusEnum] = Field(None, description="New job status ('queued', 'loading', 'complete', 'failed')")
    rows_loaded: Optional[int] = Field(None, ge=0, description="Total rows loaded so far")
    rows_failed: Optional[int] = Field(None, ge=0, description="Total rows failed so far")
    increment_loaded: Optional[int] = Field(None, ge=0, description="Increment loaded count by N")
    increment_failed: Optional[int] = Field(None, ge=0, description="Increment failed count by N")
