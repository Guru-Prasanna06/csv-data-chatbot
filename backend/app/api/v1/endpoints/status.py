import logging
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, status, Body
from app.schemas.status import StatusResponse, StatusUpdateRequest, JobStatusEnum
from app.services.job_tracker import job_tracker
from app.services.neo4j_service import neo4j_service

logger = logging.getLogger(__name__)

router = APIRouter()


async def _sync_from_neo4j(job_id: str, job: StatusResponse) -> StatusResponse:
    """
    Reconciles the tracked job against the real Row count in Neo4j for its dataset.
    The loader writes rows independently of this API, so this is the source of truth
    for "complete" rather than trusting Kafka publish alone.
    """
    if job.status in (JobStatusEnum.COMPLETE, JobStatusEnum.FAILED) or job.rows_total <= 0:
        return job

    dataset_id = job_tracker.get_dataset_id(job_id)
    if not dataset_id:
        return job

    try:
        records = await neo4j_service.execute_query(
            "MATCH (r:Row {dataset_id: $dataset_id}) RETURN count(r) AS loaded",
            {"dataset_id": dataset_id},
        )
        loaded = records[0]["loaded"] if records else 0
    except Exception as exc:
        logger.debug("Could not reconcile job %s against Neo4j: %s", job_id, exc)
        return job

    loaded = max(job.rows_loaded, min(loaded, job.rows_total))
    if loaded == job.rows_loaded:
        return job

    new_status = JobStatusEnum.COMPLETE if loaded >= job.rows_total else JobStatusEnum.LOADING
    updated = job_tracker.update_job(job_id=job_id, rows_loaded=loaded, status=new_status)
    return updated or job


@router.get("/status", response_model=StatusResponse, summary="Get Ingestion Job Status")
async def get_status(job_id: str = Query(..., description="Unique ingestion job identifier")):
    """
    Returns the real-time processing status and progress for an ingestion job.
    Reconciles against actual Neo4j Row counts so completion is genuine, not assumed
    from Kafka publish. Returns HTTP 404 if the job_id does not exist.
    """
    job = job_tracker.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )
    return await _sync_from_neo4j(job_id, job)


@router.post("/status/update", response_model=StatusResponse, summary="Update Job Progress (Loader Teammate Hook)")
async def update_status(
    job_id: str = Query(..., description="Job ID to update"),
    payload: Optional[StatusUpdateRequest] = Body(None, description="Progress update payload"),
    rows_loaded: Optional[int] = Query(None, description="Total rows loaded so far"),
    rows_failed: Optional[int] = Query(None, description="Total rows failed so far"),
    job_status: Optional[JobStatusEnum] = Query(None, alias="status", description="Explicit job status"),
):
    """
    Convenient update endpoint for the Neo4j/loader teammate to report ingestion progress.
    Can be called via JSON Body or Query Parameters.
    """
    # Prefer payload fields if provided, fallback to query params
    status_val = payload.status if (payload and payload.status) else job_status
    loaded_val = payload.rows_loaded if (payload and payload.rows_loaded is not None) else rows_loaded
    failed_val = payload.rows_failed if (payload and payload.rows_failed is not None) else rows_failed
    inc_loaded = payload.increment_loaded if payload else None
    inc_failed = payload.increment_failed if payload else None

    updated_job = job_tracker.update_job(
        job_id=job_id,
        status=status_val,
        rows_loaded=loaded_val,
        rows_failed=failed_val,
        increment_loaded=inc_loaded,
        increment_failed=inc_failed,
    )

    if not updated_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )

    return updated_job
