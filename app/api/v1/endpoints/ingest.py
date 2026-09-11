import csv
import io
import logging
import re
import uuid
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks, HTTPException, status
from app.schemas.ingest import IngestResponse
from app.schemas.status import JobStatusEnum
from app.services.job_tracker import job_tracker
from app.services.kafka_producer import kafka_service

logger = logging.getLogger(__name__)

router = APIRouter()


def generate_dataset_id(filename: Optional[str], job_id: str) -> str:
    """
    Generates a deterministic, stable dataset_id derived from filename and job_id.
    e.g. 'customers_001.csv' -> 'customers_001_b3f1'
    """
    if not filename:
        return f"dataset_{job_id}"
    base_name = filename.rsplit(".", 1)[0]
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", base_name).strip("_").lower()
    if not sanitized:
        return f"dataset_{job_id}"
    return f"{sanitized}_{job_id}"


async def publish_to_kafka_background(job_id: str, messages: List[Dict[str, Any]]) -> None:
    """
    Background worker that publishes parsed CSV row messages to Kafka.
    """
    if not messages:
        return

    try:
        # Check Kafka reachability before publishing
        kafka_available = await kafka_service.check_health()
        if not kafka_available:
            logger.warning("Kafka is offline. Messages for job %s could not be published to cluster.", job_id)
            return

        sent = await kafka_service.publish_row_messages(messages)
        logger.info("Published %d/%d messages to Kafka for job %s", sent, len(messages), job_id)
    except Exception as exc:
        logger.error("Error publishing Kafka messages for job %s: %s", job_id, exc)


@router.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest Dynamic CSV File",
)
async def ingest_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Dynamic CSV file to ingest"),
    dataset_id: Optional[str] = Form(None, description="Optional custom dataset identifier"),
):
    """
    Receives multipart/form-data CSV upload.
    Validates CSV file, parses dynamic columns, constructs row messages,
    registers job in tracker, and triggers asynchronous Kafka message production.
    """
    # 1. Validate file presence
    if not file or not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file provided. Please upload a valid CSV file.",
        )

    # 2. Validate CSV file extension
    filename_lower = file.filename.lower()
    if not filename_lower.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only CSV files (.csv) are accepted.",
        )

    # 3. Read and decode content safely
    try:
        content_bytes = await file.read()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read uploaded file: {str(exc)}",
        )

    if not content_bytes or not content_bytes.strip():
        # Handle empty CSV (0 bytes) safely
        job_id = uuid.uuid4().hex[:8]
        job_tracker.create_job(job_id=job_id, rows_total=0, status=JobStatusEnum.QUEUED)
        return IngestResponse(
            job_id=job_id,
            rows_received=0,
            status="queued",
        )

    try:
        # Support utf-8-sig to automatically strip BOM if present
        content_str = content_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            content_str = content_bytes.decode("latin-1")
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unable to decode CSV file encoding: {str(exc)}",
            )

    # 4. Parse CSV dynamically
    reader = csv.reader(io.StringIO(content_str))
    
    try:
        header_row = next(reader, None)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid CSV structure: {str(exc)}",
        )

    job_id = uuid.uuid4().hex[:8]
    effective_dataset_id = dataset_id.strip() if dataset_id and dataset_id.strip() else generate_dataset_id(file.filename, job_id)


    # Handle header-only or blank CSV
    if not header_row or not any(h.strip() for h in header_row):
        job_tracker.create_job(job_id=job_id, rows_total=0, status=JobStatusEnum.QUEUED)
        return IngestResponse(
            job_id=job_id,
            rows_received=0,
            status="queued",
        )

    # Clean headers dynamically
    headers = [h.strip() for h in header_row]

    messages: List[Dict[str, Any]] = []
    row_index = 1

    for row in reader:
        # Skip empty rows
        if not row or not any(field.strip() for field in row if isinstance(field, str)):
            continue

        row_data: Dict[str, Any] = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            val = row[idx].strip() if idx < len(row) and isinstance(row[idx], str) else (row[idx] if idx < len(row) else "")
            row_data[header] = val

        if row_data:
            message = {
                "job_id": job_id,
                "dataset_id": effective_dataset_id,
                "row_index": row_index,
                "data": row_data,
            }
            messages.append(message)
            row_index += 1


    total_rows = len(messages)

    # 5. Store in Job Tracker
    job_tracker.create_job(job_id=job_id, rows_total=total_rows, status=JobStatusEnum.QUEUED)

    # 6. Queue Kafka background publishing
    if total_rows > 0:
        background_tasks.add_task(publish_to_kafka_background, job_id, messages)

    return IngestResponse(
        job_id=job_id,
        rows_received=total_rows,
        status="queued",
    )
