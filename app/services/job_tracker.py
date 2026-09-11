import threading
from typing import Dict, Optional
from app.schemas.status import StatusResponse, JobStatusEnum


class JobTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs: Dict[str, StatusResponse] = {}

    def create_job(self, job_id: str, rows_total: int, status: JobStatusEnum = JobStatusEnum.QUEUED) -> StatusResponse:
        with self._lock:
            # If 0 rows uploaded (e.g. empty or header-only CSV), mark complete immediately or queued
            job = StatusResponse(
                job_id=job_id,
                status=status,
                rows_total=rows_total,
                rows_loaded=0,
                rows_failed=0,
            )
            self._jobs[job_id] = job
            return job

    def get_job(self, job_id: str) -> Optional[StatusResponse]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                # Return a copy to avoid external mutation outside lock
                return job.model_copy()
            return None

    def update_job(
        self,
        job_id: str,
        status: Optional[JobStatusEnum] = None,
        rows_loaded: Optional[int] = None,
        rows_failed: Optional[int] = None,
        rows_total: Optional[int] = None,
        increment_loaded: Optional[int] = None,
        increment_failed: Optional[int] = None,
    ) -> Optional[StatusResponse]:
        with self._lock:
            if job_id not in self._jobs:
                return None
            job = self._jobs[job_id]

            if rows_total is not None:
                job.rows_total = rows_total

            if rows_loaded is not None:
                job.rows_loaded = rows_loaded
            elif increment_loaded is not None:
                job.rows_loaded += increment_loaded

            if rows_failed is not None:
                job.rows_failed = rows_failed
            elif increment_failed is not None:
                job.rows_failed += increment_failed

            if status is not None:
                job.status = status
            else:
                # Auto status progression helper:
                # If loading has begun but not marked complete/failed
                if job.status == JobStatusEnum.QUEUED and (job.rows_loaded > 0 or job.rows_failed > 0):
                    job.status = JobStatusEnum.LOADING
                
                # If all rows have been processed (and total > 0)
                if job.rows_total > 0 and (job.rows_loaded + job.rows_failed >= job.rows_total):
                    job.status = JobStatusEnum.COMPLETE

            return job.model_copy()


job_tracker = JobTracker()
