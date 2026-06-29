"""Job status payload builders for the Studio API."""

from __future__ import annotations

from typing import Callable

from fastapi import HTTPException


def build_job_status_payload(
    *,
    jobs_ref: dict,
    prune_in_memory_jobs: Callable[[], None],
    get_persisted_job_state: Callable,
    record_kpi_for_job: Callable[[str, dict], None],
    persist_job_state: Callable,
):
    async def job_status_payload(job_id: str):
        prune_in_memory_jobs()
        persisted = await get_persisted_job_state(job_id)
        if isinstance(persisted, dict):
            record_kpi_for_job(job_id, persisted)
            if persisted.get("kpi_recorded"):
                await persist_job_state(job_id, persisted)
            return persisted
        if job_id not in jobs_ref:
            raise HTTPException(404, "Job not found")
        record_kpi_for_job(job_id, jobs_ref[job_id])
        return jobs_ref[job_id]

    return job_status_payload


def build_list_jobs_payload(*, jobs_ref: dict):
    async def list_jobs_payload():
        return {job_id: {key: value for key, value in job.items() if key != "output_file"} for job_id, job in jobs_ref.items()}

    return list_jobs_payload
