"""Job status payload builders for the Studio API."""

from __future__ import annotations

from typing import Callable

from fastapi import HTTPException


def job_access_allowed(job: dict, user: dict | None, admin_emails: set[str]) -> bool:
    """Return whether an authenticated caller may see a legacy job record.

    Historical records without an owner remain available to configured Studio
    administrators only.  Treating those records as public would recreate the
    original cross-account data leak.
    """
    if not isinstance(user, dict):
        return False
    email = str(user.get("email", "") or "").strip().lower()
    normalized_admins = {str(value or "").strip().lower() for value in admin_emails}
    if email and email in normalized_admins:
        return True
    user_id = str(user.get("id", "") or "").strip()
    owner_id = str(job.get("user_id", "") or job.get("owner_user_id", "") or "").strip()
    return bool(user_id and owner_id and user_id == owner_id)


def build_job_status_payload(
    *,
    jobs_ref: dict,
    prune_in_memory_jobs: Callable[[], None],
    get_persisted_job_state: Callable,
    record_kpi_for_job: Callable[[str, dict], None],
    persist_job_state: Callable,
    admin_emails: set[str],
):
    async def job_status_payload(job_id: str, *, user: dict):
        prune_in_memory_jobs()
        persisted = await get_persisted_job_state(job_id)
        if isinstance(persisted, dict):
            if not job_access_allowed(persisted, user, admin_emails):
                raise HTTPException(404, "Job not found")
            record_kpi_for_job(job_id, persisted)
            if persisted.get("kpi_recorded"):
                await persist_job_state(job_id, persisted)
            return persisted
        if job_id not in jobs_ref:
            raise HTTPException(404, "Job not found")
        job = jobs_ref[job_id]
        if not job_access_allowed(job, user, admin_emails):
            raise HTTPException(404, "Job not found")
        record_kpi_for_job(job_id, job)
        return job

    return job_status_payload


def build_list_jobs_payload(*, jobs_ref: dict, admin_emails: set[str]):
    async def list_jobs_payload(*, user: dict):
        return {
            job_id: {key: value for key, value in job.items() if key != "output_file"}
            for job_id, job in jobs_ref.items()
            if job_access_allowed(job, user, admin_emails)
        }

    return list_jobs_payload
