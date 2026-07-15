from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend_clone_handler import build_clone_video_handler


def _handler(tmp_path: Path, *, reserve, enqueue, refunds: list[dict], jobs: dict):
    async def refund(*_args, **kwargs):
        refunds.append(dict(kwargs))

    return build_clone_video_handler(
        xai_api_key="x",
        elevenlabs_api_key="e",
        get_current_user_from_request=lambda request: asyncio.sleep(
            0, result={"id": "user-1", "email": "u@example.com", "plan": "creator"}
        ),
        user_has_paid_access=lambda user: True,
        normalize_output_resolution=lambda value, priority_allowed=False: value,
        normalize_external_source_url=lambda value: value,
        temp_dir=tmp_path,
        jobs_ref=jobs,
        enqueue_generation_job=enqueue,
        queue_full_error=RuntimeError,
        run_clone_pipeline=lambda *_args, **_kwargs: None,
        persist_job_state=lambda *_args, **_kwargs: asyncio.sleep(0),
        resolve_user_plan_for_limits=lambda user: ("creator", {}),
        billing_active_for_user=lambda user: True,
        is_admin_user=lambda user: False,
        reserve_generation_credit=reserve,
        refund_generation_credit=refund,
        clone_credit_cost=20,
    )


def test_clone_refuses_before_queue_when_credits_are_insufficient(tmp_path: Path):
    queued: list[str] = []

    async def reserve(*_args, **_kwargs):
        return False, "topup_required", {"credits_total_remaining": 3}

    async def enqueue(*_args, **_kwargs):
        queued.append("queued")

    handler = _handler(tmp_path, reserve=reserve, enqueue=enqueue, refunds=[], jobs={})
    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler(topic="test", request=object()))
    assert exc.value.status_code == 402
    assert queued == []


def test_clone_records_metering_on_accepted_job(tmp_path: Path):
    async def reserve(*_args, **kwargs):
        assert kwargs["credits_needed"] == 20
        return True, "monthly", {"month_key": "2026-07", "credits_total_remaining": 55}

    async def enqueue(*_args, **_kwargs):
        return None

    jobs: dict = {}
    handler = _handler(tmp_path, reserve=reserve, enqueue=enqueue, refunds=[], jobs=jobs)
    response = asyncio.run(handler(topic="test", request=object()))
    job = jobs[response["job_id"]]
    assert job["user_id"] == "user-1"
    assert job["credit_charged"] is True
    assert job["credit_source"] == "monthly"
    assert job["credit_amount"] == job["credit_cost"] == 20
    assert job["credit_refunded"] is False


def test_clone_refunds_when_queue_rejects(tmp_path: Path):
    async def reserve(*_args, **_kwargs):
        return True, "topup", {"month_key": "2026-07", "credits_total_remaining": 55}

    async def enqueue(*_args, **_kwargs):
        raise RuntimeError("full")

    jobs: dict = {}
    refunds: list[dict] = []
    handler = _handler(tmp_path, reserve=reserve, enqueue=enqueue, refunds=refunds, jobs=jobs)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler(topic="test", request=object()))
    assert exc.value.status_code == 429
    assert refunds == [{"month_key": "2026-07", "credits": 20}]
    assert len(jobs) == 1
    job = next(iter(jobs.values()))
    assert job["credit_refunded"] is True
