from __future__ import annotations

import json
from pathlib import Path

import pytest

from long_form import pipeline, v5_pipeline
from skeleton_ai import i2v_engine
from studio_agent import production_budget, production_costs


def _persist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    job_id: str,
    cap: float,
) -> Path:
    monkeypatch.setattr(pipeline, "LF_OUTPUT_ROOT", tmp_path)
    workspace = tmp_path / job_id
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "state.json").write_text(
        json.dumps({"job_id": job_id, "phase": "queued"}),
        encoding="utf-8",
    )
    payload = pipeline.persist_longform_credit_reservation(
        job_id,
        reservation={"reservation_id": f"hold-{job_id}"},
        budget={
            "tool": "start_longform_render",
            "estimated_usd": cap,
            "max_budget_usd": cap,
        },
        user_id="user-1",
        tool="start_longform_render",
        session_id="session-1",
    )
    assert payload["budget"]["max_budget_usd"] == cap
    assert (workspace / "credit_reservation.json").is_file()
    return workspace


def _ledger(workspace: Path) -> list[dict]:
    path = workspace / "cost_ledger.jsonl"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_incremental_attempt_receipt_is_idempotent(tmp_path, monkeypatch):
    workspace = _persist(tmp_path, monkeypatch, job_id="lf_idempotent", cap=0.08)

    first = production_budget.record_incremental_spend_attempt(
        workspace,
        0.04,
        attempt_id="attempt-1",
        operation="longform_image_provider_attempt",
        provider="fal",
        model="fal-ai/seedream",
        require_context=True,
    )
    replay = production_budget.record_incremental_spend_attempt(
        workspace,
        0.04,
        attempt_id="attempt-1",
        operation="longform_image_provider_attempt",
        provider="fal",
        model="fal-ai/seedream",
        require_context=True,
    )

    assert first["recorded"] is True
    assert replay["recorded"] is False
    assert replay["idempotent_replay"] is True
    assert len(_ledger(workspace)) == 1


def test_fal_http_retries_each_consume_once_and_ceiling_blocks_next_call(
    tmp_path,
    monkeypatch,
):
    workspace = _persist(tmp_path, monkeypatch, job_id="lf_http_retry", cap=0.08)
    responses = [
        (500, {"error": "retry"}),
        (200, {"images": [{"url": "https://example.invalid/image.png"}]}),
    ]
    posted: list[str] = []

    class Response:
        def __init__(self, status_code: int, payload: dict):
            self.status_code = status_code
            self._payload = payload
            self.text = json.dumps(payload)

        def json(self):
            return self._payload

    class Client:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def post(self, url, **_kwargs):
            posted.append(url)
            status, payload = responses.pop(0)
            return Response(status, payload)

    monkeypatch.setattr(pipeline.httpx, "Client", Client)
    monkeypatch.setattr(pipeline, "_next_fal_key", lambda: "fal-key")
    monkeypatch.setattr(pipeline.time, "sleep", lambda _seconds: None)

    result = pipeline._fal_post(
        "https://fal.run/fal-ai/example",
        {"prompt": "bounded"},
        attempts=2,
        budget_workspace=workspace,
        estimated_attempt_usd=0.04,
        budget_operation="longform_image_provider_attempt",
    )
    assert result["images"]
    assert len(posted) == 2
    assert len(_ledger(workspace)) == 2

    with pytest.raises(production_budget.BudgetExceededError, match="budget_exceeded_mid_job"):
        pipeline._fal_post(
            "https://fal.run/fal-ai/example",
            {"prompt": "bounded"},
            attempts=1,
            budget_workspace=workspace,
            estimated_attempt_usd=0.04,
            budget_operation="longform_image_provider_attempt",
        )
    assert len(posted) == 2
    assert len(_ledger(workspace)) == 2


def _mock_i2v_transport(monkeypatch, *, fail_first: bool):
    calls: list[str] = []

    monkeypatch.setattr(i2v_engine, "_ensure_fal", lambda: None)
    monkeypatch.setattr(i2v_engine.fal_client, "upload_file", lambda _path: "https://example.invalid/still.png")

    def queue(endpoint, _args, *, timeout_sec):
        del timeout_sec
        calls.append(endpoint)
        if fail_first and len(calls) == 1:
            raise i2v_engine.I2VError("400 invalid argument")
        return {
            "video": {"url": "https://example.invalid/video.mp4"},
            "_fal_request_id": f"request-{len(calls)}",
        }

    monkeypatch.setattr(i2v_engine, "_queue_result", queue)
    monkeypatch.setattr(
        i2v_engine,
        "_download",
        lambda _url, target: Path(target).write_bytes(b"mock-video"),
    )
    monkeypatch.setattr(i2v_engine, "_verify_silent_output", lambda _path: {"pass": True})
    from skeleton_ai import compose

    monkeypatch.setattr(compose, "strip_clip_audio", lambda _path: None)
    return calls


def test_i2v_failed_endpoint_and_fallback_are_both_recorded(tmp_path, monkeypatch):
    first_cost = float(
        production_costs.price_fal_video(
            i2v_engine.SEEDANCE_ENDPOINT,
            seconds=5,
        )[0]
    )
    second_cost = float(
        production_costs.price_fal_video(
            i2v_engine.PIXVERSE_V6_ENDPOINT,
            seconds=5,
        )[0]
    )
    workspace = _persist(
        tmp_path,
        monkeypatch,
        job_id="lf_i2v_fallback",
        cap=first_cost + second_cost,
    )
    calls = _mock_i2v_transport(monkeypatch, fail_first=True)
    still = workspace / "stills" / "scene_0000.png"
    still.parent.mkdir(parents=True, exist_ok=True)
    still.write_bytes(b"still")
    output = workspace / "clips" / "clip_0000.mp4"

    result = v5_pipeline._gen_em_clip(
        still,
        "slow push",
        output,
        duration_sec=5,
        video_model="seedance",
    )

    assert result == output
    assert calls == [i2v_engine.SEEDANCE_ENDPOINT, i2v_engine.PIXVERSE_V6_ENDPOINT]
    events = _ledger(workspace)
    assert [event["endpoint"] for event in events] == calls
    assert len({event["metadata"]["budget_attempt_id"] for event in events}) == 2


def test_i2v_budget_blocks_fallback_before_second_provider_submission(
    tmp_path,
    monkeypatch,
):
    first_cost = float(
        production_costs.price_fal_video(
            i2v_engine.SEEDANCE_ENDPOINT,
            seconds=5,
        )[0]
    )
    workspace = _persist(
        tmp_path,
        monkeypatch,
        job_id="lf_i2v_ceiling",
        cap=first_cost,
    )
    calls = _mock_i2v_transport(monkeypatch, fail_first=True)
    still = workspace / "stills" / "scene_0000.png"
    still.parent.mkdir(parents=True, exist_ok=True)
    still.write_bytes(b"still")
    output = workspace / "clips" / "clip_0000.mp4"

    with pytest.raises(production_budget.BudgetExceededError, match="budget_exceeded_mid_job"):
        v5_pipeline._gen_em_clip(
            still,
            "slow push",
            output,
            duration_sec=5,
            video_model="seedance",
        )

    assert calls == [i2v_engine.SEEDANCE_ENDPOINT]
    assert len(_ledger(workspace)) == 1


def test_regenerate_thumbnail_budget_is_one_fal_image():
    estimate = production_budget.estimate_tool_cost(
        "regenerate_longform_thumbnail",
        {"image_model_id": "seedream_edit", "max_budget_usd": 0.25},
    )

    assert estimate.tool == "regenerate_longform_thumbnail"
    assert estimate.breakdown["thumbnails"] == 1
    assert estimate.estimated_usd > 0

