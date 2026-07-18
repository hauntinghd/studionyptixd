from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import long_form_router
from long_form import pipeline
from long_form import v5_pipeline
from long_form.text_client import StudioTextClient
from studio_agent import access
from studio_agent import jobs as agent_jobs
from studio_agent import openrouter


def _client(
    user: dict,
    *,
    is_admin: bool = False,
) -> TestClient:
    def require_auth() -> dict:
        return dict(user)

    app = FastAPI()
    app.include_router(
        long_form_router.build_long_form_router(
            require_auth=require_auth,
            is_admin_check=lambda _user: is_admin,
        )
    )
    return TestClient(app)


def test_longform_entitlement_accepts_paid_plan_and_admin_but_denies_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paid_plan = sorted(access.STUDIO_AGENT_PLANS)[0]

    monkeypatch.setattr(access, "unified_plan", lambda user_id: paid_plan if user_id == "paid" else "")

    paid = _client({"id": "paid"}).get("/api/long-form/channel/history_rewind")
    assert paid.status_code == 200, paid.text

    free = _client({"id": "free"}).get("/api/long-form/channel/history_rewind")
    assert free.status_code == 403, free.text
    assert free.json()["detail"] == "active_studio_plan_required"

    admin = _client({"id": "owner"}, is_admin=True).get(
        "/api/long-form/channel/history_rewind"
    )
    assert admin.status_code == 200, admin.text


@pytest.mark.parametrize(
    "path",
    [
        "/api/long-form/jobs/lf_private/state",
        "/api/long-form/jobs/lf_private/status",
        "/api/long-form/jobs/lf_private/scenes",
    ],
)
def test_cross_user_cannot_read_owned_longform_job(
    path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        agent_jobs,
        "job_access_metadata",
        lambda job_id, kind="": {
            "exists": True,
            "job_id": job_id,
            "kind": "longform",
            "owner_id": "creator-owner",
        },
    )

    # Even an admin-shaped request must not disclose a creator's explicitly
    # owned job when the authenticated user id does not match.
    response = _client({"id": "different-creator"}, is_admin=True).get(path)
    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "job_not_found"


@pytest.mark.parametrize(
    "channel, message",
    [
        (
            {"key": "shorts_only", "format": "shorts", "pipeline_kind": "sleep_doc"},
            "not a long-form",
        ),
        (
            {
                "key": "unregistered",
                "format": "long_form",
                "pipeline_kind": "not_registered_for_test",
            },
            "not registered",
        ),
    ],
)
def test_channel_preflight_fails_before_workspace_or_background_render(
    channel: dict,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("preflight failure reached workspace/provider execution")

    monkeypatch.setattr(pipeline, "_ensure_job_dir", forbidden)
    monkeypatch.setattr(pipeline, "_spawn_lf_background_coro", forbidden)

    with pytest.raises(pipeline.LFRenderError, match=message):
        pipeline.start_render(
            channel,
            {"title": "No spend", "chapters": [{"title": "Chapter 1"}]},
        )


def _seed_incomplete_job(
    root: Path,
    *,
    job_id: str,
    pipeline_kind: str,
    proof_pending: bool = False,
) -> None:
    job_dir = root / job_id
    (job_dir / "stills").mkdir(parents=True)
    # The manifest intentionally ignores thumbnail-sized/corrupt files and
    # only counts real still candidates larger than 4 KiB.
    (job_dir / "stills" / "scene_0000.png").write_bytes(b"x" * 5000)
    pipeline.save_state(
        job_id,
        {
            "job_id": job_id,
            "channel_key": "history_rewind" if pipeline_kind == "sleep_doc" else "economic_maverick",
            "pipeline_kind": pipeline_kind,
            "phase": "awaiting_approval",
            "expected_scene_indices": [0, 1],
            "scene_briefs": [
                {"global_idx": 0, "narration": "One"},
                {"global_idx": 1, "narration": "Two"},
            ],
            "visual_proof_only": proof_pending,
            "proof_scene_approved": not proof_pending,
        },
    )


@pytest.mark.parametrize(
    "pipeline_kind, finalizer",
    [
        ("sleep_doc", pipeline.finalize_sleep_doc_pipeline),
        ("v5_episode", v5_pipeline.finalize_v5_episode_pipeline),
    ],
)
def test_inner_finalizers_fail_closed_on_incomplete_gallery(
    pipeline_kind: str,
    finalizer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "LF_OUTPUT_ROOT", tmp_path)
    job_id = f"lf_incomplete_{pipeline_kind}"
    _seed_incomplete_job(tmp_path, job_id=job_id, pipeline_kind=pipeline_kind)

    manifest = pipeline.longform_scene_manifest(job_id)
    assert manifest["actual_indices"] == [0]
    assert manifest["missing_indices"] == [1]
    assert manifest["ready_to_finalize"] is False

    with pytest.raises(pipeline.LFRenderError, match="incomplete"):
        asyncio.run(finalizer(job_id))


def test_outer_finalize_rejects_pending_one_scene_proof_before_spawning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "LF_OUTPUT_ROOT", tmp_path)
    job_id = "lf_pending_proof"
    _seed_incomplete_job(
        tmp_path,
        job_id=job_id,
        pipeline_kind="sleep_doc",
        proof_pending=True,
    )
    # Make the gallery complete so proof approval is the only failing gate.
    (tmp_path / job_id / "stills" / "scene_0001.png").write_bytes(b"y" * 5000)

    monkeypatch.setattr(
        pipeline,
        "_spawn_lf_background_coro",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("pending proof spawned finalization")
        ),
    )

    with pytest.raises(pipeline.LFRenderError, match="approve and expand"):
        pipeline.start_finalize(job_id)


def test_selected_text_model_is_forwarded_exactly_without_grok_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    async def fake_chat_completion(**kwargs):
        calls.append(dict(kwargs))
        return {
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
            "choices": [{"message": {"content": "Selected model response"}}],
            "usage": {"prompt_tokens": 321, "completion_tokens": 123},
        }

    monkeypatch.setattr(openrouter, "chat_completion", fake_chat_completion)
    client = StudioTextClient(model="anthropic/claude-sonnet-4.6")

    assert client.complete("system", "user", max_tokens=2222, temperature=0.2) == "Selected model response"
    assert len(calls) == 1
    assert calls[0]["model"] == "anthropic/claude-sonnet-4.6"
    assert calls[0]["max_tokens"] == 2222
    assert calls[0]["messages"] == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "user"},
    ]
    assert client.last_usage == {"prompt_tokens": 321, "completion_tokens": 123}
    assert client.last_provider == "openrouter"
    assert client.last_effective_model == "anthropic/claude-sonnet-4.6"


def test_selected_text_model_failure_is_not_silently_retried_on_grok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted: list[str] = []

    async def unavailable(**kwargs):
        attempted.append(str(kwargs.get("model")))
        raise RuntimeError("selected model unavailable")

    monkeypatch.setattr(openrouter, "chat_completion", unavailable)

    with pytest.raises(RuntimeError, match="selected model unavailable"):
        StudioTextClient(model="anthropic/claude-haiku-4.5").complete("system", "user")

    assert attempted == ["anthropic/claude-haiku-4.5"]
