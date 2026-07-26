from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import skeleton_ai_router
from studio_agent import idempotent_mutations, tools
from studio_agent.command_execution import InMemoryExecutionLedger


def _client(*, reserve_credit=None, user_id: str = "skeleton-owner") -> TestClient:
    def require_auth():
        return {"id": user_id, "email": f"{user_id}@example.test"}

    app = FastAPI()
    app.include_router(
        skeleton_ai_router.build_skeleton_ai_router(
            require_auth=require_auth,
            reserve_credit=reserve_credit,
        )
    )
    return TestClient(app)


def _isolate(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        idempotent_mutations,
        "_LEDGER",
        InMemoryExecutionLedger(),
    )
    monkeypatch.setattr(skeleton_ai_router, "OUTPUT_ROOT", tmp_path)


def test_script_duplicate_replays_without_second_anthropic_call(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolate(monkeypatch, tmp_path)
    provider_calls: list[str] = []

    class FakeGrok:
        def complete(self, _system, _prompt, **_kwargs):
            provider_calls.append("anthropic")
            return "One durable generated script."

    monkeypatch.setattr(skeleton_ai_router, "GrokClient", FakeGrok)
    monkeypatch.setattr(skeleton_ai_router, "_references_for_skeleton_ai", lambda _user: "")
    client = _client()
    headers = {"X-Idempotency-Key": "script-action-1"}
    body = {"category": "people_blogs", "topic": "Dopamine", "stream": False}

    first = client.post("/api/skeleton-ai/script", headers=headers, json=body)
    replay = client.post("/api/skeleton-ai/script", headers=headers, json=body)
    conflict = client.post(
        "/api/skeleton-ai/script",
        headers=headers,
        json={**body, "topic": "A different request"},
    )

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["script"] == first.json()["script"]
    assert replay.json()["idempotent_replay"] is True
    assert conflict.status_code == 409
    assert provider_calls == ["anthropic"]


def test_streaming_script_replay_keeps_command_scope_thread_safe(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolate(monkeypatch, tmp_path)
    provider_calls: list[str] = []

    class FakeGrok:
        def stream(self, _system, _prompt, **_kwargs):
            provider_calls.append("anthropic-stream")
            yield "First "
            yield "second"

    monkeypatch.setattr(skeleton_ai_router, "GrokClient", FakeGrok)
    monkeypatch.setattr(skeleton_ai_router, "_references_for_skeleton_ai", lambda _user: "")
    client = _client()
    headers = {"X-Idempotency-Key": "script-stream-action-1"}
    body = {"category": "people_blogs", "topic": "Dopamine", "stream": True}

    first = client.post("/api/skeleton-ai/script", headers=headers, json=body)
    replay = client.post("/api/skeleton-ai/script", headers=headers, json=body)

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.text == first.text
    assert first.text.endswith("data: [DONE]\n\n")
    assert provider_calls == ["anthropic-stream"]


def test_local_full_generate_claims_before_credit_and_replays_without_second_spend(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolate(monkeypatch, tmp_path)
    events: list[str] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)

    async def reserve(_user, *, ac_cost: int):
        events.append(f"reserve:{ac_cost}")
        return True, "monthly", {"credits_total_remaining": 99}

    def render(**_kwargs):
        events.append("provider")
        return {"status": "complete", "video_path": "/generated/final.mp4"}

    monkeypatch.setattr(skeleton_ai_router, "run_pipeline", render)
    client = _client(reserve_credit=reserve)
    headers = {"X-Idempotency-Key": "full-generate-action-1"}
    body = {
        "category": "people_blogs",
        "topic": "Why habits compound",
        "script_override": "A complete script.",
        "video_model": "seedance",
        "image_model": "seedream_edit",
    }

    first = client.post("/api/skeleton-ai/generate", headers=headers, json=body)
    replay = client.post("/api/skeleton-ai/generate", headers=headers, json=body)

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["job_id"] == first.json()["job_id"]
    assert replay.json()["idempotent_replay"] is True
    assert events == [f"reserve:{skeleton_ai_router.AC_COST_STANDARD}", "provider"]


def test_generate_route_validation_happens_before_credit_or_provider(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolate(monkeypatch, tmp_path)
    events: list[str] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)

    async def reserve(_user, *, ac_cost: int):
        events.append(f"reserve:{ac_cost}")
        return True, "monthly", {}

    monkeypatch.setattr(
        skeleton_ai_router,
        "run_pipeline",
        lambda **_kwargs: events.append("provider") or {"status": "complete"},
    )
    response = _client(reserve_credit=reserve).post(
        "/api/skeleton-ai/generate",
        headers={"X-Idempotency-Key": "invalid-route-action-1"},
        json={
            "category": "not_a_real_skeleton_category",
            "topic": "Must not spend",
            "video_model": "seedance",
        },
    )

    assert response.status_code == 400
    assert events == []


def test_regenerate_checks_job_owner_before_reference_or_provider_work(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolate(monkeypatch, tmp_path)
    job_id = "ownedjob123"
    workspace = tmp_path / job_id
    workspace.mkdir(parents=True)
    (workspace / "job_spec.json").write_text(
        json.dumps({"job_id": job_id, "user_id": "different-owner"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "scenes": [
                    {
                        "beat_index": 0,
                        "outfit": "black hoodie",
                        "scene_action": "stands in a lab",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    events: list[str] = []
    monkeypatch.setattr(
        skeleton_ai_router,
        "_persist_skeleton_reference",
        lambda *_args, **_kwargs: events.append("reference") or "",
    )
    monkeypatch.setattr(
        skeleton_ai_router,
        "generate_still_edit",
        lambda *_args, **_kwargs: events.append("provider"),
    )

    response = _client(user_id="skeleton-owner").post(
        "/api/skeleton-ai/scenes/regenerate",
        headers={"X-Idempotency-Key": "cross-owner-regenerate-1"},
        json={
            "job_id": job_id,
            "beat_index": 0,
            "reference_image": "data:image/png;base64,cHJpdmF0ZQ==",
        },
    )

    assert response.status_code == 404
    assert events == []


def test_billable_routes_reject_missing_key_before_provider_construction(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(
        skeleton_ai_router,
        "GrokClient",
        lambda: (_ for _ in ()).throw(AssertionError("provider client was constructed")),
    )

    response = _client().post(
        "/api/skeleton-ai/script",
        json={"category": "people_blogs", "topic": "No lease"},
    )

    assert response.status_code == 400
    assert "X-Idempotency-Key is required" in response.text


def test_create_panel_sends_a_single_script_action_key() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "ViralShorts-App"
        / "src"
        / "studio"
        / "panels"
        / "CreatePanel.tsx"
    ).read_text(encoding="utf-8")

    assert "acquireProductionCommandLease(" in source
    script_call = source[source.index("'skeleton-script'") :]
    script_call = script_call[: script_call.index("body: JSON.stringify")]
    assert "'X-Idempotency-Key': command.commandId" in script_call
    assert "command.release()" in source


def test_create_panel_routes_every_skeleton_call_through_canonical_backend() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "ViralShorts-App"
        / "src"
        / "studio"
        / "panels"
        / "CreatePanel.tsx"
    ).read_text(encoding="utf-8")

    assert "fetch('/api/skeleton-ai/" not in source
    assert 'fetch("/api/skeleton-ai/' not in source
    assert source.count("fetch(resolveStudioBackendUrl('/api/skeleton-ai/") == 9
