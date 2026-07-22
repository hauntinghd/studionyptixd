from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import long_form_router
from long_form import pipeline as longform_pipeline
from studio_agent import jobs as agent_jobs
from studio_agent import runpod_bridge
from studio_agent import tools


def _client() -> TestClient:
    def require_auth():
        return {"id": "creator-1", "email": "creator@example.test"}

    app = FastAPI()
    app.include_router(
        long_form_router.build_long_form_router(
            require_auth=require_auth,
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def _qa_pass_state(tmp_path: Path, job_id: str) -> tuple[dict, Path]:
    job_dir = tmp_path / job_id
    job_dir.mkdir(parents=True)
    video = job_dir / "final.mp4"
    package = job_dir / "package.txt"
    video.write_bytes(b"verified-video")
    package.write_text("verified package", encoding="utf-8")
    os.utime(video, (100, 100))
    os.utime(package, (100, 100))
    render_report = {
        "status": "pass",
        "score": 100,
        "fingerprint": "sha256-current-final",
        "created_at": 200.0,
        "checks": [
            {"id": "video_probe", "status": "pass"},
            {"id": "package", "status": "pass"},
        ],
    }
    state = {
        "job_id": job_id,
        "phase": "done",
        "percent": 100,
        "ready_to_post": True,
        "mp4_path": f"{job_id}/final.mp4",
        "final_qa": {
            "status": "pass",
            "pass": True,
            "created_at": 200.0,
            "render": render_report,
            "package": {
                "status": "pass",
                "pass": True,
                "path": str(package),
            },
            "current_assets": {"status": "pass", "pass": True},
        },
    }
    return state, video


def _disable_runpod_receipts(monkeypatch) -> None:
    monkeypatch.setattr(
        runpod_bridge,
        "get_dispatch_receipt_by_studio_job_id",
        lambda _job_id: None,
    )


def test_unverified_completed_state_never_projects_media_or_complete(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = "lf_blocked"
    state, video = _qa_pass_state(tmp_path, job_id)
    state.update({"ready_to_post": False, "phase": "done"})
    state["final_qa"] = {
        **state["final_qa"],
        "status": "fail",
        "pass": False,
    }
    assert video.is_file()
    monkeypatch.setattr(longform_pipeline, "LF_OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(longform_pipeline, "list_recent_jobs", lambda limit=20: [dict(state)])
    monkeypatch.setattr(longform_pipeline, "load_state", lambda _job_id: dict(state))
    monkeypatch.setattr(
        longform_pipeline,
        "get_status",
        lambda _job_id: {"phase": "done", "percent": 100},
    )
    _disable_runpod_receipts(monkeypatch)

    client = _client()
    listed = client.get("/api/long-form/jobs").json()["jobs"][0]
    full = client.get(f"/api/long-form/jobs/{job_id}").json()
    alias = client.get(f"/api/long-form/jobs/{job_id}/state").json()
    status = client.get(f"/api/long-form/jobs/{job_id}/status").json()
    media = client.get(f"/api/long-form/jobs/{job_id}/mp4")

    for payload in (listed, full, alias, status):
        assert payload["ready_to_post"] is False
        assert payload["phase"] == "final_qa_blocked"
        assert "mp4_url" not in payload
        assert "mp4_present" not in payload
        assert "complete" not in payload
    assert "mp4_path" not in full
    assert media.status_code == 409
    assert media.json()["detail"] == "final_qa_blocked"


def test_verified_current_state_is_the_only_state_that_projects_and_serves_mp4(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = "lf_verified"
    state, video = _qa_pass_state(tmp_path, job_id)
    monkeypatch.setattr(longform_pipeline, "LF_OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(longform_pipeline, "list_recent_jobs", lambda limit=20: [dict(state)])
    monkeypatch.setattr(longform_pipeline, "load_state", lambda _job_id: dict(state))
    monkeypatch.setattr(
        longform_pipeline,
        "get_status",
        lambda _job_id: {"phase": "done", "percent": 100},
    )
    _disable_runpod_receipts(monkeypatch)

    client = _client()
    listed = client.get("/api/long-form/jobs").json()["jobs"][0]
    full = client.get(f"/api/long-form/jobs/{job_id}").json()
    status = client.get(f"/api/long-form/jobs/{job_id}/status").json()
    media = client.get(f"/api/long-form/jobs/{job_id}/mp4")

    assert listed["ready_to_post"] is True
    assert listed["mp4_present"] is True
    assert listed["mp4_url"].endswith(f"/{job_id}/mp4")
    assert full["ready_to_post"] is True
    assert full["mp4_present"] is True
    assert status["ready_to_post"] is True
    assert status["phase"] == "done"
    assert media.status_code == 200
    assert media.content == video.read_bytes()


def test_mp4_changed_after_qa_is_fail_closed(monkeypatch, tmp_path: Path) -> None:
    job_id = "lf_stale"
    state, video = _qa_pass_state(tmp_path, job_id)
    os.utime(video, (500, 500))
    monkeypatch.setattr(longform_pipeline, "LF_OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(longform_pipeline, "load_state", lambda _job_id: dict(state))
    _disable_runpod_receipts(monkeypatch)

    response = _client().get(f"/api/long-form/jobs/{job_id}/mp4")

    assert response.status_code == 409
    assert response.json()["detail"] == "final_qa_blocked"


def test_agent_longform_state_exposes_current_scene_qa_and_blocks_stale_finalize(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = "lf_agent_scene_qa"
    monkeypatch.setattr(longform_pipeline, "LF_OUTPUT_ROOT", tmp_path)
    job_dir = tmp_path / job_id
    still = job_dir / "stills" / "scene_0000.png"
    still.parent.mkdir(parents=True)
    # The release manifest intentionally excludes truncated/tiny artifacts.
    still.write_bytes(b"current-still" * 512)
    longform_pipeline.save_state(job_id, {
        "job_id": job_id,
        "phase": "awaiting_approval",
        "percent": 45,
        "scenes_generated": 1,
        "scenes_per_chapter": 1,
        "outline": {"title": "QA documentary", "render_style": "cinematic"},
    })
    (job_dir / "chapters.json").write_text(json.dumps({
        "chapters": [{
            "chapter_index": 0,
            "narration": "The letter is opened.",
            "scene_prompts": ["A person opens a letter at a desk."],
        }],
    }), encoding="utf-8")
    component = {"status": "pass", "pass": True, "summary": "pass"}
    report = {
        "status": "pass",
        "pass": True,
        "job_id": job_id,
        "stage": "image",
        "scene_index": 0,
        "components": {
            "render_style": component,
            "identity": component,
            "correspondence": component,
            "clip": component,
        },
        "asset_fingerprint": longform_pipeline._longform_asset_fingerprint(still),
        "qa_context_fingerprint": longform_pipeline._longform_qa_context_fingerprint(
            job_id, "image", 0
        ),
    }
    longform_pipeline._write_longform_visual_qa(still, report)

    current = agent_jobs._longform_status(job_id)
    assert current["can_finalize"] is True
    assert current["scenes"][0]["qa_stale"] is False
    assert current["qa_state"]["status"] == "pass"

    still.write_bytes(b"same-path-new-content")
    stale = agent_jobs._longform_status(job_id)
    assert stale["can_finalize"] is False
    assert stale["scenes"][0]["qa_stale"] is True
    assert stale["qa_state"]["status"] == "fail"


def test_agent_longform_final_qa_block_is_terminal_and_retryable(monkeypatch, tmp_path: Path) -> None:
    job_id = "lf_agent_final_block"
    monkeypatch.setattr(longform_pipeline, "LF_OUTPUT_ROOT", tmp_path)
    (tmp_path / job_id).mkdir(parents=True)
    longform_pipeline.save_state(job_id, {
        "job_id": job_id,
        "phase": "final_qa_blocked",
        "percent": 99,
        "ready_to_post": False,
        "final_qa": {"status": "fail", "pass": False, "failures": ["caption drift"]},
        "outline": {"title": "Blocked documentary"},
    })

    snapshot = agent_jobs._longform_status(job_id)

    assert snapshot["status"] == "final_qa_blocked"
    assert snapshot["running"] is False
    assert snapshot["can_finalize"] is True
    assert snapshot["qa_state"]["ready_to_post"] is False
    assert "caption drift" in snapshot["qa_state"]["reasons"]


def test_local_scene_regenerate_and_finalize_require_logged_idempotent_envelope(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = "lf_mutation"
    still = tmp_path / "scene.png"
    still.write_bytes(b"new candidate")
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)
    monkeypatch.setattr(
        agent_jobs,
        "job_access_metadata",
        lambda requested, kind="": {
            "exists": requested == job_id,
            "job_id": requested,
            "kind": "longform",
            "owner_id": "creator-1",
        },
    )
    monkeypatch.setattr(
        longform_pipeline,
        "regenerate_still",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("router must not bypass the logged tool")
        ),
    )
    monkeypatch.setattr(
        longform_pipeline,
        "start_finalize",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("router must not bypass the logged tool")
        ),
    )
    monkeypatch.setattr(longform_pipeline, "job_still_path", lambda *_args: still)

    def execute(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        if name == "regenerate_longform_still":
            return json.dumps({"ok": True, "postcondition_verified": True})
        return json.dumps({"status": "running", "accepted": True, "complete": False})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    client = _client()

    missing_regen_key = client.post(
        f"/api/long-form/jobs/{job_id}/regenerate-scene",
        json={"scene_idx": 2},
    )
    missing_finalize_key = client.post(f"/api/long-form/jobs/{job_id}/finalize")
    regenerated = client.post(
        f"/api/long-form/jobs/{job_id}/regenerate-scene",
        headers={"X-Idempotency-Key": "regen-local-1"},
        json={"scene_idx": 2, "new_prompt": "Stronger silhouette"},
    )
    finalized = client.post(
        f"/api/long-form/jobs/{job_id}/finalize",
        headers={"X-Idempotency-Key": "final-local-1"},
    )

    assert missing_regen_key.status_code == 400
    assert missing_finalize_key.status_code == 400
    assert regenerated.status_code == 200, regenerated.text
    assert regenerated.json()["postcondition_verified"] is True
    assert finalized.status_code == 200, finalized.text
    assert finalized.json()["accepted"] is True
    assert finalized.json()["complete"] is False
    assert [row[0] for row in calls] == [
        "regenerate_longform_still",
        "finalize_longform_render",
    ]
    assert calls[0][1]["_runpod_command_id"] == "regen-local-1"
    assert calls[1][1]["_runpod_command_id"] == "final-local-1"


def test_unverifiable_logged_mutation_is_not_reported_as_success(
    monkeypatch,
) -> None:
    job_id = "lf_unverified"
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)
    monkeypatch.setattr(
        agent_jobs,
        "job_access_metadata",
        lambda requested, kind="": {
            "exists": requested == job_id,
            "job_id": requested,
            "kind": "longform",
            "owner_id": "creator-1",
        },
    )
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: json.dumps({"ok": True}),
    )

    response = _client().post(
        f"/api/long-form/jobs/{job_id}/finalize",
        headers={"X-Idempotency-Key": "unverified-final"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "finalize_postcondition_unverified"
