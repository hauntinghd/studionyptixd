from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import cliplab_router
from studio_agent import idempotent_mutations
from studio_agent.command_execution import FileExecutionLedger


@pytest.fixture(autouse=True)
def _isolated_command_ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        idempotent_mutations,
        "_LEDGER",
        FileExecutionLedger(tmp_path / "command-ledger"),
    )


def _client(jobs: dict, *, debit_credits=None) -> TestClient:
    app = FastAPI()
    app.include_router(
        cliplab_router.build_cliplab_router(
            require_auth=lambda: {"id": "creator-1"},
            jobs=jobs,
            fal_json_completion=lambda *_args, **_kwargs: {},
            debit_credits=debit_credits,
        )
    )
    return TestClient(app)


def test_upload_claims_before_debit_and_replays_without_a_second_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs: dict = {}
    counters = {"ids": 0, "debits": 0, "workers": 0}
    monkeypatch.setattr(cliplab_router, "CLIPLAB_UPLOAD_DIR", tmp_path / "uploads")

    def next_id(prefix: str) -> str:
        counters["ids"] += 1
        return f"{prefix}_{counters['ids']}"

    async def write_upload(file, dest, **_kwargs):
        payload = await file.read()
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_bytes(payload)
        return len(payload)

    async def run_ingest(*_args, **_kwargs):
        counters["workers"] += 1

    def debit(*_args, **_kwargs):
        counters["debits"] += 1
        return True

    monkeypatch.setattr(cliplab_router, "new_job_id", next_id)
    monkeypatch.setattr(cliplab_router, "write_upload_limited", write_upload)
    monkeypatch.setattr(cliplab_router, "probe_duration", lambda _path: 61.0)
    monkeypatch.setattr(cliplab_router, "run_ingest_pipeline", run_ingest)
    client = _client(jobs, debit_credits=debit)
    headers = {"X-Idempotency-Key": "cliplab-upload-command-1"}

    missing = client.post(
        "/api/cliplab/ingest/upload",
        files={"file": ("source.mp4", b"video-data", "video/mp4")},
    )
    first = client.post(
        "/api/cliplab/ingest/upload",
        headers=headers,
        files={"file": ("source.mp4", b"video-data", "video/mp4")},
    )
    replay = client.post(
        "/api/cliplab/ingest/upload",
        headers=headers,
        files={"file": ("source.mp4", b"video-data", "video/mp4")},
    )
    conflict = client.post(
        "/api/cliplab/ingest/upload",
        headers=headers,
        files={"file": ("source.mp4", b"other-data", "video/mp4")},
    )

    assert missing.status_code == 400
    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["job_id"] == first.json()["job_id"]
    assert replay.json()["idempotent_replay"] is True
    assert conflict.status_code == 409
    assert "different production mutation" in conflict.text
    assert counters == {"ids": 2, "debits": 1, "workers": 1}
    assert list(jobs) == [first.json()["job_id"]]


def test_analyze_requires_a_key_and_duplicate_submission_launches_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs: dict = {}
    calls: list[str] = []
    monkeypatch.setattr(cliplab_router, "user_owns_video", lambda *_args: True)
    monkeypatch.setattr(cliplab_router, "new_job_id", lambda _prefix: "clipa_one")

    async def run_analyze(job_id, *_args, **_kwargs):
        calls.append(job_id)

    monkeypatch.setattr(cliplab_router, "run_analyze_pipeline", run_analyze)
    client = _client(jobs)
    body = {"video_id": "vid_owned", "prompt": "Find hooks", "max_segments": 12}

    missing = client.post("/api/cliplab/analyze", json=body)
    first = client.post(
        "/api/cliplab/analyze",
        headers={"X-Idempotency-Key": "cliplab-analyze-command-1"},
        json=body,
    )
    replay = client.post(
        "/api/cliplab/analyze",
        headers={"X-Idempotency-Key": "cliplab-analyze-command-1"},
        json=body,
    )

    assert missing.status_code == 400
    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["job_id"] == "clipa_one"
    assert replay.json()["idempotent_replay"] is True
    assert calls == ["clipa_one"]


def test_render_replay_is_exact_and_key_reuse_with_new_scope_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs: dict = {}
    calls: list[list[int]] = []
    monkeypatch.setattr(cliplab_router, "user_owns_video", lambda *_args: True)
    monkeypatch.setattr(
        cliplab_router,
        "load_job_state",
        lambda _job_id: {"video_id": "vid_owned", "user_id": "creator-1"},
    )
    monkeypatch.setattr(cliplab_router, "new_job_id", lambda _prefix: "clipr_one")

    async def run_render(*_args, segment_indices, **_kwargs):
        calls.append(list(segment_indices))

    monkeypatch.setattr(cliplab_router, "run_render_pipeline", run_render)
    client = _client(jobs)
    headers = {"X-Idempotency-Key": "cliplab-render-command-1"}
    body = {
        "video_id": "vid_owned",
        "prompt_run_id": "clipa_owned",
        "segment_indices": [0, 2],
        "burn_captions": True,
    }

    first = client.post("/api/cliplab/render", headers=headers, json=body)
    replay = client.post("/api/cliplab/render", headers=headers, json=body)
    conflict = client.post(
        "/api/cliplab/render",
        headers=headers,
        json={**body, "segment_indices": [1]},
    )

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["idempotent_replay"] is True
    assert conflict.status_code == 409
    assert "different production mutation" in conflict.text
    assert calls == [[0, 2]]
