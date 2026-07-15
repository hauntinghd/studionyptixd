from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import cliplab.config as cliplab_config
import cliplab.pipeline as pipeline
import cliplab.render as cliplab_render
from cliplab.models import TranscriptCue
from cliplab_router import build_cliplab_router
from studio_agent import jobs as agent_jobs


@pytest.fixture
def isolated_cliplab(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    jobs_dir = tmp_path / "jobs"
    render_dir = tmp_path / "renders"
    upload_dir = tmp_path / "uploads"
    jobs_dir.mkdir()
    render_dir.mkdir()
    upload_dir.mkdir()
    monkeypatch.setattr(pipeline, "CLIPLAB_JOBS_DIR", jobs_dir)
    monkeypatch.setattr(pipeline, "CLIPLAB_RENDER_DIR", render_dir)
    monkeypatch.setattr(pipeline, "CLIPLAB_UPLOAD_DIR", upload_dir)
    monkeypatch.setattr(cliplab_render, "CLIPLAB_RENDER_DIR", render_dir)
    monkeypatch.setattr(cliplab_config, "CLIPLAB_JOBS_DIR", jobs_dir)
    monkeypatch.setattr(cliplab_config, "CLIPLAB_RENDER_DIR", render_dir)
    monkeypatch.setattr(cliplab_config, "CLIPLAB_UPLOAD_DIR", upload_dir)
    return {"jobs": jobs_dir, "renders": render_dir, "uploads": upload_dir}


def _ingest_state(tmp_path: Path, *, video_id: str = "vid_owner", user_id: str = "creator-a") -> Path:
    source = tmp_path / f"{video_id}.mp4"
    source.write_bytes(b"source-video")
    cue = TranscriptCue(start=0.0, end=12.0, text="A strong opening and complete payoff.")
    pipeline.save_job_state(
        f"clipi_{video_id}",
        {
            "status": "complete",
            "progress": 100,
            "type": "cliplab_ingest",
            "video_id": video_id,
            "video_path": str(source),
            "user_id": user_id,
            "cues": [cue.model_dump()],
        },
    )
    return source


def test_pipeline_contract_accepts_agent_metadata_and_defines_remix() -> None:
    analyze = inspect.signature(pipeline.run_analyze_pipeline).parameters
    render = inspect.signature(pipeline.run_render_pipeline).parameters
    remix = inspect.signature(pipeline.run_remix_pipeline).parameters

    assert {"user_id", "channel_id", "registry_key", "source", "provider", "model"} <= set(analyze)
    assert {"user_id", "channel_id", "registry_key", "source"} <= set(render)
    assert {"user_id", "registry_key", "source", "catalyst_channel_id"} <= set(remix)


def test_studio_agent_tool_calls_match_pipeline_contract(
    monkeypatch: pytest.MonkeyPatch,
    isolated_cliplab: dict[str, Path],
) -> None:
    from studio_agent import tools

    captured: dict[str, dict[str, Any]] = {}
    job_ids = {"clipa": "clipa_tool", "clipr": "clipr_tool", "remix": "remix_tool"}

    async def fake_analyze(job_id: str, jobs: dict, **kwargs: Any) -> None:
        captured["analyze"] = kwargs

    async def fake_render(job_id: str, jobs: dict, **kwargs: Any) -> None:
        captured["render"] = kwargs

    async def fake_remix(job_id: str, jobs: dict, **kwargs: Any) -> None:
        captured["remix"] = kwargs

    class ImmediateThread:
        def __init__(self, *, target: Any, **kwargs: Any) -> None:
            self.target = target

        def start(self) -> None:
            self.target()

    monkeypatch.setattr(tools, "_require_cliplab_admin", lambda _user_id: None)
    monkeypatch.setattr(
        tools,
        "_session_channel_context",
        lambda _session_id: {"channel_id": "channel-1", "registry_key": "creator_channel"},
    )
    monkeypatch.setattr(
        tools.store,
        "get_session",
        lambda *args, **kwargs: {"model": "anthropic/claude-sonnet-4.6"},
    )
    monkeypatch.setattr(tools.telemetry, "record_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(tools.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(pipeline, "new_job_id", lambda prefix: job_ids[prefix])
    monkeypatch.setattr(pipeline, "run_analyze_pipeline", fake_analyze)
    monkeypatch.setattr(pipeline, "run_render_pipeline", fake_render)
    monkeypatch.setattr(pipeline, "run_remix_pipeline", fake_remix)

    tools.execute_tool(
        "analyze_cliplab_video",
        {"video_id": "vid_owner", "prompt": "Find the best hook", "provider": "local"},
        user_id="creator-a",
        content_format="short",
        session_id="session-1",
    )
    tools.execute_tool(
        "render_cliplab_segments",
        {"video_id": "vid_owner", "analyze_job_id": "clipa_tool", "segment_indices": [0]},
        user_id="creator-a",
        content_format="short",
        session_id="session-1",
    )
    tools.execute_tool(
        "remix_cliplab_short",
        {"video_id": "vid_owner", "style_preset": "documentary"},
        user_id="creator-a",
        content_format="short",
        session_id="session-1",
    )

    assert captured["analyze"]["model"] == "anthropic/claude-sonnet-4.6"
    assert captured["analyze"]["user_id"] == "creator-a"
    assert captured["render"]["registry_key"] == "creator_channel"
    assert captured["remix"]["user_id"] == "creator-a"
    assert captured["remix"]["source"] == "studio_agent_cliplab"


def test_analyze_uses_studio_model_callback_and_persists_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    isolated_cliplab: dict[str, Path],
) -> None:
    _ingest_state(tmp_path)
    captured: dict[str, Any] = {}

    async def fake_studio_completion(
        system_prompt: str,
        user_prompt: str,
        **kwargs: Any,
    ) -> dict:
        captured.update({"system": system_prompt, "user": user_prompt, **kwargs})
        return {
            "segments": [
                {
                    "start": 0.0,
                    "end": 12.0,
                    "confidence": 0.8,
                    "virality_score": 91,
                    "why_it_matches": "Complete hook and payoff",
                    "transcript_snippet": "A strong opening",
                }
            ]
        }

    monkeypatch.setattr(pipeline, "_studio_json_completion", fake_studio_completion)
    job_id = "clipa_analysis"
    memory = {
        job_id: {
            "status": "queued",
            "type": "cliplab_analyze",
            "user_id": "creator-a",
            "created_at": 1.0,
        }
    }
    pipeline.save_job_state(job_id, {**memory[job_id], "prompt": "Find the strongest hook"})

    asyncio.run(
        pipeline.run_analyze_pipeline(
            job_id,
            memory,
            video_id="vid_owner",
            prompt="Find the strongest hook",
            max_segments=4,
            json_completion=None,
            user_id="creator-a",
            channel_id="channel-1",
            registry_key="creator_channel",
            source="studio_agent_cliplab",
            provider="auto",
            model="anthropic/claude-sonnet-4.6",
        )
    )

    state = pipeline.load_job_state(job_id)
    assert captured["model"] == "anthropic/claude-sonnet-4.6"
    assert state["status"] == "complete"
    assert state["user_id"] == "creator-a"
    assert state["channel_id"] == "channel-1"
    assert state["registry_key"] == "creator_channel"
    assert state["source"] == "studio_agent_cliplab"
    assert state["provider_used"] == "local"
    assert state["segment_count"] == 1
    assert state["segments"][0]["virality_score"] == 91


def test_analyze_cross_owner_failure_is_durable(
    tmp_path: Path,
    isolated_cliplab: dict[str, Path],
) -> None:
    _ingest_state(tmp_path, user_id="creator-a")
    job_id = "clipa_cross_owner"
    memory = {job_id: {"status": "queued", "user_id": "creator-b"}}
    pipeline.save_job_state(job_id, memory[job_id])

    asyncio.run(
        pipeline.run_analyze_pipeline(
            job_id,
            memory,
            video_id="vid_owner",
            prompt="Find clips",
            max_segments=3,
            json_completion=None,
            user_id="creator-b",
        )
    )

    state = pipeline.load_job_state(job_id)
    assert state["status"] == "error"
    assert state["progress"] == 100
    assert state["user_id"] == "creator-b"
    assert "Source video not found for this user" in state["error"]


def test_render_and_remix_persist_owner_scoped_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    isolated_cliplab: dict[str, Path],
) -> None:
    paths = isolated_cliplab
    _ingest_state(tmp_path)
    pipeline.save_job_state(
        "clipa_ready",
        {
            "status": "complete",
            "video_id": "vid_owner",
            "user_id": "creator-a",
            "segments": [
                {
                    "start": 0.0,
                    "end": 12.0,
                    "confidence": 0.8,
                    "virality_score": 88,
                }
            ],
        },
    )

    async def fake_render_batch(*args: Any, **kwargs: Any) -> list[dict]:
        out = paths["renders"] / "vid_owner" / "clip_000.mp4"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"rendered")
        return [{"index": 0, "filename": out.name, "path": str(out), "start": 0, "end": 12}]

    async def fake_remix(*args: Any, job_id: str, **kwargs: Any) -> dict:
        out = paths["renders"] / "vid_owner" / f"{job_id}.mp4"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"remixed")
        return {
            "filename": out.name,
            "path": str(out),
            "style_preset": kwargs["style_preset"],
            "caption_style": kwargs["caption_style"],
            "edit_intensity": kwargs["edit_intensity"],
            "background_mode": kwargs["background_mode"],
        }

    monkeypatch.setattr(pipeline, "render_clips_batch", fake_render_batch)
    monkeypatch.setattr(pipeline, "render_remix_short", fake_remix)

    render_jobs = {"clipr_ready": {"status": "queued", "user_id": "creator-a"}}
    pipeline.save_job_state("clipr_ready", render_jobs["clipr_ready"])
    asyncio.run(
        pipeline.run_render_pipeline(
            "clipr_ready",
            render_jobs,
            video_id="vid_owner",
            analyze_job_id="clipa_ready",
            segment_indices=[0],
            burn_captions=True,
            user_id="creator-a",
            channel_id="channel-1",
            registry_key="creator_channel",
            source="studio_agent_cliplab",
        )
    )
    rendered = pipeline.load_job_state("clipr_ready")
    assert rendered["status"] == "complete"
    assert rendered["user_id"] == "creator-a"
    assert rendered["clips"][0]["url"].endswith("/vid_owner/clip_000.mp4")

    remix_jobs = {"remix_ready": {"status": "queued", "user_id": "creator-a"}}
    pipeline.save_job_state("remix_ready", {**remix_jobs["remix_ready"], "remix": {}})
    asyncio.run(
        pipeline.run_remix_pipeline(
            "remix_ready",
            remix_jobs,
            video_id="vid_owner",
            style_preset="documentary",
            caption_style="minimal",
            edit_intensity="high",
            background_mode="blur",
            burn_captions=True,
            catalyst_channel_id="channel-1",
            notes="Keep the cold open tight",
            user_id="creator-a",
            registry_key="creator_channel",
            source="studio_agent_cliplab",
        )
    )
    remixed = pipeline.load_job_state("remix_ready")
    assert remixed["status"] == "complete"
    assert remixed["user_id"] == "creator-a"
    assert remixed["registry_key"] == "creator_channel"
    assert remixed["remix"]["url"].endswith("/vid_owner/remix_ready.mp4")


def test_remix_filter_applies_requested_treatment() -> None:
    cue = TranscriptCue(start=0.0, end=2.0, text="Watch this")
    blurred = cliplab_render.build_remix_filter(
        background_mode="blur",
        style_preset="high_energy",
        edit_intensity="high",
        caption_style="bold",
        cues=[cue],
        burn_captions=True,
    )
    solid = cliplab_render.build_remix_filter(
        background_mode="solid",
        style_preset="documentary",
        edit_intensity="low",
        caption_style="minimal",
        cues=[],
        burn_captions=False,
    )

    assert "split=2" in blurred and "gblur=sigma=28" in blurred
    assert "saturation=1.200" in blurred and "drawtext=" in blurred
    assert "pad=1080:1920" in solid and "saturation=0.880" in solid


def _cliplab_app(user_id: str) -> FastAPI:
    app = FastAPI()

    async def require_auth() -> dict:
        return {"id": user_id, "email": f"{user_id}@example.com"}

    async def completion(*args: Any, **kwargs: Any) -> dict:
        return {"segments": []}

    app.include_router(
        build_cliplab_router(
            require_auth=require_auth,
            jobs={},
            fal_json_completion=completion,
        )
    )
    return app


def test_direct_clip_delivery_is_owner_scoped(
    tmp_path: Path,
    isolated_cliplab: dict[str, Path],
) -> None:
    render_dir = isolated_cliplab["renders"] / "vid_owner"
    render_dir.mkdir(parents=True)
    clip = render_dir / "clip_000.mp4"
    clip.write_bytes(b"playable")
    _ingest_state(tmp_path)
    pipeline.save_job_state(
        "clipr_delivery",
        {
            "status": "complete",
            "video_id": "vid_owner",
            "user_id": "creator-a",
            "clips": [{"filename": clip.name, "path": str(clip)}],
        },
    )

    owner = TestClient(_cliplab_app("creator-a")).get(
        "/api/cliplab/clips/vid_owner/clip_000.mp4"
    )
    stranger = TestClient(_cliplab_app("creator-b")).get(
        "/api/cliplab/clips/vid_owner/clip_000.mp4"
    )

    assert owner.status_code == 200
    assert owner.content == b"playable"
    assert stranger.status_code == 404


def test_studio_agent_media_and_package_delivery_are_owner_scoped(
    isolated_cliplab: dict[str, Path],
) -> None:
    from studio_agent_router import build_studio_agent_router

    render_dir = isolated_cliplab["renders"] / "vid_owner"
    render_dir.mkdir(parents=True)
    clip = render_dir / "clip_000.mp4"
    clip.write_bytes(b"playable")
    pipeline.save_job_state(
        "clipr_package",
        {
            "status": "complete",
            "video_id": "vid_owner",
            "user_id": "creator-a",
            "prompt": "Find the best hook",
            "clips": [{"filename": clip.name, "path": str(clip)}],
        },
    )

    def app_for(user_id: str) -> FastAPI:
        app = FastAPI()

        async def require_auth() -> dict:
            return {"id": user_id, "email": f"{user_id}@example.com"}

        app.include_router(
            build_studio_agent_router(
                require_auth=require_auth,
                lane_access_check=lambda _user: True,
            )
        )
        return app

    owner = TestClient(app_for("creator-a"))
    media = owner.get("/api/studio-agent/jobs/clipr_package/media?kind=cliplab")
    package = owner.get("/api/studio-agent/jobs/clipr_package/package?kind=cliplab")
    stranger = TestClient(app_for("creator-b")).get(
        "/api/studio-agent/jobs/clipr_package/media?kind=cliplab"
    )

    assert media.status_code == 200 and media.content == b"playable"
    assert package.status_code == 200
    assert "ClipLab export package" in package.text
    assert "clip_000.mp4" in package.text
    assert stranger.status_code == 404


def test_backend_mount_exposes_authenticated_cliplab_routes() -> None:
    import backend

    paths = backend.app.openapi()["paths"]
    assert "/api/cliplab/status" in paths
    assert "/api/cliplab/analyze" in paths
    assert "/api/cliplab/render" in paths
    assert "/api/cliplab/clips/{video_id}/{filename}" in paths
