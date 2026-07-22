from __future__ import annotations

import json
from pathlib import Path

import pytest

from long_form import pipeline
from long_form import v5_pipeline


def _route(revision: int, *, image: str = "seedream_edit", video: str = "seedance") -> dict:
    return {
        "revision": revision,
        "image_model_id": image,
        "video_model": video,
    }


def test_image_route_change_quarantines_old_result_and_commits_latest(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "LF_OUTPUT_ROOT", tmp_path)
    job_id = "lf_route_image"
    destination = tmp_path / job_id / "stills" / "scene_0002.png"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"approved-prior")
    current = {"route": _route(1, image="grok_imagine")}
    dispatched: list[str] = []

    def resolve_route() -> dict:
        return dict(current["route"])

    def dispatch(model: str, candidate: Path, _guard) -> Path:
        dispatched.append(model)
        candidate.write_bytes(f"candidate-{model}".encode())
        if len(dispatched) == 1:
            current["route"] = _route(2, image="seedream_v5_lite")
        return candidate

    committed, receipt = pipeline._dispatch_longform_media_revision_aware(
        job_id,
        stage="image",
        scene_index=2,
        destination=destination,
        dispatch=dispatch,
        fallback_model="ernie_image",
        route_resolver=resolve_route,
        qa_validator=lambda _path, _stage, _index: {
            "status": "pass", "pass": True, "summary": "mock QA pass"
        },
    )

    assert committed.read_bytes() == b"candidate-seedream_v5_lite"
    assert dispatched == ["seedream_edit", "seedream_v5_lite"]
    assert all("grok" not in model for model in dispatched)
    assert receipt["status"] == "committed"
    assert receipt["media_route_revision"] == 2
    assert receipt["provider_model"] == "seedream_v5_lite"
    quarantined = list((tmp_path / job_id / "quarantine" / "image").glob("*.png"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"candidate-seedream_edit"
    canonical_receipt = json.loads(
        destination.with_suffix(".png.media-route.json").read_text(encoding="utf-8")
    )
    assert canonical_receipt["status"] == "committed"
    assert canonical_receipt["provider_model"] == "seedream_v5_lite"


def test_repeated_video_route_changes_preserve_prior_asset(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "LF_OUTPUT_ROOT", tmp_path)
    job_id = "lf_route_video"
    destination = tmp_path / job_id / "clips" / "scene_0001.mp4"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"approved-video")
    current = {"route": _route(1, video="seedance")}

    def resolve_route() -> dict:
        return dict(current["route"])

    def dispatch(model: str, candidate: Path, guard) -> Path:
        candidate.write_bytes(f"video-{model}".encode())
        current["route"] = _route(
            int(current["route"]["revision"]) + 1,
            video="pixverse" if model == "seedance" else "seedance",
        )
        assert guard() is False
        raise pipeline.LFMediaRouteChanged("fallback route is stale")

    with pytest.raises(pipeline.LFMediaRouteChanged) as exc_info:
        pipeline._dispatch_longform_media_revision_aware(
            job_id,
            stage="video",
            scene_index=1,
            destination=destination,
            dispatch=dispatch,
            fallback_model="ltx_budget",
            route_resolver=resolve_route,
            qa_validator=lambda _path, _stage, _index: {
                "status": "pass", "pass": True
            },
            max_route_restarts=2,
        )

    assert destination.read_bytes() == b"approved-video"
    assert len(exc_info.value.receipts) == 2
    assert all(r["prior_asset_retained"] for r in exc_info.value.receipts)
    assert all(r["status"] == "stale_after_provider_error" for r in exc_info.value.receipts)


def test_v5_clip_forwards_selected_video_model_and_fallback_guard(tmp_path, monkeypatch):
    from skeleton_ai import i2v_engine

    still = tmp_path / "still.png"
    still.write_bytes(b"still")
    output = tmp_path / "clip.mp4"
    captured: dict = {}

    def fake_generate(still_path, motion_prompt, out_path, **kwargs):
        captured.update(kwargs)
        assert still_path == still
        assert motion_prompt == "slow push"
        out_path.write_bytes(b"mock-video")
        return out_path

    monkeypatch.setattr(i2v_engine, "generate", fake_generate)
    guard = lambda: True
    result = v5_pipeline._gen_em_clip(
        still,
        "slow push",
        output,
        duration_sec=7,
        video_model="seedance",
        route_guard=guard,
    )

    assert result.read_bytes() == b"mock-video"
    assert captured["video_model"] == "seedance"
    assert captured["duration_sec"] == 7
    assert captured["aspect_ratio"] == "16:9"
    assert captured["fallback_guard"] is guard
