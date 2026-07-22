from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi import HTTPException

from studio_agent import render_qa, tools, visual_qa
from studio_agent_router import ScenePromptRequest, _verified_mutation_payload


def _passing_workspace(tmp_path: Path) -> tuple[Path, Path]:
    video = tmp_path / "styled_short.mp4"
    video.write_bytes(b"v" * 300_000)
    package = tmp_path / "package.txt"
    package.write_text(
        "Title: Test\nDescription: Complete story\nTags: test\nHashtags: #test\nCTA: Follow\n",
        encoding="utf-8",
    )
    (tmp_path / "job_spec.json").write_text(
        json.dumps({"captions_enabled": True, "caption_mode": "word"}),
        encoding="utf-8",
    )
    (tmp_path / "trimmed").mkdir()
    scenes = []
    for index, (sid, narration) in enumerate((("b00", "HOOK"), ("b01", "PAYOFF"))):
        scene = {
            "index": index,
            "sid": sid,
            "narration": narration,
            "scene_action": f"Distinct physical beat {index}",
            "duration_sec": 5.0,
            "still_qa": {"status": "pass", "pass": True, "summary": "identity pass"},
            "scene_correspondence_qa": {"status": "pass", "pass": True, "summary": "story pass"},
        }
        scene["visual_qa"] = visual_qa.build_scene_visual_qa(
            tmp_path,
            scene,
            still_qa=scene["still_qa"],
            correspondence_qa=scene["scene_correspondence_qa"],
            require_clip=False,
        )
        scenes.append(scene)
        (tmp_path / "trimmed" / f"{sid}_filter.txt").write_text(
            "drawtext=text='CAPTION':y=h*0.78:enable='between(t\\,0\\,5)',"
            "drawtext=text='Studio':y=h*0.93",
            encoding="utf-8",
        )
    # Neighbor assets are part of the aggregate fingerprint, so recompute once
    # every scene exists.
    for index, scene in enumerate(scenes):
        scene["visual_qa"] = visual_qa.build_scene_visual_qa(
            tmp_path,
            scene,
            previous_scene=scenes[index - 1] if index else None,
            next_scene=scenes[index + 1] if index + 1 < len(scenes) else None,
            still_qa=scene["still_qa"],
            correspondence_qa=scene["scene_correspondence_qa"],
            require_clip=False,
        )
    (tmp_path / "scenes.json").write_text(json.dumps(scenes, indent=2), encoding="utf-8")
    return video, package


def test_strict_render_gate_passes_only_with_full_edit_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    video, package = _passing_workspace(tmp_path)
    monkeypatch.setattr(render_qa, "_probe_video", lambda _path: {
        "ok": True,
        "duration": 10.0,
        "video_duration": 10.0,
        "audio_duration": 10.0,
        "width": 720,
        "height": 1280,
        "has_audio": True,
    })
    monkeypatch.setattr(render_qa, "_probe_audio_levels", lambda _path: {
        "ok": True, "mean_db": -18.0, "max_db": -2.0,
    })
    monkeypatch.setattr(render_qa, "_sample_frame_hashes", lambda _path, _duration: {
        "ok": True, "hashes": ["a", "b", "c", "d"],
    })

    report = render_qa.analyze_render(
        job_id="strict-gate",
        kind="shortform",
        video_path=video,
        package_path=package,
    )

    assert report["status"] == "pass"
    assert report["fingerprint"]
    assert all(check["status"] == "pass" for check in report["checks"])


def test_strict_render_gate_blocks_silent_final(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    video, package = _passing_workspace(tmp_path)
    monkeypatch.setattr(render_qa, "_probe_video", lambda _path: {
        "ok": True,
        "duration": 10.0,
        "video_duration": 10.0,
        "audio_duration": 10.0,
        "width": 720,
        "height": 1280,
        "has_audio": True,
    })
    monkeypatch.setattr(render_qa, "_probe_audio_levels", lambda _path: {
        "ok": True, "mean_db": -80.0, "max_db": -70.0,
    })
    monkeypatch.setattr(render_qa, "_sample_frame_hashes", lambda _path, _duration: {
        "ok": True, "hashes": ["a", "b", "c", "d"],
    })

    report = render_qa.analyze_render(
        job_id="silent-gate",
        kind="shortform",
        video_path=video,
        package_path=package,
    )

    assert report["status"] == "fail"
    assert next(check for check in report["checks"] if check["id"] == "audio_levels")["status"] == "fail"


def test_mutation_receipts_never_treat_queue_or_qa_block_as_success() -> None:
    queued = json.loads(tools._validate_mutation_result(
        "animate_production_scenes",
        json.dumps({"ok": True, "status": "queued", "job_id": "job-1"}),
    ))
    assert "ok" not in queued
    assert queued["accepted"] is True
    assert queued["complete"] is False

    with pytest.raises(RuntimeError, match="durable postcondition"):
        tools._validate_mutation_result(
            "finalize_production",
            json.dumps({"status": "final_qa_blocked", "ready_to_post": False}),
        )

    with pytest.raises(HTTPException) as exc:
        _verified_mutation_payload(
            {"ok": False, "status": "visual_qa_failed"},
            operation="regenerate_production_scene",
        )
    assert exc.value.status_code == 409


def test_render_qa_cache_rejects_same_mtime_content_replacement(tmp_path: Path) -> None:
    video = tmp_path / "final.mp4"
    package = tmp_path / "package.txt"
    cache = tmp_path / "render_qa.json"
    video.write_bytes(b"old-render-bytes")
    package.write_text("old package", encoding="utf-8")
    cache.write_text(
        json.dumps({
            "version": render_qa.QA_VERSION,
            "fingerprint": render_qa._render_fingerprint(video, package),
            "status": "pass",
        }),
        encoding="utf-8",
    )
    older = cache.stat().st_mtime - 10
    video.write_bytes(b"new-render-bytes")
    package.write_text("new package", encoding="utf-8")
    os.utime(video, (older, older))
    os.utime(package, (older, older))

    assert render_qa._read_cache(cache, video, package) is None


def test_longform_story_gate_reads_chapters_sidecar(tmp_path: Path) -> None:
    (tmp_path / "state.json").write_text(json.dumps({"phase": "finalizing"}), encoding="utf-8")
    (tmp_path / "chapters.json").write_text(
        json.dumps({"chapters": [{"title": "Opening", "narration": "A complete narrated chapter."}]}),
        encoding="utf-8",
    )
    checks: list[dict] = []

    render_qa._longform_edit_checks(checks, tmp_path, 120.0)

    assert next(row for row in checks if row["id"] == "longform_story")["status"] == "pass"


def test_scene_prompt_api_matches_300_character_provider_contract() -> None:
    assert len(ScenePromptRequest(prompt="x" * 300).prompt) == 300
    with pytest.raises(ValueError):
        ScenePromptRequest(prompt="x" * 301)


def test_duration_mutation_rereads_value_and_revokes_final_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "scenes.json").write_text(json.dumps([{
        "index": 0,
        "sid": "b00",
        "duration_sec": 5.0,
        "approved_for_video": True,
        "approved_for_animation": True,
        "visual_qa": {"status": "pass", "pass": True},
    }]), encoding="utf-8")
    (tmp_path / "result.json").write_text(json.dumps({
        "status": "complete",
        "ready_to_post": True,
        "video_path": str(tmp_path / "accepted.mp4"),
        "render_qa": {"status": "pass"},
    }), encoding="utf-8")
    (tmp_path / "render_qa.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: tmp_path)

    receipt = json.loads(tools.set_production_scene_duration("job_1", 0, 7.25))
    durable_scene = json.loads((tmp_path / "scenes.json").read_text(encoding="utf-8"))[0]
    durable_result = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))

    assert receipt["postcondition_verified"] is True
    assert durable_scene["duration_sec"] == 7.25
    assert durable_scene["approved_for_video"] is False
    assert durable_result["ready_to_post"] is False
    assert "video_path" not in durable_result
    assert durable_result["staged_video_path"].endswith("accepted.mp4")
    assert not (tmp_path / "render_qa.json").exists()


def test_duration_mutation_cannot_report_success_when_save_does_not_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import skeleton_ai.styled_pipeline as styled_pipeline

    original = [{
        "index": 0,
        "sid": "b00",
        "duration_sec": 5.0,
        "approved_for_video": True,
        "approved_for_animation": True,
    }]
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: tmp_path)
    monkeypatch.setattr(styled_pipeline, "load_scenes", lambda _workspace: json.loads(json.dumps(original)))
    monkeypatch.setattr(styled_pipeline, "save_scenes", lambda _workspace, _scenes: None)

    receipt = json.loads(tools.set_production_scene_duration("job_1", 0, 8.0))

    assert receipt["ok"] is False
    assert receipt["postcondition_verified"] is False
    with pytest.raises(RuntimeError, match="durable postcondition"):
        tools._validate_mutation_result("set_production_scene_duration", json.dumps(receipt))

