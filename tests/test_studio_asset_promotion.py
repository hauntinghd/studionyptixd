from __future__ import annotations

import contextlib
import json
from pathlib import Path
from types import SimpleNamespace

from skeleton_ai import styled_pipeline
from studio_agent import caption_alignment, render_qa, visual_qa, visual_treatment


def _pass_report() -> dict:
    return {
        "status": "pass",
        "pass": True,
        "confidence": 0.99,
        "summary": "accepted",
        "provider": "test-vision",
    }


def _prepare_plan_mocks(monkeypatch, workspace: Path, *, correspondence_passes: bool) -> list[Path]:
    generated: list[Path] = []
    monkeypatch.setattr(
        styled_pipeline,
        "get_category",
        lambda *_args, **_kwargs: {"label": "Test", "system_prompt": "test"},
    )
    monkeypatch.setattr(styled_pipeline, "analyze_script_styled", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(styled_pipeline, "split_script_into_beats", lambda *_args, **_kwargs: ["One beat"])
    monkeypatch.setattr(
        styled_pipeline,
        "derive_beat_visuals_styled",
        lambda *_args, **_kwargs: ("blue jacket", "walk through a rain-lit alley", "slow push"),
    )
    monkeypatch.setattr(
        visual_treatment,
        "choose_visual_treatment",
        lambda *_args, **_kwargs: {"kind": "generated_scene"},
    )
    monkeypatch.setattr(
        styled_pipeline,
        "production_slot",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(styled_pipeline, "_enforce_image_attempt_budget", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        styled_pipeline.production_costs,
        "price_fal_image",
        lambda **_kwargs: (0.04, "test", "seedream_t2i"),
    )
    monkeypatch.setattr(styled_pipeline.production_costs, "record_event", lambda *_args, **_kwargs: None)

    def fake_generate(_prompt, output, **_kwargs):
        output = Path(output)
        generated.append(output)
        assert output.parent == workspace / "candidate_stills"
        assert not (workspace / "stills" / "b00.png").exists()
        output.write_bytes(b"candidate-still" * 200)
        return {"provider": "seedream_edit"}

    monkeypatch.setattr(styled_pipeline, "generate_still_t2i", fake_generate)

    def audit_still(path, **_kwargs):
        assert Path(path).parent == workspace / "candidate_stills"
        assert not (workspace / "stills" / "b00.png").exists()
        return _pass_report()

    monkeypatch.setattr(visual_qa, "audit_generic_still", audit_still)
    monkeypatch.setattr(
        visual_qa,
        "audit_scene_correspondence",
        lambda *_args, **_kwargs: (
            _pass_report()
            if correspondence_passes
            else {
                "status": "fail",
                "pass": False,
                "confidence": 0.99,
                "summary": "wrong story beat",
                "provider": "test-vision",
            }
        ),
    )
    return generated


def test_initial_still_promotes_only_after_identity_style_and_correspondence_qa(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "job"
    generated = _prepare_plan_mocks(monkeypatch, workspace, correspondence_passes=True)

    styled_pipeline.plan_scenes(
        "test",
        "topic",
        workspace,
        render_style="cinematic",
        beats_target=1,
        grok=SimpleNamespace(),
        script_override="One beat.",
    )

    canonical = workspace / "stills" / "b00.png"
    scene = styled_pipeline.load_scenes(workspace)[0]
    assert generated and generated[0] != canonical
    assert canonical.read_bytes() == b"candidate-still" * 200
    assert scene["still_rel"] == "stills/b00.png"
    assert scene["still_candidate_rel"] is None
    assert scene["visual_qa"]["pass"] is True


def test_rejected_initial_still_never_receives_canonical_path_or_preview(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "job"
    _prepare_plan_mocks(monkeypatch, workspace, correspondence_passes=False)

    styled_pipeline.plan_scenes(
        "test",
        "topic",
        workspace,
        render_style="cinematic",
        beats_target=1,
        grok=SimpleNamespace(),
        script_override="One beat.",
    )

    scene = styled_pipeline.load_scenes(workspace)[0]
    assert not (workspace / "stills" / "b00.png").exists()
    assert scene["still_rel"] is None
    assert scene["still_candidate_rel"] is None
    assert scene["status"] == "qa_blocked"
    assert list((workspace / "rejected_stills").glob("*.png"))


def _animation_workspace(tmp_path: Path, *, existing_clip: bool) -> tuple[Path, Path]:
    workspace = tmp_path / "job"
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir(parents=True)
    still = workspace / "stills" / "b00.png"
    still.write_bytes(b"accepted-still" * 200)
    clip = workspace / "clips" / "b00.mp4"
    if existing_clip:
        clip.write_bytes(b"accepted-old-clip" * 200)
    (workspace / "job_spec.json").write_text(
        json.dumps({"render_style": "cinematic"}),
        encoding="utf-8",
    )
    styled_pipeline.save_scenes(
        workspace,
        [
            {
                "index": 0,
                "sid": "b00",
                "narration": "line",
                "prompt": "cinematic alley",
                "scene_action": "walk",
                "motion_prompt": "move " * 200,
                "still_rel": "stills/b00.png",
                "clip_rel": "clips/b00.mp4" if existing_clip else None,
                "animate": True,
                "approved_for_video": True,
                "status": "clip_ready" if existing_clip else "still_ready",
                "video_model": "seedance",
                "visual_treatment": {"kind": "generated_scene"},
                "duration_sec": 5,
            }
        ],
    )
    return workspace, clip


def test_clip_candidate_is_hidden_until_clip_qa_passes(monkeypatch, tmp_path: Path) -> None:
    workspace, canonical = _animation_workspace(tmp_path, existing_clip=False)
    dispatched: list[tuple[Path, str]] = []
    monkeypatch.setattr(
        styled_pipeline,
        "production_slot",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(styled_pipeline, "_record_animation_attempt_cost", lambda *_args, **_kwargs: None)

    def fake_clip(_still, motion, output, **_kwargs):
        output = Path(output)
        dispatched.append((output, motion))
        assert output.parent == workspace / "candidate_clips"
        assert not canonical.exists()
        output.write_bytes(b"accepted-new-clip" * 200)
        output.with_suffix(".mp4.fal.json").write_text(
            json.dumps({"endpoint": "fal:test", "duration_sec": 5, "video_model": "seedance"}),
            encoding="utf-8",
        )

    monkeypatch.setattr(styled_pipeline, "gen_clip", fake_clip)

    def audit_clip(path, **_kwargs):
        assert Path(path).parent == workspace / "candidate_clips"
        assert not canonical.exists()
        return _pass_report()

    monkeypatch.setattr(visual_qa, "audit_generic_clip", audit_clip)

    result = styled_pipeline.animate_scenes_stage(workspace, indices=[0])
    scene = styled_pipeline.load_scenes(workspace)[0]
    assert result["failed"] == []
    assert canonical.read_bytes() == b"accepted-new-clip" * 200
    assert dispatched and len(dispatched[0][1]) <= 300
    assert len(scene["motion_prompt"]) <= 300
    assert scene["clip_rel"] == "clips/b00.mp4"


def test_failed_clip_candidate_is_quarantined_and_prior_clip_survives(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace, canonical = _animation_workspace(tmp_path, existing_clip=True)
    old_bytes = canonical.read_bytes()
    monkeypatch.setattr(
        styled_pipeline,
        "production_slot",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(styled_pipeline, "_record_animation_attempt_cost", lambda *_args, **_kwargs: None)

    def fake_clip(_still, _motion, output, **_kwargs):
        output = Path(output)
        assert canonical.read_bytes() == old_bytes
        output.write_bytes(b"rejected-new-clip" * 200)
        output.with_suffix(".mp4.fal.json").write_text(
            json.dumps({"endpoint": "fal:test", "duration_sec": 5, "video_model": "seedance"}),
            encoding="utf-8",
        )

    monkeypatch.setattr(styled_pipeline, "gen_clip", fake_clip)
    monkeypatch.setattr(
        visual_qa,
        "audit_generic_clip",
        lambda *_args, **_kwargs: {
            "status": "fail",
            "pass": False,
            "confidence": 0.0,
            "summary": "clip QA unavailable",
            "provider": "",
        },
    )

    result = styled_pipeline.animate_scenes_stage(workspace, indices=[0])
    scene = styled_pipeline.load_scenes(workspace)[0]
    assert result["failed"] == [0]
    assert canonical.read_bytes() == old_bytes
    assert scene["clip_rel"] == "clips/b00.mp4"
    assert list((workspace / "rejected_clips").glob("*.mp4"))
    assert not list((workspace / "candidate_clips").glob("*.mp4"))


def test_motion_graphic_candidate_requires_full_clip_qa_before_promotion(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace, canonical = _animation_workspace(tmp_path, existing_clip=False)
    scenes = styled_pipeline.load_scenes(workspace)
    scenes[0]["visual_treatment"] = {
        "kind": "motion_graphic",
        "graphic_type": "quote_card",
        "headline": "Verified fact",
    }
    styled_pipeline.save_scenes(workspace, scenes)
    observed: list[Path] = []
    monkeypatch.setattr(
        styled_pipeline,
        "production_slot",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )

    def fake_render(_treatment, output, **_kwargs):
        output = Path(output)
        assert output.parent == workspace / "candidate_clips"
        assert not canonical.exists()
        output.write_bytes(b"motion-graphic" * 300)
        return output

    def fake_audit(_workspace, candidate, **_kwargs):
        candidate = Path(candidate)
        observed.append(candidate)
        assert candidate.parent == workspace / "candidate_clips"
        assert not canonical.exists()
        return _pass_report()

    monkeypatch.setattr(visual_treatment, "render_motion_graphic_clip", fake_render)
    monkeypatch.setattr(styled_pipeline, "_audit_clip_candidate", fake_audit)

    result = styled_pipeline.animate_scenes_stage(workspace, indices=[0])

    assert result["failed"] == []
    assert observed
    assert canonical.read_bytes() == b"motion-graphic" * 300


def _prepare_finalize_workspace(monkeypatch, tmp_path: Path) -> tuple[Path, bytes, str]:
    workspace = tmp_path / "final-job"
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir(parents=True)
    (workspace / "stills" / "b00.png").write_bytes(b"accepted-still" * 200)
    prior_video = b"prior-accepted-final" * 200
    prior_package = "Title:\nPrior\n\nDescription:\nPrior\n\nTags:\nprior\n\nHashtags:\n#prior\n"
    (workspace / "styled_short.mp4").write_bytes(prior_video)
    (workspace / "package.txt").write_text(prior_package, encoding="utf-8")
    (workspace / "clips" / "b00.mp4").write_bytes(b"accepted-scene-clip" * 200)
    (workspace / "script.txt").write_text("One final line.", encoding="utf-8")
    (workspace / "job_spec.json").write_text(
        json.dumps({"render_style": "cinematic", "captions_enabled": True}),
        encoding="utf-8",
    )
    (workspace / "result.json").write_text(
        json.dumps({"render_style": "cinematic", "category": "test", "topic": "Topic"}),
        encoding="utf-8",
    )
    styled_pipeline.save_scenes(
        workspace,
        [
            {
                "index": 0,
                "sid": "b00",
                "narration": "One final line.",
                "scene_action": "A clear final beat.",
                "still_rel": "stills/b00.png",
                "clip_rel": "clips/b00.mp4",
                "approved_for_video": True,
                "animate": False,
                "duration_sec": 5,
            }
        ],
    )
    monkeypatch.setattr(
        styled_pipeline,
        "production_slot",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(styled_pipeline, "probe_duration", lambda _path: 5.0)
    def fake_trim(_src, output, **_kwargs):
        output = Path(output)
        output.write_bytes(b"trimmed" * 300)
        return output

    def fake_concat(_paths, output, _work):
        output = Path(output)
        output.write_bytes(b"silent" * 400)
        return output

    def fake_mux(_silent, _audio, output, **_kwargs):
        output = Path(output)
        output.write_bytes(b"candidate-final" * 300)
        return output

    monkeypatch.setattr(styled_pipeline, "trim_with_captions", fake_trim)
    monkeypatch.setattr(styled_pipeline, "concat_demuxer", fake_concat)
    monkeypatch.setattr(styled_pipeline, "mux_narration", fake_mux)
    monkeypatch.setattr(
        caption_alignment,
        "align_audio_words",
        lambda *_args, **_kwargs: [
            {"text": "One", "start": 0.0, "end": 1.0},
            {"text": "final", "start": 1.0, "end": 2.0},
            {"text": "line.", "start": 2.0, "end": 3.0},
        ],
    )

    def fake_run(command, **_kwargs):
        target = Path(command[-1])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"audio" * 400)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(styled_pipeline.subprocess, "run", fake_run)
    monkeypatch.setattr(
        styled_pipeline.production_costs,
        "price_tts",
        lambda *_args, **_kwargs: (0.0, "test", "tts", 1.0, "fal", "character"),
    )
    monkeypatch.setattr(styled_pipeline.production_costs, "record_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(styled_pipeline.production_costs, "load_summary", lambda _workspace: {})
    return workspace, prior_video, prior_package


class _FakeVoice:
    last_provider = "fal"

    def synthesize(self, *, text: str, out_path: Path, voice_id=None) -> Path:
        del text, voice_id
        Path(out_path).write_bytes(b"voice" * 400)
        return Path(out_path)


def test_finalize_promotes_video_and_package_only_after_strict_candidate_qa(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace, _prior_video, _prior_package = _prepare_finalize_workspace(monkeypatch, tmp_path)
    seen: list[tuple[Path, Path]] = []

    def pass_candidate(**kwargs):
        video = Path(kwargs["video_path"])
        package = Path(kwargs["package_path"])
        seen.append((video, package))
        assert ".candidate" in video.name
        assert ".candidate" in package.name
        assert (workspace / "styled_short.mp4").read_bytes().startswith(b"prior-accepted")
        return {"status": "pass", "score": 100, "checks": []}

    monkeypatch.setattr(render_qa, "analyze_render", pass_candidate)
    result = styled_pipeline.finalize_stage(workspace, el=_FakeVoice())

    assert len(seen) == 1
    assert result["status"] == "complete"
    assert result["ready_to_post"] is True
    assert (workspace / "styled_short.mp4").read_bytes() == b"candidate-final" * 300
    assert (workspace / "package.txt").read_text(encoding="utf-8").startswith("Title:")


def test_finalize_quarantines_failed_candidate_and_retains_prior_accepted_final(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace, prior_video, prior_package = _prepare_finalize_workspace(monkeypatch, tmp_path)
    monkeypatch.setattr(
        render_qa,
        "analyze_render",
        lambda **_kwargs: {
            "status": "fail",
            "score": 40,
            "checks": [{"id": "current_scene_qa", "status": "fail"}],
        },
    )

    result = styled_pipeline.finalize_stage(workspace, el=_FakeVoice())

    assert result["status"] == "final_qa_blocked"
    assert result["ready_to_post"] is False
    assert result["final_qa_blocked"] is True
    assert result["video_path"] is None
    assert result["package_path"] is None
    assert (workspace / "styled_short.mp4").read_bytes() == prior_video
    assert (workspace / "package.txt").read_text(encoding="utf-8") == prior_package
    assert list((workspace / "rejected_final").rglob("styled_short.mp4"))
