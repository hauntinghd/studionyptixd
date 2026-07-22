from __future__ import annotations

import asyncio
from pathlib import Path

from long_form import pipeline
from long_form.prompts import channels
from studio_agent import jobs as agent_jobs
from studio_agent import provider_policy, render_qa


def test_mocked_longform_runs_from_outline_through_strict_package_promotion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Exercise the real long-form lifecycle without provider or ffmpeg spend."""

    monkeypatch.setattr(pipeline, "LF_OUTPUT_ROOT", tmp_path)
    pipeline._lf_jobs_status.clear()
    calls: list[str] = []

    channel = {
        "key": "mocked_release_channel",
        "label": "Mocked Release Channel",
        "pipeline_kind": "sleep_doc",
        "visual_style": "cinematic historical documentary",
        "image_model_default": "seedream_edit",
        "video_model_default": "seedance",
        "voice_provider": "fal_minimax",
        "voice_id_default": "English_Trustworthy_Man",
        "fps": 30,
    }
    outline = {
        "title": "The Mocked Release Story",
        "topic": "A no-cost lifecycle verification",
        "visual_proof_only": False,
        "image_model_id": "seedream_edit",
        "video_model": "seedance",
        "background_music": "off",
        "sfx_enabled": False,
        "chapters": [{"title": "Opening"}],
    }

    monkeypatch.setattr(channels, "get_channel", lambda _key: dict(channel))
    monkeypatch.setattr(pipeline, "_longform_llm_client", lambda *_args, **_kwargs: object())

    def fake_chapter(_client, **kwargs):
        calls.append("anthropic_chapter")
        return {
            "chapter_index": int(kwargs["chapter_index"]),
            "title": "Opening",
            "narration": "A complete mocked narration with a hook and a payoff.",
            "scene_prompts": ["A specific cinematic opening scene with one subject."],
        }

    def fake_scenes(chapters_payload, stills_dir: Path, _image_model: str, **kwargs):
        calls.append("fal_still")
        stills_dir.mkdir(parents=True, exist_ok=True)
        outputs: list[Path] = []
        for index, _prompt in enumerate(chapters_payload[0]["scene_prompts"]):
            out = stills_dir / f"scene_{index:04d}.png"
            out.write_bytes(b"mock-still" * 600)
            outputs.append(out)
        progress = kwargs.get("on_progress")
        if progress:
            progress(len(outputs), len(outputs))
        return outputs

    def fake_voice(_text: str, out: Path, **_kwargs) -> Path:
        calls.append("fal_tts")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"mock-chapter-audio" * 400)
        return out

    def fake_concat(_parts: list[Path], out: Path) -> Path:
        calls.append("audio_concat")
        out.write_bytes(b"mock-full-narration" * 600)
        return out

    def fake_ambient(out: Path, **_kwargs) -> Path:
        calls.append("fal_ambient")
        out.write_bytes(b"mock-ambient" * 200)
        return out

    def fake_thumbnails(_channel, _outline, out_dir: Path, count: int = 3):
        calls.append("fal_thumbnails")
        out_dir.mkdir(parents=True, exist_ok=True)
        outputs = []
        for index in range(1, count + 1):
            out = out_dir / f"thumb_{index}.png"
            out.write_bytes(b"mock-thumbnail" * 300)
            outputs.append(out)
        return outputs

    def fake_compose(_stills, _narration, _ambient, out: Path, **_kwargs) -> Path:
        calls.append("compose")
        out.write_bytes(b"mock-final-video" * 800)
        return out

    def fake_package(job_id: str) -> Path:
        calls.append("package")
        out = pipeline._job_dir(job_id) / "package.txt"
        out.write_text("Title\nDescription\nTags\n", encoding="utf-8")
        return out

    def strict_render_pass(**_kwargs):
        calls.append("strict_render_qa")
        return {
            "status": "pass",
            "checks": [
                {"id": "media", "status": "pass"},
                {"id": "package", "status": "pass"},
            ],
        }

    monkeypatch.setattr(pipeline, "_gen_chapter", fake_chapter)
    monkeypatch.setattr(pipeline, "_gen_scenes_batch", fake_scenes)
    monkeypatch.setattr(pipeline, "_gen_minimax_chapter", fake_voice)
    monkeypatch.setattr(pipeline, "_ffmpeg_concat_audio", fake_concat)
    monkeypatch.setattr(pipeline, "_gen_ambient", fake_ambient)
    monkeypatch.setattr(pipeline, "_gen_thumbnails", fake_thumbnails)
    monkeypatch.setattr(pipeline, "_compose_slideshow", fake_compose)
    monkeypatch.setattr(pipeline, "_ffprobe_dur", lambda _path: 10.0)
    monkeypatch.setattr(
        pipeline,
        "_audit_current_longform_assets",
        lambda _job_id, *, require_clips: {
            "status": "pass",
            "pass": True,
            "require_clips": require_clips,
            "scenes": [{"index": 0, "status": "pass"}],
            "failures": [],
        },
    )
    monkeypatch.setattr(agent_jobs, "_build_longform_package", fake_package)
    monkeypatch.setattr(render_qa, "analyze_render", strict_render_pass)
    monkeypatch.setattr(
        pipeline,
        "_fal_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unmocked paid FAL request")
        ),
    )

    job_id = "mocked-longform-e2e"
    asyncio.run(
        pipeline.run_sleep_doc_pipeline(
            job_id,
            channel,
            outline,
            scenes_per_chapter=1,
            wpm=120,
        )
    )
    awaiting = pipeline.load_state(job_id) or {}
    assert awaiting["phase"] == "awaiting_approval"
    assert awaiting.get("ready_to_post") is not True
    assert "mp4_path" not in awaiting

    asyncio.run(pipeline.finalize_sleep_doc_pipeline(job_id))

    finished = pipeline.load_state(job_id) or {}
    assert finished["phase"] == "done"
    assert finished["ready_to_post"] is True
    assert finished["provider_policy_version"] == provider_policy.POLICY_VERSION
    assert finished["final_qa"]["status"] == "pass"
    assert finished["final_qa"]["current_assets"]["pass"] is True
    assert (tmp_path / finished["mp4_path"]).is_file()
    assert (tmp_path / finished["package_path"]).is_file()
    assert calls == [
        "anthropic_chapter",
        "fal_still",
        "fal_tts",
        "audio_concat",
        "fal_ambient",
        "fal_thumbnails",
        "compose",
        "package",
        "strict_render_qa",
    ]

