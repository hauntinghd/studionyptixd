from __future__ import annotations

from pathlib import Path

from skeleton_ai import voice_fal
from zerotier_private import pipeline


def test_zerotier_narration_uses_shared_fal_voice_adapter(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[dict] = []

    def fake_synthesize(**kwargs) -> Path:
        calls.append(kwargs)
        destination = Path(kwargs["out_path"])
        destination.write_bytes(b"fal-audio")
        return destination

    class NoNetwork:
        @staticmethod
        def subscribe(*_args, **_kwargs):
            raise AssertionError("ZeroTier attempted direct provider I/O")

    monkeypatch.setenv("ELEVENLABS_API_KEY", "stale-retired-key")
    monkeypatch.setattr(voice_fal, "synthesize", fake_synthesize)
    monkeypatch.setattr(pipeline, "fal_client", NoNetwork())

    result = pipeline._gen_vo(
        [{"narration": "First beat."}, {"narration": "Second beat."}],
        tmp_path,
    )

    assert result == tmp_path / "narration.mp3"
    assert calls == [
        {
            "text": "First beat. Second beat.",
            "out_path": tmp_path / "narration.mp3",
            "voice_id": voice_fal.DEFAULT_VOICE,
            "speed": 0.95,
        }
    ]


def test_tier_p_migrates_legacy_voice_selection_to_fal_without_provider_io(
    monkeypatch,
    tmp_path: Path,
) -> None:
    panels_dir = tmp_path / "panels"
    panels_dir.mkdir()
    (panels_dir / "panel.png").write_bytes(b"panel" * 300)
    narration_calls: list[list[dict]] = []

    def fake_clip(_scene: dict, _still: Path, clips_dir: Path) -> Path:
        result = clips_dir / "scene.mp4"
        result.write_bytes(b"clip")
        return result

    def fake_narration(scenes: list[dict], vo_dir: Path) -> Path:
        narration_calls.append(scenes)
        result = vo_dir / "narration.mp3"
        result.write_bytes(b"audio")
        return result

    def fake_compose(
        _scenes: list[dict],
        _clips: list[Path],
        _vo: Path,
        _workspace: Path,
        out_path: Path,
    ) -> Path:
        out_path.write_bytes(b"video")
        return out_path

    monkeypatch.setattr(pipeline, "_gen_clip_ken_burns", fake_clip)
    monkeypatch.setattr(pipeline, "_gen_vo", fake_narration)
    monkeypatch.setattr(pipeline, "_compose", fake_compose)
    monkeypatch.setattr(pipeline, "_probe_duration_sec", lambda _path: 4.0)

    result = pipeline.render_comic_panel_short(
        script_json={
            "title": "Policy test",
            "scenes": [
                {
                    "narration": "A provider-free narration test.",
                    "text_overlay": "POLICY",
                    "duration_sec": 4,
                    "panel_image": "panel.png",
                }
            ],
        },
        workspace=tmp_path,
        vo_provider="elevenlabs",
    )

    assert len(narration_calls) == 1
    assert result["voice_provider"] == "fal_minimax"
    assert result["voice_provider_migrated"] is True
    assert result["fal_cost_estimate_usd"] == 0.10


def test_zerotier_runtime_has_no_retired_tts_import_or_secret() -> None:
    source = Path(pipeline.__file__).read_text(encoding="utf-8").lower()
    assert "voice_elevenlabs" not in source
    assert "elevenlabs_api_key" not in source
