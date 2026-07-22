from __future__ import annotations

from pathlib import Path

from ops import render_chat_story
from skeleton_ai import voice_fal


def test_chatstory_segments_use_shared_fal_voice_adapter(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    def fake_synthesize(*, text: str, out_path: Path, voice_id: str, speed: float) -> Path:
        calls.append({"text": text, "voice_id": voice_id, "speed": speed})
        Path(out_path).write_bytes(b"audio")
        return Path(out_path)

    monkeypatch.setattr(voice_fal, "synthesize", fake_synthesize)

    outputs = render_chat_story.synthesize_raw_segments(
        [{"text": "First line"}, {"text": "Second line"}],
        {"voice_id": "retired-provider-voice"},
        tmp_path,
    )

    assert [path.name for path in outputs] == ["msg_01_raw.mp3", "msg_02_raw.mp3"]
    assert [call["voice_id"] for call in calls] == [voice_fal.DEFAULT_VOICE, voice_fal.DEFAULT_VOICE]
    assert all(call["speed"] == 1.0 for call in calls)


def test_mounted_chatstory_renderer_has_no_retired_tts_route() -> None:
    source = Path(render_chat_story.__file__).read_text(encoding="utf-8").lower()
    assert "api.elevenlabs.io" not in source
    assert "elevenlabs_api_key" not in source
    assert "edge_tts" not in source
    assert "tts.api" not in source
    assert "voice_fal.synthesize" in source
