from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace


def test_render_simulation_never_calls_fal(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("STUDIO_RENDER_SIMULATION_MODE", "1")
    monkeypatch.setenv("STUDIO_RENDER_SIMULATION_SLEEP_SCALE", "0")

    import skeleton_ai.canonical_edit as canonical_edit
    import skeleton_ai.i2v_engine as i2v_engine
    import skeleton_ai.styled_stills as styled_stills
    import skeleton_ai.voice_fal as voice_fal

    def fail(*_args, **_kwargs):
        raise AssertionError("FAL must not be called in render simulation mode")

    fal_stub = SimpleNamespace(upload_file=fail, submit=fail, status=fail, result=fail, subscribe=fail)
    canonical_edit.fal_client = canonical_edit.fal_client or fal_stub
    styled_stills.fal_client = styled_stills.fal_client or fal_stub
    i2v_engine.fal_client = i2v_engine.fal_client or fal_stub
    voice_fal.fal_client = voice_fal.fal_client or fal_stub

    monkeypatch.setattr(canonical_edit.fal_client, "upload_file", fail)
    monkeypatch.setattr(canonical_edit.fal_client, "submit", fail)
    monkeypatch.setattr(styled_stills.fal_client, "upload_file", fail)
    monkeypatch.setattr(i2v_engine.fal_client, "upload_file", fail)
    monkeypatch.setattr(i2v_engine.fal_client, "submit", fail)
    monkeypatch.setattr(voice_fal.fal_client, "subscribe", fail)

    still = tmp_path / "still.png"
    canonical_edit.generate_still_edit("test prompt", still)
    assert still.exists()

    styled = tmp_path / "styled.png"
    styled_stills.generate_still_t2i("test prompt", styled, negative_prompt="")
    assert styled.exists()

    clip = tmp_path / "clip.mp4"
    i2v_engine.generate(still, "slow push", clip, duration_sec=1)
    assert clip.exists()
    sidecar = json.loads((clip.with_suffix(clip.suffix + ".fal.json")).read_text(encoding="utf-8"))
    assert sidecar["simulated"] is True

    audio = tmp_path / "voice.mp3"
    voice_fal.synthesize(text="hello world", out_path=audio)
    assert audio.exists()
