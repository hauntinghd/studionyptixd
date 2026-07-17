from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from studio_agent import caption_alignment


def test_parse_word_chunks_supports_provider_timestamp_shapes_and_splits_text() -> None:
    words = caption_alignment.parse_word_chunks(
        {
            "chunks": [
                {"text": "Hello world", "timestamp": [0, 1.0]},
                {"word": "again", "start": "1.1", "end": "1.6"},
            ]
        }
    )

    assert words == [
        {"text": "Hello", "start": 0.0, "end": 0.5},
        {"text": "world", "start": 0.5, "end": 1.0},
        {"text": "again", "start": 1.1, "end": 1.6},
    ]


def test_validate_word_timings_rejects_non_monotonic_and_out_of_bounds_cues() -> None:
    with pytest.raises(caption_alignment.CaptionAlignmentError, match="non-monotonic"):
        caption_alignment.validate_word_timings(
            [
                {"text": "first", "start": 0.0, "end": 1.0},
                {"text": "second", "start": 0.5, "end": 1.2},
            ]
        )

    with pytest.raises(caption_alignment.CaptionAlignmentError, match="final audio clock"):
        caption_alignment.validate_word_timings(
            [{"text": "late", "start": 1.0, "end": 2.0}],
            duration_sec=1.5,
        )


def test_phrase_grouping_uses_punctuation_gap_and_three_word_ceiling() -> None:
    words = [
        {"text": "This", "start": 0.0, "end": 0.2},
        {"text": "works,", "start": 0.2, "end": 0.4},
        {"text": "then", "start": 1.0, "end": 1.2},
        {"text": "groups", "start": 1.2, "end": 1.4},
        {"text": "three", "start": 1.4, "end": 1.6},
    ]

    assert caption_alignment.group_word_cues(words, mode="phrase") == [
        {"text": "This works,", "start": 0.0, "end": 0.4},
        {"text": "then groups three", "start": 1.0, "end": 1.6},
    ]
    assert caption_alignment.group_word_cues(words, mode="word") == words


def test_write_ass_uses_requested_canvas_font_and_sanitizes_dialogue(tmp_path: Path) -> None:
    output = caption_alignment.write_ass(
        [{"text": "Use {safe}\\text\nnow", "start": 0.25, "end": 1.75}],
        tmp_path / "captions" / "final.ass",
        width=1080,
        height=1920,
        font_name="Noto Sans",
    )
    rendered = output.read_text(encoding="utf-8")

    assert "PlayResX: 1080" in rendered
    assert "PlayResY: 1920" in rendered
    assert "Style: Studio,Noto Sans,58" in rendered
    assert "Dialogue: 0,0:00:00.25,0:00:01.75" in rendered
    assert "USE (SAFE)TEXT NOW" in rendered
    assert "{safe}" not in rendered


def test_alignment_cache_is_content_addressed_and_never_contacts_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audio = tmp_path / "narration.wav"
    audio.write_bytes(b"verified-audio" * 100)
    words = [{"text": "cached", "start": 0.0, "end": 0.4}]
    cache = tmp_path / "alignment.json"
    cache.write_text(
        json.dumps(
            {
                "version": 1,
                "audio_sha256": hashlib.sha256(audio.read_bytes()).hexdigest(),
                "words": words,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("FAL_AI_KEY", raising=False)
    monkeypatch.delenv("FAL_KEY", raising=False)

    class ForbiddenClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("valid caption cache contacted the provider")

    monkeypatch.setattr(caption_alignment.httpx, "Client", ForbiddenClient)

    assert caption_alignment.align_audio_words(audio, cache_path=cache) == words


def test_stale_alignment_cache_fails_closed_without_provider_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audio = tmp_path / "narration.wav"
    audio.write_bytes(b"current-audio" * 100)
    cache = tmp_path / "alignment.json"
    cache.write_text(
        json.dumps(
            {
                "version": 1,
                "audio_sha256": "stale",
                "words": [{"text": "old", "start": 0.0, "end": 0.4}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("FAL_AI_KEY", raising=False)
    monkeypatch.delenv("FAL_KEY", raising=False)

    with pytest.raises(caption_alignment.CaptionAlignmentError, match="FAL_AI_KEY"):
        caption_alignment.align_audio_words(audio, cache_path=cache)
