from __future__ import annotations

import tempfile
from pathlib import Path

from studio_agent import render_qa


def _write_package(path: Path, *, shortform: bool = True) -> None:
    extra = "Hashtags:\n#shorts\n" if shortform else "Timestamps:\n00:00 - Open\n\nThumbnail:\nConcept\n"
    path.write_text(
        "Title:\nTest\n\nDescription:\nTest description\n\nTags:\ntest, video\n\n" + extra,
        encoding="utf-8",
    )


def test_shortform_passes_with_vertical_video_and_package() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        video = root / "styled_short.mp4"
        package = root / "package.txt"
        video.write_bytes(b"0" * 1_000_000)
        _write_package(package, shortform=True)
        original_probe = render_qa._probe_video
        render_qa._probe_video = lambda _path: {
            "ok": True,
            "duration": 33,
            "width": 1080,
            "height": 1920,
            "has_audio": True,
        }
        try:
            report = render_qa.analyze_render(
                job_id="job123",
                kind="shortform",
                video_path=video,
                package_path=package,
            )
        finally:
            render_qa._probe_video = original_probe
        assert report["status"] == "pass"
        assert report["score"] == 100
        assert (root / "render_qa.json").is_file()


def test_shortform_fails_bad_aspect_and_missing_audio() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        video = root / "styled_short.mp4"
        package = root / "package.txt"
        video.write_bytes(b"0" * 1_000_000)
        _write_package(package, shortform=True)
        original_probe = render_qa._probe_video
        render_qa._probe_video = lambda _path: {
            "ok": True,
            "duration": 33,
            "width": 1920,
            "height": 1080,
            "has_audio": False,
        }
        try:
            report = render_qa.analyze_render(
                job_id="job123",
                kind="shortform",
                video_path=video,
                package_path=package,
            )
        finally:
            render_qa._probe_video = original_probe
        assert report["status"] == "fail"
        failed_ids = {c["id"] for c in report["checks"] if c["status"] == "fail"}
        assert "aspect_ratio" in failed_ids
        assert "audio_stream" in failed_ids


def test_probe_unavailable_warns_instead_of_crashing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        video = root / "styled_short.mp4"
        package = root / "package.txt"
        video.write_bytes(b"0" * 1_000_000)
        _write_package(package, shortform=True)
        original_probe = render_qa._probe_video
        render_qa._probe_video = lambda _path: {"ok": False, "error": "ffprobe missing"}
        try:
            report = render_qa.analyze_render(
                job_id="job123",
                kind="shortform",
                video_path=video,
                package_path=package,
            )
        finally:
            render_qa._probe_video = original_probe
        assert report["status"] == "warn"
        assert any(c["id"] == "video_probe" and c["status"] == "warn" for c in report["checks"])


def run_all() -> None:
    test_shortform_passes_with_vertical_video_and_package()
    test_shortform_fails_bad_aspect_and_missing_audio()
    test_probe_unavailable_warns_instead_of_crashing()


if __name__ == "__main__":
    run_all()
    print("3 render QA tests passed")
