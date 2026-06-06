"""Render one of each card type as a 5-second 1080p60 MP4 so we can
eyeball the look against Lume's Candice McCoy episode (2026-05-16).

Usage:
    python -m long_form.motion_graphics.demo [output_dir] [--ffmpeg PATH]

If ffmpeg isn't on PATH (Windows), pass --ffmpeg with the full exe path,
e.g.:
    python -m long_form.motion_graphics.demo D:/tmp/mg_demo \\
        --ffmpeg "C:/Users/casey/AppData/Local/Microsoft/WinGet/Links/ffmpeg.EXE"
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

from long_form.motion_graphics import (
    PercentageCard,
    NewsCard,
    TimelineCard,
    CounterCard,
)


def _resolve_ffmpeg(passed: str | None) -> str:
    if passed:
        return passed
    if shutil.which("ffmpeg"):
        return "ffmpeg"
    # Common Windows paths
    for p in [
        r"C:/Users/casey/AppData/Local/Microsoft/WinGet/Links/ffmpeg.EXE",
        r"C:/ffmpeg/bin/ffmpeg.exe",
    ]:
        if Path(p).exists():
            return p
    print(
        "ERROR: ffmpeg not found. Pass --ffmpeg <path> or install it on PATH.",
        file=sys.stderr,
    )
    sys.exit(1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("output_dir", nargs="?", default="D:/tmp/mg_demo")
    ap.add_argument("--ffmpeg", default=None, help="full path to ffmpeg exe")
    ap.add_argument("--fps", type=int, default=60)
    ap.add_argument("--duration", type=float, default=5.0)
    args = ap.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ff = _resolve_ffmpeg(args.ffmpeg)

    fps = args.fps
    dur = args.duration

    plan = [
        (
            "01_percentage_70pct_erc.mp4",
            PercentageCard(
                percentage=70,
                subtitle="OF ALL ERC CLAIMS",
                body="HAD UNACCEPTABLE RISK",
                source="IRS",
                duration_sec=dur, fps=fps,
            ),
        ),
        (
            "02_news_npr_22m_unemployment.mp4",
            NewsCard(
                publisher="NPR",
                section="ECONOMY",
                headline="22 Millions Americans Have Now Filed For Unemployment In The Last 4 Weeks",
                highlight="22 Millions",
                date_str="APRIL 16, 2020 · 4:18 PM ET",
                show_attribution="ALL THINGS CONSIDERED",
                duration_sec=dur, fps=fps,
            ),
        ),
        (
            "03_timeline_pandemic_ends.mp4",
            TimelineCard(
                years=[2020, 2021, 2022, 2023, 2024, 2025],
                event_year=2021,
                event_label="PANDEMIC ENDS",
                duration_sec=dur, fps=fps,
            ),
        ),
        (
            "04_counter_59b_relief.mp4",
            CounterCard(
                final_value=59.0,
                prefix="$",
                suffix=" B",
                label="THE LARGEST ECONOMIC RELIEF PROGRAM",
                source="U.S. TREASURY",
                duration_sec=dur, fps=fps,
            ),
        ),
        (
            "05_counter_1227_returns.mp4",
            CounterCard(
                final_value=1227,
                prefix="",
                suffix=" RETURNS",
                label="ALL UNDER ONE NAME",
                source="UNITED STATES v. GOODE-MCCOY",
                duration_sec=dur, fps=fps,
            ),
        ),
        (
            "06_percentage_em_red.mp4",
            PercentageCard(
                percentage=114,
                suffix="M",
                subtitle="DRAINED IN 20 MINUTES",
                body="MANGO MARKETS, OCTOBER 11, 2022",
                source="CFTC v. EISENBERG",
                accent_color=(220, 56, 76),  # EM red porcelain accent
                duration_sec=dur, fps=fps,
            ),
        ),
    ]

    print(f"Rendering {len(plan)} cards -> {out}")
    print(f"  ffmpeg: {ff}")
    print(f"  format: 1920x1080 @ {fps}fps, {dur:.1f}s each")
    print()

    total = 0.0
    for name, card in plan:
        path = out / name
        t0 = time.time()
        card.render(path, ffmpeg_exe=ff)
        dt = time.time() - t0
        total += dt
        size_kb = path.stat().st_size // 1024
        print(f"  [OK] {name}  ({dt:.1f}s, {size_kb}kb)")

    print()
    print(f"Total wall-clock: {total:.1f}s for {len(plan)} cards")
    print(f"Output dir: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
