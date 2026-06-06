"""Motion-graphics generators for Lume-style stat cards.

These cards replace AI i2v in the cheap-modality slots of an episode:
big percentage callouts, NPR-style news cards, timeline reveals, and
animated counters. Output is silent MP4 ready to be muxed with VO.

Cost: $0 (pure Pillow + ffmpeg, no model calls).
Throughput: ~2-4 seconds wall-clock per 5-second card on a single core.

Usage:
    from long_form.motion_graphics import (
        PercentageCard, NewsCard, TimelineCard, CounterCard,
    )
    card = PercentageCard(
        percentage=70,
        subtitle="OF ALL ERC CLAIMS",
        body="HAD UNACCEPTABLE RISK",
        source="IRS",
        duration_sec=5.0,
    )
    card.render(Path("scene.mp4"))
"""
from .stat_card import (
    StatCard,
    PercentageCard,
    NewsCard,
    TimelineCard,
    CounterCard,
    ChecklistCard,
    CompareCard,
    SourceProofCard,
    get_font,
    ease_out_quad,
    ease_in_out_cubic,
)

__all__ = [
    "StatCard",
    "PercentageCard",
    "NewsCard",
    "TimelineCard",
    "CounterCard",
    "ChecklistCard",
    "CompareCard",
    "SourceProofCard",
    "get_font",
    "ease_out_quad",
    "ease_in_out_cubic",
]
