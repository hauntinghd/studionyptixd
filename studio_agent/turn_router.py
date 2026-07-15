"""Small, authoritative routing contract for Plan-mode turns.

Plan mode deliberately has only three actionable outcomes.  It must not leak
into the production state machine simply because a request mentions an asset.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


PlanAction = Literal["conversation", "thumbnail_create", "thumbnail_revise"]


@dataclass(frozen=True)
class PlanTurn:
    action: PlanAction
    thumbnail_count: int = 0
    feedback: str = ""


def route_plan_turn(text: str, *, has_thumbnail_review: bool = False) -> PlanTurn:
    """Route one Plan-mode message once, before any model or recovery path.

    A thumbnail *discussion* remains conversation.  A direct make/show request
    creates candidates.  A direct fix/edit request revises the current review.
    """
    normalized = re.sub(r"\s+", " ", str(text or "").lower()).strip()
    if "thumbnail" not in normalized:
        return PlanTurn("conversation")

    revise = bool(re.search(r"\b(?:fix|edit|revise|change|adjust|improve|redo)\b", normalized))
    if has_thumbnail_review and not revise:
        # Critique of the active review card is revision intent even without an
        # imperative verb: "these don't properly match my channel", "nowhere
        # close to what I use", "check my actual channel's thumbnails".
        revise = bool(
            re.search(
                r"\b(?:don'?t|doesn'?t|do not|does not|not|never)\s+(?:properly\s+|really\s+|even\s+)?"
                r"(?:match|fit|look|resemble)\b"
                r"|\bnothing like\b|\bnowhere (?:close|near)\b|\boff[- ]brand\b"
                r"|\bwrong (?:style|look|vibe|direction)\b|\btoo (?:generic|busy|cluttered)\b"
                r"|\bmy (?:actual |real |own )?[\w ]{0,24}channel\b",
                normalized,
            )
        )
    if has_thumbnail_review and revise:
        return PlanTurn("thumbnail_revise", feedback=str(text or "").strip())

    create = bool(re.search(
        r"\b(?:make|create|generate|preview|show|give me|build|design|produce|render)\b",
        normalized,
    ))
    if not create:
        return PlanTurn("conversation")
    if re.search(r"\b(?:three|3)\b", normalized):
        return PlanTurn("thumbnail_create", thumbnail_count=3)
    if re.search(r"\b(?:two|2)\b", normalized):
        return PlanTurn("thumbnail_create", thumbnail_count=2)
    return PlanTurn("thumbnail_create", thumbnail_count=1)
