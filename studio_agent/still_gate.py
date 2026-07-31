"""Reject a bad reference still before any animation money is spent.

Animation is 90.7% of a short's cost. The reference still it animates is 0.3%.
When the reference carries a defect, every clip inherits and amplifies it, and
no downstream QA can recover the video - the money is already gone.

Frame inspection of a finished render found featureless skulls, mismatched eyes,
thumbless hands and stray scratch lines, all present in the reference stills
before i2v ran. This gate is the cheapest possible place to catch that: one
still costs $0.04, the twelve clips it seeds cost $5.88.

The gate deliberately blocks on *structural* defects only. A seed-dependent
defect is the repair path's job (see defect_classes); a structural one means the
reference is unusable and animating it just buys twelve copies of the same
problem.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from studio_agent import defect_classes

#: Defects that make a reference unusable. Identical to the structural set,
#: because "the model reproduces this every draw" is exactly the property that
#: makes a reference not worth animating.
BLOCKING_ISSUES = defect_classes.STRUCTURAL_ISSUES


def evaluate_reference_still(
    qa_report: dict[str, Any] | None, *, still_path: Path | str = ""
) -> dict[str, Any]:
    """Decide whether a reference still may proceed to animation.

    Returns a verdict with ``animation_allowed``. A still that no QA looked at
    does not pass - unknown is not a pass here for the same reason it is not one
    in the release bar: it would wave through exactly the references least
    examined.
    """
    report = dict(qa_report or {})
    issues = [str(v) for v in (report.get("issues") or []) if str(v).strip()]

    if not report:
        return _verdict(
            False, still_path, [], "No QA report for the reference still; refusing to animate"
        )

    if "qa_unavailable" in issues:
        return _verdict(
            False, still_path, ["qa_unavailable"],
            "Reference QA did not run; refusing to spend animation budget on an unchecked still",
        )

    blocking = sorted({i for i in issues if i in BLOCKING_ISSUES})
    if blocking:
        return _verdict(
            False, still_path, blocking,
            "Reference carries defects the model reproduces on every draw; "
            "animating it would buy one copy per beat: " + ", ".join(blocking),
        )

    if report.get("pass") is True:
        return _verdict(True, still_path, [], "Reference still passed QA")

    # Failed, but only on seed-dependent grounds: the still is not good, yet it
    # is the repair path's problem, not a reason to block the whole render.
    return _verdict(
        True, still_path, [],
        "Reference has only seed-dependent issues; repair path owns it",
        seed_issues=sorted(set(issues)),
    )


def _verdict(
    allowed: bool,
    still_path: Path | str,
    blocking: list[str],
    reason: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "animation_allowed": bool(allowed),
        "blocking_issues": list(blocking),
        "reason": reason,
        "still": str(still_path or ""),
        "animation_spend_usd": 0.0 if not allowed else None,
        **extra,
    }


class ReferenceStillRejected(RuntimeError):
    """Raised when a reference still is too damaged to animate."""

    def __init__(self, verdict: dict[str, Any]) -> None:
        super().__init__(str(verdict.get("reason") or "reference still rejected"))
        self.verdict = dict(verdict)


def require_animatable_reference(
    qa_report: dict[str, Any] | None, *, still_path: Path | str = ""
) -> dict[str, Any]:
    """Gate call: raise rather than let a rejected reference reach animation."""
    verdict = evaluate_reference_still(qa_report, still_path=still_path)
    if not verdict["animation_allowed"]:
        raise ReferenceStillRejected(verdict)
    return verdict
