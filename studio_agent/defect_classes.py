"""Split QA verdicts into defects a retry can fix and defects it cannot.

Regeneration re-rolls the seed. That helps when the defect came from *this*
draw - an unlucky limb, a bad framing, a continuity break. It does nothing when
the model reproduces the defect on essentially every draw, which is the case for
skull detail, eye consistency, glass-shell refraction, and hand topology on the
skeleton subject. Frame inspection of a finished render found those four in
effectively every frame that showed them.

Retrying a structural defect is the most expensive possible no-op: it costs a
full clip or still, produces the same defect, and can loop. One short reached
$18.71 this way. So structural verdicts stop and surface to a human with the
frame attached, and spend nothing.

Mixed verdicts resolve to STRUCTURAL. A retry cannot succeed while a structural
issue is present, so paying for one because a fixable issue rode alongside it is
the same waste with extra steps.
"""
from __future__ import annotations

from typing import Any

RESEED_FIXABLE = "reseed_fixable"
STRUCTURAL = "structural"

#: Defects the model reproduces on essentially every draw. Retrying is a no-op
#: that costs money, so these fail to a human instead.
STRUCTURAL_ISSUES: frozenset[str] = frozenset({
    "skull_detail_failure",
    "eye_consistency_failure",
    "hand_topology_failure",
    "glass_shell_failure",
    "material_artifact",
})

#: Defects that vary between draws, so a fresh seed and staging can resolve them.
RESEED_FIXABLE_ISSUES: frozenset[str] = frozenset({
    "anatomy_artifact",          # one-off limb glitch; topology has its own class
    "composition_failure",
    "layout_artifact",
    "crop_artifact",
    "seam_artifact",
    "background_artifact",
    "prop_or_background_drift",
    "symbolic_clutter",
    "wardrobe_drift",
    "identity_drift",
    "human_or_skin",
    "text_artifact",
    "cast_integrity_failure",
    "style_violation",
    "anachronism",
    "artifact",
    "style_match",
})


def classify_issue(issue: str) -> str:
    """Classify one issue name. Unrecognised issues are treated as structural.

    Failing closed here fails toward *not spending*: an unknown defect surfaces
    to a human rather than silently authorising a paid retry that may not fix
    anything. New QA fields must be classified deliberately, not by default.
    """
    name = str(issue or "").strip().lower()
    if not name:
        return STRUCTURAL
    if name in RESEED_FIXABLE_ISSUES:
        return RESEED_FIXABLE
    return STRUCTURAL


def classify_report(report: dict[str, Any] | None) -> dict[str, Any]:
    """Decide whether a QA verdict may trigger a paid repair.

    Returns the defect class, the issues behind it, and ``repair_allowed`` -
    the single boolean every repair path must consult before spending.
    """
    data = dict(report or {})
    issues = [str(value) for value in (data.get("issues") or []) if str(value).strip()]

    # A passing verdict is not a defect at all.
    if data.get("pass") is True and not issues:
        return {
            "defect_class": "",
            "repair_allowed": False,
            "structural_issues": [],
            "reseed_issues": [],
            "reason": "QA passed; nothing to repair",
        }

    # QA that could not run is not evidence a retry would help.
    if "qa_unavailable" in issues:
        return {
            "defect_class": STRUCTURAL,
            "repair_allowed": False,
            "structural_issues": ["qa_unavailable"],
            "reseed_issues": [],
            "reason": "QA did not run; a paid retry would be guesswork",
        }

    structural = [i for i in issues if classify_issue(i) == STRUCTURAL]
    reseed = [i for i in issues if classify_issue(i) == RESEED_FIXABLE]

    if not issues:
        # Failed without naming an issue: no basis for believing a reroll helps.
        return {
            "defect_class": STRUCTURAL,
            "repair_allowed": False,
            "structural_issues": [],
            "reseed_issues": [],
            "reason": "QA failed without naming an issue; cannot justify paid retry",
        }

    if structural:
        return {
            "defect_class": STRUCTURAL,
            "repair_allowed": False,
            "structural_issues": structural,
            "reseed_issues": reseed,
            "reason": (
                "Model reproduces these on every draw; a retry costs money and "
                "returns the same defect: " + ", ".join(structural)
            ),
        }

    return {
        "defect_class": RESEED_FIXABLE,
        "repair_allowed": True,
        "structural_issues": [],
        "reseed_issues": reseed,
        "reason": "Seed-dependent defect; a fresh draw can resolve it",
    }


def repair_is_allowed(report: dict[str, Any] | None) -> bool:
    """True when a QA verdict justifies spending money on a retry."""
    return bool(classify_report(report).get("repair_allowed"))


def structural_hold_report(
    *,
    scene_index: int,
    qa_report: dict[str, Any] | None,
    frame_path: str = "",
) -> dict[str, Any]:
    """The record written instead of a repair when a defect is structural.

    Carries the frame so a human reviews the actual image rather than a verdict
    string, and states the spend explicitly so "we held" is never confused with
    "we retried and it worked".
    """
    verdict = classify_report(qa_report)
    return {
        "scene_index": int(scene_index),
        "status": "structural_hold",
        "failure_stage": "still",
        "defect_class": STRUCTURAL,
        "structural_issues": verdict.get("structural_issues") or [],
        "reseed_issues": verdict.get("reseed_issues") or [],
        "reason": verdict.get("reason") or "",
        "frame": str(frame_path or ""),
        "retry_spend_usd": 0.0,
        "needs_human_review": True,
        "qa": dict(qa_report or {}),
    }
