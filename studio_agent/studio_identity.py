"""Verified Studio product identity and promotion policy.

Keep product claims here factual and release-safe. Both the conversational agent
and deterministic upload-package builders consume this module so they cannot
drift into different descriptions of Studio.
"""

from __future__ import annotations

from typing import Any, Mapping


PRODUCT_NAME = "NYPTID Studio"
PRODUCT_URL = "https://studio.nyptidindustries.com"
PRODUCT_POSITIONING = (
    "an AI content-production platform for planning, creating, reviewing, and "
    "packaging short-form and long-form video"
)

STUDIO_IDENTITY_PROMPT = f"""
NYPTID STUDIO PRODUCT IDENTITY (verified product grounding):
- Product: {PRODUCT_NAME}
- Official URL: {PRODUCT_URL}
- Positioning: {PRODUCT_POSITIONING}.
- Current workflow facts: Studio supports conversational planning, explicit production,
  staged scene review, video packaging, short-form production, and long-form production.
- Never invent pricing, guarantees, customer counts, view results, release status, or a
  capability not proven by a tool result or explicitly supplied by the user.
- The user may plan videos about Studio itself. Treat real Studio screenshots or recordings
  as canonical product references: preserve the exact layout, wording, colors, proportions,
  and visible state. Do not hallucinate buttons, results, screens, or features.
- For a Studio walkthrough/demo, plan the real sequence first (user action -> visible Studio
  response -> generated result). Prefer real screen capture for readable UI. AI-generated
  inserts may extend or animate a locked screenshot, but must not replace factual UI evidence.
- In Plan mode, questions about promoting Studio are planning questions, not authorization
  to render. Discuss placement, tone, CTA, and channel fit, then carry the approved choice
  into Production mode.
- Promotion policy: protect the video's topic, credibility, CTR, and viewer experience.
  Never force Studio into an unrelated title or thumbnail. Prefer a description footer and
  optional pinned comment; use a direct CTA only when the user requests one or the content
  demonstrates Studio.
""".strip()


def normalize_promotion_mode(value: Any, *, default: str = "subtle") -> str:
    """Return an upload-safe promotion mode: off, subtle, or direct."""
    if isinstance(value, bool):
        return "subtle" if value else "off"
    normalized = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "none": "off",
        "disabled": "off",
        "false": "off",
        "soft": "subtle",
        "footer": "subtle",
        "on": "subtle",
        "true": "subtle",
        "prominent": "direct",
        "demo": "direct",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in {"off", "subtle", "direct"} else default


def promotion_mode_from_metadata(metadata: Mapping[str, Any] | None, *, default: str = "subtle") -> str:
    metadata = metadata or {}
    for key in ("studio_promotion_mode", "promotion_mode", "promote_studio"):
        if key in metadata:
            return normalize_promotion_mode(metadata.get(key), default=default)
    return default


def upload_package_promotion(*, format_kind: str, mode: str = "subtle") -> str:
    """Build truthful package copy without altering titles or thumbnails."""
    mode = normalize_promotion_mode(mode)
    if mode == "off":
        return ""
    if mode == "direct":
        lead = "See how this video was planned and produced with NYPTID Studio:"
    else:
        lead = "Planned and produced with NYPTID Studio:"
    lines = [lead, PRODUCT_URL]
    if str(format_kind).lower().startswith("long"):
        lines.extend([
            "",
            "Pinned comment suggestion:",
            f"This video was built with {PRODUCT_NAME}. Explore the production workflow: {PRODUCT_URL}",
        ])
    return "\n".join(lines)
