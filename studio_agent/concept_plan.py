"""Production concept plans — plan before spend for shortform, longform, and product ads.

Studio must never jump from "let's make a short" to Approve without a concept card.
Flow:
  1) Soft proposal / strategy → build + store pending_concept
  2) User hard-commits ("yes make it", "render that plan") → convert concept → production pending
  3) Confirm mode still shows Approve for spend tools
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Any


FORMAT_SHORT = "shortform"
FORMAT_LONG = "longform"
FORMAT_PRODUCT_AD = "product_ad"

_DURATION_RE = re.compile(
    r"\b(?:only\s+)?(\d{1,3})\s*(?:s|sec|secs|second|seconds)\b",
    re.I,
)
_MINUTE_RE = re.compile(
    r"\b(?:only\s+)?(\d{1,2})\s*(?:m|min|mins|minute|minutes)\b",
    re.I,
)
_HOUR_RE = re.compile(r"\b(?:only\s+)?(\d{1,2})\s*(?:h|hr|hrs|hour|hours)\b", re.I)


def detect_format(
    user_text: str,
    *,
    session: dict[str, Any] | None = None,
    production_intent: str = "",
) -> str:
    """Classify the production format for the concept card."""
    session = session or {}
    low = str(user_text or "").lower()
    intent = str(production_intent or session.get("production_intent") or "").strip().lower()
    if intent == "product_ad" or re.search(
        r"\b(?:product\s+ad|saas|app\s+ad|software|landing\s+page|demo\s+video|"
        r"feature\s+walkthrough|app\s+promo|product\s+promo)\b",
        low,
    ):
        return FORMAT_PRODUCT_AD
    if _HOUR_RE.search(low):
        return FORMAT_LONG
    if re.search(r"\b(?:long[- ]?form|documentary|8\s*-\s*15\s*min|deep\s+dive)\b", low) and "short" not in low:
        return FORMAT_LONG
    if str(session.get("content_format") or "").lower() in {"long", "longform"} and "short" not in low:
        return FORMAT_LONG
    pending = session.get("pending_concept") or {}
    if isinstance(pending, dict) and str(pending.get("format") or "").lower() == FORMAT_LONG and "short" not in low:
        return FORMAT_LONG
    return FORMAT_SHORT


def parse_duration_sec(user_text: str, *, default_format: str = FORMAT_SHORT) -> int:
    """Extract target duration in seconds from user text."""
    low = str(user_text or "").lower()
    m = _DURATION_RE.search(low)
    if m:
        return max(8, min(120, int(m.group(1))))
    m = _MINUTE_RE.search(low)
    if m:
        mins = int(m.group(1))
        return max(60, min(60 * 45, mins * 60))
    m = _HOUR_RE.search(low)
    if m:
        return max(60 * 60, min(60 * 60 * 12, int(m.group(1)) * 60 * 60))
    if default_format == FORMAT_LONG:
        return 8 * 60
    if default_format == FORMAT_PRODUCT_AD:
        return 45
    return 30


def scene_count_for_duration(duration_sec: int, *, fmt: str = FORMAT_SHORT) -> int:
    """Map duration → scene/beat count (shortform stills)."""
    d = max(8, int(duration_sec or 30))
    if fmt == FORMAT_LONG:
        # Long-form uses chapters; scene_count is a soft planning hint only.
        return max(12, min(48, d // 30))
    # ~4–6s per scene for shorts
    return max(4, min(12, max(4, round(d / 5.0))))


def _longform_channel(session: dict[str, Any] | None) -> dict[str, Any]:
    """Long-form registry record for the session's active channel, or {}."""
    key = str((session or {}).get("registry_key") or "").strip().lower()
    if not key:
        return {}
    try:
        from long_form.prompts.channels import CHANNELS

        return dict(CHANNELS.get(key) or {})
    except Exception:
        return {}


def _reference_payload_from_messages(messages: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Pull the latest complete-ish reference analysis from session messages."""
    for msg in reversed(list(messages or [])[-40:]):
        if not isinstance(msg, dict):
            continue
        content = str(msg.get("content") or "")
        # Prefer structured preflight dumps
        if "Reference analysis complete" in content or "visual_summary" in content.lower():
            # Try tool observation JSON blocks
            pass
        if msg.get("role") != "system":
            continue
        if "[Studio Agent preflight tool result:" not in content:
            continue
        # Extract JSON after tool header if present
        if "{" not in content:
            continue
        try:
            import json

            start = content.find("{")
            end = content.rfind("}")
            if start >= 0 and end > start:
                data = json.loads(content[start : end + 1])
                if isinstance(data, dict) and (
                    data.get("visual_summary")
                    or data.get("pacing")
                    or data.get("metadata")
                    or data.get("transcript")
                ):
                    return data
        except Exception:
            continue
    return {}


def _title_from_user_text(user_text: str, *, fmt: str = FORMAT_SHORT) -> str | None:
    """Extract a working title seed from free text when no reference exists."""
    low = str(user_text or "").lower()
    if "day trad" in low and re.search(r"\b(?:three|3)\b", low):
        return "3 Things Day Traders Should Never Do"
    # Only force the curated title when the user clearly used that angle — not any "pull away".
    if re.search(
        r"\bpull\s+away\b.+\b(?:right when|when things get serious|get serious)\b|"
        r"\bwhen things get serious\b.+\bpull\s+away\b",
        low,
    ):
        return "Why Men Pull Away Right When Things Get Serious"
    if "stop loss" in low or "stoploss" in low or "trailing" in low:
        return "ALWAYS Use a Trailing Stop Loss"
    # "about X" / "on X" / "for X"
    m = re.search(
        r"\b(?:about|on|for|covering|around)\s+([a-z0-9][\w\s'&/-]{2,60}?)(?:\?|$|,|\.|how|only|\d+\s*(?:s|sec|min))",
        low,
        re.I,
    )
    if m:
        seed = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")
        if len(seed) >= 3 and seed not in {"this", "that", "it", "our", "the app", "the product"}:
            return seed[:1].upper() + seed[1:80]
    if fmt == FORMAT_PRODUCT_AD:
        m = re.search(r"\b(?:saas|app|product)\s+([a-z0-9][\w\s'&-]{2,40})", low)
        if m:
            seed = m.group(1).strip()
            return f"{seed[:1].upper() + seed[1:]} — product ad"[:80]
        return "Product ad concept"
    if fmt == FORMAT_LONG:
        return "Long-form concept"
    return None


def _title_from_reference(ref: dict[str, Any], user_text: str, *, fmt: str = FORMAT_SHORT) -> str:
    meta = ref.get("metadata") if isinstance(ref.get("metadata"), dict) else {}
    ref_title = str(meta.get("title") or ref.get("title") or "").strip()
    # Prefer a new working title derived from user intent + reference
    low = str(user_text or "").lower()
    user_title = _title_from_user_text(user_text, fmt=fmt)
    if user_title and (
        "stop loss" in low
        or re.search(r"when things get serious", low)
        or not ref_title
        or re.search(r"\b(?:about|on|for)\b", low)
    ):
        # Prefer curated/seed titles for known topics and free-text "about X" asks.
        if "stop loss" in low or re.search(r"when things get serious", low) or not ref_title:
            return user_title
        if re.search(r"\b(?:about|on|for)\b", low):
            return user_title
    if ref_title and len(ref_title) >= 8 and "untitled" not in ref_title.lower():
        # New short inspired by reference — don't clone title verbatim when user asked for "new"
        if re.search(r"\bnew\b", low):
            # Soft rewrite: keep topic kernel
            words = [w for w in re.findall(r"[A-Za-z0-9']+", ref_title) if len(w) > 2][:8]
            if words:
                return " ".join(words[:6]).title()[:80]
        return ref_title[:100]
    if user_title:
        return user_title
    if fmt == FORMAT_PRODUCT_AD:
        return "Product ad concept"
    if fmt == FORMAT_LONG:
        return "Long-form concept"
    return "Untitled Short Concept"


def _hook_from_reference(
    ref: dict[str, Any],
    *,
    fmt: str = FORMAT_SHORT,
    channel: dict[str, Any] | None = None,
) -> str:
    # Sleep docs must never promise a pattern interrupt — the product is calm.
    if fmt == FORMAT_LONG and str((channel or {}).get("pipeline_kind") or "") == "sleep_doc":
        return "Soft 'Drift off to sleep with…' open — calm and slow, no cold-open hook or pattern interrupt"
    story = ref.get("storytelling") if isinstance(ref.get("storytelling"), dict) else {}
    for key in ("hook", "hook_line", "opening_hook", "first_line"):
        val = str(story.get(key) or "").strip()
        if val:
            return val[:160]
    visual = ref.get("visual_summary")
    if isinstance(visual, dict):
        summary = str(visual.get("summary") or "").strip()
        if summary:
            return summary.split(".")[0][:160]
    if isinstance(visual, str) and visual.strip():
        return visual.strip().split(".")[0][:160]
    pacing = ref.get("pacing") if isinstance(ref.get("pacing"), dict) else {}
    hook_window = pacing.get("hook_window_sec")
    if hook_window:
        return f"Open with a pattern interrupt in the first {hook_window}s"
    if fmt == FORMAT_LONG:
        return "Cold open on the central question and promise its answer before the first chapter"
    return "Open with a clear pattern interrupt in the first 1–2 seconds"


def _beats_for_format(
    *,
    fmt: str,
    duration_sec: int,
    ref: dict[str, Any],
    title: str,
    channel: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    scenes = scene_count_for_duration(duration_sec, fmt=fmt)
    if fmt == FORMAT_LONG:
        # Sleep-doc channels (History Rewind) are the opposite of the generic
        # tension arc: calm chronological chapters, no hooks, no cliffhangers.
        if str((channel or {}).get("pipeline_kind") or "") == "sleep_doc":
            chapters = max(2, round(duration_sec / (30 * 60)))
            chapter_sec = duration_sec // chapters if chapters else duration_sec
            return [
                {"label": "Soft drift-off open", "seconds": 120,
                 "note": "'Drift off to sleep with…' framing — no cold-open hook, no cliffhanger"},
                {"label": f"Chronological chapters ({chapters} × ~{max(1, chapter_sec // 60)} min)",
                 "seconds": duration_sec - 300,
                 "note": "One era/dynasty per chapter, calm even pacing, specific names, dates, and outcomes"},
                {"label": "Quiet resolution", "seconds": 180,
                 "note": "Wind down the final era gently — no CTA spike, no loud outro"},
            ]
        return [
            {"label": "Hook / unresolved promise", "seconds": min(90, max(30, duration_sec // 100)), "note": "Open on the central historical contradiction and promise its answer"},
            {"label": "Rising action", "seconds": duration_sec // 4, "note": "Build the world, actors, pressure, and stakes chronologically"},
            {"label": "Conflict", "seconds": duration_sec // 4, "note": "Escalate the decisive struggle, constraint, betrayal, or collision"},
            {"label": "Comeback / reversal", "seconds": duration_sec // 5, "note": "Reveal the adaptation, reversal, consequence, or attempted recovery"},
            {"label": "Final rising action / payoff", "seconds": duration_sec // 5, "note": "Resolve the promised question and land the lasting historical consequence"},
        ]
    if fmt == FORMAT_PRODUCT_AD:
        return [
            {"label": "Pain hook", "seconds": 3, "note": "Problem the user feels today"},
            {"label": "Product reveal", "seconds": max(4, duration_sec // 5), "note": "App/SaaS on-screen, one clear benefit"},
            {"label": "Feature proof", "seconds": max(6, duration_sec // 3), "note": "2–3 concrete feature moments"},
            {"label": "Social proof / outcome", "seconds": max(4, duration_sec // 5), "note": "Result metric or before/after"},
            {"label": "CTA", "seconds": 3, "note": "Try / sign up / start free"},
        ]
    # Shortform default beat sheet
    per = max(3, duration_sec // max(4, scenes))
    labels = [
        ("Hook", "Pattern interrupt + curiosity gap"),
        ("Tension", "Name the pain / paradox"),
        ("Insight", "The mechanism in plain language"),
        ("Proof", "Visual example or mini-story"),
        ("Twist", "Unexpected reframe"),
        ("CTA / loop", "Payoff + rewatch bait"),
    ]
    beats: list[dict[str, Any]] = []
    for i in range(min(scenes, len(labels))):
        label, note = labels[i]
        beats.append({"label": label, "seconds": per, "note": note})
    # Pad if more scenes
    while len(beats) < scenes:
        beats.append({"label": f"Beat {len(beats) + 1}", "seconds": per, "note": "Keep visual change + narration locked"})
    return beats


def build_concept_plan(
    *,
    user_text: str,
    session: dict[str, Any] | None = None,
    messages: list[dict[str, Any]] | None = None,
    production_intent: str = "",
    content_format: str = "short",
) -> dict[str, Any]:
    """Build a durable concept plan for the concept card UI + later production args."""
    session = session or {}
    messages = messages or list(session.get("messages") or [])
    fmt = detect_format(user_text, session=session, production_intent=production_intent)
    if content_format in {"long", "longform"} and fmt == FORMAT_SHORT and "short" not in str(user_text or "").lower():
        fmt = FORMAT_LONG

    duration_sec = parse_duration_sec(user_text, default_format=fmt)
    # Honor session defaults lightly
    if fmt == FORMAT_SHORT and duration_sec == 30:
        # If user said 30s specifically it stays; already default
        pass

    ref = _reference_payload_from_messages(messages)
    # Also accept last_live_demand / conversation intent for title seeds
    intent = session.get("conversation_intent") if isinstance(session.get("conversation_intent"), dict) else {}
    # User-locked title always wins over curated/competitor/reference seeds.
    locked = str(
        intent.get("locked_title")
        or intent.get("working_title")
        or ""
    ).strip()
    try:
        from studio_agent.conversation import extract_user_locked_title

        from_msg = extract_user_locked_title(user_text)
        if from_msg:
            locked = from_msg
    except Exception:
        pass
    # A title locked during a Shorts discussion must not leak into a long-form
    # plan (and vice versa) — that is how "History Rewind YouTube Shorts"
    # became the working title of a 9-hour documentary card.
    if locked and fmt == FORMAT_LONG and re.search(r"\bshorts?\b", locked, re.I):
        locked = ""
    if locked:
        title = locked[:100]
    else:
        title = _title_from_reference(ref, user_text, fmt=fmt)
        if intent.get("last_topic") and re.search(r"\bnew\b", str(user_text or "").lower()):
            pass
        elif intent.get("last_topic") and not ref and title.startswith("Untitled"):
            title = str(intent.get("last_topic"))[:100]

    channel = _longform_channel(session) if fmt == FORMAT_LONG else {}
    if fmt == FORMAT_LONG and title in {"Long-form concept", "Untitled"} and channel.get("label"):
        title = f"{channel['label']} long-form concept"

    hook = _hook_from_reference(ref, fmt=fmt, channel=channel)
    beats = _beats_for_format(fmt=fmt, duration_sec=duration_sec, ref=ref, title=title, channel=channel)
    scenes = scene_count_for_duration(duration_sec, fmt=fmt)

    improvements: list[str] = []
    pacing = ref.get("pacing") if isinstance(ref.get("pacing"), dict) else {}
    cut_count = int(pacing.get("cut_count") or 0) if pacing else 0
    if cut_count and cut_count < 4 and fmt == FORMAT_SHORT:
        improvements.append(
            f"Reference only had ~{cut_count} cut(s) — plan {scenes} visual changes so it doesn't feel static."
        )
    if fmt == FORMAT_SHORT:
        improvements.append("Front-load the promise in 0–2s; no soft intro.")
        improvements.append("End on a rewatchable punch line or open loop.")
    if fmt == FORMAT_PRODUCT_AD:
        improvements.append("Show the product UI within the first 5 seconds.")
        improvements.append("One CTA only — no feature laundry list.")
    if fmt == FORMAT_LONG:
        if str(channel.get("pipeline_kind") or "") == "sleep_doc":
            improvements.append("Chapter markers per era so sleepers can navigate; keep transitions seamless and quiet.")
            improvements.append("~120 wpm calm narration, no startling moments — retention here means staying asleep-friendly.")
        else:
            improvements.append("Chapter markers every 60–90s for retention navigation.")
            improvements.append("Promise payoff in cold open; deliver it before mid-roll fatigue.")

    from studio_agent.render_styles import resolve_render_style

    style = resolve_render_style(
        None,
        session_style=str(session.get("render_style") or "").strip() or None,
        user_text=user_text,
    )
    visual_style = style.key
    image_model = str(session.get("image_model") or session.get("image_model_id") or "").strip()
    video_model = str(session.get("video_model") or "").strip()

    plan_id = f"concept_{uuid.uuid4().hex[:12]}"
    plan = {
        "id": plan_id,
        "status": "awaiting_confirm",
        "format": fmt,
        "title": title,
        "hook": hook,
        "duration_sec": duration_sec,
        "scene_count": scenes,
        "beats": beats,
        "improvements": improvements[:6],
        "visual_style": visual_style,
        "visual_style_label": style.label,
        "image_model": image_model,
        "video_model": video_model,
        "reference_title": str(
            (ref.get("metadata") or {}).get("title") if isinstance(ref.get("metadata"), dict) else ""
        )[:120],
        "user_request": str(user_text or "")[:500],
        "created_at": time.time(),
        "niche": str(intent.get("niche") or "")[:120],
        "channel_title": str(session.get("channel_title") or intent.get("channel_title") or "")[:120],
    }
    return plan


def reconcile_longform_plan(
    plan: dict[str, Any],
    *,
    session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Repair a reused long-form concept so the card never shows mixed eras.

    Plan-mode reuses pending_concept instead of rebuilding it, so a plan whose
    duration/format was updated in later turns can carry stale artifacts: a
    Shorts-era working title, shortform hook language, or beats sized for the
    old 8-minute default under a 9-hour duration. Rebuild only the stale parts."""
    if str(plan.get("format") or "") != FORMAT_LONG:
        return plan
    session = session or {}
    channel = _longform_channel(session)
    if not channel:
        # Session has no registry key (older sessions): recover the channel
        # from the plan's own text so e.g. "History Rewind YouTube Shorts"
        # still resolves to the History Rewind sleep-doc contract.
        hint = f"{plan.get('channel_title') or ''} {plan.get('title') or ''} {plan.get('user_request') or ''}".lower()
        try:
            from long_form.prompts.channels import CHANNELS

            for rec in CHANNELS.values():
                label = str(rec.get("label") or "").strip().lower()
                if label and label in hint and str(rec.get("format") or "") == "long_form":
                    channel = dict(rec)
                    break
        except Exception:
            channel = {}
    sleep_doc = str(channel.get("pipeline_kind") or "") == "sleep_doc"
    duration = int(plan.get("duration_sec") or 8 * 60)

    title = str(plan.get("title") or "").strip()
    if title and re.search(r"\bshorts?\b", title, re.I):
        plan["title"] = (
            f"{channel.get('label')} long-form concept" if channel.get("label") else "Long-form concept"
        )
    # A generic placeholder title yields to the real planned video: the active
    # thumbnail review carries the actual working title for this session.
    if re.search(r"long-?form concept$|^untitled", str(plan.get("title") or "").strip(), re.I) or not plan.get("title"):
        review = session.get("thumbnail_review") if isinstance(session.get("thumbnail_review"), dict) else {}
        review_title = str((review or {}).get("title") or "").strip()
        if review_title:
            plan["title"] = review_title[:120]

    beats = [b for b in (plan.get("beats") or []) if isinstance(b, dict)]
    beat_total = sum(int(b.get("seconds") or 0) for b in beats)
    beats_stale = not beats or beat_total < duration * 0.6 or beat_total > duration * 1.4
    if sleep_doc and not any("drift" in str(b.get("label") or "").lower() for b in beats):
        beats_stale = True
    if beats_stale:
        plan["beats"] = _beats_for_format(
            fmt=FORMAT_LONG, duration_sec=duration, ref={},
            title=str(plan.get("title") or ""), channel=channel,
        )

    hook = str(plan.get("hook") or "")
    if not hook or (sleep_doc and re.search(r"pattern interrupt|first 1|first \d+\s*s", hook, re.I)):
        plan["hook"] = _hook_from_reference({}, fmt=FORMAT_LONG, channel=channel)

    if beats_stale:
        improvements = [
            tip for tip in (plan.get("improvements") or [])
            if not re.search(r"chapter markers|pattern interrupt|cold open|front-load", str(tip), re.I)
        ]
        if sleep_doc:
            improvements.append("Chapter markers per era so sleepers can navigate; keep transitions seamless and quiet.")
            improvements.append("~120 wpm calm narration, no startling moments — retention here means staying asleep-friendly.")
        else:
            improvements.append("Chapter markers every 60–90s for retention navigation.")
            improvements.append("Promise payoff in cold open; deliver it before mid-roll fatigue.")
        plan["improvements"] = improvements[:6]
    return plan


def _human_duration(sec: int) -> str:
    s = max(0, int(sec or 0))
    if s >= 3600:
        h, rem = divmod(s, 3600)
        return f"{h}h {rem // 60}m" if rem >= 60 else f"{h}h"
    if s >= 60:
        m, rem = divmod(s, 60)
        return f"{m}m {rem}s" if rem else f"{m}m"
    return f"{s or 30}s"


def concept_to_assistant_markdown(plan: dict[str, Any]) -> str:
    """Human chat summary of the concept (UI also shows a card)."""
    fmt = str(plan.get("format") or FORMAT_SHORT)
    fmt_label = {
        FORMAT_SHORT: "Short-form",
        FORMAT_LONG: "Long-form",
        FORMAT_PRODUCT_AD: "Product / SaaS ad",
    }.get(fmt, fmt)
    lines = [
        f"**Concept plan — {fmt_label}** (not rendering yet)",
        "",
        f"**Working title:** {plan.get('title') or 'Untitled'}",
        f"**Hook:** {plan.get('hook') or 'TBD'}",
        f"**Length:** ~{_human_duration(int(plan.get('duration_sec') or 30))}"
        + (f" · {int(plan.get('scene_count') or 0)} scenes" if fmt == FORMAT_SHORT else ""),
        "",
        "**Beat sheet:**",
    ]
    for i, beat in enumerate(plan.get("beats") or []):
        if not isinstance(beat, dict):
            continue
        lines.append(
            f"{i + 1}. **{beat.get('label') or f'Beat {i + 1}'}** "
            f"({_human_duration(int(beat.get('seconds') or 0))}) — {beat.get('note') or ''}"
        )
    if plan.get("improvements"):
        lines.append("")
        lines.append("**Improvements vs reference / status quo:**")
        for tip in plan.get("improvements") or []:
            lines.append(f"- {tip}")
    lines.extend(
        [
            "",
            "If this plan looks right, say **yes make it**, **render that plan**, or **make the first scene** "
            f"(optional: **only {int(plan.get('duration_sec') or 30)} seconds**).",
            "I'll then prepare production for your approval — not before.",
        ]
    )
    return "\n".join(lines)


def concept_to_production_args(
    plan: dict[str, Any],
    *,
    session: dict[str, Any] | None = None,
    user_text: str = "",
) -> tuple[str, dict[str, Any]]:
    """Convert a confirmed concept into (tool_name, arguments)."""
    session = session or {}
    fmt = str(plan.get("format") or FORMAT_SHORT)
    # Locked session title wins over a stale concept card from the prior short.
    locked = ""
    try:
        from studio_agent.store import get_locked_working_title

        locked = get_locked_working_title(session)
    except Exception:
        intent = session.get("conversation_intent") if isinstance(session.get("conversation_intent"), dict) else {}
        locked = str(intent.get("locked_title") or intent.get("working_title") or "").strip()
    title = str(locked or plan.get("title") or "Untitled").strip()[:120]
    duration_sec = int(plan.get("duration_sec") or parse_duration_sec(user_text, default_format=fmt))
    # Allow commit message to override duration
    if user_text:
        override = parse_duration_sec(user_text, default_format=fmt)
        # Only override if user explicitly mentioned seconds/minutes
        if _DURATION_RE.search(user_text) or _MINUTE_RE.search(user_text):
            duration_sec = override
    scenes = int(plan.get("scene_count") or scene_count_for_duration(duration_sec, fmt=fmt))
    if user_text and (_DURATION_RE.search(user_text) or _MINUTE_RE.search(user_text)):
        scenes = scene_count_for_duration(duration_sec, fmt=fmt)

    from studio_agent.render_styles import resolve_render_style

    style = resolve_render_style(
        str(plan.get("visual_style") or "").strip() or None,
        session_style=str(session.get("render_style") or "").strip() or None,
        user_text=user_text or str(plan.get("user_request") or ""),
    )
    render_style = style.key
    video_model = str(plan.get("video_model") or session.get("video_model") or "").strip()
    image_model = str(plan.get("image_model") or session.get("image_model") or session.get("image_model_id") or "").strip()

    hook = str(plan.get("hook") or "").strip()
    beats = plan.get("beats") if isinstance(plan.get("beats"), list) else []
    beat_lines = []
    for b in beats[:12]:
        if isinstance(b, dict):
            beat_lines.append(f"- {b.get('label')}: {b.get('note')}")
    visual_brief = (
        f"Title: {title}. Hook: {hook}. Target length ~{duration_sec}s.\n"
        f"Beat sheet:\n" + ("\n".join(beat_lines) if beat_lines else "Fast hook → insight → proof → CTA.")
    )
    if fmt == FORMAT_PRODUCT_AD:
        visual_brief += (
            "\nProduct/SaaS ad: show product UI early, one clear benefit path, single CTA. "
            "Avoid feature laundry lists."
        )

    if fmt == FORMAT_LONG:
        registry_key = str(session.get("registry_key") or "default").strip() or "default"
        try:
            from long_form.prompts.channels import resolve_channel_key

            registry_key = resolve_channel_key(registry_key) or registry_key
        except Exception:
            pass
        history_stills = registry_key == "history_rewind"
        empire_full_motion = registry_key == "empire_magnates"
        try:
            from long_form.prompts.channels import get_channel

            channel_defaults = get_channel(registry_key) if history_stills else {}
        except Exception:
            channel_defaults = {}
        default_bgm = (
            str(channel_defaults.get("default_background_music") or "off").strip() or "off"
            if history_stills
            else "off"
        )
        sound_brief = (
            str(channel_defaults.get("sound_design") or "").strip()
            if history_stills
            else ""
        )
        args = {
            "channel_key": registry_key,
            "title": title,
            "topic": title,
            "render_style": render_style,
            "motion_policy": "stills" if history_stills else ("full" if empire_full_motion else "balanced"),
            "ken_burns_enabled": history_stills,
            "light_shake_enabled": history_stills,
            "image_model_id": image_model or None,
            "sfx_enabled": False,
            "background_music": default_bgm,
            "sound_design_brief": sound_brief,
            "user_request": str(user_text or plan.get("user_request") or "")[:500],
            "concept_plan_id": plan.get("id"),
            "target_duration_sec": duration_sec,
        }
        return "start_longform_render", args

    category_key = "human_limits"
    reg = str(session.get("registry_key") or "").lower()
    topic_low = f"{title} {user_text or plan.get('user_request') or ''}".lower()
    if "day trad" in topic_low or "market" in topic_low or "finance" in topic_low:
        category_key = "science_technology"
    elif style.pipeline != "skeleton_host":
        category_key = "people_blogs"
    elif "skele" in reg or "psych" in reg:
        category_key = "human_limits"
    elif fmt == FORMAT_PRODUCT_AD:
        category_key = "people_blogs"

    args: dict[str, Any] = {
        "render_style": render_style,
        "category_key": category_key,
        "topic": title,
        "title": title,
        "video_model": video_model or None,
        "image_model_id": image_model or None,
        "visual_brief": visual_brief[:2000],
        "animate": False,
        "sfx_enabled": False,
        "background_music": "off",
        "user_request": str(user_text or plan.get("user_request") or "")[:500],
        "scene_count": scenes,
        "target_duration_sec": duration_sec,
        "concept_plan_id": plan.get("id"),
        "production_format": fmt,
    }
    # Drop empty model fields
    args = {k: v for k, v in args.items() if v is not None and v != ""}
    return "start_shortform_generate", args


def format_concept_for_pending_summary(plan: dict[str, Any]) -> str:
    fmt = str(plan.get("format") or FORMAT_SHORT)
    return (
        f"{plan.get('title') or 'Concept'} · {fmt} · "
        f"~{int(plan.get('duration_sec') or 30)}s · {int(plan.get('scene_count') or 0)} scenes"
    )
