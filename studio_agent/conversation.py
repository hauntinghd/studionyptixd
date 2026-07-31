"""Grok-class conversation synthesis for Studio Agent.

Tools and Catalyst remain the source of truth. This layer turns verified
evidence into a natural collaborator reply — never a rigid report form,
never score dumps, never dash-score lines like "topic - score 0.79".
"""
from __future__ import annotations

import re
from typing import Any


GROK_CLASS_VOICE = """You are NYPTID Studio Agent — a Grok-class creative collaborator.

Conversation is primary. Tools, Live Demand, and Catalyst are instruments you use
silently and then explain in plain language (exactly like Grok on grok.com).

Rules:
1. Answer like a smart human producer talking to a creator friend: direct, useful, warm, not corporate.
2. Never invent view counts, dates, channels, or performance stats. Use ONLY the verified evidence packet.
3. Lead with the answer to their question in 1–3 sentences, then the useful takeaways, then optional next steps.
4. Do NOT open with "I verified public YouTube demand" or rigid section headers.
5. NEVER write score numbers, "composite", "strong_public_precedent", "score 0.79", or lines like
   "title — score 0.49". Talk in plain English only.
6. Do NOT append a "Verified evidence" dump or research form. Weave a few real titles + view counts into prose.
7. Channel connection is optional. Market research works from public YouTube alone — never block on OAuth.
8. If evidence is thin or off-niche, say so honestly and ask for a clearer niche or next test.
9. Offer a clear next move: script a short, pick a packaging angle, or re-search a tighter niche.
10. If they want production, invite it in natural language — do not dump internal tool JSON.
"""


def _parse_evidence_rows(evidence: str, *, limit: int = 6) -> list[dict[str, str]]:
    """Pull title/channel/views from grounded evidence lines for conversational use.

    Channel-status lines look like:
      ``- Latest upload: Why Men Suddenly Pull Away: 310 views; published 2026-07-12``
    Never split on the first colon blindly — that duplicated title:views in the UI.
    """
    status_prefix_re = re.compile(
        r"^(?P<label>"
        r"latest upload|latest short baseline|best prior short control|"
        r"shorts metrics available|channel|limitation"
        r")\s*:\s*(?P<rest>.+)$",
        re.I,
    )
    views_re = re.compile(
        r"^(?P<title>.+?)\s*:\s*(?P<views>[\d,.]+\s*views?)\b(?P<tail>.*)$",
        re.I,
    )
    rows: list[dict[str, str]] = []
    for line in str(evidence or "").splitlines():
        s = line.strip()
        if not s.startswith("- "):
            continue
        body = s[2:].strip()
        low = body.lower()
        if any(
            skip in low
            for skip in (
                "public search coverage",
                "selected-channel",
                "search method",
                "predicted moves",
                "confirmed:",
                "blocked:",
                "note:",
                "youtube_quota",
                "score ",
                "composite",
                "i should use only",
                "retention rows",
                "limitation:",
                "shorts metrics missing",
                "metrics missing:",
                "strong_public_precedent",
                "supported_public_precedent",
                "weak_public_precedent",
                "exploratory_public_signal",
            )
        ):
            continue
        if "views" not in low and "published" not in low:
            continue

        label = ""
        rest = body
        m_status = status_prefix_re.match(body)
        if m_status:
            label = str(m_status.group("label") or "").strip()
            rest = str(m_status.group("rest") or "").strip()
            if "metrics available" in label.lower():
                continue

        title = ""
        channel = ""
        views = ""
        m_views = views_re.match(rest)
        if m_views:
            title = str(m_views.group("title") or "").strip()
            views = str(m_views.group("views") or "").strip()
            tail = str(m_views.group("tail") or "").strip()
            parts = [p.strip() for p in tail.strip(" ;").split(";") if p.strip()]
            for p in parts:
                pl = p.lower()
                if "published" in pl or "views" in pl:
                    continue
                if not channel and len(p) >= 2:
                    channel = p
                    break
        else:
            if ":" in rest:
                title, after = rest.split(":", 1)
                title = title.strip()
                parts = [p.strip() for p in after.split(";") if p.strip()]
            else:
                parts = [p.strip() for p in rest.split(";") if p.strip()]
                title = parts[0] if parts else rest
                parts = parts[1:]
            for p in parts:
                pl = p.lower()
                if "views" in pl and not views:
                    views = p
                elif "published" not in pl and not channel and p != title:
                    channel = p

        if not title or len(title) < 6:
            continue
        if re.search(r"\bscore\s+\d", title, re.I):
            continue
        if title.lower() in {
            "latest upload",
            "latest short baseline",
            "best prior short control",
            "shorts metrics available",
        }:
            continue
        rows.append(
            {
                "title": title[:140],
                "channel": channel[:80],
                "views": views[:60],
                "label": label[:60],
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _first_titles_from_evidence(evidence: str, *, limit: int = 3) -> list[str]:
    return [r["title"] for r in _parse_evidence_rows(evidence, limit=limit)]


def _human_stats_line(row: dict[str, str]) -> str:
    title = row.get("title") or "Untitled"
    channel = row.get("channel") or ""
    views = row.get("views") or ""
    label = row.get("label") or ""
    prefix = f"{label}: " if label else ""
    if channel and views and channel.lower() != views.lower():
        return f"{prefix}**{title}** ({channel}, {views})"
    if views:
        return f"{prefix}**{title}** ({views})"
    if channel:
        return f"{prefix}**{title}** ({channel})"
    return f"{prefix}**{title}**"


def _evidence_has_channel_analytics_rows(evidence: str) -> bool:
    low = str(evidence or "").lower()
    return any(
        token in low
        for token in (
            "latest upload:",
            "latest short baseline:",
            "best prior short",
            "worst prior short",
            "shorts metrics missing:",
            "shorts metrics available",
            "channel analytics",
            "avg view percentage",
            "avg_view_percentage",
        )
    )


def _clean_niche_display(niche_hint: str, user_text: str = "") -> str:
    """Never show dictation soup like 'of why men … because this would be directed…'."""
    from studio_agent.turn_plan import extract_explicit_topic_phrase, extract_known_niche_phrase

    for candidate in (
        extract_explicit_topic_phrase(user_text),
        extract_known_niche_phrase(user_text),
        extract_explicit_topic_phrase(niche_hint),
        extract_known_niche_phrase(niche_hint),
        str(niche_hint or "").strip(),
    ):
        phrase = re.sub(r"\s+", " ", str(candidate or "").strip(" ,.-"))
        if not phrase:
            continue
        phrase = re.sub(r"(?i)^(of|about|on|for|the)\s+", "", phrase).strip()
        phrase = re.split(
            r"(?i)\b(?:because|directed\s+towards?|targeting|in\s+the\s+psychology|"
            r"psychology\s+niche|keep\s+it|do\s+deep|for\s+sure|um+|yeah)\b",
            phrase,
            maxsplit=1,
        )[0].strip(" ,.-")
        phrase = re.sub(r"(?i)\s+(?:psychology(?:\s+niche)?|niche|youtube\s+shorts)\s*$", "", phrase).strip()
        if len(phrase) >= 8 and not phrase.lower().startswith("of "):
            if re.search(r"\b(?:youtube\s+shorts|shorts\s+storytime)\b", phrase, re.I):
                if not re.search(
                    r"\b(?:day\s*trad|fitness|psycholog|crypto|forex|true crime|history|love\s*bomb)\b",
                    phrase,
                    re.I,
                ):
                    return "public Shorts right now"
            return phrase[:80]
    return "this space"


def _evidence_has_public_hydrated_rows(evidence: str) -> bool:
    low = str(evidence or "").lower()
    if "hydrated_video_stats" in low or "hydrated public" in low:
        return True
    if any(
        token in low
        for token in (
            "strong_public_precedent",
            "supported_public_precedent",
            "weak_public_precedent",
            "exploratory_public_signal",
        )
    ):
        return True
    return False


def _evidence_public_search_failed(evidence: str) -> bool:
    low = str(evidence or "").lower()
    return any(
        token in low
        for token in (
            "no module named",
            "public search failed",
            "no hydrated public",
            "none cleared",
            "no verified public video stats",
            "won't claim a trend from search snippets",
            "studio_analytics_router",
        )
    )


def deterministic_conversational_research_reply(
    *,
    user_text: str,
    evidence: str,
    reference_findings: str = "",
    niche_hint: str = "",
) -> str:
    """Natural reply only — no research form, no score lines, no evidence appendix dump."""
    niche_display = _clean_niche_display(niche_hint, user_text)

    rows = _parse_evidence_rows(evidence, limit=5)
    titles = [r["title"] for r in rows]
    thin = (
        "none cleared" in evidence.lower()
        or "no strong" in evidence.lower()
        or "exploratory" in evidence.lower()
        or not titles
    )
    quota_dead = (
        "youtube_quota_exhausted" in evidence.lower()
        or "quota exceeded" in evidence.lower()
        or "daily search budget is spent" in evidence.lower()
        or "past daily cap" in evidence.lower()
    )
    no_stats = (
        "no verified public video stats" in evidence.lower()
        or "public search completed, but no verified" in evidence.lower()
        or "won't claim a trend from search snippets" in evidence.lower()
    )
    public_ok = _evidence_has_public_hydrated_rows(evidence) and not _evidence_public_search_failed(evidence)
    channel_evidence = _evidence_has_channel_analytics_rows(evidence) or any(
        str(r.get("label") or "").strip() for r in rows
    )
    q = re.sub(r"\s+", " ", str(user_text or "")).strip()
    wants_demand = bool(
        re.search(r"\b(?:data|research|demand|want|watch|working|viral|people)\b", q, re.I)
    )
    wants_best_worst = bool(
        re.search(r"\b(?:best|worst|top|underperform)\b", q, re.I)
        and re.search(r"\b(?:short|shorts|perform|retention|views)\b", q, re.I)
    )
    # Best/worst + channel rows must never be framed as "public YouTube data".
    channel_only = bool(titles) and (
        (channel_evidence and (wants_best_worst or not public_ok))
        or (not public_ok)
    )

    if quota_dead:
        return (
            f"I tried to pull public YouTube demand for **{niche_display}**, but the YouTube Data API "
            "daily search budget is spent for this project. I won't invent view counts. "
            "It usually resets around midnight Pacific Time — hit me again after that, or name a "
            "specific niche so we run a tight search as soon as quota is back."
        )

    if _evidence_public_search_failed(evidence) and not titles:
        return (
            f"I couldn't hydrate public YouTube stats for **{niche_display}** this turn "
            "(search failed or returned empty). I won't invent view counts. "
            "Give me a tighter topic phrase and I'll re-pull, or connect a channel if you want "
            "your own best/worst Shorts compared."
        )

    if no_stats and not titles:
        return (
            f"I ran a public YouTube pass for **{niche_display}**, but nothing useful hydrated this turn. "
            "Tell me the niche in plain words (day trading, fitness, dark psychology, relationship advice) "
            "and I'll pull demand again — no channel connect required."
        )

    if channel_only:
        lead = (
            f"I used your **connected channel analytics** for this answer"
            f"{f' (topic focus: **{niche_display}**)' if niche_display and niche_display != 'this space' else ''}. "
            + (
                "I also compared against public niche search where it hydrated — but the rows below are from your channel."
                if public_ok
                else "Public niche search didn't return fresh hydrated rows this turn, so I'm not claiming open-market virals."
            )
        )
    elif wants_demand and public_ok:
        lead = (
            f"Yes — I pulled public YouTube data for **{niche_display}** "
            "(no channel connect needed for market research)."
        )
    elif public_ok:
        lead = f"Here's what public YouTube is showing for **{niche_display}** right now."
    else:
        lead = f"Here's what I can verify for **{niche_display}** right now."

    if titles and not thin:
        bullets = "\n".join(f"• {_human_stats_line(r)}" for r in rows[:4])
        if wants_best_worst and channel_only:
            body = (
                "From your channel's returned Shorts/video rows:\n"
                f"{bullets}\n\n"
                "Treat these as your evidence-backed winners/controls — not invented comps. "
                "Tell me which pattern to chase for the next 30s short and I'll script it."
            )
        else:
            body = (
                "These are the strongest signals with real view stats:\n"
                f"{bullets}\n\n"
                "I'd treat those as packaging patterns (hook + promise), not guaranteed virals. "
                "Pick one angle and we can script a Short from it in this chat."
            )
    elif titles:
        bullets = "\n".join(f"• {_human_stats_line(r)}" for r in rows[:4])
        body = (
            "Window is still thin on big proven winners, but these are the cleanest signals so far:\n"
            f"{bullets}\n\n"
            "Good for test packaging, not for claiming a slam dunk."
        )
    else:
        body = (
            "Search ran, but I don't have a clear winner to recommend yet. "
            "Give me a tighter niche in one line and I'll re-pull."
        )

    next_steps = (
        "\n\nWhat I'd do next: pick one title pattern you like, or say **make a 20s short about that** "
        "and I'll produce it here. Connecting a channel is optional — only if you want this ranked "
        "against *your* retention winners too."
    )
    if wants_best_worst or channel_only:
        next_steps = (
            "\n\nNext: say the exact topic for the new 30s short (e.g. love-bomb then disappear), "
            "and I'll script it using these channel patterns — or ask me to re-pull public comps for that topic."
        )

    parts = [lead, "", body, next_steps]
    if reference_findings and str(reference_findings).strip():
        ref = re.sub(r"\s+", " ", str(reference_findings).strip())[:400]
        parts.extend(["", f"From your reference clip: {ref}"])
    return strip_robot_research_artifacts("\n".join(parts).strip())


def strip_robot_research_artifacts(text: str) -> str:
    """Remove form dumps, score lines, and machine labels from user-facing replies."""
    if not text:
        return text
    cleaned = str(text)
    # Drop entire "Verified evidence" appendices and research form tails
    cleaned = re.split(
        r"\n\s*(?:---+\s*)?(?:\*\*)?Verified evidence(?:\*\*)?",
        cleaned,
        maxsplit=1,
        flags=re.I,
    )[0]
    cleaned = re.split(
        r"\n\s*(?:I verified public YouTube demand|Public YouTube demand evidence:|"
        r"Selected-channel evidence:|Public search coverage:|Predicted moves to test next:|"
        r"Candidate angles \(internal|What is confirmed vs blocked:)",
        cleaned,
        maxsplit=1,
        flags=re.I,
    )[0]
    out_lines: list[str] = []
    for line in cleaned.splitlines():
        low = line.lower().strip()
        # Kill score dumps: "foo — score 0.79" / "foo - score 0.49; ..."
        if re.search(r"\bscore\s+\d", low) or re.search(r"\bcomposite\s+\d", low):
            continue
        if re.search(r"[—–-]\s*score\b", low):
            continue
        if "predicted niche fit" in low:
            continue
        if "strong_public_precedent" in low or "supported_public_precedent" in low:
            if low.startswith(("-", "•", "*")) and "views" not in low:
                continue
            line = re.sub(
                r"\b(?:strong|supported|weak|exploratory)_public_(?:precedent|signal)\b",
                "",
                line,
                flags=re.I,
            )
        # Form headers that must never reach the user alone or at all
        if re.match(
            r"^(?:public youtube demand check|public youtube demand evidence|"
            r"selected-channel evidence|public search coverage|predicted moves|"
            r"what is confirmed|i verified public youtube demand|"
            r"candidate angles)",
            low,
        ):
            continue
        if "for your reference niche" in low and len(low) < 80:
            continue
        if low.startswith("predicted moves") or low.startswith("what is confirmed"):
            continue
        if "i verified public youtube demand" in low:
            continue
        if "connect youtube oauth" in low or "no selected-channel analytics" in low:
            continue
        out_lines.append(line)
    cleaned = "\n".join(out_lines)
    # Final pass: any remaining "topic - score 0.xx" inline
    cleaned = re.sub(
        r"[^\n]{3,200}?\s*[—–-]\s*score\s+\d+(?:\.\d+)?(?:\s*[;,][^\n]*)?",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(r"(?im)^\s*[-•*]\s*.*?\bpredicted niche fit\b.*$", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.strip()
    # Never replace short/empty chat with a Live Demand stub (that made "hey" → demand empty pack).
    # Research paths that need a demand fallback must supply it themselves.
    if not cleaned:
        original = str(text or "").strip()
        # Form-only blob with nothing left: neutral recovery, not demand-specific
        if re.search(
            r"(?i)public youtube demand|verified public|predicted moves|score\s+\d|confirmed vs blocked",
            original,
        ):
            return (
                "I had a glitchy research form draft and cleared it. "
                "What do you want to work on — niche research, a short, or something else?"
            )
        return original
    return cleaned


async def synthesize_conversational_research_reply(
    *,
    user_text: str,
    evidence: str,
    reference_findings: str = "",
    niche_hint: str = "",
    model: str = "",
) -> str:
    """Grok-class natural reply grounded only on verified evidence — never form dumps."""
    fallback = deterministic_conversational_research_reply(
        user_text=user_text,
        evidence=evidence,
        reference_findings=reference_findings,
        niche_hint=niche_hint,
    )
    evidence = str(evidence or "").strip()
    if not evidence:
        return fallback
    try:
        from studio_agent import openrouter

        # Compact evidence for the model: titles + views only (no score lines)
        rows = _parse_evidence_rows(evidence, limit=8)
        compact = "\n".join(
            f"- {r['title']}" + (f" | {r['channel']}" if r.get("channel") else "")
            + (f" | {r['views']}" if r.get("views") else "")
            for r in rows
        ) or evidence[:3000]

        system = (
            GROK_CLASS_VOICE
            + "\n\nYou will receive VERIFIED PUBLIC EVIDENCE (titles/channels/views only). "
            "Write the user-facing reply ONLY as natural conversation. "
            "Do not append a verified-evidence section. Do not use scores. Do not use form headers. "
            "Do not require a connected channel for market research."
        )
        user_payload = (
            f"User message:\n{user_text}\n\n"
            f"Niche hint: {niche_hint or '(public demand / discovery)'}\n\n"
            f"Reference findings (optional):\n{reference_findings or '(none)'}\n\n"
            f"VERIFIED PUBLIC EVIDENCE:\n{compact}\n"
        )
        resp = await openrouter.chat_completion(
            model=str(model or "").strip() or None,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_payload},
            ],
            tools=None,
            temperature=0.45,
            reasoning_depth="fast",
        )
        msg = openrouter.message_from_response(resp)
        text = str((msg or {}).get("content") or "").strip()
        if not text or len(text) < 40:
            return fallback
        text = strip_robot_research_artifacts(text)
        # If model still leaked a form, fall back to deterministic prose
        if "i verified public youtube demand" in text.lower() or re.search(r"\bscore\s+\d", text, re.I):
            return fallback
        return text
    except Exception:
        return fallback


def conversation_system_preamble() -> str:
    """Injected into the main agent system prompt."""
    return """GROK-CLASS CONVERSATION MODE (mandatory product voice — like Grok on grok.com):
- You are a conversation-first collaborator first, a tool runner second.
- Talk like Grok: clear, direct, useful, human. Tools run silently; you explain results in plain language.
- NEVER ship rigid research forms ("I verified public YouTube demand", "Predicted moves", "Confirmed vs blocked").
- NEVER write score lines ("title - score 0.79", "composite 0.7", "strong_public_precedent").
- Market / niche research does NOT require a connected YouTube channel. Public search alone is enough.
  Channel connect is optional bonus for *their* retention winners.
- When tools return Live Demand evidence: answer → insights in prose → next move (script/short).
- Production updates also use this voice — short, clear, human.
- Never invent metrics. Cite real titles, channels, views from tools only.
- Weave Catalyst learnings as brief producer notes, not a robot dump.
- You can research and produce in the same chat.
- Keep session continuity: remember the niche, product, and channel the user already established.
- Answer the user's actual question first; do not make them read a workflow before a useful answer.
- Ask one clarifying question only when a missing choice changes cost, factual accuracy, channel scope, or the production result. Otherwise make the best grounded move.
- When the user corrects Studio, acknowledge the correction, discard conflicting stale context, and continue from it.
- Never frame completed work as "thinking", "waiting", or a future promise. Report the verified result or exact blocker.
"""


# ── Session intent continuity (niche / product / channel across long chats) ──


def get_conversation_intent(session: dict[str, Any] | None) -> dict[str, Any]:
    session = session if isinstance(session, dict) else {}
    intent = session.get("conversation_intent")
    return dict(intent) if isinstance(intent, dict) else {}


def update_conversation_intent(
    session: dict[str, Any] | None,
    *,
    niche: str = "",
    product: str = "",
    mode: str = "",
    channel_title: str = "",
    channel_id: str = "",
    registry_key: str = "",
    search_query: str = "",
    last_topic: str = "",
    locked_title: str = "",
    clear_locked_title: bool = False,
    kind: str = "",
    **extra: Any,
) -> dict[str, Any]:
    """Merge durable conversation intent onto the session dict (caller persists)."""
    base = get_conversation_intent(session)
    if clear_locked_title:
        # A direct creator correction starts a new intent contract. Keeping an
        # old title here is how stale topics leak into a new render.
        base.pop("locked_title", None)
        base.pop("working_title", None)
        base.pop("last_topic", None)
    if niche:
        base["niche"] = str(niche).strip()[:120]
    if product:
        base["product"] = str(product).strip()[:200]
    if mode:
        base["mode"] = str(mode).strip()[:40]
    if channel_title:
        base["channel_title"] = str(channel_title).strip()[:120]
    if channel_id:
        base["channel_id"] = str(channel_id).strip()[:80]
    if registry_key:
        base["registry_key"] = str(registry_key).strip()[:80]
    if search_query:
        base["search_query"] = str(search_query).strip()[:220]
    # locked_title is user-owned; last_topic may be softer continuity.
    if locked_title:
        locked = str(locked_title).strip()[:200]
        base["locked_title"] = locked
        base["last_topic"] = locked
        base["working_title"] = locked
    elif last_topic:
        # Never overwrite a locked user title with research/catalyst topics.
        if not str(base.get("locked_title") or "").strip():
            base["last_topic"] = str(last_topic).strip()[:200]
    if kind:
        base["kind"] = str(kind).strip()[:40]
    for k, v in extra.items():
        if v is not None and str(v).strip():
            # Protect locked_title from accidental extra= overwrites with empty/competitor noise.
            if k in {"locked_title", "working_title"} and base.get("locked_title"):
                continue
            if k == "last_topic" and base.get("locked_title"):
                continue
            base[k] = v
    return base


def format_intent_for_prompt(session: dict[str, Any] | None) -> str:
    intent = get_conversation_intent(session)
    if not intent:
        return ""
    bits = []
    locked = str(intent.get("locked_title") or intent.get("working_title") or "").strip()
    if locked:
        bits.append(
            f"LOCKED WORKING TITLE (user-chosen — use this exact title for production/concept; "
            f"competitor titles from research are DATA ONLY, never substitute them): {locked}"
        )
    for key, label in (
        ("niche", "Niche"),
        ("render_style", "Art style lock"),
        ("product", "Product"),
        ("mode", "Mode"),
        ("channel_title", "Channel"),
        ("last_topic", "Last topic"),
        ("search_query", "Last search"),
    ):
        val = str(intent.get(key) or "").strip()
        if not val:
            continue
        if key == "last_topic" and locked and val.lower() == locked.lower():
            continue
        bits.append(f"{label}: {val}")
    if not bits:
        return ""
    return "Session continuity (already established — do not re-ask unless user changes it):\n" + "\n".join(
        f"- {b}" for b in bits
    )


def _clean_title_fragment(cand: str) -> str:
    cand = re.sub(r"\s+", " ", str(cand or "")).strip(" -:,.\"'")
    # Strip duration / style tails that ride on hard-commit messages.
    cand = re.split(
        r"\b(?:but\s+only|only\s+\d|using|with art|instead|hows|how'?s|right\?|"
        r"seconds?\s+long|minutes?\s+long)\b",
        cand,
        maxsplit=1,
        flags=re.I,
    )[0].strip(" -:,.\"'")
    return cand[:200]


def normalize_spoken_request(user_text: str) -> str:
    """Conservatively repair common STT artifacts before intent routing.

    The original transcript is still retained in chat.  This normalized copy is
    only used for deterministic routing/contract extraction, so a dropped
    "let's" cannot turn "let's do a short" into the title "s do a short".
    """
    text = re.sub(r"\s+", " ", str(user_text or "")).strip()
    if not text:
        return ""
    # xAI/browser STT occasionally drops the opening "let" from "let's".
    text = re.sub(
        r"^(?:uh+|um+|okay[, ]+)?s\s+(do|make|create)\s+(a\s+)?(short|video)\b",
        lambda m: f"let's {m.group(1)} {m.group(2) or ''}{m.group(3)}",
        text,
        flags=re.I,
    )
    # Collapse obvious adjacent STT stutters without rewriting ordinary prose.
    text = re.sub(r"\b([A-Za-z]{2,})\s+\1\b", r"\1", text, flags=re.I)
    return text


def extract_quoted_spans(value: str, *, min_chars: int = 8, max_chars: int = 140) -> list[str]:
    """Quoted spans, without mistaking an apostrophe for a quotation mark.

    The apostrophe used to be a delimiter, so ordinary dictation hijacked the
    production title. "you should be able to tell what's making it do good and
    what's making it do bad" opened a quote inside the first contraction and
    closed it inside the second, yielding "s making it do good and what" -
    which then locked, and because quoted titles always win it outranked the
    correct title on every later turn. A real short was rendered from it.

    Double quotes are unambiguous. A single quote only delimits when it sits
    outside a word on both ends, which is exactly what a contraction never
    does.
    """
    text = str(value or "")
    spans: list[str] = []
    patterns = (
        r"[\"“]([^\"”\n]{%d,%d})[\"”]" % (min_chars, max_chars),
        r"(?<![\w])['‘]([^'’\n]{%d,%d})['’](?![\w])" % (min_chars, max_chars),
    )
    for pattern in patterns:
        spans.extend(re.findall(pattern, text))
    return spans


def extract_user_locked_title(user_text: str) -> str:
    """Extract an explicit user-chosen working title from natural chat.

    Critical path: \"yes make Why Men Suddenly… but only 30 seconds long\"
    must lock THAT title — never leave production on a prior Ready short.
    """
    value = str(user_text or "").strip()
    if not value:
        return ""
    low = value.lower()

    # A creator may cite several successful titles as style references.  In
    # that context, "Title is ... or ..." describes the examples and must not
    # become the current production title.
    if (
        re.search(r"\b(?:for example|examples?|style sort of like|those type of videos)\b", low)
        and re.search(r"\btitle is\b", low)
        and len(re.findall(r"\bor\b", low)) >= 2
    ):
        return ""

    # Dictation commonly produces conversational scaffolding such as
    # "let's do a short on …".  That scaffolding is never a title; retain the
    # actual subject so a day-trading request cannot become "s do a short".
    brief = re.search(
        r"\b(?:let'?s|lets|we(?:'| a)?ll)\s+(?:do|make|create)\s+(?:a\s+)?(?:short|video)\s+"
        r"(?:on|about|covering)\s+(.+?)(?:[.!?]|$)",
        value,
        flags=re.I,
    )
    if brief:
        subject = _clean_title_fragment(brief.group(1))
        subject_low = subject.lower()
        if "day trad" in subject_low and re.search(r"\b(?:three|3)\b", subject_low):
            return "3 Things Day Traders Should Never Do"
        if len(subject) >= 8:
            return subject[:120]

    # Pure rejection diagnostics name the wrong title — never lock that.
    is_rejection = bool(
        re.search(r"\bwhy do you keep (?:trying to )?make\b|\bstop making\b|\bwrong title\b", low)
    )
    # Affirmative title choice in the same message still wins.
    affirm = re.search(
        r"(?:if we are|we are|we'?re|we will be|we will|i am|i'?m)\s+making\s+(.{8,160}?)(?:\.|$|\?|!|\n)",
        value,
        flags=re.IGNORECASE,
    )
    if affirm:
        cand = _clean_title_fragment(affirm.group(1))
        if len(cand) >= 8:
            return cand
    if is_rejection:
        return ""

    # "We will do the title, Why Men Love Bomb Then Disappear"
    titled = re.search(
        r"\b(?:we will do|we'?ll do|let'?s do|go with|lock(?:ed)? in)\s+(?:the\s+)?title\s*[,:\-]?\s*(.{8,140})",
        value,
        flags=re.I,
    )
    if titled:
        cand = _clean_title_fragment(titled.group(1))
        cand = re.sub(r"(?i)^(the\s+)?title\s*[,:\-]?\s*", "", cand).strip()
        cand = re.split(
            r"(?i)\b(?:make the first|and animate|because|keep it|for sure)\b",
            cand,
            maxsplit=1,
        )[0].strip(" ,.-")
        if len(cand) >= 8 and len(re.findall(r"[A-Za-z0-9']+", cand)) >= 3:
            return cand[:120]

    # Quoted titles always win.
    quoted = extract_quoted_spans(value, min_chars=8, max_chars=140)
    for q in reversed(quoted):
        cleaned = _clean_title_fragment(q)
        if len(cleaned) >= 8:
            return cleaned

    # Hard-commit forms that previously FAILED to extract a title (root bug):
    #   "yes make Why Men Suddenly Pull Away When You Show You Care but only make it 30 seconds long"
    #   "yes make Why Men Suddenly… only 30 seconds"
    hard_patterns = [
        r"\b(?:yes|yeah|yep|sure|ok(?:ay)?|go ahead)[,.]?\s+"
        r"(?:make|render|produce)\s+"
        r"(?!it\b|this\b|that\b|the short\b|the video\b|a short\b|exactly\b)"
        r"(.+?)"
        r"(?:\s+but\s+only|\s+only\s+\d|\s*$)",
        r"\b(?:make|render|produce)\s+"
        r"(?!it\b|this\b|that\b|the short\b|the video\b|a short\b|exactly\b|one\b|1\b)"
        r"(.+?)"
        r"(?:\s+but\s+only|\s+only\s+\d+\s*(?:s|sec|second|seconds|m|min|minutes)?\b)",
        r"(?:lock(?:ed)?\s+in(?:\s+on)?|go(?:ing)?\s+with|title\s+is|working\s+title\s+is)\s+[:\-]?\s*(.{8,160}?)(?:\.|$|\?|!|\n)",
        r"(?:let'?s|lets)\s+make\s+[\"“']?([^\"”'\n.]{8,160})[\"”']?",
    ]
    for pattern in hard_patterns:
        m = re.search(pattern, value, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            continue
        cand = _clean_title_fragment(m.group(1))
        # Drop pronouns / non-titles
        if cand.lower() in {"it", "this", "that", "the short", "the video", "a short"}:
            continue
        if len(cand) >= 8 and len(re.findall(r"[A-Za-z0-9']+", cand)) >= 3:
            return cand
    return ""


def extract_rejected_title(user_text: str) -> str:
    """Title the user is correcting away from (diagnostic)."""
    value = str(user_text or "").strip()
    m = re.search(
        # Ordinary contrast ("that example is not the style I'm going for")
        # is not a title rejection.  The old generic ``not`` branch captured
        # the rest of the sentence and poisoned the working-title ledger.
        r"(?:why do you keep trying to make|stop making|not making|wrong title(?: is)?|"
        r"(?:the )?(?:title|topic|video idea) (?:is|was) not)\s+"
        r"[\"“']?([^\"”'\n?.]{8,140})[\"”']?",
        value,
        flags=re.IGNORECASE,
    )
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip(" -:,.?!")[:200]


def infer_intent_updates_from_user(
    user_text: str,
    session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Lightweight niche/product/title extraction for continuity.

    Signature accepts optional session for callers that pass it (must not TypeError —
    a prior arity bug wiped all session continuity + catalyst from the system prompt).
    """
    _ = session  # reserved for future channel-aware extraction
    user_text = normalize_spoken_request(user_text)
    out: dict[str, Any] = {}
    low = str(user_text or "").lower()
    try:
        from studio_agent.turn_plan import extract_known_niche_phrase

        known = extract_known_niche_phrase(user_text)
        if known:
            out["niche"] = known
            if re.search(r"\b(?:not|rather than|instead of)\b", low):
                out["clear_locked_title"] = True
    except Exception:
        pass
    # Art style is a production contract, not soft chat decoration.  Persist
    # it only when the creator explicitly names one of Studio's registry
    # styles; a previous picker selection then cannot contaminate the next job.
    try:
        from studio_agent.render_styles import explicit_render_style_from_text

        style = explicit_render_style_from_text(user_text)
        if style:
            out["render_style"] = style
    except Exception:
        pass
    if re.search(r"\b(?:product\s+ad|ads?|course|saas)\b", low) and "course" in low:
        out.setdefault("mode", "product_ad")
    locked = extract_user_locked_title(user_text)
    if locked:
        # If user is rejecting a title, extract_user_locked_title skips lock — also
        # try the affirmative "we are making X" branch which already returns X.
        out["locked_title"] = locked
        out["last_topic"] = locked
    elif session and isinstance(session, dict):
        try:
            from studio_agent.store import (
                _title_overlap_score,
                _user_affirms_assistant_topic,
                extract_production_title_from_assistant,
                get_locked_working_title,
                is_hard_production_commit,
                is_scene_one_proof_commit,
                prior_production_title,
            )

            messages = list(session.get("messages") or [])
            if (
                _user_affirms_assistant_topic(user_text)
                or is_hard_production_commit(user_text)
                or is_scene_one_proof_commit(user_text)
            ):
                from_outline = extract_production_title_from_assistant(messages)
                if from_outline:
                    out["locked_title"] = from_outline
                    out["last_topic"] = from_outline
                elif is_scene_one_proof_commit(user_text) or is_hard_production_commit(user_text):
                    prior = prior_production_title(session)
                    locked_now = get_locked_working_title(session)
                    pending = session.get("pending_concept")
                    pending_title = (
                        str(pending.get("title") or "").strip()
                        if isinstance(pending, dict)
                        else ""
                    )
                    if pending_title and prior and _title_overlap_score(pending_title, prior) < 0.75:
                        out["locked_title"] = pending_title[:200]
                        out["last_topic"] = pending_title[:200]
                    elif locked_now and prior and _title_overlap_score(locked_now, prior) >= 0.75:
                        out["clear_locked_title"] = True
        except Exception:
            pass
    return out


def merge_catalyst_into_intent(
    intent: dict[str, Any] | None,
    *,
    catalyst_notes: list[str] | None = None,
    notes: list[str] | None = None,
    predicted_topics: list[Any] | None = None,
    last_learning: str = "",
    **_extra: Any,
) -> dict[str, Any]:
    base = dict(intent or {})
    locked = str(base.get("locked_title") or base.get("working_title") or "").strip().lower()
    topic_notes: list[str] = []
    if predicted_topics:
        for row in predicted_topics[:4]:
            if isinstance(row, dict):
                topic = str(row.get("topic") or row.get("title") or "").strip()
            else:
                topic = str(row or "").strip()
            if not topic:
                continue
            # Never let catalyst competitor titles replace the user's locked title.
            if locked and topic.lower() != locked:
                topic_notes.append(f"Competitor/research angle (not our title): {topic[:140]}")
            else:
                topic_notes.append(topic[:160])
    # Meta notes after research angles; locked title stays separate in intent.
    merged_notes = list(catalyst_notes or []) + list(notes or []) + topic_notes
    if merged_notes:
        base["catalyst_notes"] = [str(n)[:200] for n in merged_notes[:8]]
    if last_learning:
        base["last_learning"] = str(last_learning)[:300]
    return base


def format_catalyst_for_prompt(session: dict[str, Any] | None) -> str:
    intent = get_conversation_intent(session)
    notes = intent.get("catalyst_notes") if isinstance(intent.get("catalyst_notes"), list) else []
    learning = str(intent.get("last_learning") or "").strip()
    locked = str(intent.get("locked_title") or intent.get("working_title") or "").strip()

    # Durable Catalyst channel memory (hooks / packaging / pacing / visuals) —
    # must feed every plan/production turn, not only session-local notes.
    memory_lines: list[str] = []
    try:
        from studio_agent.catalyst_skeleton_reference import (
            _load_runtime_channel_bucket,
            get_skeleton_visual_directives,
        )

        sess = session if isinstance(session, dict) else {}
        channel_key = (
            str(sess.get("registry_key") or "").strip()
            or str(sess.get("channel_title") or "").strip().lower().replace(" ", "")
            or str(sess.get("channel_id") or "").strip()
            or "mrskelewelly"
        )
        bucket = _load_runtime_channel_bucket(channel_key)
        for key, label in (
            ("hook_learnings", "Hook wins"),
            ("packaging_learnings", "Packaging"),
            ("pacing_learnings", "Pacing"),
            ("retention_watchouts", "Retention watchouts"),
            ("next_video_moves", "Next moves"),
        ):
            values = bucket.get(key) if isinstance(bucket, dict) else None
            if not isinstance(values, list):
                continue
            for item in values[:2]:
                text = " ".join(str(item or "").split()).strip()
                if text:
                    memory_lines.append(f"- {label}: {text[:180]}")
        directives = get_skeleton_visual_directives(channel_key)
        for item in list(directives.get("visual_watchouts") or [])[:2]:
            text = " ".join(str(item or "").split()).strip()
            if text:
                memory_lines.append(f"- Visual watchout: {text[:180]}")
        for item in list(directives.get("visual_wins") or [])[:2]:
            text = " ".join(str(item or "").split()).strip()
            if text:
                memory_lines.append(f"- Visual win: {text[:180]}")
    except Exception:
        memory_lines = []

    if not notes and not learning and not locked and not memory_lines:
        return ""
    lines = [
        "Catalyst producer memory (mandatory — use this to plan the next short/long; "
        "do not invent channel history that contradicts these notes):"
    ]
    if locked:
        lines.append(
            f"- Our locked working title is \"{locked}\". Cite other titles only as public-data comps, never as the video we are making."
        )
    for n in notes[:4]:
        lines.append(f"- {n}")
    if learning:
        lines.append(f"- {learning}")
    lines.extend(memory_lines[:8])
    return "\n".join(lines)


def weave_catalyst_into_reply(reply: str, intent: dict[str, Any] | None) -> str:
    """Optionally append one short catalyst note in natural language."""
    text = strip_robot_research_artifacts(str(reply or "").strip())
    if not text:
        return text
    intent = intent if isinstance(intent, dict) else {}
    locked = str(intent.get("locked_title") or intent.get("working_title") or "").strip()
    notes = intent.get("catalyst_notes") if isinstance(intent.get("catalyst_notes"), list) else []
    learning = str(intent.get("last_learning") or "").strip()
    # Prefer concrete research angles over generic meta tips when both exist.
    candidates: list[str] = []
    for n in notes:
        n_s = str(n or "").strip()
        if not n_s:
            continue
        if locked and locked.lower() not in n_s.lower() and re.search(
            r"\b(?:why men|pull away|competitor|research angle)\b", n_s, re.I
        ):
            # Keep research angles only when no locked title is set.
            if locked:
                continue
        candidates.append(n_s)
    tip = ""
    for n_s in candidates:
        if re.search(r"research angle|predicted|public|trap|revenge|retention|ctr", n_s, re.I):
            tip = n_s
            break
    if not tip and candidates:
        tip = candidates[0]
    if not tip:
        tip = learning
    if not tip or tip.lower() in text.lower():
        return text
    # Never append a raw competitor title as "Catalyst note: Why Men Pull Away..."
    if locked and tip.lower() != locked.lower() and len(tip.split()) <= 14 and tip[:1].isupper():
        return text
    return f"{text}\n\nCatalyst note: {tip[:180]}"


def conversational_production_status(
    *,
    status: str = "",
    scene_count: int = 0,
    approved_count: int = 0,
    message: str = "",
    kind: str = "",
    job_note: str = "",
    percent: Any = None,
    error: str = "",
    **_extra: Any,
) -> str:
    st = str(status or "").strip().lower()
    kind_l = str(kind or "").strip().lower()
    if message:
        return strip_robot_research_artifacts(str(message))
    if st in {"awaiting_scene_review", "awaiting_approval"}:
        if scene_count:
            return (
                f"Your stills are ready for review ({approved_count}/{scene_count} approved so far). "
                "Tell me what to fix in plain language, or say approve to keep going."
            )
        return "Production is waiting on your review. Tell me what to change or say approve."
    if st in {"running", "generating", "animating"}:
        if kind_l == "competitor":
            note = str(job_note or "Still analyzing the reference.").strip()
            pct = f" ({percent}%)" if percent is not None and str(percent) != "" else ""
            return f"Reference analysis is running{pct}. {note}"
        if kind_l == "cliplab":
            return str(job_note or "ClipLab is still processing — hang tight.")
        return "I'm still building this production — hang tight, I'll update you when the next stage lands."
    if st in {"complete", "completed", "done"}:
        if kind_l == "competitor":
            return "Reference analysis is complete."
        return "Your video is ready. Open the deliverable in chat to preview or download."
    if st in {"failed", "error"}:
        if kind_l == "competitor":
            return f"Reference analysis hit a blocker: {error or 'unknown error'}. Want me to retry?"
        return "That production hit a blocker. Say retry and I'll restart from the last good stage."
    return "Production update: still working."


def conversational_production_prepared(
    *,
    title: str = "",
    tool: str = "",
    auto_start: bool = False,
    **_extra: Any,
) -> str:
    t = str(title or "your short").strip()
    if auto_start:
        return f"Starting production for **{t}** now."
    return (
        f"I've prepared production for **{t}**. "
        "Approve when you're ready and I'll start building the scenes."
    )


def conversational_scene_fix_reply(
    *,
    scene_idx: int | None = None,
    scene_index: int | None = None,
    note: str = "",
    ok: bool = True,
    **_extra: Any,
) -> str:
    idx = scene_idx if scene_idx is not None else scene_index
    if idx is not None:
        base = f"Got it — updating scene {int(idx) + 1}"
        if note:
            return f"{base} ({note})."
        if ok:
            return f"{base}."
        return f"{base} — need one more detail."
    return "Got it — applying that scene fix now."
