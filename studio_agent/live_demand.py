"""Niche-agnostic Live Demand layer for Studio Agent.

Forces fresh public YouTube (and optional off-YouTube) evidence before claiming
what people want, and before demand-grounded content or product-ad production.
Works for every niche — not hard-coded to finance/day trading.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from studio_agent import store

MODE_CONTENT = "content_creation"
MODE_PRODUCT_AD = "product_ad"

# User phrasing that means "ground this in live audience demand"
_DEMAND_SIGNAL_PHRASES = (
    "what people want",
    "what people actually want",
    "what people are wanting",
    "what people want to know",
    "what people actually want to know",
    "what people are searching",
    "what people search",
    "what audiences want",
    "what viewers want",
    "whats working",
    "what's working",
    "what is working",
    "what works",
    "whats working mainly",
    "what's working mainly",
    "what is working mainly",
    "working mainly",
    "what is performing",
    "whats performing",
    "what's performing",
    "top performing",
    "best performing",
    "public demand",
    "market demand",
    "current demand",
    "live demand",
    "fresh demand",
    "based on demand",
    "based on what people",
    "based off what people",
    "highest chance of going viral",
    "most likely to go viral",
    "go viral",
    "going viral",
    "viral short",
    "viral video",
    "trending in",
    "currently trending",
    "trending now",
    "right now",
    "as updated as",
    "most up to date",
    "up-to-date",
    "up to date",
    "last 24 hours",
    "last 12 hours",
    "past 24 hours",
    "past 12 hours",
    "last day",
    "today's market",
    "market moves",
    "24/7",
)

_PRODUCTION_SIGNAL = re.compile(
    r"\b(?:make|create|generate|produce|build|render|start|ship|film|shoot)\b.+"
    r"\b(?:short|video|ad|ads|commercial|clip)\b|"
    r"\b(?:short|video|ad)\b.+\b(?:about|on|for)\b",
    re.I,
)

_NICHE_ABOUT_RE = re.compile(
    r"(?i)\b(?:about|on|for|in(?:\s+the)?)\s+([a-z0-9][a-z0-9 &'/+\-]{2,60}?)(?:\s+(?:niche|shorts?|videos?|content|ads?|market))?(?:[.!?,]|$)",
)


@dataclass
class LiveDemandPlan:
    """Resolved live-demand requirements for one agent turn."""

    required: bool = False
    mode: str = MODE_CONTENT  # content_creation | product_ad
    window_days: int = 2
    fresh: bool = True
    force_before_production: bool = False
    include_channel_analytics: bool = False
    niche_hint: str = ""
    search_query: str = ""
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "required": self.required,
            "mode": self.mode,
            "window_days": self.window_days,
            "fresh": self.fresh,
            "force_before_production": self.force_before_production,
            "include_channel_analytics": self.include_channel_analytics,
            "niche_hint": self.niche_hint,
            "search_query": self.search_query,
            "reasons": list(self.reasons),
        }


def _low(text: str) -> str:
    return store._user_message_before_attachments(text).lower()


_NICHE_STOPWORDS = frozenset({
    "what", "people", "actually", "want", "know", "learn", "based", "about", "short",
    "video", "make", "create", "need", "able", "this", "that", "the", "a", "my", "our",
    "their", "your", "how", "why", "when", "where", "who", "which", "them", "they",
})


def extract_niche_hint(user_text: str, session: dict[str, Any] | None = None) -> str:
    """Best-effort niche label from user text, prior turns, or session channel."""
    from studio_agent.turn_plan import extract_explicit_topic_phrase, extract_known_niche_phrase

    raw = store._user_message_before_attachments(user_text)
    low = raw.lower()

    # Prefer the concrete topic in THIS turn over stale session niche / dark psychology.
    explicit = extract_explicit_topic_phrase(raw)
    if explicit:
        return explicit

    known = extract_known_niche_phrase(raw)
    if known:
        return known

    # Common compact niches in free text (prefer these over brittle regex)
    for phrase in (
        "day trading",
        "swing trading",
        "stock market",
        "crypto trading",
        "crypto",
        "forex",
        "dark psychology",
        "relationship psychology",
        "self improvement",
        "fitness",
        "weight loss",
        "true crime",
        "history documentary",
        "ai tools",
        "saas",
        "ecommerce",
        "dropshipping",
        "product ads",
        "faceless youtube",
    ):
        if phrase in low:
            return phrase

    # Explicit "day trading niche" / "fitness niche"
    m = re.search(r"(?i)\b([a-z0-9][a-z0-9 &'/+\-]{2,48}?)\s+niche\b", raw)
    if m:
        label = re.sub(r"\s+", " ", m.group(1)).strip(" ,.-")
        first = label.split()[0].lower() if label else ""
        if label and first not in _NICHE_STOPWORDS | {"this", "that", "the", "a", "my", "our"}:
            return label[:80]

    # "about day trading" — take the tail after about/on, skip demand filler
    m2 = re.search(
        r"(?i)\b(?:want to know about|know about|learn about|about|on)\s+([a-z0-9][a-z0-9 &'/+\-]{2,60})",
        raw,
    )
    if m2:
        label = re.sub(r"\s+", " ", m2.group(1)).strip(" ,.-")
        label = re.sub(
            r"(?i)\b(?:short|video|content|niche|market|ads?)\b.*$",
            "",
            label,
        ).strip(" ,.-")
        tokens = [t for t in label.split() if t.lower() not in _NICHE_STOPWORDS]
        label = " ".join(tokens).strip()
        known2 = extract_known_niche_phrase(label)
        if known2:
            return known2
        if len(label) >= 3 and not label.startswith("http") and " " in label:
            return label[:80]

    session = session or {}
    # Continuity: prior user messages / last Live Demand packet (e.g. cost Q mentioned day trading)
    last = session.get("last_live_demand") if isinstance(session.get("last_live_demand"), dict) else {}
    prior_hint = str(last.get("niche_hint") or "").strip()
    if prior_hint and (
        extract_known_niche_phrase(prior_hint)
        or (len(prior_hint) >= 4 and prior_hint.lower() not in {"youtube", "shorts"})
    ):
        # Prefer known phrase form
        return extract_known_niche_phrase(prior_hint) or prior_hint[:80]
    for msg in reversed(list(session.get("messages") or [])[-12:]):
        if str(msg.get("role") or "") != "user":
            continue
        content = store._user_message_before_attachments(str(msg.get("content") or ""))
        if not content or content.strip().lower() == raw.strip().lower():
            continue
        prior = extract_known_niche_phrase(content)
        if prior:
            return prior

    title = str(session.get("channel_title") or "").strip()
    registry = str(session.get("registry_key") or "").strip().replace("_", " ")
    if title and title.lower() not in {"youtube", "channel", "selected"}:
        t_known = extract_known_niche_phrase(title)
        return t_known or title[:80]
    if registry:
        r_known = extract_known_niche_phrase(registry)
        return r_known or registry[:80]
    return ""


def demand_window_days(user_text: str, *, default: int = 2) -> int:
    """Upload recency window. Supports 12–24h markets through 1–2 day windows."""
    low = _low(user_text)
    # Explicit hour windows → 1 day (YouTube publishedAfter granularity is day-level in our path)
    if re.search(r"\b(?:12|24)\s*hours?\b", low) or "last day" in low or "past day" in low:
        return 1
    range_match = re.search(r"\b(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\s*days?\b", low)
    if range_match:
        upper = max(int(range_match.group(1)), int(range_match.group(2)))
        return max(1, min(upper, 90))
    last_match = re.search(r"\blast\s+(\d{1,2})\s*days?\b", low)
    if last_match:
        return max(1, min(int(last_match.group(1)), 90))
    if any(
        phrase in low
        for phrase in (
            "right now",
            "today",
            "trending now",
            "currently trending",
            "real time",
            "real-time",
            "realtime",
            "upload velocity",
            "24/7",
            "market moves",
            "as updated",
            "most recent",
            "latest",
            "live demand",
            "fresh demand",
            "what people actually want",
        )
    ):
        return max(1, min(default, 2))
    if any(phrase in low for phrase in ("this week", "past week", "last week")):
        return 7
    return max(1, min(int(default or 2), 90))


def has_demand_signal(user_text: str) -> bool:
    low = _low(user_text)
    if not low:
        return False
    # Normalize casual typos: whats -> what's handling via space strip
    compact = re.sub(r"[^a-z0-9\s]", " ", low)
    compact = re.sub(r"\s+", " ", compact).strip()
    if any(phrase in low or phrase in compact for phrase in _DEMAND_SIGNAL_PHRASES):
        return True
    if re.search(r"\b(?:viral|trending|demand|niche research|market research)\b", low):
        return True
    if re.search(r"\bresearch\b", low) and re.search(
        r"\b(?:working|performing|viral|trend|niche|market|demand|shorts?|youtube)\b",
        low,
    ):
        return True
    if "what people" in low and any(w in low for w in ("want", "watch", "search", "care", "learn")):
        return True
    return False


def is_research_execution_request(user_text: str) -> bool:
    """True when the user wants Studio to research/pull live demand evidence now.

    Conversation-first: natural-language demand questions count as research execution
    (Grok/ChatGPT style) — Haiku should not wait for an explicit 'run tools' phrase.
    """
    low = _low(user_text)
    if not low:
        return False
    if has_demand_signal(user_text) and re.search(
        r"\b(?:research|pull|check|find|get|fetch|look up|analyze|analyse|study|"
        r"verify|confirm|show|tell|what(?:'s| is| are))\b",
        low,
    ):
        return True
    if re.search(r"\bresearch\b", low) and re.search(
        r"\b(?:what'?s|whats|what is|working|performing|viral|niche|market|demand)\b",
        low,
    ):
        return True
    # "what's working for day trading" / "day trading shorts demand right now"
    if has_demand_signal(user_text) and re.search(
        r"\b(?:day\s*trad|trading|crypto|forex|psycholog|fitness|niche|shorts?|youtube|market)\b",
        low,
    ):
        return True
    return False


def is_demand_grounded_production(user_text: str) -> bool:
    """Make/produce a short/ad AND ground it in audience demand."""
    low = _low(user_text)
    if not low:
        return False
    if not has_demand_signal(user_text) and not re.search(
        r"\b(?:based on|using|from)\b.+\b(?:data|demand|trends?|research|what people)\b",
        low,
    ):
        # Still treat "make a short about X" as demand-worthy when explicit production
        if not (_PRODUCTION_SIGNAL.search(low) or store.is_explicit_production_request(user_text)):
            return False
        # Product ads always need demand + product fit
        if store.is_product_ad_request(user_text):
            return True
        # Organic: require niche-ish about/for
        return bool(re.search(r"\b(?:about|on|for)\b", low) and re.search(r"\b(?:short|video|ad)\b", low))
    return bool(
        _PRODUCTION_SIGNAL.search(low)
        or store.is_explicit_production_request(user_text)
        or store.is_product_ad_request(user_text)
        or re.search(r"\b(?:make|create|produce|build)\b", low)
    )


def build_live_demand_plan(
    user_text: str,
    session: dict[str, Any] | None = None,
    *,
    auto_run: bool | None = None,
) -> LiveDemandPlan:
    """Classify whether this turn needs Live Demand and how to fetch it."""
    session = session or {}
    if auto_run is None:
        auto_run = store.should_auto_run_tools(user_text)
    plan = LiveDemandPlan()
    if not auto_run and not has_demand_signal(user_text):
        return plan

    mode = store.detect_production_intent(user_text, session)
    plan.mode = mode if mode in {MODE_CONTENT, MODE_PRODUCT_AD} else MODE_CONTENT
    plan.niche_hint = extract_niche_hint(user_text, session)
    plan.window_days = demand_window_days(user_text, default=2 if plan.mode == MODE_CONTENT else 7)
    # Cache-first by default. fresh=true only for explicit live/right-now language
    # (each fresh dual-order search burns 200 units/query against the 10k daily cap).
    low = _low(user_text)
    plan.fresh = bool(
        re.search(
            r"\b(?:right now|right-now|live demand|real[- ]?time|fresh|bypass cache|"
            r"most recent|trending now|currently trending|today only)\b",
            low,
        )
        or "last 24" in low
        or "last 12" in low
    )

    demand = has_demand_signal(user_text)
    productionish = is_demand_grounded_production(user_text) or store.is_explicit_production_request(user_text)
    product_ad = plan.mode == MODE_PRODUCT_AD or store.is_product_ad_request(user_text)
    research_only = demand and not productionish
    research_exec = is_research_execution_request(user_text)

    # Ideation / research turns that mention demand
    if demand and (store.is_ideation_request(user_text) or research_only or productionish or research_exec):
        plan.required = True
        plan.reasons.append("demand_language")
    if research_exec:
        plan.required = True
        plan.reasons.append("research_execution")

    # Production grounded in audience want
    if productionish and (demand or product_ad or plan.niche_hint):
        plan.required = True
        plan.force_before_production = True
        plan.reasons.append("demand_grounded_production")

    # Product ads always pull niche demand when tools should run
    if product_ad and auto_run:
        plan.required = True
        plan.force_before_production = True
        plan.reasons.append("product_ad_mode")

    # Explicit public YouTube research already handled elsewhere — still mark required
    if store.is_public_youtube_research_request(user_text):
        plan.required = True
        plan.reasons.append("public_youtube_research")

    if not plan.required:
        return plan

    # Connected channel analytics when user has a channel selected and asks for "for my channel"
    low = _low(user_text)
    if session.get("channel_id") or session.get("registry_key"):
        if any(p in low for p in ("my channel", "this channel", "for the channel", "our channel")):
            plan.include_channel_analytics = True
            plan.reasons.append("connected_channel")

    # Build a search query seed (never chat-filler soup)
    niche = (plan.niche_hint or "").strip()
    if not niche:
        # Session continuity: prior Live Demand / conversation intent
        last = session.get("last_live_demand") if isinstance(session.get("last_live_demand"), dict) else {}
        intent = session.get("conversation_intent") if isinstance(session.get("conversation_intent"), dict) else {}
        niche = str(
            intent.get("niche")
            or last.get("niche_hint")
            or ""
        ).strip()
        if niche:
            plan.niche_hint = niche
            plan.reasons.append("session_niche_continuity")
    if product_ad:
        if niche and "product" not in niche.lower() and "ad" not in niche.lower():
            plan.search_query = f"{niche} course product ads YouTube Shorts"[:220]
        elif niche:
            plan.search_query = f"{niche} YouTube Shorts"[:220]
        else:
            plan.search_query = "online course product ads YouTube Shorts"
    else:
        if not niche:
            # Generic "what people watch" — use a real Shorts discovery query that
            # YouTube search.list actually returns (not "faceless hooks" soup).
            plan.search_query = DISCOVERY_SHORTS_QUERIES[0]
            plan.reasons.append("discovery_fallback_no_niche")
            # Discovery needs a slightly wider window than 1–2d niche research.
            if plan.window_days < 7:
                plan.window_days = 7
        elif "youtube" in niche.lower():
            plan.search_query = niche[:220]
        else:
            plan.search_query = f"{niche} YouTube Shorts".strip()[:220]

    return plan


# Proven searchable Shorts discovery seeds (order = priority).
# Must be content-bearing (survive sanitize/garbage checks). Never "faceless hooks"
# or all-stopword soup like "youtube shorts viral".
DISCOVERY_SHORTS_QUERIES = (
    "shorts storytime",
    "satisfying shorts",
    "life advice shorts",
    "relationship advice shorts",
    "gym motivation shorts",
    "viral storytime shorts",
)


def discovery_search_queries() -> list[str]:
    return list(DISCOVERY_SHORTS_QUERIES)


def resolve_demand_search_query(
    user_text: str,
    session: dict[str, Any] | None = None,
    *,
    active_label: str = "",
    registry_key: str = "",
    fallback_query: str = "",
) -> str:
    """Single source of truth for Live Demand YouTube q (never dictation soup)."""
    from studio_agent.turn_plan import (
        channel_fallback_search_query,
        coerce_public_search_query,
        extract_known_niche_phrase,
        is_garbage_public_search_query,
        is_unusable_public_search_query,
        refine_public_search_query,
    )

    session = session or {}
    from studio_agent.turn_plan import extract_explicit_topic_phrase

    plan = build_live_demand_plan(user_text, session, auto_run=True)
    explicit = extract_explicit_topic_phrase(user_text)
    # THIS-turn concrete topic always outranks stale last_live_demand / dark psychology.
    candidates = [
        f"{explicit} YouTube Shorts" if explicit else "",
        f"{plan.niche_hint} YouTube Shorts" if plan.niche_hint else "",
        extract_known_niche_phrase(user_text),
        # Only use plan.search_query if it is a real niche query, not discovery soup
        plan.search_query if plan.niche_hint else "",
        fallback_query,
        str((session.get("conversation_intent") or {}).get("niche") or ""),
        str((session.get("last_live_demand") or {}).get("niche_hint") or ""),
        str((session.get("last_live_demand") or {}).get("search_query") or ""),
        channel_fallback_search_query(active_label, registry_key),
        *DISCOVERY_SHORTS_QUERIES,
    ]
    for raw in candidates:
        cleaned = str(raw or "").strip()
        if not cleaned:
            continue
        known = extract_known_niche_phrase(cleaned)
        if known and "youtube" not in cleaned.lower():
            cleaned = f"{known} YouTube Shorts"
        refined = refine_public_search_query(cleaned) or cleaned
        # Prefer refine result over coerce when coerce turns discovery into unusable soup
        coerced = coerce_public_search_query(
            refined,
            user_text=user_text if known else "",  # don't re-parse dictation soup
            active_label=active_label,
            registry_key=registry_key,
            fallback_query=refined,
        )
        q = refined
        if coerced and not is_garbage_public_search_query(coerced) and not is_unusable_public_search_query(coerced):
            q = coerced
        if q and not is_garbage_public_search_query(q) and not is_unusable_public_search_query(q):
            out = refine_public_search_query(q) or q
            # Avoid "… YouTube Shorts YouTube Shorts" when candidates already include the suffix.
            from studio_agent.turn_plan import with_youtube_shorts_suffix

            return with_youtube_shorts_suffix(out)
    return DISCOVERY_SHORTS_QUERIES[0]


def format_live_demand_system_note(plan: LiveDemandPlan) -> str:
    """System message injected so the model cannot invent demand without tools."""
    mode_label = "product ad" if plan.mode == MODE_PRODUCT_AD else "organic YouTube content"
    return (
        "[Studio Agent Live Demand — mandatory]\n"
        f"Mode: {mode_label}. Niche hint: {plan.niche_hint or '(infer from tools)'}.\n"
        f"Recency window: last {plan.window_days} day(s), fresh=true (bypass cache).\n"
        "Rules:\n"
        "1. Answer in natural conversation (Grok-class). Lead with the user's answer, then insights, "
        "then a clear next step (script/short/ad). Do not reply with only a rigid research form.\n"
        "2. Before claiming what people want / what will go viral, cite hydrated public YouTube rows "
        "(title, channel, views, published_at, cache_status, support_label).\n"
        "3. Do not invent view counts, trends, or search volume.\n"
        "4. Prefer recent_momentum rows inside the window over old top_performers for 'right now' claims.\n"
        "5. For product ads: map demand hooks to the product CTA after ingest_product_reference.\n"
        "6. If quota/cache is stale, say so explicitly and still use support_label.\n"
        f"Planned search seed: {plan.search_query}"
    )


def format_demand_brief_from_tool_fires(
    tool_fires: list[Any],
    *,
    plan: LiveDemandPlan | None = None,
    limit: int = 6,
) -> str:
    """Compact production brief from get_public_search_trends / search_youtube_public fires."""
    import json

    rows: list[dict[str, Any]] = []
    predicted: list[Any] = []
    queries: list[str] = []
    window_days = plan.window_days if plan else None
    for fire in tool_fires or []:
        name = str(getattr(fire, "name", "") or "")
        if name not in {"get_public_search_trends", "search_youtube_public", "recommend_video_topics"}:
            continue
        try:
            data = json.loads(getattr(fire, "result", None) or "{}")
        except Exception:
            continue
        if not isinstance(data, dict) or data.get("error"):
            continue
        if data.get("window_days") is not None:
            try:
                window_days = int(data.get("window_days"))
            except (TypeError, ValueError):
                pass
        for q in data.get("queries") or []:
            if q and str(q) not in queries:
                queries.append(str(q))
        q = data.get("query")
        if q and str(q) not in queries:
            queries.append(str(q))
        for key in ("videos", "results", "items"):
            batch = data.get(key)
            if isinstance(batch, list):
                for row in batch:
                    if isinstance(row, dict):
                        rows.append(row)
        preds = data.get("predicted_topics") or data.get("topics") or []
        if isinstance(preds, list):
            predicted.extend(preds)

    # Prefer hydrated recent rows
    def _row_score(row: dict[str, Any]) -> float:
        level = str(row.get("evidence_level") or "")
        views = float(row.get("views") or row.get("view_count") or 0)
        vpd = float(row.get("views_per_day") or 0)
        age = float(row.get("age_days") or 999)
        score = views
        if level == "hydrated_video_stats":
            score += 1_000_000
        if vpd:
            score += vpd * 100
        if age <= float(window_days or 7):
            score += 500_000
        return score

    rows_sorted = sorted(rows, key=_row_score, reverse=True)
    seen_titles: set[str] = set()
    lines: list[str] = [
        "Live Demand brief (grounded — use these angles for the short/ad):",
    ]
    if plan:
        lines.append(
            f"- Mode: {'product ad' if plan.mode == MODE_PRODUCT_AD else 'organic'} | "
            f"niche: {plan.niche_hint or 'inferred'} | window: {window_days or plan.window_days}d fresh"
        )
    if queries:
        lines.append(f"- Queries: {', '.join(queries[:3])}")

    picked = 0
    for row in rows_sorted:
        title = str(row.get("title") or "").strip()
        if not title or title.lower() in seen_titles:
            continue
        seen_titles.add(title.lower())
        views = row.get("views") or row.get("view_count")
        channel = str(row.get("channel_title") or row.get("channel") or "").strip()
        published = str(row.get("published_at") or row.get("published") or "").strip()[:10]
        support = str(row.get("support_label") or row.get("evidence_level") or "").strip()
        bits = [f'"{title}"']
        if channel:
            bits.append(channel)
        if views is not None:
            try:
                bits.append(f"{int(float(views)):,} views")
            except (TypeError, ValueError):
                bits.append(f"{views} views")
        if published:
            bits.append(published)
        if support:
            bits.append(support)
        lines.append(f"- " + " · ".join(bits))
        picked += 1
        if picked >= limit:
            break

    if predicted:
        lines.append("- Predicted topic angles:")
        for pred in predicted[:5]:
            if isinstance(pred, dict):
                t = str(pred.get("title") or pred.get("topic") or pred.get("label") or "").strip()
                if t:
                    lines.append(f"  · {t}")
            elif str(pred).strip():
                lines.append(f"  · {str(pred).strip()}")

    if picked == 0:
        lines.append(
            "- No hydrated public rows returned this turn (quota/cache/empty). "
            "Do not invent demand; retry fresh search or widen the niche query."
        )
    else:
        lines.append(
            "- Production rule: hook + claim must map to at least one row above. "
            "Prefer recent_momentum over evergreen outliers when user asked for right-now demand."
        )
    return "\n".join(lines)


def inject_demand_into_production_args(
    args: dict[str, Any],
    *,
    brief: str,
    plan: LiveDemandPlan | None = None,
) -> dict[str, Any]:
    """Attach demand brief into shortform start args without wiping user topic."""
    merged = dict(args or {})
    if brief:
        existing = str(merged.get("visual_brief") or "").strip()
        demand_block = brief[:1800]
        if demand_block not in existing:
            merged["visual_brief"] = (
                f"{existing}\n\n{demand_block}".strip() if existing else demand_block
            )
        notes = str(merged.get("user_request") or "").strip()
        if "Live Demand" not in notes:
            merged["user_request"] = (
                f"{notes}\n[Live Demand grounded production]".strip()
                if notes
                else "[Live Demand grounded production]"
            )
    if plan and plan.niche_hint and not str(merged.get("topic") or "").strip():
        # Soft topic seed only when model/user left topic empty
        merged["topic"] = plan.niche_hint[:120]
    if plan and plan.mode == MODE_PRODUCT_AD:
        merged.setdefault("category_key", "people_blogs")
    return merged
