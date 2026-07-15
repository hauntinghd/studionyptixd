"""Turn-level intent planning for conversation-first Studio Agent orchestration."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from studio_agent import store

STEP_REFERENCE_ANALYSIS = "reference_analysis"
STEP_PUBLIC_YOUTUBE_DEMAND = "public_youtube_demand"
STEP_CHANNEL_ANALYTICS = "channel_analytics"


@dataclass
class TurnPlan:
    conversational_only: bool = False
    reference_analysis: bool = False
    public_youtube_demand: bool = False
    channel_analytics: bool = False
    niche_from_reference: bool = False
    execution_steps: list[str] = field(default_factory=list)

    @property
    def has_execution(self) -> bool:
        return bool(self.execution_steps)


def is_public_youtube_demand_request(text: str, *, auto_run: bool | None = None) -> bool:
    """True when the user wants public YouTube market/demand evidence pulled."""
    low = store._user_message_before_attachments(text).lower()
    if not low:
        return False
    if auto_run is None:
        auto_run = store.should_auto_run_tools(text)
    if not auto_run:
        return False
    # Niche-agnostic Live Demand (all verticals + product ads)
    try:
        from studio_agent.live_demand import build_live_demand_plan, has_demand_signal

        if has_demand_signal(text) or build_live_demand_plan(text, auto_run=auto_run).required:
            return True
    except Exception:
        pass
    demand_phrases = (
        "public youtube data",
        "youtube public data",
        "public youtube",
        "youtube data",
        "youtube market",
        "market data",
        "market research",
        "niche data",
        "niche of the video",
        "niche of this video",
        "public demand",
        "search trend",
        "search trends",
        "trend data",
        "what people are searching",
        "what people want",
        "what people actually want",
        "demand for this niche",
        "for the niche",
        "for this niche",
        "go viral",
        "going viral",
    )
    action_phrases = (
        "pull",
        "get",
        "fetch",
        "run",
        "grab",
        "check",
        "look up",
        "research",
    )
    if re.search(r"\b(?:fresh|live)\s+search\b", low):
        return True
    if store.is_explicit_tool_go_ahead(text) and re.search(r"\bsearch\b", low):
        return True
    if any(phrase in low for phrase in demand_phrases):
        return True
    if any(action in low for action in action_phrases) and any(
        term in low for term in ("youtube", "niche", "market", "trend", "public")
    ):
        return True
    if store.is_ideation_request(text):
        research_terms = (
            "market",
            "niche",
            "public",
            "trend",
            "research",
            "demand",
            "youtube data",
            "search",
            "audience",
            "positioning",
        )
        if any(term in low for term in research_terms):
            return True
    return False


def build_turn_plan(user_text: str, session: dict[str, Any] | None = None) -> TurnPlan:
    """Classify intents and ordered execution steps for this turn."""
    session = session or {}
    auto_run = store.should_auto_run_tools(user_text)
    conversational_only = not auto_run and (
        store.is_conversational_planning_turn(user_text) or store.is_ideation_request(user_text)
    )

    channel_url_reference = store.is_youtube_channel_url_reference_request(user_text)
    reference_analysis = bool(
        auto_run
        and (
            store.is_explicit_reference_analysis_request(user_text)
            or store.is_youtube_url_reference_request(user_text)
            or store.is_contextual_reference_video_request(user_text)
            or channel_url_reference
        )
    )
    live_demand_required = False
    try:
        from studio_agent.live_demand import build_live_demand_plan

        live_demand_required = bool(
            build_live_demand_plan(user_text, session, auto_run=auto_run).required
        )
    except Exception:
        live_demand_required = False
    public_youtube_demand = (
        is_public_youtube_demand_request(user_text, auto_run=auto_run)
        or store.is_public_youtube_research_request(user_text)
        # "We need to find an exact topic first" is a research request, even
        # when the creator does not know the niche keywords yet.  Do not leave
        # this to the chat model and risk a generic/stale topic response.
        or store.is_exact_topic_discovery_request(user_text)
        or live_demand_required
        or (
            store.is_competitor_channel_reference_request(user_text)
            and not channel_url_reference
        )
    )
    low = store._user_message_before_attachments(user_text).lower()
    niche_from_reference = any(
        phrase in low
        for phrase in (
            "niche of the video",
            "niche of this video",
            "niche for the video",
            "market for the video",
            "market of the video",
            "video topic",
            "topic of the video",
            "topic from the video",
            "topic we are getting",
            "getting data on",
            "data on this topic",
        )
    ) or (
        "video" in low
        and "topic" in low
        and any(term in low for term in ("data", "niche", "market", "research", "public"))
    )

    channel_analytics = bool(
        auto_run
        and store.is_connected_channel_performance_request(user_text)
        and not (reference_analysis and not store.is_connected_channel_performance_request(user_text))
    )
    if auto_run and not reference_analysis and not public_youtube_demand:
        channel_analytics = channel_analytics or bool(
            re.search(r"\b(?:pull|get|fetch)\b.+\b(?:channel|analytics)\b", low)
        )

    steps: list[str] = []
    if reference_analysis:
        steps.append(STEP_REFERENCE_ANALYSIS)
    if public_youtube_demand:
        steps.append(STEP_PUBLIC_YOUTUBE_DEMAND)
    if channel_analytics:
        steps.append(STEP_CHANNEL_ANALYTICS)

    return TurnPlan(
        conversational_only=conversational_only,
        reference_analysis=reference_analysis,
        public_youtube_demand=public_youtube_demand,
        channel_analytics=channel_analytics,
        niche_from_reference=niche_from_reference,
        execution_steps=steps,
    )


_CHAT_QUERY_NOISE_RE = re.compile(
    r"\b(?:ok(?:ay)?|yes|try again|go ahead|watch|see|look at|check|pull|fetch|get|run|analyze|"
    r"uploaded|attached|reference video|local_path|agent_video_|\.mp4)\b",
    re.IGNORECASE,
)


def reference_depth_from_payload(payload: dict[str, Any] | None) -> str:
    """Classify how deep a reference-analysis payload is without importing runner."""
    if not isinstance(payload, dict):
        return "missing"
    depth = str(payload.get("analysis_depth") or "").strip().lower()
    if depth:
        return depth
    gaps = payload.get("analysis_gaps") if isinstance(payload.get("analysis_gaps"), dict) else {}
    depth = str(gaps.get("depth") or "").strip().lower()
    if depth:
        return depth
    visual_raw = payload.get("visual_summary")
    if isinstance(visual_raw, dict):
        visual_text = str(visual_raw.get("summary") or "").strip()
    else:
        visual_text = str(visual_raw or "").strip()
    transcript = payload.get("transcript") if isinstance(payload.get("transcript"), dict) else {}
    transcript_text = str(transcript.get("text") or payload.get("transcript_excerpt") or "").strip()
    storytelling = payload.get("storytelling") if isinstance(payload.get("storytelling"), dict) else {}
    has_story = bool(
        str(storytelling.get("summary") or payload.get("storytelling_summary") or "").strip()
        or str(storytelling.get("hook") or payload.get("hook_summary") or "").strip()
    )
    if visual_text and (transcript_text or has_story):
        return "full"
    if visual_text or transcript_text or has_story:
        return "partial"
    if payload.get("pacing"):
        return "pacing_only"
    return "missing"


def reference_has_topic_signal(payload: dict[str, Any] | None) -> bool:
    """True when reference analysis returned enough signal to drive a niche search."""
    if not isinstance(payload, dict):
        return False
    if reference_depth_from_payload(payload) == "pacing_only":
        return False
    storytelling = payload.get("storytelling") if isinstance(payload.get("storytelling"), dict) else {}
    for key, flat_key in (
        ("summary", "storytelling_summary"),
        ("hook", "hook_summary"),
    ):
        if str(storytelling.get(key) or payload.get(flat_key) or "").strip():
            return True
    packaging = storytelling.get("packaging")
    if isinstance(packaging, dict):
        if any(str(packaging.get(field) or "").strip() for field in ("title_angle", "title", "appeal", "thumbnail_concept")):
            return True
    elif str(packaging or payload.get("packaging_notes") or "").strip():
        return True
    transcript = payload.get("transcript") if isinstance(payload.get("transcript"), dict) else {}
    text = str(transcript.get("text") or payload.get("transcript_excerpt") or "").strip()
    if len(text.split()) >= 8:
        return True
    visual_raw = payload.get("visual_summary")
    if isinstance(visual_raw, dict):
        visual_text = str(visual_raw.get("summary") or "").strip()
    else:
        visual_text = str(visual_raw or "").strip()
    return len(visual_text.split()) >= 12


_YOUTUBE_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s<>\"']+",
    re.IGNORECASE,
)
_YOUTUBE_CHANNEL_URL_RE = re.compile(
    r"https?://(?:www\.)?youtube\.com/(?:@([\w.-]+)|channel/([\w-]+))[^\s<>\"']*",
    re.IGNORECASE,
)
_YOUTUBE_SI_PARAM_RE = re.compile(r"[?&]si=[\w-]+", re.IGNORECASE)
_VIDEO_ID_TOKEN_RE = re.compile(r"^[\w-]{11}$", re.IGNORECASE)

_SEARCH_STOPWORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "has", "have", "he",
    "her", "his", "i", "if", "in", "into", "is", "it", "its", "just", "my", "no", "not", "of",
    "on", "or", "our", "she", "so", "that", "the", "their", "them", "then", "there", "these",
    "they", "this", "to", "was", "we", "were", "what", "when", "which", "who", "will", "with",
    "you", "your", "about", "after", "also", "been", "being", "both", "can", "could", "did",
    "does", "doing", "each", "few", "had", "how", "into", "may", "more", "most", "much", "one",
    "only", "other", "over", "same", "some", "such", "than", "too", "very", "while", "would",
    "video", "videos", "short", "shorts", "youtube", "channel", "creator", "content", "using",
    "shows", "show", "like", "make", "made", "many", "every", "each", "frame", "frames",
    "throughout", "visible", "appears", "additional", "maintains", "displays", "combining",
    "style", "text", "black", "reading", "lower", "upper", "right", "left", "with", "data",
    "recent", "latest", "fresh", "public", "search", "demand", "performance", "market", "niche",
    "download", "downloaded", "fully", "watch", "watched", "attached", "uploaded", "link",
    "linked", "paste", "pasted", "url", "youtu", "magnates", "empire",
    # Chat / research-command filler that must NEVER become a YouTube query
    "research", "whats", "working", "mainly", "since", "want", "able", "sell", "given",
    "need", "last", "hours", "hour", "moves", "provide", "possible", "value", "most",
    "actually", "people", "go", "viral", "please", "thanks", "okay", "yes", "sure",
    "should", "could", "would", "gonna", "gotta", "kinda", "really", "basically",
    "information", "info", "thing", "things", "stuff", "help", "helping", "trying",
    "look", "looking", "find", "getting", "got", "use", "using", "keep", "still",
    "already", "again", "first", "next", "before", "after", "because", "cause",
    "around", "between", "through", "across", "until", "without", "within",
    "12", "24", "12-24", "24/7", "247",
    # Dictation / conversational filler that must never become YouTube q tokens
    "figure", "figured", "exactly", "alone", "wanna", "gonna", "gotta", "any",
    "way", "ways", "out", "just", "then", "can", "there", "if", "we", "me",
    "them", "they", "those", "these", "twenty", "second", "seconds", "minute",
    "minutes", "paragraph", "sentence", "speak", "talk", "dictation", "mic",
    "microphone", "voice", "said", "saying", "tell", "told", "ask", "asked",
    "know", "knew", "think", "thought", "believe", "maybe", "probably",
    "something", "someone", "somewhere", "everything", "everyone", "nothing",
    "trending", "trend", "trends",  # alone without niche → generic
})

# Multi-word niches preferred over token soup (order = priority)
_KNOWN_NICHE_PHRASES = (
    "day trading",
    "swing trading",
    "stock market",
    "options trading",
    "crypto trading",
    "forex trading",
    "dark psychology",
    "relationship psychology",
    "self improvement",
    "weight loss",
    "true crime",
    "history documentary",
    "financial crime",
    "business scandal",
    "anime manhwa",
    "comic book",
    "product ads",
    "faceless youtube",
)

_NOISE_SEARCH_TOKENS = frozenset({
    "download", "fully", "watch", "attached", "uploaded", "analyze", "analyse", "pull",
    "fetch", "check", "research", "well", "also", "would", "could", "please", "thanks",
    "exactly", "figure", "alone", "wanna", "twenty", "second", "seconds", "viral",
    "short", "shorts", "paragraph", "dictation", "mic", "voice",
})


def extract_youtube_channel_urls_from_text(text: str) -> list[str]:
    """Return canonical YouTube channel URLs (@handle or /channel/ID)."""
    urls: list[str] = []
    seen: set[str] = set()
    for match in _YOUTUBE_CHANNEL_URL_RE.finditer(str(text or "")):
        handle = str(match.group(1) or "").strip()
        channel_id = str(match.group(2) or "").strip()
        if handle:
            canonical = f"https://www.youtube.com/@{handle}"
        elif channel_id:
            canonical = f"https://www.youtube.com/channel/{channel_id}"
        else:
            continue
        key = canonical.lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(canonical)
    return urls


def normalize_channel_handle_label(handle: str) -> str:
    """Turn @lume-channel into a readable label like Lume."""
    clean = str(handle or "").strip().lstrip("@")
    clean = re.sub(r"-channel$", "", clean, flags=re.I)
    clean = clean.replace("_", " ").replace("-", " ").strip()
    if not clean:
        return ""
    if " " in clean:
        return clean.title()[:60]
    return clean[:1].upper() + clean[1:]


def competitor_channel_url(user_text: str) -> str:
    """Resolve a competitor channel URL from pasted links or named channels."""
    urls = extract_youtube_channel_urls_from_text(user_text)
    if urls:
        return urls[0]
    label = extract_competitor_channel_label(user_text)
    if label:
        slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
        if slug:
            return f"https://www.youtube.com/@{slug}"
    return ""


def extract_youtube_urls_from_text(text: str) -> list[str]:
    """Return canonical YouTube watch URLs found in user text."""
    urls: list[str] = []
    seen: set[str] = set()
    channel_urls = {url.lower() for url in extract_youtube_channel_urls_from_text(text)}
    for match in _YOUTUBE_URL_RE.findall(str(text or "")):
        raw = str(match or "").strip().rstrip(".,);]")
        if not raw or raw.lower() in channel_urls:
            continue
        if _YOUTUBE_CHANNEL_URL_RE.search(raw):
            continue
        try:
            from catalyst_references import extract_video_id
        except Exception:
            extract_video_id = None  # type: ignore[assignment]
        video_id = extract_video_id(raw) if extract_video_id else ""
        canonical = f"https://www.youtube.com/watch?v={video_id}" if video_id else raw.split("?")[0]
        key = canonical.lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(canonical)
    return urls


def strip_youtube_urls_from_text(text: str) -> str:
    """Remove YouTube URLs and stray si=/v= tracking fragments from searchable text."""
    cleaned = _YOUTUBE_URL_RE.sub(" ", str(text or ""))
    cleaned = _YOUTUBE_SI_PARAM_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\bv=[\w-]{11}\b", " ", cleaned, flags=re.I)
    cleaned = re.sub(r"\bsi=[\w-]+\b", " ", cleaned, flags=re.I)
    return re.sub(r"\s+", " ", cleaned).strip()


def is_noise_search_token(token: str) -> bool:
    """True for video IDs, tracking params, and command words that pollute niche search."""
    raw = str(token or "").strip()
    if not raw:
        return True
    low = raw.lower().strip("'")
    if low in _NOISE_SEARCH_TOKENS or low in _SEARCH_STOPWORDS:
        return True
    if _VIDEO_ID_TOKEN_RE.fullmatch(low):
        return True
    if re.fullmatch(r"[\w-]{10,16}", low) and any(ch.isdigit() for ch in low):
        return True
    # Pure numbers / time windows (12-24, 24/7) are not niches
    if re.fullmatch(r"\d+(?:[-/]\d+)?", low):
        return True
    return False


def strip_youtube_shorts_suffix(query: str) -> str:
    """Remove trailing platform suffixes so we never double-append them."""
    cleaned = re.sub(r"\s+", " ", str(query or "").strip())
    if not cleaned:
        return ""
    return re.sub(
        r"(?i)(?:\s+youtube(?:\s+shorts)?|\s+shorts)+\s*$",
        "",
        cleaned,
    ).strip(" ,.-")


def with_youtube_shorts_suffix(query: str) -> str:
    """Append ``YouTube Shorts`` exactly once."""
    base = strip_youtube_shorts_suffix(query)
    if not base:
        return ""
    return f"{base} YouTube Shorts"[:220]


def refine_public_search_query(query: str) -> str:
    """Disambiguate niche queries so YouTube returns the intended vertical.

    Example: ``day trading`` must not surface fidget-toy "trading" virals.
    """
    cleaned = re.sub(r"\s+", " ", str(query or "").strip())
    if not cleaned:
        return ""
    low = cleaned.lower()
    known = extract_known_niche_phrase(cleaned)
    if known in {"day trading", "day trading course"} or re.search(r"\bday\s*trad", low):
        # Phrase + finance context + explicit exclusions (YouTube q supports -terms)
        base = "day trading"
        if "course" in low or known == "day trading course":
            base = "day trading course OR day trading psychology OR day trading mistakes"
        else:
            base = (
                '"day trading" OR "day trader" OR "day traders" '
                "(stock OR forex OR futures OR psychology OR strategy OR scalp)"
            )
        exclusions = '-fidget -popit -"fidget trading" -"trading game" -tiktok -roblox -minecraft'
        return with_youtube_shorts_suffix(f"{base} {exclusions}")
    if known:
        return with_youtube_shorts_suffix(known)
    return with_youtube_shorts_suffix(cleaned) if "youtube" not in low else cleaned[:220]


def extract_explicit_topic_phrase(text: str) -> str:
    """Prefer concrete topic phrases over generic niche buckets (e.g. dark psychology)."""
    raw = store._user_message_before_attachments(str(text or ""))
    raw = str(raw or "").strip()
    if not raw:
        return ""
    m = re.search(r"[\"“]([^\"”]{8,120})[\"”]", raw)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip(" ,.-")[:100]
    m = re.search(
        r"\b(?:about|topic(?:\s+is|\s+of)?|titled|called|focus(?:ing)?\s+on)\s+[\"']?([^.\n?]{8,100})",
        raw,
        re.I,
    )
    if m:
        phrase = re.sub(r"\s+", " ", m.group(1)).strip(" ,.-\"'")
        phrase = re.sub(r"(?i)^(of|about|on|for|the)\s+", "", phrase).strip()
        phrase = re.split(
            r"\b(?:because|directed\s+towards?|and then|targeting|for the|in the psychology|niche|"
            r"keep\s+it|do\s+deep)\b",
            phrase,
            maxsplit=1,
            flags=re.I,
        )[0].strip(" ,.-")
        phrase = re.sub(
            r"(?i)\s+(?:psychology(?:\s+niche)?|niche)\s*$",
            "",
            phrase,
        ).strip(" ,.-")
        phrase = strip_youtube_shorts_suffix(phrase)
        if len(phrase) >= 8:
            return phrase[:100]
    m = re.search(
        r"\b((?:why|how|what)\s+(?:men|women|people|guys?|girls?)\s+[^.\n?]{6,90})",
        raw,
        re.I,
    )
    if m:
        phrase = re.sub(r"\s+", " ", m.group(1)).strip(" ,.-")
        phrase = re.split(
            r"\b(?:because|directed\s+towards?|targeting|for the|in the|psychology niche|make a|"
            r"keep\s+it|do\s+deep)\b",
            phrase,
            maxsplit=1,
            flags=re.I,
        )[0].strip(" ,.-")
        phrase = re.sub(
            r"(?i)\s+(?:psychology(?:\s+niche)?|niche)\s*$",
            "",
            phrase,
        ).strip(" ,.-")
        phrase = strip_youtube_shorts_suffix(phrase)
        if len(phrase) >= 8:
            return phrase[:100]
    return ""


def extract_known_niche_phrase(text: str) -> str:
    """Return the highest-priority known niche phrase found in free text."""
    # Concrete why/how topics always beat generic psychology/fitness buckets.
    explicit = extract_explicit_topic_phrase(text)
    if explicit:
        return explicit
    low = re.sub(r"\s+", " ", str(text or "").lower())
    for phrase in _KNOWN_NICHE_PHRASES:
        if phrase in low:
            return phrase
    # Lightweight inferences when user omits the exact phrase
    if re.search(r"\b(?:day\s*)?trad(?:e|ing|er)s?\b", low) or re.search(
        r"\b(?:stock|futures|options|scalp)\b", low
    ):
        if "course" in low or "sell" in low:
            return "day trading course"
        return "day trading"
    # "sell a course" + market/24-7 context without saying "day trading"
    if re.search(r"\bcourse\b", low) and re.search(
        r"\b(?:market|markets|24/?7|trading|stocks?|ticker|chart)\b", low
    ):
        return "day trading course"
    if "crypto" in low:
        return "crypto trading"
    if "forex" in low:
        return "forex trading"
    if re.search(r"\bpsycholog", low):
        return "dark psychology"
    if re.search(r"\b(?:fitness|gym|workout)\b", low):
        return "fitness"
    return ""


def is_garbage_public_search_query(query: str) -> bool:
    """True when a compacted query is chat-filler soup, not a niche."""
    low = re.sub(r"\s+", " ", str(query or "").strip().lower())
    if not low:
        return True
    # Never treat proven Shorts discovery seeds as filler.
    if is_allowed_discovery_search_query(low):
        return False
    # Old dead-end that hydrated nothing and poisoned the agent reply.
    if is_banned_faceless_hooks_query(low):
        return True
    if extract_known_niche_phrase(low):
        return False
    tokens = [t for t in re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{2,}", low) if not is_noise_search_token(t)]
    if not tokens:
        return True
    # Majority stopwords / filler → garbage (e.g. "possible provide working mainly given hours")
    all_tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{1,}", low)
    if not all_tokens:
        return True
    noise = sum(1 for t in all_tokens if is_noise_search_token(t) or t in _SEARCH_STOPWORDS)
    if noise / max(1, len(all_tokens)) >= 0.55:
        return True
    # No content noun stronger than verbs like provide/working
    weak = {"possible", "provide", "working", "mainly", "given", "hours", "value", "course", "sell", "able"}
    if tokens and all(t in weak or is_noise_search_token(t) for t in tokens):
        # "course" alone is too weak without trading/finance context
        if "course" in tokens and not any(t in tokens for t in ("trading", "trade", "stock", "crypto", "forex")):
            return True
        if "course" not in tokens:
            return True
    return False


def _niche_query_with_youtube_context(label: str, *, shorts: bool = False) -> str:
    """Ensure compact niche labels are searchable on YouTube."""
    cleaned = re.sub(r"\s+", " ", str(label or "").strip())
    if not cleaned or is_unusable_public_search_query(cleaned):
        return ""
    low = cleaned.lower()
    if "youtube" not in low:
        suffix = "YouTube Shorts" if shorts or "short" in low else "YouTube"
        cleaned = f"{cleaned} {suffix}"
    return cleaned[:220]


def extract_competitor_channel_label(user_text: str) -> str:
    """Pull an external/reference channel name like Lume from user text."""
    raw = store._user_message_before_attachments(user_text)
    if not raw:
        return ""
    for url in extract_youtube_channel_urls_from_text(raw):
        match = re.search(r"youtube\.com/@([\w.-]+)", url, flags=re.I)
        if match:
            label = normalize_channel_handle_label(match.group(1))
            if label:
                return label
    inline = re.search(r"(?i)(?:for this|for the niche)\s*:\s*([A-Za-z][A-Za-z0-9'&.\-]{1,30})", raw)
    if inline:
        return str(inline.group(1) or "").strip()[:60]
    colon = re.search(
        r"(?i)channel\s+to\s+analy[sz]e\s+for\s+this\s*:\s*([A-Za-z][A-Za-z0-9'&.\-]{1,30})",
        raw,
    )
    if colon:
        return str(colon.group(1) or "").strip()[:60]
    named = re.search(r"(?i)\b(?:the\s+)?([A-Za-z][A-Za-z0-9'&.\-]{1,30})\s+youtube\s+channel\b", raw)
    if named and str(named.group(1) or "").strip().lower() not in {"that", "this", "same", "the", "a"}:
        return str(named.group(1) or "").strip()[:60]
    patterns = (
        r"(?i)channel\s+to\s+analy[sz]e\s+(?:for\s+(?:this|the\s+niche))?\s*:?\s*(.+?)\s+(?:that\s+)?youtube\s+channel",
        r"(?i)(?:here is|this is)\s+(?:a\s+)?channel\s+to\s+analy[sz]e\s+(?:for\s+this)?\s*:?\s*(.+?)\s+(?:that\s+)?youtube",
        r"(?i)(?:the\s+)?([A-Za-z0-9][A-Za-z0-9'&.\-\s]{0,40}?)\s+youtube\s+channel\s+is\s+in\s+this\s+niche",
        r"(?i)(?:reference|competitor)\s+channel\s*:?\s*([A-Za-z0-9][A-Za-z0-9'&.\-\s]{0,40})",
    )
    for pattern in patterns:
        match = re.search(pattern, raw)
        if not match:
            continue
        label = re.sub(r"\s+", " ", str(match.group(1) or "").strip(" ,.-:;"))
        if label and len(label.split()) <= 6:
            return label[:60]
    return ""


def competitor_channel_search_query(user_text: str) -> str:
    """Build a public YouTube search query for an external reference channel."""
    label = extract_competitor_channel_label(user_text)
    if not label:
        return ""
    query = f"{label} channel documentary YouTube"
    return sanitize_public_search_query(query) or query[:220]


def extract_explicit_niche_label_from_user_text(user_text: str) -> str:
    """Pull an explicit niche phrase like 'fugitives and government' from user commands."""
    raw = strip_youtube_urls_from_text(store._user_message_before_attachments(user_text))
    if not raw:
        return ""
    patterns = (
        r"(?i)(?:this\s+would\s+be|this\s+is|that's|that\s+is|for)\s+(?:the\s+)?(.+?)\s+niche\b",
        r"(?i)\b(.+?)\s+niche\s+on\s+youtube\b",
        r"(?i)\b(?:the\s+)?(.+?)\s+niche\b",
    )
    for pattern in patterns:
        match = re.search(pattern, raw)
        if not match:
            continue
        label = re.sub(r"\s+", " ", str(match.group(1) or "").strip(" ,.-"))
        if (
            label
            and len(label.split()) >= 2
            and not is_meta_research_query(label)
            and not is_unusable_public_search_query(label)
        ):
            return label[:160]
    return ""

_QUERY_TYPO_FIXES = {
    "hourse": "horse",
}

_RESEARCH_WRAPPER_RE = re.compile(
    r"(?i)^\s*(?:yes|ok|okay|please|also|and|then|now|sure|thanks)?\s*[,!.\-]*\s*"
    r"(?:(?:can|could|will)\s+you\s+)?"
    r"(?:(?:pull|get|fetch|check|find|show(?:\s+me)?|give\s+me|grab|run|search(?:\s+for)?|look\s+up)\s+)?"
    r"(?:the\s+)?(?:most\s+recent\s+)?(?:latest\s+)?(?:fresh\s+)?"
    r"(?:public\s+)?(?:youtube\s+)?(?:data|search|demand|trends?|performance|results?|stats?|evidence)"
    r"(?:\s+for|\s+on|\s+about|\s+around|\s+in)?\s*"
)

_META_QUERY_MARKERS = (
    "most recent public",
    "public youtube data",
    "youtube data for",
    "pull public youtube",
    "get public youtube",
    "public search",
    "niche of the video",
    "niche of this video",
    "for this niche",
    "performance data",
    "market data",
    "search trends",
    "public demand",
    "videos in this",
    "video in this",
    "in this youtube",
    "this youtube",
    "reference niche",
    "your reference niche",
)

_DEICTIC_NICHE_MARKERS = (
    "videos in this",
    "video in this",
    "this video",
    "the video",
    "that video",
    "in this youtube",
    "this youtube",
    "uploaded reference",
    "reference video",
    "the upload",
    "this upload",
    "the reference",
    "this reference",
)

_GENERIC_PUBLIC_QUERIES = frozenset(
    {
        "youtube documentary",
        "youtube shorts",
        "youtube shorts niche",
        "youtube shorts topic demand",
        "youtube viral",
        "youtube shorts viral",
        "viral youtube shorts",
    }
)

# Content-bearing discovery seeds that must survive sanitize/coerce (not chat filler).
_DISCOVERY_SEED_CORE = frozenset(
    {
        "shorts storytime",
        "storytime shorts",
        "satisfying shorts",
        "life advice shorts",
        "relationship advice shorts",
        "gym motivation shorts",
        "fitness motivation shorts",
        "viral storytime shorts",
        "asmr satisfying shorts",
    }
)

_DISCOVERY_CONTENT_MARKERS = (
    "storytime",
    "satisfying",
    "asmr",
    "life advice",
    "relationship advice",
    "gym motivation",
    "fitness motivation",
    "motivation shorts",
)


def is_allowed_discovery_search_query(query: str) -> bool:
    """True for proven no-niche Shorts discovery seeds (must not be wiped as filler)."""
    low = re.sub(r"\s+", " ", str(query or "").strip().lower())
    if not low:
        return False
    # Strip optional youtube suffix for membership check
    core = re.sub(r"\s+youtube(?:\s+shorts)?\s*$", "", low).strip()
    if core in _DISCOVERY_SEED_CORE or low in _DISCOVERY_SEED_CORE:
        return True
    try:
        from studio_agent.live_demand import discovery_search_queries

        for seed in discovery_search_queries():
            s = str(seed or "").strip().lower()
            if not s:
                continue
            if low == s or core == s or low.startswith(s + " ") or s in low:
                # Avoid matching tiny substrings of unrelated queries
                if len(s.split()) >= 2:
                    return True
    except Exception:
        pass
    if any(marker in low for marker in _DISCOVERY_CONTENT_MARKERS):
        # Require shorts/youtube context so plain "advice" never slips through
        if re.search(r"\b(?:shorts?|youtube)\b", low):
            return True
    return False


def is_banned_faceless_hooks_query(query: str) -> bool:
    """True for the old dead-end query that returned empty 'faceless hooks' research."""
    low = re.sub(r"\s+", " ", str(query or "").strip().lower())
    if not low:
        return False
    if re.search(r"\bfaceless\s+hooks?\b", low):
        return True
    if re.search(r"\bhooks?\b.+\bfaceless\b|\bfaceless\b.+\bhooks?\b", low):
        return True
    if "hooks faceless" in low or "faceless youtube shorts hooks" in low:
        return True
    return False


def default_discovery_search_query() -> str:
    """Safe no-niche YouTube Shorts discovery seed."""
    try:
        from studio_agent.live_demand import discovery_search_queries

        seeds = discovery_search_queries()
        if seeds:
            return str(seeds[0])
    except Exception:
        pass
    return "shorts storytime"


def _normalize_query_typos(text: str) -> str:
    out = str(text or "")
    for wrong, right in _QUERY_TYPO_FIXES.items():
        out = re.sub(rf"\b{re.escape(wrong)}\b", right, out, flags=re.I)
    return out


def is_meta_research_query(query: str) -> bool:
    """True when text is a research command wrapper, not a searchable niche query."""
    low = re.sub(r"\s+", " ", str(query or "").strip().lower())
    if not low:
        return True
    if any(marker in low for marker in _META_QUERY_MARKERS):
        return True
    if any(marker in low for marker in _DEICTIC_NICHE_MARKERS):
        return True
    if re.search(r"\b(?:pull|fetch|get|check|run)\b.+\b(?:youtube|public)\b", low):
        return True
    if re.search(r"\bpublic\b.+\b(?:youtube|data|search|demand)\b", low):
        return True
    return False


def is_unusable_public_search_query(query: str) -> bool:
    """True when a query would return generic/global winners instead of the user's niche."""
    low = re.sub(r"\s+", " ", str(query or "").strip().lower())
    if not low:
        return True
    # Discovery seeds are intentionally broad but still searchable (storytime, satisfying, …).
    if is_allowed_discovery_search_query(low):
        return False
    if is_banned_faceless_hooks_query(low):
        return True
    if is_meta_research_query(query):
        return True
    if is_garbage_public_search_query(query):
        return True
    if low in _GENERIC_PUBLIC_QUERIES:
        return True
    if re.fullmatch(r"(?:youtube\s+)?(?:documentary|shorts?|viral)", low):
        return True
    tokens = [
        token
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{2,}", low)
        if not is_noise_search_token(token)
    ]
    if tokens and all(token in {"youtube", "documentary", "shorts", "short", "video", "videos", "niche", "topic"} for token in tokens):
        return True
    return False


def channel_fallback_search_query(active_label: str = "", registry_key: str = "") -> str:
    """Channel-aware public-search fallback when reference/user text yields no niche.

    Each channel maps to its own niche query — never collapse every Shorts channel
    into psychology.
    """
    label = str(active_label or "").lower()
    reg_key = str(registry_key or "").strip().lower()
    combined = f"{label} {reg_key}"
    if "skele" in combined or "mrskele" in combined:
        return "dark psychology human behavior relationship facts YouTube Shorts"
    if "empire" in combined or "magnates" in combined:
        return "financial crime documentary business scandal YouTube"
    if "zerotier" in combined or "zero tier" in combined or "zero_tier" in combined:
        return "comic book mystery lore shorts YouTube Shorts"
    if "cryptic" in combined:
        return "science mystery deep science YouTube Shorts"
    if "lexi" in combined or "manhwa" in combined or "manhua" in combined:
        return "anime manhwa shorts dramatic reveal betrayal revenge YouTube Shorts"
    if "history" in combined and "rewind" in combined:
        return "history documentary ancient empire YouTube"
    if "day trad" in combined or "trading" in combined or "forex" in combined:
        return "day trading psychology stock market shorts YouTube Shorts"
    if "fitness" in combined or "gym" in combined:
        return "fitness motivation workout tips YouTube Shorts"
    reg_key_raw = str(registry_key or "").strip()
    if reg_key_raw:
        try:
            from long_form.prompts.channels import get_channel

            ch = get_channel(reg_key_raw)
            ch_label = str(ch.get("label") or reg_key_raw).replace("_", " ").strip()
            fmt = str(ch.get("format") or "long_form")
            # Use the channel's own label as the niche — do not append psychology.
            if fmt == "shorts":
                return f"{ch_label} YouTube Shorts"[:220]
            return f"{ch_label} documentary YouTube"[:220]
        except Exception:
            pass
    clean_label = str(active_label or "").replace("_", " ").strip()
    if clean_label and clean_label.lower() not in {"youtube", "your target niche", "ok"}:
        return f"{clean_label} YouTube Shorts"[:220]
    # Never return generic "YouTube Shorts niche" — that hydrates nothing useful.
    return default_discovery_search_query()


def coerce_public_search_query(
    query: str,
    *,
    user_text: str = "",
    active_label: str = "",
    registry_key: str = "",
    fallback_query: str = "",
) -> str:
    """Return a searchable niche query, replacing meta/deictic chat wrappers with channel fallbacks."""
    raw = str(query or "").strip()
    # Pass proven discovery seeds through untouched (sanitize used to wipe 2-word seeds).
    if raw and is_allowed_discovery_search_query(raw) and not is_banned_faceless_hooks_query(raw):
        return raw[:220]
    fallback_raw = str(fallback_query or "").strip()
    if fallback_raw and is_banned_faceless_hooks_query(fallback_raw):
        fallback_raw = ""
    fallback = (
        fallback_raw
        or channel_fallback_search_query(active_label, registry_key)
    )
    if is_banned_faceless_hooks_query(fallback):
        fallback = default_discovery_search_query()

    # Deictic wrappers ("videos in this niche") → prefer concrete multi-word fallback.
    low_user = str(user_text or "").lower()
    deictic = bool(
        re.search(r"\b(?:this|that|the)\s+(?:niche|space|lane|channel|topic|market)\b", low_user)
        or is_unusable_public_search_query(raw)
    )
    if deictic and fallback_raw and len(fallback_raw.split()) >= 3:
        if not is_meta_research_query(fallback_raw) and not is_garbage_public_search_query(fallback_raw):
            return fallback_raw[:220]

    for candidate in (raw, fallback):
        if not candidate:
            continue
        if is_banned_faceless_hooks_query(candidate):
            continue
        if is_allowed_discovery_search_query(candidate):
            return candidate[:220]
        if is_unusable_public_search_query(candidate):
            continue
        sanitized = sanitize_public_search_query(candidate, active_label=active_label)
        if sanitized and is_banned_faceless_hooks_query(sanitized):
            continue
        if sanitized and not is_unusable_public_search_query(sanitized):
            return sanitized
    derived = derive_niche_search_query(
        None,
        user_text=user_text,
        active_label=active_label,
        fallback_query=fallback,
    )
    if derived and not is_unusable_public_search_query(derived) and not is_banned_faceless_hooks_query(derived):
        return derived
    coerced_fallback = sanitize_public_search_query(fallback, active_label=active_label)
    if (
        coerced_fallback
        and not is_unusable_public_search_query(coerced_fallback)
        and not is_banned_faceless_hooks_query(coerced_fallback)
    ):
        return coerced_fallback
    ch = channel_fallback_search_query(active_label, registry_key)
    if ch and not is_banned_faceless_hooks_query(ch) and not is_unusable_public_search_query(ch):
        return ch
    return default_discovery_search_query()


def extract_niche_terms_from_user_text(user_text: str) -> list[str]:
    """Pull searchable niche phrases out of conversational research commands."""
    raw = _normalize_query_typos(
        strip_youtube_urls_from_text(store._user_message_before_attachments(user_text))
    )
    known = extract_known_niche_phrase(raw)
    if known:
        return [known]
    cleaned = _RESEARCH_WRAPPER_RE.sub("", raw).strip()
    cleaned = re.sub(
        r"(?i)\s+(?:niche|topic|category|space|angle)"
        r"(?:\s+of\s+(?:the\s+)?(?:video|content|upload|reference))?\s*$",
        "",
        cleaned,
    ).strip()
    cleaned = _CHAT_QUERY_NOISE_RE.sub(" ", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
    if not cleaned:
        return []
    # Never return full multi-clause chat sentences as "niche terms"
    if len(cleaned.split()) > 10 or is_garbage_public_search_query(cleaned) or is_meta_research_query(cleaned):
        return []
    phrases: list[str] = []
    for segment in re.split(r"[/|,]+", cleaned):
        segment = str(segment or "").strip()
        if (
            segment
            and not is_meta_research_query(segment)
            and not is_garbage_public_search_query(segment)
            and len(segment.split()) <= 8
        ):
            phrases.append(segment)
    if not phrases and cleaned and not is_meta_research_query(cleaned) and not is_garbage_public_search_query(cleaned):
        phrases.append(cleaned)
    return phrases


def _reference_topic_phrases(reference_payload: dict[str, Any]) -> list[str]:
    """Collect short topic-bearing phrases — never full vision paragraphs."""
    phrases: list[str] = []
    storytelling = reference_payload.get("storytelling")
    if isinstance(storytelling, dict):
        hook = str(storytelling.get("hook") or "").strip()
        if hook:
            phrases.append(hook[:180])
        summary = str(storytelling.get("summary") or "").strip()
        if summary:
            phrases.append(summary[:180])
        packaging = storytelling.get("packaging")
        if isinstance(packaging, dict):
            for key in ("title_angle", "title", "appeal"):
                val = str(packaging.get(key) or "").strip()
                if val:
                    phrases.append(val[:120])
        elif isinstance(packaging, str) and packaging.strip():
            phrases.append(packaging.strip()[:120])
        beats = storytelling.get("story_beats")
        if isinstance(beats, list):
            for beat in beats[:3]:
                val = str(beat or "").strip()
                if val:
                    phrases.append(val[:120])
    transcript = reference_payload.get("transcript")
    if isinstance(transcript, dict):
        text = str(transcript.get("text") or "").strip()
        if text:
            first = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0].strip()
            phrases.append((first or text)[:180])
    meta = reference_payload.get("metadata")
    if isinstance(meta, dict):
        title = str(meta.get("title") or "").strip()
        if title and not re.search(r"\bagent_video_\d+\.mp4\b", title, re.I):
            phrases.append(title[:120])
    return [phrase for phrase in phrases if phrase]


def _compact_search_keywords(phrases: list[str], *, max_terms: int = 8) -> list[str]:
    """Extract deduped search terms from reference phrases.

    Prefer known niche multi-word phrases. Never promote chat filler into query tokens.
    """
    joined = " ".join(str(p or "") for p in phrases)
    known = extract_known_niche_phrase(joined)
    if known:
        return known.split()[:max_terms]

    scores: dict[str, float] = {}
    for phrase in phrases:
        for raw in re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{2,}", str(phrase or "")):
            token = raw.strip("'").lower()
            if len(token) < 3 or is_noise_search_token(token):
                continue
            weight = 1.0
            if raw[:1].isupper():
                weight += 0.35
            # Prefer content nouns over long chat verbs (provide/working/possible)
            if token in {"trading", "trader", "traders", "psychology", "crypto", "forex", "fitness", "manhwa", "anime"}:
                weight += 1.5
            if len(token) >= 7:
                weight += 0.15
            scores[token] = max(scores.get(token, 0.0), weight)
    ranked = sorted(scores.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    out = [term for term, _score in ranked[:max(4, min(max_terms, 10))]]
    # Drop if result is still garbage filler
    if out and is_garbage_public_search_query(" ".join(out)):
        return []
    return out


def simplify_public_search_query(query: str) -> str:
    """Collapse an over-long query into a short keyword search."""
    cleaned = re.sub(r"\s+", " ", str(query or "").strip())
    if not cleaned:
        return ""
    known = extract_known_niche_phrase(cleaned)
    if known:
        return with_youtube_shorts_suffix(known)
    if len(cleaned.split()) <= 8 and not is_garbage_public_search_query(cleaned):
        return with_youtube_shorts_suffix(cleaned) if "youtube" not in cleaned.lower() else cleaned[:220]
    terms = _compact_search_keywords([cleaned], max_terms=7)
    if not terms:
        return ""
    joined = " ".join(terms)
    if is_garbage_public_search_query(joined):
        return ""
    return joined[:220]


def sanitize_public_search_query(query: str, *, active_label: str = "") -> str:
    """Strip chat boilerplate so public search never runs on raw command text."""
    cleaned = _normalize_query_typos(re.sub(r"\s+", " ", str(query or "").strip()))
    if not cleaned:
        return ""
    # Never ship the old faceless-hooks dead-end.
    if is_banned_faceless_hooks_query(cleaned):
        return ""
    # Proven discovery seeds must survive (2-word seeds used to be wiped below).
    if is_allowed_discovery_search_query(cleaned):
        return cleaned[:220]
    # Prefer known niches before any token soup
    known = extract_known_niche_phrase(cleaned)
    if known and (
        is_meta_research_query(cleaned)
        or len(cleaned.split()) > 6
        or is_garbage_public_search_query(cleaned)
        or "research" in cleaned.lower()
        or "whats working" in cleaned.lower()
        or "what's working" in cleaned.lower()
    ):
        return with_youtube_shorts_suffix(known)
    if re.search(r"\bagent_video_\d+\.mp4\b", cleaned, re.I):
        cleaned = _CHAT_QUERY_NOISE_RE.sub(" ", cleaned).strip()
    lowered = cleaned.lower()
    if any(
        phrase in lowered
        for phrase in (
            "watch this video",
            "watch the video",
            "try again",
            "go ahead",
            "check all recent",
            "getting data on",
            "video topic we are",
            "local_path",
            "uploaded reference",
        )
    ):
        return ""
    cleaned = _RESEARCH_WRAPPER_RE.sub("", cleaned).strip()
    cleaned = _CHAT_QUERY_NOISE_RE.sub(" ", cleaned).strip()
    # Strip long research/command wrappers that produce filler tokens
    cleaned = re.sub(
        r"(?i)\b(?:research|whats?|what's|working|mainly|since|we want|be able to|go viral|"
        r"sell a course|given we|need data|last \d+[-–/]?\d*\s*hours?|market moves|24/?7|"
        r"provide the most value|possible|as updated|most value)\b",
        " ",
        cleaned,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
    lowered = cleaned.lower()
    if any(token in lowered for token in ("watch", "try again", "go ahead", "check all")):
        return ""
    known2 = extract_known_niche_phrase(cleaned) or known
    if is_meta_research_query(cleaned) or len(cleaned.split()) > 8 or is_garbage_public_search_query(cleaned):
        if known2:
            return with_youtube_shorts_suffix(known2)
        terms = _compact_search_keywords([cleaned], max_terms=8)
        if terms and not is_garbage_public_search_query(" ".join(terms)):
            cleaned = " ".join(terms)
        else:
            return ""
    if is_meta_research_query(cleaned) or is_garbage_public_search_query(cleaned):
        if known2:
            return with_youtube_shorts_suffix(known2)
        return ""
    if len(cleaned) < 4:
        return ""
    if is_banned_faceless_hooks_query(cleaned):
        return ""
    if is_allowed_discovery_search_query(cleaned):
        return cleaned[:220]
    if len(cleaned.split()) <= 2 and "youtube" not in lowered and not known2:
        # Two content words (e.g. "dark psychology") are fine if known niche; else drop.
        if known2:
            return with_youtube_shorts_suffix(known2)
        # Keep short niche-like phrases with a real content noun (not pure filler).
        content_tokens = [
            t for t in re.findall(r"[A-Za-z][A-Za-z'\-]{2,}", cleaned.lower())
            if t not in _SEARCH_STOPWORDS and not is_noise_search_token(t)
        ]
        if len(content_tokens) >= 1 and not is_garbage_public_search_query(cleaned):
            return with_youtube_shorts_suffix(cleaned)
        return ""
    if known2 and known2 not in cleaned.lower():
        return with_youtube_shorts_suffix(known2)
    if "youtube" not in cleaned.lower() and len(cleaned.split()) <= 5:
        return with_youtube_shorts_suffix(cleaned)
    # Already has a platform suffix — collapse any accidental doubles.
    return with_youtube_shorts_suffix(cleaned) if re.search(r"(?i)youtube\s+shorts\s+youtube", cleaned) else cleaned[:220]


def reference_ready_for_public_search(
    payload: dict[str, Any] | None,
    *,
    niche_from_reference: bool,
) -> bool:
    """Gate public search until reference analysis yields a real topic signal."""
    if not niche_from_reference:
        return True
    return reference_has_topic_signal(payload)


def derive_niche_search_query(
    reference_payload: dict[str, Any] | None,
    *,
    user_text: str = "",
    active_label: str = "",
    fallback_query: str = "",
) -> str:
    """Build a compact public-search query from reference topic signals."""
    low = strip_youtube_urls_from_text(store._user_message_before_attachments(user_text)).lower()
    shorts_context = "short" in low or "shorts" in low or True  # demand research defaults to Shorts
    known = extract_known_niche_phrase(user_text)
    if known:
        return f"{known} YouTube Shorts"[:220]
    explicit_niche = extract_explicit_niche_label_from_user_text(user_text)
    if explicit_niche and not is_garbage_public_search_query(explicit_niche):
        sanitized = sanitize_public_search_query(
            _niche_query_with_youtube_context(explicit_niche, shorts=True),
            active_label=active_label,
        )
        if sanitized and not is_garbage_public_search_query(sanitized):
            return sanitized

    # Pure deictic chat ("videos in this niche") must not invent a weaker
    # active_label soup when a concrete fallback already carries the niche.
    deictic_only = bool(
        re.search(r"\b(?:this|that|the)\s+(?:niche|space|lane|channel|topic|market)\b", low)
    ) and not known and not explicit_niche
    if deictic_only and fallback_query:
        raw_fb = re.sub(r"\s+", " ", str(fallback_query or "").strip())
        # Keep multi-signal fallbacks intact — sanitize can collapse
        # "psychology ... self improvement" down to a single known niche.
        if (
            raw_fb
            and len(raw_fb.split()) >= 3
            and not is_meta_research_query(raw_fb)
            and not is_garbage_public_search_query(raw_fb)
            and not is_banned_faceless_hooks_query(raw_fb)
        ):
            return raw_fb[:220]
        fb = sanitize_public_search_query(raw_fb, active_label=active_label)
        if fb and not is_meta_research_query(fb) and not is_garbage_public_search_query(fb):
            return fb
        fb_known = extract_known_niche_phrase(fallback_query or "")
        if fb_known:
            return f"{fb_known} YouTube Shorts"[:220]

    phrases: list[str] = []
    ref_has_signal = reference_has_topic_signal(reference_payload)
    if isinstance(reference_payload, dict) and ref_has_signal:
        phrases.extend(_reference_topic_phrases(reference_payload))
    if not ref_has_signal:
        phrases.extend(extract_niche_terms_from_user_text(user_text))

    if active_label and active_label not in {"YouTube", "your target niche", "ok"}:
        phrases.append(active_label.replace("_", " "))

    # Prefer known niches inside any phrase list before keyword soup
    for phrase in phrases:
        k = extract_known_niche_phrase(phrase)
        if k:
            return f"{k} YouTube Shorts"[:220]

    terms = _compact_search_keywords(phrases, max_terms=8)
    if terms:
        query = " ".join(terms)
        if shorts_context or (
            isinstance(reference_payload, dict)
            and str(
                ((reference_payload.get("analysis_profile") or {}).get("content_format") or "")
            ).lower().startswith("short")
        ):
            query = f"{query} YouTube Shorts"
        sanitized = sanitize_public_search_query(query, active_label=active_label)
        if sanitized and not is_garbage_public_search_query(sanitized):
            return sanitized

    fallback = sanitize_public_search_query(
        str(fallback_query or "").strip(),
        active_label=active_label,
    )
    if fallback and not is_meta_research_query(fallback) and not is_garbage_public_search_query(fallback):
        return fallback
    fb_known = extract_known_niche_phrase(fallback_query or "")
    if fb_known:
        return f"{fb_known} YouTube Shorts"[:220]
    label = str(active_label or "").replace("_", " ").strip()
    if label and label.lower() not in {"youtube", "your target niche", "ok"}:
        return f"{label} YouTube Shorts"[:220]
    return default_discovery_search_query()
