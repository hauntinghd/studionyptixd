"""Rank next video candidates from reference + public + channel signals."""
from __future__ import annotations

import re
from typing import Any

from studio_agent.turn_plan import _compact_search_keywords, is_meta_research_query, is_noise_search_token


def _topic_text(row: dict[str, Any]) -> str:
    return str(row.get("topic") or row.get("title") or row.get("angle") or "").strip()


def _reference_keywords(reference_payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(reference_payload, dict):
        return []
    phrases: list[str] = []
    storytelling = reference_payload.get("storytelling")
    if isinstance(storytelling, dict):
        for key in ("hook", "summary"):
            val = str(storytelling.get(key) or "").strip()
            if val:
                phrases.append(val[:180])
    transcript = reference_payload.get("transcript")
    if isinstance(transcript, dict):
        text = str(transcript.get("text") or "").strip()
        if text:
            phrases.append(text[:180])
    return _compact_search_keywords(phrases, max_terms=10)


def _reference_topic_candidates(reference_payload: dict[str, Any] | None) -> list[tuple[str, str]]:
    """Derive actionable next-video angles from reference storytelling, not search spam."""
    if not isinstance(reference_payload, dict):
        return []
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    storytelling = reference_payload.get("storytelling")
    if not isinstance(storytelling, dict):
        return out

    def _push(label: str, reason: str) -> None:
        clean = re.sub(r"\s+", " ", str(label or "").strip())
        if not clean or is_meta_research_query(clean):
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        out.append((clean[:140], reason[:220]))

    packaging = storytelling.get("packaging")
    if isinstance(packaging, dict):
        title_angle = str(packaging.get("title_angle") or packaging.get("title") or "").strip()
        if title_angle:
            _push(title_angle, "Packaging angle from reference analysis")
    elif isinstance(packaging, str) and packaging.strip():
        _push(packaging.strip(), "Packaging angle from reference analysis")

    hook = str(storytelling.get("hook") or "").strip()
    if hook:
        _push(hook, "Hook angle from reference analysis")

    beats = storytelling.get("story_beats")
    if isinstance(beats, list):
        for beat in beats[:4]:
            val = str(beat or "").strip()
            if val:
                _push(val, "Story beat from reference analysis")

    return out


def _overlap_score(text: str, keywords: list[str]) -> float:
    low = str(text or "").lower()
    if not low or not keywords:
        return 0.0
    hits = sum(1 for kw in keywords if kw.lower() in low)
    return min(1.0, hits / max(1, len(keywords)))


def _channel_niche_keywords(
    channel_insights: dict[str, Any] | None,
    search_query: str,
) -> list[str]:
    """Keywords for ranking/display — prefer channel title evidence over the search query.

    The search query alone must NOT become the only niche filter (see runner).
    """
    phrases: list[str] = []
    for row in list((channel_insights or {}).get("top_titles") or []):
        if isinstance(row, dict):
            title = str(row.get("title") or "").strip()
            if title:
                phrases.append(title[:160])
    # Only fold search query in after channel titles so it cannot wipe domain niches.
    if search_query:
        phrases.append(str(search_query).strip()[:160])
    return _compact_search_keywords(phrases, max_terms=12)


# Format / packaging noise — NOT domain niches. Psychology, trading, manhwa etc. must match.
_FORMAT_NOISE_TOKENS = frozenset(
    {
        "shorts",
        "short",
        "youtube",
        "video",
        "videos",
        "documentary",
        "channel",
        "facts",  # packaging suffix, not a niche
        "tricks",
        "powerful",
        "smart",
        "life",
        "viral",
        "fyp",
        "feed",
        "shortfeed",
        "trending",
        "best",
        "top",
        "new",
        "watch",
        "must",
    }
)

# Back-compat alias used by older call sites / tests
_GENERIC_NICHE_TOKENS = _FORMAT_NOISE_TOKENS


def _token_is_format_noise(token: str) -> bool:
    low = str(token or "").strip().lower()
    if not low or is_noise_search_token(low):
        return True
    return low in _FORMAT_NOISE_TOKENS


def _extract_match_tokens(phrases: list[str], *, allow_short: bool = False) -> set[str]:
    """Extract niche-bearing tokens; keep domain words (psychology, trading, manhwa…)."""
    tokens: set[str] = set()
    min_len = 3 if allow_short else 4
    for phrase in phrases:
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{2,}", str(phrase or "").lower()):
            if len(token) < min_len:
                continue
            if _token_is_format_noise(token):
                continue
            tokens.add(token)
    return tokens


def _channel_signature_tokens(channel_titles: list[str]) -> set[str]:
    return _extract_match_tokens(list(channel_titles or []), allow_short=False)


# Homonym / toy / game false positives for finance "trading" niches
_DAY_TRADING_FALSE_POSITIVE_RE = re.compile(
    r"\b(?:"
    r"fidget|popit|pop\s*its?|pop\s*it|tiktok|toy|toys|gameplay|roblox|minecraft|"
    r"pokemon|nba\s*2k|fortnite|asmr|slime|squishy|blind\s*bag|"
    r"trading\s*card|card\s*trading|football\s*cards?|soccer\s*cards?|"
    r"steam\s*trading|skin\s*trading|csgo|cs2\s*skin|opensea\s*nft\s*flip"
    r")\b",
    re.I,
)
_DAY_TRADING_POSITIVE_RE = re.compile(
    r"\b(?:"
    r"day\s*trad(?:e|ing|er|ers)?|scalp(?:ing)?|forex|futures|options|nasdaq|nifty|"
    r"banknifty|candlestick|risk\s*management|broker|stock\s*market|price\s*action|"
    r"support\s*and\s*resistance|chart\s*pattern|retail\s*trad|"
    r"trading\s*psychology|trading\s*strategy|prop\s*firm"
    r")\b",
    re.I,
)
_FINANCE_CONTEXT_RE = re.compile(
    r"\b(?:stock|stocks|crypto|bitcoin|forex|futures|options|chart|trader|traders|"
    r"profit|loss|psychology|strategy|market|invest|scalp|broker|portfolio)\b",
    re.I,
)


def _query_is_day_trading_niche(phrases: list[str]) -> bool:
    blob = " ".join(str(p or "").lower() for p in phrases)
    return bool(
        re.search(r"\bday\s*trad", blob)
        or ("trading" in blob and re.search(r"\b(?:stock|forex|futures|market|course)\b", blob))
    )


def _is_day_trading_false_positive(title: str) -> bool:
    """Reject fidget/toy 'trading' and other homonyms of day trading."""
    low = str(title or "").lower()
    if not low:
        return True
    if _DAY_TRADING_FALSE_POSITIVE_RE.search(low):
        return True
    # "trading" alone with game/meme packaging and no finance context
    if "trading" in low and not _DAY_TRADING_POSITIVE_RE.search(low):
        if re.search(r"\b(?:game|hack|pro in no time|funny|viral tiktok)\b", low):
            return True
        if not _FINANCE_CONTEXT_RE.search(low):
            return True
    return False


def title_matches_day_trading_intent(title: str) -> bool:
    """True when a title is actually about financial day trading, not fidget toys."""
    low = str(title or "").lower()
    if not low or _is_day_trading_false_positive(low):
        return False
    if _DAY_TRADING_POSITIVE_RE.search(low):
        return True
    if "trading" in low and _FINANCE_CONTEXT_RE.search(low):
        return True
    return False


# Discovery seed → title must match at least one of these (else off-niche viral noise).
_DISCOVERY_TITLE_MARKERS: dict[str, tuple[str, ...]] = {
    "storytime": (
        "storytime", "story time", "story", "stories", "confession", "reddit", "aita",
        "pov", "true story", "told me", "happened", "when i", "i was",
    ),
    "satisfying": ("satisfying", "asmr", "oddly", "oddly satisfying", "relaxing"),
    "advice": ("advice", "lesson", "wisdom", "life tip", "relationship", "dating", "trust"),
    "gym": ("gym", "workout", "fitness", "lift", "gains", "motivation", "training"),
    "motivation": ("motivation", "mindset", "discipline", "hustle", "success", "grind"),
}


def _discovery_seed_key(query: str) -> str:
    low = re.sub(r"\s+", " ", str(query or "").strip().lower())
    if "storytime" in low or re.search(r"\bstory\b", low):
        return "storytime"
    if "satisfying" in low or "asmr" in low:
        return "satisfying"
    if "advice" in low or "relationship" in low:
        return "advice"
    if "gym" in low or "fitness" in low or "workout" in low:
        return "gym"
    if "motivation" in low:
        return "motivation"
    return ""


def title_matches_discovery_seed(title: str, search_query: str) -> bool:
    """True when a discovery-seed search hit is actually on-theme (not random #shorts virals)."""
    key = _discovery_seed_key(search_query)
    if not key:
        return True  # not a discovery seed — leave to other filters
    markers = _DISCOVERY_TITLE_MARKERS.get(key) or ()
    low = str(title or "").lower()
    if not low:
        return False
    return any(m in low for m in markers)


def filter_public_rows_for_query(
    rows: list[dict[str, Any]] | None,
    *,
    search_query: str = "",
    user_text: str = "",
) -> list[dict[str, Any]]:
    """Root-level niche filter applied at every boundary (tools, warm, grounded summary).

    Day-trading queries hard-drop fidget/toy/game "trading" homonyms and require a
    real finance signal. Discovery seeds require title markers so fruit-comedy
    megavirals cannot masquerade as "storytime demand".
    """
    out: list[dict[str, Any]] = []
    intent_phrases = [str(search_query or "").strip(), str(user_text or "").strip()]
    enforce_day_trading = _query_is_day_trading_niche(intent_phrases)
    joined_intent = " ".join(intent_phrases)
    enforce_discovery = bool(_discovery_seed_key(joined_intent))
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or row.get("topic") or "").strip()
        if not title:
            continue
        if enforce_day_trading:
            if _is_day_trading_false_positive(title) or not title_matches_day_trading_intent(title):
                continue
        if enforce_discovery and not title_matches_discovery_seed(title, joined_intent):
            continue
        out.append(row)
    return out


def _niche_relevance_score(
    text: str,
    *,
    niche_keywords: list[str],
    channel_titles: list[str],
    search_query: str = "",
) -> float:
    """Score how well a title fits the niche.

    Domain terms like psychology/trading are valid match tokens. Only format noise
    is stripped. Multi-word phrases (e.g. "dark psychology") also score as a whole.
    Day-trading niches hard-reject fidget/toy "trading" false positives.

    ``search_query`` is always consulted for day-trading intent so channel-title
    keyword compaction cannot drop the finance niche and re-admit fidget virals.
    """
    low = str(text or "").lower()
    if not low:
        return 0.0
    phrases = [str(p).strip() for p in list(niche_keywords) + list(channel_titles) if str(p).strip()]
    if search_query and str(search_query).strip():
        phrases = [str(search_query).strip(), *phrases]
    if not phrases:
        # No niche signal to enforce — caller should trust the search results as-is.
        return 1.0

    if _query_is_day_trading_niche(phrases):
        if _is_day_trading_false_positive(low):
            return 0.0
        if not title_matches_day_trading_intent(low):
            # Soft reject: "trading" without day-trade / finance signal
            if "trading" in low or "trader" in low:
                return 0.0
            return 0.05

    # Phrase hits (stronger signal)
    phrase_hits = 0
    phrase_total = 0
    for phrase in phrases[:12]:
        cleaned = re.sub(r"\s+", " ", phrase.lower()).strip()
        # Drop pure format wrappers + YouTube boolean operators so scoring stays on niche words
        cleaned = re.sub(r"\b(?:youtube|shorts?|video|videos|documentary)\b", " ", cleaned)
        cleaned = re.sub(r"""["()]""", " ", cleaned)
        cleaned = re.sub(r"\b(?:or|and)\b", " ", cleaned)
        cleaned = re.sub(r"(?:^|\s)-[a-z0-9\"'][\w\"'-]*", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if len(cleaned) < 4:
            continue
        phrase_total += 1
        # Multi-word niche: require full phrase or all content words (day+trading)
        parts = [t for t in cleaned.split() if not _token_is_format_noise(t) and len(t) >= 3]
        if cleaned in low:
            phrase_hits += 2
        elif len(parts) >= 2 and all(p in low for p in parts):
            phrase_hits += 2
        elif parts and sum(1 for t in parts if t in low) >= max(1, (len(parts) + 1) // 2):
            # Partial match is weaker for multi-word niches
            phrase_hits += 0.5 if len(parts) >= 2 else 1

    tokens = _extract_match_tokens(phrases, allow_short=True)
    if not tokens and phrase_total == 0:
        # No usable niche signal → do not pretend off-niche (caller should trust search)
        return 1.0
    token_hits = sum(1 for token in tokens if token in low) if tokens else 0
    token_score = token_hits / max(1, min(len(tokens), 8)) if tokens else 0.0
    phrase_score = phrase_hits / max(1, phrase_total * 2) if phrase_total else 0.0
    return min(1.0, max(token_score, phrase_score))


def _channel_title_overlap_score(text: str, channel_titles: list[str]) -> float:
    """Overlap against distinctive tokens from the creator's actual winning titles."""
    low = str(text or "").lower()
    if not low or not channel_titles:
        return 0.0
    signature = _channel_signature_tokens(channel_titles)
    if not signature:
        # Fall back to domain-aware niche score on channel titles
        return _niche_relevance_score(text, niche_keywords=[], channel_titles=channel_titles)
    hits = sum(1 for token in signature if token in low)
    return min(1.0, hits / max(1, min(len(signature), 6)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _channel_retention_rows(channel_video_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in list(channel_video_rows or [])
        if isinstance(row, dict) and str(row.get("title") or "").strip()
    ]
    if not rows:
        return []
    return sorted(
        rows,
        key=lambda row: (
            _safe_float(row.get("average_view_percentage")),
            _safe_int(row.get("views", row.get("view_count", 0))),
            _safe_int(row.get("average_view_duration_sec")),
        ),
        reverse=True,
    )


def _channel_winner_candidates(
    channel_video_rows: list[dict[str, Any]] | None,
    channel_insights: dict[str, Any] | None,
) -> list[tuple[str, str, float, str]]:
    """Build next-video moves from connected-channel retention winners."""
    out: list[tuple[str, str, float, str]] = []
    seen: set[str] = set()
    retention_rows = _channel_retention_rows(channel_video_rows)
    for index, row in enumerate(retention_rows[:4]):
        title = str(row.get("title") or "").strip()
        if not title:
            continue
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        avp = _safe_float(row.get("average_view_percentage"))
        avd = _safe_int(row.get("average_view_duration_sec"))
        views = _safe_int(row.get("views", row.get("view_count", 0)))
        stats: list[str] = []
        if avp:
            stats.append(f"{avp:.2f}% APV")
        if avd:
            stats.append(f"{avd}s AVD")
        if views:
            stats.append(f"{views:,} views")
        reason = "Selected-channel retention winner"
        if stats:
            reason += f" ({', '.join(stats)})"
        out.append((title[:140], reason[:220], max(0.82, 0.97 - index * 0.04), "channel_winner"))

    insights = channel_insights if isinstance(channel_insights, dict) else {}
    for hint in list(insights.get("hook_patterns") or [])[:3]:
        clean = re.sub(r"\s+", " ", str(hint or "").strip())
        if not clean or clean.lower() in seen or is_meta_research_query(clean):
            continue
        seen.add(clean.lower())
        out.append(
            (
                clean[:140],
                "Packaging pattern from your connected channel analytics",
                0.86,
                "channel_pattern",
            )
        )
    return out


def _public_row_base_score(row: dict[str, Any]) -> float | None:
    """Return None when a public row is too weak to cite as a next-video move.

    Short-window Live Demand rows may be ``exploratory_public_signal`` — still
    usable as ranked test moves when labeled honestly (not as 100k precedents).
    """
    support = str(row.get("support_label") or "").strip()
    views = int(row.get("views") or 0)
    views_per_day = float(row.get("views_per_day") or 0)
    window = int(row.get("query_window_days") or 0)
    if support in {"unsupported_or_low_signal", "unsupported_no_hydrated_stats"}:
        return None
    if views < 1 and views_per_day <= 0:
        return None
    if support.startswith("strong"):
        base = 0.92
    elif support.startswith("supported"):
        base = 0.78
    elif support.startswith("weak"):
        base = 0.58
    elif support.startswith("exploratory"):
        # Only promote exploratory rows inside short Live Demand windows.
        if window and window <= 7 and (views >= 200 or views_per_day >= 100):
            base = 0.42
        else:
            return None
    elif views >= 100_000:
        base = 0.65
    elif window and window <= 7 and views >= 500:
        base = 0.40
    else:
        return None

    profile = str(row.get("search_profile") or "").strip().lower()
    if profile == "top_performers":
        base = min(1.0, base + 0.06)
    elif profile in {"recent_momentum", "recent_momentum_derived"}:
        base = min(1.0, base + 0.02)
    return base


def rank_next_video_candidates(
    *,
    reference_payload: dict[str, Any] | None = None,
    public_rows: list[dict[str, Any]] | None = None,
    predicted_topics: list[dict[str, Any]] | None = None,
    channel_insights: dict[str, Any] | None = None,
    channel_video_rows: list[dict[str, Any]] | None = None,
    search_query: str = "",
    limit: int = 6,
) -> list[dict[str, Any]]:
    """Merge public demand, reference overlap, and channel gap into ranked moves."""
    from youtube import score_topic_opportunity

    keywords = _reference_keywords(reference_payload)
    channel_titles = [
        str(row.get("title") or "")
        for row in list((channel_insights or {}).get("top_titles") or [])
        if isinstance(row, dict) and str(row.get("title") or "").strip()
    ]
    for row in _channel_retention_rows(channel_video_rows):
        title = str(row.get("title") or "").strip()
        if title and title not in channel_titles:
            channel_titles.append(title)
    channel_keywords = _channel_niche_keywords(channel_insights, search_query)
    if channel_titles:
        channel_keywords = _compact_search_keywords(
            channel_keywords + channel_titles,
            max_terms=12,
        )
    niche_keywords = keywords or channel_keywords or _compact_search_keywords([search_query], max_terms=6)
    # Always keep the raw search query as a niche phrase so day-trading intent cannot
    # be lost when channel titles dominate compact keyword extraction.
    if search_query and str(search_query).strip():
        niche_keywords = [str(search_query).strip(), *list(niche_keywords or [])]
    public_rows = filter_public_rows_for_query(
        list(public_rows or []),
        search_query=search_query,
        user_text=search_query,
    )
    has_channel_retention = bool(_channel_retention_rows(channel_video_rows))
    require_public_niche_fit = bool(channel_titles or channel_keywords or keywords or search_query)
    min_public_niche_score = 0.2 if channel_titles else 0.12
    min_channel_overlap = 0.17 if channel_titles else 0.0
    trending_titles = [
        str(row.get("title") or "")
        for row in list(public_rows or [])
        if isinstance(row, dict)
        and str(row.get("title") or "").strip()
        and _public_row_base_score(row) is not None
    ]

    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add_candidate(
        label: str,
        *,
        reason: str,
        base_score: float,
        source: str,
    ) -> None:
        clean = re.sub(r"\s+", " ", str(label or "").strip())
        if not clean or is_meta_research_query(clean):
            return
        tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{2,}", clean)
        if tokens and all(is_noise_search_token(token) for token in tokens):
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        overlap = _overlap_score(clean, keywords or channel_keywords)
        channel_overlap = _channel_title_overlap_score(clean, channel_titles)
        if source in {"channel_winner", "channel_pattern"}:
            composite = round(min(1.0, base_score * 0.55 + channel_overlap * 0.45), 3)
        else:
            composite = round(min(1.0, base_score * 0.7 + overlap * 0.3), 3)
        candidates.append(
            {
                "topic": clean[:140],
                "composite_score": composite,
                "reason": reason[:220],
                "source": source,
                "reference_overlap": round(overlap, 3),
                "channel_overlap": round(channel_overlap, 3),
            }
        )

    for title, reason, base_score, source in _channel_winner_candidates(channel_video_rows, channel_insights):
        _add_candidate(title, reason=reason, base_score=base_score, source=source)

    for row in list(public_rows or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("evidence_level") or "") != "hydrated_video_stats":
            continue
        title = str(row.get("title") or "").strip()
        if not title:
            continue
        base = _public_row_base_score(row)
        if base is None:
            continue
        if has_channel_retention:
            channel_fit = _channel_title_overlap_score(title, channel_titles)
            if channel_fit < min_channel_overlap:
                continue
        if require_public_niche_fit:
            niche_fit = _niche_relevance_score(
                title,
                niche_keywords=niche_keywords,
                channel_titles=channel_titles,
                search_query=search_query,
            )
            if niche_fit < min_public_niche_score:
                continue
        # Absolute guard: never promote day-trading false positives even if score leaked.
        if _query_is_day_trading_niche([search_query, *list(niche_keywords or [])]):
            if _is_day_trading_false_positive(title) or not title_matches_day_trading_intent(title):
                continue
        views = int(row.get("views") or 0)
        support = str(row.get("support_label") or "").strip()
        profile = str(row.get("search_profile") or "").strip().replace("_", " ")
        public_base = base * (0.72 if has_channel_retention else 1.0)
        _add_candidate(
            title,
            reason=(
                f"Hydrated public precedent ({views:,} views, {support or 'public signal'}"
                + (f", {profile})" if profile else ")")
            ),
            base_score=public_base,
            source="public_demand",
        )

    for topic, reason in _reference_topic_candidates(reference_payload):
        overlap = _overlap_score(topic, keywords)
        base = 0.62 + overlap * 0.18
        _add_candidate(
            topic,
            reason=reason,
            base_score=base,
            source="reference_story",
        )

    weak_public_titles = {
        str(row.get("title") or "").strip().lower()
        for row in list(public_rows or [])
        if isinstance(row, dict)
        and str(row.get("evidence_level") or "") == "hydrated_video_stats"
        and str(row.get("support_label") or "") == "unsupported_or_low_signal"
        and str(row.get("title") or "").strip()
    }
    has_public_demand = any(str(row.get("source") or "") == "public_demand" for row in candidates)

    for row in list(predicted_topics or []):
        if not isinstance(row, dict):
            continue
        topic = _topic_text(row)
        if not topic:
            continue
        if topic.lower() in weak_public_titles:
            continue
        if _query_is_day_trading_niche([search_query, *list(niche_keywords or [])]):
            if _is_day_trading_false_positive(topic) or not title_matches_day_trading_intent(topic):
                continue
        scored = score_topic_opportunity(topic, channel_titles, trending_titles, niche_keywords)
        base_score = float(scored.get("composite_score") or 0.4)
        if has_public_demand and base_score < 0.55:
            continue
        if not trending_titles and base_score < 0.5:
            continue
        _add_candidate(
            topic,
            reason=str(row.get("reason") or "Public niche signal"),
            base_score=base_score,
            source="predicted_topic",
        )

    # Intentionally no "niche_query" fallback candidate — raw search queries
    # must never surface as ranked moves (leaked as "query — score 0.79").

    def _source_rank(source: str) -> int:
        if has_channel_retention:
            return {
                "channel_winner": 5,
                "channel_pattern": 4,
                "reference_story": 3,
                "predicted_topic": 2,
                "niche_query": 1,
                "public_demand": 0,
            }.get(source, 0)
        return {
            "channel_winner": 4,
            "channel_pattern": 3,
            "reference_story": 3,
            "predicted_topic": 2,
            "public_demand": 1,
            "niche_query": 1,
        }.get(source, 0)

    candidates.sort(
        key=lambda row: (
            _source_rank(str(row.get("source") or "")),
            float(row.get("composite_score") or 0),
            float(row.get("channel_overlap") or 0),
            float(row.get("reference_overlap") or 0),
        ),
        reverse=True,
    )
    ranked = candidates[: max(1, min(int(limit or 6), 10))]
    if has_channel_retention:
        channel_first = [
            row for row in ranked if str(row.get("source") or "") in {"channel_winner", "channel_pattern"}
        ]
        if channel_first:
            others = [
                row
                for row in ranked
                if str(row.get("source") or "") not in {"channel_winner", "channel_pattern", "public_demand"}
            ][: max(0, int(limit or 6) - len(channel_first))]
            ranked = (channel_first + others)[: max(1, min(int(limit or 6), 10))]
    return ranked


def format_prediction_lines(candidates: list[dict[str, Any]]) -> list[str]:
    """Internal ranking lines for grounding only — never user-facing score dumps.

    User-facing replies must use conversation.py (natural prose). These lines
    intentionally omit composite scores so they cannot leak as
    ``title — score 0.79`` into chat.
    """
    lines: list[str] = []
    for row in list(candidates or []):
        topic = str(row.get("topic") or "").strip()
        if not topic:
            continue
        # Never promote the raw search query as a "move"
        if str(row.get("source") or "") == "niche_query":
            continue
        reason = str(row.get("reason") or "").strip()
        # Strip machine score language from reasons
        reason = re.sub(r"(?i)\b(?:score|composite)\s+\d+(?:\.\d+)?\b", "", reason).strip(" ;,.—–-")
        reason = re.sub(r"(?i)predicted niche fit.*$", "", reason).strip(" ;,.—–-")
        if reason and "hydrated public" in reason.lower():
            # Short human reason
            m = re.search(r"([\d,]+)\s*views", reason, re.I)
            if m:
                lines.append(f"- {topic} ({m.group(1)} views on public YouTube)")
            else:
                lines.append(f"- {topic}")
        elif reason and len(reason) < 120 and "score" not in reason.lower():
            lines.append(f"- {topic}: {reason}")
        else:
            lines.append(f"- {topic}")
    return lines