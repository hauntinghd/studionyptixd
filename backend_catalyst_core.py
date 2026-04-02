import re


def _clip_text(value: str, max_chars: int = 320) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def _dedupe_preserve_order(values: list[str], max_items: int = 8, max_chars: int = 200) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in list(values or []):
        value = _clip_text(str(raw or "").strip(), max_chars)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= max_items:
            break
    return out


def _simple_title_signature_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9']+", str(text or "").strip().lower())


def _title_opening_signature(text: str, max_tokens: int = 3) -> str:
    cleaned = str(text or "").strip().lower()
    if not cleaned:
        return ""
    cleaned = re.sub(r"^\s*top\s+\d+\b", "toplist", cleaned)
    cleaned = re.sub(r"^\s*what\s+(?:really\s+)?happened(?:\s+to)?\b", "whathappened", cleaned)
    tokens = _simple_title_signature_tokens(cleaned)
    if not tokens:
        return ""
    return " ".join(tokens[: max(1, int(max_tokens or 3))]).strip()


def _clean_same_arena_phrase(text: str, max_words: int = 8) -> str:
    phrase = str(text or "").strip()
    if not phrase:
        return ""
    phrase = re.sub(r"\s*\|\s*.*$", "", phrase)
    phrase = re.sub(r"^\s*top\s+\d+\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"^\s*(how|why)\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"^\s*what\s+(?:really\s+)?happened\s+to\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"^\s*(the\s+truth\s+about|inside|the\s+story\s+of)\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(
        r"\b(disturbing|shocking|hidden|dangerous|crazy|insane|darkest|biggest|worst|ultimate|complete|full)\b",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"\b(secrets?|facts?|truths?|lessons?|reasons?|ways?|mistakes?|patterns?|blind\s+spots?)\b",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"\b(hides?\s+from\s+you|hide\s+from\s+you|keeps?\s+from\s+you|keep\s+from\s+you|you\s+never\s+notice|most\s+people\s+never\s+notice)\b",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(r"\s+", " ", phrase).strip(" -,:")
    words = phrase.split()
    if len(words) > max_words:
        phrase = " ".join(words[:max_words]).strip()
    return phrase


def _title_is_too_close_to_source(candidate: str, source_title: str) -> bool:
    cand = str(candidate or "").strip()
    source = str(source_title or "").strip()
    if not cand or not source:
        return False
    cand_lower = re.sub(r"\s+", " ", cand.lower())
    source_lower = re.sub(r"\s+", " ", source.lower())
    if cand_lower == source_lower:
        return True
    cand_tokens = _simple_title_signature_tokens(cand)
    source_tokens = _simple_title_signature_tokens(source)
    if not cand_tokens or not source_tokens:
        return False
    if cand_tokens == source_tokens:
        return True
    shared = len(set(cand_tokens) & set(source_tokens))
    overlap = shared / max(len(set(source_tokens)), 1)
    if cand_lower.startswith("top ") and source_lower.startswith("top ") and overlap >= 0.50:
        return True
    if overlap >= 0.78:
        return True
    if shared >= 4 and len(source_tokens) <= 5:
        return True
    return False


def _title_is_too_close_to_any(candidate: str, existing_titles: list[str]) -> bool:
    cand = str(candidate or "").strip()
    if not cand:
        return False
    for existing in list(existing_titles or []):
        current = str(existing or "").strip()
        if current and _title_is_too_close_to_source(cand, current):
            return True
    return False


def _title_reuses_opening_pattern(candidate: str, source_title: str = "", recent_titles: list[str] | None = None) -> bool:
    cand = str(candidate or "").strip()
    if not cand:
        return False
    cand_sig = _title_opening_signature(cand)
    compare_titles = [str(source_title or "").strip(), *[str(v).strip() for v in list(recent_titles or []) if str(v).strip()]]
    for existing in compare_titles:
        if not existing:
            continue
        if cand_sig and cand_sig == _title_opening_signature(existing):
            return True
    return False


def _catalyst_text_overlap_score(primary: str, secondary: str) -> float:
    left = str(primary or "").strip().lower()
    right = str(secondary or "").strip().lower()
    if not left or not right:
        return 0.0
    left_tokens = set(re.findall(r"[a-z0-9']+", left))
    right_tokens = set(re.findall(r"[a-z0-9']+", right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(len(left_tokens), 1)


def _catalyst_channel_memory_key(user_id: str, channel_id: str, format_preset: str) -> str:
    user_key = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(user_id or "").strip()) or "anon"
    channel_key = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(channel_id or "").strip()) or "unbound"
    format_key = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(format_preset or "").strip().lower()) or "explainer"
    return f"{user_key}:{channel_key}:{format_key}"


def _catalyst_series_memory_key(series_anchor: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(series_anchor or "").strip().lower()).strip("_")
    return normalized or "general"


_CATALYST_KEYWORD_STOPWORDS = {
    "the", "and", "with", "that", "this", "from", "your", "into", "what", "when", "will", "have",
    "about", "their", "them", "they", "over", "more", "than", "just", "make", "made", "make", "video",
    "videos", "how", "why", "top", "best", "worst", "full", "complete", "guide", "explained", "breakdown",
    "documentary", "channel", "studio", "nyptid",
}


_CATALYST_NICHE_RULES = {
    "manga_recap": {
        "label": "Manga / Manhua Recap",
        "keywords": [
            "manga", "manhwa", "manhua", "webtoon", "recap", "chapter", "murim", "isekai",
            "cultivation", "regressor", "reincarnated", "hunter", "necromancer", "dungeon",
            "martial", "ranker", "overpowered", "wrecker", "driver", "wrecker driver",
        ],
        "follow_up_rule": "Stay in recap mode. Build the next video around a stronger arc turn, power jump, betrayal, reveal, or chapter escalation instead of generic documentary framing.",
    },
    "day_trading": {
        "label": "Day Trading / Investing",
        "keywords": [
            "day trading", "daytrading", "trading", "trader", "market", "stock", "stocks", "options",
            "forex", "crypto", "futures", "spy", "nasdaq", "setup", "chart", "liquidity", "risk", "scalp",
        ],
        "follow_up_rule": "Keep the video anchored to real setups, risk, trader psychology, and chart consequences. Avoid generic wealth-posturing or broad business fluff.",
    },
    "dark_psychology": {
        "label": "Psychology / Hidden Behavior",
        "keywords": [
            "brain", "mind", "memory", "attention", "psychology", "manipulation", "disturbing",
            "subconscious", "secret", "lies", "blind spot", "decision", "habits", "behavior",
        ],
        "follow_up_rule": "Lean into hidden mechanisms, mental blind spots, and consequence-first reveals. Keep the emotional charge high without drifting into textbook visuals.",
    },
    "business_documentary": {
        "label": "Business Documentary",
        "keywords": [
            "business", "company", "startup", "industry", "money", "market", "billion", "brand",
            "economy", "economics", "finance", "capital", "investor", "wealth",
        ],
        "follow_up_rule": "Frame the next video around systems, incentives, leverage, money flow, or power structures. Keep it engineered and documentary-driven.",
    },
    "geopolitics_history": {
        "label": "History / Geopolitics",
        "keywords": [
            "war", "empire", "leader", "iran", "battle", "killed", "assassin", "history",
            "nation", "military", "power", "border", "regime", "operation", "strategy",
        ],
        "follow_up_rule": "Stay on power shifts, hidden chains of events, and consequence-led storytelling. Avoid generic business framing.",
    },
}


def _extract_catalyst_keywords(*texts: str, max_items: int = 12) -> list[str]:
    scores: dict[str, int] = {}
    for text in texts:
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9'-]{2,}", str(text or "").lower()):
            if token in _CATALYST_KEYWORD_STOPWORDS:
                continue
            scores[token] = scores.get(token, 0) + 1
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [token for token, _ in ranked[:max_items]]


def _catalyst_infer_niche(*texts: str, format_preset: str = "") -> dict:
    normalized_texts = [str(text or "").strip() for text in texts if str(text or "").strip()]
    combined = " \n ".join(normalized_texts).lower()
    scored: list[tuple[str, int, list[str]]] = []
    for niche_key, spec in _CATALYST_NICHE_RULES.items():
        hits: list[str] = []
        score = 0
        for raw_keyword in list(spec.get("keywords") or []):
            keyword = str(raw_keyword or "").strip().lower()
            if not keyword:
                continue
            if keyword in combined:
                hits.append(keyword)
                score += 3 if " " in keyword else 2
        if score > 0:
            scored.append((niche_key, score, hits))
    preset = str(format_preset or "").strip().lower()
    if preset == "recap":
        scored.append(("manga_recap", 6, ["recap"]))
    elif preset == "documentary":
        scored.append(("business_documentary", 1, ["documentary"]))
    if not scored:
        fallback_key = "business_documentary" if preset == "documentary" else ("manga_recap" if preset == "recap" else "")
        if not fallback_key:
            return {"key": "", "label": "", "confidence": 0.0, "keywords": [], "follow_up_rule": ""}
        fallback = dict(_CATALYST_NICHE_RULES.get(fallback_key) or {})
        return {
            "key": fallback_key,
            "label": str(fallback.get("label", "") or ""),
            "confidence": 0.35,
            "keywords": [],
            "follow_up_rule": str(fallback.get("follow_up_rule", "") or ""),
        }
    scored.sort(key=lambda item: (-item[1], item[0]))
    chosen_key, chosen_score, chosen_hits = scored[0]
    chosen = dict(_CATALYST_NICHE_RULES.get(chosen_key) or {})
    confidence = 0.45 if chosen_score <= 3 else (0.72 if chosen_score <= 7 else 0.9)
    return {
        "key": chosen_key,
        "label": str(chosen.get("label", "") or ""),
        "confidence": round(confidence, 2),
        "keywords": _dedupe_preserve_order(chosen_hits, max_items=8, max_chars=40),
        "follow_up_rule": str(chosen.get("follow_up_rule", "") or ""),
    }


def _catalyst_titlecase_phrase(text: str) -> str:
    words = [str(word or "").strip() for word in str(text or "").split() if str(word or "").strip()]
    out: list[str] = []
    for word in words:
        if word.isupper() and len(word) <= 5:
            out.append(word)
        else:
            out.append(word[:1].upper() + word[1:])
    return " ".join(out).strip()


def _catalyst_extract_series_anchor(*texts: str, niche_key: str = "") -> str:
    raw_texts = [str(text or "").strip() for text in texts if str(text or "").strip()]
    if not raw_texts:
        return ""
    combined = " \n ".join(raw_texts)
    lower_combined = combined.lower()
    if str(niche_key or "").strip().lower() == "manga_recap" or "wrecker driver" in lower_combined:
        if "wrecker driver" in lower_combined:
            return "Wrecker Driver"
        phrase_match = re.search(r"\b([A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+){1,3})\b", combined)
        if phrase_match:
            candidate = _clean_same_arena_phrase(phrase_match.group(1), max_words=4)
            if candidate and len(candidate.split()) >= 2:
                return _catalyst_titlecase_phrase(candidate)
    return ""


def _catalyst_metric_average(total: float, count: int, digits: int = 2) -> float:
    safe_count = max(0, int(count or 0))
    if safe_count <= 0:
        return 0.0
    try:
        return round(float(total or 0.0) / safe_count, digits)
    except Exception:
        return 0.0


def _catalyst_build_channel_series_clusters(videos: list[dict] | None, *, top_videos: list[dict] | None = None) -> list[dict]:
    merged_by_id: dict[str, dict] = {}
    for raw in list(videos or []):
        if not isinstance(raw, dict):
            continue
        video_id = str(raw.get("video_id", "") or "").strip()
        key = video_id or f"title:{str(raw.get('title', '') or '').strip().lower()}"
        if not key:
            continue
        merged_by_id[key] = dict(raw or {})
    for raw in list(top_videos or []):
        if not isinstance(raw, dict):
            continue
        video_id = str(raw.get("video_id", "") or "").strip()
        if not video_id:
            continue
        base = dict(merged_by_id.get(video_id) or {})
        merged = dict(base)
        merged.update({k: v for k, v in dict(raw or {}).items() if v not in (None, "", [], {})})
        merged_by_id[video_id] = merged
    cluster_map: dict[str, dict] = {}
    for video in list(merged_by_id.values()):
        title = str(video.get("title", "") or "").strip()
        description = str(video.get("description", "") or "").strip()
        tags = [str(tag).strip() for tag in list(video.get("tags") or []) if str(tag).strip()]
        if not title:
            continue
        niche = _catalyst_infer_niche(title, description, " ".join(tags), format_preset="")
        niche_key = str(niche.get("key", "") or "").strip().lower()
        series_anchor = _catalyst_extract_series_anchor(title, description, " ".join(tags), niche_key=niche_key)
        cluster_key = f"series:{_catalyst_series_memory_key(series_anchor)}" if series_anchor else f"niche:{niche_key or 'general'}"
        bucket = dict(cluster_map.get(cluster_key) or {})
        bucket.setdefault("series_anchor", series_anchor)
        bucket.setdefault("niche_key", niche_key)
        bucket.setdefault("niche_label", str(niche.get("label", "") or ""))
        bucket.setdefault("niche_follow_up_rule", str(niche.get("follow_up_rule", "") or ""))
        bucket.setdefault("titles", [])
        bucket.setdefault("keywords", [])
        bucket.setdefault("video_count", 0)
        bucket.setdefault("views_sum", 0.0)
        bucket.setdefault("ctr_sum", 0.0)
        bucket.setdefault("ctr_count", 0)
        bucket.setdefault("avp_sum", 0.0)
        bucket.setdefault("avp_count", 0)
        bucket.setdefault("top_title", "")
        bucket.setdefault("top_views", 0.0)
        bucket["video_count"] = int(bucket.get("video_count", 0) or 0) + 1
        bucket["titles"] = _dedupe_preserve_order([title, *list(bucket.get("titles") or [])], max_items=10, max_chars=160)
        bucket["keywords"] = _dedupe_preserve_order([*_extract_catalyst_keywords(title, description, *tags, max_items=10), *list(bucket.get("keywords") or [])], max_items=10, max_chars=60)
        views = float(video.get("views", 0.0) or 0.0)
        bucket["views_sum"] = float(bucket.get("views_sum", 0.0) or 0.0) + views
        ctr = float(video.get("impression_click_through_rate", 0.0) or 0.0)
        if ctr > 0:
            bucket["ctr_sum"] = float(bucket.get("ctr_sum", 0.0) or 0.0) + ctr
            bucket["ctr_count"] = int(bucket.get("ctr_count", 0) or 0) + 1
        avp = float(video.get("average_view_percentage", video.get("average_percentage_viewed", 0.0)) or 0.0)
        if avp > 0:
            bucket["avp_sum"] = float(bucket.get("avp_sum", 0.0) or 0.0) + avp
            bucket["avp_count"] = int(bucket.get("avp_count", 0) or 0) + 1
        if views > float(bucket.get("top_views", 0.0) or 0.0):
            bucket["top_views"] = views
            bucket["top_title"] = title
        cluster_map[cluster_key] = bucket
    clusters: list[dict] = []
    for key, bucket in list(cluster_map.items()):
        avg_views = _catalyst_metric_average(float(bucket.get("views_sum", 0.0) or 0.0), int(bucket.get("video_count", 0) or 0), 0)
        avg_ctr = _catalyst_metric_average(float(bucket.get("ctr_sum", 0.0) or 0.0), int(bucket.get("ctr_count", 0) or 0), 2)
        avg_avp = _catalyst_metric_average(float(bucket.get("avp_sum", 0.0) or 0.0), int(bucket.get("avp_count", 0) or 0), 2)
        cluster_label = str(bucket.get("series_anchor", "") or bucket.get("niche_label", "") or "Channel lane").strip()
        clusters.append({
            "key": key,
            "kind": "series" if str(bucket.get("series_anchor", "") or "").strip() else "niche",
            "series_anchor": str(bucket.get("series_anchor", "") or "").strip(),
            "label": cluster_label,
            "niche_key": str(bucket.get("niche_key", "") or "").strip(),
            "niche_label": str(bucket.get("niche_label", "") or "").strip(),
            "follow_up_rule": str(bucket.get("niche_follow_up_rule", "") or "").strip(),
            "video_count": int(bucket.get("video_count", 0) or 0),
            "average_views": avg_views,
            "average_ctr": avg_ctr,
            "average_avp": avg_avp,
            "top_title": str(bucket.get("top_title", "") or "").strip(),
            "sample_titles": _dedupe_preserve_order(list(bucket.get("titles") or []), max_items=4, max_chars=160),
            "keywords": _dedupe_preserve_order(list(bucket.get("keywords") or []), max_items=8, max_chars=60),
            "score": round((float(bucket.get("top_views", 0.0) or 0.0) * 0.02) + (avg_views * 0.01) + (avg_ctr * 6.0) + (avg_avp * 2.2) + (int(bucket.get("video_count", 0) or 0) * 5.0), 2),
        })
    clusters.sort(key=lambda row: (-float(row.get("score", 0.0) or 0.0), -int(row.get("video_count", 0) or 0), str(row.get("label", "") or "").lower()))
    return clusters[:12]


def _catalyst_weighted_signal_items(signal_map: dict | None, *, max_items: int = 8, max_chars: int = 180) -> list[str]:
    rows: list[tuple[str, float]] = []
    for raw_text, raw_weight in dict(signal_map or {}).items():
        text = _clip_text(str(raw_text or "").strip(), max_chars)
        if not text:
            continue
        try:
            weight = float(raw_weight or 0.0)
        except Exception:
            continue
        if weight <= 0:
            continue
        rows.append((text, weight))
    rows.sort(key=lambda item: (-item[1], item[0].lower()))
    return [text for text, _ in rows[:max_items]]


def _catalyst_merge_signal_lists(*groups: list[str], max_items: int = 10, max_chars: int = 180) -> list[str]:
    merged: list[str] = []
    for group in groups:
        merged.extend(str(v or "").strip() for v in list(group or []) if str(v or "").strip())
    return _dedupe_preserve_order(merged, max_items=max_items, max_chars=max_chars)


def _catalyst_merge_weighted_signals(existing_map: dict | None, signals: list[str] | None, weight: float, *, max_items: int = 10, max_chars: int = 180) -> dict:
    merged = dict(existing_map or {})
    numeric_weight = float(weight or 0.0)
    if numeric_weight <= 0:
        return merged
    for raw in list(signals or []):
        value = _clip_text(str(raw or "").strip(), max_chars)
        if not value:
            continue
        merged[value] = float(merged.get(value, 0.0) or 0.0) + numeric_weight
    ranked = sorted(merged.items(), key=lambda item: (-float(item[1] or 0.0), str(item[0]).lower()))
    return {str(text): float(score or 0.0) for text, score in ranked[:max_items]}


def _catalyst_update_weighted_signals(target: dict, field_name: str, signals: list[str] | None, weight: float, *, max_items: int = 10, max_chars: int = 180) -> None:
    if not isinstance(target, dict):
        return
    target[field_name] = _catalyst_merge_weighted_signals(
        target.get(field_name) or {},
        signals,
        weight,
        max_items=max_items,
        max_chars=max_chars,
    )


def _catalyst_outcome_weight(metrics: dict | None) -> float:
    payload = dict(metrics or {})
    views = float(payload.get("views", 0.0) or 0.0)
    impressions = float(payload.get("impressions", 0.0) or 0.0)
    ctr = float(payload.get("impression_click_through_rate", 0.0) or 0.0)
    avp = float(payload.get("average_percentage_viewed", 0.0) or 0.0)
    first30 = float(payload.get("first_30_sec_retention_pct", 0.0) or 0.0)
    weight = 0.35
    weight += min(1.5, views / 25000.0)
    weight += min(0.75, impressions / 100000.0)
    if ctr > 0:
        weight += min(0.9, ctr / 8.0)
    if avp > 0:
        weight += min(0.8, avp / 80.0)
    if first30 > 0:
        weight += min(0.7, first30 / 75.0)
    return round(max(0.25, min(weight, 3.5)), 3)


def _render_catalyst_series_cluster_context(cluster: dict | None) -> str:
    cluster = dict(cluster or {})
    if not cluster:
        return ""
    parts = [
        f"Matched channel series cluster: {str(cluster.get('label', '') or '').strip()}." if str(cluster.get("label", "") or "").strip() else "",
        f"Series anchor: {str(cluster.get('series_anchor', '') or '').strip()}." if str(cluster.get("series_anchor", "") or "").strip() else "",
        f"Cluster niche: {str(cluster.get('niche_label', '') or '').strip()}." if str(cluster.get("niche_label", "") or "").strip() else "",
        f"Cluster performance snapshot: {int(cluster.get('video_count', 0) or 0)} videos, avg views {int(float(cluster.get('average_views', 0.0) or 0.0)):,}, avg CTR {float(cluster.get('average_ctr', 0.0) or 0.0):.2f}%, avg viewed {float(cluster.get('average_avp', 0.0) or 0.0):.2f}%." if int(cluster.get("video_count", 0) or 0) > 0 else "",
        ("Cluster sample titles: " + ", ".join(list(cluster.get("sample_titles") or [])[:3])) if list(cluster.get("sample_titles") or []) else "",
        ("Cluster keywords: " + ", ".join(list(cluster.get("keywords") or [])[:6])) if list(cluster.get("keywords") or []) else "",
        ("Cluster follow-up rule: " + _clip_text(str(cluster.get("follow_up_rule", "") or ""), 220)) if str(cluster.get("follow_up_rule", "") or "").strip() else "",
    ]
    return " ".join(part for part in parts if part).strip()


def _catalyst_cluster_memory_adjustment(channel_memory: dict | None, cluster: dict | None) -> float:
    raw_memory = dict(channel_memory or {})
    cluster = dict(cluster or {})
    if not raw_memory or not cluster:
        return 0.0
    series_anchor = str(cluster.get("series_anchor", "") or "").strip()
    public = _catalyst_channel_memory_public_view(raw_memory, series_anchor_override=series_anchor)
    if not any(public.values()):
        return 0.0
    outcome_count = int(public.get("outcome_count", 0) or 0)
    ctr = float(public.get("average_ctr", 0.0) or 0.0)
    avp = float(public.get("average_average_percentage_viewed", 0.0) or 0.0)
    first30 = float(public.get("average_first_30_sec_retention_pct", 0.0) or 0.0)
    ref_overall = float(public.get("average_reference_overall_score", 0.0) or 0.0)
    retention_watchouts = len([str(v).strip() for v in list(public.get("retention_watchouts") or []) if str(v).strip()])
    packaging_watchouts = len([str(v).strip() for v in list(public.get("packaging_watchouts") or []) if str(v).strip()])

    adjustment = 0.0
    adjustment += min(18.0, outcome_count * 3.0)
    adjustment += max(0.0, ctr - 4.5) * 5.0
    adjustment += max(0.0, avp - 40.0) * 0.5
    adjustment += max(0.0, first30 - 55.0) * 0.25
    adjustment += max(0.0, ref_overall - 70.0) * 0.35
    adjustment -= max(0.0, 3.0 - ctr) * 10.0
    adjustment -= max(0.0, 32.0 - avp) * 0.75
    adjustment -= min(12.0, retention_watchouts * 2.5)
    adjustment -= min(10.0, packaging_watchouts * 2.0)
    return round(adjustment, 2)


def _select_catalyst_channel_series_cluster(channel_context: dict | None, *, topic: str = "", source_title: str = "", channel_memory: dict | None = None, format_preset: str = "") -> dict:
    channel_context = dict(channel_context or {})
    clusters = [dict(row or {}) for row in list(channel_context.get("series_clusters") or []) if isinstance(row, dict)]
    if not clusters:
        return {}
    memory_public = _catalyst_channel_memory_public_view(channel_memory)
    memory_anchor = str(memory_public.get("series_anchor", "") or "").strip().lower()
    memory_niche = str(memory_public.get("niche_key", "") or "").strip().lower()
    inferred = _catalyst_infer_niche(topic, source_title, format_preset=format_preset)
    inferred_niche = str(inferred.get("key", "") or "").strip().lower()
    ref_text = f"{topic} {source_title}".strip()
    best_score = -1.0
    best_cluster: dict = {}
    for cluster in clusters:
        score = float(cluster.get("score", 0.0) or 0.0)
        anchor = str(cluster.get("series_anchor", "") or "").strip()
        niche_key = str(cluster.get("niche_key", "") or "").strip().lower()
        label = str(cluster.get("label", "") or "").strip()
        score += _catalyst_cluster_memory_adjustment(channel_memory, cluster)
        if memory_anchor and anchor and anchor.lower() == memory_anchor:
            score += 80.0
        if memory_niche and niche_key and niche_key == memory_niche:
            score += 22.0
        if inferred_niche and niche_key and niche_key == inferred_niche:
            score += 18.0
        if ref_text:
            score += _catalyst_text_overlap_score(ref_text, " ".join([anchor, label, *list(cluster.get("sample_titles") or []), *list(cluster.get("keywords") or [])])) * 100.0
        if score > best_score:
            best_score = score
            best_cluster = cluster
    return best_cluster


def _catalyst_metric_score(value: float, low: float, good: float, elite: float, *, neutral: int = 55) -> int:
    try:
        parsed = float(value or 0.0)
    except Exception:
        parsed = 0.0
    if parsed <= 0:
        return int(neutral)
    if parsed <= low:
        return int(max(20, round((parsed / max(low, 0.001)) * 45.0)))
    if parsed <= good:
        return int(round(45.0 + ((parsed - low) / max(good - low, 0.001)) * 30.0))
    if parsed <= elite:
        return int(round(75.0 + ((parsed - good) / max(elite - good, 0.001)) * 20.0))
    return 95


def _catalyst_signal_balance_score(wins: list[str] | None, watchouts: list[str] | None, *, neutral: int = 60) -> int:
    score = int(neutral)
    score += min(3, len([str(v).strip() for v in list(wins or []) if str(v).strip()])) * 8
    score -= min(3, len([str(v).strip() for v in list(watchouts or []) if str(v).strip()])) * 10
    return max(20, min(95, int(score)))


def _catalyst_title_novelty_score(title: str, source_title: str = "", recent_titles: list[str] | None = None) -> int:
    value = str(title or "").strip()
    if not value:
        return 30
    score = 88
    source_value = str(source_title or "").strip()
    recent = [str(v).strip() for v in list(recent_titles or []) if str(v).strip()]
    if source_value and _title_is_too_close_to_source(value, source_value):
        score -= 48
    elif recent and _title_is_too_close_to_any(value, recent):
        score -= 34
    if _title_reuses_opening_pattern(value, source_value, recent):
        score -= 22
    words = re.findall(r"[A-Za-z0-9']+", value)
    if len(words) < 4:
        score -= 10
    elif len(words) > 12:
        score -= 12
    if len(value) > 72:
        score -= 15
    if re.match(r"^\s*\d+", value) and re.match(r"^\s*\d+", source_value):
        score -= 8
    return max(20, min(95, int(score)))


def _catalyst_pressure_label(score: float) -> str:
    numeric = float(score or 0.0)
    if numeric >= 78:
        return "critical"
    if numeric >= 60:
        return "high"
    if numeric >= 42:
        return "medium"
    if numeric >= 24:
        return "low"
    return "stable"


def _catalyst_channel_memory_public_view(memory: dict | None, series_anchor_override: str = "") -> dict:
    data = dict(memory or {})
    series_map = dict(data.get("series_memory_map") or {})
    series_catalog = _dedupe_preserve_order(
        [
            _clip_text(str((row or {}).get("series_anchor", "") or ""), 120)
            for row in list(series_map.values())
            if isinstance(row, dict) and str((row or {}).get("series_anchor", "") or "").strip()
        ],
        max_items=8,
        max_chars=120,
    )
    active_series_anchor = _clip_text(str(series_anchor_override or data.get("series_anchor", "") or ""), 120)
    active_series_key = ""
    active_series_bucket: dict = {}
    if active_series_anchor:
        active_series_key = _catalyst_series_memory_key(active_series_anchor)
        active_series_bucket = dict(series_map.get(active_series_key) or {})
        if not active_series_bucket:
            active_series_bucket = next(
                (
                    dict(row)
                    for row in list(series_map.values())
                    if isinstance(row, dict)
                    and str(row.get("series_anchor", "") or "").strip().lower() == active_series_anchor.lower()
                ),
                {},
            )
    if active_series_bucket:
        data = {**data, **active_series_bucket}
        data["series_anchor"] = active_series_anchor or str(active_series_bucket.get("series_anchor", "") or "")
    for field in (
        "hook_wins_map", "hook_watchouts_map", "pacing_wins_map", "pacing_watchouts_map",
        "visual_wins_map", "visual_watchouts_map", "sound_wins_map", "sound_watchouts_map",
        "packaging_wins_map", "packaging_watchouts_map", "retention_wins_map", "retention_watchouts_map",
    ):
        if field not in data or not isinstance(data.get(field), dict):
            data[field] = {}
    outcome_count = int(data.get("outcome_count", 0) or 0)
    hook_wins = _catalyst_merge_signal_lists(_catalyst_weighted_signal_items(data.get("hook_wins_map") or {}, max_items=6), list(data.get("hook_learnings") or [])[:4], max_items=10, max_chars=180)
    pacing_wins = _catalyst_merge_signal_lists(_catalyst_weighted_signal_items(data.get("pacing_wins_map") or {}, max_items=6), list(data.get("pacing_learnings") or [])[:4], max_items=10, max_chars=180)
    visual_wins = _catalyst_merge_signal_lists(_catalyst_weighted_signal_items(data.get("visual_wins_map") or {}, max_items=6), list(data.get("visual_learnings") or [])[:4], max_items=10, max_chars=180)
    sound_wins = _catalyst_merge_signal_lists(_catalyst_weighted_signal_items(data.get("sound_wins_map") or {}, max_items=6), list(data.get("sound_learnings") or [])[:4], max_items=10, max_chars=180)
    packaging_wins = _catalyst_merge_signal_lists(_catalyst_weighted_signal_items(data.get("packaging_wins_map") or {}, max_items=6), list(data.get("packaging_learnings") or [])[:4], max_items=10, max_chars=180)
    retention_wins = _catalyst_merge_signal_lists(_catalyst_weighted_signal_items(data.get("retention_wins_map") or {}, max_items=6), list(data.get("retention_wins") or [])[:4], max_items=10, max_chars=180)
    hook_watchouts = _catalyst_weighted_signal_items(data.get("hook_watchouts_map") or {}, max_items=6)
    pacing_watchouts = _catalyst_weighted_signal_items(data.get("pacing_watchouts_map") or {}, max_items=6)
    visual_watchouts = _catalyst_weighted_signal_items(data.get("visual_watchouts_map") or {}, max_items=6)
    sound_watchouts = _catalyst_weighted_signal_items(data.get("sound_watchouts_map") or {}, max_items=6)
    packaging_watchouts = _catalyst_weighted_signal_items(data.get("packaging_watchouts_map") or {}, max_items=6)
    retention_watchouts = _catalyst_weighted_signal_items(data.get("retention_watchouts_map") or {}, max_items=6)
    public = {
        "channel_id": str(data.get("channel_id", "") or ""),
        "series_anchor": str(data.get("series_anchor", "") or ""),
        "niche_key": str(data.get("niche_key", "") or ""),
        "niche_follow_up_rule": str(data.get("niche_follow_up_rule", "") or ""),
        "summary": _clip_text(str(data.get("summary", "") or ""), 320),
        "outcome_count": outcome_count,
        "run_count": int(data.get("run_count", 0) or 0),
        "series_run_count": int(data.get("run_count", 0) or 0),
        "average_views": float(data.get("average_views", 0.0) or 0.0),
        "average_impressions": float(data.get("average_impressions", 0.0) or 0.0),
        "average_ctr": float(data.get("average_ctr", 0.0) or 0.0),
        "average_average_percentage_viewed": float(data.get("average_average_percentage_viewed", 0.0) or 0.0),
        "average_view_duration_sec": float(data.get("average_view_duration_sec", 0.0) or 0.0),
        "average_first_30_sec_retention_pct": float(data.get("average_first_30_sec_retention_pct", 0.0) or 0.0),
        "average_first_60_sec_retention_pct": float(data.get("average_first_60_sec_retention_pct", 0.0) or 0.0),
        "average_reference_overall_score": float(data.get("average_reference_overall_score", 0.0) or 0.0),
        "average_reference_hook_score": float(data.get("average_reference_hook_score", 0.0) or 0.0),
        "average_reference_pacing_score": float(data.get("average_reference_pacing_score", 0.0) or 0.0),
        "average_reference_visual_score": float(data.get("average_reference_visual_score", 0.0) or 0.0),
        "average_reference_sound_score": float(data.get("average_reference_sound_score", 0.0) or 0.0),
        "average_reference_packaging_score": float(data.get("average_reference_packaging_score", 0.0) or 0.0),
        "average_reference_title_novelty_score": float(data.get("average_reference_title_novelty_score", 0.0) or 0.0),
        "proven_keywords": list(data.get("proven_keywords") or []),
        "hook_learnings": hook_wins,
        "pacing_learnings": pacing_wins,
        "visual_learnings": visual_wins,
        "sound_learnings": sound_wins,
        "packaging_learnings": packaging_wins,
        "retention_learnings": retention_wins,
        "next_video_moves": list(data.get("next_video_moves") or []),
        "hook_wins": hook_wins,
        "hook_watchouts": hook_watchouts,
        "pacing_wins": pacing_wins,
        "pacing_watchouts": pacing_watchouts,
        "visual_wins": visual_wins,
        "visual_watchouts": visual_watchouts,
        "sound_wins": sound_wins,
        "sound_watchouts": sound_watchouts,
        "packaging_wins": packaging_wins,
        "packaging_watchouts": packaging_watchouts,
        "retention_wins": retention_wins,
        "retention_watchouts": retention_watchouts,
        "reference_summary": _clip_text(str(data.get("last_reference_summary", "") or ""), 280),
        "reference_hook_rewrites": list(data.get("reference_hook_rewrites") or []),
        "reference_pacing_rewrites": list(data.get("reference_pacing_rewrites") or []),
        "reference_visual_rewrites": list(data.get("reference_visual_rewrites") or []),
        "reference_sound_rewrites": list(data.get("reference_sound_rewrites") or []),
        "reference_packaging_rewrites": list(data.get("reference_packaging_rewrites") or []),
        "reference_next_video_moves": list(data.get("reference_next_video_moves") or []),
        "last_outcome_summary": _clip_text(str(data.get("last_outcome_summary", "") or ""), 220),
        "series_catalog": series_catalog,
        "series_memory_key": active_series_key,
        "selected_cluster_label": str(data.get("selected_cluster_label", "") or ""),
        "selected_cluster_key": str(data.get("selected_cluster_key", "") or ""),
    }
    public["rewrite_pressure"] = _catalyst_rewrite_pressure_profile(public)
    return public


def _resolve_catalyst_series_context(channel_context: dict | None, *, channel_memory: dict | None = None, topic: str = "", source_title: str = "", input_title: str = "", input_description: str = "", format_preset: str = "") -> dict:
    channel_context = dict(channel_context or {})
    channel_memory_raw = dict(channel_memory or {})
    extracted_anchor = _catalyst_extract_series_anchor(input_title, source_title, topic, input_description, niche_key=str(channel_memory_raw.get("niche_key", "") or ""))
    selected_cluster = _select_catalyst_channel_series_cluster(
        channel_context,
        topic=" ".join(part for part in [topic, input_title, input_description] if str(part or "").strip()),
        source_title=source_title or input_title,
        channel_memory=channel_memory_raw,
        format_preset=format_preset,
    )
    cluster_anchor = str((selected_cluster or {}).get("series_anchor", "") or "").strip()
    series_anchor_override = cluster_anchor or extracted_anchor
    memory_view = _catalyst_channel_memory_public_view(channel_memory_raw, series_anchor_override=series_anchor_override)
    return {
        "selected_cluster": dict(selected_cluster or {}),
        "series_anchor_override": series_anchor_override,
        "cluster_context": _render_catalyst_series_cluster_context(selected_cluster),
        "memory_view": memory_view,
    }


def _catalyst_rewrite_pressure_profile(memory_public: dict | None) -> dict:
    public = dict(memory_public or {})
    hook_wins = [str(v).strip() for v in list(public.get("hook_wins") or []) if str(v).strip()]
    hook_watchouts = [str(v).strip() for v in list(public.get("hook_watchouts") or []) if str(v).strip()]
    pacing_wins = [str(v).strip() for v in list(public.get("pacing_wins") or []) if str(v).strip()]
    pacing_watchouts = [str(v).strip() for v in list(public.get("pacing_watchouts") or []) if str(v).strip()]
    visual_wins = [str(v).strip() for v in list(public.get("visual_wins") or []) if str(v).strip()]
    visual_watchouts = [str(v).strip() for v in list(public.get("visual_watchouts") or []) if str(v).strip()]
    sound_wins = [str(v).strip() for v in list(public.get("sound_wins") or []) if str(v).strip()]
    sound_watchouts = [str(v).strip() for v in list(public.get("sound_watchouts") or []) if str(v).strip()]
    packaging_wins = [str(v).strip() for v in list(public.get("packaging_wins") or []) if str(v).strip()]
    packaging_watchouts = [str(v).strip() for v in list(public.get("packaging_watchouts") or []) if str(v).strip()]
    retention_watchouts = [str(v).strip() for v in list(public.get("retention_watchouts") or []) if str(v).strip()]
    next_moves = [str(v).strip() for v in list(public.get("next_video_moves") or []) if str(v).strip()]
    reference_hook_rewrites = [str(v).strip() for v in list(public.get("reference_hook_rewrites") or []) if str(v).strip()]
    reference_pacing_rewrites = [str(v).strip() for v in list(public.get("reference_pacing_rewrites") or []) if str(v).strip()]
    reference_visual_rewrites = [str(v).strip() for v in list(public.get("reference_visual_rewrites") or []) if str(v).strip()]
    reference_sound_rewrites = [str(v).strip() for v in list(public.get("reference_sound_rewrites") or []) if str(v).strip()]
    reference_packaging_rewrites = [str(v).strip() for v in list(public.get("reference_packaging_rewrites") or []) if str(v).strip()]
    avg_ctr = float(public.get("average_ctr", 0.0) or 0.0)
    avg_avp = float(public.get("average_average_percentage_viewed", 0.0) or 0.0)
    avg_first30 = float(public.get("average_first_30_sec_retention_pct", 0.0) or 0.0)
    avg_first60 = float(public.get("average_first_60_sec_retention_pct", 0.0) or 0.0)
    avg_hook = float(public.get("average_reference_hook_score", 0.0) or 0.0)
    avg_pacing = float(public.get("average_reference_pacing_score", 0.0) or 0.0)
    avg_visual = float(public.get("average_reference_visual_score", 0.0) or 0.0)
    avg_sound = float(public.get("average_reference_sound_score", 0.0) or 0.0)
    avg_packaging = float(public.get("average_reference_packaging_score", 0.0) or 0.0)
    avg_title_novelty = float(public.get("average_reference_title_novelty_score", 0.0) or 0.0)
    hook_pressure = 12.0 + max(0.0, (62.0 - avg_first30) * 1.2) + max(0.0, (76.0 - avg_hook) * 0.55) + (len(hook_watchouts) * 7.0) + (min(2, len(retention_watchouts)) * 4.0) - (len(hook_wins) * 4.0)
    pacing_pressure = 10.0 + max(0.0, (42.0 - avg_avp) * 1.1) + max(0.0, (52.0 - avg_first60) * 0.8) + max(0.0, (76.0 - avg_pacing) * 0.5) + (len(pacing_watchouts) * 7.0) - (len(pacing_wins) * 4.0)
    visual_pressure = 9.0 + max(0.0, (78.0 - avg_visual) * 0.55) + (len(visual_watchouts) * 7.0) - (len(visual_wins) * 4.0)
    sound_pressure = 8.0 + max(0.0, (78.0 - avg_sound) * 0.55) + max(0.0, (52.0 - avg_first60) * 0.35) + (len(sound_watchouts) * 7.0) - (len(sound_wins) * 4.0)
    packaging_pressure = 10.0 + max(0.0, (4.5 - avg_ctr) * 12.0) + max(0.0, (76.0 - avg_packaging) * 0.65) + max(0.0, (82.0 - avg_title_novelty) * 0.35) + (len(packaging_watchouts) * 7.0) - (len(packaging_wins) * 4.0)
    categories = [
        {"key": "hook", "label": "Hook", "score": max(8, min(100, int(round(hook_pressure)))), "wins": hook_wins[:3], "watchouts": hook_watchouts[:3], "rewrites": reference_hook_rewrites[:3]},
        {"key": "pacing", "label": "Pacing", "score": max(8, min(100, int(round(pacing_pressure)))), "wins": pacing_wins[:3], "watchouts": pacing_watchouts[:3], "rewrites": reference_pacing_rewrites[:3]},
        {"key": "visuals", "label": "Visuals", "score": max(8, min(100, int(round(visual_pressure)))), "wins": visual_wins[:3], "watchouts": visual_watchouts[:3], "rewrites": reference_visual_rewrites[:3]},
        {"key": "sound", "label": "Sound", "score": max(8, min(100, int(round(sound_pressure)))), "wins": sound_wins[:3], "watchouts": sound_watchouts[:3], "rewrites": reference_sound_rewrites[:3]},
        {"key": "packaging", "label": "Packaging", "score": max(8, min(100, int(round(packaging_pressure)))), "wins": packaging_wins[:3], "watchouts": packaging_watchouts[:3], "rewrites": reference_packaging_rewrites[:3]},
    ]
    categories.sort(key=lambda row: int(row.get("score", 0) or 0), reverse=True)
    for row in categories:
        row["severity"] = _catalyst_pressure_label(float(row.get("score", 0) or 0.0))
    top = categories[0] if categories else {}
    secondary = categories[1] if len(categories) > 1 else {}
    priorities = _dedupe_preserve_order([*(top.get("rewrites") or []), *(top.get("watchouts") or []), *(secondary.get("rewrites") or []), *(secondary.get("watchouts") or []), *next_moves[:3]], max_items=8, max_chars=180)
    summary = ""
    if top:
        summary = f"Primary rewrite pressure is {str(top.get('label', 'Hook'))} ({int(top.get('score', 0) or 0)}/100, {str(top.get('severity', 'medium'))}). "
        if secondary:
            summary += f"Secondary pressure is {str(secondary.get('label', 'Packaging'))} ({int(secondary.get('score', 0) or 0)}/100, {str(secondary.get('severity', 'medium'))}). "
        if priorities:
            summary += "Next run should prioritize: " + "; ".join(priorities[:3]) + "."
    return {
        "summary": _clip_text(summary, 320),
        "primary_focus": str(top.get("key", "") or ""),
        "secondary_focus": str(secondary.get("key", "") or ""),
        "categories": categories,
        "next_run_priorities": priorities,
    }


def _catalyst_reference_score_tier(score: float) -> str:
    numeric = float(score or 0.0)
    if numeric >= 88:
        return "breakout"
    if numeric >= 75:
        return "strong"
    if numeric >= 60:
        return "competitive"
    if numeric >= 45:
        return "developing"
    return "early"


def _catalyst_reference_signal_list(chosen_entries: list[dict], field_name: str, *, max_items: int = 6) -> list[str]:
    rows: list[str] = []
    for entry in list(chosen_entries or []):
        memory = dict(entry.get("memory_seed") or {})
        rows.extend(str(v).strip() for v in list(memory.get(field_name) or []) if str(v).strip())
    return _dedupe_preserve_order(rows, max_items=max_items, max_chars=180)
