from __future__ import annotations

import asyncio
import base64
import hashlib
import html as html_lib
import hmac
import json
import logging
import re
import secrets
import shutil
import subprocess
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode, urlparse

import httpx
from fastapi import HTTPException
from fastapi.responses import RedirectResponse, Response

from backend_settings import (
    ALGROW_API_BASE_URL,
    ALGROW_API_KEY,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_INSTALLED_CLIENT_ID,
    GOOGLE_INSTALLED_CLIENT_SECRET,
    GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE,
    GOOGLE_INSTALLED_OAUTH_SOURCE,
    GOOGLE_INSTALLED_REDIRECT_URI,
    GOOGLE_OAUTH_CLIENT_KIND,
    GOOGLE_OAUTH_CONFIG_ISSUE,
    GOOGLE_OAUTH_SOURCE,
    GOOGLE_REDIRECT_URI,
    SITE_URL,
    SUPABASE_JWT_SECRET,
    YOUTUBE_API_KEY,
    YOUTUBE_API_KEYS,
    YOUTUBE_CONNECTIONS_FILE,
    YOUTUBE_OAUTH_MODE,
    YOUTUBE_OAUTH_STATES_FILE,
    YOUTUBE_SIGNAL_LOG_FILE,
    TEMP_DIR,
)

YOUTUBE_OAUTH_STATE_TTL_SEC = 20 * 60
YOUTUBE_TOKEN_REFRESH_MARGIN_SEC = 120
YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube.upload",
]
YOUTUBE_AUTH_BASE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"
YOUTUBE_DATA_API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_ANALYTICS_API_BASE = "https://youtubeanalytics.googleapis.com/v2"
YOUTUBE_ALGROW_LONGFORM_WORKSPACES = {"documentary", "recap", "explainer", "story_channel"}

_youtube_connections: dict[str, dict] = {}
_youtube_connections_lock = asyncio.Lock()
_youtube_oauth_states: dict[str, dict] = {}
_youtube_oauth_states_lock = asyncio.Lock()
_youtube_historical_compare_public_view = lambda payload: dict(payload or {})
_youtube_channel_audit_public_view = lambda payload: dict(payload or {})
_youtube_source_url_video_id = lambda source_url: ""
_youtube_normalize_external_source_url = lambda raw_value: str(raw_value or "").strip()
_youtube_duration_text_to_seconds = lambda raw: 0
_youtube_parse_vtt_text = lambda raw: ""
_youtube_ytdlp_module = None
_youtube_ytdlp_extract_info_blocking = lambda source_url: {}
_youtube_dedupe_clip_list = lambda values, max_items=6: _dedupe_preserve_order(
    [str(v).strip() for v in list(values or []) if str(v).strip()],
    max_items=max_items,
    max_chars=200,
)
_youtube_catalyst_infer_niche = lambda title, description, tags: {}
_youtube_catalyst_infer_archetype = lambda title, description, tags, niche_key="": {}
_youtube_catalyst_extract_series_anchor = lambda title, description, tags, niche_key="": ""
_youtube_catalyst_build_channel_series_clusters = lambda rows, top_videos=None: []
_youtube_build_catalyst_cluster_playbook = lambda clusters: {}
_youtube_packaging_tokens = lambda text, max_items=18: _dedupe_preserve_order(
    re.findall(r"[A-Za-z0-9']+", str(text or "").lower()),
    max_items=max_items,
    max_chars=40,
)
_youtube_same_arena_subject = lambda row, topic="": str(topic or "").strip()
_youtube_same_arena_title_variants = lambda row, topic="", format_preset="", max_items=5: []

log = logging.getLogger("nyptid-studio")

SHORTS_REFERENCE_STOPWORDS = {
    "the", "and", "with", "your", "that", "from", "this", "what", "when", "have", "into", "about", "just",
    "they", "them", "their", "there", "then", "than", "will", "would", "could", "shorts", "short", "viral",
    "story", "video", "videos", "you", "how", "why", "for", "are", "not", "was", "his", "her", "our", "out",
    "new", "top", "best", "make", "made", "using", "used",
}


def _clip_text(value: str, max_chars: int = 240) -> str:
    compact = re.sub(r"\s+", " ", str(value or "").strip())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 1)].rstrip() + "…"



def _youtube_clean_error_text(value: str) -> str:
    text = html_lib.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if text.endswith("."):
        text = text[:-1].rstrip()
    return text


def _youtube_extract_error_reason_and_message(raw_text: str) -> tuple[str, str]:
    reason = ""
    message = _youtube_clean_error_text(raw_text)
    try:
        payload = json.loads(str(raw_text or ""))
    except Exception:
        return reason, message
    if not isinstance(payload, dict):
        return reason, message
    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        error_message = _youtube_clean_error_text(error_payload.get("message", ""))
        if error_message:
            message = error_message
        for row in list(error_payload.get("errors") or []):
            if not isinstance(row, dict):
                continue
            row_reason = str(row.get("reason", "") or "").strip().lower()
            row_message = _youtube_clean_error_text(row.get("message", ""))
            if row_reason and not reason:
                reason = row_reason
            if row_message and not message:
                message = row_message
            if reason and message:
                break
    elif isinstance(payload.get("message"), str):
        top_message = _youtube_clean_error_text(payload.get("message", ""))
        if top_message:
            message = top_message
    return reason, message


def _youtube_error_is_quota_related(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return any(
        token in lowered
        for token in (
            "quotaexceeded",
            "exceeded your quota",
            "quota exceeded",
            "youtube.quota",
            "dailylimitexceeded",
            "userratelimitexceeded",
            "ratelimitexceeded",
        )
    )


def _youtube_error_is_auth_related(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return any(
        token in lowered
        for token in (
            "invalid_grant",
            "token has been expired or revoked",
            "token has been revoked",
            "unauthorized_client",
            "unauthorized",
            "disabled_client",
            "oauth client was disabled",
            "oauth client is disabled or deleted",
            "access_denied",
            "insufficientpermissions",
            "insufficient permissions",
        )
    )


def _youtube_format_api_failure(status_code: int, raw_text: str, *, using_api_key: bool = False) -> str:
    reason, clean_message = _youtube_extract_error_reason_and_message(raw_text)
    probe = " ".join(
        part
        for part in (
            reason,
            clean_message,
            _youtube_clean_error_text(raw_text),
        )
        if str(part or "").strip()
    ).lower()
    if _youtube_error_is_quota_related(probe):
        if using_api_key:
            return (
                "YouTube API quota is exhausted for the configured key pool right now. "
                "Catalyst will keep using public channel-page fallback data until quota resets."
            )
        return (
            "YouTube API quota is exhausted for this Google project right now. "
            "Catalyst can still use public channel fallback data, but private impressions/CTR/retention are temporarily unavailable."
        )
    if status_code in (401, 403) and _youtube_error_is_auth_related(probe):
        return "YouTube authorization for this channel needs to be refreshed before private metrics can sync again."
    detail = clean_message or _youtube_clean_error_text(raw_text)
    if reason and reason not in detail.lower():
        detail = f"{detail} (reason: {reason})"
    return f"YouTube API failed ({status_code}): {_clip_text(detail, 240)}"


def _youtube_private_metrics_limitation(sync_error: str) -> str:
    probe = str(sync_error or "").strip()
    if not probe:
        return ""
    if _youtube_error_is_quota_related(probe):
        return (
            "Private impressions, CTR, and retention metrics are temporarily unavailable because the YouTube API quota is exhausted. "
            "Catalyst is using public channel fallback data."
        )
    if _youtube_error_is_auth_related(probe):
        return (
            "Private impressions, CTR, and retention metrics are unavailable until this channel is reconnected to Google OAuth. "
            "Catalyst is using public channel fallback data."
        )
    return (
        "Private impressions, CTR, and retention metrics are temporarily unavailable from YouTube, "
        "so Catalyst is using public channel fallback data."
    )


def _dedupe_preserve_order(values: list[str], max_items: int = 250, max_chars: int = 128) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in list(values or []):
        value = str(raw or "").strip()
        if not value:
            continue
        value = value[:max_chars]
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(value)
        if len(out) >= max_items:
            break
    return out


def _youtube_title_keywords(title: str, max_items: int = 6) -> list[str]:
    tokens = re.findall(r"[A-Za-z0-9']+", str(title or "").lower())
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if len(token) < 3 or token in SHORTS_REFERENCE_STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= max_items:
            break
    return out


def _load_youtube_connections() -> None:
    global _youtube_connections
    try:
        if YOUTUBE_CONNECTIONS_FILE.exists():
            data = json.loads(YOUTUBE_CONNECTIONS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _youtube_connections = data
                return
    except Exception:
        pass
    _youtube_connections = {}


def _save_youtube_connections() -> None:
    try:
        YOUTUBE_CONNECTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        YOUTUBE_CONNECTIONS_FILE.write_text(
            json.dumps(_youtube_connections, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def _prune_youtube_oauth_states() -> None:
    now = time.time()
    stale = [
        state
        for state, payload in list(_youtube_oauth_states.items())
        if now - float((payload or {}).get("created_at", 0.0) or 0.0) > YOUTUBE_OAUTH_STATE_TTL_SEC
    ]
    for state in stale:
        _youtube_oauth_states.pop(state, None)


def _load_youtube_oauth_states() -> None:
    global _youtube_oauth_states
    try:
        if YOUTUBE_OAUTH_STATES_FILE.exists():
            data = json.loads(YOUTUBE_OAUTH_STATES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _youtube_oauth_states = data
                _prune_youtube_oauth_states()
                return
    except Exception:
        pass
    _youtube_oauth_states = {}


def _save_youtube_oauth_states() -> None:
    try:
        _prune_youtube_oauth_states()
        YOUTUBE_OAUTH_STATES_FILE.parent.mkdir(parents=True, exist_ok=True)
        YOUTUBE_OAUTH_STATES_FILE.write_text(
            json.dumps(_youtube_oauth_states, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def _youtube_oauth_state_signing_key() -> bytes:
    for raw in (
        SUPABASE_JWT_SECRET,
        GOOGLE_CLIENT_SECRET,
        GOOGLE_INSTALLED_CLIENT_SECRET,
    ):
        cleaned = str(raw or "").strip()
        if cleaned:
            return cleaned.encode("utf-8")
    return b"nyptid-youtube-oauth-state"


def _youtube_oauth_state_payload(payload: dict | None) -> dict:
    item = dict(payload or {})
    created_at_raw = item.get("created_at", 0.0)
    try:
        created_at = float(created_at_raw or 0.0)
    except Exception:
        created_at = 0.0
    if created_at <= 0.0:
        created_at = time.time()
    oauth_mode = str(item.get("oauth_mode", "") or "").strip().lower()
    if oauth_mode not in {"web", "installed"}:
        oauth_mode = "web"
    return {
        "user_id": str(item.get("user_id", "") or "").strip(),
        "created_at": created_at,
        "next_url": str(item.get("next_url", "") or "").strip(),
        "oauth_mode": oauth_mode,
        "pkce_verifier": str(item.get("pkce_verifier", "") or "").strip(),
    }


def _youtube_oauth_state_encode(payload: dict | None) -> str:
    canonical = _youtube_oauth_state_payload(payload)
    raw_bytes = json.dumps(canonical, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(raw_bytes).decode("ascii").rstrip("=")
    signature = hmac.new(
        _youtube_oauth_state_signing_key(),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).digest()
    signature_b64 = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
    return f"v2.{payload_b64}.{signature_b64}"


def _youtube_oauth_state_b64_decode(raw_value: str) -> bytes:
    value = str(raw_value or "").strip()
    padding = "=" * ((4 - (len(value) % 4)) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _youtube_oauth_state_decode(state_token: str) -> dict:
    token = str(state_token or "").strip()
    if not token.startswith("v2."):
        return {}
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    payload_b64 = str(parts[1] or "").strip()
    signature_b64 = str(parts[2] or "").strip()
    if not payload_b64 or not signature_b64:
        return {}
    expected_signature = hmac.new(
        _youtube_oauth_state_signing_key(),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).digest()
    expected_signature_b64 = base64.urlsafe_b64encode(expected_signature).decode("ascii").rstrip("=")
    if not hmac.compare_digest(expected_signature_b64, signature_b64):
        return {}
    try:
        payload = json.loads(_youtube_oauth_state_b64_decode(payload_b64).decode("utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized = _youtube_oauth_state_payload(payload)
    created_at = float(normalized.get("created_at", 0.0) or 0.0)
    if created_at <= 0.0:
        return {}
    if time.time() - created_at > YOUTUBE_OAUTH_STATE_TTL_SEC:
        return {}
    if not str(normalized.get("user_id", "") or "").strip():
        return {}
    return normalized


def _youtube_oauth_state_lookup(state_token: str, *, consume: bool = False) -> dict:
    token = str(state_token or "").strip()
    if not token:
        return {}
    if consume:
        payload = dict(_youtube_oauth_states.pop(token, {}) or {})
    else:
        payload = dict(_youtube_oauth_states.get(token, {}) or {})
    if payload:
        return _youtube_oauth_state_payload(payload)
    decoded = _youtube_oauth_state_decode(token)
    if decoded and not consume:
        _youtube_oauth_states[token] = dict(decoded)
    return decoded


def _youtube_web_auth_configured() -> bool:
    return bool(
        GOOGLE_CLIENT_ID
        and GOOGLE_CLIENT_SECRET
        and GOOGLE_REDIRECT_URI
        and GOOGLE_OAUTH_CLIENT_KIND == "web"
        and not GOOGLE_OAUTH_CONFIG_ISSUE
    )


def _youtube_installed_auth_configured() -> bool:
    return bool(
        GOOGLE_INSTALLED_CLIENT_ID
        and GOOGLE_INSTALLED_CLIENT_SECRET
        and GOOGLE_INSTALLED_REDIRECT_URI
        and not GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE
    )


def _youtube_active_oauth_mode(preferred_mode: str | None = None) -> str:
    normalized = str(preferred_mode or YOUTUBE_OAUTH_MODE or "auto").strip().lower()
    if normalized not in {"auto", "web", "installed"}:
        normalized = "auto"
    if normalized == "web":
        return "web" if _youtube_web_auth_configured() else ""
    if normalized == "installed":
        if _youtube_installed_auth_configured():
            return "installed"
        return "web" if _youtube_web_auth_configured() else ""
    if _youtube_web_auth_configured():
        return "web"
    if _youtube_installed_auth_configured():
        return "installed"
    return ""


def _youtube_auth_context(mode: str | None = None) -> dict:
    resolved_mode = _youtube_active_oauth_mode(mode)
    if resolved_mode == "installed":
        return {
            "mode": "installed",
            "client_id": GOOGLE_INSTALLED_CLIENT_ID,
            "client_secret": GOOGLE_INSTALLED_CLIENT_SECRET,
            "redirect_uri": GOOGLE_INSTALLED_REDIRECT_URI,
            "source": GOOGLE_INSTALLED_OAUTH_SOURCE,
            "client_kind": "installed",
        }
    if resolved_mode == "web":
        return {
            "mode": "web",
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "source": GOOGLE_OAUTH_SOURCE,
            "client_kind": GOOGLE_OAUTH_CLIENT_KIND or "web",
        }
    return {
        "mode": "",
        "client_id": "",
        "client_secret": "",
        "redirect_uri": "",
        "source": "",
        "client_kind": "",
    }


def _youtube_auth_configured() -> bool:
    return bool(_youtube_active_oauth_mode())


def _youtube_auth_issue_message() -> str:
    preferred_mode = str(YOUTUBE_OAUTH_MODE or "auto").strip().lower()
    if preferred_mode == "installed":
        if GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE == "client_secrets_missing_redirect_uri":
            return "Google YouTube OAuth fallback is missing an installed-app redirect URI in client_secrets.json."
        if GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE == "client_secrets_missing_client_credentials":
            return "Google YouTube OAuth fallback client_secrets.json is present but missing installed-app credentials."
        if GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE == "client_secrets_invalid_json":
            return "Google YouTube OAuth fallback client_secrets.json is unreadable JSON."
        if GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE == "missing_google_oauth_credentials":
            return "Google YouTube OAuth fallback is enabled, but client_secrets.json is missing from the backend image."
        return "Google YouTube OAuth installed-client fallback is not configured on the backend yet."
    issue = str(GOOGLE_OAUTH_CONFIG_ISSUE or "").strip().lower()
    if issue == "desktop_client_not_supported_for_backend_oauth":
        return (
            "Google YouTube OAuth is loading a desktop/installed client file, but Studio is still configured for the backend web callback path. "
            "Either set YOUTUBE_OAUTH_MODE=installed so Studio uses the installed-client fallback flow, or create a Google Web application OAuth client and register "
            f"{GOOGLE_REDIRECT_URI} as an authorized redirect URI."
        )
    if issue == "redirect_uri_not_listed_in_google_client":
        return (
            "Google YouTube OAuth is loading a web client whose authorized redirect URIs do not include "
            f"{GOOGLE_REDIRECT_URI}. Update the Google OAuth client or the backend redirect URI."
        )
    if issue == "client_secrets_missing_client_credentials":
        return "Google YouTube OAuth client_secrets.json is present but missing client credentials."
    if issue == "client_secrets_invalid_json":
        return "Google YouTube OAuth client_secrets.json is unreadable JSON."
    if issue == "missing_google_oauth_credentials":
        return "Google YouTube OAuth credentials are missing on the backend."
    return "Google YouTube OAuth is not configured on the backend yet"


def _format_google_oauth_failure(action: str, status_code: int, body: str, oauth_mode: str | None = None) -> str:
    detail = _clip_text(body, 220)
    lower = detail.lower()
    attempted_mode = str(oauth_mode or "").strip().lower()
    active_mode = _youtube_active_oauth_mode()
    if "consumer project" in lower or "project_number" in lower or "suspend" in lower:
        if attempted_mode == "web" and active_mode == "installed" and _youtube_installed_auth_configured():
            return (
                f"Google {action} failed ({status_code}): this connected YouTube channel is still tied to the old backend web OAuth client, and that client's Google Cloud project is suspended. "
                "Reconnect the channel under the installed OAuth flow, or replace GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET for the old web client."
            )
        if _youtube_installed_auth_configured():
            return (
                f"Google {action} failed ({status_code}): the configured backend web OAuth client belongs to a suspended Google Cloud project. "
                "Switch YOUTUBE_OAUTH_MODE to installed or replace GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET on the deployed backend, then reconnect the YouTube channel."
            )
        return (
            f"Google {action} failed ({status_code}): the configured backend OAuth client belongs to a suspended Google Cloud project. "
            "Replace GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET on the deployed backend, then reconnect the YouTube channel."
        )
    if "oauth client was deleted" in lower or "oauth client was disabled" in lower or "deleted_client" in lower or "disabled_client" in lower:
        if attempted_mode == "web" and active_mode == "installed" and _youtube_installed_auth_configured():
            return (
                f"Google {action} failed ({status_code}): this connected YouTube channel is still tied to the old backend web OAuth client, and that client is disabled or deleted. "
                "Reconnect the channel under the installed OAuth flow, or replace GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET for the old web client."
            )
        if _youtube_installed_auth_configured():
            return (
                f"Google {action} failed ({status_code}): the configured backend web OAuth client is disabled or deleted. "
                "Switch YOUTUBE_OAUTH_MODE to installed or replace GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET on the deployed backend, then reconnect the YouTube channel."
            )
        return (
            f"Google {action} failed ({status_code}): the configured backend OAuth client is disabled or deleted. "
            "Replace GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET on the deployed backend, then reconnect the YouTube channel."
        )
    if "unauthorized_client" in lower or '"error": "unauthorized"' in lower or ("error_description" in lower and '"unauthorized"' in lower):
        return (
            f"Google {action} failed ({status_code}): this connected YouTube channel's saved Google grant no longer matches the active backend OAuth client. "
            "Reconnect this specific channel under the current Google OAuth flow so private YouTube metrics can refresh again."
        )
    if "redirect_uri_mismatch" in lower:
        return (
            f"Google {action} failed ({status_code}): GOOGLE_REDIRECT_URI does not match an authorized redirect URI in the configured Google OAuth client."
        )
    return f"Google {action} failed ({status_code}): {detail}"


def _youtube_bucket_for_user(user_id: str) -> dict:
    user_key = str(user_id or "").strip()
    bucket = _youtube_connections.get(user_key)
    if not isinstance(bucket, dict):
        bucket = {"default_channel_id": "", "channels": {}}
        _youtube_connections[user_key] = bucket
    channels = bucket.get("channels")
    if not isinstance(channels, dict):
        channels = {}
        bucket["channels"] = channels
    return bucket


def configure_youtube_public_view_hooks(
    *,
    historical_compare_public_view=None,
    channel_audit_public_view=None,
) -> None:
    global _youtube_historical_compare_public_view, _youtube_channel_audit_public_view
    if callable(historical_compare_public_view):
        _youtube_historical_compare_public_view = historical_compare_public_view
    if callable(channel_audit_public_view):
        _youtube_channel_audit_public_view = channel_audit_public_view


def configure_youtube_runtime_hooks(
    *,
    source_url_video_id=None,
    normalize_external_source_url=None,
    duration_text_to_seconds=None,
    parse_vtt_text=None,
    ytdlp_module=None,
    ytdlp_extract_info_blocking=None,
) -> None:
    global _youtube_source_url_video_id
    global _youtube_normalize_external_source_url
    global _youtube_duration_text_to_seconds
    global _youtube_parse_vtt_text
    global _youtube_ytdlp_module
    global _youtube_ytdlp_extract_info_blocking
    if callable(source_url_video_id):
        _youtube_source_url_video_id = source_url_video_id
    if callable(normalize_external_source_url):
        _youtube_normalize_external_source_url = normalize_external_source_url
    if callable(duration_text_to_seconds):
        _youtube_duration_text_to_seconds = duration_text_to_seconds
    if callable(parse_vtt_text):
        _youtube_parse_vtt_text = parse_vtt_text
    if ytdlp_module is not None:
        _youtube_ytdlp_module = ytdlp_module
    if callable(ytdlp_extract_info_blocking):
        _youtube_ytdlp_extract_info_blocking = ytdlp_extract_info_blocking


def configure_youtube_analysis_hooks(
    *,
    dedupe_clip_list=None,
    catalyst_infer_niche=None,
    catalyst_infer_archetype=None,
    catalyst_extract_series_anchor=None,
    catalyst_build_channel_series_clusters=None,
    build_catalyst_cluster_playbook=None,
    packaging_tokens=None,
    same_arena_subject=None,
    same_arena_title_variants=None,
) -> None:
    global _youtube_dedupe_clip_list
    global _youtube_catalyst_infer_niche
    global _youtube_catalyst_infer_archetype
    global _youtube_catalyst_extract_series_anchor
    global _youtube_catalyst_build_channel_series_clusters
    global _youtube_build_catalyst_cluster_playbook
    global _youtube_packaging_tokens
    global _youtube_same_arena_subject
    global _youtube_same_arena_title_variants
    if callable(dedupe_clip_list):
        _youtube_dedupe_clip_list = dedupe_clip_list
    if callable(catalyst_infer_niche):
        _youtube_catalyst_infer_niche = catalyst_infer_niche
    if callable(catalyst_infer_archetype):
        _youtube_catalyst_infer_archetype = catalyst_infer_archetype
    if callable(catalyst_extract_series_anchor):
        _youtube_catalyst_extract_series_anchor = catalyst_extract_series_anchor
    if callable(catalyst_build_channel_series_clusters):
        _youtube_catalyst_build_channel_series_clusters = catalyst_build_channel_series_clusters
    if callable(build_catalyst_cluster_playbook):
        _youtube_build_catalyst_cluster_playbook = build_catalyst_cluster_playbook
    if callable(packaging_tokens):
        _youtube_packaging_tokens = packaging_tokens
    if callable(same_arena_subject):
        _youtube_same_arena_subject = same_arena_subject
    if callable(same_arena_title_variants):
        _youtube_same_arena_title_variants = same_arena_title_variants


async def _youtube_start_oauth_for_user(user: dict, next_url: str = "") -> dict:
    oauth_mode = _youtube_active_oauth_mode()
    if not oauth_mode:
        raise HTTPException(500, _youtube_auth_issue_message())
    pkce_verifier = ""
    if oauth_mode == "installed":
        pkce_verifier, _ = _youtube_pkce_pair()
    state_payload = {
        "user_id": str(user.get("id", "") or "").strip(),
        "created_at": time.time(),
        "next_url": str(next_url or "").strip(),
        "oauth_mode": oauth_mode,
        "pkce_verifier": pkce_verifier,
    }
    state_token = _youtube_oauth_state_encode(state_payload)
    async with _youtube_oauth_states_lock:
        _load_youtube_oauth_states()
        _prune_youtube_oauth_states()
        _youtube_oauth_states[state_token] = _youtube_oauth_state_payload(state_payload)
        _save_youtube_oauth_states()
    auth_url = _youtube_build_auth_url(state_token, oauth_mode)
    if oauth_mode == "installed":
        auth_url = _youtube_helper_page_url(state_token)
    return {"auth_url": auth_url, "oauth_mode": oauth_mode}


async def _youtube_finalize_oauth_connection(state_payload: dict, code: str = "", error: str = "") -> RedirectResponse:
    next_url = str(state_payload.get("next_url", "") or "").strip()
    user_id = str(state_payload.get("user_id", "") or "").strip()
    oauth_mode = str(state_payload.get("oauth_mode", "") or "").strip().lower()
    if oauth_mode not in {"web", "installed"}:
        oauth_mode = "web"
    code_verifier = str(state_payload.get("pkce_verifier", "") or "").strip()
    requested_channel_id = ""
    if next_url:
        try:
            requested_channel_id = str((parse_qs(urlparse(next_url).query or "").get("youtube_channel_id") or [""])[0] or "").strip()
        except Exception:
            requested_channel_id = ""
    if error:
        return RedirectResponse(_youtube_redirect_target(next_url, False, f"Google OAuth error: {error}"), status_code=302)
    if not user_id or not code:
        return RedirectResponse(_youtube_redirect_target(next_url, False, "Google OAuth callback was missing state or code"), status_code=302)
    try:
        token_payload = await _google_exchange_code_for_tokens(code, oauth_mode, code_verifier)
        access_token = str(token_payload.get("access_token", "") or "").strip()
        refresh_token = str(token_payload.get("refresh_token", "") or "").strip()
        expires_in = max(300, int(token_payload.get("expires_in", 3600) or 3600))
        scope = str(token_payload.get("scope", "") or "").strip()
        auth_context = _youtube_auth_context(oauth_mode)
        channels = await _youtube_fetch_my_channels(access_token)
        if not channels:
            return RedirectResponse(_youtube_redirect_target(next_url, False, "Google account returned no YouTube channels"), status_code=302)
        now = time.time()
        async with _youtube_connections_lock:
            _load_youtube_connections()
            bucket = _youtube_bucket_for_user(user_id)
            existing_channels = dict(bucket.get("channels") or {})
            for channel in channels:
                channel_id = str(channel.get("channel_id", "") or "").strip()
                if not channel_id:
                    continue
                previous = dict(existing_channels.get(channel_id) or {})
                existing_channels[channel_id] = {
                    **previous,
                    **channel,
                    "channel_id": channel_id,
                    "user_id": user_id,
                    "access_token": access_token,
                    "refresh_token": refresh_token or str(previous.get("refresh_token", "") or "").strip(),
                    "token_expires_at": now + expires_in,
                    "token_scope": scope,
                    "oauth_mode": oauth_mode,
                    "oauth_source": str(auth_context.get("source", "") or "").strip(),
                    "linked_at": float(previous.get("linked_at", now) or now),
                    "last_synced_at": float(previous.get("last_synced_at", 0.0) or 0.0),
                    "last_sync_error": "",
                }
            bucket["channels"] = existing_channels
            default_channel_id = str(bucket.get("default_channel_id", "") or "").strip()
            if requested_channel_id and requested_channel_id in existing_channels:
                bucket["default_channel_id"] = requested_channel_id
            elif not default_channel_id or default_channel_id not in existing_channels:
                bucket["default_channel_id"] = str(channels[0].get("channel_id", "") or "").strip()
            _save_youtube_connections()
            default_channel_id = str(bucket.get("default_channel_id", "") or "").strip()
        if default_channel_id:
            try:
                await _youtube_sync_and_persist_for_user(user_id, default_channel_id)
            except Exception:
                pass
        success_message = "YouTube channel connected"
        if oauth_mode == "installed":
            success_message = "YouTube channel connected through the installed Google client fallback"
        return RedirectResponse(_youtube_redirect_target(next_url, True, success_message), status_code=302)
    except Exception as e:
        return RedirectResponse(_youtube_redirect_target(next_url, False, str(e)), status_code=302)


async def _youtube_start_oauth_browser_redirect(next_url: str = "", access_token: str = "", get_current_user=None) -> RedirectResponse:
    token = str(access_token or "").strip()
    target = str(next_url or "").strip()
    if not token:
        return RedirectResponse(
            _youtube_redirect_target(target, False, "Authentication required. Please sign in."),
            status_code=302,
        )

    class _FakeCred:
        credentials = ""

    fake = _FakeCred()
    fake.credentials = token
    if not callable(get_current_user):
        return RedirectResponse(
            _youtube_redirect_target(target, False, "Authentication required. Please sign in."),
            status_code=302,
        )
    user = await get_current_user(fake)
    if not user:
        return RedirectResponse(
            _youtube_redirect_target(target, False, "Authentication required. Please sign in."),
            status_code=302,
        )
    try:
        payload = await _youtube_start_oauth_for_user(user, target)
        auth_url = str(payload.get("auth_url", "") or "").strip()
        if not auth_url:
            raise HTTPException(500, "Google auth URL missing")
        return RedirectResponse(auth_url, status_code=302)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else "Failed to start Google YouTube connection"
        return RedirectResponse(_youtube_redirect_target(target, False, detail), status_code=302)
    except Exception as exc:
        return RedirectResponse(_youtube_redirect_target(target, False, str(exc)), status_code=302)


async def _google_youtube_oauth_installed_helper_response(state: str = "") -> Response | RedirectResponse:
    state_token = str(state or "").strip()
    async with _youtube_oauth_states_lock:
        _load_youtube_oauth_states()
        _prune_youtube_oauth_states()
        state_payload = _youtube_oauth_state_lookup(state_token, consume=False)
        _save_youtube_oauth_states()
    if not state_payload:
        return Response(
            _youtube_installed_helper_html("", "", "That YouTube connection request expired. Start the connection again from Studio."),
            media_type="text/html",
        )
    oauth_mode = str(state_payload.get("oauth_mode", "") or "").strip().lower()
    if oauth_mode != "installed":
        return RedirectResponse(_youtube_redirect_target(str(state_payload.get("next_url", "") or "").strip(), False, "This YouTube connection request is not using the installed-client fallback"), status_code=302)
    pkce_verifier = str(state_payload.get("pkce_verifier", "") or "").strip()
    if not pkce_verifier:
        return Response(
            _youtube_installed_helper_html(state_token, "", "Studio lost the PKCE verifier for this request. Start the YouTube connection again."),
            media_type="text/html",
        )
    auth_url = _youtube_build_auth_url(state_token, "installed", _youtube_pkce_challenge(pkce_verifier))
    return Response(_youtube_installed_helper_html(state_token, auth_url), media_type="text/html")


async def _google_youtube_oauth_complete_redirect(state: str = "", redirect_url: str = "") -> RedirectResponse:
    code, error = _youtube_extract_code_or_error(redirect_url)
    state_token = str(state or "").strip()
    async with _youtube_oauth_states_lock:
        _load_youtube_oauth_states()
        _prune_youtube_oauth_states()
        state_payload = _youtube_oauth_state_lookup(state_token, consume=True)
        _save_youtube_oauth_states()
    if not state_payload:
        return RedirectResponse(_youtube_redirect_target("", False, "That YouTube connection request expired before it was completed"), status_code=302)
    return await _youtube_finalize_oauth_connection(state_payload, code=code, error=error)


async def _google_youtube_oauth_callback_redirect(code: str = "", state: str = "", error: str = "") -> RedirectResponse:
    async with _youtube_oauth_states_lock:
        _load_youtube_oauth_states()
        _prune_youtube_oauth_states()
        state_payload = _youtube_oauth_state_lookup(str(state or "").strip(), consume=True)
        _save_youtube_oauth_states()
    return await _youtube_finalize_oauth_connection(state_payload, code=code, error=error)


def _youtube_extract_code_or_error(raw_value: str) -> tuple[str, str]:
    value = str(raw_value or "").strip()
    if not value:
        return "", ""
    if value.startswith("http://") or value.startswith("https://"):
        try:
            parsed = urlparse(value)
            params = parse_qs(parsed.query or "")
            code = str((params.get("code") or [""])[0] or "").strip()
            error = str((params.get("error") or [""])[0] or "").strip()
            return code, error
        except Exception:
            return "", ""
    if "code=" in value or "error=" in value:
        try:
            params = parse_qs(value.lstrip("?#"))
            code = str((params.get("code") or [""])[0] or "").strip()
            error = str((params.get("error") or [""])[0] or "").strip()
            return code, error
        except Exception:
            return "", ""
    return value, ""


def _youtube_installed_helper_html(state_token: str, auth_url: str, error_message: str = "") -> str:
    safe_auth_url = html_lib.escape(str(auth_url or "").strip(), quote=True)
    safe_state = html_lib.escape(str(state_token or "").strip(), quote=True)
    safe_error = html_lib.escape(str(error_message or "").strip())
    error_block = f'<p class="error">{safe_error}</p>' if safe_error else ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Connect YouTube</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #07131a;
      --panel: rgba(8, 23, 31, 0.92);
      --border: rgba(125, 211, 252, 0.18);
      --text: #e5f4ff;
      --muted: #9fb4c4;
      --accent: #38bdf8;
      --danger: #fca5a5;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: radial-gradient(circle at top, rgba(56, 189, 248, 0.2), transparent 38%), linear-gradient(180deg, #040b10, var(--bg));
      color: var(--text);
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    .panel {{
      width: min(760px, 100%);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.45);
    }}
    h1 {{ margin: 0 0 12px; font-size: 28px; }}
    p {{ color: var(--muted); line-height: 1.55; }}
    ol {{ margin: 20px 0; padding-left: 20px; color: var(--muted); }}
    li {{ margin-bottom: 10px; }}
    .actions {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 24px 0 18px; }}
    .button {{
      appearance: none;
      border: 0;
      border-radius: 14px;
      background: linear-gradient(135deg, #0ea5e9, #2563eb);
      color: white;
      padding: 14px 18px;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
    }}
    .button.secondary {{
      background: rgba(255, 255, 255, 0.06);
      border: 1px solid rgba(255, 255, 255, 0.08);
    }}
    textarea {{
      width: 100%;
      min-height: 150px;
      resize: vertical;
      border-radius: 16px;
      border: 1px solid rgba(255, 255, 255, 0.12);
      background: rgba(0, 0, 0, 0.28);
      color: var(--text);
      padding: 14px;
      font: inherit;
    }}
    .hint {{ font-size: 13px; }}
    .error {{
      margin: 0 0 16px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(127, 29, 29, 0.35);
      border: 1px solid rgba(252, 165, 165, 0.25);
      color: var(--danger);
    }}
  </style>
</head>
<body>
  <main class="panel">
    <h1>Connect YouTube with the installed Google client</h1>
    <p>Studio can still connect your YouTube channel even when the backend web OAuth client is unavailable. Google will return you to a localhost URL after consent. That page will not load. Copy the full URL from your browser bar and paste it below.</p>
    {error_block}
    <ol>
      <li>Open the Google consent screen in a new tab.</li>
      <li>Approve access to YouTube and YouTube Analytics.</li>
      <li>When your browser lands on a <code>http://localhost...</code> URL, copy the full address.</li>
      <li>Paste it here and submit to finish connecting the channel.</li>
    </ol>
    <div class="actions">
      <a class="button" href="{safe_auth_url}" target="_blank" rel="noopener noreferrer">Open Google Consent</a>
      <a class="button secondary" href="{html_lib.escape(_youtube_redirect_target('', False, 'YouTube connection canceled'), quote=True)}">Cancel</a>
    </div>
    <form method="post" action="/api/oauth/google/youtube/complete">
      <input type="hidden" name="state" value="{safe_state}" />
      <textarea name="redirect_url" placeholder="Paste the full localhost URL or just the code value" required></textarea>
      <p class="hint">Example: <code>http://localhost/?state=...&amp;code=4/0...</code></p>
      <div class="actions">
        <button type="submit" class="button">Finish Connection</button>
      </div>
    </form>
  </main>
</body>
</html>"""


async def _google_exchange_code_for_tokens(code: str, oauth_mode: str | None = None, code_verifier: str = "") -> dict:
    auth_context = _youtube_auth_context(oauth_mode)
    form_payload = {
        "code": code,
        "client_id": str(auth_context.get("client_id", "") or "").strip(),
        "redirect_uri": str(auth_context.get("redirect_uri", "") or "").strip(),
        "grant_type": "authorization_code",
    }
    client_secret = str(auth_context.get("client_secret", "") or "").strip()
    if client_secret:
        form_payload["client_secret"] = client_secret
    if str(auth_context.get("mode", "") or "").strip() == "installed" and code_verifier:
        form_payload["code_verifier"] = code_verifier
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data=form_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if resp.status_code != 200:
        raise RuntimeError(_format_google_oauth_failure("token exchange", resp.status_code, resp.text, oauth_mode))
    payload = resp.json()
    if not isinstance(payload, dict) or not str(payload.get("access_token", "")).strip():
        raise RuntimeError("Google token exchange returned no access token")
    return payload


async def _google_refresh_access_token(refresh_token: str, oauth_mode: str | None = None) -> dict:
    auth_context = _youtube_auth_context(oauth_mode)
    form_payload = {
        "refresh_token": refresh_token,
        "client_id": str(auth_context.get("client_id", "") or "").strip(),
        "grant_type": "refresh_token",
    }
    client_secret = str(auth_context.get("client_secret", "") or "").strip()
    if client_secret:
        form_payload["client_secret"] = client_secret
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data=form_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if resp.status_code != 200:
        raise RuntimeError(_format_google_oauth_failure("token refresh", resp.status_code, resp.text, oauth_mode))
    payload = resp.json()
    if not isinstance(payload, dict) or not str(payload.get("access_token", "")).strip():
        raise RuntimeError("Google token refresh returned no access token")
    return payload


async def _youtube_fetch_channel_search(access_token: str, channel_id: str, order: str = "date", max_results: int = 12) -> list[dict]:
    clean_channel_id = str(channel_id or "").strip()
    if not clean_channel_id:
        return []
    remaining = max(1, min(int(max_results or 12), 250))
    page_token = ""
    video_ids: list[str] = []
    seen_ids: set[str] = set()
    while remaining > 0:
        payload = await _youtube_api_get(
            access_token,
            "/search",
            params={
                "part": "snippet",
                "channelId": clean_channel_id,
                "order": order,
                "maxResults": min(50, remaining),
                "type": "video",
            },
        )
        for raw in list(payload.get("items") or []):
            if not isinstance(raw, dict):
                continue
            vid = str(((raw.get("id") or {}).get("videoId")) or "").strip()
            if not vid or vid in seen_ids:
                continue
            seen_ids.add(vid)
            video_ids.append(vid)
        remaining = int(max_results or 12) - len(video_ids)
        page_token = str(payload.get("nextPageToken", "") or "").strip()
        if not page_token or remaining <= 0:
            break
    return await _youtube_fetch_videos(access_token, video_ids)


async def _youtube_fetch_owned_channel_videos(
    access_token: str,
    channel_id: str,
    *,
    max_results: int = 250,
) -> list[dict]:
    clean_channel_id = str(channel_id or "").strip()
    if not clean_channel_id:
        return []
    remaining = max(1, min(int(max_results or 250), 500))
    page_token = ""
    video_ids: list[str] = []
    seen_ids: set[str] = set()
    while remaining > 0:
        params = {
            "part": "snippet",
            "forMine": "true",
            "type": "video",
            "order": "date",
            "maxResults": min(50, remaining),
        }
        if page_token:
            params["pageToken"] = page_token
        payload = await _youtube_api_get(access_token, "/search", params=params)
        for raw in list(payload.get("items") or []):
            if not isinstance(raw, dict):
                continue
            snippet = dict(raw.get("snippet") or {})
            snippet_channel_id = str(snippet.get("channelId", "") or "").strip()
            if snippet_channel_id and snippet_channel_id != clean_channel_id:
                continue
            vid = str(((raw.get("id") or {}).get("videoId")) or "").strip()
            if not vid or vid in seen_ids:
                continue
            seen_ids.add(vid)
            video_ids.append(vid)
        remaining = int(max_results or 250) - len(video_ids)
        page_token = str(payload.get("nextPageToken", "") or "").strip()
        if not page_token or remaining <= 0:
            break
    return await _youtube_fetch_videos(access_token, video_ids)


async def _youtube_fetch_video_analytics_bulk(
    access_token: str,
    channel_id: str,
    video_ids: list[str],
    *,
    video_meta: dict[str, dict] | None = None,
) -> dict[str, dict]:
    ids = [str(v).strip() for v in list(video_ids or []) if str(v).strip()]
    if not ids:
        return {}
    metrics_map: dict[str, dict] = {}
    meta_map = {str(k or "").strip(): dict(v or {}) for k, v in dict(video_meta or {}).items() if str(k or "").strip()}
    end_date = datetime.now(timezone.utc).date()
    for start in range(0, len(ids), 25):
        chunk = ids[start : start + 25]
        earliest_published = None
        for video_id in chunk:
            published = _youtube_parse_published_date(str((meta_map.get(video_id) or {}).get("published_at", "") or ""))
            if published and (earliest_published is None or published < earliest_published):
                earliest_published = published
        chunk_start_date = earliest_published or (end_date - timedelta(days=365))
        if chunk_start_date > end_date:
            chunk_start_date = end_date - timedelta(days=365)
        for metrics in (
            "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,impressions,impressionClickThroughRate",
            "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
        ):
            try:
                payload = await _youtube_api_get(
                    access_token,
                    "/reports",
                    analytics=True,
                    params={
                        "ids": f"channel=={channel_id}",
                        "startDate": chunk_start_date.isoformat(),
                        "endDate": end_date.isoformat(),
                        "dimensions": "video",
                        "filters": f"video=={','.join(chunk)}",
                        "maxResults": min(25, len(chunk)),
                        "metrics": metrics,
                    },
                )
                headers = [str((col or {}).get("name", "") or "") for col in list(payload.get("columnHeaders") or [])]
                for row in list(payload.get("rows") or []):
                    if not isinstance(row, list) or not row:
                        continue
                    video_id = str(row[0] or "").strip()
                    if not video_id:
                        continue
                    parsed: dict[str, float] = {}
                    for idx, header in enumerate(headers):
                        if idx >= len(row) or not header:
                            continue
                        if header == "video":
                            continue
                        try:
                            parsed[header] = float(row[idx] or 0)
                        except Exception:
                            pass
                    if parsed:
                        metrics_map[video_id] = parsed
                if any(str(v or "").strip() in metrics_map for v in chunk):
                    break
            except Exception:
                continue
    return metrics_map


def _youtube_connected_failure_mode(video: dict) -> tuple[str, str]:
    views = int(float((video or {}).get("views", 0) or 0))
    impressions = int(float((video or {}).get("impressions", 0) or 0))
    ctr = float((video or {}).get("impression_click_through_rate", 0.0) or 0.0)
    avp = float((video or {}).get("average_view_percentage", 0.0) or 0.0)
    if views <= 1 and impressions <= 40:
        return "no_distribution", "No Distribution"
    if impressions >= 50 and ctr > 0 and ctr < 2.2:
        return "packaging_fail", "Packaging Fail"
    if views >= 20 and avp > 0 and avp < 34.0:
        return "retention_fail", "Retention Fail"
    if views >= 10 or impressions >= 50:
        return "mixed", "Mixed Signal"
    return "mixed", "Mixed Signal"


def _youtube_historical_video_score(video: dict) -> float:
    views = float((video or {}).get("views", 0.0) or 0.0)
    likes = float((video or {}).get("likes", 0.0) or 0.0)
    impressions = float((video or {}).get("impressions", 0.0) or 0.0)
    ctr = float((video or {}).get("impression_click_through_rate", 0.0) or 0.0)
    avp = float((video or {}).get("average_view_percentage", 0.0) or 0.0)
    return round((views * 0.08) + (likes * 3.0) + (ctr * 20.0) + (avp * 3.0) + min(impressions * 0.02, 40.0), 2)


def _youtube_order_inventory_rows(rows: list[dict] | None) -> list[dict]:
    ordered_rows = [
        dict(row or {})
        for row in list(rows or [])
        if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
    ]
    if not ordered_rows:
        return []
    if all(str((row or {}).get("published_at", "") or "").strip() for row in ordered_rows):
        ordered_rows.sort(
            key=lambda row: (
                str((row or {}).get("published_at", "") or ""),
                str((row or {}).get("video_id", "") or ""),
            ),
            reverse=True,
        )
    return ordered_rows


def _slugify_file_component(value: str, fallback: str = "item", max_len: int = 64) -> str:
    return _youtube_slugify_file_component(value, fallback=fallback, max_len=max_len)


def _youtube_watch_url(video_id: str) -> str:
    clean = str(video_id or "").strip()
    return f"https://www.youtube.com/watch?v={clean}" if clean else ""


def _pick_catalyst_reference_video(channel_context: dict | None, requested_video_id: str = "") -> dict:
    context = dict(channel_context or {})
    requested = str(requested_video_id or "").strip()
    uploaded_videos = [
        dict(v or {})
        for v in list(context.get("uploaded_videos") or [])
        if isinstance(v, dict) and str((v or {}).get("privacy_status", "") or "").strip().lower() in {"", "public"}
    ]
    if not uploaded_videos:
        uploaded_videos = [dict(v or {}) for v in list(context.get("uploaded_videos") or []) if isinstance(v, dict)]
    measured_recent = _youtube_order_inventory_rows(uploaded_videos)
    measured_best = sorted(
        uploaded_videos,
        key=lambda row: (
            -int(float(row.get("views", 0) or 0)),
            -float(row.get("average_view_percentage", 0.0) or 0.0),
            -int(float(row.get("likes", 0) or 0)),
            str(row.get("published_at", "") or ""),
        ),
    )
    if measured_best:
        if requested:
            match = next(
                (
                    dict(row)
                    for row in measured_best
                    if str(row.get("video_id", "") or "").strip() == requested
                ),
                None,
            )
            if match:
                return match
        return dict(measured_best[0] or {})
    candidate_rows = [
        *measured_best[:25],
        *measured_recent[:25],
        *[dict(v or {}) for v in list(context.get("top_videos") or []) if isinstance(v, dict)],
        dict((context.get("historical_compare") or {}).get("best_recent_video") or {}),
        dict((context.get("historical_compare") or {}).get("latest_video") or {}),
        dict((context.get("historical_compare") or {}).get("previous_video") or {}),
    ]
    deduped: list[dict] = []
    seen_ids: set[str] = set()
    for row in candidate_rows:
        video_id = str(row.get("video_id", "") or "").strip()
        if not video_id or video_id in seen_ids:
            continue
        seen_ids.add(video_id)
        deduped.append(row)
    if requested:
        match = next((dict(row) for row in deduped if str(row.get("video_id", "") or "").strip() == requested), None)
        if match:
            return match
    return dict(deduped[0] or {}) if deduped else {}


def _manual_catalyst_reference_video_id(source_url: str = "", title: str = "", filename: str = "") -> str:
    source_id = str(_source_url_video_id(source_url) or "").strip()
    if source_id:
        return source_id
    label = _slugify_file_component(title or filename or "manual-reference", fallback="manual-reference", max_len=24)
    seed = "|".join(
        part
        for part in [
            str(source_url or "").strip(),
            str(title or "").strip(),
            str(filename or "").strip(),
        ]
        if part
    ).strip()
    if not seed:
        return label
    digest = hashlib.sha1(seed.encode("utf-8", "ignore")).hexdigest()[:12]
    return f"{label}-{digest}"


def _reference_video_analysis_dir(user_id: str, channel_id: str, workspace_id: str, video_id: str) -> Path:
    root = TEMP_DIR / "catalyst_reference_video"
    bucket = (
        _slugify_file_component(user_id or "owner", fallback="owner", max_len=48),
        _slugify_file_component(channel_id or "channel", fallback="channel", max_len=48),
        _slugify_file_component(workspace_id or "documentary", fallback="documentary", max_len=32),
        _slugify_file_component(video_id or "video", fallback="video", max_len=32),
    )
    path = root.joinpath(*bucket)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _reference_preview_frame_family_key(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        stem = str(Path(parsed.path).stem or "").strip().lower()
    except Exception:
        stem = ""
    if not stem:
        return raw.lower()
    normalized = re.sub(r"^(mq|hq|sd|maxres)", "", stem)
    normalized = normalized or stem
    if normalized == "default":
        return "0"
    return normalized


def _reference_preview_frame_rank(url: str) -> tuple[int, int]:
    lower = str(url or "").strip().lower()
    if not lower:
        return (99, 99)
    checks = [
        (0, ["/3.", "/mq3.", "/hq3.", "/sd3.", "/maxres3."]),
        (1, ["/2.", "/mq2.", "/hq2.", "/sd2.", "/maxres2."]),
        (2, ["/1.", "/mq1.", "/hq1.", "/sd1.", "/maxres1."]),
        (3, ["/0.", "/default.", "/mqdefault.", "/hqdefault.", "/sddefault.", "/maxresdefault."]),
    ]
    for bucket, patterns in checks:
        if any(pattern in lower for pattern in patterns):
            quality_bonus = 0
            if "/maxres" in lower:
                quality_bonus = -3
            elif "/sd" in lower:
                quality_bonus = -2
            elif "/hq" in lower:
                quality_bonus = -1
            return (bucket, quality_bonus)
    return (8, 0)


def _pick_reference_preview_frame_urls(
    source_bundle: dict | None,
    info: dict | None,
    *,
    max_items: int = 6,
) -> list[str]:
    source_bundle = dict(source_bundle or {})
    info = dict(info or {})
    candidates: list[dict] = []
    for thumb in list(info.get("thumbnails") or []):
        if not isinstance(thumb, dict):
            continue
        url = str(thumb.get("url", "") or "").strip()
        if not url:
            continue
        candidates.append(
            {
                "url": url,
                "family": _reference_preview_frame_family_key(url),
                "rank": _reference_preview_frame_rank(url),
            }
        )
    thumb_url = str(source_bundle.get("thumbnail_url", "") or "").strip()
    if thumb_url:
        candidates.append(
            {
                "url": thumb_url,
                "family": _reference_preview_frame_family_key(thumb_url),
                "rank": _reference_preview_frame_rank(thumb_url),
            }
        )
    ranked = sorted(candidates, key=lambda item: (item.get("rank") or (99, 99), len(str(item.get("url", "") or ""))))
    selected: list[str] = []
    seen_families: set[str] = set()
    seen_urls: set[str] = set()
    for item in ranked:
        url = str(item.get("url", "") or "").strip()
        if not url or url in seen_urls:
            continue
        family = str(item.get("family", "") or "").strip()
        if family and family in seen_families:
            continue
        selected.append(url)
        seen_urls.add(url)
        if family:
            seen_families.add(family)
        if len(selected) >= max(3, int(max_items or 6)):
            break
    return selected


def _youtube_historical_compare_public_view(video: dict | None) -> dict:
    payload = dict(video or {})
    failure_mode_key, failure_mode_label = _youtube_connected_failure_mode(payload)
    return {
        "video_id": str(payload.get("video_id", "") or "").strip(),
        "title": str(payload.get("title", "") or "").strip(),
        "published_at": str(payload.get("published_at", "") or "").strip(),
        "thumbnail_url": str(payload.get("thumbnail_url", "") or "").strip(),
        "views": int(float(payload.get("views", 0) or 0)),
        "likes": int(float(payload.get("likes", 0) or 0)),
        "comments": int(float(payload.get("comments", 0) or 0)),
        "impressions": int(float(payload.get("impressions", 0) or 0)),
        "impression_click_through_rate": round(float(payload.get("impression_click_through_rate", 0.0) or 0.0), 2),
        "average_view_duration_sec": int(float(payload.get("average_view_duration_sec", 0) or 0)),
        "average_view_percentage": round(float(payload.get("average_view_percentage", 0.0) or 0.0), 2),
        "duration_sec": int(float(payload.get("duration_sec", 0) or 0)),
        "series_anchor": str(payload.get("series_anchor", "") or "").strip(),
        "niche_key": str(payload.get("niche_key", "") or "").strip(),
        "niche_label": str(payload.get("niche_label", "") or "").strip(),
        "archetype_key": str(payload.get("archetype_key", "") or "").strip(),
        "archetype_label": str(payload.get("archetype_label", "") or "").strip(),
        "score": _youtube_historical_video_score(payload),
        "failure_mode_key": failure_mode_key,
        "failure_mode_label": failure_mode_label,
    }


def _youtube_build_historical_compare(videos: list[dict]) -> dict:
    rows = _youtube_order_inventory_rows(videos)
    if not rows:
        return {}
    latest = dict(rows[0] or {})
    previous = dict(rows[1] or {}) if len(rows) > 1 else {}
    ranked = sorted(rows, key=_youtube_historical_video_score, reverse=True)
    best = dict(ranked[0] or {})
    worst = dict(ranked[-1] or {})
    latest_view = _youtube_historical_compare_public_view(latest)
    previous_view = _youtube_historical_compare_public_view(previous) if previous else {}
    best_view = _youtube_historical_compare_public_view(best)
    worst_view = _youtube_historical_compare_public_view(worst)
    winner_title = str(best_view.get("title", "") or "").strip()
    loser_title = str(worst_view.get("title", "") or "").strip()
    winner_len = len(winner_title)
    loser_len = len(loser_title)
    winner_words = len([word for word in re.split(r"\s+", winner_title) if word.strip()])
    loser_words = len([word for word in re.split(r"\s+", loser_title) if word.strip()])
    winner_series = str(best_view.get("series_anchor", "") or "").strip()
    loser_series = str(worst_view.get("series_anchor", "") or "").strip()
    winner_mode = str(best_view.get("failure_mode_label", "") or "").strip()
    loser_mode = str(worst_view.get("failure_mode_label", "") or "").strip()
    measured_parts: list[str] = []
    inference_parts: list[str] = []
    limitations: list[str] = []
    if latest_view and previous_view:
        measured_parts.append(
            f"Latest upload '{latest_view.get('title', '')}' currently has {int(latest_view.get('views', 0) or 0)} views"
            + (f", {int(latest_view.get('impressions', 0) or 0)} impressions, and {float(latest_view.get('impression_click_through_rate', 0.0) or 0.0):.2f}% CTR" if float(latest_view.get('impression_click_through_rate', 0.0) or 0.0) > 0 or int(latest_view.get('impressions', 0) or 0) > 0 else "")
            + f"; previous upload '{previous_view.get('title', '')}' has {int(previous_view.get('views', 0) or 0)} views."
        )
    elif latest_view:
        measured_parts.append(
            f"Latest upload '{latest_view.get('title', '')}' currently has {int(latest_view.get('views', 0) or 0)} views"
            + (f", {int(latest_view.get('impressions', 0) or 0)} impressions, and {float(latest_view.get('impression_click_through_rate', 0.0) or 0.0):.2f}% CTR." if float(latest_view.get('impression_click_through_rate', 0.0) or 0.0) > 0 or int(latest_view.get('impressions', 0) or 0) > 0 else ".")
        )
    if winner_title and loser_title:
        inference_parts.append(
            f"Catalyst currently ranks '{winner_title}' as the strongest recent package signal ({winner_mode}) and '{loser_title}' as the weakest ({loser_mode})."
        )
    learnings: list[str] = []
    if winner_len and loser_len and winner_len + 8 < loser_len:
        learnings.append("Winning uploads skew shorter and easier to process at a glance; avoid long premise chains.")
    if winner_words and loser_words and winner_words + 3 < loser_words:
        learnings.append("Winning uploads use fewer words before the payoff becomes obvious.")
    if "," in loser_title or " then " in loser_title.lower():
        learnings.append("Weak uploads often read like recap summaries with chained events instead of one clean click promise.")
    if winner_series and loser_series and winner_series.lower() != loser_series.lower():
        learnings.append(f"The stronger upload stayed in a clearer emotional arena ({winner_series}) while the weaker one leaned harder on lore-heavy framing ({loser_series}).")
    next_moves: list[str] = []
    if str(latest_view.get("failure_mode_key", "") or "") == "no_distribution":
        next_moves.append("Treat the latest underperformer as a distribution/package miss first: fix title clarity and thumbnail promise before blaming pacing.")
    if str(worst_view.get("failure_mode_key", "") or "") == "packaging_fail":
        next_moves.append("Push a cleaner curiosity gap with one conflict, one victim, and one payoff instead of stacked lore descriptors.")
    next_moves.append("Favor human conflict or betrayal framing over abstract lore stacking when titling recap uploads.")
    if int(latest_view.get("impressions", 0) or 0) <= 0 and float(latest_view.get("impression_click_through_rate", 0.0) or 0.0) <= 0:
        limitations.append("Recent impression and CTR data are missing or zero, so packaging judgments are less certain.")
    if len(rows) < 5:
        limitations.append("Only a small recent sample is available, so strongest/weakest comparisons are based on limited history.")
    honesty_note = (
        "Views, impressions, CTR, average viewed, and upload counts are measured YouTube metrics when available. "
        "Best/worst package labels and failure-mode labels are Catalyst classifications from those metrics, not direct YouTube labels."
    )
    return {
        "latest_video": latest_view,
        "previous_video": previous_view,
        "best_recent_video": best_view,
        "worst_recent_video": worst_view,
        "winner_vs_loser_summary": " ".join(part for part in [*measured_parts, *inference_parts] if part).strip(),
        "measured_summary": " ".join(part for part in measured_parts if part).strip(),
        "inference_summary": " ".join(part for part in inference_parts if part).strip(),
        "honesty_note": honesty_note,
        "limitations": _youtube_dedupe_clip_list(limitations, max_items=4),
        "winner_patterns": _youtube_dedupe_clip_list(learnings, max_items=4),
        "next_moves": _youtube_dedupe_clip_list(next_moves, max_items=4),
    }


def _youtube_apply_public_inventory_to_snapshot(snapshot: dict | None, public_rows: list[dict] | None) -> dict:
    base_snapshot = dict(snapshot or {})
    public_inventory_rows = [
        dict(row or {})
        for row in list(public_rows or [])
        if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
    ]
    if not public_inventory_rows:
        return base_snapshot

    existing_rows = [
        dict(row or {})
        for row in list(base_snapshot.get("uploaded_videos") or [])
        if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
    ]
    existing_by_id = {
        str((row or {}).get("video_id", "") or "").strip(): dict(row or {})
        for row in list(existing_rows or [])
        if isinstance(row, dict)
    }

    merged_rows: list[dict] = []
    for row in list(public_inventory_rows or []):
        clean_video_id = str((row or {}).get("video_id", "") or "").strip()
        if not clean_video_id:
            continue
        merged = dict(existing_by_id.get(clean_video_id) or {})
        for key, value in dict(row or {}).items():
            if value not in (None, "", [], {}):
                merged[key] = value
        merged["video_id"] = clean_video_id
        merged["privacy_status"] = str(
            (row or {}).get("privacy_status", "") or merged.get("privacy_status", "") or "public"
        ).strip() or "public"
        merged_rows.append(merged)

    merged_rows = _youtube_order_inventory_rows(merged_rows)

    updated_snapshot = dict(base_snapshot)
    updated_snapshot["uploaded_videos"] = merged_rows[:250]
    updated_snapshot["channel_video_count"] = len(merged_rows)
    updated_snapshot["recent_upload_titles"] = [
        str((row or {}).get("title", "") or "").strip()
        for row in list(merged_rows[:12])
        if str((row or {}).get("title", "") or "").strip()
    ]
    merged_top_rows = sorted(
        [dict(row or {}) for row in list(merged_rows or []) if isinstance(row, dict)],
        key=lambda row: (
            -int(float((row or {}).get("views", 0) or 0) or 0),
            str((row or {}).get("published_at", "") or ""),
        ),
    )
    updated_snapshot["top_video_titles"] = [
        str((row or {}).get("title", "") or "").strip()
        for row in list(merged_top_rows[:12])
        if str((row or {}).get("title", "") or "").strip()
    ]
    updated_snapshot["top_videos"] = merged_top_rows[:10]
    updated_snapshot["historical_compare"] = _youtube_build_historical_compare(merged_rows[:20])
    updated_snapshot["channel_audit"] = _youtube_build_channel_audit(updated_snapshot)
    return updated_snapshot


def _youtube_build_channel_audit(snapshot: dict | None) -> dict:
    payload = dict(snapshot or {})
    historical_compare = dict(payload.get("historical_compare") or {})
    recent_titles = [str(v).strip() for v in list(payload.get("recent_upload_titles") or []) if str(v).strip()]
    top_titles = [str(v).strip() for v in list(payload.get("top_video_titles") or []) if str(v).strip()]
    packaging_learnings = [str(v).strip() for v in list(payload.get("packaging_learnings") or []) if str(v).strip()]
    retention_learnings = [str(v).strip() for v in list(payload.get("retention_learnings") or []) if str(v).strip()]
    title_pattern_hints = [str(v).strip() for v in list(payload.get("title_pattern_hints") or []) if str(v).strip()]
    series_clusters = [dict(v or {}) for v in list(payload.get("series_clusters") or []) if isinstance(v, dict)]
    top_videos = [dict(v or {}) for v in list(payload.get("top_videos") or []) if isinstance(v, dict)]
    latest_video = dict(historical_compare.get("latest_video") or {})
    previous_video = dict(historical_compare.get("previous_video") or {})
    best_recent_video = dict(historical_compare.get("best_recent_video") or {})
    worst_recent_video = dict(historical_compare.get("worst_recent_video") or {})
    latest_failure_mode = str(latest_video.get("failure_mode_label", "") or "").strip()
    strongest_arc = str((series_clusters[0] or {}).get("label", "") or "").strip() if series_clusters else ""
    weakest_arc = str((series_clusters[-1] or {}).get("label", "") or "").strip() if len(series_clusters) > 1 else ""
    strengths = _youtube_dedupe_clip_list(
        [
            *title_pattern_hints[:2],
            *packaging_learnings[:2],
            *retention_learnings[:2],
            (f"Best recent title: {best_recent_video.get('title', '')}" if best_recent_video.get("title") else ""),
            (f"Strongest active arc: {strongest_arc}" if strongest_arc else ""),
        ],
        max_items=6,
    )
    warnings = _youtube_dedupe_clip_list(
        [
            (f"Latest upload is currently classified as {latest_failure_mode}." if latest_failure_mode else ""),
            (f"Weakest recent title/package: {worst_recent_video.get('title', '')}" if worst_recent_video.get("title") else ""),
            (f"Weakest active arc: {weakest_arc}" if weakest_arc else ""),
            *[item for item in retention_learnings if "weak" in item.lower() or "watchout" in item.lower()][:2],
            *[item for item in packaging_learnings if "historical next moves" in item.lower()][:1],
        ],
        max_items=6,
    )
    measured_facts = _youtube_dedupe_clip_list(
        [
            str(historical_compare.get("measured_summary", "") or "").strip(),
            (f"Average percentage viewed is about {float(payload.get('average_view_percentage', 0.0) or 0.0):.2f}%." if float(payload.get("average_view_percentage", 0.0) or 0.0) > 0 else ""),
            (f"Average view duration is about {int(float(payload.get('average_view_duration_sec', 0) or 0))} seconds." if int(float(payload.get("average_view_duration_sec", 0) or 0)) > 0 else ""),
            f"Audited {len(recent_titles)} recent uploads and {len(top_videos)} top videos.",
        ],
        max_items=6,
    )
    inferred_notes = _youtube_dedupe_clip_list(
        [
            (f"Catalyst currently identifies '{strongest_arc}' as the strongest active arc." if strongest_arc else ""),
            (f"Catalyst currently identifies '{weakest_arc}' as the weakest active arc." if weakest_arc else ""),
            str(historical_compare.get("inference_summary", "") or "").strip(),
            *strengths[:3],
            *warnings[:2],
        ],
        max_items=8,
    )
    limitations = _youtube_dedupe_clip_list(
        [
            *list(historical_compare.get("limitations") or []),
            ("No active arc clustering is available yet, so series judgments are weaker." if not series_clusters else ""),
            ("CTR is missing or zero for the sampled uploads, so package labels are less certain." if not float(payload.get("average_ctr", 0.0) or 0.0) else ""),
        ],
        max_items=6,
    )
    honesty_note = (
        "Measured channel facts are shown separately below. Arc labels, strongest/weakest package calls, failure modes, and suggested next videos are Catalyst inferences."
    )
    next_moves = _youtube_dedupe_clip_list(
        [
            *list(historical_compare.get("next_moves") or []),
            *packaging_learnings[:2],
            *retention_learnings[:2],
        ],
        max_items=6,
    )
    best_title = str(best_recent_video.get("title", "") or "").strip()
    focus_seed = best_title or (top_titles[0] if top_titles else "") or (recent_titles[0] if recent_titles else "")
    focus_subject = _youtube_same_arena_subject({"title": focus_seed}, topic=strongest_arc or focus_seed)
    next_video_candidates = _youtube_same_arena_title_variants(
        {"title": focus_seed},
        topic=strongest_arc or focus_subject,
        format_preset="documentary",
        max_items=5,
    ) if focus_seed else []
    coverage = {
        "recent_uploads": len(recent_titles),
        "top_videos": len(top_videos),
        "series_clusters": len(series_clusters),
    }
    summary_parts = [
        f"Catalyst inference summary: audited {coverage['recent_uploads']} recent uploads and {coverage['top_videos']} top videos.",
        (f"Strongest arc: {strongest_arc}." if strongest_arc else ""),
        (f"Weakest arc: {weakest_arc}." if weakest_arc else ""),
        str(historical_compare.get("inference_summary", "") or "").strip(),
        (f"Latest failure mode: {latest_failure_mode}." if latest_failure_mode else ""),
    ]
    if latest_video and previous_video:
        latest_views = int(latest_video.get("views", 0) or 0)
        previous_views = int(previous_video.get("views", 0) or 0)
        summary_parts.append(
            f"Latest vs previous: {latest_views:,} vs {previous_views:,} views."
        )
    return {
        "summary": _clip_text(" ".join(part for part in summary_parts if part).strip(), 480),
        "honesty_note": honesty_note,
        "measured_facts": measured_facts,
        "inferred_notes": inferred_notes,
        "limitations": limitations,
        "strengths": strengths,
        "warnings": warnings,
        "next_moves": next_moves,
        "strongest_arc": strongest_arc,
        "weakest_arc": weakest_arc,
        "latest_failure_mode_label": latest_failure_mode,
        "best_recent_title": str(best_recent_video.get("title", "") or "").strip(),
        "worst_recent_title": str(worst_recent_video.get("title", "") or "").strip(),
        "coverage": coverage,
        "focus_subject": focus_subject,
        "next_video_candidates": next_video_candidates,
    }


def _youtube_historical_compare_measured_public_view(payload: dict | None) -> dict:
    raw = dict(payload or {})
    if not raw:
        return {}
    return {
        "latest_video": _youtube_historical_compare_public_view(raw.get("latest_video") or {}),
        "previous_video": _youtube_historical_compare_public_view(raw.get("previous_video") or {}),
        "measured_summary": str(raw.get("measured_summary", "") or "").strip(),
        "honesty_note": (
            "This hub view only shows measured YouTube metrics and explicit data limitations. "
            "Catalyst classifications are kept internal and are not shown here."
        ),
        "limitations": _youtube_dedupe_clip_list(
            [str(v).strip() for v in list(raw.get("limitations") or []) if str(v).strip()],
            max_items=6,
        ),
    }


def _youtube_channel_audit_measured_public_view(payload: dict | None) -> dict:
    raw = dict(payload or {})
    if not raw:
        return {}
    coverage = dict(raw.get("coverage") or {})
    return {
        "honesty_note": (
            "This hub view only shows measured channel data, reference evidence, and explicit limitations. "
            "Catalyst strategy labels and recommendations are kept internal."
        ),
        "measured_facts": _youtube_dedupe_clip_list(
            [str(v).strip() for v in list(raw.get("measured_facts") or []) if str(v).strip()],
            max_items=8,
        ),
        "limitations": _youtube_dedupe_clip_list(
            [str(v).strip() for v in list(raw.get("limitations") or []) if str(v).strip()],
            max_items=6,
        ),
        "coverage": {
            "recent_uploads": int(coverage.get("recent_uploads", 0) or 0),
            "top_videos": int(coverage.get("top_videos", 0) or 0),
            "series_clusters": int(coverage.get("series_clusters", 0) or 0),
        },
    }


configure_youtube_public_view_hooks(
    historical_compare_public_view=_youtube_historical_compare_measured_public_view,
    channel_audit_public_view=_youtube_channel_audit_measured_public_view,
)


def _youtube_connection_public_view(record: dict) -> dict:
    data = dict(record or {})
    analytics_snapshot = dict(data.get("analytics_snapshot") or {})
    return {
        "channel_id": str(data.get("channel_id", "") or ""),
        "title": str(data.get("title", "") or ""),
        "custom_url": str(data.get("custom_url", "") or ""),
        "thumbnail_url": str(data.get("thumbnail_url", "") or ""),
        "channel_handle": str(data.get("channel_handle", "") or ""),
        "channel_url": str(data.get("channel_url", "") or ""),
        "subscriber_count": int(data.get("subscriber_count", 0) or 0),
        "video_count": int(data.get("video_count", 0) or 0),
        "view_count": int(data.get("view_count", 0) or 0),
        "linked_at": float(data.get("linked_at", 0.0) or 0.0),
        "last_synced_at": float(data.get("last_synced_at", 0.0) or 0.0),
        "last_outcome_sync_at": float(data.get("last_outcome_sync_at", 0.0) or 0.0),
        "last_outcome_sync_count": int(data.get("last_outcome_sync_count", 0) or 0),
        "last_outcome_sync_error": str(data.get("last_outcome_sync_error", "") or ""),
        "last_sync_error": str(data.get("last_sync_error", "") or ""),
        "token_expires_at": float(data.get("token_expires_at", 0.0) or 0.0),
        "oauth_mode": str(data.get("oauth_mode", "") or ""),
        "oauth_source": str(data.get("oauth_source", "") or ""),
        "oauth_client_kind": str(data.get("oauth_client_kind", "") or ""),
        "analytics_snapshot": {
            "channel_video_count": int(analytics_snapshot.get("channel_video_count", 0) or 0),
            "recent_upload_titles": list(analytics_snapshot.get("recent_upload_titles") or []),
            "uploaded_videos": [
                {
                    "video_id": str((row or {}).get("video_id", "") or "").strip(),
                    "title": _clip_text(str((row or {}).get("title", "") or "").strip(), 180),
                    "published_at": str((row or {}).get("published_at", "") or "").strip(),
                    "thumbnail_url": str((row or {}).get("thumbnail_url", "") or "").strip(),
                    "views": int(float((row or {}).get("views", 0) or 0) or 0),
                    "average_view_percentage": round(float((row or {}).get("average_view_percentage", 0.0) or 0.0), 2),
                    "impression_click_through_rate": round(float((row or {}).get("impression_click_through_rate", 0.0) or 0.0), 2),
                    "duration_sec": int(float((row or {}).get("duration_sec", 0) or 0) or 0),
                    "privacy_status": str((row or {}).get("privacy_status", "") or "").strip(),
                }
                for row in list(analytics_snapshot.get("uploaded_videos") or [])[:250]
                if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
            ],
            "top_video_titles": list(analytics_snapshot.get("top_video_titles") or []),
            "historical_compare": _youtube_historical_compare_public_view(
                analytics_snapshot.get("historical_compare") or {}
            ),
            "channel_audit": _youtube_channel_audit_public_view(
                analytics_snapshot.get("channel_audit") or {}
            ),
        },
    }


def _append_youtube_signal_log(entry: dict) -> None:
    try:
        YOUTUBE_SIGNAL_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with YOUTUBE_SIGNAL_LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _google_oauth_error_suggests_stale_client(message: str) -> bool:
    lower = str(message or "").strip().lower()
    if not lower:
        return False
    return any(
        needle in lower
        for needle in (
            "oauth client is disabled or deleted",
            "oauth client belongs to a suspended",
            "disabled_client",
            "deleted_client",
            "consumer project",
            "project_number",
            "suspended google cloud project",
        )
    )


def _google_oauth_error_suggests_reconnect_required(message: str) -> bool:
    lower = str(message or "").strip().lower()
    if not lower:
        return False
    return any(
        needle in lower
        for needle in (
            "invalid_grant",
            "token has been expired or revoked",
            "token has been revoked",
            "unauthorized_client",
            "bad request",
        )
    )


def _youtube_mode_is_configured(mode: str | None) -> bool:
    normalized = str(mode or "").strip().lower()
    return normalized in {"web", "installed"} and _youtube_active_oauth_mode(normalized) == normalized


async def _youtube_ensure_access_token(record: dict) -> tuple[str, dict]:
    updated = dict(record or {})
    access_token = str(updated.get("access_token", "") or "").strip()
    refresh_token = str(updated.get("refresh_token", "") or "").strip()
    expires_at = float(updated.get("token_expires_at", 0.0) or 0.0)
    now = time.time()
    if access_token and expires_at > now + YOUTUBE_TOKEN_REFRESH_MARGIN_SEC:
        return access_token, updated
    if not refresh_token:
        raise RuntimeError("Missing Google refresh token for connected YouTube channel")
    stored_oauth_mode = str(updated.get("oauth_mode", "") or "").strip().lower()
    active_oauth_mode = _youtube_active_oauth_mode()
    oauth_mode = (
        stored_oauth_mode
        if stored_oauth_mode in {"web", "installed"} and _youtube_mode_is_configured(stored_oauth_mode)
        else str(active_oauth_mode or "").strip().lower()
    )
    if oauth_mode not in {"web", "installed"}:
        oauth_mode = "web"
    modes_to_try: list[str] = []
    for candidate in [oauth_mode, str(active_oauth_mode or "").strip().lower()]:
        if candidate in {"web", "installed"} and _youtube_mode_is_configured(candidate) and candidate not in modes_to_try:
            modes_to_try.append(candidate)
    refreshed: dict | None = None
    last_refresh_error: Exception | None = None
    for candidate_mode in modes_to_try or [oauth_mode]:
        try:
            refreshed = await _google_refresh_access_token(refresh_token, candidate_mode)
            oauth_mode = candidate_mode
            break
        except Exception as e:
            last_refresh_error = e
            if (
                candidate_mode != str(active_oauth_mode or "").strip().lower()
                and str(active_oauth_mode or "").strip().lower() in {"web", "installed"}
                and _google_oauth_error_suggests_stale_client(str(e))
            ):
                continue
            if (
                stored_oauth_mode == "web"
                and candidate_mode == "installed"
                and _google_oauth_error_suggests_reconnect_required(str(e))
            ):
                raise RuntimeError(
                    "This connected YouTube channel is still carrying a refresh token from the old backend web OAuth client. "
                    "Reconnect the channel under the installed OAuth flow so private YouTube metrics can refresh again."
                )
            raise
    if not isinstance(refreshed, dict):
        raise last_refresh_error or RuntimeError("Google token refresh returned no access token")
    updated["access_token"] = str(refreshed.get("access_token", "") or "").strip()
    updated["token_expires_at"] = now + max(300, int(refreshed.get("expires_in", 3600) or 3600))
    if str(refreshed.get("refresh_token", "") or "").strip():
        updated["refresh_token"] = str(refreshed.get("refresh_token", "") or "").strip()
    updated["token_scope"] = str(refreshed.get("scope", updated.get("token_scope", "")) or "").strip()
    updated["oauth_mode"] = oauth_mode
    updated["last_synced_at"] = now
    return str(updated.get("access_token", "") or "").strip(), updated


async def _youtube_fetch_channel_analytics(access_token: str, channel_id: str) -> dict:
    channels = await _youtube_fetch_my_channels(access_token)
    channel_meta = next((dict(row) for row in channels if str(row.get("channel_id", "") or "").strip() == str(channel_id or "").strip()), {})
    uploads_playlist_id = str(channel_meta.get("uploads_playlist_id", "") or "").strip()
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=90)
    summary_payload = {}
    try:
        summary_payload = await _youtube_api_get(
            access_token,
            "/reports",
            analytics=True,
            params={
                "ids": f"channel=={channel_id}",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,impressions,impressionClickThroughRate",
            },
        )
    except Exception:
        summary_payload = await _youtube_api_get(
            access_token,
            "/reports",
            analytics=True,
            params={
                "ids": f"channel=={channel_id}",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost",
            },
        )

    summary_rows = list(summary_payload.get("rows") or [])
    summary_map: dict[str, float] = {}
    if summary_rows:
        first_row = list(summary_rows[0] or [])
        header_names = [str((col or {}).get("name", "") or "") for col in list(summary_payload.get("columnHeaders") or [])]
        for idx, header in enumerate(header_names):
            if idx < len(first_row):
                try:
                    summary_map[header] = float(first_row[idx] or 0)
                except Exception:
                    pass

    top_videos_payload = {}
    top_rows: list = []
    top_header_names: list[str] = []
    for metrics in (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,impressions,impressionClickThroughRate",
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
    ):
        try:
            top_videos_payload = await _youtube_api_get(
                access_token,
                "/reports",
                analytics=True,
                params={
                    "ids": f"channel=={channel_id}",
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "dimensions": "video",
                    "sort": "-views",
                    "maxResults": 10,
                    "metrics": metrics,
                },
            )
            top_rows = list(top_videos_payload.get("rows") or [])
            top_header_names = [str((col or {}).get("name", "") or "") for col in list(top_videos_payload.get("columnHeaders") or [])]
            break
        except Exception:
            top_rows = []
            top_header_names = []
            continue

    top_video_ids = [str(row[0] or "").strip() for row in top_rows if isinstance(row, list) and row]
    top_video_meta = {str(v.get("video_id", "")): v for v in await _youtube_fetch_videos(access_token, top_video_ids)}
    analytics_top_videos: list[dict] = []
    for row in top_rows:
        if not isinstance(row, list) or not row:
            continue
        video_id = str(row[0] or "").strip()
        metrics_map: dict[str, float] = {}
        for idx, header in enumerate(top_header_names):
            if idx >= len(row):
                continue
            try:
                metrics_map[header] = float(row[idx] or 0)
            except Exception:
                pass
        meta = dict(top_video_meta.get(video_id) or {})
        analytics_top_videos.append(
            {
                "video_id": video_id,
                "title": str(meta.get("title", "") or "").strip(),
                "published_at": str(meta.get("published_at", "") or "").strip(),
                "thumbnail_url": str(meta.get("thumbnail_url", "") or "").strip(),
                "views": int(metrics_map.get("views", meta.get("views", 0) or 0) or 0),
                "average_view_duration_sec": int(metrics_map.get("averageViewDuration", 0) or 0),
                "average_view_percentage": round(float(metrics_map.get("averageViewPercentage", 0.0) or 0.0), 2),
                "impressions": int(metrics_map.get("impressions", 0) or 0),
                "impression_click_through_rate": round(float(metrics_map.get("impressionClickThroughRate", 0.0) or 0.0), 2),
            }
        )

    upload_inventory_target = max(int(float(channel_meta.get("video_count", 0) or 0)), 50)
    upload_inventory_target = min(upload_inventory_target, 500)
    public_page_rows: list[dict] = await _youtube_fetch_public_channel_page_videos(
        access_token,
        channel_url=str(channel_meta.get("channel_url", "") or "").strip(),
        channel_id=channel_id,
        max_results=max(3, min(upload_inventory_target, 100)),
    )
    uploaded_videos = await _youtube_fetch_uploads_playlist_videos(access_token, uploads_playlist_id, max_results=upload_inventory_target)
    owned_channel_videos: list[dict] = []
    try:
        owned_channel_videos = await _youtube_fetch_owned_channel_videos(
            access_token,
            channel_id,
            max_results=upload_inventory_target,
        )
    except Exception:
        owned_channel_videos = []
    public_search_rows: list[dict] = []
    try:
        public_search_rows = await _youtube_fetch_public_channel_search_api_key(
            channel_id,
            order="date",
            max_results=max(3, min(upload_inventory_target, 100)),
        )
    except Exception:
        public_search_rows = []
    if not public_search_rows:
        try:
            public_search_rows = await _youtube_fetch_channel_search(
                access_token,
                channel_id,
                order="date",
                max_results=max(3, min(upload_inventory_target, 100)),
            )
        except Exception:
            public_search_rows = []
    owned_video_by_id = {
        str((row or {}).get("video_id", "") or "").strip(): dict(row or {})
        for row in list(owned_channel_videos or [])
        if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
    }

    def _is_public_studio_video(row: dict) -> bool:
        privacy = str((row or {}).get("privacy_status", "") or "").strip().lower()
        return privacy in {"", "public"}

    uploads_inventory_rows = [dict(row or {}) for row in list(uploaded_videos or []) if isinstance(row, dict)]
    public_uploaded_videos = [
        dict(row or {})
        for row in list(uploads_inventory_rows or [])
        if _is_public_studio_video(row)
    ]

    def _merge_owned_video_details(row: dict) -> dict:
        base = dict(row or {})
        clean_video_id = str(base.get("video_id", "") or "").strip()
        if not clean_video_id:
            return base
        owned_row = dict(owned_video_by_id.get(clean_video_id) or {})
        if not owned_row:
            return base
        merged = dict(base)
        for key, value in owned_row.items():
            if value not in (None, "", [], {}):
                merged[key] = value
        if str(base.get("privacy_status", "") or "").strip():
            merged["privacy_status"] = str(base.get("privacy_status", "") or "").strip()
        return merged

    owned_inventory_rows = [
        dict(row or {})
        for row in list(owned_channel_videos or [])
        if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
    ]
    owned_inventory_rows = _youtube_order_inventory_rows(owned_inventory_rows)
    owned_public_rows = [dict(row or {}) for row in list(owned_inventory_rows or []) if _is_public_studio_video(row)]
    public_page_inventory_rows = [
        dict(row or {})
        for row in list(public_page_rows or [])
        if (
            isinstance(row, dict)
            and str((row or {}).get("video_id", "") or "").strip()
            and _is_public_studio_video(row)
        )
    ]
    public_search_inventory_rows = [
        dict(row or {})
        for row in list(public_search_rows or [])
        if (
            isinstance(row, dict)
            and str((row or {}).get("video_id", "") or "").strip()
            and _is_public_studio_video(row)
        )
    ]

    if public_page_inventory_rows:
        inventory_rows = []
        for row in list(public_page_inventory_rows or []):
            merged = dict(row or {})
            clean_video_id = str(merged.get("video_id", "") or "").strip()
            if clean_video_id and clean_video_id in owned_video_by_id:
                for key, value in dict(owned_video_by_id.get(clean_video_id) or {}).items():
                    if value not in (None, "", [], {}):
                        merged[key] = value
                inventory_rows.append(merged)
            else:
                inventory_rows.append(merged)
    elif public_search_inventory_rows:
        inventory_rows = []
        for row in list(public_search_inventory_rows or []):
            merged = dict(row or {})
            clean_video_id = str(merged.get("video_id", "") or "").strip()
            if clean_video_id and clean_video_id in owned_video_by_id:
                for key, value in dict(owned_video_by_id.get(clean_video_id) or {}).items():
                    if value not in (None, "", [], {}):
                        merged[key] = value
                inventory_rows.append(merged)
            else:
                inventory_rows.append(merged)
    elif owned_inventory_rows:
        inventory_rows = [dict(row or {}) for row in list(owned_public_rows or owned_inventory_rows)]
    else:
        inventory_rows = [_merge_owned_video_details(row) for row in list(public_uploaded_videos or uploads_inventory_rows)]
        if not inventory_rows:
            inventory_rows = [_merge_owned_video_details(row) for row in list(uploads_inventory_rows)]
    recent_uploads = [dict(row or {}) for row in list(inventory_rows or [])[:20] if isinstance(row, dict)]
    popular_uploads = []
    if inventory_rows:
        popular_uploads = sorted(
            [dict(row or {}) for row in list(inventory_rows or []) if isinstance(row, dict)],
            key=lambda row: (
                -int(float(row.get("views", 0) or 0)),
                -int(float(row.get("likes", 0) or 0)),
                str(row.get("published_at", "") or ""),
            ),
        )[:25]
    else:
        recent_uploads = await _youtube_fetch_channel_search(access_token, channel_id, order="date", max_results=12)
        popular_uploads = await _youtube_fetch_channel_search(access_token, channel_id, order="viewCount", max_results=12)
    candidate_video_ids = _dedupe_preserve_order(
        [
            *[str((row or {}).get("video_id", "") or "").strip() for row in list(recent_uploads or [])],
            *[str((row or {}).get("video_id", "") or "").strip() for row in list(popular_uploads or [])],
            *[str((row or {}).get("video_id", "") or "").strip() for row in list(analytics_top_videos or [])],
        ],
        max_items=80,
        max_chars=24,
    )
    bulk_source_rows = {
        str((row or {}).get("video_id", "") or "").strip(): dict(row or {})
        for row in [*list(inventory_rows or []), *list(analytics_top_videos or [])]
        if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
    }
    bulk_video_metrics = await _youtube_fetch_video_analytics_bulk(
        access_token,
        channel_id,
        candidate_video_ids,
        video_meta=bulk_source_rows,
    )
    for bucket in (inventory_rows, recent_uploads, popular_uploads, analytics_top_videos):
        for row in list(bucket or []):
            if not isinstance(row, dict):
                continue
            metrics = dict(bulk_video_metrics.get(str(row.get("video_id", "") or "").strip()) or {})
            if not metrics:
                continue
            row["views"] = int(metrics.get("views", row.get("views", 0) or 0) or 0)
            row["average_view_duration_sec"] = int(metrics.get("averageViewDuration", row.get("average_view_duration_sec", 0) or 0) or 0)
            row["average_view_percentage"] = round(float(metrics.get("averageViewPercentage", row.get("average_view_percentage", 0.0) or 0.0) or 0.0), 2)
            row["impressions"] = int(metrics.get("impressions", row.get("impressions", 0) or 0) or 0)
            row["impression_click_through_rate"] = round(float(metrics.get("impressionClickThroughRate", row.get("impression_click_through_rate", 0.0) or 0.0) or 0.0), 2)
            niche = _youtube_catalyst_infer_niche(str(row.get("title", "") or ""), str(row.get("description", "") or ""), " ".join(list(row.get("tags") or [])))
            row["niche_key"] = str(niche.get("key", "") or "").strip()
            row["niche_label"] = str(niche.get("label", "") or "").strip()
            archetype = _youtube_catalyst_infer_archetype(
                str(row.get("title", "") or ""),
                str(row.get("description", "") or ""),
                " ".join(list(row.get("tags") or [])),
                niche_key=str(row.get("niche_key", "") or "").strip().lower(),
            )
            row["archetype_key"] = str(archetype.get("key", "") or "").strip()
            row["archetype_label"] = str(archetype.get("label", "") or "").strip()
            row["series_anchor"] = _youtube_catalyst_extract_series_anchor(
                str(row.get("title", "") or ""),
                str(row.get("description", "") or ""),
                " ".join(list(row.get("tags") or [])),
                niche_key=str(row.get("niche_key", "") or "").strip().lower(),
            )
    top_videos = [dict(v or {}) for v in list(popular_uploads or recent_uploads or analytics_top_videos) if isinstance(v, dict)]
    popular_titles = [str(v.get("title", "") or "").strip() for v in top_videos if str(v.get("title", "") or "").strip()]
    recent_titles = [str(v.get("title", "") or "").strip() for v in recent_uploads if str(v.get("title", "") or "").strip()]
    series_clusters = _youtube_catalyst_build_channel_series_clusters(
        [*list(recent_uploads or []), *list(popular_uploads or [])],
        top_videos=top_videos,
    )
    series_cluster_playbook = _youtube_build_catalyst_cluster_playbook(series_clusters)
    title_tokens = _youtube_packaging_tokens(" ".join(popular_titles), max_items=18)
    title_pattern_hints: list[str] = []
    if title_tokens:
        title_pattern_hints.append("Winning topics/keywords recently cluster around: " + ", ".join(title_tokens[:8]))
    if top_videos:
        title_pattern_hints.append("Top channel titles avoid exact repetition while staying in one recognizable arena.")
    if series_clusters:
        title_pattern_hints.append(
            "Detected channel series/niche arcs: "
            + ", ".join(
                str((cluster or {}).get("label", "") or "").strip()
                for cluster in list(series_clusters)[:4]
                if str((cluster or {}).get("label", "") or "").strip()
            )
        )

    packaging_learnings: list[str] = []
    retention_learnings: list[str] = []
    ctr = summary_map.get("impressionClickThroughRate", 0.0)
    avp = summary_map.get("averageViewPercentage", 0.0)
    avd = summary_map.get("averageViewDuration", 0.0)
    if ctr:
        packaging_learnings.append(f"Channel CTR over the recent window is about {ctr:.2f}%.")
    if avp:
        retention_learnings.append(f"Average percentage viewed is about {avp:.2f}%.")
    if avd:
        retention_learnings.append(f"Average view duration is about {int(avd)} seconds.")
    if top_videos:
        packaging_learnings.append("Strong packaging tends to revolve around one dominant promise and one obvious focal idea per title.")
    if series_clusters:
        lead_cluster = dict(series_clusters[0] or {})
        lead_label = str(lead_cluster.get("label", "") or "").strip()
        if lead_label:
            packaging_learnings.append(f"One strong active content arc right now is {lead_label}; Catalyst should stay in that arena when building follow-ups.")
    if str(series_cluster_playbook.get("summary", "") or "").strip():
        packaging_learnings.append(str(series_cluster_playbook.get("summary", "") or "").strip())
    winning_patterns = [str(v).strip() for v in list(series_cluster_playbook.get("winning_patterns") or []) if str(v).strip()]
    losing_patterns = [str(v).strip() for v in list(series_cluster_playbook.get("losing_patterns") or []) if str(v).strip()]
    if winning_patterns:
        title_pattern_hints.append("Best-performing arc playbook: " + "; ".join(winning_patterns[:2]))
    if losing_patterns:
        retention_learnings.append("Weak-arc watchouts: " + "; ".join(losing_patterns[:2]))
    historical_compare = _youtube_build_historical_compare(recent_uploads[:20])
    historical_summary = str(historical_compare.get("winner_vs_loser_summary", "") or "").strip()
    if historical_summary:
        packaging_learnings.append(historical_summary)
    historical_patterns = [str(v).strip() for v in list(historical_compare.get("winner_patterns") or []) if str(v).strip()]
    if historical_patterns:
        title_pattern_hints.append("Historical winner pattern: " + "; ".join(historical_patterns[:2]))
    historical_moves = [str(v).strip() for v in list(historical_compare.get("next_moves") or []) if str(v).strip()]
    if historical_moves:
        packaging_learnings.append("Historical next moves: " + "; ".join(historical_moves[:2]))

    summary_parts = [
        f"Connected channel recent views: {int(summary_map.get('views', 0) or 0):,}" if summary_map.get("views") else "",
        f"CTR: {ctr:.2f}%" if ctr else "",
        f"Average viewed: {avp:.2f}%" if avp else "",
        f"Top titles: {', '.join(popular_titles[:3])}" if popular_titles else "",
        f"Active arcs: {', '.join(str((cluster or {}).get('label', '') or '').strip() for cluster in list(series_clusters)[:3] if str((cluster or {}).get('label', '') or '').strip())}" if series_clusters else "",
        _clip_text(str(series_cluster_playbook.get("summary", "") or "").strip(), 220) if str(series_cluster_playbook.get("summary", "") or "").strip() else "",
    ]
    snapshot = {
        "channel_summary": " | ".join(part for part in summary_parts if part),
        "uploads_playlist_id": uploads_playlist_id,
        "channel_video_count": int(len(inventory_rows or recent_uploads or [])),
        "recent_upload_titles": recent_titles[:12],
        "top_video_titles": popular_titles[:12],
        "top_videos": top_videos[:10],
        "uploaded_videos": [dict(v or {}) for v in list(inventory_rows or recent_uploads or [])[:250] if isinstance(v, dict)],
        "packaging_learnings": _youtube_dedupe_clip_list(packaging_learnings, max_items=6),
        "retention_learnings": _youtube_dedupe_clip_list(retention_learnings, max_items=6),
        "title_pattern_hints": _youtube_dedupe_clip_list(title_pattern_hints, max_items=6),
        "series_clusters": list(series_clusters),
        "series_cluster_playbook": series_cluster_playbook,
        "historical_compare": historical_compare,
    }
    snapshot["channel_audit"] = _youtube_build_channel_audit(snapshot)
    return snapshot


async def _youtube_refresh_public_channel_record_without_oauth(record: dict, sync_error: str = "") -> dict:
    updated = dict(record or {})
    channel_id = str(updated.get("channel_id", "") or "").strip()
    existing_snapshot = dict(updated.get("analytics_snapshot") or {})
    upload_inventory_target = max(int(float(updated.get("video_count", 0) or 0) or 0), 25)
    upload_inventory_target = min(upload_inventory_target, 100)
    public_rows: list[dict] = []
    if channel_id:
        try:
            public_rows = await _youtube_fetch_public_channel_page_videos(
                "",
                channel_url=str(updated.get("channel_url", "") or "").strip(),
                channel_id=channel_id,
                max_results=upload_inventory_target,
            )
        except Exception:
            public_rows = []
        if not public_rows:
            try:
                public_rows = await _youtube_fetch_public_channel_search_api_key(
                    channel_id,
                    order="date",
                    max_results=max(3, min(upload_inventory_target, 50)),
                )
            except Exception:
                public_rows = []
    fallback_snapshot = dict(existing_snapshot)
    if public_rows:
        fallback_snapshot = _youtube_apply_public_inventory_to_snapshot(fallback_snapshot, public_rows)
        historical_compare = dict(fallback_snapshot.get("historical_compare") or {})
        private_metrics_limitation = _youtube_private_metrics_limitation(sync_error)
        historical_compare["limitations"] = _youtube_dedupe_clip_list(
            [
                *[str(v).strip() for v in list(historical_compare.get("limitations") or []) if str(v).strip()],
                ("Private impressions and CTR are temporarily unavailable from YouTube, so public views are the strongest available signal." if sync_error else ""),
            ],
            max_items=6,
        )
        fallback_snapshot["historical_compare"] = historical_compare
        channel_audit = dict(fallback_snapshot.get("channel_audit") or _youtube_build_channel_audit(fallback_snapshot))
        channel_audit["measured_facts"] = _youtube_dedupe_clip_list(
            [
                f"Recovered {len(public_rows)} public uploads from YouTube page/API fallback." if public_rows else "",
                *[str(v).strip() for v in list(channel_audit.get("measured_facts") or []) if str(v).strip()],
            ],
            max_items=8,
        )
        channel_audit["limitations"] = _youtube_dedupe_clip_list(
            [
                *[str(v).strip() for v in list(channel_audit.get("limitations") or []) if str(v).strip()],
                private_metrics_limitation,
            ],
            max_items=6,
        )
        fallback_snapshot["channel_audit"] = channel_audit
    updated["analytics_snapshot"] = fallback_snapshot
    updated["last_sync_error"] = _clip_text(sync_error, 220)
    updated["last_synced_at"] = time.time()
    return updated


async def _youtube_sync_channel_record(record: dict) -> dict:
    access_token, updated = await _youtube_ensure_access_token(record)
    channel_id = str(updated.get("channel_id", "") or "").strip()
    existing_snapshot = dict(updated.get("analytics_snapshot") or {})

    try:
        channels = await _youtube_fetch_my_channels(access_token)
    except Exception as e:
        channels = []
        updated["last_sync_error"] = _clip_text(str(e), 220)

    matching = next((row for row in channels if str(row.get("channel_id", "") or "").strip() == channel_id), None)
    if matching:
        updated.update(matching)

    public_page_rows: list[dict] = []
    try:
        public_page_rows = await _youtube_fetch_public_channel_page_videos(
            access_token,
            channel_url=str(updated.get("channel_url", "") or "").strip(),
            channel_id=channel_id,
            max_results=max(int(float(updated.get("video_count", 0) or 0) or 0), 25),
        )
    except Exception:
        public_page_rows = []

    try:
        analytics_snapshot = await _youtube_fetch_channel_analytics(access_token, channel_id)
        analytics_snapshot = _youtube_apply_public_inventory_to_snapshot(analytics_snapshot, public_page_rows)
        updated["analytics_snapshot"] = analytics_snapshot
        updated["last_sync_error"] = ""
    except Exception as e:
        merged_existing_by_id = {
            str((row or {}).get("video_id", "") or "").strip(): dict(row or {})
            for row in list(existing_snapshot.get("uploaded_videos") or [])
            if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
        }
        merged_uploaded_rows: list[dict] = []
        if public_page_rows:
            for row in list(public_page_rows or []):
                clean_video_id = str((row or {}).get("video_id", "") or "").strip()
                merged = dict(merged_existing_by_id.get(clean_video_id) or {})
                merged.update(dict(row or {}))
                merged_uploaded_rows.append(merged)
        else:
            merged_uploaded_rows = [dict(row or {}) for row in list(existing_snapshot.get("uploaded_videos") or []) if isinstance(row, dict)]

        merged_uploaded_rows = _youtube_order_inventory_rows(merged_uploaded_rows)
        fallback_snapshot = dict(existing_snapshot)
        if merged_uploaded_rows:
            fallback_snapshot = _youtube_apply_public_inventory_to_snapshot(fallback_snapshot, merged_uploaded_rows)
        updated["analytics_snapshot"] = fallback_snapshot
        updated["last_sync_error"] = _clip_text(str(e), 220)

    updated["last_synced_at"] = time.time()
    _append_youtube_signal_log(
        {
            "ts": int(time.time()),
            "channel_id": channel_id,
            "channel_title": str(updated.get("title", "") or ""),
            "recent_upload_titles": list(dict(updated.get("analytics_snapshot") or {}).get("recent_upload_titles") or []),
            "top_video_titles": list(dict(updated.get("analytics_snapshot") or {}).get("top_video_titles") or []),
            "packaging_learnings": list(dict(updated.get("analytics_snapshot") or {}).get("packaging_learnings") or []),
            "retention_learnings": list(dict(updated.get("analytics_snapshot") or {}).get("retention_learnings") or []),
        }
    )
    return updated


async def _youtube_selected_channel_context(user: dict, preferred_channel_id: str = "") -> dict:
    user_id = str((user or {}).get("id", "") or "").strip()
    if not user_id:
        return {}
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        channels = dict(bucket.get("channels") or {})
        chosen_id = str(preferred_channel_id or bucket.get("default_channel_id", "") or "").strip()
        if not chosen_id and channels:
            chosen_id = next(iter(channels.keys()))
        record = dict(channels.get(chosen_id) or {})
    if not record:
        return {}
    try:
        refreshed = await _youtube_sync_channel_record(record)
    except Exception as e:
        refreshed = await _youtube_refresh_public_channel_record_without_oauth(
            record,
            sync_error=_clip_text(str(e), 220),
        )
    async with _youtube_connections_lock:
        bucket = _youtube_bucket_for_user(user_id)
        bucket["channels"][chosen_id] = refreshed
        if not str(bucket.get("default_channel_id", "") or "").strip():
            bucket["default_channel_id"] = chosen_id
        _save_youtube_connections()
    analytics_snapshot = dict(refreshed.get("analytics_snapshot") or {})
    return {
        "channel_id": chosen_id,
        "channel_title": str(refreshed.get("title", "") or "").strip(),
        "channel_handle": str(refreshed.get("channel_handle", "") or "").strip(),
        "channel_url": str(refreshed.get("channel_url", "") or "").strip(),
        "summary": str(analytics_snapshot.get("channel_summary", "") or "").strip(),
        "channel_video_count": int(analytics_snapshot.get("channel_video_count", 0) or 0),
        "recent_upload_titles": list(analytics_snapshot.get("recent_upload_titles") or []),
        "uploaded_videos": list(analytics_snapshot.get("uploaded_videos") or []),
        "top_video_titles": list(analytics_snapshot.get("top_video_titles") or []),
        "top_videos": list(analytics_snapshot.get("top_videos") or []),
        "title_pattern_hints": list(analytics_snapshot.get("title_pattern_hints") or []),
        "packaging_learnings": list(analytics_snapshot.get("packaging_learnings") or []),
        "retention_learnings": list(analytics_snapshot.get("retention_learnings") or []),
        "series_clusters": list(analytics_snapshot.get("series_clusters") or []),
        "series_cluster_playbook": dict(analytics_snapshot.get("series_cluster_playbook") or {}),
        "historical_compare": dict(analytics_snapshot.get("historical_compare") or {}),
        "channel_audit": dict(analytics_snapshot.get("channel_audit") or _youtube_build_channel_audit(analytics_snapshot)),
        "last_sync_error": str(refreshed.get("last_sync_error", "") or "").strip(),
    }


async def _youtube_connected_channel_access_token(user: dict, channel_id: str) -> tuple[str, dict]:
    user_id = str((user or {}).get("id", "") or "").strip()
    chosen_id = str(channel_id or "").strip()
    if not user_id or not chosen_id:
        return "", {}
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        record = dict((bucket.get("channels") or {}).get(chosen_id) or {})
    if not record:
        return "", {}
    try:
        access_token, updated = await _youtube_ensure_access_token(record)
    except Exception:
        return "", record
    async with _youtube_connections_lock:
        bucket = _youtube_bucket_for_user(user_id)
        bucket["channels"][chosen_id] = updated
        _save_youtube_connections()
    return str(access_token or "").strip(), dict(updated or {})


async def _youtube_repair_channel_record_from_sibling(
    user_id: str,
    channel_id: str,
    failed_record: dict | None,
) -> dict:
    user_key = str(user_id or "").strip()
    channel_key = str(channel_id or "").strip()
    failed = dict(failed_record or {})
    if not user_key or not channel_key:
        return {}
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_key)
        sibling_records = [
            dict(record or {})
            for sibling_id, record in dict(bucket.get("channels") or {}).items()
            if str(sibling_id or "").strip() != channel_key and isinstance(record, dict)
        ]
    if not sibling_records:
        return {}
    sibling_records.sort(
        key=lambda row: (
            -float(row.get("last_synced_at", 0.0) or 0.0),
            -float(row.get("linked_at", 0.0) or 0.0),
            str(row.get("channel_id", "") or "").lower(),
        )
    )
    seen_refresh_tokens: set[str] = set()
    for sibling in sibling_records:
        refresh_token = str(sibling.get("refresh_token", "") or "").strip()
        if not refresh_token or refresh_token in seen_refresh_tokens:
            continue
        seen_refresh_tokens.add(refresh_token)
        try:
            access_token, sibling_updated = await _youtube_ensure_access_token(sibling)
        except Exception:
            continue
        try:
            candidate_channels = await _youtube_fetch_my_channels(access_token)
        except Exception:
            continue
        matching = next(
            (
                dict(row or {})
                for row in list(candidate_channels or [])
                if str((row or {}).get("channel_id", "") or "").strip() == channel_key
            ),
            {},
        )
        if not matching:
            continue
        repaired = dict(failed or {})
        repaired.update(matching)
        repaired["access_token"] = str(sibling_updated.get("access_token", "") or "").strip()
        repaired["refresh_token"] = str(sibling_updated.get("refresh_token", "") or refresh_token).strip()
        repaired["token_expires_at"] = float(sibling_updated.get("token_expires_at", 0.0) or 0.0)
        repaired["token_scope"] = str(sibling_updated.get("token_scope", "") or repaired.get("token_scope", "") or "").strip()
        repaired["oauth_mode"] = str(sibling_updated.get("oauth_mode", "") or repaired.get("oauth_mode", "") or "").strip().lower()
        repaired["last_sync_error"] = ""
        repaired["last_synced_at"] = time.time()
        return repaired
    return {}


async def _youtube_sync_and_persist_for_user(user_id: str, channel_id: str) -> dict:
    user_key = str(user_id or "").strip()
    channel_key = str(channel_id or "").strip()
    if not user_key or not channel_key:
        return {}
    async with _youtube_connections_lock:
        _load_youtube_connections()
        record = dict((_youtube_bucket_for_user(user_key).get("channels") or {}).get(channel_key) or {})
    if not record:
        return {}
    try:
        refreshed = await _youtube_sync_channel_record(record)
    except Exception as e:
        repair_error = e
        repaired_record: dict = {}
        if _google_oauth_error_suggests_reconnect_required(str(e)):
            try:
                repaired_record = await _youtube_repair_channel_record_from_sibling(user_key, channel_key, record)
            except Exception as repair_exc:
                log.warning(
                    "YouTube sibling token repair failed for user=%s channel=%s: %s",
                    user_key,
                    channel_key,
                    repair_exc,
                )
                repaired_record = {}
        if repaired_record:
            try:
                refreshed = await _youtube_sync_channel_record(repaired_record)
            except Exception as repaired_sync_error:
                repair_error = repaired_sync_error
                refreshed = await _youtube_refresh_public_channel_record_without_oauth(
                    repaired_record,
                    sync_error=_clip_text(str(repair_error), 220),
                )
        else:
            refreshed = await _youtube_refresh_public_channel_record_without_oauth(
                record,
                sync_error=_clip_text(str(repair_error), 220),
            )
    async with _youtube_connections_lock:
        bucket = _youtube_bucket_for_user(user_key)
        bucket["channels"][channel_key] = refreshed
        if not str(bucket.get("default_channel_id", "") or "").strip():
            bucket["default_channel_id"] = channel_key
        _save_youtube_connections()
    return _youtube_connection_public_view(refreshed)


async def _youtube_connected_channel_public_view(user_id: str, channel_id: str) -> dict:
    user_key = str(user_id or "").strip()
    channel_key = str(channel_id or "").strip()
    if not user_key or not channel_key:
        return {}
    async with _youtube_connections_lock:
        _load_youtube_connections()
        record = dict((_youtube_bucket_for_user(user_key).get("channels") or {}).get(channel_key) or {})
    if not record:
        return {}
    return _youtube_connection_public_view(record)


async def _list_connected_youtube_channels_for_user(user: dict, sync: bool = True) -> dict:
    user_id = str((user or {}).get("id", "") or "").strip()
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        default_channel_id = str(bucket.get("default_channel_id", "") or "").strip()
        channels = {str(k): dict(v or {}) for k, v in dict(bucket.get("channels") or {}).items()}
    if (not default_channel_id or default_channel_id not in channels) and channels:
        default_channel_id = str(next(iter(channels.keys())) or "").strip()
    if sync and default_channel_id in channels:
        try:
            channels[default_channel_id] = await _youtube_sync_and_persist_for_user(user_id, default_channel_id)
        except Exception as e:
            stale = dict(channels.get(default_channel_id) or {})
            stale["last_sync_error"] = _clip_text(str(e), 220)
            channels[default_channel_id] = stale
    public_channels = []
    for _channel_id, record in channels.items():
        if "analytics_snapshot" in record or "access_token" in record:
            public_channels.append(_youtube_connection_public_view(record))
        else:
            public_channels.append(dict(record))
    public_channels.sort(key=lambda row: 0 if str(row.get("channel_id", "") or "") == default_channel_id else 1)
    return {
        "oauth_configured": _youtube_auth_configured(),
        "default_channel_id": default_channel_id,
        "channels": public_channels,
    }


async def _select_connected_youtube_channel_for_user(user: dict, channel_id: str) -> dict:
    user_id = str((user or {}).get("id", "") or "").strip()
    channel_key = str(channel_id or "").strip()
    if not channel_key:
        raise HTTPException(400, "channel_id required")
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        channels = dict(bucket.get("channels") or {})
        if channel_key not in channels:
            raise HTTPException(404, "Connected YouTube channel not found")
        bucket["default_channel_id"] = channel_key
        _save_youtube_connections()
    try:
        channel_public = await _youtube_sync_and_persist_for_user(user_id, channel_key)
    except Exception as e:
        log.warning(f"YouTube channel select sync failed for user={user_id} channel={channel_key}: {e}")
        channel_public = await _youtube_connected_channel_public_view(user_id, channel_key)
    return {
        "ok": True,
        "default_channel_id": channel_key,
        "channel": channel_public,
    }


async def _sync_connected_youtube_channel_for_user(user: dict, channel_id: str) -> dict:
    channel_public = await _youtube_sync_and_persist_for_user(str((user or {}).get("id", "") or ""), channel_id)
    if not channel_public:
        raise HTTPException(404, "Connected YouTube channel not found")
    return {"ok": True, "channel": channel_public}


async def _sync_connected_youtube_channel_outcomes_for_user(
    *,
    user: dict,
    channel_id: str,
    session_id: str = "",
    candidate_limit: int = 18,
    refresh_existing: bool = False,
    longform_owner_beta_enabled=None,
    harvest_catalyst_outcomes_for_channel=None,
) -> dict:
    if not callable(longform_owner_beta_enabled) or not longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    if not callable(harvest_catalyst_outcomes_for_channel):
        raise HTTPException(500, "Catalyst outcome sync is not configured")
    return await harvest_catalyst_outcomes_for_channel(
        user_id=str((user or {}).get("id", "") or ""),
        channel_id=str(channel_id or "").strip(),
        session_id=str(session_id or "").strip(),
        candidate_limit=int(candidate_limit or 18),
        refresh_existing=bool(refresh_existing),
    )


async def _disconnect_connected_youtube_channel_for_user(user: dict, channel_id: str) -> dict:
    user_id = str((user or {}).get("id", "") or "").strip()
    channel_key = str(channel_id or "").strip()
    removed = {}
    default_channel_id = ""
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        channels = dict(bucket.get("channels") or {})
        removed = dict(channels.pop(channel_key, {}) or {})
        bucket["channels"] = channels
        if str(bucket.get("default_channel_id", "") or "").strip() == channel_key:
            bucket["default_channel_id"] = next(iter(channels.keys()), "")
        default_channel_id = str(bucket.get("default_channel_id", "") or "").strip()
        _save_youtube_connections()
    refresh_token = str(removed.get("refresh_token", "") or "").strip()
    if refresh_token:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                await client.post(GOOGLE_REVOKE_URL, data={"token": refresh_token})
        except Exception:
            pass
    return {"ok": True, "default_channel_id": default_channel_id}


def _youtube_redirect_target(next_url: str, ok: bool, message: str = "") -> str:
    fallback = f"{SITE_URL.rstrip('/')}/?page=settings"
    target = str(next_url or "").strip()
    if not target:
        target = fallback
    try:
        parsed = urlparse(target)
        allowed_hosts = {
            "studio.nyptidindustries.com",
            "www.studio.nyptidindustries.com",
            "localhost",
            "127.0.0.1",
        }
        if parsed.scheme not in {"http", "https"} or str(parsed.hostname or "").lower() not in allowed_hosts:
            target = fallback
    except Exception:
        target = fallback
    sep = "&" if "?" in target else "?"
    status_value = "connected" if ok else "error"
    out = f"{target}{sep}youtube={status_value}"
    if message:
        out += "&youtube_message=" + quote(_clip_text(message, 160), safe="")
    return out


def _youtube_pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    return verifier, _youtube_pkce_challenge(verifier)


def _youtube_pkce_challenge(verifier: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(str(verifier or "").encode("utf-8")).digest()).decode("ascii").rstrip("=")


def _youtube_build_auth_url(state_token: str, oauth_mode: str | None = None, code_challenge: str = "") -> str:
    auth_context = _youtube_auth_context(oauth_mode)
    query_payload = {
        "client_id": str(auth_context.get("client_id", "") or "").strip(),
        "redirect_uri": str(auth_context.get("redirect_uri", "") or "").strip(),
        "response_type": "code",
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "scope": " ".join(YOUTUBE_SCOPES),
        "state": state_token,
    }
    if str(auth_context.get("mode", "") or "").strip() == "installed" and code_challenge:
        query_payload["code_challenge"] = code_challenge
        query_payload["code_challenge_method"] = "S256"
    query = urlencode(query_payload)
    return f"{YOUTUBE_AUTH_BASE_URL}?{query}"


def _youtube_helper_page_url(state_token: str) -> str:
    return f"/api/oauth/google/youtube/installed?state={quote(str(state_token or '').strip(), safe='')}"


async def _youtube_api_get(access_token: str, path: str, *, params: dict | None = None, analytics: bool = False) -> dict:
    base = YOUTUBE_ANALYTICS_API_BASE if analytics else YOUTUBE_DATA_API_BASE
    url = path if path.startswith("http") else f"{base}{path}"
    async with httpx.AsyncClient(timeout=45) as client:
        resp = await client.get(
            url,
            params=params or {},
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if resp.status_code != 200:
        raise RuntimeError(_youtube_format_api_failure(resp.status_code, resp.text, using_api_key=False))
    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("YouTube API returned an invalid payload")
    return payload


def _parse_youtube_iso8601_duration(raw_duration: str) -> int:
    value = str(raw_duration or "").strip().upper()
    if not value.startswith("PT"):
        return 0
    hours_match = re.search(r"(\d+)H", value)
    minutes_match = re.search(r"(\d+)M", value)
    seconds_match = re.search(r"(\d+)S", value)
    hours = int(hours_match.group(1) or 0) if hours_match else 0
    minutes = int(minutes_match.group(1) or 0) if minutes_match else 0
    seconds = int(seconds_match.group(1) or 0) if seconds_match else 0
    return (hours * 3600) + (minutes * 60) + seconds


def _youtube_channel_url(channel_id: str, custom_url: str = "") -> str:
    custom = str(custom_url or "").strip().lstrip("@")
    if custom:
        return f"https://www.youtube.com/@{custom}"
    cid = str(channel_id or "").strip()
    return f"https://www.youtube.com/channel/{cid}" if cid else ""


async def _youtube_fetch_my_channels(access_token: str) -> list[dict]:
    payload = await _youtube_api_get(
        access_token,
        "/channels",
        params={
            "part": "snippet,statistics,contentDetails",
            "mine": "true",
            "maxResults": 50,
        },
    )
    out: list[dict] = []
    for raw in list(payload.get("items") or []):
        if not isinstance(raw, dict):
            continue
        snippet = dict(raw.get("snippet") or {})
        stats = dict(raw.get("statistics") or {})
        thumbs = dict(snippet.get("thumbnails") or {})
        thumb = (
            ((thumbs.get("high") or {}).get("url"))
            or ((thumbs.get("medium") or {}).get("url"))
            or ((thumbs.get("default") or {}).get("url"))
            or ""
        )
        channel_id = str(raw.get("id", "") or "").strip()
        custom_url = str(snippet.get("customUrl", "") or "").strip()
        uploads_playlist = str(((raw.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads", "") or "").strip()
        out.append(
            {
                "channel_id": channel_id,
                "title": str(snippet.get("title", "") or "").strip(),
                "description": str(snippet.get("description", "") or "").strip(),
                "custom_url": custom_url,
                "channel_handle": ("@" + custom_url.lstrip("@")) if custom_url else "",
                "thumbnail_url": str(thumb or "").strip(),
                "channel_url": _youtube_channel_url(channel_id, custom_url),
                "subscriber_count": int(float(stats.get("subscriberCount", 0) or 0)),
                "video_count": int(float(stats.get("videoCount", 0) or 0)),
                "view_count": int(float(stats.get("viewCount", 0) or 0)),
                "uploads_playlist_id": uploads_playlist,
            }
        )
    return out


async def _youtube_fetch_videos(access_token: str, video_ids: list[str]) -> list[dict]:
    ids = [str(v).strip() for v in list(video_ids or []) if str(v).strip()]
    if not ids:
        return []
    items_by_id: dict[str, dict] = {}
    for start in range(0, len(ids), 50):
        chunk = ids[start : start + 50]
        payload = await _youtube_api_get(
            access_token,
            "/videos",
            params={
                "part": "snippet,statistics,contentDetails,status",
                "id": ",".join(chunk),
                "maxResults": min(50, len(chunk)),
            },
        )
        for raw in list(payload.get("items") or []):
            if not isinstance(raw, dict):
                continue
            vid = str(raw.get("id", "") or "").strip()
            snippet = dict(raw.get("snippet") or {})
            stats = dict(raw.get("statistics") or {})
            status = dict(raw.get("status") or {})
            items_by_id[vid] = {
                "video_id": vid,
                "title": str(snippet.get("title", "") or "").strip(),
                "description": str(snippet.get("description", "") or "").strip(),
                "published_at": str(snippet.get("publishedAt", "") or "").strip(),
                "thumbnail_url": str((((snippet.get("thumbnails") or {}).get("high") or {}).get("url") or "")).strip(),
                "tags": [str(tag).strip() for tag in list(snippet.get("tags") or []) if str(tag).strip()][:20],
                "duration_sec": _parse_youtube_iso8601_duration(str((raw.get("contentDetails") or {}).get("duration", "") or "")),
                "views": int(float(stats.get("viewCount", 0) or 0)),
                "likes": int(float(stats.get("likeCount", 0) or 0)),
                "comments": int(float(stats.get("commentCount", 0) or 0)),
                "privacy_status": str(status.get("privacyStatus", "") or "").strip(),
            }
    return [items_by_id[vid] for vid in ids if vid in items_by_id]


async def _youtube_fetch_uploads_playlist_videos(
    access_token: str,
    uploads_playlist_id: str,
    max_results: int = 250,
) -> list[dict]:
    playlist_id = str(uploads_playlist_id or "").strip()
    if not playlist_id:
        return []
    remaining = max(1, min(int(max_results or 250), 500))
    page_token = ""
    video_ids: list[str] = []
    while remaining > 0:
        params = {
            "part": "contentDetails,snippet",
            "playlistId": playlist_id,
            "maxResults": min(50, remaining),
        }
        if page_token:
            params["pageToken"] = page_token
        payload = await _youtube_api_get(access_token, "/playlistItems", params=params)
        for item in list(payload.get("items") or []):
            if not isinstance(item, dict):
                continue
            vid = str(((item.get("contentDetails") or {}).get("videoId")) or "").strip()
            if vid:
                video_ids.append(vid)
        remaining = int(max_results or 250) - len(video_ids)
        page_token = str(payload.get("nextPageToken", "") or "").strip()
        if not page_token or remaining <= 0:
            break
    return await _youtube_fetch_videos(access_token, _dedupe_preserve_order(video_ids, max_items=max_results, max_chars=24))


def _youtube_parse_published_date(raw_value: str) -> date | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _normalize_external_source_url(raw_value: str) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = "https://" + raw.lstrip("/")
    try:
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return ""
        return parsed.geturl()
    except Exception:
        return ""


def _parse_vtt_text(raw_vtt: str) -> str:
    lines = []
    for raw_line in str(raw_vtt or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("WEBVTT") or "-->" in line:
            continue
        if re.fullmatch(r"[0-9:\.\- ]+", line):
            continue
        line = re.sub(r"<[^>]+>", " ", line)
        line = re.sub(r"\{[^}]+\}", " ", line)
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    deduped: list[str] = []
    prev = ""
    for line in lines:
        if line != prev:
            deduped.append(line)
            prev = line
    return _clip_text(" ".join(deduped), 2200)


def _pick_subtitle_candidate(info: dict, language: str = "en") -> tuple[str, str]:
    preferred = []
    for lang in [str(language or "").strip().lower(), "en", "en-us", "en-gb"]:
        if lang and lang not in preferred:
            preferred.append(lang)
    pools = [
        info.get("subtitles") if isinstance(info, dict) else {},
        info.get("automatic_captions") if isinstance(info, dict) else {},
    ]
    for pool in pools:
        if not isinstance(pool, dict):
            continue
        for lang in preferred:
            variants = [lang]
            if "-" in lang:
                variants.append(lang.split("-", 1)[0])
            for variant in variants:
                entries = pool.get(variant)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    url = str(entry.get("url", "") or "").strip()
                    if url and ext == "vtt":
                        return url, ext
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = str(entry.get("url", "") or "").strip()
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    if url:
                        return url, ext or "unknown"
    return "", ""


def _source_url_video_id(source_url: str) -> str:
    raw = str(source_url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    host = str(parsed.netloc or "").lower()
    if "youtu.be" in host:
        return str(parsed.path or "").strip("/").split("/", 1)[0]
    if "youtube.com" in host or "youtube-nocookie.com" in host:
        query = parse_qs(str(parsed.query or ""))
        values = query.get("v") or []
        if values:
            return str(values[0] or "").strip()
        parts = [part for part in str(parsed.path or "").split("/") if part]
        if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live"}:
            return str(parts[1]).strip()
    return ""


def _algrow_enabled() -> bool:
    return bool(str(ALGROW_API_KEY or "").strip())


def _algrow_headers() -> dict[str, str]:
    token = str(ALGROW_API_KEY or "").strip()
    if not token:
        return {}
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


async def _algrow_json_request(
    method: str,
    path: str,
    *,
    payload: dict | None = None,
    params: dict | None = None,
    timeout_sec: float = 45.0,
) -> dict:
    if not _algrow_enabled():
        raise RuntimeError("Algrow API is not configured")
    target = str(path or "").strip()
    if not target.startswith("/"):
        target = "/" + target
    url = f"{ALGROW_API_BASE_URL}{target}"
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.request(
            method.upper(),
            url,
            headers=_algrow_headers(),
            json=(payload if payload is not None else None),
            params=(params if params is not None else None),
        )
    raw_text = str(resp.text or "").strip()
    data: dict = {}
    try:
        parsed = json.loads(raw_text or "{}")
        if isinstance(parsed, dict):
            data = parsed
    except Exception:
        data = {}
    if resp.status_code >= 400 or data.get("success") is False:
        message = (
            str(data.get("error", "") or "").strip()
            or str(data.get("message", "") or "").strip()
            or _clip_text(raw_text, 220)
            or f"Algrow request failed ({resp.status_code})"
        )
        raise RuntimeError(message)
    return data


async def _algrow_download_transcript_excerpt(transcript_url: str, max_chars: int = 4000) -> str:
    target = str(transcript_url or "").strip()
    if not target:
        return ""
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(target)
    if resp.status_code != 200 or not str(resp.text or "").strip():
        return ""
    text = re.sub(r"\s+", " ", str(resp.text or "").strip())
    return _clip_text(text, max_chars)


async def _algrow_poll_youtube_scraper(job_id: str, timeout_sec: float = 90.0) -> dict:
    target_job_id = str(job_id or "").strip()
    if not target_job_id:
        return {}
    deadline = time.monotonic() + max(float(timeout_sec or 90.0), 10.0)
    last_status = ""
    last_message = ""
    while time.monotonic() < deadline:
        data = await _algrow_json_request("GET", f"/api/youtube-scraper/{target_job_id}", timeout_sec=45.0)
        status = str(data.get("status", "") or "").strip().lower()
        if status == "completed":
            return data
        if status == "failed":
            raise RuntimeError(
                _clip_text(
                    str(data.get("error", "") or data.get("message", "") or "Algrow YouTube scraper failed").strip(),
                    220,
                )
            )
        last_status = status or last_status
        last_message = str(data.get("message", "") or data.get("status_detail_message", "") or last_message).strip()
        await asyncio.sleep(2.5)
    detail = f"Algrow YouTube scraper timed out while waiting for job {target_job_id}"
    if last_status:
        detail += f" (last status: {last_status})"
    if last_message:
        detail += f": {_clip_text(last_message, 120)}"
    raise RuntimeError(_clip_text(detail, 220))


def _algrow_simplify_comment_rows(rows: list[dict] | None, max_items: int = 5) -> list[dict]:
    comments: list[dict] = []
    for row in list(rows or [])[: max(1, int(max_items or 5))]:
        if not isinstance(row, dict):
            continue
        text = _clip_text(str(row.get("text", "") or "").strip(), 220)
        if not text:
            continue
        comments.append(
            {
                "author": _clip_text(str(row.get("author", "") or "").strip(), 80),
                "text": text,
                "likes": int(float(row.get("likes", 0) or 0) or 0),
                "published_time": _clip_text(str(row.get("published_time", "") or "").strip(), 80),
            }
        )
    return comments


async def _algrow_scrape_youtube_reference(source_url: str) -> dict:
    normalized_url = _normalize_external_source_url(source_url)
    if not normalized_url or not _algrow_enabled():
        return {}
    queued = await _algrow_json_request(
        "POST",
        "/api/youtube-scraper",
        payload={
            "url": normalized_url,
            "video_type": "videos",
            "sort": "recent",
            "max_videos": 1,
            "include_transcripts": True,
            "include_comments": True,
        },
        timeout_sec=45.0,
    )
    job_id = str(queued.get("job_id", "") or "").strip()
    if not job_id:
        return {}
    completed = await _algrow_poll_youtube_scraper(job_id, timeout_sec=90.0)
    result = dict(completed.get("result") or {})
    videos = [dict(v or {}) for v in list(result.get("videos") or []) if isinstance(v, dict)]
    if not videos:
        return {"job_id": job_id, "status": "completed", "summary": "Algrow scrape completed but returned no video rows."}
    row = videos[0]
    transcript_excerpt = await _algrow_download_transcript_excerpt(str(row.get("transcript_url", "") or "").strip(), max_chars=4000)
    comments = _algrow_simplify_comment_rows(list(row.get("comments") or []), max_items=5)
    summary_bits = [
        (
            f"Algrow scrape measured {int(float(row.get('view_count', 0) or 0) or 0)} views, "
            f"{int(float(row.get('like_count', 0) or 0) or 0)} likes, and "
            f"{int(float(row.get('comment_count', 0) or 0) or 0)} comments."
        ),
        (
            "Algrow transcript excerpt: "
            + _clip_text(transcript_excerpt, 220)
            if transcript_excerpt
            else ""
        ),
        (
            "Algrow top comments: "
            + "; ".join(
                _clip_text(
                    f"{str(comment.get('author', '') or '').strip()}: {str(comment.get('text', '') or '').strip()}",
                    140,
                )
                for comment in comments[:3]
            )
            if comments
            else ""
        ),
    ]
    return {
        "job_id": job_id,
        "video_id": str(row.get("video_id", "") or "").strip(),
        "title": _clip_text(str(row.get("title", "") or "").strip(), 220),
        "url": str(row.get("url", "") or normalized_url).strip(),
        "thumbnail_url": str(row.get("thumbnail", "") or "").strip(),
        "view_count": int(float(row.get("view_count", 0) or 0) or 0),
        "like_count": int(float(row.get("like_count", 0) or 0) or 0),
        "comment_count": int(float(row.get("comment_count", 0) or 0) or 0),
        "duration_sec": int(float(row.get("duration_seconds", 0) or 0) or 0),
        "duration_human": _clip_text(str(row.get("duration_human", "") or "").strip(), 80),
        "upload_date": str(row.get("publish_date", "") or "").strip(),
        "channel": _clip_text(str(row.get("channel", "") or "").strip(), 180),
        "channel_id": str(row.get("channel_id", "") or "").strip(),
        "transcript_url": str(row.get("transcript_url", "") or "").strip(),
        "transcript_excerpt": transcript_excerpt,
        "comments": comments,
        "summary": " | ".join(part for part in summary_bits if part),
    }


async def _algrow_thumbnail_reference_matches(
    *,
    video_url: str = "",
    image_url: str = "",
    limit: int = 5,
) -> list[dict]:
    if not _algrow_enabled():
        return []
    payload: dict[str, object] = {
        "limit": max(1, min(int(limit or 5), 10)),
        "min_similarity": 0.35,
        "min_views": 1000,
    }
    if str(video_url or "").strip():
        payload["video_url"] = str(video_url or "").strip()
    elif str(image_url or "").strip():
        payload["image_url"] = str(image_url or "").strip()
    else:
        return []
    data = await _algrow_json_request("POST", "/api/thumbnail-search", payload=payload, timeout_sec=45.0)
    videos = [dict(v or {}) for v in list(data.get("videos") or []) if isinstance(v, dict)]
    matches: list[dict] = []
    for row in videos[: max(1, min(int(limit or 5), 10))]:
        title = _clip_text(str(row.get("title", "") or "").strip(), 180)
        if not title:
            continue
        matches.append(
            {
                "video_id": str(row.get("video_id", "") or "").strip(),
                "title": title,
                "channel_name": _clip_text(str(row.get("channel_name", "") or "").strip(), 120),
                "view_count": int(float(row.get("view_count", 0) or 0) or 0),
                "similarity_score": int(float(row.get("similarity_score", 0) or 0) or 0),
                "thumbnail_url": str(row.get("thumbnail_url", "") or "").strip(),
                "url": str(row.get("url", "") or "").strip(),
                "upload_date": str(row.get("upload_date", "") or "").strip(),
                "duration": int(float(row.get("duration", 0) or 0) or 0),
            }
        )
    return matches


def _algrow_search_query_from_title(title: str) -> str:
    tokens = [
        str(token or "").strip()
        for token in re.split(r"[^A-Za-z0-9]+", str(title or ""))
        if str(token or "").strip()
    ]
    if not tokens:
        return ""
    filtered = [
        token
        for token in tokens
        if len(token) >= 3 and token.lower() not in {
            "the", "and", "with", "that", "this", "from", "into", "your", "what", "when", "then",
            "after", "before", "best", "worst", "video", "manhwa", "manga", "recap",
        }
    ]
    if not filtered:
        filtered = tokens[:6]
    return " ".join(filtered[:6])


async def _algrow_viral_reference_matches(
    *,
    source_url: str = "",
    title: str = "",
    content_type: str = "longform",
    limit: int = 5,
) -> list[dict]:
    if not _algrow_enabled():
        return []
    params: dict[str, object] = {
        "content_type": "shorts" if str(content_type or "").strip().lower() == "shorts" else "longform",
        "per_page": max(1, min(int(limit or 5), 10)),
        "languages": "English",
        "sort_by": "similarity",
    }
    video_id = str(_source_url_video_id(source_url) or "").strip()
    if video_id:
        params["video_id"] = video_id
    else:
        query = _algrow_search_query_from_title(title)
        if not query:
            return []
        params["q"] = query
    data = await _algrow_json_request("GET", "/api/viral-videos/search", params=params, timeout_sec=45.0)
    videos = [dict(v or {}) for v in list(data.get("videos") or []) if isinstance(v, dict)]
    matches: list[dict] = []
    for row in videos[: max(1, min(int(limit or 5), 10))]:
        row_title = _clip_text(str(row.get("title", "") or "").strip(), 180)
        if not row_title:
            continue
        matches.append(
            {
                "video_id": str(row.get("video_id", "") or "").strip(),
                "title": row_title,
                "channel_name": _clip_text(str(row.get("channel_name", "") or "").strip(), 120),
                "view_count": int(float(row.get("view_count", 0) or 0) or 0),
                "subscriber_count": int(float(row.get("subscriber_count", 0) or 0) or 0),
                "similarity_score": int(float(row.get("similarity_score", 0) or 0) or 0),
                "thumbnail_url": str(row.get("thumbnail_url", "") or "").strip(),
                "url": str(row.get("url", "") or "").strip(),
                "upload_date": str(row.get("upload_date", "") or "").strip(),
                "duration": int(float(row.get("duration", 0) or 0) or 0),
            }
        )
    return matches


async def _fetch_algrow_reference_enrichment(
    *,
    source_url: str = "",
    source_bundle: dict | None = None,
    workspace_id: str = "documentary",
) -> dict:
    normalized_url = _normalize_external_source_url(source_url)
    if not _algrow_enabled() or not normalized_url:
        return {}
    current_bundle = dict(source_bundle or {})
    enrichment: dict[str, object] = {
        "enabled": True,
        "source_url": normalized_url,
        "source_video": {},
        "thumbnail_matches": [],
        "viral_matches": [],
        "summary": "",
        "errors": [],
    }
    errors: list[str] = []
    source_video: dict = {}
    try:
        source_video = await _algrow_scrape_youtube_reference(normalized_url)
    except Exception as e:
        errors.append(_clip_text(f"Algrow source scrape failed: {e}", 220))
    thumbnail_matches: list[dict] = []
    try:
        thumbnail_matches = await _algrow_thumbnail_reference_matches(
            video_url=normalized_url,
            image_url=str((source_video or {}).get("thumbnail_url", "") or current_bundle.get("thumbnail_url", "") or "").strip(),
            limit=5,
        )
    except Exception as e:
        errors.append(_clip_text(f"Algrow thumbnail search failed: {e}", 220))
    viral_matches: list[dict] = []
    try:
        viral_matches = await _algrow_viral_reference_matches(
            source_url=normalized_url,
            title=str((source_video or {}).get("title", "") or current_bundle.get("title", "") or "").strip(),
            content_type="longform" if str(workspace_id or "").strip().lower() in YOUTUBE_ALGROW_LONGFORM_WORKSPACES else "shorts",
            limit=5,
        )
    except Exception as e:
        errors.append(_clip_text(f"Algrow viral search failed: {e}", 220))
    summary_parts = [
        _clip_text(str((source_video or {}).get("summary", "") or "").strip(), 320) if source_video else "",
        (
            "Algrow similar thumbnails: "
            + "; ".join(
                _clip_text(
                    f"{str(row.get('title', '') or '').strip()} ({int(float(row.get('view_count', 0) or 0) or 0)} views, similarity {int(float(row.get('similarity_score', 0) or 0) or 0)})",
                    140,
                )
                for row in thumbnail_matches[:3]
            )
            if thumbnail_matches
            else ""
        ),
        (
            "Algrow viral analogs: "
            + "; ".join(
                _clip_text(
                    f"{str(row.get('title', '') or '').strip()} by {str(row.get('channel_name', '') or '').strip()} ({int(float(row.get('view_count', 0) or 0) or 0)} views)",
                    160,
                )
                for row in viral_matches[:3]
            )
            if viral_matches
            else ""
        ),
    ]
    enrichment["source_video"] = source_video
    enrichment["thumbnail_matches"] = thumbnail_matches
    enrichment["viral_matches"] = viral_matches
    enrichment["summary"] = " | ".join(part for part in summary_parts if part)
    enrichment["errors"] = errors
    return enrichment


def _extract_html_meta_content(html_text: str, key: str, attr_names: tuple[str, ...] = ("property", "name", "itemprop")) -> str:
    source = str(html_text or "")
    if not source:
        return ""
    key_l = key.lower()
    for attr in attr_names:
        pattern = (
            r"<meta[^>]+"
            + attr
            + r"\s*=\s*['\"]"
            + re.escape(key_l)
            + r"['\"][^>]+content\s*=\s*['\"]([^'\"]+)['\"][^>]*>"
        )
        match = re.search(pattern, source, flags=re.IGNORECASE)
        if match:
            return html_lib.unescape(match.group(1)).strip()
        pattern_rev = (
            r"<meta[^>]+content\s*=\s*['\"]([^'\"]+)['\"][^>]+"
            + attr
            + r"\s*=\s*['\"]"
            + re.escape(key_l)
            + r"['\"][^>]*>"
        )
        match = re.search(pattern_rev, source, flags=re.IGNORECASE)
        if match:
            return html_lib.unescape(match.group(1)).strip()
    return ""


def _extract_html_link_href(html_text: str, rel_value: str) -> str:
    source = str(html_text or "")
    if not source:
        return ""
    pattern = (
        r"<link[^>]+rel\s*=\s*['\"]"
        + re.escape(str(rel_value or "").strip().lower())
        + r"['\"][^>]+href\s*=\s*['\"]([^'\"]+)['\"][^>]*>"
    )
    match = re.search(pattern, source, flags=re.IGNORECASE)
    if match:
        return html_lib.unescape(match.group(1)).strip()
    pattern_rev = (
        r"<link[^>]+href\s*=\s*['\"]([^'\"]+)['\"][^>]+rel\s*=\s*['\"]"
        + re.escape(str(rel_value or "").strip().lower())
        + r"['\"][^>]*>"
    )
    match = re.search(pattern_rev, source, flags=re.IGNORECASE)
    if match:
        return html_lib.unescape(match.group(1)).strip()
    return ""


def _parse_youtube_duration_from_html(html_text: str) -> int:
    source = str(html_text or "")
    if not source:
        return 0
    for pattern in (
        r'"lengthSeconds":"(\d+)"',
        r'"approxDurationMs":"(\d+)"',
        r'"duration":"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"',
    ):
        match = re.search(pattern, source, flags=re.IGNORECASE)
        if not match:
            continue
        if len(match.groups()) == 1:
            value = int(match.group(1) or 0)
            if "DurationMs" in pattern:
                return int(round(value / 1000.0))
            return value
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        total = (hours * 3600) + (minutes * 60) + seconds
        if total > 0:
            return total
    return 0


async def _fetch_public_video_bundle_fallback(source_url: str) -> dict:
    normalized_url = _normalize_external_source_url(source_url)
    if not normalized_url:
        return {}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
        )
    }
    html_text = ""
    final_url = normalized_url
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
        try:
            resp = await client.get(normalized_url)
            if resp.status_code == 200:
                html_text = resp.text
                final_url = str(resp.url)
        except Exception:
            html_text = ""
        title = ""
        author_name = ""
        thumbnail_url = ""
        try:
            oembed_url = "https://www.youtube.com/oembed?url=" + quote(final_url or normalized_url, safe="") + "&format=json"
            oembed_resp = await client.get(oembed_url)
            if oembed_resp.status_code == 200:
                oembed = oembed_resp.json()
                title = str(oembed.get("title", "") or "").strip()
                author_name = str(oembed.get("author_name", "") or "").strip()
                thumbnail_url = str(oembed.get("thumbnail_url", "") or "").strip()
        except Exception:
            pass

    if not html_text and not title and not author_name and not thumbnail_url:
        return {}

    scraped_title = (
        _extract_html_meta_content(html_text, "og:title")
        or _extract_html_meta_content(html_text, "twitter:title")
        or title
    )
    scraped_description = (
        _extract_html_meta_content(html_text, "og:description")
        or _extract_html_meta_content(html_text, "description")
    )
    scraped_thumbnail = (
        _extract_html_meta_content(html_text, "og:image")
        or _extract_html_meta_content(html_text, "twitter:image")
        or thumbnail_url
    )
    scraped_channel = (
        _extract_html_meta_content(html_text, "author")
        or _extract_html_meta_content(html_text, "og:video:tag")
        or author_name
    )
    canonical_url = _extract_html_link_href(html_text, "canonical") or final_url or normalized_url
    duration_sec = _parse_youtube_duration_from_html(html_text)
    public_summary_parts = [
        f"Title: {scraped_title}" if scraped_title else "",
        f"Channel: {scraped_channel}" if scraped_channel else "",
        f"Duration: {duration_sec}s" if duration_sec > 0 else "",
        f"Description: {_clip_text(scraped_description, 240)}" if scraped_description else "",
        "Metadata extracted from public page fallback because direct video metadata was blocked." if html_text else "",
    ]
    return {
        "source_url": normalized_url,
        "canonical_url": canonical_url,
        "platform": "youtube_public_fallback" if _source_url_video_id(normalized_url) else "web_public_fallback",
        "title": scraped_title,
        "description": scraped_description,
        "channel": scraped_channel,
        "channel_url": "",
        "thumbnail_url": scraped_thumbnail,
        "duration_sec": duration_sec,
        "view_count": 0,
        "like_count": 0,
        "comment_count": 0,
        "upload_date": "",
        "tags": [],
        "categories": [],
        "chapters": [],
        "transcript_excerpt": "",
        "public_summary": " | ".join(part for part in public_summary_parts if part),
    }


def _yt_dlp_extract_info_blocking(source_url: str) -> dict:
    yt_dlp_bin = shutil.which("yt-dlp")
    if not yt_dlp_bin and _youtube_ytdlp_module is None:
        raise RuntimeError("yt-dlp is not installed")
    client_sets = [
        ["android", "web"],
        ["tv_embedded", "android", "web"],
        ["ios", "android", "web"],
        ["mweb", "android", "web"],
    ]
    last_error = ""
    if yt_dlp_bin:
        for player_clients in client_sets:
            cmd = [
                yt_dlp_bin,
                "--dump-single-json",
                "--no-warnings",
                "--skip-download",
                "--no-playlist",
                "--extractor-args",
                f"youtube:player_client={','.join(player_clients)}",
                "--add-header",
                (
                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                ),
                source_url,
            ]
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if proc.returncode == 0 and str(proc.stdout or "").strip():
                    info = json.loads(proc.stdout)
                    if isinstance(info, dict) and info:
                        return info
                last_error = _clip_text(str(proc.stderr or proc.stdout or last_error), 220)
            except Exception as e:
                last_error = str(e)
                continue
    if _youtube_ytdlp_module is not None:
        for player_clients in client_sets:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "noplaylist": True,
                "extract_flat": False,
                "extractor_args": {"youtube": {"player_client": player_clients}},
                "http_headers": {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/135.0.0.0 Safari/537.36"
                    )
                },
            }
            try:
                with _youtube_ytdlp_module.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(source_url, download=False) or {}
                if isinstance(info, dict) and info:
                    return info
            except Exception as e:
                last_error = str(e)
                continue
    raise RuntimeError(last_error or "yt-dlp metadata extraction failed")


def _youtube_public_api_key_candidates() -> list[str]:
    keys = [str(value or "").strip() for value in list(YOUTUBE_API_KEYS or []) if str(value or "").strip()]
    if YOUTUBE_API_KEY and YOUTUBE_API_KEY not in keys:
        keys.insert(0, YOUTUBE_API_KEY)
    return keys


async def _youtube_public_api_get(path: str, *, params: dict | None = None, timeout_sec: int = 30) -> tuple[dict, str]:
    keys = _youtube_public_api_key_candidates()
    if not keys:
        raise RuntimeError("YouTube API key not configured")
    url = path if str(path or "").startswith("http") else f"{YOUTUBE_DATA_API_BASE}{path}"
    last_error: str = ""
    async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
        for key in keys:
            query = dict(params or {})
            query["key"] = key
            try:
                resp = await client.get(url, params=query)
            except Exception as exc:
                last_error = str(exc)
                continue
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, dict):
                    return payload, key
                last_error = "YouTube API returned an invalid payload"
                continue
            last_error = _youtube_format_api_failure(resp.status_code, resp.text, using_api_key=True)
    raise RuntimeError(last_error or "YouTube API key request failed")


async def _youtube_fetch_public_trend_titles(query: str, max_results: int = 6) -> list[str]:
    search_query = re.sub(r"\s+", " ", str(query or "").strip())
    if not search_query:
        return []
    published_after = datetime.now(timezone.utc) - timedelta(days=45)
    try:
        payload, _active_key = await _youtube_public_api_get(
            "/search",
            params={
                "part": "snippet",
                "type": "video",
                "q": search_query,
                "order": "date",
                "maxResults": max(1, min(int(max_results or 6), 10)),
                "publishedAfter": published_after.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            },
            timeout_sec=20,
        )
    except Exception:
        return []
    titles: list[str] = []
    for item in list(payload.get("items") or []):
        title = _clip_text(str(((item or {}).get("snippet") or {}).get("title", "") or "").strip(), 120)
        if title:
            titles.append(title)
    return _dedupe_preserve_order(titles, max_items=max_results, max_chars=120)


async def _youtube_fetch_public_reference_shorts(query: str, max_results: int = 8) -> list[dict]:
    search_query = re.sub(r"\s+", " ", str(query or "").strip())
    if not search_query:
        return []
    published_after = datetime.now(timezone.utc) - timedelta(days=120)
    try:
        payload, _active_key = await _youtube_public_api_get(
            "/search",
            params={
                "part": "snippet",
                "type": "video",
                "q": search_query,
                "order": "viewCount",
                "maxResults": max(3, min(int(max_results or 8), 12)),
                "publishedAfter": published_after.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            },
            timeout_sec=25,
        )
    except Exception:
        return []
    raw_items = [dict(item or {}) for item in list(payload.get("items") or []) if isinstance(item, dict)]
    video_ids = [str(((item.get("id") or {}).get("videoId")) or "").strip() for item in raw_items]
    video_ids = [video_id for video_id in video_ids if video_id]
    if not video_ids:
        return []
    try:
        video_payload, _active_key = await _youtube_public_api_get(
            "/videos",
            params={
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(video_ids[:12]),
                "maxResults": min(12, len(video_ids)),
            },
            timeout_sec=25,
        )
    except Exception:
        return []
    rows_by_id: dict[str, dict] = {}
    for raw in list(video_payload.get("items") or []):
        if not isinstance(raw, dict):
            continue
        video_id = str(raw.get("id", "") or "").strip()
        if not video_id:
            continue
        snippet = dict(raw.get("snippet") or {})
        stats = dict(raw.get("statistics") or {})
        title = _clip_text(str(snippet.get("title", "") or "").strip(), 120)
        duration_sec = _parse_youtube_iso8601_duration(str((raw.get("contentDetails") or {}).get("duration", "") or ""))
        if duration_sec <= 0 or duration_sec > 180:
            continue
        rows_by_id[video_id] = {
            "video_id": video_id,
            "title": title,
            "channel_title": str(snippet.get("channelTitle", "") or "").strip(),
            "published_at": str(snippet.get("publishedAt", "") or "").strip(),
            "description": _clip_text(str(snippet.get("description", "") or "").strip(), 220),
            "duration_sec": duration_sec,
            "views": int(float(stats.get("viewCount", 0) or 0)),
            "likes": int(float(stats.get("likeCount", 0) or 0)),
            "query": search_query,
        }
    rows = list(rows_by_id.values())
    rows.sort(
        key=lambda row: (
            -int(row.get("views", 0) or 0),
            int(row.get("duration_sec", 0) or 0),
            str(row.get("published_at", "") or ""),
        )
    )
    return rows[: max(3, min(int(max_results or 8), 10))]


async def _youtube_fetch_public_video_bundle_api_key(source_url: str) -> dict:
    video_id = _youtube_source_url_video_id(source_url)
    if not video_id:
        return {}
    try:
        payload, _active_key = await _youtube_public_api_get(
            "/videos",
            params={
                "part": "snippet,contentDetails,statistics",
                "id": video_id,
            },
        )
    except Exception:
        return {}
    items = list(payload.get("items") or [])
    if not items:
        return {}
    raw = dict(items[0] or {})
    snippet = dict(raw.get("snippet") or {})
    stats = dict(raw.get("statistics") or {})
    thumbs = dict(snippet.get("thumbnails") or {})
    tags = [str(tag).strip() for tag in list(snippet.get("tags") or []) if str(tag).strip()][:20]
    categories = [str(snippet.get("categoryId", "") or "").strip()] if str(snippet.get("categoryId", "") or "").strip() else []
    duration_sec = _parse_youtube_iso8601_duration(str((raw.get("contentDetails") or {}).get("duration", "") or ""))
    title = str(snippet.get("title", "") or "").strip()
    channel = str(snippet.get("channelTitle", "") or "").strip()
    description = str(snippet.get("description", "") or "").strip()
    thumbnail_url = str((((thumbs.get("high") or {}).get("url")) or ((thumbs.get("medium") or {}).get("url")) or ((thumbs.get("default") or {}).get("url")) or "")).strip()
    view_count = int(float(stats.get("viewCount", 0) or 0))
    like_count = int(float(stats.get("likeCount", 0) or 0))
    comment_count = int(float(stats.get("commentCount", 0) or 0))
    summary_parts = [
        f"Title: {title}" if title else "",
        f"Channel: {channel}" if channel else "",
        f"Duration: {duration_sec}s" if duration_sec > 0 else "",
        f"Views: {view_count}" if view_count > 0 else "",
        f"Likes: {like_count}" if like_count > 0 else "",
        f"Tags: {', '.join(tags[:8])}" if tags else "",
        "Metadata extracted through the official YouTube Data API.",
    ]
    return {
        "source_url": str(source_url or "").strip(),
        "canonical_url": f"https://www.youtube.com/watch?v={video_id}",
        "platform": "youtube_data_api",
        "title": title,
        "description": description,
        "channel": channel,
        "channel_url": "",
        "thumbnail_url": thumbnail_url,
        "duration_sec": duration_sec,
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "upload_date": str(snippet.get("publishedAt", "") or "").strip(),
        "tags": tags,
        "categories": categories,
        "chapters": [],
        "transcript_excerpt": "",
        "public_summary": " | ".join(part for part in summary_parts if part),
    }


async def _youtube_fetch_public_videos_api_key(video_ids: list[str]) -> list[dict]:
    ids = [str(v).strip() for v in list(video_ids or []) if str(v).strip()]
    if not ids:
        return []
    items_by_id: dict[str, dict] = {}
    for start in range(0, len(ids), 50):
        chunk = ids[start : start + 50]
        payload, _active_key = await _youtube_public_api_get(
            "/videos",
            params={
                "part": "snippet,statistics,contentDetails,status",
                "id": ",".join(chunk),
                "maxResults": min(50, len(chunk)),
            },
        )
        for raw in list(payload.get("items") or []):
            if not isinstance(raw, dict):
                continue
            vid = str(raw.get("id", "") or "").strip()
            if not vid:
                continue
            snippet = dict(raw.get("snippet") or {})
            stats = dict(raw.get("statistics") or {})
            status = dict(raw.get("status") or {})
            items_by_id[vid] = {
                "video_id": vid,
                "title": str(snippet.get("title", "") or "").strip(),
                "description": str(snippet.get("description", "") or "").strip(),
                "published_at": str(snippet.get("publishedAt", "") or "").strip(),
                "thumbnail_url": str((((snippet.get("thumbnails") or {}).get("high") or {}).get("url") or "")).strip(),
                "tags": [str(tag).strip() for tag in list(snippet.get("tags") or []) if str(tag).strip()][:20],
                "duration_sec": _parse_youtube_iso8601_duration(str((raw.get("contentDetails") or {}).get("duration", "") or "")),
                "views": int(float(stats.get("viewCount", 0) or 0)),
                "likes": int(float(stats.get("likeCount", 0) or 0)),
                "comments": int(float(stats.get("commentCount", 0) or 0)),
                "privacy_status": str(status.get("privacyStatus", "") or "").strip(),
            }
    return [items_by_id[vid] for vid in ids if vid in items_by_id]


async def _youtube_fetch_public_channel_search_api_key(channel_id: str, order: str = "date", max_results: int = 12) -> list[dict]:
    clean_channel_id = str(channel_id or "").strip()
    if not clean_channel_id:
        return []
    remaining = max(1, min(int(max_results or 12), 250))
    page_token = ""
    video_ids: list[str] = []
    seen_ids: set[str] = set()
    while remaining > 0:
        payload, _active_key = await _youtube_public_api_get(
            "/search",
            params={
                "part": "snippet",
                "channelId": clean_channel_id,
                "order": order,
                "maxResults": min(50, remaining),
                "type": "video",
                **({"pageToken": page_token} if page_token else {}),
            },
        )
        for raw in list(payload.get("items") or []):
            if not isinstance(raw, dict):
                continue
            vid = str(((raw.get("id") or {}).get("videoId")) or "").strip()
            if not vid or vid in seen_ids:
                continue
            seen_ids.add(vid)
            video_ids.append(vid)
        remaining = int(max_results or 12) - len(video_ids)
        page_token = str(payload.get("nextPageToken", "") or "").strip()
        if not page_token or remaining <= 0:
            break
    return await _youtube_fetch_public_videos_api_key(video_ids)


def _youtube_channel_videos_page_url(channel_url: str, channel_id: str) -> str:
    base = str(channel_url or "").strip()
    if base:
        return base.rstrip("/") + "/videos"
    clean_channel_id = str(channel_id or "").strip()
    return f"https://www.youtube.com/channel/{clean_channel_id}/videos" if clean_channel_id else ""


def _youtube_extract_video_ids_from_channel_page(html: str) -> list[str]:
    text = str(html or "")
    ids: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r'"videoId":"([A-Za-z0-9_-]{11})"', text):
        video_id = str(match.group(1) or "").strip()
        if not video_id or video_id in seen:
            continue
        seen.add(video_id)
        ids.append(video_id)
    return ids


def _youtube_renderer_text(node: object) -> str:
    if isinstance(node, dict):
        simple_text = str(node.get("simpleText", "") or "").strip()
        if simple_text:
            return simple_text
        runs = list(node.get("runs") or [])
        return "".join(str((run or {}).get("text", "") or "") for run in runs if isinstance(run, dict)).strip()
    if isinstance(node, list):
        return "".join(_youtube_renderer_text(value) for value in node).strip()
    return str(node or "").strip()


def _youtube_parse_compact_count(raw_value: str) -> int:
    text = str(raw_value or "").strip().replace(",", "")
    if not text:
        return 0
    if text.lower().startswith("no "):
        return 0
    match = re.search(r"(\d+(?:\.\d+)?)\s*([kmb])?\b", text, flags=re.IGNORECASE)
    if not match:
        return 0
    value = float(match.group(1) or 0.0)
    suffix = str(match.group(2) or "").strip().lower()
    multiplier = {
        "k": 1_000,
        "m": 1_000_000,
        "b": 1_000_000_000,
    }.get(suffix, 1)
    return int(round(value * multiplier))


def _youtube_video_renderer_duration_sec(video_renderer: dict) -> int:
    duration_text = _youtube_renderer_text((video_renderer or {}).get("lengthText") or {})
    if not duration_text:
        for overlay in list((video_renderer or {}).get("thumbnailOverlays") or []):
            overlay_payload = dict((overlay or {}).get("thumbnailOverlayTimeStatusRenderer") or {})
            duration_text = _youtube_renderer_text(overlay_payload.get("text") or {})
            if duration_text:
                break
    return _youtube_duration_text_to_seconds(duration_text)


def _youtube_extract_public_channel_page_rows(html: str) -> list[dict]:
    text = str(html or "")
    initial_data_match = (
        re.search(r"var ytInitialData = (\{.*?\});", text)
        or re.search(r"ytInitialData\s*=\s*(\{.*?\});", text)
    )
    if not initial_data_match:
        return []
    try:
        payload = json.loads(str(initial_data_match.group(1) or "{}"))
    except Exception:
        return []

    rows: list[dict] = []
    seen_ids: set[str] = set()

    def _walk(node: object) -> None:
        if isinstance(node, dict):
            video_renderer = dict(node.get("videoRenderer") or {})
            if video_renderer:
                video_id = str(video_renderer.get("videoId", "") or "").strip()
                if video_id and video_id not in seen_ids:
                    seen_ids.add(video_id)
                    title = _youtube_renderer_text(video_renderer.get("title") or {})
                    thumbnail_url = ""
                    thumb_candidates = list((video_renderer.get("thumbnail") or {}).get("thumbnails") or [])
                    if thumb_candidates:
                        thumbnail_url = str((thumb_candidates[-1] or {}).get("url", "") or "").strip()
                    published_label = _youtube_renderer_text(video_renderer.get("publishedTimeText") or {})
                    view_label = _youtube_renderer_text(
                        (video_renderer.get("viewCountText") or {})
                        or (video_renderer.get("shortViewCountText") or {})
                    )
                    rows.append(
                        {
                            "video_id": video_id,
                            "title": title,
                            "published_at": "",
                            "published_label": published_label,
                            "thumbnail_url": thumbnail_url,
                            "views": _youtube_parse_compact_count(view_label),
                            "duration_sec": _youtube_video_renderer_duration_sec(video_renderer),
                            "privacy_status": "public",
                        }
                    )
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for value in node:
                _walk(value)

    _walk(payload)
    return rows


def _youtube_extract_public_channel_rows_with_ytdlp(channel_url: str, channel_id: str, max_results: int = 100) -> list[dict]:
    page_url = _youtube_channel_videos_page_url(channel_url, channel_id)
    if not page_url or _youtube_ytdlp_module is None:
        return []
    try:
        info = _youtube_ytdlp_extract_info_blocking(page_url)
    except Exception:
        return []
    entries = [dict(v or {}) for v in list((info or {}).get("entries") or []) if isinstance(v, dict)]
    rows: list[dict] = []
    for entry in entries[: max(1, min(int(max_results or 100), 250))]:
        video_id = str(entry.get("id", "") or "").strip()
        if not video_id:
            continue
        upload_date = str(entry.get("upload_date", "") or "").strip()
        published_at = ""
        if len(upload_date) == 8 and upload_date.isdigit():
            published_at = f"{upload_date[0:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00Z"
        rows.append(
            {
                "video_id": video_id,
                "title": _clip_text(str(entry.get("title", "") or "").strip(), 180),
                "published_at": published_at,
                "published_label": "",
                "thumbnail_url": str(entry.get("thumbnail", "") or "").strip(),
                "views": int(float(entry.get("view_count", 0) or 0) or 0),
                "likes": int(float(entry.get("like_count", 0) or 0) or 0),
                "comments": int(float(entry.get("comment_count", 0) or 0) or 0),
                "duration_sec": int(float(entry.get("duration", 0) or 0) or 0),
                "privacy_status": "public",
            }
        )
    return rows


def _youtube_merge_public_video_rows(primary_rows: list[dict] | None, supplemental_rows: list[dict] | None) -> list[dict]:
    primary = [dict(row or {}) for row in list(primary_rows or []) if isinstance(row, dict)]
    supplemental = [dict(row or {}) for row in list(supplemental_rows or []) if isinstance(row, dict)]
    if not primary:
        return supplemental
    supplemental_by_id = {
        str((row or {}).get("video_id", "") or "").strip(): dict(row or {})
        for row in supplemental
        if str((row or {}).get("video_id", "") or "").strip()
    }
    merged_rows: list[dict] = []
    seen_ids: set[str] = set()
    for row in primary:
        clean_video_id = str((row or {}).get("video_id", "") or "").strip()
        merged = dict(row or {})
        if clean_video_id and clean_video_id in supplemental_by_id:
            for key, value in dict(supplemental_by_id.get(clean_video_id) or {}).items():
                if value not in (None, "", [], {}):
                    merged[key] = value
        if clean_video_id:
            seen_ids.add(clean_video_id)
        merged_rows.append(merged)
    for row in supplemental:
        clean_video_id = str((row or {}).get("video_id", "") or "").strip()
        if clean_video_id and clean_video_id in seen_ids:
            continue
        merged_rows.append(dict(row or {}))
    return merged_rows


async def _youtube_fetch_public_channel_page_videos(
    access_token: str,
    *,
    channel_url: str = "",
    channel_id: str = "",
    max_results: int = 100,
) -> list[dict]:
    page_url = _youtube_channel_videos_page_url(channel_url, channel_id)
    if not page_url:
        return []
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(
                page_url,
                headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"},
            )
        if resp.status_code != 200:
            return []
        page_rows = _youtube_extract_public_channel_page_rows(resp.text)
        if not page_rows:
            video_ids = _youtube_extract_video_ids_from_channel_page(resp.text)
            page_rows = [
                {
                    "video_id": str(video_id or "").strip(),
                    "title": "",
                    "published_at": "",
                    "published_label": "",
                    "thumbnail_url": "",
                    "privacy_status": "public",
                }
                for video_id in video_ids
                if str(video_id or "").strip()
            ]
        if not page_rows:
            page_rows = _youtube_extract_public_channel_rows_with_ytdlp(
                channel_url=channel_url,
                channel_id=channel_id,
                max_results=max_results,
            )
        if not page_rows:
            return []
        page_rows = page_rows[: max(1, min(int(max_results or 100), 100))]
        if not any(int(float((row or {}).get("views", 0) or 0) or 0) > 0 for row in page_rows):
            ytdlp_rows = _youtube_extract_public_channel_rows_with_ytdlp(
                channel_url=channel_url,
                channel_id=channel_id,
                max_results=max_results,
            )
            if ytdlp_rows:
                page_rows = _youtube_merge_public_video_rows(page_rows, ytdlp_rows)
        page_ids = [str((row or {}).get("video_id", "") or "").strip() for row in list(page_rows or []) if str((row or {}).get("video_id", "") or "").strip()]
        hydrated_rows: list[dict] = []
        if page_ids:
            try:
                hydrated_rows = await _youtube_fetch_public_videos_api_key(page_ids)
            except Exception:
                hydrated_rows = []
            if not hydrated_rows and access_token:
                try:
                    hydrated_rows = await _youtube_fetch_videos(access_token, page_ids)
                except Exception:
                    hydrated_rows = []
        if not hydrated_rows:
            return page_rows
        hydrated_by_id = {
            str((row or {}).get("video_id", "") or "").strip(): dict(row or {})
            for row in list(hydrated_rows or [])
            if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
        }
        merged_rows: list[dict] = []
        for row in list(page_rows or []):
            clean_video_id = str((row or {}).get("video_id", "") or "").strip()
            merged = dict(row or {})
            if clean_video_id and clean_video_id in hydrated_by_id:
                merged = {
                    **merged,
                    **dict(hydrated_by_id.get(clean_video_id) or {}),
                }
            merged_rows.append(merged)
        return merged_rows
    except Exception:
        return _youtube_extract_public_channel_rows_with_ytdlp(
            channel_url=channel_url,
            channel_id=channel_id,
            max_results=max_results,
        )


def _youtube_caption_language_candidates(language: str = "en") -> list[str]:
    preferred: list[str] = []
    for raw in [str(language or "").strip().lower(), "en", "en-us", "en-gb"]:
        if raw and raw not in preferred:
            preferred.append(raw)
        if "-" in raw:
            base = raw.split("-", 1)[0].strip()
            if base and base not in preferred:
                preferred.append(base)
    return preferred


def _youtube_caption_track_sort_key(track: dict, language: str = "en") -> tuple[int, int, int, int, str]:
    payload = dict(track or {})
    snippet = dict(payload.get("snippet") or {})
    lang = str(snippet.get("language", "") or "").strip().lower()
    status = str(snippet.get("status", "") or "").strip().lower()
    is_draft = bool(snippet.get("isDraft"))
    is_auto_synced = bool(snippet.get("isAutoSynced"))
    language_rank = 99
    for idx, candidate in enumerate(_youtube_caption_language_candidates(language)):
        if lang == candidate:
            language_rank = idx
            break
    return (
        language_rank,
        0 if status == "serving" else 1,
        1 if is_draft else 0,
        0 if is_auto_synced else 1,
        str(payload.get("id", "") or "").strip(),
    )


async def _youtube_download_caption_vtt(access_token: str, caption_id: str) -> str:
    clean_id = str(caption_id or "").strip()
    if not clean_id:
        return ""
    url = f"{YOUTUBE_DATA_API_BASE}/captions/{quote(clean_id, safe='')}"
    async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
        resp = await client.get(
            url,
            params={"tfmt": "vtt"},
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if resp.status_code != 200:
        return ""
    content_type = str(resp.headers.get("content-type", "") or "").lower()
    text = resp.text
    if "json" in content_type:
        return ""
    return _youtube_parse_vtt_text(text)


async def _youtube_download_public_timedtext_transcript(video_id: str, language: str = "en") -> str:
    clean_id = str(video_id or "").strip()
    if not clean_id:
        return ""
    param_variants: list[dict[str, str]] = []
    seen_keys: set[tuple[tuple[str, str], ...]] = set()
    for lang in _youtube_caption_language_candidates(language):
        for base_params in (
            {"v": clean_id, "lang": lang, "fmt": "vtt"},
            {"v": clean_id, "lang": lang, "fmt": "vtt", "kind": "asr"},
        ):
            key = tuple(sorted((str(k), str(v)) for k, v in base_params.items()))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            param_variants.append(base_params)
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for params in param_variants:
            try:
                resp = await client.get("https://www.youtube.com/api/timedtext", params=params)
            except Exception:
                continue
            if resp.status_code != 200 or not str(resp.text or "").strip():
                continue
            transcript_excerpt = _youtube_parse_vtt_text(resp.text)
            if transcript_excerpt:
                return transcript_excerpt
    return ""


async def _youtube_fetch_owned_video_bundle_oauth(
    access_token: str,
    video_id: str,
    *,
    language: str = "en",
) -> dict:
    clean_id = str(video_id or "").strip()
    if not clean_id:
        return {}
    payload = await _youtube_api_get(
        access_token,
        "/videos",
        params={
            "part": "snippet,statistics,contentDetails,status",
            "id": clean_id,
            "maxResults": 1,
        },
    )
    items = [dict(v or {}) for v in list(payload.get("items") or []) if isinstance(v, dict)]
    if not items:
        return {}
    raw = dict(items[0] or {})
    snippet = dict(raw.get("snippet") or {})
    stats = dict(raw.get("statistics") or {})
    thumbs = dict(snippet.get("thumbnails") or {})
    transcript_excerpt = ""
    try:
        captions_payload = await _youtube_api_get(
            access_token,
            "/captions",
            params={
                "part": "snippet",
                "videoId": clean_id,
                "maxResults": 50,
            },
        )
        caption_items = [dict(v or {}) for v in list(captions_payload.get("items") or []) if isinstance(v, dict)]
        if caption_items:
            best_track = sorted(caption_items, key=lambda row: _youtube_caption_track_sort_key(row, language=language))[0]
            transcript_excerpt = await _youtube_download_caption_vtt(access_token, str(best_track.get("id", "") or "").strip())
    except Exception:
        transcript_excerpt = ""
    if not transcript_excerpt:
        try:
            transcript_excerpt = await _youtube_download_public_timedtext_transcript(clean_id, language=language)
        except Exception:
            transcript_excerpt = ""
    title = str(snippet.get("title", "") or "").strip()
    description = str(snippet.get("description", "") or "").strip()
    channel = str(snippet.get("channelTitle", "") or "").strip()
    duration_sec = _parse_youtube_iso8601_duration(str((raw.get("contentDetails") or {}).get("duration", "") or ""))
    view_count = int(float(stats.get("viewCount", 0) or 0))
    like_count = int(float(stats.get("likeCount", 0) or 0))
    comment_count = int(float(stats.get("commentCount", 0) or 0))
    tags = [str(tag).strip() for tag in list(snippet.get("tags") or []) if str(tag).strip()][:20]
    thumb = (
        ((thumbs.get("maxres") or {}).get("url"))
        or ((thumbs.get("standard") or {}).get("url"))
        or ((thumbs.get("high") or {}).get("url"))
        or ((thumbs.get("medium") or {}).get("url"))
        or ((thumbs.get("default") or {}).get("url"))
        or ""
    )
    summary_parts = [
        f"Title: {title}" if title else "",
        f"Channel: {channel}" if channel else "",
        f"Duration: {duration_sec}s" if duration_sec > 0 else "",
        f"Views: {view_count}" if view_count > 0 else "",
        f"Likes: {like_count}" if like_count > 0 else "",
        f"Comments: {comment_count}" if comment_count > 0 else "",
        f"Tags: {', '.join(tags[:8])}" if tags else "",
        f"Transcript excerpt: {transcript_excerpt}" if transcript_excerpt else "",
        "Metadata extracted through the connected YouTube OAuth channel.",
    ]
    return {
        "source_url": f"https://www.youtube.com/watch?v={clean_id}",
        "canonical_url": f"https://www.youtube.com/watch?v={clean_id}",
        "platform": "youtube_oauth",
        "title": title,
        "description": description,
        "channel": channel,
        "channel_url": "",
        "thumbnail_url": str(thumb or "").strip(),
        "duration_sec": duration_sec,
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "upload_date": str(snippet.get("publishedAt", "") or "").strip(),
        "tags": tags,
        "categories": [str(snippet.get("categoryId", "") or "").strip()] if str(snippet.get("categoryId", "") or "").strip() else [],
        "chapters": [],
        "transcript_excerpt": transcript_excerpt,
        "public_summary": " | ".join(part for part in summary_parts if part),
    }


async def _fetch_source_video_bundle(source_url: str, language: str = "en") -> dict:
    normalized_url = _normalize_external_source_url(source_url)
    if not normalized_url:
        return {}
    official_api_bundle = await _youtube_fetch_public_video_bundle_api_key(normalized_url)
    if official_api_bundle:
        video_id = _source_url_video_id(normalized_url)
        merged = dict(official_api_bundle)
        transcript_excerpt = ""
        try:
            info = await asyncio.to_thread(_youtube_ytdlp_extract_info_blocking, normalized_url)
            if isinstance(info, dict):
                merged["_yt_dlp_info"] = info
                subtitle_url, subtitle_ext = _pick_subtitle_candidate(info, language=language)
                if subtitle_url and subtitle_ext == "vtt":
                    try:
                        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                            resp = await client.get(subtitle_url)
                            if resp.status_code == 200:
                                transcript_excerpt = _parse_vtt_text(resp.text)
                    except Exception:
                        transcript_excerpt = ""
                if transcript_excerpt:
                    merged["transcript_excerpt"] = transcript_excerpt
                    merged["public_summary"] = " | ".join(
                        part
                        for part in [
                            str(merged.get("public_summary", "") or "").strip(),
                            f"Transcript excerpt: {transcript_excerpt}",
                        ]
                        if part
                    )
        except Exception:
            transcript_excerpt = ""
        if not transcript_excerpt and video_id:
            try:
                transcript_excerpt = await _youtube_download_public_timedtext_transcript(video_id, language=language)
            except Exception:
                transcript_excerpt = ""
        if transcript_excerpt:
            merged["transcript_excerpt"] = transcript_excerpt
            merged["public_summary"] = " | ".join(
                part
                for part in [
                    str(merged.get("public_summary", "") or "").strip(),
                    f"Transcript excerpt: {transcript_excerpt}",
                ]
                if part
            )
        return merged
    has_ytdlp = bool(shutil.which("yt-dlp") or _youtube_ytdlp_module is not None)
    if not has_ytdlp:
        fallback_bundle = await _fetch_public_video_bundle_fallback(normalized_url)
        if fallback_bundle:
            fallback_bundle = dict(fallback_bundle)
            fallback_bundle["error"] = "yt_dlp_unavailable"
            return fallback_bundle
        return {
            "source_url": normalized_url,
            "error": "yt_dlp_unavailable",
            "public_summary": "Source URL provided, but yt-dlp is not available on this deployment.",
        }
    try:
        info = await asyncio.to_thread(_youtube_ytdlp_extract_info_blocking, normalized_url)
    except Exception as e:
        fallback_bundle = await _fetch_public_video_bundle_fallback(normalized_url)
        if fallback_bundle:
            fallback_bundle = dict(fallback_bundle)
            fallback_bundle["error"] = str(e)
            fallback_bundle["public_summary"] = " | ".join(
                part
                for part in [
                    str(fallback_bundle.get("public_summary", "") or "").strip(),
                    "yt-dlp fallback used after metadata extraction was blocked.",
                ]
                if part
            )
            return fallback_bundle
        return {
            "source_url": normalized_url,
            "error": str(e),
            "public_summary": f"Source URL provided but metadata extraction failed: {_clip_text(str(e), 180)}",
        }

    chapters = []
    for raw in list(info.get("chapters") or [])[:12]:
        if not isinstance(raw, dict):
            continue
        chapters.append(
            {
                "title": str(raw.get("title", "") or "").strip(),
                "start_sec": float(raw.get("start_time", 0.0) or 0.0),
                "end_sec": float(raw.get("end_time", 0.0) or 0.0),
            }
        )

    transcript_excerpt = ""
    subtitle_url, subtitle_ext = _pick_subtitle_candidate(info, language=language)
    if subtitle_url and subtitle_ext == "vtt":
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(subtitle_url)
                if resp.status_code == 200:
                    transcript_excerpt = _parse_vtt_text(resp.text)
        except Exception:
            transcript_excerpt = ""

    title = str(info.get("title", "") or "").strip()
    description = str(info.get("description", "") or "").strip()
    channel = str(info.get("channel", "") or info.get("uploader", "") or "").strip()
    duration_sec = int(float(info.get("duration", 0) or 0))
    view_count = int(float(info.get("view_count", 0) or 0))
    like_count = int(float(info.get("like_count", 0) or 0))
    comment_count = int(float(info.get("comment_count", 0) or 0))
    tags = [str(tag).strip() for tag in list(info.get("tags") or []) if str(tag).strip()][:20]
    categories = [str(cat).strip() for cat in list(info.get("categories") or []) if str(cat).strip()][:8]
    upload_date = str(info.get("upload_date", "") or "").strip()
    public_summary_parts = [
        f"Title: {title}" if title else "",
        f"Channel: {channel}" if channel else "",
        f"Duration: {duration_sec}s" if duration_sec > 0 else "",
        f"Views: {view_count}" if view_count > 0 else "",
        f"Likes: {like_count}" if like_count > 0 else "",
        f"Comments: {comment_count}" if comment_count > 0 else "",
        f"Tags: {', '.join(tags[:8])}" if tags else "",
        f"Top chapters: {', '.join(ch['title'] for ch in chapters[:4] if ch.get('title'))}" if chapters else "",
        f"Transcript excerpt: {transcript_excerpt}" if transcript_excerpt else "",
    ]

    return {
        "source_url": normalized_url,
        "canonical_url": str(info.get("webpage_url", "") or normalized_url),
        "platform": str(info.get("extractor_key", "") or info.get("extractor", "") or "web").strip(),
        "title": title,
        "description": description,
        "channel": channel,
        "channel_url": str(info.get("channel_url", "") or "").strip(),
        "thumbnail_url": str(info.get("thumbnail", "") or "").strip(),
        "duration_sec": duration_sec,
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "upload_date": upload_date,
        "tags": tags,
        "categories": categories,
        "chapters": chapters,
        "transcript_excerpt": transcript_excerpt,
        "public_summary": " | ".join(part for part in public_summary_parts if part),
        "_yt_dlp_info": info,
    }


def _youtube_slugify_file_component(value: str, fallback: str = "item", max_len: int = 64) -> str:
    raw = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip()).strip("-").lower()
    raw = re.sub(r"-{2,}", "-", raw)
    raw = raw[:max_len].strip("-")
    return raw or fallback


def _youtube_ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


async def _download_youtube_video_for_reference_analysis(source_url: str, output_dir: Path) -> dict:
    normalized_url = _youtube_normalize_external_source_url(source_url)
    if not normalized_url:
        raise RuntimeError("Source URL missing for reference analysis")
    yt_dlp_bin = shutil.which("yt-dlp")
    if _youtube_ytdlp_module is None and not yt_dlp_bin:
        raise RuntimeError("yt-dlp is not available for reference analysis")

    output_dir.mkdir(parents=True, exist_ok=True)

    def _download() -> dict:
        info = {}
        extract_error = ""
        video_file = None
        download_error = ""
        try:
            info = _youtube_ytdlp_extract_info_blocking(normalized_url)
        except Exception as e:
            extract_error = str(e)
            download_error = extract_error
        client_sets = [
            ["android", "web"],
            ["tv_embedded", "android", "web"],
            ["ios", "android", "web"],
            ["mweb", "android", "web"],
        ]
        if yt_dlp_bin:
            for player_clients in client_sets:
                cmd = [
                    yt_dlp_bin,
                    "--no-warnings",
                    "--no-playlist",
                    "--merge-output-format",
                    "mp4",
                    "-f",
                    "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
                    "-o",
                    str(output_dir / "%(id)s.%(ext)s"),
                    "--extractor-args",
                    f"youtube:player_client={','.join(player_clients)}",
                    "--add-header",
                    (
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                    ),
                    normalized_url,
                ]
                try:
                    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=420)
                    if proc.returncode == 0:
                        if not info:
                            try:
                                info = _youtube_ytdlp_extract_info_blocking(normalized_url)
                            except Exception:
                                info = {}
                        video_id = str(info.get("id", "") or "").strip()
                        files = sorted(output_dir.glob(f"{video_id or '*'}*"), key=lambda p: p.stat().st_mtime, reverse=True)
                        video_file = next((p for p in files if p.suffix.lower() in {".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi"}), None)
                        if video_file:
                            download_error = ""
                            break
                    download_error = _clip_text(str(proc.stderr or proc.stdout or download_error), 220)
                except Exception as e:
                    download_error = str(e or download_error or extract_error)
                    continue
        if not video_file and _youtube_ytdlp_module is not None:
            for player_clients in client_sets:
                opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "noplaylist": True,
                    "merge_output_format": "mp4",
                    "format": "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
                    "outtmpl": str(output_dir / "%(id)s.%(ext)s"),
                    "extractor_args": {"youtube": {"player_client": player_clients}},
                    "http_headers": {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/135.0.0.0 Safari/537.36"
                        )
                    },
                }
                try:
                    with _youtube_ytdlp_module.YoutubeDL(opts) as ydl:
                        download_info = ydl.extract_info(normalized_url, download=True) or {}
                    if isinstance(download_info, dict) and download_info:
                        info = download_info
                    video_id = str(info.get("id", "") or "").strip()
                    files = sorted(output_dir.glob(f"{video_id}*"), key=lambda p: p.stat().st_mtime, reverse=True)
                    video_file = next((p for p in files if p.suffix.lower() in {".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi"}), None)
                    if video_file:
                        download_error = ""
                        break
                except Exception as e:
                    download_error = str(e or download_error or extract_error)
                    continue
        return {
            "info": info,
            "video_path": str(video_file) if video_file else "",
            "download_error": str(download_error or "").strip(),
        }

    return await asyncio.to_thread(_download)


def _pick_reference_stream_urls(info: dict | None) -> tuple[str, str]:
    payload = dict(info or {})
    requested_formats = [dict(v or {}) for v in list(payload.get("requested_formats") or []) if isinstance(v, dict)]
    video_url = ""
    audio_url = ""
    for fmt in requested_formats:
        vcodec = str(fmt.get("vcodec", "") or "").strip().lower()
        acodec = str(fmt.get("acodec", "") or "").strip().lower()
        url = str(fmt.get("url", "") or "").strip()
        if not url:
            continue
        if not video_url and vcodec and vcodec != "none":
            video_url = url
        if not audio_url and acodec and acodec != "none":
            audio_url = url
    if video_url:
        return video_url, audio_url

    formats = [dict(v or {}) for v in list(payload.get("formats") or []) if isinstance(v, dict)]
    ranked_video = sorted(
        [
            fmt
            for fmt in formats
            if str(fmt.get("url", "") or "").strip()
            and str(fmt.get("vcodec", "") or "").strip().lower() not in {"", "none"}
        ],
        key=lambda fmt: (
            abs(int(float(fmt.get("height", 720) or 720)) - 720),
            int(float(fmt.get("tbr", 0.0) or 0.0)),
        ),
    )
    ranked_audio = sorted(
        [
            fmt
            for fmt in formats
            if str(fmt.get("url", "") or "").strip()
            and str(fmt.get("acodec", "") or "").strip().lower() not in {"", "none"}
            and str(fmt.get("vcodec", "") or "").strip().lower() in {"", "none"}
        ],
        key=lambda fmt: -int(float(fmt.get("abr", fmt.get("tbr", 0.0)) or 0.0)),
    )
    video_url = str((ranked_video[0] if ranked_video else {}).get("url", "") or "").strip()
    audio_url = str((ranked_audio[0] if ranked_audio else {}).get("url", "") or "").strip()
    return video_url, audio_url


async def _extract_reference_video_stream_clip(
    info: dict | None,
    output_dir: Path,
    *,
    max_seconds: float = 180.0,
) -> dict:
    payload = dict(info or {})
    if not payload:
        return {"video_path": "", "mode": "stream_clip", "error": "missing_stream_info"}
    if not _youtube_ffmpeg_available():
        return {"video_path": "", "mode": "stream_clip", "error": "ffmpeg_unavailable"}

    video_url, audio_url = _pick_reference_stream_urls(payload)
    if not video_url:
        return {"video_path": "", "mode": "stream_clip", "error": "missing_video_stream_url"}

    output_dir.mkdir(parents=True, exist_ok=True)
    video_id = _youtube_slugify_file_component(str(payload.get("id", "") or "reference"), fallback="reference", max_len=40)
    clip_path = output_dir / f"{video_id}_stream_clip.mp4"
    seconds = max(20.0, min(float(max_seconds or 180.0), 300.0))
    cmd = ["ffmpeg", "-y", "-t", f"{seconds:.2f}", "-i", video_url]
    if audio_url:
        cmd += ["-i", audio_url, "-map", "0:v:0", "-map", "1:a:0"]
    cmd += ["-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", "-shortest", str(clip_path)]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _stdout, stderr = await proc.communicate()
    error_text = _clip_text(stderr.decode(errors="ignore"), 420)
    if proc.returncode != 0 or not clip_path.exists() or clip_path.stat().st_size <= 0:
        try:
            clip_path.unlink(missing_ok=True)
        except Exception:
            pass
        return {"video_path": "", "mode": "stream_clip", "error": error_text or "ffmpeg_stream_clip_failed"}
    return {"video_path": str(clip_path), "mode": "stream_clip", "error": ""}


# ─── YouTube Video Upload (Resumable) ────────────────────────────────────────

async def youtube_upload_video(
    access_token: str,
    video_path: str,
    title: str,
    description: str = "",
    tags: list[str] | None = None,
    privacy: str = "private",
    category_id: str = "27",
    thumbnail_path: str | None = None,
) -> dict:
    """Upload a video to YouTube using resumable upload protocol.

    Returns {"video_id": "...", "video_url": "https://youtu.be/..."} on success.
    """
    video_file = Path(video_path)
    if not video_file.exists() or video_file.stat().st_size == 0:
        raise ValueError(f"Video file not found or empty: {video_path}")

    file_size = video_file.stat().st_size
    metadata = {
        "snippet": {
            "title": str(title or "Untitled")[:100],
            "description": str(description or "")[:5000],
            "tags": list(tags or [])[:30],
            "categoryId": str(category_id),
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }

    log.info("YouTube upload starting: %s (%.1f MB, privacy=%s)", title[:50], file_size / 1e6, privacy)

    # Step 1: Initiate resumable upload
    async with httpx.AsyncClient(timeout=30) as client:
        init_resp = await client.post(
            "https://www.googleapis.com/upload/youtube/v3/videos"
            "?uploadType=resumable&part=snippet,status",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Length": str(file_size),
                "X-Upload-Content-Type": "video/*",
            },
            content=json.dumps(metadata),
        )
        if init_resp.status_code not in {200, 308}:
            raise RuntimeError(f"YouTube upload init failed ({init_resp.status_code}): {init_resp.text[:300]}")

        upload_url = init_resp.headers.get("Location")
        if not upload_url:
            raise RuntimeError("YouTube upload init returned no Location header")

    # Step 2: Upload video in 5MB chunks
    chunk_size = 5 * 1024 * 1024  # 5MB
    uploaded = 0
    video_id = None

    async with httpx.AsyncClient(timeout=120) as client:
        with open(video_path, "rb") as f:
            while uploaded < file_size:
                chunk = f.read(chunk_size)
                end = uploaded + len(chunk) - 1
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {uploaded}-{end}/{file_size}",
                    "Content-Type": "video/*",
                }
                resp = await client.put(upload_url, headers=headers, content=chunk)

                if resp.status_code == 200 or resp.status_code == 201:
                    # Upload complete
                    result = resp.json()
                    video_id = result.get("id", "")
                    log.info("YouTube upload complete: video_id=%s", video_id)
                    break
                elif resp.status_code == 308:
                    # Chunk accepted, continue
                    uploaded += len(chunk)
                    pct = int(uploaded / file_size * 100)
                    if pct % 20 == 0:
                        log.info("YouTube upload progress: %d%%", pct)
                else:
                    raise RuntimeError(
                        f"YouTube upload chunk failed ({resp.status_code}): {resp.text[:300]}"
                    )

    if not video_id:
        raise RuntimeError("YouTube upload completed but no video_id returned")

    # Step 3: Set custom thumbnail if provided
    if thumbnail_path and Path(thumbnail_path).exists():
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                with open(thumbnail_path, "rb") as thumb_f:
                    thumb_resp = await client.post(
                        f"https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
                        f"?videoId={video_id}",
                        headers={
                            "Authorization": f"Bearer {access_token}",
                            "Content-Type": "image/png",
                        },
                        content=thumb_f.read(),
                    )
                    if thumb_resp.status_code == 200:
                        log.info("YouTube thumbnail set for video %s", video_id)
                    else:
                        log.warning("YouTube thumbnail upload failed: %s", thumb_resp.text[:200])
        except Exception as thumb_exc:
            log.warning("YouTube thumbnail upload error: %s", str(thumb_exc)[:200])

    return {
        "video_id": video_id,
        "video_url": f"https://youtu.be/{video_id}",
    }


async def youtube_get_latest_video_velocity(
    access_token: str,
    channel_id: str,
) -> dict:
    """Get the latest video's view velocity (views per hour since upload).

    Returns {"video_id", "title", "views", "hours_since_upload", "velocity_vph", "is_decaying"}.
    """
    async with httpx.AsyncClient(timeout=20) as client:
        # Get latest video from channel
        search_resp = await client.get(
            f"{YOUTUBE_DATA_API_BASE}/search",
            params={
                "part": "snippet",
                "channelId": channel_id,
                "order": "date",
                "maxResults": "1",
                "type": "video",
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        search_resp.raise_for_status()
        items = search_resp.json().get("items", [])
        if not items:
            return {"video_id": "", "velocity_vph": 0, "is_decaying": True, "error": "no_videos"}

        video_id = items[0]["id"].get("videoId", "")
        title = items[0]["snippet"].get("title", "")
        published = items[0]["snippet"].get("publishedAt", "")

        # Get view count
        stats_resp = await client.get(
            f"{YOUTUBE_DATA_API_BASE}/videos",
            params={"part": "statistics", "id": video_id},
            headers={"Authorization": f"Bearer {access_token}"},
        )
        stats_resp.raise_for_status()
        stats_items = stats_resp.json().get("items", [])
        views = int(stats_items[0]["statistics"].get("viewCount", 0)) if stats_items else 0

        # Calculate velocity
        try:
            pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            hours = max(1, (datetime.now(timezone.utc) - pub_dt).total_seconds() / 3600)
        except Exception:
            hours = 24

        velocity = round(views / hours, 2)
        # Decay threshold: <50 views/hour after first 24 hours
        is_decaying = hours >= 24 and velocity < 50

        return {
            "video_id": video_id,
            "title": title,
            "views": views,
            "hours_since_upload": round(hours, 1),
            "velocity_vph": velocity,
            "is_decaying": is_decaying,
        }


async def youtube_fetch_niche_trending_signals(
    niche_keywords: list[str],
    max_results: int = 10,
) -> list[dict]:
    """Fetch trending videos for niche keywords from YouTube public API. No auth needed."""
    if not niche_keywords:
        return []
    trending: list[dict] = []
    try:
        query = " | ".join(niche_keywords[:3])
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{YOUTUBE_DATA_API_BASE}/search",
                params={
                    "part": "snippet",
                    "q": query,
                    "order": "date",
                    "maxResults": str(max_results),
                    "type": "video",
                    "publishedAfter": (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "relevanceLanguage": "en",
                    "key": _youtube_public_api_key_candidates()[0] if _youtube_public_api_key_candidates() else "",
                },
            )
            if resp.status_code != 200:
                log.warning("YouTube trending search failed: %s", resp.status_code)
                return []
            items = resp.json().get("items", [])
            for item in items:
                snippet = item.get("snippet", {})
                trending.append({
                    "title": str(snippet.get("title", "") or ""),
                    "channel": str(snippet.get("channelTitle", "") or ""),
                    "published": str(snippet.get("publishedAt", "") or ""),
                    "description": str(snippet.get("description", "") or "")[:200],
                })
    except Exception as e:
        log.warning("YouTube trending signals fetch failed: %s", str(e)[:200])
    return trending


def score_topic_opportunity(
    candidate_topic: str,
    channel_titles: list[str],
    trending_titles: list[str],
    niche_keywords: list[str],
) -> dict:
    """Score a candidate topic by niche fit, gap, and trend momentum."""
    candidate_lower = str(candidate_topic or "").lower()

    # Niche alignment: how many niche keywords appear in the topic
    niche_hits = sum(1 for kw in niche_keywords if kw.lower() in candidate_lower)
    niche_score = min(1.0, niche_hits / max(1, len(niche_keywords))) if niche_keywords else 0.5

    # Gap score: has channel already covered this? (lower = already covered)
    channel_overlap = 0
    for title in channel_titles:
        common_words = set(candidate_lower.split()) & set(str(title).lower().split())
        if len(common_words) >= 3:
            channel_overlap += 1
    gap_score = max(0.0, 1.0 - (channel_overlap / max(1, len(channel_titles))))

    # Trend momentum: how many trending videos have similar keywords
    trend_hits = 0
    for title in trending_titles:
        common_words = set(candidate_lower.split()) & set(str(title).lower().split())
        if len(common_words) >= 2:
            trend_hits += 1
    trend_score = min(1.0, trend_hits / max(1, min(5, len(trending_titles))))

    # Composite: weighted blend
    composite = (niche_score * 0.3) + (gap_score * 0.4) + (trend_score * 0.3)

    return {
        "topic": candidate_topic,
        "niche_score": round(niche_score, 3),
        "gap_score": round(gap_score, 3),
        "trend_score": round(trend_score, 3),
        "composite_score": round(composite, 3),
    }


def _analyze_optimal_upload_window(
    upload_history: list[dict],
) -> dict:
    """Analyze historical uploads to find optimal posting window.

    Each entry: {"published_at": ISO str, "first_24h_views": int, "velocity_vph": float}
    Returns: {"best_day": "Tuesday", "best_hour": 14, "confidence": 0.6}
    """
    if len(upload_history) < 3:
        # Not enough data — use industry defaults
        return {"best_day": "Tuesday", "best_hour": 14, "confidence": 0.0, "source": "industry_default"}

    day_scores: dict[str, list[float]] = {}
    hour_scores: dict[int, list[float]] = {}

    for entry in upload_history:
        try:
            pub = str(entry.get("published_at", "") or "")
            if not pub:
                continue
            dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            day_name = dt.strftime("%A")
            hour = dt.hour
            velocity = float(entry.get("velocity_vph", 0) or 0)
            if velocity > 0:
                day_scores.setdefault(day_name, []).append(velocity)
                hour_scores.setdefault(hour, []).append(velocity)
        except Exception:
            continue

    if not day_scores or not hour_scores:
        return {"best_day": "Tuesday", "best_hour": 14, "confidence": 0.0, "source": "industry_default"}

    best_day = max(day_scores.items(), key=lambda x: sum(x[1]) / len(x[1]))[0]
    best_hour = max(hour_scores.items(), key=lambda x: sum(x[1]) / len(x[1]))[0]
    confidence = min(1.0, len(upload_history) / 15.0)

    return {
        "best_day": best_day,
        "best_hour": best_hour,
        "confidence": round(confidence, 2),
        "source": "channel_data",
        "sample_size": len(upload_history),
    }
