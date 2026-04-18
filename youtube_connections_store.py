"""Supabase-backed persistence for YouTube OAuth channel connections.

Bridges the in-memory dict structure that youtube.py uses
(`{user_id: {default_channel_id, channels: {channel_id: record}}}`) with a
flat Supabase table (`youtube_channel_connections`) keyed by
`(user_id, channel_id)`.

Why this exists: RunPod workers' filesystems are ephemeral. Every template
cycle wiped `$TEMP_DIR/youtube_connections.json` and forced every user to
reconnect YouTube. Supabase is authoritative; the disk file is a per-worker
hot cache.

Public API:
    configured() -> bool
    hydrate() -> dict   # pull all rows into the in-memory dict shape
    upsert(user_id, channel_id, record, is_default=False) -> None
    delete(user_id, channel_id) -> None

All functions are sync (the existing load/save in youtube.py is sync too).
Network timeouts are short; on any failure we log and fall back to disk-only.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx

log = logging.getLogger(__name__)

_SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
_SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "").strip() or os.getenv("SUPABASE_ANON_KEY", "").strip()
_TABLE = "youtube_channel_connections"
_REQUEST_TIMEOUT = 10.0

# Columns explicitly modeled in the table; anything else on the record goes
# into the `extra` jsonb column. Keep in sync with migrations/*.sql.
_KNOWN_COLUMNS = {
    "access_token",
    "refresh_token",
    "token_expires_at",
    "token_scope",
    "oauth_mode",
    "oauth_source",
    "linked_at",
    "last_synced_at",
}


def configured() -> bool:
    return bool(_SUPABASE_URL and _SUPABASE_SERVICE_KEY)


def _headers() -> dict[str, str]:
    return {
        "apikey": _SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {_SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def _split_record(record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split a record into (known columns, extra jsonb)."""
    known: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    for key, value in record.items():
        # user_id and channel_id are on the row itself, not "extra"
        if key in ("user_id", "channel_id"):
            continue
        if key in _KNOWN_COLUMNS:
            known[key] = value
        else:
            extra[key] = value
    return known, extra


def _merge_row_to_record(row: dict[str, Any]) -> dict[str, Any]:
    """Turn a Supabase row back into the in-memory record shape."""
    record: dict[str, Any] = {}
    for col in _KNOWN_COLUMNS:
        if col in row and row[col] is not None:
            record[col] = row[col]
    extra = row.get("extra")
    if isinstance(extra, dict):
        for key, value in extra.items():
            record.setdefault(key, value)
    record["user_id"] = row.get("user_id", "")
    record["channel_id"] = row.get("channel_id", "")
    return record


def hydrate() -> dict[str, Any]:
    """Fetch all rows and return them in the canonical
    `{user_id: {default_channel_id, channels: {...}}}` shape.

    Returns {} on any Supabase error (caller should fall back to disk).
    """
    if not configured():
        return {}
    try:
        with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
            resp = client.get(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers=_headers(),
                params={"select": "*"},
            )
            resp.raise_for_status()
            rows = resp.json()
    except Exception as exc:
        log.warning("supabase hydrate failed: %s", str(exc)[:200])
        return {}

    out: dict[str, Any] = {}
    for row in rows or []:
        user_id = str(row.get("user_id", "") or "")
        channel_id = str(row.get("channel_id", "") or "")
        if not user_id or not channel_id:
            continue
        bucket = out.setdefault(user_id, {"default_channel_id": "", "channels": {}})
        bucket["channels"][channel_id] = _merge_row_to_record(row)
        if bool(row.get("is_default")):
            bucket["default_channel_id"] = channel_id
    return out


def upsert(user_id: str, channel_id: str, record: dict[str, Any], is_default: bool = False) -> bool:
    """Upsert a (user_id, channel_id) row. Returns True on success, False on
    any Supabase error (caller keeps the disk save as its fallback)."""
    if not configured():
        return False
    user_id = str(user_id or "").strip()
    channel_id = str(channel_id or "").strip()
    if not user_id or not channel_id:
        return False
    known, extra = _split_record(record or {})
    payload = {
        "user_id": user_id,
        "channel_id": channel_id,
        "is_default": bool(is_default),
        "extra": extra or None,
        **known,
    }
    try:
        with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
            resp = client.post(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers={**_headers(), "Prefer": "resolution=merge-duplicates"},
                json=payload,
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(
                    "supabase upsert yt_channel (%s / %s) status=%d body=%s",
                    user_id[:12], channel_id, resp.status_code, resp.text[:200],
                )
                return False
    except Exception as exc:
        log.warning("supabase upsert failed: %s", str(exc)[:200])
        return False
    return True


def clear_default_except(user_id: str, keep_channel_id: str) -> bool:
    """Set is_default=false on every channel for this user except the named one.

    Called when the in-memory dict promotes a different channel to default
    so Supabase stays consistent.
    """
    if not configured():
        return False
    user_id = str(user_id or "").strip()
    keep_channel_id = str(keep_channel_id or "").strip()
    if not user_id:
        return False
    try:
        with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
            resp = client.patch(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers=_headers(),
                params={
                    "user_id": f"eq.{user_id}",
                    "channel_id": f"neq.{keep_channel_id}",
                },
                json={"is_default": False},
            )
            if resp.status_code not in (200, 204):
                log.warning("supabase clear_default status=%d", resp.status_code)
                return False
    except Exception as exc:
        log.warning("supabase clear_default failed: %s", str(exc)[:200])
        return False
    return True


def delete(user_id: str, channel_id: str) -> bool:
    if not configured():
        return False
    user_id = str(user_id or "").strip()
    channel_id = str(channel_id or "").strip()
    if not user_id or not channel_id:
        return False
    try:
        with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
            resp = client.delete(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers=_headers(),
                params={
                    "user_id": f"eq.{user_id}",
                    "channel_id": f"eq.{channel_id}",
                },
            )
            if resp.status_code not in (200, 204):
                return False
    except Exception as exc:
        log.warning("supabase delete failed: %s", str(exc)[:200])
        return False
    return True
