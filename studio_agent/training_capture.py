"""Consent-gated, append-only capture and dataset compilation for Studio.

Operational telemetry remains separate. This module records complete training
lineage only for users who explicitly opted in. Google/YouTube-authorized data
is quarantined and never emitted into general training datasets.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
APP_DATA = Path(os.getenv("APP_DATA_DIR", str(ROOT / "data"))).expanduser()
CAPTURE_ROOT = Path(os.getenv("STUDIO_TRAINING_CAPTURE_DIR", str(APP_DATA / "training_capture")))
CONSENT_DIR = CAPTURE_ROOT / "consent"
OUTBOX_DIR = CAPTURE_ROOT / "outbox"
DATASET_DIR = CAPTURE_ROOT / "datasets"
DELETION_DIR = CAPTURE_ROOT / "deletions"
COMPILER_STATE = CAPTURE_ROOT / "compiler_state.json"

CONSENT_VERSION = os.getenv("STUDIO_TRAINING_CONSENT_VERSION", "2026-06-v1")
COMPILER_INTERVAL_SEC = max(300, int(os.getenv("STUDIO_TRAINING_COMPILER_INTERVAL_SEC", "21600")))

_lock = threading.RLock()
_compiler_task: asyncio.Task | None = None

_SECRET_KEYS = frozenset({
    "access_token", "refresh_token", "authorization", "password", "secret",
    "api_key", "apikey", "service_key", "client_secret", "stripe_secret",
})
_PII_KEYS = frozenset({"email", "phone", "ip", "ip_address", "user_agent"})
_YOUTUBE_MARKERS = (
    "youtube_analytics_live",
    "youtube_data_v3",
    "youtube_analytics_reporting",
    "yt-analytics.readonly",
    "youtube_channel_connections",
)


def _safe_user_id(user_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(user_id or "").strip())[:128]


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex[:8]}.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def _consent_path(user_id: str) -> Path:
    return CONSENT_DIR / f"{_safe_user_id(user_id)}.json"


def get_consent(user_id: str) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    default = {
        "user_id": uid,
        "training_opt_in": False,
        "human_review_opt_in": False,
        "include_prompts": False,
        "include_uploads": False,
        "include_outputs": False,
        "include_feedback": False,
        "consent_version": CONSENT_VERSION,
        "consented_at": None,
        "revoked_at": None,
        "updated_at": None,
    }
    path = _consent_path(uid)
    if not uid or not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return {**default, **(data if isinstance(data, dict) else {})}


def _supabase_headers() -> dict[str, str]:
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


def _supabase_sync_enabled() -> bool:
    return os.getenv("STUDIO_TRAINING_SUPABASE_SYNC_ENABLED", "false").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _sync_consent_supabase(payload: dict[str, Any]) -> None:
    if not _supabase_sync_enabled():
        return
    url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        return
    try:
        with httpx.Client(timeout=8.0) as client:
            client.post(
                f"{url}/rest/v1/training_consents?on_conflict=user_id",
                headers=_supabase_headers(),
                json=payload,
            )
    except Exception:
        pass


def _sync_event_supabase(row: dict[str, Any]) -> None:
    if not _supabase_sync_enabled():
        return
    url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        return
    payload = {
        "event_id": row.get("event_id"),
        "user_id": row.get("user_id"),
        "session_id": row.get("session_id") or None,
        "turn_id": row.get("turn_id") or None,
        "event_type": row.get("event_type"),
        "consent_version": row.get("consent_version"),
        "trainable": bool(row.get("trainable")),
        "youtube_authorized_data": bool(row.get("youtube_authorized_data")),
        "payload": row.get("payload") or {},
        "lineage": row.get("lineage") or {},
    }
    try:
        with httpx.Client(timeout=8.0) as client:
            client.post(
                f"{url}/rest/v1/training_event_outbox?on_conflict=event_id",
                headers=_supabase_headers(),
                json=payload,
            )
    except Exception:
        pass


def set_consent(
    user_id: str,
    *,
    training_opt_in: bool,
    human_review_opt_in: bool = False,
    include_prompts: bool = True,
    include_uploads: bool = True,
    include_outputs: bool = True,
    include_feedback: bool = True,
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        raise ValueError("user_id required")
    now = time.time()
    prior = get_consent(uid)
    enabled = bool(training_opt_in)
    payload = {
        **prior,
        "user_id": uid,
        "training_opt_in": enabled,
        "human_review_opt_in": bool(human_review_opt_in and enabled),
        "include_prompts": bool(include_prompts and enabled),
        "include_uploads": bool(include_uploads and enabled),
        "include_outputs": bool(include_outputs and enabled),
        "include_feedback": bool(include_feedback and enabled),
        "consent_version": CONSENT_VERSION,
        "consented_at": prior.get("consented_at") or now if enabled else prior.get("consented_at"),
        "revoked_at": None if enabled else now,
        "updated_at": now,
    }
    with _lock:
        _atomic_write_json(_consent_path(uid), payload)
    _sync_consent_supabase(payload)
    return payload


def _redact(value: Any, *, key: str = "") -> Any:
    low_key = str(key or "").lower()
    if low_key in _SECRET_KEYS:
        return "[REDACTED_SECRET]"
    if low_key in _PII_KEYS:
        return "[REDACTED_PII]"
    if isinstance(value, dict):
        return {str(k): _redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(v) for v in value]
    if isinstance(value, str):
        text = value
        if text.startswith("data:") and "," in text:
            header, encoded = text.split(",", 1)
            digest = hashlib.sha256(encoded.encode("utf-8", errors="ignore")).hexdigest()[:20]
            return f"[REDACTED_DATA_URL type={header[:80]} sha256={digest}]"
        text = re.sub(r"Bearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", text, flags=re.I)
        text = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[REDACTED_EMAIL]", text)
        return text
    return value


def _contains_youtube_authorized_data(value: Any) -> bool:
    try:
        blob = json.dumps(value, ensure_ascii=False).lower()
    except Exception:
        blob = str(value or "").lower()
    return any(marker in blob for marker in _YOUTUBE_MARKERS)


def capture_event(
    user_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    session_id: str = "",
    turn_id: str = "",
    lineage: dict[str, Any] | None = None,
    contains_youtube_authorized_data: bool | None = None,
) -> str:
    consent = get_consent(user_id)
    if not consent.get("training_opt_in"):
        return ""
    category_flag = {
        "user_turn": "include_prompts",
        "model_request": "include_prompts",
        "attachment": "include_uploads",
        "artifact": "include_outputs",
        "assistant_turn": "include_outputs",
        "model_response": "include_outputs",
        "tool_call": "include_outputs",
        "production_feedback": "include_feedback",
    }.get(str(event_type or ""))
    if category_flag and not consent.get(category_flag):
        return ""
    raw_payload = payload or {}
    youtube_quarantine = (
        _contains_youtube_authorized_data(raw_payload)
        if contains_youtube_authorized_data is None
        else bool(contains_youtube_authorized_data)
    )
    event_id = f"te_{uuid.uuid4().hex}"
    row = {
        "schema_version": 1,
        "event_id": event_id,
        "created_at": time.time(),
        "event_type": str(event_type or "unknown")[:80],
        "user_id": str(user_id),
        "session_id": str(session_id or ""),
        "turn_id": str(turn_id or ""),
        "consent_version": str(consent.get("consent_version") or CONSENT_VERSION),
        "human_review_allowed": bool(consent.get("human_review_opt_in")),
        "youtube_authorized_data": youtube_quarantine,
        "trainable": not youtube_quarantine,
        "lineage": _redact(lineage or {}),
        "payload": _redact(raw_payload),
    }
    path = OUTBOX_DIR / _safe_user_id(user_id) / f"{event_id}.json"
    with _lock:
        _atomic_write_json(path, row)
    _sync_event_supabase(row)
    return event_id


def artifact_manifest(path_value: str, *, role: str, parent_event_id: str = "") -> dict[str, Any]:
    path = Path(str(path_value or "")).expanduser()
    out = {
        "role": str(role or "artifact"),
        "path": str(path),
        "exists": path.is_file(),
        "size": 0,
        "sha256": "",
        "parent_event_id": parent_event_id,
    }
    if path.is_file():
        try:
            out["size"] = path.stat().st_size
            h = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    h.update(chunk)
            out["sha256"] = h.hexdigest()
        except OSError:
            pass
    return out


def _load_compiler_state() -> dict[str, Any]:
    if not COMPILER_STATE.exists():
        return {"processed": []}
    try:
        data = json.loads(COMPILER_STATE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"processed": []}
    return data if isinstance(data, dict) else {"processed": []}


def compile_dataset() -> dict[str, Any]:
    """Compile newly captured, consented, non-YouTube events into JSONL."""
    state = _load_compiler_state()
    processed = set(str(v) for v in state.get("processed") or [])
    rows: list[dict[str, Any]] = []
    quarantined = 0
    for path in sorted(OUTBOX_DIR.glob("*/*.json")):
        if path.stem in processed:
            continue
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        consent = get_consent(str(row.get("user_id") or ""))
        if not consent.get("training_opt_in") or consent.get("consent_version") != row.get("consent_version"):
            processed.add(path.stem)
            continue
        if row.get("youtube_authorized_data"):
            quarantined += 1
            processed.add(path.stem)
            continue
        rows.append(row)
        processed.add(path.stem)
    if rows:
        DATASET_DIR.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        output = DATASET_DIR / f"studio-training-events-{stamp}.jsonl"
        with output.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        manifest = {
            "dataset_file": str(output),
            "created_at": time.time(),
            "schema_version": 1,
            "row_count": len(rows),
            "youtube_rows_quarantined": quarantined,
            "consent_version": CONSENT_VERSION,
        }
        _atomic_write_json(output.with_suffix(".manifest.json"), manifest)
    state = {
        "processed": sorted(processed)[-500000:],
        "updated_at": time.time(),
        "last_compiled_rows": len(rows),
        "last_quarantined_rows": quarantined,
    }
    _atomic_write_json(COMPILER_STATE, state)
    return state


def delete_user_training_data(user_id: str) -> dict[str, Any]:
    uid = _safe_user_id(user_id)
    deleted = 0
    with _lock:
        for path in OUTBOX_DIR.glob(f"{uid}/*.json"):
            path.unlink(missing_ok=True)
            deleted += 1
        shutil.rmtree(OUTBOX_DIR / uid, ignore_errors=True)
        for dataset in DATASET_DIR.glob("*.jsonl"):
            kept: list[str] = []
            changed = False
            try:
                for line in dataset.read_text(encoding="utf-8").splitlines():
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if str(row.get("user_id") or "") == str(user_id):
                        changed = True
                        deleted += 1
                    else:
                        kept.append(json.dumps(row, ensure_ascii=False))
                if changed:
                    dataset.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
            except OSError:
                continue
        deletion = {
            "user_id": str(user_id),
            "deleted_at": time.time(),
            "deleted_rows": deleted,
            "reason": "user_request",
        }
        _atomic_write_json(DELETION_DIR / f"{uid}-{int(time.time())}.json", deletion)
    return deletion


async def _compiler_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(compile_dataset)
        except Exception:
            pass
        await asyncio.sleep(COMPILER_INTERVAL_SEC)


def start_compiler_loop() -> None:
    global _compiler_task
    if _compiler_task is None or _compiler_task.done():
        _compiler_task = asyncio.create_task(_compiler_loop())


def stop_compiler_loop() -> None:
    global _compiler_task
    if _compiler_task is not None and not _compiler_task.done():
        _compiler_task.cancel()
    _compiler_task = None
