"""Archived signed RunPod contract retained for legacy receipt verification.

Execution is permanently retired: both effective feature flags return false,
and every live bridge/worker entrypoint calls the unconditional retirement
guard. Signing and normalization remain so historical receipts can still be
validated without reviving a second production plane.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
from typing import Any


RUNPOD_ENVELOPE_SCHEMA = "nyptid.studio.production.v1"
RUNPOD_AUTH_ALGORITHM = "hmac-sha256"
RUNPOD_AUTH_KEY_ID = "studio-runpod-v1"
RUNPOD_DISPATCH_SECRET_ENV = "RUNPOD_DISPATCH_SECRET"
RUNPOD_PRODUCTION_ENABLED_ENV = "STUDIO_RUNPOD_PRODUCTION_ENABLED"
RUNPOD_LONGFORM_ENABLED_ENV = "STUDIO_RUNPOD_LONGFORM_ENABLED"
RUNPOD_ENABLED_VALUES = frozenset({"1", "true", "yes", "on", "enabled"})
RUNPOD_RETIREMENT_CODE = "runpod_retired"
RUNPOD_RETIREMENT_MESSAGE = (
    "RunPod execution is permanently retired; Studio production is Contabo-owned."
)

# Deliberately excludes chat, planning, reads, polling/status, arbitrary HTTP,
# project-file writes, and shell/build execution.
RUNPOD_PRODUCTION_TOOL_ALLOWLIST = frozenset(
    {
        "start_shortform_generate",
        "expand_visual_proof_shortform",
        "start_longform_render",
        "expand_longform_visual_proof",
        "finalize_longform_render",
        "edit_production_scene_still",
        "edit_production_scenes_still",
        "regenerate_production_scene_still",
        "regenerate_production_scene",
        "animate_production_scenes",
        "repair_production_scene_animation",
        "finalize_production",
        "re_edit_production",
        "regenerate_longform_still",
    }
)

_ENVELOPE_KEYS = frozenset(
    {"schema", "dispatch_id", "command_id", "tool", "arguments", "context", "auth"}
)
_CONTEXT_KEYS = frozenset({"user_id", "session_id", "content_format"})
_AUTH_KEYS = frozenset({"algorithm", "key_id", "signature"})
_VOLATILE_ARGUMENT_KEYS = frozenset(
    {
        "dispatch_id",
        "runpod_job_id",
        "idempotency_key",
        "queued_at",
        "started_at",
        "requested_at",
    }
)
_DISPATCH_ID_RE = re.compile(r"^rpd_[0-9a-f]{40}$")
_SIGNATURE_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_CANONICAL_BYTES = 2_000_000
_MAX_DEPTH = 32


class RunPodContractError(ValueError):
    """A stable, non-secret contract rejection."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = str(code or "invalid_envelope")


def runpod_production_enabled() -> bool:
    """Return the effective production policy.

    The historical environment variable is intentionally ignored. A saved key
    or stale deployment flag must never recreate a second execution plane.
    """

    return False


def runpod_longform_enabled() -> bool:
    """Return the permanently disabled long-form execution policy."""

    return False


def assert_runpod_execution_retired() -> None:
    """Fail before credentials, storage, or network can be consulted."""

    raise RunPodContractError(RUNPOD_RETIREMENT_CODE, RUNPOD_RETIREMENT_MESSAGE)


def _canonical_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RunPodContractError("not_json_safe", "Envelope contains a non-JSON value") from exc
    if len(encoded) > _MAX_CANONICAL_BYTES:
        raise RunPodContractError("envelope_too_large", "Production envelope exceeds 2 MB")
    return encoded


def _is_orderless_index_key(key: str) -> bool:
    normalized = str(key or "").strip().lower()
    return normalized == "scene_indices" or normalized.endswith("_scene_indices") or normalized in {
        "beat_indices",
        "selected_indices",
    }


def _normalize_json(
    value: Any,
    *,
    semantic: bool,
    depth: int,
    top_level_arguments: bool = False,
    parent_key: str = "",
) -> Any:
    if depth > _MAX_DEPTH:
        raise RunPodContractError("too_deep", "Production arguments are nested too deeply")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RunPodContractError("not_json_safe", "Production arguments contain a non-finite number")
        return value
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise RunPodContractError("invalid_key", "Production argument keys must be strings")
            key = raw_key
            if semantic and top_level_arguments and (
                key.startswith("_") or key in _VOLATILE_ARGUMENT_KEYS
            ):
                continue
            normalized[key] = _normalize_json(
                raw_value,
                semantic=semantic,
                depth=depth + 1,
                parent_key=key,
            )
        return normalized
    if isinstance(value, (list, tuple)):
        rows = [
            _normalize_json(row, semantic=semantic, depth=depth + 1, parent_key=parent_key)
            for row in value
        ]
        if semantic and _is_orderless_index_key(parent_key) and all(
            isinstance(row, int) and not isinstance(row, bool) for row in rows
        ):
            return sorted(set(rows))
        return rows
    raise RunPodContractError("not_json_safe", "Production arguments contain a non-JSON value")


def normalize_tool_arguments(arguments: Any, *, semantic: bool = False) -> dict[str, Any]:
    """Return JSON-safe tool arguments, optionally removing dispatch-only noise."""

    if not isinstance(arguments, dict):
        raise RunPodContractError("invalid_arguments", "Production arguments must be an object")
    normalized = _normalize_json(
        arguments,
        semantic=bool(semantic),
        depth=0,
        top_level_arguments=True,
    )
    _canonical_bytes(normalized)
    return normalized


def _required_text(value: Any, field: str, *, allow_empty: bool = False, limit: int = 512) -> str:
    if not isinstance(value, str):
        raise RunPodContractError("invalid_field", f"{field} must be a string")
    text = value.strip()
    if not allow_empty and not text:
        raise RunPodContractError("missing_field", f"{field} is required")
    if len(text) > limit:
        raise RunPodContractError("invalid_field", f"{field} is too long")
    return text


def _dispatch_secret(secret: str | None) -> bytes:
    value = str(secret if secret is not None else os.getenv(RUNPOD_DISPATCH_SECRET_ENV, "")).strip()
    if len(value.encode("utf-8")) < 32:
        raise RunPodContractError(
            "missing_signing_secret",
            f"{RUNPOD_DISPATCH_SECRET_ENV} must contain at least 32 UTF-8 bytes",
        )
    return value.encode("utf-8")


def semantic_dispatch_id(
    tool: str,
    arguments: Any,
    *,
    command_id: str = "",
    user_id: str = "",
) -> str:
    """Stable id for one semantic production request (runtime metadata excluded)."""

    name = _required_text(tool, "tool", limit=128)
    if name not in RUNPOD_PRODUCTION_TOOL_ALLOWLIST:
        raise RunPodContractError("tool_not_allowed", "Only allowlisted production tools can use RunPod")
    canonical = {
        "schema": RUNPOD_ENVELOPE_SCHEMA,
        "command_id": _required_text(command_id, "command_id", allow_empty=True),
        "tool": name,
        "user_id": _required_text(user_id, "user_id", limit=256),
        "arguments": normalize_tool_arguments(arguments, semantic=True),
    }
    return "rpd_" + hashlib.sha256(_canonical_bytes(canonical)).hexdigest()[:40]


def build_signed_envelope(
    tool: str,
    arguments: Any,
    *,
    command_id: str = "",
    user_id: str = "",
    session_id: str = "",
    content_format: str = "",
    secret: str | None = None,
) -> dict[str, Any]:
    """Build the only payload accepted by the production RunPod worker."""

    name = _required_text(tool, "tool", limit=128)
    full_arguments = normalize_tool_arguments(arguments, semantic=False)
    uid = _required_text(user_id, "user_id", limit=256)
    command = _required_text(command_id, "command_id", allow_empty=True)
    envelope: dict[str, Any] = {
        "schema": RUNPOD_ENVELOPE_SCHEMA,
        "dispatch_id": semantic_dispatch_id(
            name,
            full_arguments,
            command_id=command,
            user_id=uid,
        ),
        "command_id": command,
        "tool": name,
        "arguments": full_arguments,
        "context": {
            "user_id": uid,
            "session_id": _required_text(session_id, "session_id", allow_empty=True),
            "content_format": _required_text(
                content_format or "shortform",
                "content_format",
                limit=128,
            ),
        },
    }
    signature = hmac.new(_dispatch_secret(secret), _canonical_bytes(envelope), hashlib.sha256).hexdigest()
    envelope["auth"] = {
        "algorithm": RUNPOD_AUTH_ALGORITHM,
        "key_id": RUNPOD_AUTH_KEY_ID,
        "signature": signature,
    }
    return envelope


def verify_signed_envelope(
    envelope: Any,
    *,
    secret: str | None = None,
) -> dict[str, Any]:
    """Authenticate and fully validate a production envelope."""

    if not isinstance(envelope, dict):
        raise RunPodContractError("invalid_envelope", "RunPod input must be a production envelope object")
    keys = frozenset(envelope.keys())
    if keys != _ENVELOPE_KEYS:
        if {"method", "path", "body"} & keys:
            raise RunPodContractError("http_proxy_rejected", "Arbitrary HTTP proxy payloads are forbidden")
        raise RunPodContractError("invalid_envelope", "Production envelope fields do not match schema")
    if envelope.get("schema") != RUNPOD_ENVELOPE_SCHEMA:
        raise RunPodContractError("unsupported_schema", "Unsupported production envelope schema")

    auth = envelope.get("auth")
    if not isinstance(auth, dict) or frozenset(auth.keys()) != _AUTH_KEYS:
        raise RunPodContractError("invalid_auth", "Envelope auth fields do not match schema")
    if auth.get("algorithm") != RUNPOD_AUTH_ALGORITHM or auth.get("key_id") != RUNPOD_AUTH_KEY_ID:
        raise RunPodContractError("invalid_auth", "Unsupported envelope authentication metadata")
    signature = str(auth.get("signature") or "").strip().lower()
    if not _SIGNATURE_RE.fullmatch(signature):
        raise RunPodContractError("invalid_signature", "Envelope signature is malformed")

    unsigned = {key: envelope[key] for key in envelope if key != "auth"}
    expected_signature = hmac.new(
        _dispatch_secret(secret),
        _canonical_bytes(unsigned),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        raise RunPodContractError("invalid_signature", "Envelope signature verification failed")

    name = _required_text(envelope.get("tool"), "tool", limit=128)
    if name not in RUNPOD_PRODUCTION_TOOL_ALLOWLIST:
        raise RunPodContractError("tool_not_allowed", "Only allowlisted production tools can use RunPod")
    arguments = normalize_tool_arguments(envelope.get("arguments"), semantic=False)
    command_id = _required_text(envelope.get("command_id"), "command_id", allow_empty=True)
    context = envelope.get("context")
    if not isinstance(context, dict) or frozenset(context.keys()) != _CONTEXT_KEYS:
        raise RunPodContractError("invalid_context", "Envelope context fields do not match schema")
    normalized_context = {
        "user_id": _required_text(context.get("user_id"), "user_id", limit=256),
        "session_id": _required_text(context.get("session_id"), "session_id", allow_empty=True),
        "content_format": _required_text(context.get("content_format"), "content_format", limit=128),
    }
    dispatch_id = _required_text(envelope.get("dispatch_id"), "dispatch_id", limit=64)
    if not _DISPATCH_ID_RE.fullmatch(dispatch_id):
        raise RunPodContractError("invalid_dispatch_id", "Envelope dispatch_id is malformed")
    expected_dispatch_id = semantic_dispatch_id(
        name,
        arguments,
        command_id=command_id,
        user_id=normalized_context["user_id"],
    )
    if not hmac.compare_digest(dispatch_id, expected_dispatch_id):
        raise RunPodContractError("dispatch_id_mismatch", "Envelope dispatch_id does not match its command")

    return {
        "schema": RUNPOD_ENVELOPE_SCHEMA,
        "dispatch_id": dispatch_id,
        "command_id": command_id,
        "tool": name,
        "arguments": arguments,
        "context": normalized_context,
        "auth": {
            "algorithm": RUNPOD_AUTH_ALGORITHM,
            "key_id": RUNPOD_AUTH_KEY_ID,
            "signature": signature,
        },
    }
