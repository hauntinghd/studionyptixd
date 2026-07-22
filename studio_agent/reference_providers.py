"""Studio reference analysis: direct Anthropic semantics and FAL-only STT."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Callable

import httpx

from studio_agent import provider_policy
from studio_agent.stt_utils import httpx_json_dict


def _fal_key() -> str:
    try:
        from backend_settings import FAL_AI_KEY
    except Exception:
        FAL_AI_KEY = ""
    return str(os.getenv("FAL_KEY", "") or FAL_AI_KEY or "").strip()


def _openrouter_key() -> str:
    return ""


def _xai_key() -> str:
    return ""


def _anthropic_key() -> str:
    from studio_agent.openrouter import anthropic_api_key

    return anthropic_api_key()


def _anthropic_base() -> str:
    return "https://api.anthropic.com/v1"


def reference_vision_model() -> str:
    from studio_agent.openrouter import _normalize_anthropic_model

    raw = str(
        os.getenv("STUDIO_REFERENCE_VISION_MODEL")
        or os.getenv("STUDIO_AGENT_VISION_MODEL_ANTHROPIC")
        or "claude-haiku-4-5-20251001"
    ).strip()
    return _normalize_anthropic_model(raw)


def reference_analysis_model() -> str:
    from studio_agent.openrouter import _normalize_anthropic_model

    raw = str(
        os.getenv("STUDIO_REFERENCE_ANALYSIS_MODEL")
        or os.getenv("STUDIO_AGENT_ANALYSIS_MODEL_ANTHROPIC")
        or "claude-haiku-4-5-20251001"
    ).strip()
    return _normalize_anthropic_model(raw)


def reference_fal_vision_model() -> str:
    provider_policy.assert_provider_allowed("fal", provider_policy.SEMANTIC_QA_CAPABILITY)
    raise AssertionError("unreachable")


def reference_fal_analysis_model() -> str:
    provider_policy.assert_provider_allowed("fal", provider_policy.SEMANTIC_QA_CAPABILITY)
    raise AssertionError("unreachable")


def reference_openrouter_vision_model() -> str:
    provider_policy.assert_provider_allowed("openrouter", provider_policy.SEMANTIC_QA_CAPABILITY)
    raise AssertionError("unreachable")


def reference_openrouter_analysis_model() -> str:
    provider_policy.assert_provider_allowed("openrouter", provider_policy.SEMANTIC_QA_CAPABILITY)
    raise AssertionError("unreachable")


def vision_provider_order() -> list[str]:
    return ["anthropic"]


def analysis_provider_order() -> list[str]:
    return ["anthropic"]


def stt_provider_order() -> list[str]:
    return ["fal"]


def _provider_available(name: str) -> bool:
    if name == "anthropic":
        return bool(_anthropic_key())
    if name == "fal":
        return bool(_fal_key())
    return False


def _anthropic_text_from_response(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for block in payload.get("content") or []:
        if not isinstance(block, dict):
            continue
        if str(block.get("type") or "") == "text":
            text = str(block.get("text") or "").strip()
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def anthropic_messages_completion(
    *,
    prompt: str,
    model: str,
    max_tokens: int = 2048,
    temperature: float = 0.2,
    image_paths: list[str] | None = None,
) -> dict[str, Any]:
    provider_policy.assert_provider_allowed("anthropic", provider_policy.SEMANTIC_QA_CAPABILITY)
    model = provider_policy.assert_runner_model_allowed(model)
    api_key = _anthropic_key()
    if not api_key:
        return {"error": "anthropic_not_configured", "text": ""}
    content: list[dict[str, Any]] = []
    for path in list(image_paths or []):
        try:
            encoded = base64.standard_b64encode(Path(path).read_bytes()).decode("ascii")
        except Exception as exc:
            return {"error": f"frame_read_failed:{exc}", "text": ""}
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": encoded,
            },
        })
    content.append({"type": "text", "text": prompt})
    try:
        with httpx.Client(timeout=120.0) as client:
            resp = client.post(
                f"{_anthropic_base()}/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": os.getenv("ANTHROPIC_VERSION", "2023-06-01"),
                    "content-type": "application/json",
                },
                json=provider_policy.sanitize_anthropic_payload(model, {
                    "model": model,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "messages": [{"role": "user", "content": content}],
                }),
            )
        if resp.status_code >= 400:
            return {"error": f"anthropic_failed:{resp.status_code}:{resp.text[:180]}", "text": ""}
        payload = resp.json()
        if not isinstance(payload, dict):
            return {"error": "anthropic_empty_payload", "text": ""}
        text = _anthropic_text_from_response(payload)
        if not text:
            return {"error": "empty_anthropic_response", "text": "", "model": model}
        return {"text": text, "model": model, "provider": "anthropic"}
    except Exception as exc:
        return {"error": str(exc)[:240], "text": "", "model": model}


def run_provider_chain(
    order: list[str],
    runners: dict[str, Callable[[], dict[str, Any]]],
    *,
    success_key: str,
) -> dict[str, Any]:
    """Try providers in order; return first result with success_key populated."""
    last: dict[str, Any] = {"error": "no_reference_providers_configured"}
    # The fixed FAL-only order is the STT chain. Every other reference chain is
    # semantic QA and therefore direct Anthropic only.
    normalized_order = [str(item).strip().lower() for item in order]
    capability = (
        provider_policy.STT_CAPABILITY
        if normalized_order == stt_provider_order()
        else provider_policy.SEMANTIC_QA_CAPABILITY
    )
    for name in order:
        try:
            provider_policy.assert_provider_allowed(name, capability)
        except provider_policy.ProviderPolicyDenied as exc:
            last = {"error": str(exc)}
            continue
        runner = runners.get(name)
        if not runner:
            continue
        if not _provider_available(name):
            last = {"error": f"{name}_not_configured"}
            continue
        result = runner()
        if not isinstance(result, dict):
            last = {"error": f"{name}_invalid_result"}
            continue
        if str(result.get(success_key) or "").strip():
            return result
        last = result
    return last


def transcribe_fal_segments(audio_path: str) -> dict[str, Any]:
    provider_policy.assert_provider_allowed("fal", provider_policy.STT_CAPABILITY)
    fal_key = _fal_key()
    path = Path(str(audio_path or ""))
    if not path.is_file():
        return {"error": "audio_missing", "text": "", "segments": []}
    if not fal_key:
        return {"error": "fal_not_configured", "text": "", "segments": []}
    try:
        import fal_client

        os.environ.setdefault("FAL_KEY", fal_key)
        try:
            audio_url = fal_client.upload_file(str(path))
        except json.JSONDecodeError:
            return {"error": "fal_upload_empty_response", "text": "", "segments": []}
        except Exception as exc:
            return {"error": f"fal_upload_failed:{str(exc)[:180]}", "text": "", "segments": []}
        if not str(audio_url or "").strip():
            return {"error": "fal_upload_empty_url", "text": "", "segments": []}
        with httpx.Client(timeout=180.0) as client:
            resp = client.post(
                "https://fal.run/fal-ai/whisper",
                headers={"Authorization": f"Key {fal_key}"},
                json={
                    "audio_url": audio_url,
                    "task": "transcribe",
                    "chunk_level": "segment",
                    "language": "en",
                },
            )
        if resp.status_code != 200:
            return {"error": f"whisper_failed:{resp.status_code}", "text": "", "segments": []}
        payload = httpx_json_dict(resp)
        if not payload:
            return {"error": "whisper_empty_payload", "text": "", "segments": []}
        from studio_agent.competitor import _coerce_segment_times

        segments: list[dict[str, Any]] = []
        for chunk in payload.get("chunks") or payload.get("segments") or []:
            if not isinstance(chunk, dict):
                continue
            text = str(chunk.get("text") or "").strip()
            if not text:
                continue
            start, end = _coerce_segment_times(chunk)
            segments.append({"start_sec": round(start, 2), "end_sec": round(end, 2), "text": text})
        text = str(payload.get("text") or "").strip()
        if not text and segments:
            text = " ".join(str(s.get("text") or "") for s in segments).strip()
        if not text:
            return {"error": "no_speech_detected", "text": "", "segments": []}
        return {
            "text": text,
            "segments": segments,
            "word_count": len(text.split()),
            "provider": "fal",
        }
    except Exception as exc:
        return {"error": str(exc)[:240], "text": "", "segments": []}
