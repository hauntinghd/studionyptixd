"""Durable per-production cost ledger.

This records the provider operations Studio actually ran for a job. It is a
receipt-style ledger derived from FAL pricing units and request metadata where
available; it is separate from preflight estimates.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

USD_QUANT = Decimal("0.000001")
XAI_IMAGE_RATES = {
    "grok_imagine": Decimal("0.05"),
    "grok_imagine_quality": Decimal("0.05"),
    "grok-imagine-image-quality": Decimal("0.05"),
    "grok_imagine_standard": Decimal("0.02"),
    "grok-imagine-image": Decimal("0.02"),
}
XAI_VIDEO_RATES = {
    "grok_imagine_video": Decimal("0.05"),
    "grok_imagine_video_15": Decimal("0.08"),
    "grok_imagine_video_15_1080p": Decimal("0.25"),
    "xai:grok-imagine-video": Decimal("0.05"),
    "xai:grok-imagine-video-1.5": Decimal("0.08"),
}
XAI_TTS_PER_MILLION_CHARS = Decimal("15.00")
PROVIDER_LABELS = {
    "fal": "FAL",
    "xai": "xAI",
    "simulation": "Simulation",
}


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value if value is not None else "0"))
    except Exception:
        return Decimal("0")


def _usd(value: Any) -> Decimal:
    amount = _dec(value)
    if amount <= 0:
        return Decimal("0.000000")
    return amount.quantize(USD_QUANT, rounding=ROUND_HALF_UP)


def _provider_decimal(summary: dict[str, Any], provider: str) -> Decimal:
    decimals = summary.get("by_provider_decimal")
    floats = summary.get("by_provider")
    if isinstance(decimals, dict) and provider in decimals:
        return _usd(decimals.get(provider))
    if isinstance(floats, dict) and provider in floats:
        return _usd(floats.get(provider))
    return Decimal("0.000000")


def _provider_breakdown(summary: dict[str, Any]) -> list[dict[str, Any]]:
    values = summary.get("by_provider_decimal") or summary.get("by_provider") or {}
    if not isinstance(values, dict):
        return []
    rows: list[dict[str, Any]] = []
    for provider, raw_amount in sorted(values.items()):
        amount = _usd(raw_amount)
        if amount <= 0:
            continue
        key = str(provider or "unknown")
        rows.append(
            {
                "provider": key,
                "label": PROVIDER_LABELS.get(key, key),
                "usd": float(amount),
                "usd_decimal": str(amount),
            }
        )
    return rows


def _ledger_path(workspace: Path) -> Path:
    return Path(workspace) / "cost_ledger.jsonl"


def _summary_path(workspace: Path) -> Path:
    return Path(workspace) / "cost_summary.json"


def _billing_path(workspace: Path) -> Path:
    return Path(workspace) / "cost_billing_state.json"


def _read_events(workspace: Path) -> list[dict[str, Any]]:
    path = _ledger_path(workspace)
    if not path.is_file():
        return []
    events: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                events.append(row)
    except Exception:
        return events
    return events


def summarize(workspace: Path) -> dict[str, Any]:
    events = _read_events(workspace)
    total = Decimal("0")
    by_stage: dict[str, Decimal] = {}
    by_provider: dict[str, Decimal] = {}
    for row in events:
        amount = _usd(row.get("usd_decimal", row.get("usd", 0)))
        total += amount
        stage = str(row.get("stage") or "unknown")
        provider = str(row.get("provider") or "unknown")
        by_stage[stage] = by_stage.get(stage, Decimal("0")) + amount
        by_provider[provider] = by_provider.get(provider, Decimal("0")) + amount
    summary = {
        "status": "derived_from_job_events",
        "total_usd": float(_usd(total)),
        "total_usd_decimal": str(_usd(total)),
        "event_count": len(events),
        "by_stage": {k: float(_usd(v)) for k, v in sorted(by_stage.items())},
        "by_stage_decimal": {k: str(_usd(v)) for k, v in sorted(by_stage.items())},
        "by_provider": {k: float(_usd(v)) for k, v in sorted(by_provider.items())},
        "by_provider_decimal": {k: str(_usd(v)) for k, v in sorted(by_provider.items())},
        "updated_at": time.time(),
    }
    try:
        _summary_path(workspace).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    except Exception:
        pass
    return summary


def load_summary(workspace: Path) -> dict[str, Any]:
    path = _summary_path(workspace)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return summarize(workspace)


def load_billing_state(workspace: Path) -> dict[str, Any]:
    path = _billing_path(workspace)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("charged_usd_decimal", "0.000000")
                data.setdefault("charges", [])
                return data
        except Exception:
            pass
    return {"charged_usd_decimal": "0.000000", "charges": []}


def pending_billable_usd(workspace: Path) -> Decimal:
    summary = load_summary(workspace)
    billing = load_billing_state(workspace)
    total = _usd(summary.get("total_usd_decimal", summary.get("total_usd", 0)))
    charged = _usd(billing.get("charged_usd_decimal", 0))
    if total <= charged:
        return Decimal("0.000000")
    return _usd(total - charged)


def mark_billed(
    workspace: Path,
    *,
    usd: Any,
    credits: int,
    user_id: str = "",
    reservation_id: str = "",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    workspace = Path(workspace)
    state = load_billing_state(workspace)
    amount = _usd(usd)
    charged = _usd(state.get("charged_usd_decimal", 0)) + amount
    row = {
        "ts": time.time(),
        "usd": float(amount),
        "usd_decimal": str(amount),
        "credits": int(credits or 0),
        "user_id": str(user_id or ""),
        "reservation_id": str(reservation_id or ""),
        "reason": str(reason or ""),
        "metadata": metadata or {},
    }
    charges = list(state.get("charges") or [])
    charges.append(row)
    state = {
        "charged_usd": float(_usd(charged)),
        "charged_usd_decimal": str(_usd(charged)),
        "charges": charges[-200:],
        "updated_at": time.time(),
    }
    try:
        workspace.mkdir(parents=True, exist_ok=True)
        _billing_path(workspace).write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass
    return state


def record_event(
    workspace: Path,
    *,
    stage: str,
    provider: str,
    operation: str,
    usd: Any,
    quantity: Any = None,
    unit: str = "",
    endpoint: str = "",
    request_id: str = "",
    scene_index: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    amount = _usd(usd)
    row = {
        "ts": time.time(),
        "stage": str(stage or "unknown"),
        "provider": str(provider or "fal"),
        "operation": str(operation or "operation"),
        "usd": float(amount),
        "usd_decimal": str(amount),
        "quantity": quantity,
        "unit": str(unit or ""),
        "endpoint": str(endpoint or ""),
        "request_id": str(request_id or ""),
        "scene_index": scene_index,
        "metadata": metadata or {},
    }
    with _ledger_path(workspace).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=True) + "\n")
    summarize(workspace)
    return row


def fal_unit_cost(key: str, *, fallback_key: str, quantity: float = 1.0) -> tuple[Decimal, str]:
    try:
        from long_form import fal_pricing as fp

        snap = fp.get_pricing_snapshot()
        amount, note = fp.unit_cost(snap, key, fallback_key=fallback_key, quantity=float(quantity))
        return _usd(amount), note
    except Exception as exc:
        return Decimal("0.000000"), f"pricing_error:{str(exc)[:120]}"


def price_fal_image(*, edit: bool = False, quantity: int = 1) -> tuple[Decimal, str, str]:
    key = "seedream_v45_edit" if edit else "seedream_v45"
    fallback = "seedream_v45_edit_per_image" if edit else "seedream_v45_per_image"
    amount, note = fal_unit_cost(key, fallback_key=fallback, quantity=max(1, int(quantity or 1)))
    return amount, note, key


def price_xai_image(model_id: str, *, quantity: int = 1, edit: bool = False) -> tuple[Decimal, str, str]:
    key = str(model_id or "grok_imagine").strip().lower()
    unit = XAI_IMAGE_RATES.get(key, XAI_IMAGE_RATES["grok_imagine"])
    qty = max(1, int(quantity or 1))
    # xAI image edits are metered as an input image plus an output image when
    # usage metadata is unavailable. If the API returns usage.cost_in_usd_ticks,
    # callers replace this fallback with the exact returned charge.
    multiplier = Decimal("2") if edit else Decimal("1")
    amount = _usd(unit * Decimal(qty) * multiplier)
    api_model = "grok-imagine-image-quality" if unit == Decimal("0.05") else "grok-imagine-image"
    suffix = "per_edit" if edit else "per_image"
    return amount, f"xai:{api_model}_{suffix}", key


def price_fal_tts(text: str) -> tuple[Decimal, str, str, float]:
    chars = max(1, len(str(text or "")))
    thousands = chars / 1000.0
    amount, note = fal_unit_cost(
        "minimax_speech",
        fallback_key="fal_minimax_per_1k_chars",
        quantity=thousands,
    )
    return amount, note, "minimax_speech", thousands


def price_xai_tts(text: str) -> tuple[Decimal, str, str, int]:
    chars = max(1, len(str(text or "")))
    amount = _usd(XAI_TTS_PER_MILLION_CHARS * Decimal(chars) / Decimal(1_000_000))
    return amount, "xai:tts_per_character", "grok_tts", chars


def price_tts(provider: str, text: str) -> tuple[Decimal, str, str, float | int, str, str]:
    normalized = str(provider or "").strip().lower()
    if normalized == "xai":
        amount, note, key, qty = price_xai_tts(text)
        return amount, note, key, qty, "xai", "char"
    amount, note, key, qty = price_fal_tts(text)
    return amount, note, key, qty, "fal", "1k_chars"


def price_fal_video(endpoint: str, *, seconds: float) -> tuple[Decimal, str, str]:
    ep = str(endpoint or "").lower()
    sec = max(0.0, float(seconds or 0.0))
    if "pixverse" in ep:
        key, fallback = "pixverse_v6", "pixverse_v6_per_second"
    elif "kling" in ep and "pro" in ep:
        key, fallback = "kling_v21_pro", "kling_v21_pro_per_second"
    elif "kling" in ep:
        key, fallback = "kling_v21_standard", "kling_v21_standard_per_second"
    elif "ltxv-13b-098" in ep:
        key, fallback = "ltx_098_distilled", "ltx_098_distilled_per_second"
    elif "ltx" in ep:
        key, fallback = "ltx_13b_distilled", "ltx_13b_distilled_per_second"
    elif "wan" in ep:
        key, fallback = "wan_i2v", "wan_i2v_per_second"
    elif "seedance" in ep:
        key, fallback = "seedance_20_i2v", "seedance_20_i2v_per_second"
    else:
        key, fallback = "pixverse_v6", "pixverse_v6_per_second"
    amount, note = fal_unit_cost(key, fallback_key=fallback, quantity=sec)
    return amount, note, key


def price_xai_video(model_or_endpoint: str, *, seconds: float, resolution: str = "") -> tuple[Decimal, str, str]:
    raw = str(model_or_endpoint or "").strip().lower()
    res = str(resolution or "").strip().lower()
    if (
        raw == "grok_imagine_video_15_1080p"
        or (res == "1080p" and ("1.5" in raw or raw in {"grok_imagine_video_15", "xai:grok-imagine-video-1.5"}))
    ):
        key = "grok_imagine_video_15_1080p"
    elif raw == "grok_imagine_video_15" or "1.5" in raw:
        key = "grok_imagine_video_15"
    else:
        key = "grok_imagine_video"
    qty = max(0.0, float(seconds or 0.0))
    amount = _usd(XAI_VIDEO_RATES[key] * Decimal(str(qty)))
    return amount, f"xai:{key}_per_second", key


def attach_to_progress(workspace: Path, payload: dict[str, Any]) -> dict[str, Any]:
    summary = load_summary(workspace)
    provider_breakdown = _provider_breakdown(summary)
    active_providers: list[str] = []
    for row in provider_breakdown:
        active_providers.append(str(row.get("provider") or ""))
    if len(active_providers) == 1:
        spend_label = f"{PROVIDER_LABELS.get(active_providers[0], active_providers[0])} spent so far"
    elif len(active_providers) > 1:
        spend_label = "Total provider spend so far"
    else:
        spend_label = "Provider spend so far"
    fal_usd = _provider_decimal(summary, "fal")
    xai_usd = _provider_decimal(summary, "xai")
    payload["cost"] = {
        "actual_usd": summary.get("total_usd", 0.0),
        "actual_usd_decimal": summary.get("total_usd_decimal", "0.000000"),
        "event_count": summary.get("event_count", 0),
        "by_provider": summary.get("by_provider", {}),
        "by_provider_decimal": summary.get("by_provider_decimal", {}),
        "provider_breakdown": provider_breakdown,
        "fal_usd": float(fal_usd),
        "fal_usd_decimal": str(fal_usd),
        "xai_usd": float(xai_usd),
        "xai_usd_decimal": str(xai_usd),
        "spend_label": spend_label,
        "status": summary.get("status", "derived_from_job_events"),
    }
    return payload
