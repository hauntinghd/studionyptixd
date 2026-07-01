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


def _ledger_path(workspace: Path) -> Path:
    return Path(workspace) / "cost_ledger.jsonl"


def _summary_path(workspace: Path) -> Path:
    return Path(workspace) / "cost_summary.json"


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


def price_fal_tts(text: str) -> tuple[Decimal, str, str, float]:
    chars = max(1, len(str(text or "")))
    thousands = chars / 1000.0
    amount, note = fal_unit_cost(
        "minimax_speech",
        fallback_key="fal_minimax_per_1k_chars",
        quantity=thousands,
    )
    return amount, note, "minimax_speech", thousands


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


def attach_to_progress(workspace: Path, payload: dict[str, Any]) -> dict[str, Any]:
    summary = load_summary(workspace)
    payload["cost"] = {
        "actual_usd": summary.get("total_usd", 0.0),
        "actual_usd_decimal": summary.get("total_usd_decimal", "0.000000"),
        "event_count": summary.get("event_count", 0),
        "status": summary.get("status", "derived_from_job_events"),
    }
    return payload
