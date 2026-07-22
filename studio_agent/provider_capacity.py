"""Shared provider-capacity classification and job-scoped lane state."""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


XAI_CREDIT_EXHAUSTED = "xai_credit_exhausted"
PROVIDER_UNAVAILABLE = "provider_unavailable"
STUDIO_BUDGET_EXCEEDED = "studio_budget_exceeded"
AUTHORIZATION_ERROR = "authorization_error"
UNKNOWN_PROVIDER_ERROR = "unknown_provider_error"


@dataclass(frozen=True)
class ProviderError:
    kind: str
    provider: str
    retryable: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_provider_error(exc: BaseException | str, *, provider: str = "") -> ProviderError:
    text = str(exc or "").strip()
    low = text.lower()
    provider_name = str(provider or ("xai" if any(x in low for x in ("xai", "grok")) else "")).lower()
    if "budget_exceeded" in low or exc.__class__.__name__ == "BudgetExceededError":
        return ProviderError(STUDIO_BUDGET_EXCEEDED, "studio", False, text[:500])

    credit_signal = any(
        phrase in low
        for phrase in (
            "used all available credits",
            "reached its monthly spending limit",
            "monthly spending limit",
            "credit balance",
            "insufficient credits",
            "insufficient balance",
            "billing limit",
            "spending limit",
        )
    )
    if provider_name == "xai" and credit_signal and any(
        signal in low for signal in ("403", "permission-denied", "credit", "billing", "spending")
    ):
        return ProviderError(XAI_CREDIT_EXHAUSTED, "xai", True, text[:500])

    if any(signal in low for signal in ("401", "invalid api key", "invalid entitlement", "unauthorized")):
        return ProviderError(AUTHORIZATION_ERROR, provider_name, False, text[:500])
    if any(signal in low for signal in ("429", "rate limit", "503", "temporarily unavailable", "timeout")):
        return ProviderError(PROVIDER_UNAVAILABLE, provider_name, True, text[:500])
    return ProviderError(UNKNOWN_PROVIDER_ERROR, provider_name, False, text[:500])


def _state_path(workspace: Path) -> Path:
    return Path(workspace) / "provider_capacity.json"


def load_capacity_state(workspace: Path) -> dict[str, Any]:
    path = _state_path(workspace)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("lanes", {})
                return data
        except Exception:
            pass
    return {"lanes": {}}


def mark_lane_exhausted(
    workspace: Path,
    provider: str,
    *,
    reason: BaseException | str,
    route_revision: int = 0,
    model: str = "",
    endpoint: str = "",
) -> dict[str, Any]:
    workspace = Path(workspace)
    state = load_capacity_state(workspace)
    lanes = dict(state.get("lanes") or {})
    key = str(provider or "").strip().lower()
    error = classify_provider_error(reason, provider=key)
    lanes[key] = {
        "status": "exhausted",
        "error": error.as_dict(),
        "route_revision": max(0, int(route_revision or 0)),
        "model": str(model or ""),
        "endpoint": str(endpoint or ""),
        "updated_at": time.time(),
    }
    state["lanes"] = lanes
    state["updated_at"] = time.time()
    workspace.mkdir(parents=True, exist_ok=True)
    _state_path(workspace).write_text(json.dumps(state, indent=2), encoding="utf-8")
    return dict(lanes[key])


def lane_exhausted(
    workspace: Path,
    provider: str,
    *,
    route_revision: int = 0,
) -> bool:
    lane = dict(
        (load_capacity_state(Path(workspace)).get("lanes") or {}).get(
            str(provider or "").strip().lower()
        )
        or {}
    )
    if str(lane.get("status") or "") != "exhausted":
        return False
    recorded_revision = int(lane.get("route_revision") or 0)
    current_revision = max(0, int(route_revision or 0))
    # A deliberate picker change starts a new route generation and permits one
    # fresh provider attempt. Revision 0 is a job-wide capacity observation.
    return recorded_revision == 0 or current_revision == 0 or recorded_revision == current_revision

