from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from studio_agent import runpod_storage, runpod_worker
from studio_agent.runpod_contract import (
    RUNPOD_PRODUCTION_TOOL_ALLOWLIST,
    RunPodContractError,
    build_signed_envelope,
    semantic_dispatch_id,
    verify_signed_envelope,
)


SECRET = "worker-test-dispatch-secret-at-least-32-bytes"


def _event(tool: str = "finalize_production", arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "id": "runpod-job-test",
        "input": build_signed_envelope(
            tool,
            arguments or {"job_id": "studio-job-1"},
            command_id="command-1",
            user_id="user-1",
            session_id="session-1",
            content_format="shortform",
            secret=SECRET,
        ),
    }


def _resign(envelope: dict[str, Any]) -> dict[str, Any]:
    unsigned = {key: value for key, value in envelope.items() if key != "auth"}
    payload = json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    envelope["auth"]["signature"] = hmac.new(
        SECRET.encode("utf-8"), payload, hashlib.sha256
    ).hexdigest()
    return envelope


@pytest.fixture(autouse=True)
def _worker_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path / "studio"))
    monkeypatch.setenv("RUNPOD_DISPATCH_SECRET", SECRET)
    monkeypatch.delenv("SKELETON_AI_OUTPUT_ROOT", raising=False)
    monkeypatch.delenv("LF_OUTPUT_ROOT", raising=False)
    monkeypatch.delenv("STUDIO_RUNPOD_DISPATCH_ID", raising=False)
    monkeypatch.setattr(runpod_worker, "_verify_runpod_network_volume", lambda _root: None)
    monkeypatch.setattr(
        runpod_storage,
        "reconcile_staged_workspace",
        lambda job_id, kind: {
            "ok": True,
            "status": "manifest_reconciled",
            "job_id": job_id,
            "kind": kind,
        },
    )


def test_contract_round_trip_and_semantic_idempotency() -> None:
    left = build_signed_envelope(
        "animate_production_scenes",
        {
            "job_id": "studio-job-1",
            "scene_indices": [5, 2, 2, 1],
            "_credit_reservation": {"reservation_id": "reserve-a"},
            "queued_at": 1,
        },
        command_id="command-1",
        user_id="user-1",
        secret=SECRET,
    )
    right_id = semantic_dispatch_id(
        "animate_production_scenes",
        {
            "job_id": "studio-job-1",
            "scene_indices": [1, 2, 5],
            "_credit_reservation": {"reservation_id": "reserve-b"},
            "queued_at": 999,
        },
        command_id="command-1",
        user_id="user-1",
    )

    assert verify_signed_envelope(left, secret=SECRET)["arguments"]["scene_indices"] == [5, 2, 2, 1]
    assert left["dispatch_id"] == right_id


def test_contract_rejects_tampering_http_proxy_and_preproduction_tools() -> None:
    tampered = _event()["input"]
    tampered["arguments"]["job_id"] = "different-job"
    with pytest.raises(RunPodContractError, match="signature"):
        verify_signed_envelope(tampered, secret=SECRET)

    with pytest.raises(RunPodContractError) as proxy_error:
        verify_signed_envelope({"method": "POST", "path": "/api/chat", "body": {}})
    assert proxy_error.value.code == "http_proxy_rejected"

    assert "generate_longform_thumbnails" not in RUNPOD_PRODUCTION_TOOL_ALLOWLIST
    for forbidden in ("chat", "poll_render_job", "status", "generate_longform_thumbnails"):
        envelope = _event()["input"]
        envelope["tool"] = forbidden
        _resign(envelope)
        with pytest.raises(RunPodContractError) as forbidden_error:
            verify_signed_envelope(envelope, secret=SECRET)
        assert forbidden_error.value.code == "tool_not_allowed"


def test_worker_requires_absolute_durable_data_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("APP_DATA_DIR")
    assert runpod_worker.handler(_event())["error"] == "worker_not_configured"

    monkeypatch.setenv("APP_DATA_DIR", "relative/studio")
    assert runpod_worker.handler(_event())["error"] == "worker_not_configured"


def test_worker_rejects_an_unverified_network_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runpod_worker,
        "_verify_runpod_network_volume",
        lambda _root: (_ for _ in ()).throw(
            runpod_worker.WorkerConfigurationError("not a mounted volume")
        ),
    )

    result = runpod_worker.handler(_event())

    assert result["error"] == "worker_not_configured"
    assert "mounted volume" in result["detail"]

def test_worker_executes_once_and_replays_durable_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []

    def execute(*args: Any, **kwargs: Any) -> str:
        calls.append((args, kwargs))
        return json.dumps({"ok": True, "job_id": "studio-job-1"})

    monkeypatch.setattr(runpod_worker, "_load_tool_executor", lambda: execute)
    event = _event()

    first = runpod_worker.handler(event)
    second = runpod_worker.handler(event)

    assert first["ok"] is True
    assert first["execution_count"] == 1
    assert first["worker_mode"] is True
    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    assert second["execution_count"] == 1
    assert len(calls) == 1
    assert calls[0][0] == ("finalize_production", {"job_id": "studio-job-1"})
    assert calls[0][1] == {
        "user_id": "user-1",
        "content_format": "shortform",
        "session_id": "session-1",
    }


def test_retry_only_credit_metadata_does_not_duplicate_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(
        runpod_worker,
        "_load_tool_executor",
        lambda: lambda *a, **k: calls.append((a, k)) or json.dumps({"ok": True}),
    )
    first = _event(
        "finalize_production",
        {"job_id": "studio-job-1", "_credit_reservation": {"reservation_id": "reserve-a"}},
    )
    retry = _event(
        "finalize_production",
        {"job_id": "studio-job-1", "_credit_reservation": {"reservation_id": "reserve-b"}},
    )

    assert first["input"]["dispatch_id"] == retry["input"]["dispatch_id"]
    assert runpod_worker.handler(first)["idempotent_replay"] is False
    assert runpod_worker.handler(retry)["idempotent_replay"] is True
    assert len(calls) == 1


def test_worker_never_retries_a_claim_without_a_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(runpod_worker, "_load_tool_executor", lambda: lambda *a, **k: calls.append((a, k)))
    event = _event()
    envelope = verify_signed_envelope(event["input"], secret=SECRET)
    payload_hash = runpod_worker._semantic_payload_hash(envelope)
    root = runpod_worker.prepare_worker_environment()
    claim_path, receipt_path = runpod_worker._receipt_paths(root, envelope["dispatch_id"])
    status, _ = runpod_worker._claim_once(
        claim_path,
        receipt_path,
        dispatch_id=envelope["dispatch_id"],
        payload_hash=payload_hash,
        tool=envelope["tool"],
    )

    result = runpod_worker.handler(event)

    assert status == "new"
    assert result["status"] == "already_claimed"
    assert calls == []


def test_failed_execution_is_receipted_and_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fail(*args: Any, **kwargs: Any) -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("provider failed")

    monkeypatch.setattr(runpod_worker, "_load_tool_executor", lambda: fail)
    event = _event()

    first = runpod_worker.handler(event)
    second = runpod_worker.handler(event)

    assert first["status"] == "failed"
    assert second["idempotent_replay"] is True
    assert calls == 1


def test_worker_emits_operation_scoped_shortform_provider_fact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from studio_agent import production_costs

    def execute(*_args: Any, **_kwargs: Any) -> str:
        workspace = Path(os.environ["SKELETON_AI_OUTPUT_ROOT"]) / "studio-job-1"
        production_costs.record_event(
            workspace,
            stage="animation",
            provider="fal",
            operation="scene_animation",
            usd="0.25",
        )
        return json.dumps({"ok": True, "job_id": "studio-job-1"})

    monkeypatch.setattr(runpod_worker, "_load_tool_executor", lambda: execute)

    result = runpod_worker.handler(_event())

    assert result["billing"]["report_complete"] is True
    provider = [
        fact
        for fact in result["billing"]["provider_cost_facts"]
        if fact.get("kind") == "provider_usd"
    ]
    assert len(provider) == 1
    assert provider[0]["provider_usd_decimal"] == "0.250000"
    assert provider[0]["operation_scoped"] is True
    assert provider[0]["authoritative"] is True


def test_async_production_waits_and_receives_original_runpod_event(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, str, Any]] = []
    event = _event("start_shortform_generate", {"topic": "skeleton history"})
    monkeypatch.setattr(
        runpod_worker,
        "_load_tool_executor",
        lambda: lambda *a, **k: json.dumps({"job_id": "short-job-1"}),
    )
    monkeypatch.setattr(
        runpod_worker,
        "_wait_for_production_gate",
        lambda tool, job_id, job_event: seen.append((tool, job_id, job_event))
        or {"job_id": job_id, "status": "awaiting_scene_review", "running": False},
    )

    result = runpod_worker.handler(event)

    assert result["ok"] is True
    assert result["job_snapshot"]["status"] == "awaiting_scene_review"
    assert seen == [("start_shortform_generate", "short-job-1", event)]


def test_progress_update_is_optional_and_compact(monkeypatch: pytest.MonkeyPatch) -> None:
    updates: list[tuple[Any, Any]] = []
    fake = types.SimpleNamespace(
        serverless=types.SimpleNamespace(
            progress_update=lambda event, progress: updates.append((event, progress))
        )
    )
    monkeypatch.setitem(sys.modules, "runpod", fake)
    event = {"id": "runpod-job-test", "input": {}}

    runpod_worker._emit_progress_update(
        event,
        {
            "job_id": "short-job-1",
            "status": "running",
            "percent": 25,
            "secret_internal_field": "not exposed",
        },
    )

    assert updates[0][0] is event
    progress = json.loads(updates[0][1])
    assert progress == {"job_id": "short-job-1", "status": "running", "percent": 25}


def test_worker_credit_mode_records_facts_without_mutating_wallet(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import unified_credits

    wallet = tmp_path / "wallet.json"
    ledger = tmp_path / "ledger.jsonl"
    wallet.write_text('{"sentinel":true}', encoding="utf-8")
    ledger.write_text("sentinel\n", encoding="utf-8")
    monkeypatch.setattr(unified_credits, "WALLETS_PATH", wallet)
    monkeypatch.setattr(unified_credits, "LEDGER_PATH", ledger)
    monkeypatch.setenv("STUDIO_RUNPOD_WORKER_MODE", "1")
    monkeypatch.setenv("STUDIO_RUNPOD_DISPATCH_ID", "rpd_" + "a" * 40)

    charged, _ = unified_credits.debit_usd(
        "user-1", "0.25", reason="provider-cost", metadata={"provider": "fal"}
    )
    reservation = unified_credits.reserve_credits(
        "user-1", 100, reason="production", metadata={"tool": "finalize_production"}
    )
    unified_credits.commit_reservation(
        "user-1", reservation["reservation_id"], actual_credits=charged
    )
    facts = unified_credits.get_runpod_worker_cost_facts("rpd_" + "a" * 40)

    assert wallet.read_text(encoding="utf-8") == '{"sentinel":true}'
    assert ledger.read_text(encoding="utf-8") == "sentinel\n"
    assert reservation["worker_deferred"] is True
    assert {fact["kind"] for fact in facts} >= {"provider_usd", "reservation_commit"}


def test_longform_coroutines_run_inline_only_in_worker_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    from long_form import pipeline

    completed: list[str] = []

    async def stage(value: str) -> None:
        await asyncio.sleep(0)
        completed.append(value)

    monkeypatch.setenv("STUDIO_RUNPOD_WORKER_MODE", "1")
    result = pipeline._spawn_lf_background_coro(stage("inline"), "long-job-1")
    assert result is None
    assert completed == ["inline"]

    async def run_under_sdk_loop() -> None:
        result = pipeline._spawn_lf_background_coro(stage("sdk-loop-inline"), "long-job-2")
        assert result is None

    asyncio.run(run_under_sdk_loop())
    assert completed == ["inline", "sdk-loop-inline"]

    monkeypatch.delenv("STUDIO_RUNPOD_WORKER_MODE")

    async def schedule_on_api_loop() -> None:
        task = pipeline._spawn_lf_background_coro(stage("background"), "long-job-3")
        assert isinstance(task, asyncio.Task)
        await task

    asyncio.run(schedule_on_api_loop())
    assert completed == ["inline", "sdk-loop-inline", "background"]
