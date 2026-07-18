from __future__ import annotations

import json

import pytest

import unified_credits
from studio_agent import (
    production_budget,
    runpod_bridge,
    runpod_storage,
    tools,
    training_capture,
)
from studio_agent.execution_context import production_command_scope


@pytest.fixture(autouse=True)
def _quiet_execution_side_effects(monkeypatch):
    monkeypatch.setattr(tools.telemetry, "record_tool_call", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(training_capture, "capture_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(production_budget, "enforce_budget", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        production_budget,
        "with_budget_metadata",
        lambda result, _estimate, _arguments=None: result,
    )
    monkeypatch.setattr(tools, "_public_provider_block_message", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(runpod_storage, "assert_configured", lambda: None)
    monkeypatch.delenv("STUDIO_RUNPOD_LONGFORM_ENABLED", raising=False)
    monkeypatch.setattr(
        runpod_bridge,
        "acquire_production_lease",
        lambda dispatch_id, **_kwargs: {
            "dispatch_id": dispatch_id,
            "acquired": True,
            "idempotent_replay": False,
        },
    )
    monkeypatch.setattr(runpod_bridge, "release_production_lease", lambda _dispatch_id: True)
    monkeypatch.setattr(
        runpod_storage,
        "stage_job_workspace",
        lambda job_id, kind: {
            "ok": True,
            "status": "staged",
            "job_id": job_id,
            "kind": kind,
            "files_uploaded": 1,
        },
    )


def _call(name: str, arguments: dict) -> str:
    return tools.execute_tool_logged(
        name,
        arguments,
        user_id="user-1",
        content_format="shortform",
        session_id="session-1",
    )


def test_flag_off_keeps_allowlisted_production_local(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "0")
    local_calls = []
    dispatch_calls = []
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda name, arguments, **kwargs: local_calls.append((name, arguments, kwargs)) or '{"local": true}',
    )
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *args, **kwargs: dispatch_calls.append((args, kwargs)),
    )

    result = json.loads(_call("expand_visual_proof_shortform", {"job_id": "studio-job-1"}))

    assert result == {"local": True}
    assert len(local_calls) == 1
    assert dispatch_calls == []


def test_flag_on_dispatches_once_and_never_executes_locally(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "true")
    dispatch_calls = []

    def dispatch(name, arguments, **kwargs):
        dispatch_calls.append((name, arguments, kwargs))
        return {
            "ok": True,
            "status": "accepted",
            "dispatch_id": "rpd_" + "a" * 40,
            "runpod_job_id": "runpod-1",
            "idempotent_replay": False,
        }

    monkeypatch.setattr(runpod_bridge, "dispatch_production_tool", dispatch)
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("RunPod-routed production executed locally"),
    )

    result = json.loads(
        _call(
            "expand_visual_proof_shortform",
            {"job_id": "studio-job-1", "command_id": "command-1"},
        )
    )

    assert len(dispatch_calls) == 1
    assert dispatch_calls[0][2]["command_id"] == "command-1"
    assert result["job_id"] == "studio-job-1"
    assert result["studio_job_id"] == "studio-job-1"
    assert result["runpod_job_id"] == "runpod-1"
    assert result["execution_backend"] == "runpod_serverless"
    assert result["credits"]["status"] == "pending_worker_cost_reconciliation"
    assert result["credits"]["local_commit"] is False
    assert result["workspace_stage"]["job_id"] == "studio-job-1"
    assert result["workspace_stage"]["kind"] == "shortform"


def test_existing_workspace_is_staged_before_dispatch(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    events = []

    monkeypatch.setattr(
        runpod_storage,
        "stage_job_workspace",
        lambda job_id, kind: events.append(("stage", job_id, kind))
        or {"ok": True, "status": "staged", "job_id": job_id, "kind": kind},
    )
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: events.append(("dispatch",))
        or {
            "ok": True,
            "status": "accepted",
            "dispatch_id": "rpd_" + "b" * 40,
            "runpod_job_id": "runpod-stage-order",
        },
    )

    result = json.loads(
        _call(
            "expand_visual_proof_shortform",
            {"job_id": "studio-job-1", "command_id": "expand-command-1"},
        )
    )

    assert events == [("stage", "studio-job-1", "shortform"), ("dispatch",)]
    assert result["workspace_stage"]["status"] == "staged"


@pytest.mark.parametrize(
    ("tool_name", "expected_prefix"),
    (("start_shortform_generate", "sf_"), ("start_longform_render", "lf_")),
)
def test_new_runpod_production_gets_stable_studio_job_id(
    monkeypatch,
    tool_name,
    expected_prefix,
):
    monkeypatch.setattr(tools, "_require_longform_entitlement", lambda _user_id: None)
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    if "longform" in tool_name:
        monkeypatch.setenv("STUDIO_RUNPOD_LONGFORM_ENABLED", "1")
    calls = []

    def dispatch(name, arguments, **kwargs):
        calls.append((name, dict(arguments), kwargs))
        return {
            "ok": True,
            "status": "accepted",
            "dispatch_id": "rpd_" + "d" * 40,
            "runpod_job_id": "runpod-start-1",
        }

    monkeypatch.setattr(runpod_bridge, "dispatch_production_tool", dispatch)
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("RunPod start executed locally"),
    )

    arguments = {"command_id": "start-command-1"}
    if tool_name == "start_longform_render":
        arguments.update({"channel_key": "history_rewind", "title": "Test", "topic": "Test"})
    first = json.loads(_call(tool_name, arguments))
    second = json.loads(_call(tool_name, arguments))

    assert first["job_id"].startswith(expected_prefix)
    assert second["job_id"] == first["job_id"]
    assert calls[0][1]["studio_job_id"] == first["job_id"]
    assert calls[0][1]["_requested_job_id"] == first["job_id"]
    assert calls[1][1]["studio_job_id"] == first["job_id"]


def test_runpod_longform_is_default_off_without_local_fallback(monkeypatch):
    monkeypatch.setattr(tools, "_require_longform_entitlement", lambda _user_id: None)
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: pytest.fail("disabled longform reached RunPod"),
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("disabled longform fell back locally"),
    )

    with pytest.raises(RuntimeError, match="STUDIO_RUNPOD_LONGFORM_ENABLED"):
        _call(
            "start_longform_render",
            {
                "command_id": "longform-disabled",
                "channel_key": "history_rewind",
                "title": "Test",
                "topic": "Test",
            },
        )


def test_read_and_poll_tool_never_dispatches_even_when_enabled(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    dispatch_calls = []
    local_calls = []
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *args, **kwargs: dispatch_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda name, arguments, **kwargs: local_calls.append((name, arguments, kwargs)) or '{"phase": "running"}',
    )

    result = json.loads(_call("poll_render_job", {"job_id": "studio-job-1"}))

    assert result["phase"] == "running"
    assert len(local_calls) == 1
    assert dispatch_calls == []


@pytest.mark.parametrize("enabled_value", ["1", "true", "yes", "on", "enabled"])
def test_every_enabled_flag_token_preserves_runpod_poll_ownership(
    monkeypatch,
    enabled_value,
):
    from studio_agent import runpod_reconciliation

    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", enabled_value)

    assert tools._runpod_production_enabled() is True
    assert runpod_reconciliation.runpod_production_enabled() is True


def test_enabled_production_without_stable_id_fails_closed(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "yes")
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: pytest.fail("missing identity reached RunPod"),
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("missing identity fell back locally"),
    )

    with pytest.raises(RuntimeError, match="stable command_id"):
        _call("expand_visual_proof_shortform", {"job_id": "studio-job-1"})


@pytest.mark.parametrize(
    "tool_name,arguments",
    [
        (
            "expand_visual_proof_shortform",
            {"job_id": "studio-job-1", "command_id": "expand-storage-missing"},
        ),
        (
            "start_shortform_generate",
            {"command_id": "start-storage-missing"},
        ),
    ],
)
def test_missing_storage_fails_before_credit_or_dispatch(
    monkeypatch,
    tool_name,
    arguments,
):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    monkeypatch.setattr(
        runpod_storage,
        "assert_configured",
        lambda: (_ for _ in ()).throw(
            runpod_storage.RunPodStorageConfigurationError("S3 return path missing")
        ),
    )
    monkeypatch.setattr(
        production_budget,
        "enforce_budget",
        lambda *_args, **_kwargs: pytest.fail("storage failure reached credit budgeting"),
    )
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: pytest.fail("storage failure reached RunPod"),
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("storage failure fell back locally"),
    )

    with pytest.raises(runpod_storage.RunPodStorageConfigurationError, match="return path"):
        _call(tool_name, arguments)


def test_stream_context_supplies_stable_command_identity(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "on")
    seen = []
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda _name, _arguments, **kwargs: seen.append(kwargs["command_id"])
        or {"ok": True, "status": "accepted", "runpod_job_id": "runpod-ctx"},
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("context-routed production executed locally"),
    )

    with production_command_scope("stream-run-123"):
        _call("expand_visual_proof_shortform", {"job_id": "studio-job-1"})

    assert seen == ["stream-run-123"]


def test_runpod_preflight_failure_releases_credit_hold(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    estimate = production_budget.BudgetEstimate(
        tool="expand_visual_proof_shortform",
        estimated_usd=1.25,
        max_budget_usd=8.0,
        mode="test",
        breakdown={},
    )
    reservation = {"reservation_id": "reserve-1", "credits": 150, "unlimited": False}
    released = []
    monkeypatch.setattr(production_budget, "enforce_budget", lambda *_args, **_kwargs: estimate)
    monkeypatch.setattr(unified_credits, "reserve_usd", lambda *_args, **_kwargs: dict(reservation))
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda user_id, reservation_id, *, reason: released.append((user_id, reservation_id, reason)),
    )
    monkeypatch.setattr(
        unified_credits,
        "commit_reservation",
        lambda *_args, **_kwargs: pytest.fail("failed RunPod dispatch committed credits"),
    )
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            runpod_bridge.RunPodPaymentRequired("funding required", status_code=402)
        ),
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: pytest.fail("failed RunPod dispatch fell back locally"),
    )

    with pytest.raises(runpod_bridge.RunPodPaymentRequired):
        _call(
            "expand_visual_proof_shortform",
            {"job_id": "studio-job-1", "command_id": "command-payment"},
        )

    assert released == [
        (
            "user-1",
            "reserve-1",
            "studio_tool_runpod_failed:expand_visual_proof_shortform",
        )
    ]


def test_accepted_runpod_dispatch_keeps_hold_pending_without_local_commit(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    estimate = production_budget.BudgetEstimate(
        tool="expand_visual_proof_shortform",
        estimated_usd=1.25,
        max_budget_usd=8.0,
        mode="test",
        breakdown={},
    )
    reservation = {"reservation_id": "reserve-2", "credits": 150, "unlimited": False}
    releases = []
    monkeypatch.setattr(production_budget, "enforce_budget", lambda *_args, **_kwargs: estimate)
    monkeypatch.setattr(unified_credits, "reserve_usd", lambda *_args, **_kwargs: dict(reservation))
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *_args, **_kwargs: releases.append((_args, _kwargs)),
    )
    monkeypatch.setattr(
        unified_credits,
        "commit_reservation",
        lambda *_args, **_kwargs: pytest.fail("accepted RunPod dispatch committed estimate locally"),
    )
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "accepted",
            "runpod_job_id": "runpod-pending",
            "idempotent_replay": False,
        },
    )

    result = json.loads(
        _call(
            "expand_visual_proof_shortform",
            {"job_id": "studio-job-1", "command_id": "command-pending"},
        )
    )

    assert releases == []
    assert result["credits"]["reservation_id"] == "reserve-2"
    assert result["credits"]["charged"] == 0
    assert result["credits"]["status"] == "pending_worker_cost_reconciliation"


def test_ambiguous_post_failure_keeps_hold_for_later_reconciliation(monkeypatch):
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    estimate = production_budget.BudgetEstimate(
        tool="expand_visual_proof_shortform",
        estimated_usd=1.25,
        max_budget_usd=8.0,
        mode="test",
        breakdown={},
    )
    reservation = {"reservation_id": "reserve-ambiguous", "credits": 150, "unlimited": False}
    releases = []
    monkeypatch.setattr(production_budget, "enforce_budget", lambda *_args, **_kwargs: estimate)
    monkeypatch.setattr(unified_credits, "reserve_usd", lambda *_args, **_kwargs: dict(reservation))
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *_args, **_kwargs: releases.append((_args, _kwargs)),
    )
    monkeypatch.setattr(
        runpod_bridge,
        "dispatch_production_tool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            runpod_bridge.RunPodEndpointUnavailable("response lost after POST")
        ),
    )
    monkeypatch.setattr(
        runpod_bridge,
        "get_dispatch_receipt",
        lambda _dispatch_id: {
            "status": "dispatch_unknown",
            "fail_closed": True,
        },
    )

    with pytest.raises(runpod_bridge.RunPodEndpointUnavailable):
        _call(
            "expand_visual_proof_shortform",
            {"job_id": "studio-job-1", "command_id": "command-ambiguous"},
        )

    assert releases == []
