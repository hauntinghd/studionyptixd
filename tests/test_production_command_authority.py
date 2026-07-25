from __future__ import annotations

import asyncio
import json

import pytest
from starlette.requests import Request

from studio_agent import production_budget, store, tools
from studio_agent.direct_production import execute_logged_production
from studio_agent.execution_context import (
    ProductionCommandViolation,
    authorize_production_mutation,
    current_production_command,
    production_command_scope,
)
from studio_agent.production_command_state import build_session_production_view


def _session(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    return store.create_session(user_id="creator-1", model="claude-haiku-4-5")


def test_protected_tool_rejects_before_budget_without_backend_authority(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_bind_backend_scene_scope",
        lambda *_args, **_kwargs: pytest.fail("unauthorized mutation resolved scene state"),
    )
    monkeypatch.setattr(
        production_budget,
        "enforce_budget",
        lambda *_args, **_kwargs: pytest.fail("unauthorized mutation reached budget"),
    )

    with pytest.raises(
        ProductionCommandViolation,
        match="no backend production command authority",
    ):
        tools.execute_tool_logged(
            "cancel_production_job",
            {"job_id": "job-1"},
            user_id="creator-1",
            content_format="short",
            session_id="session-1",
        )


def test_command_authority_is_bound_to_exact_user_and_session():
    with production_command_scope(
        "command-1",
        user_id="creator-1",
        session_id="session-1",
        source="test",
    ):
        with pytest.raises(ProductionCommandViolation, match="command user"):
            authorize_production_mutation(
                "cancel_production_job",
                {"job_id": "job-1"},
                user_id="creator-2",
                session_id="session-1",
            )
        with pytest.raises(ProductionCommandViolation, match="command session"):
            authorize_production_mutation(
                "cancel_production_job",
                {"job_id": "job-1"},
                user_id="creator-1",
                session_id="session-2",
            )


def test_one_parent_command_gets_deterministic_non_colliding_step_ids():
    with production_command_scope(
        "ship-command",
        user_id="creator-1",
        session_id="session-1",
        source="test",
    ):
        approve_args, approve = authorize_production_mutation(
            "set_production_scenes_animate",
            {"job_id": "job-1", "scene_indices": [0, 1], "animate": True},
            user_id="creator-1",
            session_id="session-1",
        )
        replay_args, replay = authorize_production_mutation(
            "set_production_scenes_animate",
            {"job_id": "job-1", "scene_indices": [0, 1], "animate": True},
            user_id="creator-1",
            session_id="session-1",
        )
        animate_args, animate = authorize_production_mutation(
            "animate_production_scenes",
            {"job_id": "job-1", "scene_indices": [0, 1]},
            user_id="creator-1",
            session_id="session-1",
        )

    assert approve is not None and replay is not None and animate is not None
    assert approve.command_id == replay.command_id == animate.command_id == "ship-command"
    assert approve.mutation_id == replay.mutation_id
    assert approve.mutation_id != animate.mutation_id
    assert approve_args["command_id"] == replay_args["command_id"] == approve.mutation_id
    assert animate_args["command_id"] == animate.mutation_id


def test_logged_mutation_persists_v2_envelope_and_canonical_projection(
    tmp_path,
    monkeypatch,
):
    session = _session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    monkeypatch.setattr(tools.telemetry, "record_tool_call", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(production_budget, "enforce_budget", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        production_budget,
        "with_budget_metadata",
        lambda result, _estimate, _arguments=None: result,
    )
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: json.dumps(
            {
                "ok": True,
                "status": "complete",
                "postcondition_verified": True,
                "job_id": "job-1",
            }
        ),
    )

    before = int(session.get("production_view_revision") or 0)
    with production_command_scope(
        "cancel-command",
        user_id="creator-1",
        session_id=session_id,
        source="server_workflow",
        user_text="Cancel this render.",
    ):
        result = json.loads(
            tools.execute_tool_logged(
                "cancel_production_job",
                {"job_id": "job-1"},
                user_id="creator-1",
                content_format="short",
                session_id=session_id,
            )
        )

    assert result["postcondition_verified"] is True
    persisted = store.get_session(session_id, user_id="creator-1")
    assert persisted is not None
    latest = persisted["latest_production_command"]
    step = latest["steps"][-1]
    envelope = step["command_envelope"]
    assert envelope["schema_version"] == "production-command-v2"
    assert envelope["action"] == "cancel"
    assert envelope["target"]["job_id"] == "job-1"
    assert envelope["target"]["owner_session_id"] == session_id
    assert envelope["target"]["owner_user_id"] == "creator-1"
    assert envelope["idempotency_key"].startswith("mut_")
    assert int(persisted["production_view_revision"]) > before

    view = build_session_production_view(persisted)
    assert view.session_id == session_id
    assert view.command is not None
    assert view.command.action == "cancel"
    assert view.command.lifecycle == "completed"
    assert view.command.target_job_id == "job-1"


def test_production_view_revision_is_monotonic_for_all_session_writes(
    tmp_path,
    monkeypatch,
):
    session = _session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    first = int(session["production_view_revision"])
    updated = store.update_session(session_id, pending_actions=[])
    second = int(updated["production_view_revision"])
    updated = store.update_session(session_id, active_jobs=[])
    third = int(updated["production_view_revision"])

    assert first < second < third


def test_production_view_keeps_legacy_active_job_identity_visible(
    tmp_path,
    monkeypatch,
):
    session = _session(tmp_path, monkeypatch)
    persisted = store.update_session(
        session["session_id"],
        active_jobs=[
            {
                "job_id": "sf_exact_123",
                "kind": "shortform",
                "title": "Existing short",
                "started_at": 1.0,
            }
        ],
    )

    view = build_session_production_view(persisted)

    assert [job.job_id for job in view.jobs] == ["sf_exact_123"]
    assert view.jobs[0].status == "running"


def test_direct_production_panel_issues_backend_authority(monkeypatch):
    def execute(name, arguments, **context):
        authority = current_production_command()
        assert authority is not None
        assert authority.command_id == "direct-click-1"
        assert authority.user_id == "creator-1"
        assert authority.source == "server_workflow"
        assert name == "cancel_production_job"
        assert arguments == {"job_id": "job-1"}
        assert context == {
            "user_id": "creator-1",
            "content_format": "short",
        }
        return json.dumps({"ok": True, "status": "complete"})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    request = Request(
        {
            "type": "http",
            "headers": [(b"x-idempotency-key", b"direct-click-1")],
        }
    )

    result = asyncio.run(
        execute_logged_production(
            "cancel_production_job",
            {"job_id": "job-1"},
            request=request,
            user_id="creator-1",
            content_format="short",
        )
    )

    assert result["status"] == "complete"
