from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend_youtube_catalyst_routes
import catalyst
import catalyst_references_router
from studio_agent import idempotent_mutations
from studio_agent.command_execution import InMemoryExecutionLedger
from studio_agent.execution_context import current_production_command


@pytest.fixture(autouse=True)
def _isolated_command_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        idempotent_mutations,
        "_LEDGER",
        InMemoryExecutionLedger(),
    )


def _hub_client(tmp_path: Path, calls: list[str]) -> TestClient:
    user = {"id": "catalyst-owner", "email": "owner@example.test"}

    async def get_current_user_from_request(_request):
        return dict(user)

    async def mutation(name: str, **_kwargs):
        authority = current_production_command()
        assert authority is not None
        assert authority.user_id == user["id"]
        calls.append(name)
        return {"ok": True, "marker": len(calls), "name": name}

    async def noop(**_kwargs):
        return {}

    app = FastAPI()
    app.include_router(
        backend_youtube_catalyst_routes.build_youtube_catalyst_app_router(
            require_auth=lambda: dict(user),
            get_current_user=lambda *_args, **_kwargs: dict(user),
            get_current_user_from_request=get_current_user_from_request,
            youtube_start_oauth_for_user=noop,
            youtube_start_oauth_browser_redirect=noop,
            google_youtube_oauth_installed_helper_response=noop,
            google_youtube_oauth_complete_redirect=noop,
            google_youtube_oauth_callback_redirect=noop,
            catalyst_hub_snapshot_for_user=lambda **_kwargs: mutation("snapshot"),
            catalyst_hub_refresh_for_user=lambda **kwargs: mutation("refresh", **kwargs),
            catalyst_hub_reference_video_analysis_for_user=lambda **kwargs: mutation("analysis", **kwargs),
            catalyst_hub_reference_video_analysis_manual_for_user=lambda **kwargs: mutation("manual", **kwargs),
            catalyst_hub_clear_reference_video_analysis_for_user=lambda **kwargs: mutation("clear", **kwargs),
            catalyst_hub_save_instructions_for_user=lambda **kwargs: mutation("instructions", **kwargs),
            catalyst_hub_launch_longform_for_user=lambda **kwargs: mutation("launch", **kwargs),
            catalyst_hub_longform_suggestions_for_user=lambda **kwargs: mutation("suggestions", **kwargs),
            list_connected_youtube_channels_for_user=lambda **kwargs: mutation("list_channels", **kwargs),
            select_connected_youtube_channel_for_user=lambda **kwargs: mutation("select_channel", **kwargs),
            sync_connected_youtube_channel_for_user=lambda **kwargs: mutation("sync_channel", **kwargs),
            sync_connected_youtube_channel_outcomes_for_user=lambda **kwargs: mutation("sync_outcomes", **kwargs),
            disconnect_connected_youtube_channel_for_user=lambda **kwargs: mutation("disconnect_channel", **kwargs),
            bool_from_any=lambda value, default=False: default if value is None else bool(value),
            catalyst_reference_analysis_default_minutes=20.0,
            upload_dir=tmp_path,
            longform_owner_beta_enabled=lambda _user: True,
            harvest_catalyst_outcomes_for_channel=noop,
            youtube_upload_video_for_user=lambda **kwargs: mutation("upload_longform", **kwargs),
            youtube_upload_short_for_user=lambda **kwargs: mutation("upload_short", **kwargs),
            youtube_get_velocity_for_user=lambda **kwargs: mutation("velocity", **kwargs),
        )
    )
    return TestClient(app)


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("post", "/api/catalyst/hub/refresh", {"json": {}}),
        ("post", "/api/catalyst/hub/reference-video-analysis", {"json": {}}),
        ("post", "/api/catalyst/hub/reference-video-analysis/manual", {"data": {}}),
        ("post", "/api/catalyst/hub/reference-video-analysis/clear", {"json": {}}),
        ("post", "/api/catalyst/hub/instructions", {"json": {}}),
        ("post", "/api/catalyst/hub/launch", {"json": {}}),
        ("post", "/api/catalyst/hub/longform-suggestions", {"json": {}}),
        ("post", "/api/catalyst/hub/auto-tick", {"data": {}}),
        ("post", "/api/catalyst/hub/auto-pilot", {"data": {}}),
        ("post", "/api/catalyst/hub/upload", {"data": {}}),
        ("post", "/api/short/upload-to-youtube", {"data": {}}),
        ("post", "/api/youtube/channels/channel-1/sync", {}),
        ("post", "/api/youtube/channels/channel-1/sync-outcomes", {"json": {}}),
        ("get", "/api/youtube/channels?sync=true", {}),
    ],
)
def test_catalyst_mutations_require_key_before_callback(
    tmp_path: Path,
    method: str,
    path: str,
    kwargs: dict[str, Any],
) -> None:
    calls: list[str] = []
    response = getattr(_hub_client(tmp_path, calls), method)(path, **kwargs)

    assert response.status_code == 400, response.text
    assert "X-Idempotency-Key is required" in response.text
    assert calls == []


def test_hub_refresh_claims_authority_and_replays_exactly_once(tmp_path: Path) -> None:
    calls: list[str] = []
    client = _hub_client(tmp_path, calls)
    headers = {"X-Idempotency-Key": "catalyst-refresh-command-1"}
    body = {
        "channel_id": "channel-1",
        "include_public_benchmarks": True,
        "refresh_outcomes": True,
    }

    first = client.post("/api/catalyst/hub/refresh", headers=headers, json=body)
    replay = client.post("/api/catalyst/hub/refresh", headers=headers, json=body)
    conflict = client.post(
        "/api/catalyst/hub/refresh",
        headers=headers,
        json={**body, "refresh_outcomes": False},
    )

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["marker"] == first.json()["marker"]
    assert replay.json()["idempotent_replay"] is True
    assert conflict.status_code == 409
    assert "different production mutation" in conflict.text
    assert calls == ["refresh"]


def test_hub_get_uses_only_cached_state_and_never_refreshes_or_rebuilds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_calls: list[str] = []
    user_id = "catalyst-owner"
    channel_id = "channel-1"
    channel_record = {
        "channel_id": channel_id,
        "title": "Cached Channel",
        "analytics_snapshot": {
            "channel_summary": "Stored summary",
            "recent_upload_titles": ["Stored title"],
            "uploaded_videos": [{"video_id": "video-1", "title": "Stored title"}],
            "channel_audit": {"summary": "Stored audit"},
        },
    }
    memory_key = f"{user_id}:{channel_id}:documentary"
    memory_store = {
        memory_key: {
            "summary": "Stored memory",
            "reference_video_analysis": {
                "video": {"video_id": "video-1", "title": "Stored reference"},
                "evidence": {"analysis_mode": "preview_frames"},
                "analysis": {"summary": "Stored analysis"},
            },
        }
    }

    async def forbidden(name: str, *_args, **_kwargs):
        provider_calls.append(name)
        raise AssertionError(f"GET contacted {name}")

    monkeypatch.setattr(catalyst, "CATALYST_HUB_SHORT_WORKSPACES", [])
    monkeypatch.setattr(catalyst, "CATALYST_HUB_LONGFORM_WORKSPACES", ["documentary"])
    monkeypatch.setattr(catalyst, "_is_admin_user", lambda _user: True)
    monkeypatch.setattr(catalyst, "_youtube_connections_lock", asyncio.Lock())
    monkeypatch.setattr(catalyst, "_load_youtube_connections", lambda: None)
    monkeypatch.setattr(
        catalyst,
        "_youtube_bucket_for_user",
        lambda _uid: {
            "default_channel_id": channel_id,
            "channels": {channel_id: dict(channel_record)},
        },
    )
    monkeypatch.setattr(catalyst, "_youtube_connection_public_view", lambda row: dict(row or {}))
    monkeypatch.setattr(catalyst, "_youtube_sync_and_persist_for_user", lambda *a, **k: forbidden("youtube_sync", *a, **k))
    monkeypatch.setattr(catalyst, "_youtube_selected_channel_context", lambda *a, **k: forbidden("channel_context", *a, **k))
    monkeypatch.setattr(catalyst, "_youtube_connected_channel_access_token", lambda *a, **k: forbidden("access_token", *a, **k))
    monkeypatch.setattr(catalyst, "_youtube_fetch_public_channel_page_videos", lambda *a, **k: forbidden("public_inventory", *a, **k))
    monkeypatch.setattr(catalyst, "_harvest_catalyst_outcomes_for_channel", lambda *a, **k: forbidden("outcomes", *a, **k))
    monkeypatch.setattr(catalyst, "_persist_public_shorts_playbook_memory", lambda *a, **k: forbidden("benchmarks", *a, **k))
    monkeypatch.setattr(catalyst, "_build_catalyst_reference_video_analysis", lambda *a, **k: forbidden("analysis_rebuild", *a, **k))
    monkeypatch.setattr(catalyst, "_catalyst_memory_lock", asyncio.Lock())
    monkeypatch.setattr(catalyst, "_load_catalyst_memory", lambda: None)
    monkeypatch.setattr(catalyst, "_catalyst_channel_memory_getter", lambda: memory_store)
    monkeypatch.setattr(
        catalyst,
        "_catalyst_channel_memory_key",
        lambda uid, cid, workspace: f"{uid}:{cid}:{workspace}",
    )
    monkeypatch.setattr(catalyst, "_catalyst_learning_records_getter", lambda: {})
    monkeypatch.setattr(
        catalyst,
        "_reconcile_reference_video_analysis_with_inventory",
        lambda analysis, _context: dict(analysis or {}),
    )
    monkeypatch.setattr(
        catalyst,
        "_catalyst_reference_video_analysis_public_view",
        lambda analysis: dict(analysis or {}),
    )
    monkeypatch.setattr(
        catalyst,
        "_resolve_catalyst_series_context",
        lambda *_args, channel_memory=None, **_kwargs: {
            "memory_view": dict(channel_memory or {}),
            "selected_cluster": {},
        },
    )
    monkeypatch.setattr(
        catalyst,
        "_catalyst_channel_memory_public_view",
        lambda row: dict(row or {}),
    )

    payload = asyncio.run(
        catalyst._catalyst_hub_snapshot_for_user(
            user={"id": user_id},
            channel_id=channel_id,
            refresh=True,
        )
    )

    assert payload["selected_channel"]["analytics_snapshot"]["recent_upload_titles"] == ["Stored title"]
    assert (
        payload["workspace_snapshots"]["documentary"]["reference_video_analysis"]["analysis"]["summary"]
        == "Stored analysis"
    )
    assert provider_calls == []


def test_reference_ingest_requires_key_and_replays_provider_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def ingest(**kwargs):
        authority = current_production_command()
        assert authority is not None
        calls.append(str(kwargs["url"]))
        return {"id": "ref-1", **kwargs}

    monkeypatch.setattr(catalyst_references_router, "ingest_reference_video", ingest)
    app = FastAPI()
    app.include_router(
        catalyst_references_router.build_catalyst_references_router(
            require_auth=lambda: {"id": "catalyst-owner"},
        )
    )
    client = TestClient(app)
    body = {
        "url": "https://www.youtube.com/watch?v=abcdefghijk",
        "channel_key": "empire_magnates",
        "notes": "Winning hook",
    }

    missing = client.post("/api/catalyst/references", json=body)
    first = client.post(
        "/api/catalyst/references",
        headers={"X-Idempotency-Key": "catalyst-reference-command-1"},
        json=body,
    )
    replay = client.post(
        "/api/catalyst/references",
        headers={"X-Idempotency-Key": "catalyst-reference-command-1"},
        json=body,
    )

    assert missing.status_code == 400
    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json()["reference"] == first.json()["reference"]
    assert replay.json()["idempotent_replay"] is True
    assert calls == [body["url"]]


def test_catalyst_frontends_use_durable_production_leases() -> None:
    root = Path(__file__).resolve().parents[1] / "ViralShorts-App" / "src" / "studio" / "panels"
    panel = (root / "CatalystPanel.tsx").read_text(encoding="utf-8")
    references = (root / "CatalystReferencesSection.tsx").read_text(encoding="utf-8")

    assert "acquireProductionCommandLease" in panel
    assert "fetchCatalystProductionJson" in panel
    assert "fetchCatalystProductionResponse" in panel
    assert "X-Idempotency-Key" in panel
    assert "acquireProductionCommandLease" in references
    assert "fetchReferenceMutation" in references
    assert "X-Idempotency-Key" in references
