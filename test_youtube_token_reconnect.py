import asyncio
import time

import youtube


def test_connected_channel_access_token_persists_reconnect_required(monkeypatch):
    user_id = "user-1"
    channel_id = "UC-selected"
    youtube._youtube_connections = {
        user_id: {
            "default_channel_id": channel_id,
            "channels": {
                channel_id: {
                    "channel_id": channel_id,
                    "refresh_token": "dead-refresh",
                    "token_expires_at": 0,
                }
            },
        }
    }

    async def fail_refresh(_record):
        raise RuntimeError("invalid_grant: token has been expired or revoked")

    async def no_repair(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(youtube, "_load_youtube_connections", lambda: None)
    monkeypatch.setattr(youtube, "_reload_youtube_connections_from_supabase", lambda: False)
    monkeypatch.setattr(youtube, "_save_youtube_connections", lambda: None)
    monkeypatch.setattr(youtube, "_youtube_ensure_access_token", fail_refresh)
    monkeypatch.setattr(youtube, "_youtube_repair_channel_record_from_sibling", no_repair)

    access_token, record = asyncio.run(
        youtube._youtube_connected_channel_access_token({"id": user_id}, channel_id)
    )

    saved = youtube._youtube_connections[user_id]["channels"][channel_id]
    assert access_token == ""
    assert "invalid_grant" in record["last_sync_error"]
    assert saved["last_sync_error"] == record["last_sync_error"]
    assert saved["token_refresh_retry_at"] > time.time()


def test_connected_channel_access_token_repairs_using_selected_channel_key_first(monkeypatch):
    user_id = "user-1"
    selected_channel_id = "UC-selected"
    stale_record_channel_id = "UC-stale"
    repair_attempts: list[str] = []
    youtube._youtube_connections = {
        user_id: {
            "default_channel_id": selected_channel_id,
            "channels": {
                selected_channel_id: {
                    "channel_id": stale_record_channel_id,
                    "refresh_token": "dead-refresh",
                    "token_expires_at": 0,
                }
            },
        }
    }

    async def ensure(record):
        if record.get("refresh_token") == "good-refresh":
            return "live-access", {
                **record,
                "access_token": "live-access",
                "token_expires_at": time.time() + 3600,
            }
        raise RuntimeError("invalid_grant: token has been expired or revoked")

    async def repair(_user_id, repair_channel_id, _record):
        repair_attempts.append(repair_channel_id)
        if repair_channel_id == selected_channel_id:
            return {
                "channel_id": selected_channel_id,
                "refresh_token": "good-refresh",
                "token_expires_at": 0,
            }
        return {}

    monkeypatch.setattr(youtube, "_load_youtube_connections", lambda: None)
    monkeypatch.setattr(youtube, "_reload_youtube_connections_from_supabase", lambda: False)
    monkeypatch.setattr(youtube, "_save_youtube_connections", lambda: None)
    monkeypatch.setattr(youtube, "_youtube_ensure_access_token", ensure)
    monkeypatch.setattr(youtube, "_youtube_repair_channel_record_from_sibling", repair)

    access_token, record = asyncio.run(
        youtube._youtube_connected_channel_access_token({"id": user_id}, selected_channel_id)
    )

    assert access_token == "live-access"
    assert record["channel_id"] == selected_channel_id
    assert repair_attempts == [selected_channel_id]
    assert youtube._youtube_connections[user_id]["channels"][selected_channel_id]["access_token"] == "live-access"
