from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

import youtube
import youtube_connections_store


def _key(seed: int) -> str:
    raw = bytes((seed + offset) % 256 for offset in range(32))
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _connections() -> dict:
    return {
        "user-1": {
            "default_channel_id": "channel-1",
            "channels": {
                "channel-1": {
                    "channel_id": "channel-1",
                    "channel_title": "Release channel",
                    "access_token": "access-plain-secret",
                    "refresh_token": "refresh-plain-secret",
                }
            },
        }
    }


def _reset(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    *,
    key_seed: int | None = 1,
) -> None:
    monkeypatch.setattr(
        youtube,
        "YOUTUBE_CONNECTIONS_FILE",
        tmp_path / "youtube_connections.json",
    )
    monkeypatch.setattr(youtube, "_youtube_connections", {})
    monkeypatch.setattr(
        youtube,
        "_youtube_connections_hydrated_from_supabase",
        False,
    )
    monkeypatch.setattr(youtube, "_youtube_token_box_cache", None)
    monkeypatch.setattr(youtube_connections_store, "configured", lambda: False)
    monkeypatch.delenv(
        "YOUTUBE_TOKEN_ENCRYPTION_KEY_PREVIOUS",
        raising=False,
    )
    if key_seed is None:
        monkeypatch.delenv("YOUTUBE_TOKEN_ENCRYPTION_KEY", raising=False)
    else:
        monkeypatch.setenv("YOUTUBE_TOKEN_ENCRYPTION_KEY", _key(key_seed))


def test_connection_cache_encrypts_tokens_and_round_trips(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path)
    monkeypatch.setattr(youtube, "_youtube_connections", _connections())

    youtube._save_youtube_connections()

    raw = youtube.YOUTUBE_CONNECTIONS_FILE.read_text(encoding="utf-8")
    assert "access-plain-secret" not in raw
    assert "refresh-plain-secret" not in raw
    stored = json.loads(raw)
    record = stored["user-1"]["channels"]["channel-1"]
    assert record["access_token"].startswith("sbx1:")
    assert record["refresh_token"].startswith("sbx1:")
    assert (
        youtube._youtube_connections["user-1"]["channels"]["channel-1"][
            "access_token"
        ]
        == "access-plain-secret"
    )

    monkeypatch.setattr(youtube, "_youtube_connections", {})
    youtube._load_youtube_connections()
    hydrated = youtube._youtube_connections["user-1"]["channels"]["channel-1"]
    assert hydrated["access_token"] == "access-plain-secret"
    assert hydrated["refresh_token"] == "refresh-plain-secret"


def test_plaintext_supabase_rows_are_rewritten_as_ciphertext(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path)
    captured: list[dict] = []
    monkeypatch.setattr(youtube_connections_store, "configured", lambda: True)
    monkeypatch.setattr(
        youtube_connections_store,
        "hydrate",
        lambda: _connections(),
    )
    monkeypatch.setattr(
        youtube_connections_store,
        "upsert",
        lambda _user_id, _channel_id, record, is_default=False: (
            captured.append(dict(record)) or True
        ),
    )
    monkeypatch.setattr(
        youtube_connections_store,
        "clear_default_except",
        lambda _user_id, _channel_id: True,
    )

    youtube._load_youtube_connections()

    assert captured
    assert captured[0]["access_token"].startswith("sbx1:")
    assert captured[0]["refresh_token"].startswith("sbx1:")
    assert "access-plain-secret" not in json.dumps(captured)
    in_memory = youtube._youtube_connections["user-1"]["channels"]["channel-1"]
    assert in_memory["access_token"] == "access-plain-secret"


def test_failed_plaintext_migration_is_loud_and_retried(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path)
    original_memory = {"already": {"channels": {}}}
    monkeypatch.setattr(youtube, "_youtube_connections", original_memory)
    monkeypatch.setattr(youtube_connections_store, "configured", lambda: True)
    monkeypatch.setattr(
        youtube_connections_store,
        "hydrate",
        lambda: _connections(),
    )
    monkeypatch.setattr(
        youtube_connections_store,
        "upsert",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        youtube_connections_store,
        "clear_default_except",
        lambda *_args, **_kwargs: True,
    )

    with pytest.raises(RuntimeError, match="migration"):
        youtube._load_youtube_connections()

    assert youtube._youtube_connections is original_memory
    assert youtube._youtube_connections_hydrated_from_supabase is False


def test_connection_save_fails_closed_without_encryption_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path, key_seed=None)
    monkeypatch.setattr(youtube, "_youtube_connections", _connections())

    with pytest.raises(RuntimeError, match="YOUTUBE_TOKEN_ENCRYPTION_KEY"):
        youtube._save_youtube_connections()

    assert not youtube.YOUTUBE_CONNECTIONS_FILE.exists()


def test_ciphertext_cannot_be_loaded_with_wrong_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path, key_seed=3)
    stored = youtube._youtube_connections_for_storage(_connections())
    monkeypatch.setenv("YOUTUBE_TOKEN_ENCRYPTION_KEY", _key(9))
    monkeypatch.setattr(youtube, "_youtube_token_box_cache", None)

    with pytest.raises(RuntimeError, match="could not be decrypted"):
        youtube._youtube_connections_for_memory(stored)


def test_wrong_key_load_is_loud_and_preserves_existing_memory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path, key_seed=3)
    youtube._write_youtube_connections_cache(
        youtube._youtube_connections_for_storage(_connections())
    )
    original_memory = {"already": {"channels": {}}}
    monkeypatch.setattr(youtube, "_youtube_connections", original_memory)
    monkeypatch.setenv("YOUTUBE_TOKEN_ENCRYPTION_KEY", _key(9))
    monkeypatch.setattr(youtube, "_youtube_token_box_cache", None)

    with pytest.raises(RuntimeError, match="could not be decrypted"):
        youtube._load_youtube_connections()

    assert youtube._youtube_connections is original_memory


def test_previous_key_is_read_and_rewritten_under_primary_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path, key_seed=4)
    previous_ciphertext = youtube._youtube_connections_for_storage(_connections())
    monkeypatch.setenv("YOUTUBE_TOKEN_ENCRYPTION_KEY", _key(8))
    monkeypatch.setenv("YOUTUBE_TOKEN_ENCRYPTION_KEY_PREVIOUS", _key(4))
    monkeypatch.setattr(youtube, "_youtube_token_box_cache", None)

    in_memory, rewritten, migration_required = (
        youtube._prepare_youtube_connections_payload(previous_ciphertext)
    )

    assert migration_required is True
    assert (
        in_memory["user-1"]["channels"]["channel-1"]["access_token"]
        == "access-plain-secret"
    )
    monkeypatch.delenv(
        "YOUTUBE_TOKEN_ENCRYPTION_KEY_PREVIOUS",
        raising=False,
    )
    monkeypatch.setattr(youtube, "_youtube_token_box_cache", None)
    assert (
        youtube._youtube_connections_for_memory(rewritten)["user-1"]["channels"][
            "channel-1"
        ]["refresh_token"]
        == "refresh-plain-secret"
    )
    assert (
        youtube._youtube_connections_require_reencryption(rewritten)
        is False
    )


def test_remote_fallback_receives_only_ciphertext(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path)
    monkeypatch.setattr(youtube, "_youtube_connections", _connections())
    captured: list[dict] = []
    monkeypatch.setattr(
        youtube,
        "_write_youtube_connections_cache",
        lambda _payload: (_ for _ in ()).throw(OSError("disk unavailable")),
    )
    monkeypatch.setattr(youtube_connections_store, "configured", lambda: True)
    monkeypatch.setattr(
        youtube_connections_store,
        "upsert",
        lambda _user_id, _channel_id, record, is_default=False: (
            captured.append(dict(record)) or True
        ),
    )
    monkeypatch.setattr(
        youtube_connections_store,
        "clear_default_except",
        lambda _user_id, _channel_id: True,
    )

    youtube._save_youtube_connections()

    assert captured
    assert "access-plain-secret" not in json.dumps(captured)
    assert captured[0]["access_token"].startswith("sbx1:")


def test_configured_authoritative_store_failure_is_not_hidden_by_disk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path)
    monkeypatch.setattr(youtube, "_youtube_connections", _connections())
    monkeypatch.setattr(youtube_connections_store, "configured", lambda: True)
    monkeypatch.setattr(
        youtube_connections_store,
        "upsert",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        youtube_connections_store,
        "clear_default_except",
        lambda *_args, **_kwargs: True,
    )

    with pytest.raises(RuntimeError, match="authoritative store"):
        youtube._save_youtube_connections()

    raw = youtube.YOUTUBE_CONNECTIONS_FILE.read_text(encoding="utf-8")
    assert "access-plain-secret" not in raw


def test_persistence_module_rejects_plaintext_token_fields() -> None:
    with pytest.raises(ValueError, match="encrypted sbx1: envelope"):
        youtube_connections_store._split_record(
            {
                "access_token": "plaintext-is-forbidden",
                "refresh_token": "sbx1:ciphertext",
            }
        )


def test_cache_is_private_and_leaves_no_temporary_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _reset(monkeypatch, tmp_path)
    payload = youtube._youtube_connections_for_storage(_connections())

    youtube._write_youtube_connections_cache(payload)

    assert not list(tmp_path.glob("*.tmp"))
    if youtube.os.name != "nt":
        assert youtube.YOUTUBE_CONNECTIONS_FILE.stat().st_mode & 0o777 == 0o600


def test_public_channel_view_never_emits_token_material() -> None:
    record = _connections()["user-1"]["channels"]["channel-1"]
    public = youtube._youtube_connection_public_view(record)

    assert "access_token" not in public
    assert "refresh_token" not in public
    assert "access-plain-secret" not in repr(public)
    assert "refresh-plain-secret" not in repr(public)


def test_privacy_policy_matches_encrypted_token_storage() -> None:
    policy = (
        Path(__file__).resolve().parents[1]
        / "ViralShorts-App"
        / "src"
        / "studio"
        / "pages"
        / "PrivacyPage.tsx"
    ).read_text(encoding="utf-8")
    assert "access and refresh tokens are encrypted at rest" in policy
    assert "dedicated server-managed key" in policy
