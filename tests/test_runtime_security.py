from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

import backend_health
import backend_runtime
import backend_settings


def test_error_alert_omits_query_values_and_unverified_jwt_claims(monkeypatch) -> None:
    captured: dict = {}

    def capture_exception(_exc, *, source, context):
        captured.update({"source": source, "context": context})

    monkeypatch.setattr(backend_runtime._studio_alerts, "send_exception", capture_exception)
    app = FastAPI()
    app.middleware("http")(backend_runtime._studio_error_reporter)

    @app.get("/explode")
    async def explode():
        raise RuntimeError("test failure")

    spoofed_claim = "eyJlbWFpbCI6InZpY3RpbUBleGFtcGxlLmNvbSJ9"
    response = TestClient(app, raise_server_exceptions=False).get(
        "/explode?code=oauth-secret-value&token=query-secret&topic=safe",
        headers={"Authorization": f"Bearer x.{spoofed_claim}.x"},
    )

    assert response.status_code == 500
    assert captured["context"]["request_identity"] == "credential_present"
    assert captured["context"]["query_keys"] == ["code", "token", "topic"]
    serialized = repr(captured)
    assert "oauth-secret-value" not in serialized
    assert "query-secret" not in serialized
    assert "victim@example.com" not in serialized


def test_public_error_codes_never_echo_provider_details() -> None:
    secret_error = "401 from https://private-provider.invalid?api_key=super-secret"
    code = backend_health._public_error_code(secret_error)
    assert code == "provider_authentication_failed"
    assert "private-provider" not in code
    assert "super-secret" not in code

    queue = backend_health._public_queue_consumer_health({
        "ready": False,
        "last_error": "Redis acknowledgement failed at redis://user:password@private-host",
    })
    assert queue["last_error"] == "acknowledgement_pending"
    assert "password" not in repr(queue)


def test_production_docs_are_disabled_and_not_replaced_by_spa(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_ENVIRONMENT", "production")
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html>studio-shell</html>", encoding="utf-8")
    monkeypatch.setenv("FRONTEND_DIST_DIR", str(dist))

    app = FastAPI(**backend_runtime.fastapi_documentation_kwargs())
    backend_runtime.configure_frontend_static(app)
    client = TestClient(app)

    assert client.get("/").status_code == 200
    assert client.get("/docs").status_code == 404
    assert client.get("/redoc").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_local_development_keeps_framework_docs(monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_ENVIRONMENT", "development")
    monkeypatch.delenv("FLY_APP_NAME", raising=False)
    monkeypatch.delenv("FLY_MACHINE_ID", raising=False)
    app = FastAPI(**backend_runtime.fastapi_documentation_kwargs())
    client = TestClient(app)
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200


def test_runtime_security_headers_trusted_hosts_and_cors() -> None:
    app = FastAPI()
    backend_runtime.configure_backend_runtime(app)

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    client = TestClient(app)
    allowed = client.get("/ping", headers={"Host": "nyptid-studio.fly.dev"})
    assert allowed.status_code == 200
    csp = allowed.headers["content-security-policy"]
    assert "default-src 'self'" in csp
    assert "object-src 'none'" in csp
    assert "frame-ancestors 'none'" in csp
    assert client.get("/ping", headers={"Host": "attacker.invalid"}).status_code == 400

    preflight = client.options(
        "/ping",
        headers={
            "Host": "nyptid-studio.fly.dev",
            "Origin": "https://studio.nyptidindustries.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization,x-idempotency-key",
        },
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "https://studio.nyptidindustries.com"
    assert "x-idempotency-key" in preflight.headers["access-control-allow-headers"].lower()

    tauri_preflight = client.options(
        "/ping",
        headers={
            "Host": "nyptid-studio.fly.dev",
            "Origin": "http://tauri.localhost",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization",
        },
    )
    assert tauri_preflight.status_code == 200
    assert tauri_preflight.headers["access-control-allow-origin"] == "http://tauri.localhost"

    rejected = client.options(
        "/ping",
        headers={
            "Host": "nyptid-studio.fly.dev",
            "Origin": "https://tauri.localhost.attacker.invalid",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert rejected.status_code == 400


def test_backend_registers_cors_once_and_public_health_omits_provider_url() -> None:
    import backend

    assert sum(item.cls is CORSMiddleware for item in backend.app.user_middleware) == 1
    payload = asyncio.run(backend._base_health_payload())
    assert "comfyui_url" not in payload
    assert payload["comfyui_configured"] is False
    assert payload["runway_key_configured"] is False
    assert payload["runway_key_source"] == ""
    assert payload["runway_video_model"] == ""
    assert payload["xai_image_fallback_enabled"] is False
    assert set(payload["image_provider_order"]) <= {"fal"}
    assert "http" not in str(payload["queue_consumer"].get("last_error", ""))


def test_public_health_and_client_manifest_share_deployment_identity(monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_DEPLOYMENT_TARGET", "contabo")
    monkeypatch.setenv("STUDIO_RELEASE_ID", "studio:release/161280d")
    monkeypatch.setenv("STUDIO_INSTANCE_ID", "studio api 01")
    monkeypatch.setenv("UNRELATED_SECRET", "must-not-be-published")

    import backend

    health = asyncio.run(backend._base_health_payload())
    assert health["deployment_target"] == "contabo"
    assert health["release_id"] == "studio:release-161280d"
    assert health["instance_id"] == "studio-api-01"

    app = FastAPI()
    backend_runtime.configure_backend_runtime(app)
    manifest = TestClient(app).get(
        "/api/studio/client-manifest",
        headers={"Host": "api-studio.nyptidindustries.com"},
    ).json()
    assert manifest["deployment_target"] == health["deployment_target"]
    assert manifest["release_id"] == health["release_id"]
    assert manifest["instance_id"] == health["instance_id"]
    assert "must-not-be-published" not in repr(health)
    assert "must-not-be-published" not in repr(manifest)


def test_canonical_api_and_google_callback_defaults(monkeypatch) -> None:
    monkeypatch.delenv("API_PUBLIC_URL", raising=False)
    monkeypatch.setattr(backend_settings, "SITE_URL", "")

    assert backend_settings.api_public_url() == "https://api-studio.nyptidindustries.com"
    assert backend_settings.GOOGLE_DEFAULT_REDIRECT_URI == (
        "https://api-studio.nyptidindustries.com/api/oauth/google/youtube/callback"
    )


def test_provider_facing_media_urls_use_api_origin(tmp_path, monkeypatch) -> None:
    import base64
    import backend

    api_origin = "https://api-studio.nyptidindustries.com"
    monkeypatch.setattr(backend, "_api_public_url", lambda: api_origin)
    monkeypatch.setattr(backend, "TEMP_DIR", tmp_path)

    assert backend._longform_reference_file_public_url("../reference.png") == (
        f"{api_origin}/api/longform/reference-file/reference.png"
    )

    inline_reference = (
        "data:image/png;base64,"
        + base64.b64encode(b"provider-reference").decode("ascii")
    )
    session = {
        "reference_image_url": inline_reference,
        "skeleton_reference_image": inline_reference,
        "template": "skeleton",
    }
    public_url = backend._ensure_reference_public_url("session-1", session)
    assert public_url == f"{api_origin}/api/creative/reference-file/session-1_reference.png"
    assert session["skeleton_reference_image"] == public_url


def test_fly_health_check_uses_a_trusted_host() -> None:
    fly = (Path(__file__).resolve().parents[1] / "fly.toml").read_text(encoding="utf-8")
    assert 'STUDIO_ENVIRONMENT = "production"' in fly
    assert 'Host = "nyptid-studio.fly.dev"' in fly
