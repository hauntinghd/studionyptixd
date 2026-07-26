from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend_runtime import _security_headers, configure_frontend_static


ROOT = Path(__file__).resolve().parents[1]


def test_hosted_frontend_api_origin_fails_closed_to_canonical_backend() -> None:
    source = (ROOT / "ViralShorts-App" / "src" / "studio" / "shared.tsx").read_text(
        encoding="utf-8"
    )
    routing = source.split("// API routing:", 1)[1].split(
        "/** Resolve every production operation", 1
    )[0]

    assert (
        'export const PROD_API_BASE_URL = "https://api-studio.nyptidindustries.com";'
        in source
    )
    assert "viteEnv.VITE_PROD_API_BASE_URL" not in routing
    assert "VITE_PROD_GENERATION_API_BASE_URL" not in source
    assert "export const API = isLocalDevHost ? rawLocalApi : PROD_API_BASE_URL;" in routing
    assert (
        "export const DIRECT_API = isLocalDevHost ? (rawLocalApi || API) : PROD_API_BASE_URL;"
        in routing
    )
    assert (
        "export const GENERATION_API = isLocalDevHost"
        in source
    )


def test_hosted_large_uploads_use_only_the_fixed_contabo_ingress() -> None:
    studio_root = ROOT / "ViralShorts-App" / "src" / "studio"
    shared = (studio_root / "shared.tsx").read_text(encoding="utf-8")
    upload_routing = shared.split(
        "// Cloudflare Workers cannot accept", 1
    )[1].split(
        "/** WebSocket upgrades use the same canonical origin", 1
    )[0]

    assert (
        'export const STUDIO_DIRECT_UPLOAD_BASE_URL = '
        '"https://studio.82.197.67.155.sslip.io";'
    ) in upload_routing
    assert (
        "const base = isLocalDevHost ? API : STUDIO_DIRECT_UPLOAD_BASE_URL;"
        in upload_routing
    )
    assert "Direct Studio upload route is not allowed" in upload_routing
    assert upload_routing.count("    /^\\/api\\/") == 4
    assert "^\\/api\\/cliplab\\/ingest\\/upload$" in upload_routing
    assert "^\\/api\\/catalyst\\/hub\\/reference-video-analysis\\/manual$" in upload_routing
    assert "^\\/api\\/studio-agent\\/sessions\\/[^/]+\\/attachments\\/video$" in upload_routing
    assert "^\\/api\\/thumbnails\\/upload-video$" in upload_routing

    upload_calls = {
        "panels/AgentPanel.tsx": (
            "resolveStudioUploadUrl("
            "`/api/studio-agent/sessions/${sessionId}/attachments/video`"
        ),
        "panels/CatalystPanel.tsx": (
            "resolveStudioUploadUrl("
            "'/api/catalyst/hub/reference-video-analysis/manual'"
        ),
        "panels/ClipLabPanel.tsx": (
            "resolveStudioUploadUrl('/api/cliplab/ingest/upload')"
        ),
        "panels/ThumbnailPanel.tsx": (
            "resolveStudioUploadUrl('/api/thumbnails/upload-video')"
        ),
    }
    for relative_path, expected_call in upload_calls.items():
        source = (studio_root / relative_path).read_text(encoding="utf-8")
        assert expected_call in source
        assert "STUDIO_DIRECT_UPLOAD_BASE_URL" not in source


def test_ordinary_frontend_commands_remain_on_the_canonical_api() -> None:
    studio_root = ROOT / "ViralShorts-App" / "src" / "studio"
    cliplab = (studio_root / "panels" / "ClipLabPanel.tsx").read_text(encoding="utf-8")
    catalyst = (studio_root / "panels" / "CatalystPanel.tsx").read_text(encoding="utf-8")
    thumbnail = (studio_root / "panels" / "ThumbnailPanel.tsx").read_text(encoding="utf-8")
    dictation = (studio_root / "hooks" / "useSpeechDictation.ts").read_text(encoding="utf-8")
    skeleton = (studio_root / "panels" / "CreatePanel.tsx").read_text(encoding="utf-8")

    # These are production mutations with small JSON bodies. They must not use
    # direct ingress merely because an adjacent upload route does.
    assert "const api = DIRECT_API || API;" in cliplab
    assert "`${api}/api/cliplab/analyze`" in cliplab
    assert "`${api}/api/cliplab/render`" in cliplab
    assert "`${API}/api/catalyst/hub/reference-video-analysis`" in catalyst
    assert "`${API}/api/catalyst/hub/reference-video-analysis/clear`" in catalyst
    assert "`${api}/api/thumbnails/generate`" in thumbnail

    # Small multipart routes remain on the canonical command contract too.
    assert "resolveStudioBackendUrl('/api/studio-agent/dictation')" in dictation
    assert "resolveStudioBackendUrl('/api/skeleton-ai/reference')" in skeleton
    assert "resolveStudioUploadUrl(" not in dictation
    assert "resolveStudioUploadUrl(" not in skeleton


def test_fly_spa_is_served_last_without_shadowing_api(tmp_path, monkeypatch) -> None:
    dist = tmp_path / "dist"
    assets = dist / "assets"
    assets.mkdir(parents=True)
    (dist / "index.html").write_text("<html><body>studio-shell</body></html>", encoding="utf-8")
    (assets / "app.js").write_text("window.STUDIO = true;", encoding="utf-8")
    monkeypatch.setenv("FRONTEND_DIST_DIR", str(dist))

    app = FastAPI()

    @app.get("/api/ping")
    async def ping():
        return {"ok": True}

    configure_frontend_static(app)
    client = TestClient(app)

    assert client.get("/api/ping").json() == {"ok": True}
    assert client.get("/api/not-real").status_code == 404
    assert "studio-shell" in client.get("/").text
    assert "studio-shell" in client.get("/dashboard/agent").text
    assert client.get("/assets/app.js").text == "window.STUDIO = true;"


def test_security_headers_are_applied() -> None:
    app = FastAPI()
    app.middleware("http")(_security_headers)

    @app.get("/")
    async def root():
        return {"ok": True}

    response = TestClient(app).get("/")
    assert response.headers["strict-transport-security"].startswith("max-age=63072000")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    assert "microphone=(self)" in response.headers["permissions-policy"]
