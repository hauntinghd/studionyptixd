from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend_runtime import _security_headers, configure_frontend_static


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
