from __future__ import annotations

from fastapi.testclient import TestClient

import backend


def test_production_app_mounts_private_generation_surfaces() -> None:
    """The deployed monolith must not silently omit its generation routers."""

    client = TestClient(backend.app)
    probes = (
        ("GET", "/api/skeleton-ai/categories"),
        ("GET", "/api/skeleton-ai/jobs/not-a-job/stills/not-a-file.png"),
        ("GET", "/api/long-form/channels"),
        ("GET", "/api/long-form/jobs/not-a-job/mp4"),
        ("GET", "/api/zerotier-private/jobs"),
        ("GET", "/api/zerotier-private/jobs/not-a-job/mp4"),
        ("POST", "/api/alt-history-private/generate-topics"),
        ("POST", "/api/history-rewind-private/generate-topics"),
        ("GET", "/api/catalyst/references"),
        ("POST", "/api/catalyst/references"),
        ("GET", "/api/thumbnails/models"),
        ("POST", "/api/thumbnails/generate"),
    )

    for method, path in probes:
        response = client.request(method, path, json={} if method == "POST" else None)
        assert response.status_code == 401, path
