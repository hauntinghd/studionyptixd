"""FastAPI runtime wiring for NYPTID Studio.

This module keeps app lifecycle, frontend asset cache control, and runtime
hotfix routes out of the main backend feature implementation.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

import studio_alerts as _studio_alerts


_deploy_meta_cache = {"ts": 0.0, "backend_commit": "", "frontend_bundle": ""}
_frontend_asset_cache = {"ts": 0.0, "js": "", "css": ""}
_frontend_cache_buster = str(int(time.time()))
_maintenance_lock_fd: int | None = None
_log = logging.getLogger("nyptid-studio.runtime")

_PRODUCTION_ENVIRONMENTS = {"production", "prod", "live"}
_TRUSTED_HOSTS = (
    "studio.nyptidindustries.com",
    "api-studio.nyptidindustries.com",
    "billing.nyptidindustries.com",
    "invoicer.nyptidindustries.com",
    "nyptid-studio.fly.dev",
    "nyptid-studio.internal",
    "localhost",
    "127.0.0.1",
    "testserver",
)
_CORS_ORIGINS = (
    "https://studio.nyptidindustries.com",
    "https://billing.nyptidindustries.com",
    "https://invoicer.nyptidindustries.com",
    "https://nyptid-studio.fly.dev",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
)
_CONTENT_SECURITY_POLICY = "; ".join(
    (
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' https://www.googletagmanager.com https://unpkg.com",
        "style-src 'self' 'unsafe-inline'",
        "connect-src 'self' https: wss:",
        "img-src 'self' data: blob: https:",
        "media-src 'self' data: blob: https:",
        "font-src 'self' data: https:",
        "frame-src https://www.youtube-nocookie.com https://www.youtube.com",
        "worker-src 'self' blob:",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self' https:",
        "frame-ancestors 'none'",
        "manifest-src 'self'",
    )
)


def production_runtime() -> bool:
    """Return whether browser-facing production restrictions must apply."""
    explicit = str(os.getenv("STUDIO_ENVIRONMENT", "") or "").strip().lower()
    if explicit:
        return explicit in _PRODUCTION_ENVIRONMENTS
    return bool(str(os.getenv("FLY_APP_NAME", "") or "").strip() or str(os.getenv("FLY_MACHINE_ID", "") or "").strip())


def fastapi_documentation_kwargs() -> dict[str, str | None]:
    """Disable framework discovery endpoints on production, retain them locally."""
    if not production_runtime():
        return {}
    return {"docs_url": None, "redoc_url": None, "openapi_url": None}


def _error_query_key_summary(request: Request) -> list[str] | str:
    """Expose query names for debugging without forwarding any query values."""
    keys = sorted({str(key or "")[:80] for key in request.query_params.keys() if str(key or "")})
    return keys[:50] if keys else "-"


def _acquire_maintenance_singleton() -> bool:
    """Allow exactly one web worker to own periodic Catalyst maintenance.

    Fly runs several Uvicorn workers for responsive chat.  Without this lock,
    every worker starts duplicate Catalyst/backfill loops and consumes the same
    quota four times.  An advisory flock is released automatically if its owner
    crashes, so another worker can take over on the next app start.
    """
    global _maintenance_lock_fd
    if _maintenance_lock_fd is not None:
        return True
    if os.name == "nt":
        # Local Windows dev normally runs one worker; avoid platform-specific
        # locking failures while production Linux uses flock below.
        _maintenance_lock_fd = -1
        return True
    try:
        import fcntl

        lock_root = Path(os.getenv("APP_DATA_DIR") or Path.home() / ".nyptid-studio")
        lock_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = lock_root / "maintenance.lock"
        open_flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            open_flags |= os.O_NOFOLLOW
        fd = os.open(lock_path, open_flags, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _maintenance_lock_fd = fd
        return True
    except OSError:
        return False


def _release_maintenance_singleton() -> None:
    global _maintenance_lock_fd
    fd = _maintenance_lock_fd
    _maintenance_lock_fd = None
    if fd is None or fd < 0:
        return
    try:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(fd)
    except OSError:
        pass


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


async def _start_persistent_background_maintenance() -> None:
    import catalyst_backfill
    import studio_release_notes
    import youtube as youtube_module
    from studio_agent import training_capture
    from studio_agent import catalyst_runtime

    if _acquire_maintenance_singleton():
        _log.info("Studio background-maintenance singleton acquired")
        catalyst_backfill.start_auto_loop()
        catalyst_runtime.start_studio_catalyst_loop()
        youtube_module.start_youtube_token_maintenance()
        training_capture.start_compiler_loop()
        try:
            from long_form import pipeline as lf_pipeline

            resumed = lf_pipeline.resume_stalled_jobs()
            if resumed:
                _log.info("Resumed stalled long-form jobs: %s", ", ".join(resumed))
        except Exception:
            pass
        try:
            studio_release_notes.announce_pending_catalog_releases()
        except Exception:
            pass
    else:
        _log.info("Studio background-maintenance owned by another worker")


async def _stop_persistent_background_maintenance() -> None:
    import catalyst_backfill
    import youtube as youtube_module
    from studio_agent import training_capture
    from studio_agent import catalyst_runtime

    if _maintenance_lock_fd is not None:
        catalyst_backfill.stop_auto_loop()
        catalyst_runtime.stop_studio_catalyst_loop()
        youtube_module.stop_youtube_token_maintenance()
        training_capture.stop_compiler_loop()
        _release_maintenance_singleton()


def _resolve_latest_frontend_assets() -> tuple[str, str]:
    now = time.time()
    if now - float(_frontend_asset_cache.get("ts", 0.0)) < 10.0:
        return str(_frontend_asset_cache.get("js", "")), str(_frontend_asset_cache.get("css", ""))
    js_name = ""
    css_name = ""
    try:
        default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
        dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
        assets_dir = dist_root / "assets"
        if assets_dir.exists():
            js_candidates = sorted(assets_dir.glob("index-*.js"), key=lambda p: p.stat().st_mtime, reverse=True)
            css_candidates = sorted(assets_dir.glob("index-*.css"), key=lambda p: p.stat().st_mtime, reverse=True)
            if js_candidates:
                js_name = js_candidates[0].name
            if css_candidates:
                css_name = css_candidates[0].name
    except Exception:
        js_name = ""
        css_name = ""
    _frontend_asset_cache["ts"] = now
    _frontend_asset_cache["js"] = js_name
    _frontend_asset_cache["css"] = css_name
    return js_name, css_name


def _resolve_frontend_asset_path(filename: str) -> Path:
    default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
    dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
    return dist_root / "assets" / filename


def _apply_runtime_js_text_hotfix(js: str) -> str:
    """Patch legacy pricing strings in stale frontend bundles."""
    if not js:
        return js
    js = js.replace("Unlimited videos", "300 videos/month")
    js = js.replace("Sign Up Free", "Sign Up to Subscribe")
    js = js.replace("Start Creating Free", "Start Creating")
    return js


def _read_deploy_meta() -> tuple[str, str]:
    now = time.time()
    if now - float(_deploy_meta_cache.get("ts", 0.0)) < 15.0:
        return str(_deploy_meta_cache.get("backend_commit", "")), str(_deploy_meta_cache.get("frontend_bundle", ""))

    backend_commit = (
        os.getenv("STUDIO_GIT_SHA", "")
        or os.getenv("STUDIO_COMMIT_SHA", "")
        or os.getenv("GITHUB_SHA", "")
    ).strip()
    frontend_bundle = (os.getenv("STUDIO_BUILD_ID", "") or "").strip()

    # Prefer image-baked deploy_meta.json (written at docker build from git SHA).
    try:
        meta_path = Path(__file__).resolve().parent / "ops" / "deploy_meta.json"
        if meta_path.is_file():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(meta, dict):
                backend_commit = str(
                    meta.get("git_sha")
                    or meta.get("backend_commit")
                    or backend_commit
                    or ""
                ).strip()
                frontend_bundle = str(
                    meta.get("build_id")
                    or meta.get("frontend_bundle")
                    or frontend_bundle
                    or ""
                ).strip()
    except Exception:
        pass

    if not backend_commit:
        try:
            backend_commit = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(Path(__file__).resolve().parent),
                text=True,
                timeout=2,
            ).strip()
        except Exception:
            backend_commit = ""

    if not frontend_bundle:
        try:
            default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
            current_dist = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
            index_path = current_dist / "index.html"
            if index_path.exists():
                html = index_path.read_text(encoding="utf-8", errors="ignore")
                m = re.search(r"/assets/(index-[^\"']+\.js)", html)
                if m:
                    frontend_bundle = m.group(1)
            if not frontend_bundle:
                latest_js, _ = _resolve_latest_frontend_assets()
                frontend_bundle = latest_js
        except Exception:
            frontend_bundle = ""

    _deploy_meta_cache["ts"] = now
    _deploy_meta_cache["backend_commit"] = backend_commit
    _deploy_meta_cache["frontend_bundle"] = frontend_bundle
    return backend_commit, frontend_bundle


async def _studio_error_reporter(request: Request, call_next):
    try:
        return await call_next(request)
    except HTTPException:
        raise
    except Exception as exc:
        path = request.url.path or "?"
        method = request.method or "?"
        credential_present = bool(
            str(request.headers.get("authorization", "") or request.headers.get("x-access-token", "")).strip()
        )
        _studio_alerts.send_exception(
            exc,
            source=f"{method} {path}",
            context={
                "endpoint": f"{method} {path}",
                # Never decode or forward unverified JWT claims/PII to alerts.
                "request_identity": "credential_present" if credential_present else "anonymous",
                "query_keys": _error_query_key_summary(request),
            },
        )
        raise


async def _disable_html_cache(request: Request, call_next):
    """Prevent stale frontend shell and asset caching so new bundles load immediately."""
    response = await call_next(request)
    path = request.url.path or ""
    if path == "/" or path.endswith(".html"):
        try:
            content_type = str(response.headers.get("content-type", "")).lower()
            if "text/html" in content_type and hasattr(response, "body_iterator"):
                body = b""
                async for chunk in response.body_iterator:
                    body += chunk
                html = body.decode("utf-8", errors="ignore")
                _, latest_css = _resolve_latest_frontend_assets()
                if latest_css:
                    html = re.sub(
                        r"/assets/index-[^\"']+\.css(\?[^\"']*)?",
                        f"/assets/{latest_css}?v={_frontend_cache_buster}",
                        html,
                    )
                headers = dict(response.headers)
                headers.pop("content-length", None)
                headers.pop("Content-Length", None)
                headers.pop("content-type", None)
                headers.pop("Content-Type", None)
                response = Response(
                    content=html,
                    status_code=response.status_code,
                    headers=headers,
                    media_type="text/html",
                )
        except Exception:
            pass
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    if path.startswith("/assets/") and (path.endswith(".js") or path.endswith(".css")):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["CDN-Cache-Control"] = "no-store"
        response.headers["Cloudflare-CDN-Cache-Control"] = "no-store"
    return response


async def _security_headers(request: Request, call_next):
    """Apply conservative browser security headers to API and SPA responses."""
    response = await call_next(request)
    response.headers.setdefault("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), geolocation=(), microphone=(self), payment=(self), usb=()",
    )
    response.headers.setdefault("Content-Security-Policy", _CONTENT_SECURITY_POLICY)
    return response


async def serve_runtime_hotfix_js():
    """Serve JS with runtime text hotfixes to bypass stale CDN bundle caching."""
    latest_js, _ = _resolve_latest_frontend_assets()
    target = _resolve_frontend_asset_path(latest_js) if latest_js else _resolve_frontend_asset_path("index-BlMPK7KO.js")
    if not target.exists():
        fallback = _resolve_frontend_asset_path("index-BlMPK7KO.js")
        if fallback.exists():
            target = fallback
        else:
            raise HTTPException(status_code=404, detail="Hotfix JS not found")
    js = _apply_runtime_js_text_hotfix(target.read_text(encoding="utf-8", errors="ignore"))
    resp = Response(content=js, media_type="text/javascript")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


async def serve_legacy_firefox_bundle_alias():
    """Alias stale cached bundle URL to the latest built JS asset."""
    latest_js, _ = _resolve_latest_frontend_assets()
    if latest_js:
        latest_path = _resolve_frontend_asset_path(latest_js)
        if latest_path.exists():
            return FileResponse(str(latest_path), media_type="text/javascript")
    legacy_path = _resolve_frontend_asset_path("index-BlMPK7KO.js")
    if legacy_path.exists():
        return FileResponse(str(legacy_path), media_type="text/javascript")
    raise HTTPException(status_code=404, detail="Asset not found")


def configure_backend_runtime(app: FastAPI) -> None:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=list(_TRUSTED_HOSTS),
        www_redirect=False,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(_CORS_ORIGINS),
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "Cache-Control",
            "X-Access-Token",
            "X-Idempotency-Key",
        ],
        max_age=86400,
    )
    app.on_event("startup")(_start_persistent_background_maintenance)
    app.on_event("shutdown")(_stop_persistent_background_maintenance)
    app.middleware("http")(_studio_error_reporter)
    app.middleware("http")(_disable_html_cache)
    app.middleware("http")(_security_headers)
    app.add_api_route("/assets/runtime-hotfix.js", serve_runtime_hotfix_js, methods=["GET"])
    app.add_api_route("/assets/index-BlMPK7KO.js", serve_legacy_firefox_bundle_alias, methods=["GET"])

    @app.get("/api/studio/release-notes")
    async def studio_release_notes_feed(limit: int = 30):
        import studio_release_notes as release_notes

        return {"releases": release_notes.list_release_notes(limit=limit)}

    @app.post("/api/studio/release-notes/announce")
    async def studio_release_notes_announce(request: Request):
        """Deploy hook: announce catalog entries not yet on this Fly volume."""
        import studio_release_notes as release_notes

        expected = str(os.getenv("STUDIO_RELEASE_ANNOUNCE_KEY", "") or "").strip()
        if not expected:
            raise HTTPException(503, "release announce disabled")
        provided = str(request.headers.get("X-Studio-Release-Key") or "").strip()
        if provided != expected:
            raise HTTPException(401, "unauthorized")
        pending = release_notes.pending_catalog_release_ids()
        count = release_notes.announce_pending_catalog_releases()
        return {"ok": True, "announced": count, "pending_before": pending}

    @app.get("/api/studio/client-manifest")
    async def studio_client_manifest():
        backend_commit, frontend_bundle = _read_deploy_meta()
        return {
            "backend_commit": backend_commit,
            "frontend_bundle": frontend_bundle,
            "built_at": time.time(),
        }


def configure_frontend_static(app: FastAPI) -> None:
    """Serve the built React SPA from Fly after every API router is mounted."""
    default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
    dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
    index_path = dist_root / "index.html"
    assets_root = dist_root / "assets"
    if not index_path.is_file() or not assets_root.is_dir():
        _log.warning("Frontend dist is unavailable at %s; SPA routes will remain disabled", dist_root)
        return

    # This must be registered after the API routers so the SPA cannot shadow an
    # API route. StaticFiles provides safe path resolution and HEAD support.
    app.mount(
        "/assets",
        StaticFiles(directory=str(assets_root), check_dir=True),
        name="frontend-assets",
    )

    @app.api_route("/{full_path:path}", methods=["GET", "HEAD"], include_in_schema=False)
    async def frontend_spa(full_path: str):
        normalized = str(full_path or "").lstrip("/")
        if normalized == "api" or normalized.startswith("api/"):
            raise HTTPException(404, "Not found")
        if production_runtime() and normalized.rstrip("/") in {"docs", "redoc", "openapi.json"}:
            raise HTTPException(404, "Not found")

        if normalized:
            candidate = (dist_root / normalized).resolve()
            try:
                candidate.relative_to(dist_root)
            except ValueError:
                raise HTTPException(404, "Not found")
            if candidate.is_file():
                return FileResponse(
                    str(candidate),
                    headers={"Cache-Control": "public, max-age=3600"},
                )

        return FileResponse(
            str(index_path),
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
