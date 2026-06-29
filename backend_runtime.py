"""FastAPI runtime wiring for NYPTID Studio.

This module keeps app lifecycle, frontend asset cache control, and runtime
hotfix routes out of the main backend feature implementation.
"""
from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response

import studio_alerts as _studio_alerts


_deploy_meta_cache = {"ts": 0.0, "backend_commit": "", "frontend_bundle": ""}
_frontend_asset_cache = {"ts": 0.0, "js": "", "css": ""}
_frontend_cache_buster = str(int(time.time()))


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


async def _start_persistent_background_maintenance() -> None:
    import catalyst_backfill
    import youtube as youtube_module
    from studio_agent import training_capture

    catalyst_backfill.start_auto_loop()
    youtube_module.start_youtube_token_maintenance()
    training_capture.start_compiler_loop()


async def _stop_persistent_background_maintenance() -> None:
    import catalyst_backfill
    import youtube as youtube_module
    from studio_agent import training_capture

    catalyst_backfill.stop_auto_loop()
    youtube_module.stop_youtube_token_maintenance()
    training_capture.stop_compiler_loop()


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

    backend_commit = (os.getenv("STUDIO_COMMIT_SHA", "") or os.getenv("GITHUB_SHA", "")).strip()
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

    frontend_bundle = ""
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
        user_hint = ""
        try:
            auth = str(request.headers.get("authorization", "") or request.headers.get("x-access-token", ""))
            if auth.lower().startswith("bearer "):
                parts = auth.split(" ", 1)[1].split(".")
                if len(parts) >= 2:
                    pad = "=" * (-len(parts[1]) % 4)
                    user_hint = str(json.loads(base64.urlsafe_b64decode(parts[1] + pad)).get("email", ""))
        except Exception:
            user_hint = ""
        _studio_alerts.send_exception(
            exc,
            source=f"{method} {path}",
            context={
                "endpoint": f"{method} {path}",
                "user": user_hint or "(anonymous)",
                "query": dict(request.query_params) or "-",
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
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.on_event("startup")(_start_persistent_background_maintenance)
    app.on_event("shutdown")(_stop_persistent_background_maintenance)
    app.middleware("http")(_studio_error_reporter)
    app.middleware("http")(_disable_html_cache)
    app.add_api_route("/assets/runtime-hotfix.js", serve_runtime_hotfix_js, methods=["GET"])
    app.add_api_route("/assets/index-BlMPK7KO.js", serve_legacy_firefox_bundle_alias, methods=["GET"])
