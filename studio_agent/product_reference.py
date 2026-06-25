"""Durable, SSRF-safe product reference ingestion for Studio advertisements."""
from __future__ import annotations

import ipaddress
import json
import mimetypes
import os
import socket
import time
import uuid
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx


OUTPUT_ROOT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
REFERENCE_ROOT = OUTPUT_ROOT / "_product_references"
REFERENCE_ROOT.mkdir(parents=True, exist_ok=True)
MAX_HTML_BYTES = 2 * 1024 * 1024
MAX_IMAGE_BYTES = 12 * 1024 * 1024


class ProductReferenceError(ValueError):
    pass


def _safe_token(value: str, fallback: str) -> str:
    token = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in "-_")[:80]
    return token or fallback


def _assert_public_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ProductReferenceError("product URL must be public http(s)")
    host = parsed.hostname.lower().rstrip(".")
    if host in {"localhost", "localhost.localdomain"}:
        raise ProductReferenceError("local product URLs are not allowed")
    try:
        addresses = {info[4][0] for info in socket.getaddrinfo(host, parsed.port or 443)}
    except OSError as exc:
        raise ProductReferenceError(f"product host could not be resolved: {host}") from exc
    if any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ProductReferenceError("private, loopback, and link-local product URLs are not allowed")
    return parsed.geturl()


class _ProductHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self._in_title = False
        self.meta: dict[str, str] = {}
        self.images: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        data = {str(k).lower(): str(v or "") for k, v in attrs}
        if tag.lower() == "title":
            self._in_title = True
        elif tag.lower() == "meta":
            key = (data.get("property") or data.get("name") or "").lower()
            value = data.get("content") or ""
            if key and value:
                self.meta[key] = value
        elif tag.lower() == "img":
            src = data.get("src") or data.get("data-src") or data.get("data-lazy-src") or ""
            if src:
                self.images.append((src, (data.get("alt") or "")[:240]))

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data


def _image_candidates(base_url: str, parser: _ProductHTMLParser) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in ("og:image", "og:image:secure_url", "twitter:image", "twitter:image:src"):
        if parser.meta.get(key):
            rows.append({"url": urljoin(base_url, parser.meta[key]), "source": key, "alt": ""})
    rows.extend(
        {"url": urljoin(base_url, src), "source": "img", "alt": alt}
        for src, alt in parser.images
    )

    def score(row: dict[str, str]) -> int:
        text = f"{row['url']} {row['alt']}".lower()
        value = 10 if row["source"].startswith(("og:", "twitter:")) else 0
        value += 3 * sum(word in text for word in ("product", "hero", "gallery", "feature", "app", "device"))
        value -= 5 * sum(word in text for word in ("logo", "icon", "avatar", "sprite", "pixel", "tracking"))
        return value

    seen: set[str] = set()
    output: list[dict[str, str]] = []
    for row in sorted(rows, key=score, reverse=True):
        url = row["url"].split("#", 1)[0]
        if url not in seen:
            seen.add(url)
            output.append({**row, "url": url})
    return output


def _download_image(client: httpx.Client, url: str, target_dir: Path, index: int) -> dict[str, Any] | None:
    try:
        _assert_public_url(url)
        with client.stream("GET", url, headers={"Accept": "image/*"}) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
            if not content_type.startswith("image/") or content_type == "image/svg+xml":
                return None
            suffix = mimetypes.guess_extension(content_type) or Path(urlparse(url).path).suffix or ".jpg"
            if suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}:
                suffix = ".jpg"
            path = target_dir / f"product_{index:02d}{suffix}"
            size = 0
            with path.open("wb") as handle:
                for chunk in response.iter_bytes(256 * 1024):
                    size += len(chunk)
                    if size > MAX_IMAGE_BYTES:
                        raise ProductReferenceError("product image exceeds 12MB")
                    handle.write(chunk)
            if size < 1024:
                path.unlink(missing_ok=True)
                return None
            return {"path": str(path.resolve()), "source_url": url, "content_type": content_type, "bytes": size}
    except (httpx.HTTPError, OSError, ProductReferenceError):
        return None


def ingest(
    *,
    session_id: str,
    user_id: str,
    website_url: str = "",
    attached_paths: list[str] | None = None,
    product_name: str = "",
    product_description: str = "",
) -> dict[str, Any]:
    reference_id = f"prd_{uuid.uuid4().hex[:12]}"
    target_dir = REFERENCE_ROOT / _safe_token(user_id, "user") / reference_id
    target_dir.mkdir(parents=True, exist_ok=True)
    images: list[dict[str, Any]] = []

    for raw_path in list(attached_paths or [])[:4]:
        path = Path(str(raw_path or "")).resolve()
        try:
            path.relative_to(OUTPUT_ROOT.resolve())
        except ValueError:
            continue
        if path.is_file() and 1024 < path.stat().st_size <= MAX_IMAGE_BYTES:
            images.append({"path": str(path), "source": "chat_attachment", "bytes": path.stat().st_size})

    page: dict[str, Any] = {}
    if website_url:
        website_url = _assert_public_url(website_url)
        with httpx.Client(
            timeout=httpx.Timeout(20.0, read=30.0),
            follow_redirects=True,
            headers={"User-Agent": "NyptidStudioProductBot/1.0"},
        ) as client:
            response = client.get(website_url, headers={"Accept": "text/html,application/xhtml+xml"})
            response.raise_for_status()
            if len(response.content) > MAX_HTML_BYTES:
                raise ProductReferenceError("product page exceeds 2MB")
            parser = _ProductHTMLParser()
            parser.feed(response.text)
            page = {
                "url": str(response.url),
                "title": parser.title.strip()[:240],
                "description": (
                    parser.meta.get("og:description")
                    or parser.meta.get("description")
                    or parser.meta.get("twitter:description")
                    or ""
                )[:1000],
            }
            for candidate in _image_candidates(str(response.url), parser):
                if len(images) >= 6:
                    break
                downloaded = _download_image(client, candidate["url"], target_dir, len(images))
                if downloaded:
                    downloaded.update({"source": candidate["source"], "alt": candidate["alt"]})
                    images.append(downloaded)

    if not images:
        raise ProductReferenceError(
            "No usable product images were found. Attach product images or provide a page with public product imagery."
        )
    manifest = {
        "reference_id": reference_id,
        "session_id": session_id,
        "user_id": user_id,
        "product_name": product_name.strip()[:160] or page.get("title") or "Product",
        "product_description": product_description.strip()[:2000] or page.get("description") or "",
        "website": page,
        "images": images,
        "created_at": time.time(),
    }
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path.resolve())
    return manifest


def load(reference_id: str, *, user_id: str = "") -> dict[str, Any]:
    token = _safe_token(reference_id, "")
    if not token.startswith("prd_"):
        raise ProductReferenceError("invalid product reference id")
    roots = [REFERENCE_ROOT / _safe_token(user_id, "user")] if user_id else list(REFERENCE_ROOT.glob("*"))
    for root in roots:
        path = root / token / "manifest.json"
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            if not user_id or str(data.get("user_id") or "") == str(user_id):
                return data
    raise ProductReferenceError("product reference not found")
