"""Durable, SSRF-safe product reference ingestion for Studio advertisements."""
from __future__ import annotations

import ipaddress
import http.client
import json
import mimetypes
import os
import re
import socket
import ssl
import time
import uuid
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse


OUTPUT_ROOT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
REFERENCE_ROOT = OUTPUT_ROOT / "_product_references"
REFERENCE_ROOT.mkdir(parents=True, exist_ok=True)
MAX_HTML_BYTES = 2 * 1024 * 1024
MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_REDIRECTS = 4
PUBLIC_WEB_PORTS = {80, 443}


class ProductReferenceError(ValueError):
    pass


def _safe_token(value: str, fallback: str) -> str:
    token = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in "-_")[:80]
    return token or fallback


def _resolve_public_url(url: str) -> tuple[str, tuple[str, ...]]:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ProductReferenceError("product URL must be public http(s)")
    if parsed.username is not None or parsed.password is not None:
        raise ProductReferenceError("credentials in product URLs are not allowed")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise ProductReferenceError("product URL has an invalid port") from exc
    if port not in PUBLIC_WEB_PORTS:
        raise ProductReferenceError("product URL must use port 80 or 443")
    try:
        host = parsed.hostname.lower().rstrip(".").encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ProductReferenceError("product URL has an invalid hostname") from exc
    if host in {"localhost", "localhost.localdomain"}:
        raise ProductReferenceError("local product URLs are not allowed")
    try:
        resolved = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ProductReferenceError(f"product host could not be resolved: {host}") from exc
    addresses: list[str] = []
    for info in resolved:
        address = str(info[4][0]).split("%", 1)[0]
        try:
            ip = ipaddress.ip_address(address)
        except ValueError as exc:
            raise ProductReferenceError("product host resolved to an invalid address") from exc
        if (
            not ip.is_global
            or ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_unspecified
            or ip.is_multicast
        ):
            raise ProductReferenceError(
                "private, loopback, link-local, reserved, and non-public product URLs are not allowed"
            )
        normalized = str(ip)
        if normalized not in addresses:
            addresses.append(normalized)
    if not addresses:
        raise ProductReferenceError(f"product host could not be resolved: {host}")
    return parsed.geturl(), tuple(addresses)


def _assert_public_url(url: str) -> str:
    normalized, _addresses = _resolve_public_url(url)
    return normalized


def _open_pinned_socket(address: str, port: int, timeout: float) -> socket.socket:
    """Connect to an already-validated numeric address without another DNS lookup."""
    ip = ipaddress.ip_address(address)
    family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        destination: tuple[Any, ...]
        if family == socket.AF_INET6:
            destination = (str(ip), port, 0, 0)
        else:
            destination = (str(ip), port)
        sock.connect(destination)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock
    except BaseException:
        sock.close()
        raise


class _PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host: str, port: int, address: str, *, timeout: float) -> None:
        super().__init__(host, port=port, timeout=timeout)
        self._pinned_address = address

    def connect(self) -> None:
        self.sock = _open_pinned_socket(self._pinned_address, self.port, float(self.timeout or 20.0))


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, port: int, address: str, *, timeout: float) -> None:
        super().__init__(host, port=port, timeout=timeout, context=ssl.create_default_context())
        self._pinned_address = address

    def connect(self) -> None:
        raw = _open_pinned_socket(self._pinned_address, self.port, float(self.timeout or 20.0))
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


def _fetch_from_pinned_address(
    url: str,
    address: str,
    *,
    max_bytes: int,
    accept: str,
) -> dict[str, Any]:
    parsed = urlparse(url)
    host = str(parsed.hostname or "").encode("idna").decode("ascii")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    connection_cls = _PinnedHTTPSConnection if parsed.scheme == "https" else _PinnedHTTPConnection
    connection = connection_cls(host, port, address, timeout=30.0)
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    try:
        connection.request(
            "GET",
            target,
            headers={
                "Accept": accept,
                "Accept-Encoding": "identity",
                "Connection": "close",
                "User-Agent": "NyptidStudioProductBot/1.0",
            },
        )
        response = connection.getresponse()
        headers = {str(key).lower(): str(value) for key, value in response.getheaders()}
        try:
            declared_size = int(headers.get("content-length", "") or 0)
        except ValueError:
            declared_size = 0
        if declared_size > max_bytes:
            raise ProductReferenceError(f"remote resource exceeds {max_bytes} bytes")
        body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise ProductReferenceError(f"remote resource exceeds {max_bytes} bytes")
        return {"status": int(response.status), "headers": headers, "body": body}
    finally:
        connection.close()


def _fetch_public_resource(url: str, *, max_bytes: int, accept: str) -> dict[str, Any]:
    """Fetch a public resource while pinning DNS and validating every redirect."""
    current = str(url or "").strip()
    for hop in range(MAX_REDIRECTS + 1):
        current, addresses = _resolve_public_url(current)
        result: dict[str, Any] | None = None
        last_error: BaseException | None = None
        for address in addresses:
            try:
                result = _fetch_from_pinned_address(
                    current,
                    address,
                    max_bytes=max_bytes,
                    accept=accept,
                )
                break
            except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
                last_error = exc
        if result is None:
            raise ProductReferenceError("product resource could not be fetched") from last_error
        status = int(result.get("status") or 0)
        if status in {301, 302, 303, 307, 308}:
            location = str((result.get("headers") or {}).get("location") or "").strip()
            if not location:
                raise ProductReferenceError("product redirect is missing a destination")
            if hop >= MAX_REDIRECTS:
                raise ProductReferenceError("product URL redirected too many times")
            # The next loop resolves, validates, and pins the redirect target
            # before any connection is attempted.
            current = urljoin(current, location)
            continue
        if status < 200 or status >= 300:
            raise ProductReferenceError(f"product resource returned HTTP {status}")
        return {**result, "url": current}
    raise ProductReferenceError("product URL redirected too many times")


class _ProductHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self._in_title = False
        self._heading_tag = ""
        self.meta: dict[str, str] = {}
        self.images: list[tuple[str, str]] = []
        self.headings: list[str] = []
        self.cta_candidates: list[str] = []
        self.price_candidates: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        data = {str(k).lower(): str(v or "") for k, v in attrs}
        tag_l = tag.lower()
        if tag_l == "title":
            self._in_title = True
        elif tag_l == "meta":
            key = (data.get("property") or data.get("name") or "").lower()
            value = data.get("content") or ""
            if key and value:
                self.meta[key] = value
        elif tag_l == "img":
            src = data.get("src") or data.get("data-src") or data.get("data-lazy-src") or ""
            if src:
                self.images.append((src, (data.get("alt") or "")[:240]))
        elif tag_l in {"h1", "h2", "h3"}:
            self._heading_tag = tag_l
        elif tag_l in {"a", "button"}:
            label = (data.get("aria-label") or data.get("title") or "").strip()
            if label:
                self.cta_candidates.append(label[:120])

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False
        if tag.lower() in {"h1", "h2", "h3"}:
            self._heading_tag = ""

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
            return
        text = " ".join(str(data or "").split())
        if not text:
            return
        if self._heading_tag in {"h1", "h2", "h3"}:
            self.headings.append(text[:240])
        if re.search(r"(?:\$|€|£)\s?\d", text) or re.search(r"\b\d+(?:\.\d{2})?\s?(?:usd|eur|gbp|/mo|/month)\b", text, re.I):
            self.price_candidates.append(text[:120])


def _extract_ad_signals(parser: _ProductHTMLParser) -> dict[str, Any]:
    ctas: list[str] = []
    for row in parser.cta_candidates + parser.headings:
        low = row.lower()
        if any(word in low for word in ("buy", "shop", "get", "start", "join", "sign", "trial", "access", "order", "subscribe")):
            ctas.append(row)
    benefits = [h for h in parser.headings if h not in ctas][:6]
    return {
        "headline": (parser.headings[0] if parser.headings else parser.title.strip())[:240],
        "benefits": benefits,
        "cta_candidates": list(dict.fromkeys(ctas))[:5],
        "price_hints": list(dict.fromkeys(parser.price_candidates))[:4],
    }


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


def _download_image(url: str, target_dir: Path, index: int) -> dict[str, Any] | None:
    try:
        fetched = _fetch_public_resource(url, max_bytes=MAX_IMAGE_BYTES, accept="image/*")
        headers = fetched.get("headers") if isinstance(fetched.get("headers"), dict) else {}
        content_type = str(headers.get("content-type") or "").split(";", 1)[0].lower()
        if not content_type.startswith("image/") or content_type == "image/svg+xml":
            return None
        final_url = str(fetched.get("url") or url)
        suffix = mimetypes.guess_extension(content_type) or Path(urlparse(final_url).path).suffix or ".jpg"
        if suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}:
            suffix = ".jpg"
        body = bytes(fetched.get("body") or b"")
        if len(body) < 1024:
            return None
        path = target_dir / f"product_{index:02d}{suffix}"
        path.write_bytes(body)
        return {
            "path": str(path.resolve()),
            "source_url": final_url,
            "content_type": content_type,
            "bytes": len(body),
        }
    except (OSError, ProductReferenceError):
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
        fetched = _fetch_public_resource(
            website_url,
            max_bytes=MAX_HTML_BYTES,
            accept="text/html,application/xhtml+xml",
        )
        headers = fetched.get("headers") if isinstance(fetched.get("headers"), dict) else {}
        content_type = str(headers.get("content-type") or "").split(";", 1)[0].lower()
        if content_type and content_type not in {"text/html", "application/xhtml+xml"}:
            raise ProductReferenceError("product URL did not return an HTML page")
        raw_html = bytes(fetched.get("body") or b"")
        charset_match = re.search(r"charset=([A-Za-z0-9._-]+)", str(headers.get("content-type") or ""), re.I)
        charset = str(charset_match.group(1) if charset_match else "utf-8")
        try:
            html = raw_html.decode(charset, errors="replace")
        except LookupError:
            html = raw_html.decode("utf-8", errors="replace")
        parser = _ProductHTMLParser()
        parser.feed(html)
        ad_signals = _extract_ad_signals(parser)
        final_url = str(fetched.get("url") or website_url)
        page = {
            "url": final_url,
            "title": parser.title.strip()[:240],
            "description": (
                parser.meta.get("og:description")
                or parser.meta.get("description")
                or parser.meta.get("twitter:description")
                or ""
            )[:1000],
            "ad_signals": ad_signals,
        }
        for candidate in _image_candidates(final_url, parser):
            if len(images) >= 6:
                break
            downloaded = _download_image(candidate["url"], target_dir, len(images))
            if downloaded:
                downloaded.update({"source": candidate["source"], "alt": candidate["alt"]})
                images.append(downloaded)

    if not images:
        raise ProductReferenceError(
            "No usable product images were found. Attach product images or provide a page with public product imagery."
        )
    ad_signals = page.get("ad_signals") if isinstance(page.get("ad_signals"), dict) else {}
    manifest = {
        "reference_id": reference_id,
        "session_id": session_id,
        "user_id": user_id,
        "product_name": product_name.strip()[:160] or page.get("title") or "Product",
        "product_description": product_description.strip()[:2000] or page.get("description") or "",
        "website": page,
        "images": images,
        "ad_brief": {
            "headline": str(ad_signals.get("headline") or page.get("title") or product_name or "").strip()[:240],
            "benefits": [str(x)[:180] for x in (ad_signals.get("benefits") or [])[:6]],
            "cta_candidates": [str(x)[:120] for x in (ad_signals.get("cta_candidates") or [])[:5]],
            "price_hints": [str(x)[:120] for x in (ad_signals.get("price_hints") or [])[:4]],
            "conversion_goal": "purchase_or_signup",
        },
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
