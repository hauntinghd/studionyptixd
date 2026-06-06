"""Custom CrypticScience CTR+SS thumbnail — hand-built, zero fal.

Markus layout language (host right, bold $ hook, red bars) + SS lane colors
(yellow senior pill, purple-gold glow). Aurora face pulled from the actual MP4.

Run:
  python long_form/compose_cryptic_ctr_ss_thumb.py
  python long_form/compose_cryptic_ctr_ss_thumb.py --host-frame 5
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROJECT = Path(r"D:/recaps/cryptic_science/ctr_ss_rook_v1")
MP4 = PROJECT / "CrypticScience_CTR_SS_Rook_v1.mp4"
HOST_STILL = PROJECT / "assets" / "host_v2.png"
OUT_JPG = PROJECT / "CrypticScience_CTR_SS_Rook_v1_THUMB.jpg"
OUT_PNG = PROJECT / "CrypticScience_CTR_SS_Rook_v1_THUMB.png"
DL_JPG = Path.home() / "Downloads" / OUT_JPG.name
DL_PNG = Path.home() / "Downloads" / OUT_PNG.name

W, H = 1920, 1080


def _font(size: int, bold: bool = True) -> ImageFont.FreeTypeFont:
    if bold:
        for p in (r"C:\Windows\Fonts\impact.ttf", r"C:\Windows\Fonts\arialbd.ttf"):
            if Path(p).exists():
                return ImageFont.truetype(p, size)
    for p in (r"C:\Windows\Fonts\arial.ttf", r"C:\Windows\Fonts\segoeui.ttf"):
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def _stroke(
    d: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: tuple,
    stroke: int = 4,
) -> None:
    x, y = xy
    for ox in range(-stroke, stroke + 1):
        for oy in range(-stroke, stroke + 1):
            if ox or oy:
                d.text((x + ox, y + oy), text, fill=(0, 0, 0), font=font)
    d.text((x, y), text, fill=fill, font=font)


def _extract_host_frame(sec: float) -> Path:
    assets = PROJECT / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    out = assets / f"aurora_frame_{int(sec)}s.jpg"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-ss", str(sec), "-i", str(MP4), "-frames:v", "1", str(out)],
        check=True,
    )
    return out


def _resolve_host(sec: float) -> Path:
    for p in (
        PROJECT / "assets" / f"aurora_probe_{int(sec)}.jpg",
        PROJECT / "assets" / f"aurora_frame_{int(sec)}s.jpg",
    ):
        if p.exists() and p.stat().st_size > 5000:
            return p
    if MP4.exists():
        return _extract_host_frame(sec)
    if HOST_STILL.exists():
        return HOST_STILL
    raise FileNotFoundError("No Aurora frame found")


def _custom_background() -> Image.Image:
    """Parchment + SS purple-gold wash. No competitor thumb pixels."""
    px_arr = np.zeros((H, W, 3), dtype=np.float32)
    for y in range(H):
        t = y / H
        for x in range(W):
            u = x / W
            r = 185 - t * 40 + u * 10
            g = 168 - t * 45 + u * 6
            b = 128 - t * 35
            px_arr[y, x] = (max(r, 35), max(g, 32), max(b, 28))

    base = Image.fromarray(px_arr.astype(np.uint8), "RGB").filter(ImageFilter.GaussianBlur(1))

    # SS-lane purple radial (top-left)
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((-200, -180, 900, 700), fill=(80, 30, 120, 90))
    gd.ellipse((-80, -60, 600, 480), fill=(200, 160, 40, 35))
    base = Image.alpha_composite(base.convert("RGBA"), glow).convert("RGB")

    # Paper grain
    noise = np.random.default_rng(42).integers(0, 18, (H, W), dtype=np.uint8)
    arr = np.array(base, dtype=np.int16)
    arr = np.clip(arr + noise[:, :, None] - 9, 0, 255).astype(np.uint8)
    base = Image.fromarray(arr, "RGB")

    # Vignette
    vig = Image.new("L", (W, H), 0)
    vd = ImageDraw.Draw(vig)
    vd.ellipse((-160, -100, W + 160, H + 100), fill=215)
    vd.rectangle((0, 0, W, H), fill=50)
    return Image.composite(base, Image.new("RGB", (W, H), (18, 14, 10)), vig)


def _cutout_host(host_path: Path) -> Image.Image:
    img = Image.open(host_path).convert("RGBA")
    arr = np.array(img, dtype=np.float32)
    h, w = arr.shape[:2]
    corners = np.array(
        [arr[0, 0, :3], arr[0, w - 1, :3], arr[h - 1, 0, :3], arr[h - 1, w - 1, :3]],
        dtype=np.float32,
    )
    bg = corners.mean(axis=0)
    r, g, b = arr[..., 0], arr[..., 1], arr[..., 2]
    dist = np.sqrt((r - bg[0]) ** 2 + (g - bg[1]) ** 2 + (b - bg[2]) ** 2)
    alpha = np.clip((dist - 18) * 11, 0, 255).astype(np.uint8)
    lum = 0.299 * r + 0.587 * g + 0.114 * b
    alpha = np.where(lum < 55, np.maximum(alpha, 235), alpha)
    alpha = np.where(dist < 10, 0, alpha)
    arr[..., 3] = alpha
    out = Image.fromarray(arr.astype(np.uint8), "RGBA")
    out.putalpha(out.split()[3].filter(ImageFilter.GaussianBlur(1.2)))
    return out


def _draw_cash_stack(d: ImageDraw.ImageDraw, cx: int, cy: int) -> None:
    for i, (ox, oy, col) in enumerate([(0, 8, (28, 95, 52)), (-14, 0, (22, 78, 44)), (14, -4, (34, 110, 58))]):
        w, h = 52, 28
        x0, y0 = cx + ox - w // 2, cy + oy - h // 2
        d.rounded_rectangle((x0, y0, x0 + w, y0 + h), radius=4, fill=col, outline=(235, 235, 235), width=2)
        d.text((x0 + 16, y0 + 4), "$", fill=(255, 255, 255), font=_font(16, False))


def _left_badges(base: Image.Image) -> None:
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)

    # Bank / CTR coin
    cx1, cy1, r1 = 210, 310, 138
    d.ellipse((cx1 - r1, cy1 - r1, cx1 + r1, cy1 + r1), fill=(24, 26, 24), outline=(240, 240, 240), width=7)
    d.ellipse((cx1 - r1 + 16, cy1 - r1 + 16, cx1 + r1 - 16, cy1 + r1 - 16), outline=(160, 160, 160), width=2)
    _draw_cash_stack(d, cx1, cy1 - 22)
    f = _font(54)
    bb = d.textbbox((0, 0), "$10K", font=f)
    _stroke(d, (cx1 - (bb[2] - bb[0]) // 2, cy1 + 28), "$10K", f, (255, 255, 255), stroke=3)
    sub = _font(24, False)
    bb2 = d.textbbox((0, 0), "CASH CTR", font=sub)
    d.text((cx1 - (bb2[2] - bb2[0]) // 2, cy1 + 92), "CASH CTR", fill=(210, 210, 210), font=sub)

    # SS COLA coin
    cx2, cy2, r2 = 210, 690, 128
    d.ellipse((cx2 - r2, cy2 - r2, cx2 + r2, cy2 + r2), fill=(16, 52, 118), outline=(240, 240, 240), width=7)
    d.ellipse((cx2 - r2 + 16, cy2 - r2 + 16, cx2 + r2 - 16, cy2 + r2 - 16), outline=(160, 160, 160), width=2)
    f2 = _font(58)
    bb3 = d.textbbox((0, 0), "2.8%", font=f2)
    _stroke(d, (cx2 - (bb3[2] - bb3[0]) // 2, cy2 - 42), "2.8%", f2, (255, 255, 255), stroke=3)
    bb4 = d.textbbox((0, 0), "SS COLA", font=sub)
    d.text((cx2 - (bb4[2] - bb4[0]) // 2, cy2 + 34), "SS COLA", fill=(220, 230, 255), font=sub)

    # Yellow senior pill (SS thumb signature)
    label = "FOR SENIORS"
    f3 = _font(30)
    bb5 = d.textbbox((0, 0), label, font=f3)
    tw = bb5[2] - bb5[0]
    px, py = cx2 - tw // 2 - 18, cy2 + r2 + 16
    d.rounded_rectangle((px, py, px + tw + 36, py + 46), radius=6, fill=(255, 220, 0), outline=(0, 0, 0), width=2)
    d.text((px + 18, py + 8), label, fill=(0, 0, 0), font=f3)

    base.paste(Image.alpha_composite(base.convert("RGBA"), layer).convert("RGB"))


def _headline(base: Image.Image) -> None:
    d = ImageDraw.Draw(base)
    white = (255, 252, 245)
    red = (220, 28, 28)
    x0, y0 = 430, 248

    # Red brush bars behind key lines
    d.rectangle((x0 - 10, y0 + 98, x0 + 680, y0 + 210), fill=red)
    d.rectangle((x0 - 10, y0 + 358, x0 + 620, y0 + 468), fill=red)

    _stroke(d, (x0, y0), "$10,000", _font(92), white, stroke=5)
    _stroke(d, (x0, y0 + 112), "BANK RULE", _font(108), white, stroke=6)
    _stroke(d, (x0, y0 + 248), "+ SOCIAL", _font(82), white, stroke=5)
    _stroke(d, (x0, y0 + 372), "SECURITY", _font(102), white, stroke=6)

    d.rounded_rectangle((x0, y0 + 498, x0 + 240, y0 + 558), radius=10, fill=(12, 12, 12), outline=white, width=3)
    d.text((x0 + 22, y0 + 508), "VERIFIED", fill=white, font=_font(32))


def _paste_host(base: Image.Image, host: Image.Image) -> None:
    """Markus-style: tall host anchored bottom-right, bleeding off frame."""
    host = host.crop((int(host.width * 0.01), 0, int(host.width * 0.99), int(host.height * 0.94)))

    target_h = int(H * 1.08)
    scale = target_h / host.height
    target_w = min(int(host.width * scale), 880)
    target_h = int(host.height * (target_w / host.width))
    host = host.resize((target_w, target_h), Image.Resampling.LANCZOS)

    x = W - target_w + 55
    y = H - target_h + 18

    rgba = base.convert("RGBA")
    shadow = Image.new("RGBA", (target_w, target_h), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rectangle((40, 20, target_w, target_h), fill=(0, 0, 0, 90))
    shadow = shadow.filter(ImageFilter.GaussianBlur(24))
    rgba.paste(shadow, (x + 12, y + 12), shadow)
    rgba.paste(host, (x, y), host)
    base.paste(rgba.convert("RGB"))


def compose(*, host_sec: float = 5.0) -> Path:
    host_path = _resolve_host(host_sec)
    print(f"Host source: {host_path}")
    base = _custom_background()
    host = _cutout_host(host_path)
    _left_badges(base)
    _headline(base)
    _paste_host(base, host)

    OUT_JPG.parent.mkdir(parents=True, exist_ok=True)
    base.save(OUT_JPG, quality=97, optimize=True, subsampling=0)
    base.save(OUT_PNG, optimize=True)
    shutil.copy2(OUT_JPG, DL_JPG)
    shutil.copy2(OUT_PNG, DL_PNG)
    print(f"Saved {OUT_JPG}")
    print(f"Saved {OUT_PNG}")
    print(f"Saved {DL_JPG}")
    print(f"Saved {DL_PNG}")
    return OUT_JPG


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host-frame", type=float, default=5.0, help="Seconds into MP4 for Aurora face")
    args = ap.parse_args()
    compose(host_sec=args.host_frame)


if __name__ == "__main__":
    main()
