"""CrypticScience — minimal punch thumbnail (3 elements only).

Designed for 120px-wide YouTube preview:
  1. Huge dollar hook (left)
  2. One subline
  3. Host face filling the right half

Also exports Canva layers to assets/thumb_layers/.

Run:
  python long_form/compose_cryptic_thumb_minimal.py
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1]
PROJECT = Path(r"D:/recaps/cryptic_science/ctr_ss_rook_v1")
MP4 = PROJECT / "CrypticScience_CTR_SS_Rook_v1.mp4"
LAYERS = PROJECT / "assets" / "thumb_layers"
OUT = PROJECT / "CrypticScience_Minimal_THUMB.jpg"
OUT_PNG = PROJECT / "CrypticScience_Minimal_THUMB.png"
DL = Path.home() / "Downloads" / OUT.name
DL_PNG = Path.home() / "Downloads" / OUT_PNG.name

W, H = 1280, 720
NAVY = (8, 18, 38)
GOLD = (255, 196, 48)


def _font(size: int) -> ImageFont.FreeTypeFont:
    for p in (r"C:\Windows\Fonts\impact.ttf", r"C:\Windows\Fonts\arialbd.ttf"):
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def _best_host_frame() -> Path:
    candidates = sorted(PROJECT.glob("assets/host_pick_*.jpg"))
    preferred = PROJECT / "assets" / "host_pick_28.jpg"
    if preferred.exists():
        return preferred
    if candidates:
        return candidates[0]
    out = PROJECT / "assets" / "host_pick_28.jpg"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-ss", "28", "-i", str(MP4), "-frames:v", "1", str(out)],
        check=True,
    )
    return out


def _grade_host(img: Image.Image) -> Image.Image:
    img = ImageEnhance.Contrast(img).enhance(1.12)
    img = ImageEnhance.Color(img).enhance(1.08)
    img = ImageEnhance.Sharpness(img).enhance(1.25)
    return img


def _left_panel() -> Image.Image:
    panel = Image.new("RGB", (W, H), NAVY)
    draw = ImageDraw.Draw(panel)
    # Warm accent streak
    streak = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(streak)
    sd.polygon([(0, H), (520, 0), (520, H)], fill=(255, 196, 48, 28))
    panel = Image.alpha_composite(panel.convert("RGBA"), streak).convert("RGB")
    draw = ImageDraw.Draw(panel)

    x, y = 56, 118
    hook = "$10,000"
    f1 = _font(118)
    for ox, oy in [(-3, 0), (3, 0), (0, -3), (0, 3)]:
        draw.text((x + ox, y + oy), hook, fill=(0, 0, 0), font=f1)
    draw.text((x, y), hook, fill=(255, 255, 255), font=f1)

    y += 128
    f2 = _font(92)
    line = "BANK RULE"
    for ox, oy in [(-3, 0), (3, 0), (0, -3), (0, 3)]:
        draw.text((x + ox, y + oy), line, fill=(0, 0, 0), font=f2)
    draw.text((x, y), line, fill=GOLD, font=f2)

    y += 108
    f3 = _font(42)
    draw.text((x, y), "+ SOCIAL SECURITY", fill=(220, 228, 240), font=f3)

    y += 72
    draw.rounded_rectangle((x, y, x + 190, y + 44), radius=8, fill=(34, 120, 78))
    draw.text((x + 18, y + 8), "VERIFIED", fill=(255, 255, 255), font=_font(28))

    return panel


def _host_layer(host_path: Path) -> Image.Image:
    host = Image.open(host_path).convert("RGB")
    host = _grade_host(host)
    w, h = host.size
    host = host.crop((int(w * 0.10), 0, int(w * 0.98), int(h * 0.94)))

    max_w = int(W * 0.50)
    scale = min(max_w / host.width, H / host.height)
    nw, nh = int(host.width * scale), int(host.height * scale)
    host = host.resize((nw, nh), Image.Resampling.LANCZOS)

    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    x = W - nw
    y = H - nh
    layer.paste(host, (x, y))

    arr = np.array(layer)
    seam = x + 20
    for col in range(max(0, seam - 100), min(W, seam + 80)):
        t = min(1.0, max(0.0, (col - (seam - 100)) / 180))
        arr[:, col, 3] = (arr[:, col, 3].astype(np.float32) * t).astype(np.uint8)
    return Image.fromarray(arr, "RGBA")


def compose() -> Path:
    host_path = _best_host_frame()
    print(f"Host: {host_path}")

    base = _left_panel().convert("RGBA")
    host = _host_layer(host_path)
    out = Image.alpha_composite(base, host).convert("RGB")

    LAYERS.mkdir(parents=True, exist_ok=True)
    base.convert("RGB").save(LAYERS / "01_background_navy.jpg", quality=95)
    host.save(LAYERS / "02_host_cutout.png")
    out.save(LAYERS / "03_composite.jpg", quality=95)

    out.save(OUT, quality=97, subsampling=0)
    out.save(OUT_PNG)
    shutil.copy2(OUT, DL)
    shutil.copy2(OUT_PNG, DL_PNG)

    spec = LAYERS / "CANVA_INSTRUCTIONS.txt"
    spec.write_text(
        "CANVA QUICK FIX (5 min)\n"
        "=======================\n"
        "1. New design → YouTube Thumbnail (1280×720)\n"
        "2. Upload 01_background_navy.jpg OR use solid #081226\n"
        "3. Upload 02_host_cutout.png → align right, full height\n"
        "4. Text (Impact or Anton):\n"
        "   - $10,000 (white, ~110pt)\n"
        "   - BANK RULE (gold #FFC430, ~85pt)\n"
        "   - + SOCIAL SECURITY (white 70% opacity, 36pt)\n"
        "5. Optional: green VERIFIED pill\n"
        "6. Export PNG\n",
        encoding="utf-8",
    )

    print(f"Saved {OUT}")
    print(f"Saved {DL}")
    print(f"Layers: {LAYERS}")
    return OUT


if __name__ == "__main__":
    compose()
