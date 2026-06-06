"""Local History Rewind thumbnail fix — no API. Overlays correct runtime badge."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageFilter


def _font(size: int, bold: bool = True) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def make_thumbnail(
    base_image: Path,
    out_path: Path,
    *,
    title_words: str = "KHMER EMPIRE",
    badge: str = "1 HOUR",
) -> Path:
    W, H = 1280, 720
    img = Image.open(base_image).convert("RGB")
    img = img.resize((W, H), Image.Resampling.LANCZOS)
    img = img.filter(ImageFilter.GaussianBlur(radius=0.6))

    # Darken for sleep-doc mood
    overlay = Image.new("RGB", (W, H), (0, 0, 0))
    img = Image.blend(img, overlay, alpha=0.45)

    draw = ImageDraw.Draw(img)
    title_font = _font(72)
    badge_font = _font(36)

    # Title top third
    tw = draw.textlength(title_words, font=title_font)
    tx = (W - tw) // 2
    ty = 48
    for dx, dy in [(-2, 0), (2, 0), (0, -2), (0, 2)]:
        draw.text((tx + dx, ty + dy), title_words, fill=(0, 0, 0), font=title_font)
    draw.text((tx, ty), title_words, fill=(255, 235, 120), font=title_font)

    # Runtime badge top-right
    pad_x, pad_y = 18, 10
    bw = draw.textlength(badge, font=badge_font) + pad_x * 2
    bh = 52
    bx, by = W - bw - 28, 28
    draw.rounded_rectangle((bx, by, bx + bw, by + bh), radius=8, fill=(180, 30, 30))
    draw.text((bx + pad_x, by + pad_y - 2), badge, fill=(255, 255, 255), font=badge_font)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG", optimize=True)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", default="khmer_full")
    ap.add_argument("--badge", default="")
    ap.add_argument("--title", default="KHMER EMPIRE")
    ap.add_argument("--scene", default="", help="scene png stem, default scene_0007")
    args = ap.parse_args()

    root = Path("D:/recaps/history_rewind") / args.slug
    meta = json.loads((root / "upload_meta.json").read_text(encoding="utf-8"))
    badge = args.badge or "1 HOUR"
    if meta.get("duration_sec"):
        sec = float(meta["duration_sec"])
        h = int(round(sec / 3600)) or 1
        badge = f"{h} HOUR" if h == 1 else f"{h} HOURS"

    scene = root / "stills" / (args.scene or "scene_0007.png")
    if not scene.exists():
        scenes = sorted((root / "stills").glob("scene_*.png"))
        scene = scenes[len(scenes) // 2] if scenes else scene

    out = make_thumbnail(scene, root / "thumbnail.png", title_words=args.title, badge=badge)
    print(f"Saved {out}  badge={badge}")


if __name__ == "__main__":
    main()
