"""CrypticScience thumbnail — Verified Brief style (channel-native, not Markus clone).

Mirrors the SourceProofCard motion graphics already in the video:
  dark browser chrome · green VERIFIED SOURCE · yellow stat highlight · host panel

Zero fal. Distinct from finance-YouTube parchment/red-bar templates.

Run:
  python long_form/compose_cryptic_thumb_verified_brief.py
  python long_form/compose_cryptic_thumb_verified_brief.py --host-frame 5
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROJECT = Path(r"D:/recaps/cryptic_science/ctr_ss_rook_v1")
MP4 = PROJECT / "CrypticScience_CTR_SS_Rook_v1.mp4"
OUT_JPG = PROJECT / "CrypticScience_VerifiedBrief_THUMB.jpg"
OUT_PNG = PROJECT / "CrypticScience_VerifiedBrief_THUMB.png"
DL_JPG = Path.home() / "Downloads" / OUT_JPG.name
DL_PNG = Path.home() / "Downloads" / OUT_PNG.name

W, H = 1920, 1080

# SourceProofCard palette (stat_card.py)
BG = (14, 16, 22)
PANEL = (28, 32, 42)
CHROME = (45, 50, 62)
BORDER = (70, 85, 110)
TEXT = (245, 247, 250)
MUTED = (130, 145, 170)
URL_BG = (22, 26, 34)
VERIFIED = (40, 120, 80)
HIGHLIGHT = (255, 214, 64)
ACCENT = (56, 189, 248)


def _font(size: int, role: str = "bold") -> ImageFont.FreeTypeFont:
    paths = {
        "bold": [r"C:\Windows\Fonts\arialbd.ttf", r"C:\Windows\Fonts\segoeuib.ttf"],
        "regular": [r"C:\Windows\Fonts\arial.ttf", r"C:\Windows\Fonts\segoeui.ttf"],
        "serif": [r"C:\Windows\Fonts\georgiab.ttf", r"C:\Windows\Fonts\arialbd.ttf"],
        "mono": [r"C:\Windows\Fonts\consola.ttf", r"C:\Windows\Fonts\cour.ttf"],
    }
    for p in paths.get(role, paths["bold"]):
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


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
    still = PROJECT / "assets" / "host_v2.png"
    if still.exists():
        return still
    raise FileNotFoundError("No host frame found")


def _background() -> Image.Image:
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    # Subtle radial glow behind card
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((80, 120, 1100, 980), fill=(30, 55, 90, 55))
    gd.ellipse((120, 180, 900, 860), fill=(40, 120, 80, 25))
    img = Image.alpha_composite(img.convert("RGBA"), glow).convert("RGB")

    # Fine grid (data / brief aesthetic)
    grid = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd2 = ImageDraw.Draw(grid)
    for x in range(0, W, 48):
        gd2.line((x, 0, x, H), fill=(255, 255, 255, 8))
    for y in range(0, H, 48):
        gd2.line((0, y, W, y), fill=(255, 255, 255, 8))
    return Image.alpha_composite(img.convert("RGBA"), grid).convert("RGB")


def _draw_source_card(base: Image.Image) -> None:
    """Left-side verified source brief — matches in-video motion graphics."""
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)

    bx, by, bw, bh = 72, 88, 1180, 904
    d.rounded_rectangle((bx + 10, by + 18, bx + bw + 10, by + bh + 18), radius=22, fill=(0, 0, 0, 140))
    d.rounded_rectangle((bx, by, bx + bw, by + bh), radius=22, fill=(*PANEL, 255), outline=(*BORDER, 255), width=3)

    # Browser chrome
    ch = 58
    d.rectangle((bx, by, bx + bw, by + ch), fill=(*CHROME, 255))
    for i, col in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        d.ellipse((bx + 22 + i * 30, by + 18, bx + 38 + i * 30, by + 34), fill=(*col, 255))
    d.rounded_rectangle((bx + 118, by + 14, bx + bw - 24, by + 44), radius=8, fill=(*URL_BG, 255))
    d.text((bx + 132, by + 18), "fincen.gov/.../bank-secrecy-act", fill=(*MUTED, 255), font=_font(22, "mono"))

    # Verified badge
    badge = "VERIFIED SOURCE"
    bf = _font(24)
    bb = d.textbbox((0, 0), badge, font=bf)
    bwid = bb[2] - bb[0] + 28
    d.rounded_rectangle((bx + bw - bwid - 20, by + ch + 18, bx + bw - 20, by + ch + 56), radius=8, fill=(*VERIFIED, 255))
    d.text((bx + bw - bwid - 6, by + ch + 24), badge, fill=(255, 255, 255, 255), font=bf)

    cx = bx + 48
    cy = by + ch + 78

    # Headline block
    title = "The $10,000 Bank Rule"
    tf = _font(62, "serif")
    for line in (title, "+ Your Social Security Deposits"):
        d.text((cx, cy), line, fill=(*TEXT, 255), font=tf if line == title else _font(44, "serif"))
        cy += 72 if line == title else 58

    cy += 12
    d.line((cx, cy, bx + bw - 48, cy), fill=(*BORDER, 255), width=2)
    cy += 36

    meta = "FinCEN / SSA · Verified May 27, 2026"
    d.text((cx, cy), meta, fill=(*MUTED, 255), font=_font(26, "regular"))
    cy += 52

    # Pull quote — highlight $10,000
    line1 = "Financial institutions must report cash"
    line2 = "transactions over $10,000 in one business day."
    qf = _font(34, "regular")
    d.text((cx, cy), line1, fill=(*TEXT, 255), font=qf)
    d.text((cx, cy + 44), "transactions over ", fill=(*TEXT, 255), font=qf)
    hl_x = cx + d.textbbox((0, 0), "transactions over ", font=qf)[2]
    d.rounded_rectangle((hl_x - 4, cy + 40, hl_x + 168, cy + 82), radius=4, fill=(*HIGHLIGHT, 255))
    d.text((hl_x, cy + 44), "$10,000", fill=(20, 20, 20, 255), font=_font(36))
    rest_x = hl_x + d.textbbox((0, 0), "$10,000", font=_font(36))[2] + 6
    d.text((rest_x, cy + 44), " in one business day.", fill=(*TEXT, 255), font=qf)

    cy += 130
    # Two stat chips
    chips = [
        ("$10,000", "CTR threshold", (56, 189, 248)),
        ("2.8%", "SS COLA 2026", (40, 120, 80)),
    ]
    chip_x = cx
    for val, label, col in chips:
        cw, ch2 = 280, 120
        d.rounded_rectangle((chip_x, cy, chip_x + cw, cy + ch2), radius=14, fill=(18, 22, 30, 255), outline=(*col, 255), width=2)
        d.text((chip_x + 24, cy + 18), val, fill=(*TEXT, 255), font=_font(48))
        d.text((chip_x + 24, cy + 76), label, fill=(*MUTED, 255), font=_font(22, "regular"))
        chip_x += cw + 28

    cy += 150
    d.text((cx, cy), "What actually gets reported — and what does not.", fill=(*ACCENT, 255), font=_font(30, "regular"))

    # Channel strip
    d.rounded_rectangle((bx + 24, by + bh - 72, bx + 420, by + bh - 24), radius=10, fill=(18, 22, 30, 255), outline=(*BORDER, 255), width=2)
    d.text((bx + 44, by + bh - 62), "CRYPTIC SCIENCE", fill=(*TEXT, 255), font=_font(28))
    d.text((bx + 290, by + bh - 58), "Verified explainer", fill=(*MUTED, 255), font=_font(22, "regular"))

    base.paste(Image.alpha_composite(base.convert("RGBA"), layer).convert("RGB"))


def _draw_host_panel(base: Image.Image, host_path: Path) -> None:
    """Broadcast panel — host stays in natural studio frame (no hacky cutout)."""
    host = Image.open(host_path).convert("RGB")
    host = host.crop((int(host.width * 0.02), 0, int(host.width * 0.98), int(host.height * 0.96)))

    px, py, pw, ph = 1288, 96, 560, 896
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)

    d.rounded_rectangle((px + 12, py + 16, px + pw + 12, py + ph + 16), radius=20, fill=(0, 0, 0, 150))
    d.rounded_rectangle((px, py, px + pw, py + ph), radius=20, fill=(*PANEL, 255), outline=(*ACCENT, 255), width=4)

    # LIVE pill
    d.rounded_rectangle((px + 24, py + 20, px + 118, py + 56), radius=8, fill=(180, 40, 40, 255))
    d.ellipse((px + 36, py + 34, px + 46, py + 44), fill=(255, 100, 100, 255))
    d.text((px + 54, py + 26), "LIVE", fill=(255, 255, 255, 255), font=_font(20))

    d.text((px + 132, py + 26), "Primary sources only", fill=(*MUTED, 255), font=_font(20, "regular"))

    # Fit host inside panel
    inner = (px + 16, py + 68, px + pw - 16, py + ph - 16)
    iw, ih = inner[2] - inner[0], inner[3] - inner[1]
    scale = min(iw / host.width, ih / host.height)
    nw, nh = int(host.width * scale), int(host.height * scale)
    host = host.resize((nw, nh), Image.Resampling.LANCZOS)
    hx = inner[0] + (iw - nw) // 2
    hy = inner[1] + (ih - nh) // 2

    rgba = base.convert("RGBA")
    rgba.paste(Image.alpha_composite(rgba, layer))
    rgba.paste(host, (hx, hy))
    base.paste(rgba.convert("RGB"))


def compose(*, host_sec: float = 5.0) -> Path:
    host_path = _resolve_host(host_sec)
    print(f"Style: verified_brief (CrypticScience native)")
    print(f"Host: {host_path}")

    base = _background()
    _draw_source_card(base)
    _draw_host_panel(base, host_path)

    OUT_JPG.parent.mkdir(parents=True, exist_ok=True)
    base.save(OUT_JPG, quality=97, optimize=True, subsampling=0)
    base.save(OUT_PNG, optimize=True)
    shutil.copy2(OUT_JPG, DL_JPG)
    shutil.copy2(OUT_PNG, DL_PNG)
    print(f"Saved {OUT_JPG}")
    print(f"Saved {OUT_PNG}")
    print(f"Saved {DL_JPG}")
    return OUT_JPG


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host-frame", type=float, default=5.0)
    args = ap.parse_args()
    compose(host_sec=args.host_frame)


if __name__ == "__main__":
    main()
