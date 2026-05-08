"""
ZeroTier Short #9 — 'The Time Wally West Outran A Black Hole'

DATA-LOCKED render plan:
  - Title format: 'The Time Wally West [past-tense outcome]'  (4-of-4 channel
    winners hit 64-77% retention vs 2 'Wally vs X' both <60%)
  - Conflict Arc structure (3× lift validated on Wally #3)
  - 12 beats, ~36s, matches channel-average length
  - Catalyst-fed Grok pattern recognition (top titles, hook formulas, retention)
  - Visual: DC Comics action panels (NOT Skeleton AI character)

Pipeline:
  Seedream v4.5 stills → Pixverse V6 i2v → ElevenLabs Brian narration →
  ffmpeg compose with single-word captions baked in.

Output: D:/recaps/ZeroTier/short9_BlackHole/ZeroTier_S09_BlackHole.mp4
"""
from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT))

# Bootstrap env from the canonical .env.
ENV_FILE = Path(r"D:/Games/asd/.env")
for line in ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from skeleton_ai.stills_engine import generate as gen_still
from skeleton_ai.i2v_engine import generate as gen_clip
from skeleton_ai.voice_elevenlabs import ElevenLabsClient
from skeleton_ai.compose import probe_duration, concat_demuxer, mux_narration
from skeleton_ai import captions as cap


# ────────────────────────────────────────────────────────────────────────
# Output paths
# ────────────────────────────────────────────────────────────────────────
OUT_ROOT = Path(r"D:/recaps/ZeroTier/short9_BlackHole")
STILLS_DIR = OUT_ROOT / "stills"
CLIPS_DIR = OUT_ROOT / "clips"
TRIMMED_DIR = OUT_ROOT / "trimmed"
WORK_DIR = OUT_ROOT / "work"
for d in (OUT_ROOT, STILLS_DIR, CLIPS_DIR, TRIMMED_DIR, WORK_DIR):
    d.mkdir(parents=True, exist_ok=True)

FINAL_MP4 = OUT_ROOT / "ZeroTier_S09_BlackHole.mp4"
SILENT_MP4 = OUT_ROOT / "silent.mp4"
NARRATION_MP3 = OUT_ROOT / "narration.mp3"


# ────────────────────────────────────────────────────────────────────────
# Visual style — DC Comics action-panel render. Channel-canonical for ZeroTier.
# ────────────────────────────────────────────────────────────────────────
DC_COMICS_BASE = (
    "DC Comics action-panel style cinematic 3D render, vibrant comic book "
    "aesthetic with bold colors and dynamic lighting. Wally West as The "
    "Flash in his canonical costume — full red bodysuit with golden lightning "
    "bolt emblem on chest, golden lightning bolts on red boots and gauntlets, "
    "white wings on red cowl above the ears, mask covering upper face but "
    "exposing chin and mouth. Athletic adult build (~7 head heights). "
    "Vivid red-and-yellow color palette, dramatic comic-book lighting, "
    "depth of field, motion lines and lightning effects. "
)

DC_NEG = (
    "text overlap, watermark, logo on costume, blurry, low quality, "
    "deformed hands, extra fingers, multiple heads, child, "
    "skeleton, anatomical bones, ribcage, mascot, chibi, funko, "
    "cartoon mascot, plastic toy figure, "
    "dull colors, faded palette, grayscale, monochrome, "
    "wrong costume colors, blue suit, green suit, black suit, "
    "modern military uniform, plain clothes"
)


def assemble_dc_prompt(scene_action: str, motion_prompt: str = "") -> str:
    """Compose a DC-comics-style scene prompt. NOT skeleton-AI."""
    parts = [DC_COMICS_BASE, f"Scene: {scene_action.strip()}. "]
    parts.append(
        "Vertical 9:16 frame. Comic book panel composition with cinematic "
        "depth and dramatic dynamic lighting. Lightning crackles and motion "
        "trails emphasize speed."
    )
    return "".join(parts)


# ────────────────────────────────────────────────────────────────────────
# 12-beat script — locked to Conflict Arc structure
# ────────────────────────────────────────────────────────────────────────
BEATS: list[dict] = [
    {
        "sid": "b00", "duration": 3.0, "structure": "HOOK",
        "narration": "Wally West got caught in a black hole. Couldn't stop running.",
        "scene": "Wide cinematic shot — Wally West as The Flash running at the event horizon of a colossal black hole filling most of the frame. The accretion disk glows orange-red, stars warping around the gravity well, space distortion bending the starfield. Wally is angled forward at sprint pace, golden lightning trailing behind him.",
        "motion": "Lightning trails behind Wally spiral around him. The accretion disk rotates slowly clockwise. Stars twinkle and warp.",
    },
    {
        "sid": "b01", "duration": 2.0, "structure": "SETUP",
        "narration": "Slow down — pulled apart atom by atom.",
        "scene": "Tight close-up on Wally's masked face mid-run, gritted teeth visible, eyes hidden behind the mask but lightning crackling across his cheekbones. The gravitational pull behind him is visualized as luminous orange tension lines stretching back toward the void.",
        "motion": "Lightning flickers across Wally's face. Tension lines pulse and stretch toward the void.",
    },
    {
        "sid": "b02", "duration": 3.0, "structure": "RISING",
        "narration": "He ran around the event horizon for days.",
        "scene": "Mid-distance shot — Wally circling the colossal black hole as a streaking red-and-gold blur, time-streaks of lightning trailing the curve of his path. Black hole dominates the upper-right of the frame, accretion disk glowing.",
        "motion": "Lightning blur intensifies. Particles flicker off Wally's path. Black hole accretion disk swirls.",
    },
    {
        "sid": "b03", "duration": 1.0, "structure": "RISING",
        "narration": "Then weeks.",
        "scene": "Same composition as before but now Wally's blur has become a continuous ring of red-gold lightning around the event horizon, blue-shifted at the leading edge from relativistic speed.",
        "motion": "The lightning ring pulses and shimmers. Particles dance off the leading edge.",
    },
    {
        "sid": "b04", "duration": 5.0, "structure": "CONFLICT",
        "narration": "Time inside moves different. An hour for him, months for Earth.",
        "scene": "Split-panel composition. Left half: Wally still running, sweat beading, lightning crackling. Right half: a view of Earth from orbit with seasons cycling rapidly — green to gold to white snow to green again, clouds racing.",
        "motion": "Earth seasons cycle visibly fast. Wally's lightning intensifies.",
    },
    {
        "sid": "b05", "duration": 5.0, "structure": "COMEBACK",
        "narration": "He found the angle. Threaded the gravity. Escaped.",
        "scene": "Dramatic comic-book hero shot — Wally bursting outward at a precise tangent angle from the event horizon, golden lightning slingshotting in a massive arc behind him as he escapes the gravity well. The black hole fills the lower-left of frame as he angles toward the upper-right.",
        "motion": "Lightning arc whips outward and forward. Wally accelerates away from the black hole.",
    },
    {
        "sid": "b06", "duration": 3.0, "structure": "PAYOFF",
        "narration": "Landed back on Earth. Gasping.",
        "scene": "Wally collapsed on cracked asphalt of an empty city street, chest heaving, suit torn at the shoulders, weak lightning crackling off his arms. Tall buildings rise behind him in pre-dawn light.",
        "motion": "Wally's chest heaves. Lightning flickers weakly off his shoulders. A faint breeze ripples his cape.",
    },
    {
        "sid": "b07", "duration": 2.0, "structure": "PAYOFF",
        "narration": "60 years had passed.",
        "scene": "Pull-back wide shot — the city around Wally is now futuristic-decayed: holographic ad billboards float between buildings, sleek hovering cars zip past at altitude, weathered solar arrays line rooftops. Subtly different from the Earth Wally remembered.",
        "motion": "Holographic ads flicker. Hovering cars drift past in the distance.",
    },
    {
        "sid": "b08", "duration": 3.0, "structure": "PAYOFF",
        "narration": "His daughter was older than him.",
        "scene": "An elderly woman with kind eyes and silver-white hair stands several feet from Wally on the cracked asphalt — wearing soft modern clothing, looking down at him with shock and dawning recognition. She is clearly in her seventies.",
        "motion": "The woman's eyes widen. Her hand slowly raises toward her mouth.",
    },
    {
        "sid": "b09", "duration": 3.0, "structure": "PAYOFF",
        "narration": "She didn't recognize him.",
        "scene": "Reverse angle — Wally still on the ground looking up at her, his masked face visible. Confusion in his posture. The elderly woman's expression slowly shifts from shock toward recognition as she stares.",
        "motion": "Her expression shifts subtly. Wally's chest still rises and falls.",
    },
    {
        "sid": "b10", "duration": 3.0, "structure": "PAYOFF",
        "narration": "He didn't age a day.",
        "scene": "Two-shot composition — Wally now standing, young face still visible behind the Flash mask, the elderly daughter facing him at the same height. The generation gap is the visual subject. Faint dust particles drift in the dawn light between them.",
        "motion": "Dust particles drift between them. Wally's lightning weakly flickers.",
    },
    {
        "sid": "b11", "duration": 3.0, "structure": "HOOK_OUT",
        "narration": "Worth it? Comments below.",
        "scene": "Final close-up — Wally staring at his own gloved hands, residual golden lightning still coursing weakly through his fingers. The elderly daughter's reflection visible faintly in the lenses of his Flash goggles.",
        "motion": "Lightning crackles softly across his fingertips. Reflection slightly shifts.",
    },
]


# ────────────────────────────────────────────────────────────────────────
# Caption helper — single-word white bold + black stroke. NO Cryptic Science
# watermark (this is ZeroTier, not Cryptic Science).
# ────────────────────────────────────────────────────────────────────────
def zerotier_caption_drawtext(phrase: cap.CaptionPhrase, *, width: int = 720) -> str:
    """Override of cap.caption_drawtext — all white (no orange tier for ZeroTier)."""
    n = len(phrase.text)
    size = 96 if n <= 12 else (76 if n <= 20 else 60)
    enable = (
        f"between(t\\,{phrase.start_sec:.3f}\\,"
        f"{phrase.start_sec + phrase.duration_sec:.3f})"
    )
    safe_text = cap._esc(phrase.text)
    return (
        f"drawtext=fontfile='{cap.DEFAULT_FONT}':text='{safe_text}':"
        f"fontsize={size}:fontcolor=white:"
        f"bordercolor=black:borderw=6:"
        f"x=(w-text_w)/2:y=h*0.78:enable='{enable}'"
    )


def trim_with_zerotier_captions(src_clip: Path, out_path: Path, *,
                                duration_sec: float, narration_text: str,
                                width: int = 720, height: int = 1280,
                                fps: int = 30) -> Path:
    """Like skeleton_ai.compose.trim_with_captions but no watermark."""
    import subprocess
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    phrases = cap.split_into_phrases(narration_text)
    timed = cap.time_phrases(phrases, duration_sec)
    drawtexts = [zerotier_caption_drawtext(p, width=width) for p in timed]

    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,fps={fps},"
        f"{','.join(drawtexts)}"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    filter_script = out_path.parent / f"{out_path.stem}_filter.txt"
    filter_script.write_text(vf, encoding="utf-8")

    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(src_clip),
        "-t", f"{duration_sec:.3f}",
        "-filter_script:v", str(filter_script),
        "-an",
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-pix_fmt", "yuv420p",
        str(out_path),
    ], check=True)
    return out_path


# ────────────────────────────────────────────────────────────────────────
# Full pipeline
# ────────────────────────────────────────────────────────────────────────
def main() -> int:
    t0 = time.time()
    print(f"[1/4] Rendering {len(BEATS)} stills (Seedream v4.5)...")
    for b in BEATS:
        out = STILLS_DIR / f"{b['sid']}.png"
        if out.exists() and out.stat().st_size > 1024:
            print(f"  [skip] {b['sid']} cached")
            continue
        prompt = assemble_dc_prompt(b["scene"], b["motion"])
        ts = time.time()
        gen_still("seedream_45", prompt, out, negative_prompt=DC_NEG)
        print(f"  [{b['sid']}] {time.time()-ts:.1f}s  '{b['narration'][:50]}...'")

    print(f"\n[2/4] Animating {len(BEATS)} beats (Pixverse V6 fallback chain)...")
    trimmed_paths: list[Path] = []
    for b in BEATS:
        sid = b["sid"]
        clip_out = CLIPS_DIR / f"{sid}.mp4"
        if not clip_out.exists():
            ts = time.time()
            # Seedance/Pixverse min duration is 4s — always render 5s, then
            # trim_with_zerotier_captions truncates to b["duration"] via -t.
            gen_clip(STILLS_DIR / f"{sid}.png", b["motion"], clip_out,
                     tier="standard", duration_sec=5)
            print(f"  [{sid}] i2v {time.time()-ts:.1f}s")
        trimmed_out = TRIMMED_DIR / f"{sid}.mp4"
        trim_with_zerotier_captions(
            clip_out, trimmed_out,
            duration_sec=b["duration"],
            narration_text=b["narration"],
        )
        trimmed_paths.append(trimmed_out)

    print(f"\n[3/4] ElevenLabs Brian narration...")
    if not NARRATION_MP3.exists():
        full_script = " ".join(b["narration"] for b in BEATS)
        el = ElevenLabsClient()
        el.synthesize(full_script, NARRATION_MP3)
        print(f"  wrote {NARRATION_MP3}")
    else:
        print(f"  cached {NARRATION_MP3}")

    print(f"\n[4/4] Concat + mux...")
    if not SILENT_MP4.exists():
        concat_demuxer(trimmed_paths, SILENT_MP4, WORK_DIR)
    if not FINAL_MP4.exists():
        mux_narration(SILENT_MP4, NARRATION_MP3, FINAL_MP4)
    print(f"\nDONE in {time.time()-t0:.1f}s")
    print(f"  FINAL: {FINAL_MP4}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
