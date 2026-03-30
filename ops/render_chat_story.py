from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import httpx
from PIL import Image, ImageDraw, ImageFont, ImageOps
from pydub import AudioSegment

ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = ROOT / "ViralShorts-App"
PUBLIC_DIR = APP_ROOT / "public"
DIST_DIR = APP_ROOT / "dist"
CHATSTORY_ROOTS = [
    PUBLIC_DIR / "chatstory",
    DIST_DIR / "chatstory",
]
VOICE_REFERENCE = Path(
    os.getenv("CHATSTORY_VOICE_REFERENCE", str(ROOT / "voice_reference.wav"))
).expanduser()
LANGUAGE = str(os.getenv("CHATSTORY_LANGUAGE", "en") or "en").strip() or "en"
ELEVENLABS_API_KEY = str(os.getenv("ELEVENLABS_API_KEY", "") or "").strip()
ELEVENLABS_MODEL_ID = str(
    os.getenv("CHATSTORY_ELEVENLABS_MODEL_ID", "eleven_multilingual_v2") or "eleven_multilingual_v2"
).strip()
ELEVENLABS_TIMEOUT_SEC = max(15.0, float(os.getenv("CHATSTORY_ELEVENLABS_TIMEOUT_SEC", "120") or 120))
FORCE_LOCAL_TTS = str(os.getenv("CHATSTORY_FORCE_LOCAL_TTS", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
CANVAS_W = 720
CANVAS_H = 1280
FPS = 12

THEMES = {
    "dark": {
        "label": "Dark Modern",
        "top_bg": "#17171b",
        "screen_bg": "#1c1c1f",
        "incoming": "#2c2c30",
        "incoming_text": "#ffffff",
        "outgoing": "#2f7cff",
        "outgoing_text": "#ffffff",
        "shell_bg": "#121214",
        "shell_border": "#34425f",
        "text": "#ffffff",
    },
    "light": {
        "label": "Light Clean",
        "top_bg": "#ffffff",
        "screen_bg": "#f2f5f9",
        "incoming": "#ffffff",
        "incoming_text": "#0f172a",
        "outgoing": "#7cc4ff",
        "outgoing_text": "#0f172a",
        "shell_bg": "#ffffff",
        "shell_border": "#d1d5db",
        "text": "#0f172a",
    },
    "purple": {
        "label": "Purple Pop",
        "top_bg": "#1f1630",
        "screen_bg": "#18131f",
        "incoming": "#2b2436",
        "incoming_text": "#ffffff",
        "outgoing": "#8b5cf6",
        "outgoing_text": "#ffffff",
        "shell_bg": "#191224",
        "shell_border": "#6d4ad5",
        "text": "#f5f3ff",
    },
}

BACKGROUNDS = {
    "subway": {"label": "Subway Dash", "gradient": ("#09111f", "#173885", "#35d6f8")},
    "minecraft": {"label": "Minecraft Parkour", "gradient": ("#102313", "#2f6c18", "#9cd46a")},
    "cooking": {"label": "Cooking Sizzle", "gradient": ("#28140d", "#7b2f12", "#f5ab31")},
}


def _resolve_chatstory_asset(*parts: str) -> Path:
    for root in CHATSTORY_ROOTS:
        candidate = root.joinpath(*parts)
        if candidate.exists():
            return candidate
    return CHATSTORY_ROOTS[0].joinpath(*parts)


MUSIC = {
    "none": None,
    "midnight_pulse": _resolve_chatstory_asset("music", "midnight-pulse.mp3"),
    "neon_afterglow": _resolve_chatstory_asset("music", "neon-afterglow.mp3"),
    "late_night_bounce": _resolve_chatstory_asset("music", "late-night-bounce.mp3"),
}

SFX = {
    "message_send": _resolve_chatstory_asset("sfx", "message-send.wav"),
    "message_receive": _resolve_chatstory_asset("sfx", "message-receive.wav"),
    "awkward_pause": _resolve_chatstory_asset("sfx", "awkward-pause.wav"),
    "comical_disappointment": _resolve_chatstory_asset("sfx", "comical-disappointment.wav"),
}

ELEVENLABS_DEFAULT_VOICES = {
    "sarah": "EXAVITQu4vr4xnSDxMaL",
    "laura": "FGY2WhTYpPnrIDTdsKH5",
    "charlotte": "XB0fDUnXU5powFXDhCwa",
    "adam": "pNInz6obpgDQGcFmaJgB",
    "daniel": "onwK4e9ZLuTAKqWW03F9",
}

VOICE_PRESETS = {
    "studio_voice_core": {"name": "Core Neutral", "speed": 1.0, "pitch": 1.0, "voice_id": ELEVENLABS_DEFAULT_VOICES["laura"]},
    "studio_voice_hook": {"name": "Hook Sprint", "speed": 1.12, "pitch": 1.03, "voice_id": ELEVENLABS_DEFAULT_VOICES["adam"]},
    "studio_voice_drama": {"name": "Dark Drama", "speed": 0.96, "pitch": 0.94, "voice_id": ELEVENLABS_DEFAULT_VOICES["daniel"]},
    "studio_voice_confession": {"name": "Relatable Confession", "speed": 1.04, "pitch": 0.98, "voice_id": ELEVENLABS_DEFAULT_VOICES["charlotte"]},
    "studio_voice_founder": {"name": "Founder Calm", "speed": 0.98, "pitch": 0.97, "voice_id": ELEVENLABS_DEFAULT_VOICES["adam"]},
    "studio_voice_punch": {"name": "Viral Punch", "speed": 1.1, "pitch": 1.02, "voice_id": ELEVENLABS_DEFAULT_VOICES["adam"]},
    "studio_voice_doc": {"name": "Documentary Steel", "speed": 0.97, "pitch": 0.95, "voice_id": ELEVENLABS_DEFAULT_VOICES["daniel"]},
    "studio_voice_luxe": {"name": "Luxury Ad", "speed": 1.01, "pitch": 1.01, "voice_id": ELEVENLABS_DEFAULT_VOICES["laura"]},
    "studio_voice_story": {"name": "Storyteller Warm", "speed": 0.99, "pitch": 1.04, "voice_id": ELEVENLABS_DEFAULT_VOICES["charlotte"]},
    "studio_voice_intense": {"name": "Intense Clarity", "speed": 1.08, "pitch": 0.97, "voice_id": ELEVENLABS_DEFAULT_VOICES["daniel"]},
    "studio_voice_genz": {"name": "Gen Z Hook", "speed": 1.14, "pitch": 1.05, "voice_id": ELEVENLABS_DEFAULT_VOICES["sarah"]},
    "studio_voice_motive": {"name": "Motivation Rise", "speed": 1.03, "pitch": 1.06, "voice_id": ELEVENLABS_DEFAULT_VOICES["sarah"]},
    "studio_voice_noir": {"name": "Noir Tension", "speed": 0.95, "pitch": 0.92, "voice_id": ELEVENLABS_DEFAULT_VOICES["daniel"]},
}

_LOCAL_XTTS: Any | None = None


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def ffprobe_duration(path: Path) -> float:
    result = run([
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ])
    return float((result.stdout or "0").strip() or 0)


def atempo_chain(factor: float) -> str:
    filters: list[str] = []
    while factor < 0.5:
        filters.append("atempo=0.5")
        factor /= 0.5
    while factor > 2.0:
        filters.append("atempo=2.0")
        factor /= 2.0
    filters.append(f"atempo={factor:.6f}")
    return ",".join(filters)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def hex_rgb(value: str) -> tuple[int, int, int]:
    value = value.lstrip("#")
    return tuple(int(value[i:i + 2], 16) for i in (0, 2, 4))


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    env_font = str(os.getenv("CHATSTORY_FONT_PATH", "") or "").strip()
    candidates = [
        env_font,
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\segoeuib.ttf" if bold else r"C:\Windows\Fonts\segoeui.ttf",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            path = Path(candidate)
            if path.exists():
                return ImageFont.truetype(str(path), size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = (text or "").split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        trial = f"{current} {word}"
        bbox = draw.textbbox((0, 0), trial, font=font)
        if (bbox[2] - bbox[0]) <= max_width:
            current = trial
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def sanitize_text(text: str) -> str:
    return " ".join(str(text or "").replace("\n", " ").split()).strip()


def make_gradient_image(path: Path, colors: tuple[str, str, str], width: int = CANVAS_W, height: int = CANVAS_H) -> None:
    top = hex_rgb(colors[0])
    mid = hex_rgb(colors[1])
    bottom = hex_rgb(colors[2])
    img = Image.new("RGB", (width, height))
    px = img.load()
    for y in range(height):
        t = y / max(1, height - 1)
        if t < 0.55:
            tt = t / 0.55
            c0, c1 = top, mid
        else:
            tt = (t - 0.55) / 0.45
            c0, c1 = mid, bottom
        rgb = tuple(int(c0[i] + (c1[i] - c0[i]) * tt) for i in range(3))
        for x in range(width):
            px[x, y] = rgb
    img.save(path)


def draw_phone_overlay(path: Path, theme_id: str, character_name: str, messages: list[dict[str, Any]], avatar_path: Path | None) -> None:
    theme = THEMES.get(theme_id, THEMES["dark"])
    canvas = Image.new("RGBA", (CANVAS_W, CANVAS_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)

    header_font = load_font(26, bold=True)
    bubble_font = load_font(28, bold=False)
    ui_font = load_font(18, bold=True)

    phone_x, phone_y, phone_w, phone_h = 150, 120, 420, 1040
    shell_pad = 16
    top_h = 110
    screen_y = phone_y + 140
    screen_h = 830

    shell_bg = hex_rgb(theme["shell_bg"])
    shell_border = hex_rgb(theme["shell_border"])
    top_bg = hex_rgb(theme["top_bg"])
    screen_bg = hex_rgb(theme["screen_bg"])
    incoming = hex_rgb(theme["incoming"])
    outgoing = hex_rgb(theme["outgoing"])
    incoming_text = hex_rgb(theme["incoming_text"])
    outgoing_text = hex_rgb(theme["outgoing_text"])
    top_text = hex_rgb(theme["text"])

    draw.rounded_rectangle(
        (phone_x, phone_y, phone_x + phone_w, phone_y + phone_h),
        radius=48,
        fill=shell_bg + (255,),
        outline=shell_border + (255,),
        width=5,
    )
    draw.rounded_rectangle(
        (phone_x + shell_pad, phone_y + shell_pad, phone_x + phone_w - shell_pad, phone_y + shell_pad + top_h),
        radius=32,
        fill=top_bg + (255,),
    )
    draw.rounded_rectangle(
        (phone_x + shell_pad, screen_y, phone_x + phone_w - shell_pad, screen_y + screen_h),
        radius=32,
        fill=screen_bg + (255,),
    )

    avatar_center = (phone_x + phone_w // 2, phone_y + 58)
    avatar_box = (avatar_center[0] - 28, avatar_center[1] - 28, avatar_center[0] + 28, avatar_center[1] + 28)
    draw.ellipse(avatar_box, fill=(154, 171, 195, 255))
    if avatar_path and avatar_path.exists():
        try:
            avatar = Image.open(avatar_path).convert("RGBA")
            avatar = ImageOps.fit(avatar, (56, 56), centering=(0.5, 0.5))
            mask = Image.new("L", (56, 56), 0)
            ImageDraw.Draw(mask).ellipse((0, 0, 55, 55), fill=255)
            canvas.paste(avatar, (avatar_box[0], avatar_box[1]), mask)
        except Exception:
            pass
    else:
        initial = (character_name[:1] or "D").upper()
        initial_font = load_font(24, bold=True)
        bbox = draw.textbbox((0, 0), initial, font=initial_font)
        draw.text(
            (avatar_center[0] - (bbox[2] - bbox[0]) / 2, avatar_center[1] - (bbox[3] - bbox[1]) / 2 - 1),
            initial,
            fill=(255, 255, 255, 255),
            font=initial_font,
        )

    name_bbox = draw.textbbox((0, 0), character_name or "Omatic", font=header_font)
    draw.text(
        (phone_x + phone_w / 2 - (name_bbox[2] - name_bbox[0]) / 2, phone_y + 87),
        character_name or "Omatic",
        fill=top_text + (255,),
        font=header_font,
    )
    draw.text((phone_x + 30, phone_y + 50), "<", fill=top_text + (255,), font=load_font(34))
    draw.text((phone_x + phone_w - 124, phone_y + 56), "Video", fill=(216, 190, 255, 255), font=ui_font)
    draw.text((phone_x + phone_w - 64, phone_y + 56), "Call", fill=(255, 82, 166, 255), font=ui_font)

    bubble_max_w = 240
    y = screen_y + 26
    bubble_pad_x = 18
    bubble_pad_y = 14
    for message in messages[-8:]:
        lines = wrap_text(draw, sanitize_text(message.get("text", "")) or "...", bubble_font, bubble_max_w - bubble_pad_x * 2)
        line_heights = [draw.textbbox((0, 0), line, font=bubble_font)[3] for line in lines]
        bubble_h = max(54, sum(line_heights) + bubble_pad_y * 2 + (len(lines) - 1) * 4)
        bubble_w = min(
            bubble_max_w,
            max(110, max(draw.textbbox((0, 0), line, font=bubble_font)[2] for line in lines) + bubble_pad_x * 2),
        )
        if message.get("side") == "right":
            x0 = phone_x + phone_w - shell_pad - 22 - bubble_w
            fill = outgoing + (255,)
            text_fill = outgoing_text + (255,)
        else:
            x0 = phone_x + shell_pad + 22
            fill = incoming + (255,)
            text_fill = incoming_text + (255,)
        x1 = x0 + bubble_w
        y1 = y + bubble_h
        draw.rounded_rectangle((x0, y, x1, y1), radius=24, fill=fill)
        text_y = y + bubble_pad_y - 2
        for line in lines:
            draw.text((x0 + bubble_pad_x, text_y), line, fill=text_fill, font=bubble_font)
            bbox = draw.textbbox((0, 0), line, font=bubble_font)
            text_y += (bbox[3] - bbox[1]) + 4
        y = y1 + 14
        if y > screen_y + screen_h - 120:
            break

    canvas.save(path)


def _load_local_xtts() -> Any:
    global _LOCAL_XTTS
    if _LOCAL_XTTS is not None:
        return _LOCAL_XTTS
    try:
        import torch  # type: ignore
        from TTS.api import TTS  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Local XTTS is unavailable. Set ELEVENLABS_API_KEY for hosted Chat Story TTS "
            "or install TTS + torch for local rendering."
        ) from exc

    original_torch_load = torch.load

    def torch_load_compat(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("weights_only", False)
        return original_torch_load(*args, **kwargs)

    torch.load = torch_load_compat  # type: ignore[assignment]
    _LOCAL_XTTS = TTS("tts_models/multilingual/multi-dataset/xtts_v2", gpu=False)
    return _LOCAL_XTTS


def _synthesize_segments_elevenlabs(messages: list[dict[str, Any]], preset: dict[str, Any], work_dir: Path) -> list[Path]:
    if not ELEVENLABS_API_KEY:
        raise RuntimeError("ELEVENLABS_API_KEY is not configured")
    voice_id = str(preset.get("voice_id") or ELEVENLABS_DEFAULT_VOICES["laura"])
    outputs: list[Path] = []
    with httpx.Client(timeout=ELEVENLABS_TIMEOUT_SEC) as client:
        for idx, message in enumerate(messages, start=1):
            text = sanitize_text(message.get("text", ""))
            out_path = work_dir / f"msg_{idx:02d}_raw.mp3"
            response = client.post(
                f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
                headers={
                    "xi-api-key": ELEVENLABS_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "text": text,
                    "model_id": ELEVENLABS_MODEL_ID,
                    "voice_settings": {
                        "stability": 0.5,
                        "similarity_boost": 0.75,
                        "style": 0.3,
                    },
                },
            )
            response.raise_for_status()
            out_path.write_bytes(response.content)
            outputs.append(out_path)
    return outputs


def _synthesize_segments_local_xtts(messages: list[dict[str, Any]], work_dir: Path) -> list[Path]:
    if not VOICE_REFERENCE.exists():
        raise RuntimeError(
            f"Local XTTS voice reference is missing at {VOICE_REFERENCE}. "
            "Set CHATSTORY_VOICE_REFERENCE or configure ELEVENLABS_API_KEY."
        )
    tts = _load_local_xtts()
    outputs: list[Path] = []
    for idx, message in enumerate(messages, start=1):
        text = sanitize_text(message.get("text", ""))
        out_path = work_dir / f"msg_{idx:02d}_raw.wav"
        tts.tts_to_file(text=text, file_path=str(out_path), speaker_wav=str(VOICE_REFERENCE), language=LANGUAGE)
        outputs.append(out_path)
    return outputs


def synthesize_raw_segments(messages: list[dict[str, Any]], preset: dict[str, Any], work_dir: Path) -> list[Path]:
    if ELEVENLABS_API_KEY and not FORCE_LOCAL_TTS:
        return _synthesize_segments_elevenlabs(messages, preset, work_dir)
    return _synthesize_segments_local_xtts(messages, work_dir)


def style_segment(raw_path: Path, out_path: Path, speed: float, pitch: float) -> None:
    speed = clamp(speed, 0.82, 1.28)
    pitch = clamp(pitch, 0.92, 1.08)
    tempo = clamp(speed / max(pitch, 0.01), 0.5, 2.0)
    filt = (
        f"asetrate=24000*{pitch:.5f},aresample=24000,{atempo_chain(tempo)},"
        "highpass=f=70,lowpass=f=12000,"
        "acompressor=threshold=-18dB:ratio=2.2:attack=8:release=90,"
        "alimiter=limit=0.95"
    )
    run(["ffmpeg", "-y", "-i", str(raw_path), "-filter:a", filt, "-ar", "24000", "-ac", "1", str(out_path)])


def build_audio(messages: list[dict[str, Any]], voice_id: str, voice_speed: float, music_id: str, sfx_ids: list[str], work_dir: Path) -> tuple[Path, list[float]]:
    preset = VOICE_PRESETS.get(voice_id, VOICE_PRESETS["studio_voice_core"])
    raw_paths = synthesize_raw_segments(messages, preset, work_dir)

    chosen_sfx = {sid: SFX[sid] for sid in sfx_ids if sid in SFX and SFX[sid].exists()}
    send_sfx = AudioSegment.from_file(str(chosen_sfx["message_send"])) - 5 if "message_send" in chosen_sfx else None
    receive_sfx = AudioSegment.from_file(str(chosen_sfx["message_receive"])) - 6 if "message_receive" in chosen_sfx else None
    awkward_sfx = AudioSegment.from_file(str(chosen_sfx["awkward_pause"])) - 10 if "awkward_pause" in chosen_sfx else None
    comic_sfx = AudioSegment.from_file(str(chosen_sfx["comical_disappointment"])) - 9 if "comical_disappointment" in chosen_sfx else None

    segment_durations: list[float] = []
    cursor = 0
    final_audio = AudioSegment.silent(duration=1)

    for idx, (message, raw_path) in enumerate(zip(messages, raw_paths), start=1):
        styled_path = work_dir / f"msg_{idx:02d}_styled.wav"
        style_segment(
            raw_path,
            styled_path,
            float(voice_speed or 1.0) * float(preset["speed"]),
            float(preset["pitch"]),
        )
        voice_audio = AudioSegment.from_file(str(styled_path))
        lead = 260
        tail = 420
        duration_ms = max(int(len(voice_audio) + lead + tail), 2200)
        while len(final_audio) < cursor + duration_ms + 500:
            final_audio += AudioSegment.silent(duration=1000)

        if message.get("role") == "sender" and send_sfx:
            final_audio = final_audio.overlay(send_sfx, position=cursor + 60)
        if message.get("role") == "receiver" and receive_sfx:
            final_audio = final_audio.overlay(receive_sfx, position=cursor + 60)
        if idx == len(messages) and comic_sfx:
            final_audio = final_audio.overlay(comic_sfx, position=cursor + 140)
        elif awkward_sfx and ("..." in str(message.get("text", "")) or len(str(message.get("text", "")).split()) <= 4):
            final_audio = final_audio.overlay(awkward_sfx, position=cursor + 160)

        final_audio = final_audio.overlay(voice_audio, position=cursor + lead)
        segment_durations.append(duration_ms / 1000.0)
        cursor += duration_ms

    final_audio = final_audio[:cursor + 120]

    music_path = MUSIC.get(music_id)
    if music_path and Path(music_path).exists():
        music = AudioSegment.from_file(str(music_path)) - 24
        music_loop = AudioSegment.silent(duration=0)
        while len(music_loop) < len(final_audio):
            music_loop += music
        final_audio = music_loop[:len(final_audio)].overlay(final_audio)

    out_path = work_dir / "chatstory_voice_mix.wav"
    final_audio.export(out_path, format="wav")
    return out_path, segment_durations


def make_segment_video(bg_source: Path, overlay_png: Path, duration: float, out_path: Path, is_video_bg: bool) -> None:
    if is_video_bg:
        cmd = [
            "ffmpeg",
            "-y",
            "-stream_loop",
            "-1",
            "-i",
            str(bg_source),
            "-i",
            str(overlay_png),
            "-filter_complex",
            (
                f"[0:v]scale={CANVAS_W}:{CANVAS_H}:force_original_aspect_ratio=increase,"
                f"crop={CANVAS_W}:{CANVAS_H},trim=duration={duration:.3f},setpts=PTS-STARTPTS[bg];"
                "[bg][1:v]overlay=0:0:format=auto"
            ),
            "-t",
            f"{duration:.3f}",
            "-r",
            str(FPS),
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            str(out_path),
        ]
    else:
        cmd = [
            "ffmpeg",
            "-y",
            "-loop",
            "1",
            "-i",
            str(bg_source),
            "-i",
            str(overlay_png),
            "-filter_complex",
            f"[0:v]scale={CANVAS_W}:{CANVAS_H}[bg];[bg][1:v]overlay=0:0:format=auto",
            "-t",
            f"{duration:.3f}",
            "-r",
            str(FPS),
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            str(out_path),
        ]
    run(cmd)


def concat_video_clips(clips: list[Path], out_path: Path) -> None:
    manifest = out_path.parent / "concat.txt"
    with manifest.open("w", encoding="utf-8") as handle:
        for clip in clips:
            handle.write(f"file '{clip.as_posix()}'\n")
    run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(manifest), "-c", "copy", str(out_path)])


def mux(video_path: Path, audio_path: Path, out_path: Path) -> None:
    run([
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-shortest",
        str(out_path),
    ])


def build_render(payload: dict[str, Any], avatar_path: Path | None, background_video_path: Path | None, output_path: Path) -> dict[str, Any]:
    messages = [m for m in payload.get("messages", []) if sanitize_text(m.get("text", ""))]
    if not messages:
        raise RuntimeError("No chat messages provided")

    work_dir = Path(tempfile.mkdtemp(prefix="chatstory_render_"))
    try:
        theme_id = str(payload.get("themeId") or "dark")
        background_id = str(payload.get("backgroundId") or "subway")
        character_name = str(payload.get("characterName") or "Omatic")
        voice_id = str(payload.get("voiceId") or "studio_voice_core")
        voice_speed = float(payload.get("voiceSpeed") or 1.0)
        music_id = str(payload.get("musicId") or "none")
        sfx_ids = [str(v) for v in (payload.get("sfxIds") or [])]

        audio_path, segment_durations = build_audio(messages, voice_id, voice_speed, music_id, sfx_ids, work_dir)

        bg_source = work_dir / "background.png"
        is_video_bg = bool(background_video_path and background_video_path.exists())
        if is_video_bg:
            bg_source = background_video_path
        else:
            gradient = BACKGROUNDS.get(background_id, BACKGROUNDS["subway"])["gradient"]
            make_gradient_image(bg_source, gradient)

        clips: list[Path] = []
        for idx, duration in enumerate(segment_durations, start=1):
            overlay_path = work_dir / f"overlay_{idx:02d}.png"
            clip_path = work_dir / f"clip_{idx:02d}.mp4"
            draw_phone_overlay(overlay_path, theme_id, character_name, messages[:idx], avatar_path)
            make_segment_video(bg_source, overlay_path, duration, clip_path, is_video_bg)
            clips.append(clip_path)

        stitched = work_dir / "stitched.mp4"
        concat_video_clips(clips, stitched)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        mux(stitched, audio_path, output_path)

        return {
            "output_file": output_path.name,
            "duration_sec": round(ffprobe_duration(output_path), 2),
            "message_count": len(messages),
            "theme": THEMES.get(theme_id, THEMES["dark"])["label"],
            "background": BACKGROUNDS.get(background_id, BACKGROUNDS["subway"])["label"],
            "voice": VOICE_PRESETS.get(voice_id, VOICE_PRESETS["studio_voice_core"])["name"],
            "used_background_video": is_video_bg,
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--avatar", default="")
    parser.add_argument("--background-video", default="")
    args = parser.parse_args()

    payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
    avatar_path = Path(args.avatar) if args.avatar else None
    background_video_path = Path(args.background_video) if args.background_video else None
    result = build_render(payload, avatar_path, background_video_path, Path(args.output))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
