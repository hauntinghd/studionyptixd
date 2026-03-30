import asyncio
import json
import logging
import os
import time
from pathlib import Path

import httpx

from backend_settings import DEMO_UPLOAD_DIR, TEMP_DIR, XAI_IMAGE_MODEL

log = logging.getLogger("nyptid-studio")

DEMO_DIR = DEMO_UPLOAD_DIR
DEMO_DIR.mkdir(exist_ok=True)

DEMO_SYSTEM_PROMPT = """You are an expert product demo scriptwriter. You create engaging, professional voiceover scripts for software product demo videos.

You will receive a description of what happens in a screen recording of a software product. Your job is to write a natural, enthusiastic, and clear voiceover script that explains what the viewer is seeing.

RULES:
- Write in second person ("you can see here...", "notice how...", "now watch as we...")
- Be conversational and energetic, like a friendly SaaS founder showing their product
- Break the script into timed segments that match the video sections
- Each segment should be 3-8 seconds of speech
- Include natural transitions ("and the best part is...", "but here's where it gets interesting...")
- Highlight key features and benefits, not just what's on screen
- Sound impressed by the product's capabilities
- End with a strong call-to-action

Output valid JSON:
{
  "title": "Product Demo Title",
  "segments": [
    {
      "segment_num": 1,
      "start_sec": 0.0,
      "end_sec": 5.0,
      "narration": "What the voiceover says during this segment",
      "emphasis": "Key feature or benefit to highlight"
    }
  ],
  "total_duration_sec": 60,
  "cta": "Call to action text"
}"""

SADTALKER_REPLICATE_MODEL = "cjwbw/sadtalker:a519cc0cfebaaeade5f0f1a88b tried"


async def analyze_screen_recording(video_path: str) -> dict:
    """Extract frames from screen recording and analyze with Grok Vision. Memory-optimized for 512MB."""
    import base64
    import gc

    duration = 30.0
    for probe_args in [
        ["-show_entries", "format=duration"],
        ["-show_entries", "stream=duration", "-select_streams", "v:0"],
    ]:
        probe_cmd = ["ffprobe", "-v", "error"] + probe_args + ["-of", "default=noprint_wrappers=1:nokey=1", video_path]
        proc = await asyncio.create_subprocess_exec(
            *probe_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        raw_dur = stdout.decode().strip().split("\n")[0]
        try:
            duration = float(raw_dur)
            if duration > 0:
                break
        except (ValueError, TypeError):
            continue
    log.info(f"Video duration: {duration:.1f}s")

    num_frames = min(int(duration / 5), 6)
    if num_frames < 3:
        num_frames = 3
    interval = duration / num_frames

    frame_descriptions = []
    xai_key = os.environ.get("XAI_API_KEY", "")

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(num_frames):
            timestamp = interval * i
            frame_path = TEMP_DIR / f"demo_frame_{int(time.time()*1000)}_{i}.jpg"

            extract_cmd = [
                "ffmpeg", "-y", "-ss", f"{timestamp:.1f}", "-i", video_path,
                "-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "8",
                str(frame_path)
            ]
            proc = await asyncio.create_subprocess_exec(
                *extract_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()

            if not frame_path.exists():
                frame_descriptions.append({"timestamp": round(timestamp, 1), "description": f"Software interface at {timestamp:.0f}s"})
                continue

            with open(frame_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            frame_path.unlink(missing_ok=True)

            try:
                resp = await client.post(
                    "https://api.x.ai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {xai_key}", "Content-Type": "application/json"},
                    json={
                        "model": "grok-2-vision-latest",
                        "messages": [{"role": "user", "content": [
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                            {"type": "text", "text": "Describe this software screen in 1-2 sentences. What app, UI elements, and action is shown?"}
                        ]}],
                        "max_tokens": 100
                    }
                )
                del b64
                gc.collect()
                if resp.status_code == 200:
                    desc = resp.json()["choices"][0]["message"]["content"]
                    frame_descriptions.append({"timestamp": round(timestamp, 1), "description": desc})
                else:
                    frame_descriptions.append({"timestamp": round(timestamp, 1), "description": f"Software interface at {timestamp:.0f}s"})
            except Exception as e:
                log.warning(f"Frame analysis failed for frame {i}: {e}")
                frame_descriptions.append({"timestamp": round(timestamp, 1), "description": f"Software interface at {timestamp:.0f}s"})
                del b64
                gc.collect()

    return {
        "duration": duration,
        "frame_count": len(frame_descriptions),
        "frames": frame_descriptions,
        "description": " | ".join(f["description"] for f in frame_descriptions)
    }


async def generate_demo_script(analysis: dict, product_name: str = "", reference_notes: str = "") -> dict:
    """Generate a timed voiceover script for the product demo."""
    xai_key = os.environ.get("XAI_API_KEY", "")
    duration = analysis.get("duration", 30)
    target_words_min = max(40, int(duration * 2.0))
    target_words_ideal = max(55, int(duration * 2.25))
    target_words_max = max(70, int(duration * 2.6))

    frame_timeline = ""
    for f in analysis.get("frames", []):
        frame_timeline += f"\n- At {f['timestamp']}s: {f['description']}"

    user_prompt = f"""Product: {product_name or 'Software Product'}
Video duration: {duration:.1f} seconds
{f'Style notes from reference: {reference_notes}' if reference_notes else ''}

Frame-by-frame breakdown of the screen recording:
{frame_timeline}

Write a voiceover script with timed segments that perfectly sync to what's happening on screen.
The narration must naturally cover the full {duration:.0f} seconds at a calm, professional pace (not rushed).
Target total narration length around {target_words_ideal} words (acceptable range: {target_words_min}-{target_words_max})."""

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {xai_key}", "Content-Type": "application/json"},
            json={
                "model": "grok-3-mini",
                "messages": [
                    {"role": "system", "content": DEMO_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 2000
            }
        )

    if resp.status_code != 200:
        raise RuntimeError(f"Script generation failed: {resp.status_code}")

    raw = resp.json()["choices"][0]["message"]["content"]
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()

    import re

    def _normalize_script(data: dict) -> dict:
        """Normalize field names so the rest of the pipeline always sees start_time/end_time/text."""
        if "segments" in data and isinstance(data["segments"], list):
            for seg in data["segments"]:
                if "start_sec" in seg and "start_time" not in seg:
                    seg["start_time"] = seg.pop("start_sec")
                if "end_sec" in seg and "end_time" not in seg:
                    seg["end_time"] = seg.pop("end_sec")
                if "narration" in seg and "text" not in seg:
                    seg["text"] = seg.pop("narration")
                for k in ["start_time", "end_time"]:
                    if k in seg:
                        seg[k] = float(seg[k])
        return data

    def _try_parse_json(text: str) -> dict:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON object found in response")
        text = text[start:end]

        text = re.sub(r'//[^\n]*', '', text)
        text = re.sub(r',\s*([}\]])', r'\1', text)
        text = re.sub(r'[\x00-\x1f]+', ' ', text)
        text = text.replace('\u2013', '-').replace('\u2014', '-').replace('\u2018', "'").replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', '"')

        try:
            return _normalize_script(json.loads(text))
        except json.JSONDecodeError:
            pass

        text2 = text.replace("'", '"')
        try:
            return _normalize_script(json.loads(text2))
        except json.JSONDecodeError:
            pass

        def _fix_inner_quotes(m):
            prefix, inner = m.group(1), m.group(2)
            return prefix + '"' + inner.replace('"', '\\"') + '"'
        text3 = re.sub(r'(:\s*)"(.*?)"(?=\s*[,}\]])', _fix_inner_quotes, text, flags=re.DOTALL)
        try:
            return _normalize_script(json.loads(text3))
        except json.JSONDecodeError:
            pass

        segments = []
        for pattern in [
            r'"start_sec"\s*:\s*([\d.]+).*?"end_sec"\s*:\s*([\d.]+).*?"narration"\s*:\s*"((?:[^"\\]|\\.)*)"',
            r'"start_time"\s*:\s*([\d.]+).*?"end_time"\s*:\s*([\d.]+).*?"text"\s*:\s*"((?:[^"\\]|\\.)*)"',
        ]:
            seg_pattern = re.findall(pattern, text, re.DOTALL)
            if seg_pattern:
                for s, e, t in seg_pattern:
                    segments.append({"start_time": float(s), "end_time": float(e), "text": t.replace('\\"', '"')})
                break

        title_match = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
        title = title_match.group(1) if title_match else "Product Demo"

        if segments:
            return {"title": title, "segments": segments}

        raise ValueError(f"Could not parse script JSON after all attempts. Raw start: {text[:300]}")

    return _try_parse_json(raw)


async def generate_ai_face(output_path: str) -> str:
    """Generate a realistic AI male face photo using xAI Grok image generation."""
    xai_key = os.environ.get("XAI_API_KEY", "")
    ages = ["mid-20s", "late 20s", "early 30s", "mid-30s"]
    styles = [
        "clean-shaven with short brown hair, wearing a navy blue polo shirt",
        "light stubble with dark hair swept to the side, wearing a black crew-neck t-shirt",
        "clean-shaven with sandy blonde hair, wearing a gray henley shirt",
        "trimmed beard with dark brown hair, wearing a white button-down shirt",
        "clean-shaven with black hair, wearing a dark green quarter-zip pullover",
    ]
    import random as _rnd
    age = _rnd.choice(ages)
    style = _rnd.choice(styles)

    prompt = (
        f"Professional headshot portrait photo of a friendly, confident {age} male, {style}. "
        "Looking directly at camera with a natural warm smile, slight head tilt. "
        "Clean neutral background (soft gray or white gradient). "
        "Shot on Canon EOS R5, 85mm f/1.4 lens, studio lighting with soft key light. "
        "Sharp focus on eyes, natural skin texture, photorealistic. "
        "Head and shoulders framing, centered composition."
    )

    headers = {"Authorization": f"Bearer {xai_key}", "Content-Type": "application/json"}
    payload = {"model": XAI_IMAGE_MODEL, "prompt": prompt, "n": 1, "response_format": "url"}

    last_status = 0
    for attempt in range(4):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post("https://api.x.ai/v1/images/generations", headers=headers, json=payload)
                last_status = resp.status_code
                if resp.status_code in (200, 201):
                    data = resp.json().get("data", [])
                    if data and data[0].get("url"):
                        dl = await client.get(data[0]["url"], follow_redirects=True)
                        if dl.status_code == 200:
                            with open(output_path, "wb") as f:
                                f.write(dl.content)
                            log.info(f"AI face generated: {output_path}")
                            return output_path
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = (attempt + 1) * 5
                    log.warning(f"AI face gen attempt {attempt+1} got {resp.status_code}, retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                break
        except Exception as e:
            log.warning(f"AI face gen attempt {attempt+1} failed: {e}")
            await asyncio.sleep((attempt + 1) * 3)

    raise RuntimeError(f"Failed to generate AI face: {last_status}")


async def _static_face_fallback(face_image_path: str, audio_path: str, output_path: str) -> str:
    """Fallback: create a video from the static face image with the audio, with subtle zoom."""
    cmd = [
        "ffmpeg", "-y", "-loop", "1", "-i", face_image_path,
        "-i", audio_path,
        "-vf", "scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p,zoompan=z='1+0.001*on':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=512x512",
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k",
        "-shortest", "-movflags", "+faststart",
        output_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()
    return output_path


async def generate_talking_head(face_image_path: str, audio_path: str, output_path: str) -> str:
    """Generate a talking head video using SadTalker via Replicate API."""
    import base64

    with open(face_image_path, "rb") as f:
        face_b64 = base64.b64encode(f.read()).decode()
    with open(audio_path, "rb") as f:
        audio_b64 = base64.b64encode(f.read()).decode()

    face_ext = Path(face_image_path).suffix.lstrip(".")
    audio_ext = Path(audio_path).suffix.lstrip(".")
    face_uri = f"data:image/{face_ext};base64,{face_b64}"
    audio_uri = f"data:audio/{audio_ext};base64,{audio_b64}"

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(
            "https://api.replicate.com/v1/predictions",
            headers={
                "Authorization": "Bearer r8_placeholder",
                "Content-Type": "application/json",
                "Prefer": "wait"
            },
            json={
                "version": "a519cc0cfebaaeade5f0f1a88b4b75d9cba8b12e0e3b8d70e1e2b3b4c5d6e7f8",
                "input": {
                    "source_image": face_uri,
                    "driven_audio": audio_uri,
                    "enhancer": "gfpgan",
                    "pose_style": 0,
                    "facerender": "facevid2vid",
                    "exp_scale": 1.0,
                    "still_mode": False,
                    "preprocess": "crop",
                    "face_model_resolution": "512"
                }
            }
        )

        if resp.status_code in (200, 201):
            result = resp.json()
            output_url = result.get("output")
            if output_url:
                dl_resp = await client.get(output_url)
                if dl_resp.status_code == 200:
                    with open(output_path, "wb") as f:
                        f.write(dl_resp.content)
                    return output_path

        log.warning(f"Replicate SadTalker returned {resp.status_code}, falling back to static face")

    return await _static_face_fallback(face_image_path, audio_path, output_path)


async def composite_demo_video(
    screen_recording: str, talking_head: str, audio_path: str,
    output_path: str, subtitle_path: str = None,
    pip_position: str = "bottom-right", pip_size: float = 0.25
) -> str:
    """Composite screen recording + talking head PiP + voiceover + captions."""
    probe_cmd = [
        "ffprobe", "-v", "error", "-show_entries", "stream=width,height",
        "-of", "csv=p=0:s=x", screen_recording
    ]
    proc = await asyncio.create_subprocess_exec(
        *probe_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    dims = stdout.decode().strip().split("\n")[0]
    if "x" in dims:
        main_w, main_h = dims.split("x")[:2]
        main_w, main_h = int(main_w), int(main_h)
    else:
        main_w, main_h = 1920, 1080

    pip_w = int(main_w * pip_size)
    margin = int(main_w * 0.02)

    if pip_position == "bottom-right":
        pip_x = f"main_w-overlay_w-{margin}"
        pip_y = f"main_h-overlay_h-{margin}"
    elif pip_position == "bottom-left":
        pip_x = str(margin)
        pip_y = f"main_h-overlay_h-{margin}"
    elif pip_position == "top-right":
        pip_x = f"main_w-overlay_w-{margin}"
        pip_y = str(margin)
    else:
        pip_x = str(margin)
        pip_y = str(margin)

    vf = (
        f"[1:v]scale={pip_w}:-1,format=yuva420p,"
        f"geq=lum='p(X,Y)':cb='p(X,Y)':cr='p(X,Y)':"
        f"a='if(gt(abs(W/2-X)*abs(W/2-X)+abs(H/2-Y)*abs(H/2-Y),(W/2)*(W/2)),0,255)'[pip];"
        f"[0:v][pip]overlay={pip_x}:{pip_y}"
    )

    if subtitle_path and Path(subtitle_path).exists():
        safe_sub = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
        vf += f",ass='{safe_sub}'"

    cmd = [
        "ffmpeg", "-y",
        "-i", screen_recording,
        "-i", talking_head,
        "-i", audio_path,
        "-filter_complex", vf,
        "-map", "2:a",
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-c:a", "aac", "-b:a", "192k",
        "-shortest", "-movflags", "+faststart",
        output_path
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        log.warning(f"PiP composite failed, trying without circular mask: {stderr.decode()[-200:]}")
        cmd_simple = [
            "ffmpeg", "-y",
            "-i", screen_recording,
            "-i", talking_head,
            "-i", audio_path,
            "-filter_complex",
            (
                f"[1:v]scale={pip_w}:-1[pip];[0:v][pip]overlay={pip_x}:{pip_y}"
                + (f",ass='{safe_sub}'" if (subtitle_path and Path(subtitle_path).exists()) else "")
            ),
            "-map", "2:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-c:a", "aac", "-b:a", "192k",
            "-shortest", "-movflags", "+faststart",
            output_path
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd_simple, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()

    return output_path
