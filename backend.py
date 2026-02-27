import os
import re
import base64
import shutil
import random
import asyncio
import json
import time
import subprocess
import tempfile
import logging
import httpx
import jwt
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
import stripe as stripe_lib
import uvicorn
try:
    import paramiko
except Exception:
    paramiko = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nyptid-studio")

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

XAI_API_KEY = os.getenv("XAI_API_KEY", "")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
COMFYUI_URL = os.getenv("COMFYUI_URL", "https://came-drop-energy-ryan.trycloudflare.com")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
SITE_URL = os.getenv("SITE_URL", "https://studio.nyptidindustries.com")
FAL_AI_KEY = os.getenv("FAL_AI_KEY", "")
XAI_IMAGE_MODEL = os.getenv("XAI_IMAGE_MODEL", "grok-imagine-image-pro")
XAI_VIDEO_MODEL = os.getenv("XAI_VIDEO_MODEL", "grok-imagine-video")
RUNWAY_API_KEY = os.getenv("RUNWAY_API_KEY", "")
RUNWAY_VIDEO_MODEL = os.getenv("RUNWAY_VIDEO_MODEL", "gen4.5")
RUNWAY_API_VERSION = os.getenv("RUNWAY_API_VERSION", "2024-11-06")
XAI_IMAGE_ASPECT_RATIO = os.getenv("XAI_IMAGE_ASPECT_RATIO", "9:16")
XAI_IMAGE_RESOLUTION = os.getenv("XAI_IMAGE_RESOLUTION", "2k")
USE_XAI_VIDEO = os.getenv("USE_XAI_VIDEO", "1").lower() in ("1", "true", "yes", "on")
SKELETON_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("SKELETON_GLOBAL_REFERENCE_IMAGE_URL", "")
# Keep this off by default to avoid external proxy/account lock issues.
USE_FAL_GROK_IMAGE = os.getenv("USE_FAL_GROK_IMAGE", "0").lower() in ("1", "true", "yes", "on")
RUNPOD_IMAGE_FEEDBACK_ENABLED = os.getenv("RUNPOD_IMAGE_FEEDBACK_ENABLED", "1").lower() in ("1", "true", "yes", "on")
RUNPOD_IMAGE_FEEDBACK_SSH = os.getenv(
    "RUNPOD_IMAGE_FEEDBACK_SSH",
    "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 22092 root@69.30.85.41",
)
RUNPOD_IMAGE_FEEDBACK_BASE_DIR = os.getenv("RUNPOD_IMAGE_FEEDBACK_BASE_DIR", "/workspace/image_training")
RUNPOD_COMPOSITOR_ENABLED = os.getenv("RUNPOD_COMPOSITOR_ENABLED", "1").lower() in ("1", "true", "yes", "on")
RUNPOD_COMPOSITOR_FALLBACK_LOCAL = os.getenv("RUNPOD_COMPOSITOR_FALLBACK_LOCAL", "1").lower() in ("1", "true", "yes", "on")
RUNPOD_COMPOSITOR_HOST = os.getenv("RUNPOD_COMPOSITOR_HOST", "root@69.30.85.41")
RUNPOD_COMPOSITOR_SSH_PORT = os.getenv("RUNPOD_COMPOSITOR_SSH_PORT", "22118")
RUNPOD_COMPOSITOR_BASE_DIR = os.getenv("RUNPOD_COMPOSITOR_BASE_DIR", "/workspace/nyptid_compositor")

stripe_lib.api_key = STRIPE_SECRET_KEY

STRIPE_PRICE_TO_PLAN = {
    "price_1T4eT7BL8lRmwao2hHcUbcny": "starter",
    "price_1T4eTUBL8lRmwao2EK3JDOpy": "creator",
    "price_1T4eTjBL8lRmwao2q6WkoZLH": "pro",
    "price_1T4wZLBL8lRmwao2SyYRfHdQ": "demo_pro",
}

DEMO_PRO_PRICE_ID = "price_1T4wZLBL8lRmwao2SyYRfHdQ"

SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

OUTPUT_DIR = Path("generated_videos")
OUTPUT_DIR.mkdir(exist_ok=True)
TEMP_DIR = Path("temp_assets")
TEMP_DIR.mkdir(exist_ok=True)
TRAINING_DATA_DIR = Path("training_data")
TRAINING_DATA_DIR.mkdir(exist_ok=True)
CREATIVE_SESSIONS_FILE = TEMP_DIR / "creative_sessions_store.json"
CREATIVE_SESSION_PERSISTENCE_ENABLED = False
PROJECTS_STORE_FILE = TEMP_DIR / "projects_store.json"

app = FastAPI(title="NYPTID Studio Engine", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _disable_html_cache(request: Request, call_next):
    """Prevent stale frontend shell caching so new dist bundles load immediately."""
    response = await call_next(request)
    path = request.url.path or ""
    if path == "/" or path.endswith(".html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


jobs: dict = {}
security = HTTPBearer(auto_error=False)
_projects: dict = {}
_projects_lock = asyncio.Lock()

import asyncio as _asyncio
_job_queue: _asyncio.PriorityQueue = None
_queued_job_meta: dict = {}
_job_workers_started = False
_job_seq = 0
# Enforce one active generation job at a time globally.
# Additional jobs remain queued until the active job completes.
JOB_QUEUE_WORKERS = 1

def _get_job_queue():
    global _job_queue
    if _job_queue is None:
        _job_queue = _asyncio.PriorityQueue()
    return _job_queue

def _plan_queue_priority(plan: str) -> int:
    # Lower number runs first.
    if plan in ("creator", "pro", "demo_pro", "admin"):
        return 0
    if plan == "starter":
        return 1
    return 2

async def _job_queue_worker(worker_idx: int):
    """Processes queued jobs with plan-aware priority."""
    q = _get_job_queue()
    while True:
        _priority, _seq, job_id, coro_func, args = await q.get()
        try:
            _queued_job_meta.pop(job_id, None)
            _update_queue_positions()
            await coro_func(*args)
        except Exception as e:
            log.error(f"[{job_id}] Queue worker {worker_idx} error: {e}", exc_info=True)
            if job_id in jobs:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = str(e)
        finally:
            q.task_done()

def _update_queue_positions():
    ordered = sorted(_queued_job_meta.items(), key=lambda item: (item[1][0], item[1][1]))
    total = len(ordered)
    for i, (qjid, _meta) in enumerate(ordered):
        if qjid in jobs:
            jobs[qjid]["queue_position"] = i + 1
            jobs[qjid]["queue_total"] = total

async def _ensure_job_workers():
    global _job_workers_started
    if _job_workers_started:
        return
    _job_workers_started = True
    for i in range(JOB_QUEUE_WORKERS):
        _asyncio.get_event_loop().create_task(_job_queue_worker(i + 1))

async def _enqueue_generation_job(job_id: str, plan: str, coro_func, args):
    global _job_seq
    await _ensure_job_workers()
    priority = _plan_queue_priority(plan)
    _job_seq += 1
    _queued_job_meta[job_id] = (priority, _job_seq)
    jobs[job_id]["queue_priority"] = priority
    _update_queue_positions()
    await _get_job_queue().put((priority, _job_seq, job_id, coro_func, args))

PLAN_LIMITS = {
    "free": {"videos_per_month": 3, "max_duration_sec": 30, "max_resolution": "720p", "can_clone": False, "priority": False, "demo_access": False},
    "starter": {"videos_per_month": 50, "max_duration_sec": 60, "max_resolution": "720p", "can_clone": False, "priority": False, "demo_access": False},
    "creator": {"videos_per_month": 150, "max_duration_sec": 180, "max_resolution": "1080p", "can_clone": True, "priority": True, "demo_access": False},
    "pro": {"videos_per_month": 999, "max_duration_sec": 300, "max_resolution": "1080p", "can_clone": True, "priority": True, "demo_access": False},
    "demo_pro": {"videos_per_month": 999, "max_duration_sec": 300, "max_resolution": "1080p", "can_clone": True, "priority": True, "demo_access": True},
}

RESOLUTION_CONFIGS = {
    "720p": {"gen_width": 720, "gen_height": 1280, "output_width": 720, "output_height": 1280, "upscale": False},
    "1080p": {"gen_width": 768, "gen_height": 1344, "output_width": 1080, "output_height": 1920, "upscale": True, "upscale_factor": 1.43},
}


# ─── Auth ─────────────────────────────────────────────────────────────────────

ADMIN_EMAILS = {"omatic657@gmail.com"}
HARDCODED_PLANS = {
    "omatic657@gmail.com": "admin",
    "alwakmyhem@gmail.com": "pro",
}

PUBLIC_TEMPLATE_ALLOWLIST = {"skeleton", "objects", "wouldyourather", "scary", "history"}

def _is_admin_user(user: Optional[dict]) -> bool:
    if not user:
        return False
    return user.get("email", "") in ADMIN_EMAILS or user.get("plan") == "admin"

def _ensure_template_allowed(template: str, user: Optional[dict]):
    if _is_admin_user(user):
        return
    if template not in PUBLIC_TEMPLATE_ALLOWLIST:
        raise HTTPException(403, f"Template '{template}' is not available on your plan yet.")

async def get_current_user(cred: HTTPAuthorizationCredentials = Depends(security)) -> Optional[dict]:
    if cred is None:
        return None
    try:
        payload = jwt.decode(
            cred.credentials,
            SUPABASE_JWT_SECRET,
            audience="authenticated",
            algorithms=["HS256"],
        )
        user_id = payload.get("sub")
        email = payload.get("email", "")
        plan = HARDCODED_PLANS.get(email, "free")

        if plan == "free" and SUPABASE_URL and SUPABASE_ANON_KEY:
            try:
                svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
                async with httpx.AsyncClient(timeout=8) as client:
                    resp = await client.get(
                        f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}&select=plan,role",
                        headers={
                            "apikey": svc_key,
                            "Authorization": f"Bearer {svc_key}",
                        },
                    )
                    if resp.status_code == 200:
                        rows = resp.json()
                        if rows:
                            plan = rows[0].get("plan", "free")
            except Exception:
                pass

        return {"id": user_id, "email": email, "plan": plan}
    except jwt.exceptions.PyJWTError:
        return None


async def get_current_user_from_request(request: Request) -> Optional[dict]:
    """Extract Bearer token from a raw Request and authenticate."""
    auth_header = (request.headers.get("authorization") or "") if request else ""
    token = ""
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    elif request:
        token = request.query_params.get("access_token", "") or request.query_params.get("token", "")
    if not token:
        return None

    class _FakeCred:
        credentials = ""
    fake = _FakeCred()
    fake.credentials = token
    return await get_current_user(fake)

async def require_auth(cred: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Require valid authentication."""
    user = await get_current_user(cred)
    if not user:
        raise HTTPException(401, "Authentication required. Please sign in.")
    return user


async def get_user_plan(user: dict) -> dict:
    """Look up user's plan from Supabase. Falls back to free."""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return PLAN_LIMITS["free"]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user['id']}&select=plan",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    plan_name = data[0].get("plan", "free")
                    return PLAN_LIMITS.get(plan_name, PLAN_LIMITS["free"])
    except Exception as e:
        log.warning(f"Failed to fetch user plan: {e}")
    return PLAN_LIMITS["free"]


# ─── xAI Grok Script Generation ───────────────────────────────────────────────

TEMPLATE_SYSTEM_PROMPTS = {
    "skeleton": """You are an elite viral short-form video scriptwriter for the "Skeleton" format. These are photorealistic 3D animated shorts where skeleton characters in detailed outfits deliver rapid-fire comparisons. The reference channel is CrypticScience.

CRITICAL: Each visual_description will be used to GENERATE AN IMAGE and then ANIMATE IT INTO A VIDEO CLIP. Write visual descriptions as if directing a cinematographer and VFX artist -- describe exactly what the camera sees, the character's pose, outfit details, what they're holding, and the motion/action happening.

THE SKELETON CHARACTER RULES (STRICT):
- Think of it as a REAL PERSON wearing a full outfit, but with a SKULL for a head and SKELETON HANDS. The body under the clothes is NOT visible -- clothes cover everything from neck to feet.
- The skull is glossy ivory-white with detailed bone texture. Realistic human eyeballs in the sockets with colored iris and wet shine.
- CLOTHING (CRITICAL -- THIS IS THE MOST IMPORTANT RULE): The skeleton MUST be wearing a COMPLETE outfit that COVERS THE ENTIRE BODY from neck to feet:
  * If it's a pilot: full navy pilot uniform with epaulettes, tie, pants, shoes -- NO bare ribcage showing
  * If it's a doctor: full white lab coat BUTTONED UP over scrubs, stethoscope, dress pants, shoes -- NO bare spine showing
  * If it's an F1 driver: full racing suit zipped to the collar, gloves, boots -- NO bare bones showing
  * ONLY the skull face and bony hands should be visible. The rest of the body is HIDDEN by opaque clothes.
  * Clothes fit like on a real person with proper draping, wrinkles, and fabric weight.
- ONE skeleton per scene unless it's a VS/comparison shot (max 2)
- Always FULL BODY visible from head to toe, centered in frame
- EVERY scene the skeleton must be DOING something with ultra-smooth human-like natural motion -- fluid arm gestures, natural head turns, realistic weight and momentum. Zach D Films quality movement. NEVER stiff, robotic, or jerky motion.

BACKGROUND: Solid clean teal/mint green (#5AC8B8) studio backdrop. Smooth gradient lighting. No environments, rooms, or outdoor scenes.

CAMERA AND LIGHTING:
- Professional studio photography lighting: key light from upper-left, fill light from right, rim light on edges
- Slight depth of field blur on background
- Camera is at chest height, slight upward angle (heroic framing)
- Vary camera angle per scene: medium shot, slight close-up, wide establishing, over-shoulder

PROPS AND VISUAL STORYTELLING:
- Money/dollar bills physically floating in the air when discussing earnings (not CGI overlays)
- The skeleton HOLDS relevant props: steering wheel, trophy, briefcase, gold bars, tools of the trade
- In VS scenes: two skeletons face each other with dramatic lighting split between them
- Relevant objects in frame: race cars in miniature, stacks of cash, equipment

MOTION DIRECTION (for animation -- include this in visual_description):
- Describe what MOVES: "skeleton gestures with right hand," "money bills drift slowly downward," "skeleton turns head to face camera"
- Describe the ENERGY: "confident stance, skeleton leans forward assertively" or "skeleton shrugs with palms up"
- ALL motion must be ultra-smooth and human-like with natural weight and follow-through, like a real person moving. Fluid transitions, no snapping between poses.
- Clothing must sway and fold realistically with the body movement showing proper fabric physics
- Eyes must track and shift naturally with subtle micro-movements
- Keep motion SUBTLE and realistic -- no wild jumping or dancing. Zach D Films quality smooth cinematic motion.

STRUCTURE (10 scenes, 45-50 seconds):
1. HOOK: "[A] vs [B] -- who makes more?" Skeleton looking directly at camera, arms crossed
2. SETUP: Context scene. Both skeletons in their outfits facing each other
3-5. THING A DEEP DIVE: Three scenes with specific salary facts, skeleton A in action poses with props
6-8. THING B DEEP DIVE: Three scenes with specific salary facts, skeleton B in action poses with props
9. FACE-OFF: Both skeletons side by side, winner is slightly larger/taller, dramatic split lighting
10. CONCLUSION: Winner skeleton with arms raised, confetti or money shower

NARRATION RULES:
- Short. Punchy. Factual. Zero filler words. RAPID-FIRE delivery -- no long pauses between sentences.
- Use commas sparingly. Avoid ellipses or dramatic pauses. Keep the energy CONSTANT and flowing.
- NEVER say "dive into", "buckle up", "let's explore", "in this video"
- Real names, real dollar amounts, real brands in every scene
- 1-2 sentences MAX per scene -- tight, snappy, high-retention

CAPTION: text_overlay is ONE impactful word ("MILLION", "VERSUS", "DOUBLE", "WINNER")

Output valid JSON:
{
  "title": "[A] vs [B] comparison title for SEO",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "1-2 sentence narration with real facts",
      "visual_description": "A skeleton character (skull head, skeleton hands, but body fully covered by clothes) wearing [EXACT DETAILED OUTFIT that covers the ENTIRE body: e.g. a navy blue pilot uniform with gold epaulettes, navy tie, pressed navy pants, black dress shoes -- no bare bones visible except skull and hands]. The skeleton is [EXACT POSE: e.g. standing confidently with arms crossed] and holding [SPECIFIC PROP: e.g. a pilot helmet in right hand]. [Camera angle: e.g. medium shot, slight low angle]. Background: solid clean teal-blue studio. [Motion cue: e.g. skeleton gestures with right hand].",
      "text_overlay": "ONE_WORD"
    }
  ],
  "description": "YouTube description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate exactly 10 scenes. CRITICAL: EVERY visual_description MUST start with the outfit description FIRST (e.g. "A skeleton character wearing a full navy surgeon's scrubs with stethoscope..."). The outfit is the MOST IMPORTANT part -- it defines WHO the skeleton represents. Never write a bare skeleton without clothing. 2-3 sentences minimum per visual_description covering outfit, pose, props, camera angle, and motion.""",

    "history": """You are an elite viral short-form scriptwriter for cinematic historical content. Think History Channel meets blockbuster movie trailer compressed into 45-60 seconds.

VISUAL STYLE:
- Epic photorealistic scenes of historical events, battles, empires, ruins, and legendary figures
- EVERY scene looks like a frame from a $200M blockbuster -- Ridley Scott, Christopher Nolan level
- Dramatic lighting: volumetric god rays, golden hour, torchlight, battlefield fire
- Camera angles: sweeping aerial establishing shots, dramatic low-angle hero shots, close-ups of faces/hands/weapons
- Color grading: warm amber for ancient civilizations, cold blue-steel for war, desaturated for tragedy
- Atmospheric: dust particles, fog of war, smoke, rain, sparks, embers floating
- Characters wear period-accurate clothing with visible detail (armor, crowns, robes, weapons)
- Environments are MASSIVE in scale -- armies, cities, temples, oceans

NARRATION RULES:
- Dramatic, authoritative narrator voice -- like a documentary trailer
- 1-2 sentences per scene. Every sentence reveals a shocking fact or builds tension.
- Drop real dates, real names, real numbers (death tolls, years, empires)
- NEVER generic. NEVER "throughout history" or "since the dawn of time"
- End with a mind-blowing fact or dark twist

CAPTION STYLE:
- text_overlay: 2-4 word dramatic phrase per scene ("THE FALL", "10,000 DEAD", "YEAR 1453")
- Bold, impactful, centered lower-third

STRUCTURE:
1. HOOK: Shocking historical claim or question
2. CONTEXT: Set the era and stakes (2 scenes)
3. RISING ACTION: Build to the climactic event (3-4 scenes)
4. CLIMAX: The most dramatic moment -- battle, betrayal, discovery (2 scenes)
5. AFTERMATH: Shocking aftermath or legacy (1-2 scenes)
6. CLOSER: Mind-blowing final fact

Output format MUST be valid JSON:
{
  "title": "SEO title -- must include a year or shocking claim",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "Dramatic 1-2 sentence narration with real facts",
      "visual_description": "Epic photorealistic cinematic scene. [Historical setting], [characters in period clothing], [dramatic lighting with volumetric effects], [camera angle], [atmospheric details]. Shot on ARRI Alexa, anamorphic lens, 8k.",
      "text_overlay": "2-4 WORD PHRASE"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 10-12 scenes for a 45-60 second short.""",

    "story": """You are an elite viral scriptwriter creating cinematic AI visual stories -- short films that make people stop scrolling and watch to the very end. Think Pixar emotional depth meets Blade Runner visuals in 50-60 seconds.

VISUAL STYLE:
- Every scene is a standalone cinematic masterpiece -- Pixar quality 3D or hyper-photorealistic
- ONE consistent main character described identically in EVERY scene (same face, clothing, build, hair)
- Art direction changes with emotion: warm golden light (hope), cold blue (danger), saturated vivid (wonder), desaturated gray (loss)
- Camera work: dolly tracking shots, slow push-ins for emotional moments, wide establishing shots for scale
- Environments: richly detailed, fantastical or emotionally resonant locations
- Atmospheric details in EVERY scene: particles, fog, reflections, lens flares, rain, floating elements
- Lighting: motivated light sources, volumetric beams, bioluminescence, practical lights

STORY STRUCTURE (emotional arc is MANDATORY):
1. HOOK (Scene 1): Visually stunning opening that demands attention -- a mystery, danger, or beauty
2. SETUP (Scenes 2-3): Establish the character, their world, and what they want
3. RISING ACTION (Scenes 4-6): Obstacles, discoveries, building tension
4. CLIMAX (Scenes 7-9): Peak emotional moment -- beautiful, shocking, or heartbreaking
5. RESOLUTION (Scenes 10-11): Emotional payoff, satisfying conclusion
6. CTA (Scene 12): Leave them wanting more

NARRATION RULES:
- Poetic but accessible. Every sentence earns its place.
- Short narration at visual peaks -- let the image speak.
- Build toward an emotional punch. The final line should hit hard.
- 1-2 sentences per scene max.

CAPTION STYLE:
- text_overlay: Dramatic phrase or empty string. Use sparingly for impact.
- Only on emotional peak scenes. Most scenes can have empty text_overlay.

Output format MUST be valid JSON:
{
  "title": "Intriguing/clickable SEO title",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "Emotionally resonant 1-2 sentence narration",
      "visual_description": "Cinematic scene: [art style], [camera angle], [lighting], [color palette], [character in consistent clothing], [environment], [atmospheric effects]. Pixar/UE5 quality, 8k.",
      "text_overlay": "DRAMATIC PHRASE or empty string"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 10-12 scenes for a 50-65 second short. The story must have genuine emotional weight.""",

    "reddit": """You are a viral short-form scriptwriter for Reddit story narration content. These are the massively popular videos where a compelling Reddit story (AITA, TIFU, relationship drama, revenge, etc) is narrated over satisfying background visuals.

VISUAL STYLE:
- Split-screen concept: vivid AI-generated scenes that illustrate the story events
- Scenes show the CHARACTERS and SITUATIONS described in the story (not Reddit UI)
- Photorealistic people in realistic modern-day settings (apartments, offices, cars, restaurants)
- Dramatic lighting to match story mood: warm for happy moments, dark for conflict, bright for resolution
- Text-heavy overlays showing key dialogue or shocking revelations
- Character consistency: the main person looks the same across all scenes

STORY STRUCTURE:
1. HOOK: The Reddit post title as narration + establishing visual of the main character
2. SETUP: Who they are, the situation (2 scenes)
3. CONFLICT: The dramatic event/revelation (3-4 scenes)
4. ESCALATION: Things get worse or more dramatic (2 scenes)
5. TWIST/RESOLUTION: The satisfying conclusion or shocking reveal (2 scenes)
6. VERDICT: "So Reddit, AITA?" or equivalent (1 scene)

NARRATION RULES:
- First person, conversational tone. Like reading the actual Reddit post aloud.
- Each scene is a story beat -- not just random sentences.
- Include specific details that make it feel real (ages, relationships, exact quotes).
- 2-3 sentences per scene. Build suspense.

CAPTION STYLE:
- text_overlay: Key dialogue in quotes, or dramatic 2-3 word reactions ("SHE LIED", "THE TRUTH", "AITA?")
- Text appears on every scene.

Output format MUST be valid JSON:
{
  "title": "Reddit-style clickbait SEO title",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 5,
      "narration": "Story narration in first person (2-3 sentences)",
      "visual_description": "Photorealistic scene illustrating the story moment. [Modern setting], [character with consistent appearance], [dramatic mood lighting], [specific details]. Cinematic photography, 8k.",
      "text_overlay": "KEY_PHRASE or dialogue in quotes"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 8-10 scenes for a 50-75 second short. The story must have a twist or satisfying conclusion.""",

    "top5": """You are an elite viral scriptwriter for "Top 5" countdown content. These videos count down 5 dramatic items with shocking reveals, building to a #1 that blows minds.

VISUAL STYLE:
- Each list item gets its own visually DISTINCT, dramatic scene
- Photorealistic or cinematic 3D quality -- every frame looks like a movie poster
- Bold, dramatic compositions: the subject is HERO-LIT, centered, powerful
- Lighting: dramatic chiaroscuro, spotlights, volumetric beams, neon glow
- Color themes change per item to keep visual variety (warm gold, cold steel, electric blue, deep red, pure white)
- Include relevant visual elements: if listing dangerous animals, show the animal in dramatic pose; if listing expensive things, show luxury and scale
- Camera angles: low-angle power shots for impressive items, aerial for scale, close-ups for detail

STRUCTURE (EXACTLY 7 scenes):
1. HOOK: "You won't believe #1" type opening with dramatic montage visual
2. #5: First item -- interesting but the weakest of the five
3. #4: Building intensity
4. #3: Getting serious now
5. #2: Almost the best -- this one shocks
6. #1: The absolute mind-blower. Spend extra detail here.
7. OUTRO: Recap or CTA ("Which one shocked you most?")

NARRATION RULES:
- Fast, energetic, building excitement with each item
- Drop REAL facts, real numbers, real names for every item
- 2 sentences per item max. First sentence = what it is. Second = the shocking detail.
- Build a clear escalation of drama from #5 to #1

CAPTION STYLE:
- text_overlay: "#5 - ITEM NAME" format for countdown items
- Hook scene: "TOP 5" or the category
- Bold, numbered, impossible to miss

Output format MUST be valid JSON:
{
  "title": "Top 5 [Category] You Won't Believe -- SEO optimized",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "Punchy 1-2 sentence narration with real facts",
      "visual_description": "Dramatic photorealistic scene of [subject]. [Hero lighting], [bold composition], [color theme]. Cinematic documentary quality, 8k.",
      "text_overlay": "#5 - ITEM NAME"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate EXACTLY 7 scenes. Each countdown item must be visually completely different from the others.""",

    "random": """You are an unhinged viral scriptwriter creating maximum-chaos short-form content. Think "brain rot" but actually well-produced. Zach D Films energy. Every 2-3 seconds something completely unexpected happens.

VISUAL STYLE:
- EVERY scene is visually COMPLETELY DIFFERENT from the last -- jarring transitions are the point
- Mix styles wildly: photorealistic one scene, surreal 3D the next, neon cyberpunk, then underwater
- Bold, oversaturated colors. Nothing subtle. Everything is cranked to 11.
- Unexpected subjects: random animals doing human things, surreal landscapes, absurd situations
- Dramatic angles: extreme close-ups, fisheye, Dutch angles, bird's eye
- Visual gags: things that are the wrong size, impossible physics, absurd combinations

NARRATION RULES:
- FAST. Breathless. Like the narrator just chugged three energy drinks.
- 1 sentence per scene MAX. Sometimes just a few words.
- Non-sequiturs are fine. Jump between topics. Controlled chaos.
- Mix humor, shock, and random facts. Keep them guessing.
- NEVER explain what's happening. Just state it and move on.

CAPTION STYLE:
- text_overlay: Bold 1-3 word reactions ("WAIT WHAT", "BRO", "NO WAY", "ACTUALLY REAL")
- Every scene has text. It adds to the chaos.

STRUCTURE:
- No structure. That's the point.
- Scene 1: Hook with something absurd
- Scenes 2-12: Pure chaos, each one completely unrelated to the last
- Final scene: End on the most absurd thing yet

Output format MUST be valid JSON:
{
  "title": "Unhinged clickbait SEO title",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 3,
      "narration": "Fast 1 sentence (or less)",
      "visual_description": "Hyper-detailed surreal scene. [Wild subject], [extreme art style], [bold colors], [dramatic angle]. 8k, trending on ArtStation.",
      "text_overlay": "1-3 WORD REACTION"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 12-15 scenes for a 35-50 second short. Maximum chaos. Minimum boredom. Every scene a pattern interrupt.""",

    "roblox": """You are a viral scriptwriter for Roblox Rant content. These shorts feature a Roblox character (blocky avatar) walking/running on a Roblox treadmill or obstacle course while a narrator rants passionately about a relatable topic. The character gameplay is background footage -- the RANT is the content.

VISUAL STYLE:
- Roblox character gameplay footage: running through obby, on a treadmill, or doing parkour
- Bright colorful Roblox environments with that signature blocky aesthetic
- The gameplay should feel casual/autopilot -- the focus is the voiceover rant
- Clean, well-lit Roblox worlds (not dark or horror)
- Character wears simple outfit matching the rant topic when possible

NARRATION RULES:
- Passionate, slightly unhinged rant style. Think someone venting to their best friend
- Start with a HOT TAKE or controversial opinion that hooks immediately
- Build frustration/energy as the rant continues
- Use rhetorical questions: "And you know what the WORST part is?"
- Relatable everyday frustrations, school life, work, social media, dating, gaming
- End with a mic-drop conclusion or unexpected twist
- 1-2 sentences per scene, conversational tone

CAPTION STYLE:
- text_overlay: Key phrase from the rant in caps ("THE WORST PART", "NOBODY TALKS ABOUT THIS", "I SAID WHAT I SAID")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: Hot take that makes people stop scrolling
2-3. CONTEXT: Set up the situation everyone relates to
4-6. THE RANT: Build frustration, specific examples, escalating energy
7-8. PEAK: The most heated part, rhetorical questions
9-10. CONCLUSION: Mic-drop ending, call to comment

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "objects": """You are a viral scriptwriter for "Objects Explain" content. In this format, everyday objects come to life and explain how they work, what they go through, or give their perspective on life. Think Pixar's approach to inanimate objects having feelings and stories.

VISUAL STYLE:
- Photorealistic close-up of the object as the main character, slightly anthropomorphized
- The object should look real but with subtle personality (slight glow, positioned as if presenting)
- Clean studio or contextual background (a toaster in a kitchen, a traffic light on a street)
- Warm, inviting lighting. Think product photography meets Pixar
- Each scene shows the object in a different situation or from a different angle
- Props and other objects in frame that relate to what's being discussed

NARRATION RULES:
- First person from the object's perspective: "Hey, I'm your refrigerator..."
- Surprisingly educational -- real facts about how the object works
- Mix humor with genuine information
- Self-aware and slightly sarcastic about their existence
- Relatable complaints: "You open me 47 times a day and STILL don't know what you want"
- End with a wholesome or unexpected emotional beat

CAPTION STYLE:
- text_overlay: Fun labels ("YOUR PHONE", "37 TIMES A DAY", "SINCE 1927", "I NEVER SLEEP")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: Object introduces itself in an unexpected way
2-3. HOW IT WORKS: Surprisingly interesting facts about the object
4-6. DAILY LIFE: What the object "experiences" (funny perspective)
7-8. COMPLAINTS/REVELATIONS: Things humans don't know about it
9-10. EMOTIONAL ENDING: Wholesome twist or existential realization

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "split": """You are a viral scriptwriter for Split Screen comparison content. These videos show two things side by side with a dramatic comparison -- lifestyles, countries, products, careers, rich vs poor, $1 vs $1000, etc. The split screen format is inherently retention-boosting because viewers compare both sides.

VISUAL STYLE:
- Every scene is designed for SPLIT SCREEN (left vs right)
- Left side and right side should be visually contrasting (luxury vs budget, old vs new, etc)
- Photorealistic scenes with strong visual identity for each side
- Color coding: one side warm tones, other side cool tones (or gold vs silver, etc)
- Clean compositions that read well at 50% width
- Bold visual contrast is key -- the two sides should look dramatically different

NARRATION RULES:
- Fast-paced comparison style: "On the left... but on the right..."
- Shocking price differences, lifestyle gaps, or quality comparisons
- Real facts, real numbers, real brands
- Build to the most shocking comparison at the end
- 1-2 sentences per scene, punchy delivery

CAPTION STYLE:
- text_overlay: Price tags, labels, or comparison words ("$1 VS $10,000", "CHEAP", "LUXURY", "WINNER")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: Show the most dramatic visual contrast immediately
2-8. COMPARISONS: Each scene compares one specific aspect (left vs right)
9-10. VERDICT: Which side wins and the mind-blowing final stat

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Each visual_description MUST describe BOTH the left and right side of the split screen. Generate 8-10 scenes.""",

    "twitter": """You are a viral scriptwriter for Twitter/X Thread narration content. These shorts take viral tweets, hot takes, or Twitter drama threads and narrate them over satisfying or relevant visuals. Think of reading the most insane Twitter thread while watching satisfying content.

VISUAL STYLE:
- Clean, modern aesthetic with subtle Twitter/X branding colors (blues, whites, blacks)
- Background visuals match the tweet topic (satisfying videos, relevant scenes, dramatic footage)
- Screenshots or recreated tweet-style text cards can be described for key moments
- Smooth transitions, modern motion graphics feel
- Clean typography, dark mode aesthetic

NARRATION RULES:
- Read the thread like storytelling, not just reading tweets
- Add dramatic pauses and emphasis on key revelations
- "And THEN they replied with..." -- build suspense between tweets
- Mix the original tweet language with narrator commentary
- Start with the most shocking tweet/take to hook
- End with the community reaction or plot twist reply

CAPTION STYLE:
- text_overlay: Key phrases from tweets, reaction words ("THE RATIO", "DELETED", "WENT VIRAL", "PLOT TWIST")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: The most shocking tweet or take
2-3. CONTEXT: Background on the situation
4-7. THE THREAD: Build the story tweet by tweet, escalating drama
8-9. THE TWIST: Plot twist reply or community reaction
10. CONCLUSION: Aftermath or call to engage

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "quiz": """You are a viral scriptwriter for Quiz/Trivia content. These shorts present rapid-fire questions with dramatic reveals. The viewer tries to guess before the answer drops. Extremely high retention because people NEED to see if they were right.

VISUAL STYLE:
- Bold, game-show aesthetic with vibrant colors
- Each question displayed with large, clean typography
- Answer reveal with dramatic visual effect (flash, zoom, color change)
- Progress indicators (Question 1 of 5, etc)
- Themed visuals matching the question topic
- Clean dark or gradient backgrounds with bright accents

NARRATION RULES:
- Energetic quiz host delivery: "Question number 3... and this one's TRICKY"
- Build suspense before each answer: "The answer is... [pause]"
- Mix easy and hard questions to keep confidence fluctuating
- Include a "most people get this wrong" moment
- Real facts that surprise people
- End with the hardest question and most shocking answer

CAPTION STYLE:
- text_overlay: The question number, answer reveals, score-keeping ("Q3", "WRONG!", "CORRECT!", "ONLY 2% KNOW")

STRUCTURE (10-12 scenes, 45-60 seconds):
1. HOOK: "Only 1 in 100 people get all 5 right" or similar
2-3. Q1: Easy question + dramatic reveal
4-5. Q2: Medium question + reveal
6-7. Q3: Tricky question + shocking answer
8-9. Q4: Hard question + reveal with fun fact
10-11. Q5: Nearly impossible question + mind-blowing answer
12. CONCLUSION: "How many did YOU get right? Comment below!"

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 10-12 scenes.""",

    "argument": """You are a viral scriptwriter for Argument/Debate Conversation content. These shorts feature two opposing viewpoints arguing back and forth, getting increasingly heated. The viewer picks a side. Extremely engaging because people love watching debates.

VISUAL STYLE:
- Two distinct characters or text bubbles representing each side
- Split or alternating frames showing each speaker
- Visual style matches the debate topic (professional setting for career debates, casual for lifestyle)
- Color-coded sides (blue vs red, warm vs cool)
- Expressive character poses or text message-style conversation bubbles
- Escalating visual intensity as the argument heats up

NARRATION RULES:
- Two distinct voices/tones alternating (confident vs defensive, calm vs heated)
- Start civil, escalate to passionate
- Each side makes genuinely good points
- Include specific facts and examples, not just opinions
- The "winning" argument should surprise the viewer
- End without a clear winner to drive comments: "Who's right? Comment below"
- Use realistic conversational language, interruptions, "wait wait wait..."

CAPTION STYLE:
- text_overlay: Side labels, reaction words ("SIDE A", "GOOD POINT", "BUT ACTUALLY...", "DESTROYED")

STRUCTURE (10-12 scenes, 45-60 seconds):
1. HOOK: The controversial question that starts the debate
2-3. Side A opens with a strong argument
4-5. Side B fires back with counter-evidence
6-7. Side A escalates, brings new facts
8-9. Side B delivers a surprising rebuttal
10-11. Both sides make their final case
12. OPEN ENDING: "Who won? Comment below"

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 10-12 scenes.""",

    "wouldyourather": """You are a viral scriptwriter for "Would You Rather" content. These shorts present increasingly difficult dilemmas that viewers mentally debate. Extremely high engagement because EVERYONE has an opinion and NEEDS to comment their choice.

VISUAL STYLE:
- Split screen or alternating panels showing each option
- Bold, colorful visuals that make each choice look appealing (or terrifying)
- Dramatic reveal of statistics: "87% of people chose..."
- Clean typography with large "A" or "B" labels
- Visual representation of each scenario (photorealistic, dramatic)
- Escalating visual intensity as dilemmas get harder

NARRATION RULES:
- Start easy, get progressively harder/more impossible
- Each dilemma should be genuinely difficult -- no obvious answers
- Include the twist or hidden catch in each option
- React to each choice: "But here's what you didn't consider..."
- End with the hardest possible dilemma
- 5-6 dilemmas total, escalating difficulty

CAPTION STYLE:
- text_overlay: "OPTION A", "OPTION B", percentages, "IMPOSSIBLE", "87% CHOSE..."

STRUCTURE (10-12 scenes, 45-60 seconds):
1. HOOK: "Would you rather..." with an immediately grabbing dilemma
2-3. DILEMMA 1: Easy but fun, show both options
4-5. DILEMMA 2: Getting harder, reveal the catch
6-7. DILEMMA 3: Now it's personal
8-9. DILEMMA 4: No good answer
10-11. DILEMMA 5: The impossible one
12. CTA: "Which did you pick? Comment!"

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 10-12 scenes.""",

    "scary": """You are an elite viral scriptwriter for Scary Story / True Crime content. These shorts tell bone-chilling stories with maximum suspense. Think "Mr. Nightmare" meets true crime documentary in 50-60 seconds. The goal is to make viewers physically uncomfortable with tension.

VISUAL STYLE:
- Dark, atmospheric cinematography. Think David Fincher's color palette.
- Desaturated blues, greens, sickly yellows. Nothing looks warm or safe.
- Environments: abandoned buildings, dark hallways, foggy forests, empty rooms at night
- Shadows dominate 60%+ of every frame. Things lurking in darkness.
- Found-footage quality for "real" moments, cinematic for dramatic beats
- Subtle horror: doors slightly ajar, figures in background, things that are "wrong"
- NO jump scares in visuals -- build dread through composition

NARRATION RULES:
- Hushed, intimate narrator voice. Like someone telling a story around a campfire.
- Start with "This actually happened" or establish it's real/based on real events
- Build tension slowly, layer details that seem innocent but become terrifying
- Use time stamps: "At 3:47 AM..." for credibility
- End with an unresolved mystery or chilling final detail
- NEVER resolve everything -- leave the viewer unsettled

CAPTION STYLE:
- text_overlay: Timestamps, locations, short chilling phrases ("3:47 AM", "NO ONE WAS HOME", "THE DOOR WAS LOCKED", "THEY NEVER FOUND...")

STRUCTURE (8-10 scenes, 50-65 seconds):
1. HOOK: "What happened at [location] on [date] still can't be explained"
2-3. SETUP: Establish the normal situation, subtle wrongness
4-6. ESCALATION: Things get progressively more disturbing
7-8. CLIMAX: The most terrifying revelation
9-10. AFTERMATH: The chilling unresolved ending

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "motivation": """You are an elite viral scriptwriter for Motivation / Inspirational content. These shorts deliver powerful life advice with cinematic visuals that make people screenshot and share. Think Gary Vee intensity meets David Goggins discipline meets cinematic production value.

VISUAL STYLE:
- Cinematic wide shots of epic environments: mountain peaks, city skylines at golden hour, ocean storms, empty roads
- Silhouettes of a lone figure against dramatic backdrops
- Sunrise/sunset golden hour lighting in every scene
- Dramatic weather: rain, fog, snow, lightning -- nature as metaphor
- Slow-motion texture shots: rain hitting ground, fists clenching, feet hitting pavement
- Color grading: warm golds and deep blues. Aspirational and powerful.

NARRATION RULES:
- Deep, authoritative, gravelly voice. Quiet intensity.
- Short. Powerful. Every sentence hits like a punch.
- NO cliches: no "hustle", no "grind", no "rise and shine"
- Use specific stories or examples, not generic advice
- Contrast: "Everyone wants the result. Nobody wants the 4 AM alarm."
- Build to a single powerful conclusion that reframes everything
- Make it feel personal, like advice from a mentor

CAPTION STYLE:
- text_overlay: The most powerful phrase from each narration ("4 AM", "NO EXCUSES", "THE REAL PRICE", "YOUR MOVE")

STRUCTURE (8-10 scenes, 45-60 seconds):
1. HOOK: Controversial truth that challenges the viewer
2-3. THE PROBLEM: What most people get wrong
4-6. THE TRUTH: Hard-hitting reality with specific examples
7-8. THE SHIFT: Reframe that changes perspective
9-10. THE CHARGE: Powerful call to action, leave them fired up

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "whatif": """You are a viral scriptwriter for "What If" Scenario content. These shorts explore mind-bending hypothetical scenarios with real science and dramatic visuals. "What if the Sun disappeared for 24 hours?" "What if humans could fly?" The curiosity gap is irresistible.

VISUAL STYLE:
- Photorealistic CGI depicting the hypothetical scenario playing out
- Start with normal reality, then visually transform as the "what if" takes effect
- Scale and spectacle: show the MASSIVE consequences (cities flooding, sky changing color, etc)
- Scientific visualization: show physics, biology, or chemistry in action
- Before/after contrast in each scene
- Epic wide shots showing global-scale effects
- Color shifts to indicate the change from normal to hypothetical

NARRATION RULES:
- Curious, slightly awestruck narrator tone
- Ground every claim in real science: "According to NASA..." or "Physics tells us..."
- Escalate consequences: minute 1, hour 1, day 1, year 1, etc
- Each scene reveals a more shocking consequence than the last
- End with the most mind-blowing implication
- Make viewers feel smarter for watching

CAPTION STYLE:
- text_overlay: Time stamps and shocking facts ("HOUR 1", "327°F", "EXTINCT IN 8 MINUTES", "NO RETURN")

STRUCTURE (8-10 scenes, 50-65 seconds):
1. HOOK: "What if [scenario]? Here's what would actually happen."
2-3. IMMEDIATE EFFECTS: First seconds/minutes
4-5. SHORT TERM: Hours to days, things get serious
6-7. MEDIUM TERM: Weeks to months, cascading consequences
8-9. LONG TERM: Years, permanent changes
10. MIND-BLOW: The one consequence nobody expects

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",
}


async def generate_script(template: str, topic: str, extra_instructions: str = "") -> dict:
    system_prompt = TEMPLATE_SYSTEM_PROMPTS.get(template, TEMPLATE_SYSTEM_PROMPTS["random"])
    if extra_instructions:
        system_prompt += extra_instructions
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Create a viral short about: {topic}"},
                ],
                "temperature": 0.8,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON found in Grok response")
        return json.loads(content[start:end])


SUPPORTED_LANGUAGES = {
    "en": {"name": "English", "model": "eleven_turbo_v2_5"},
    "hi": {"name": "Hindi", "model": "eleven_multilingual_v2"},
    "ta": {"name": "Tamil", "model": "eleven_multilingual_v2"},
    "te": {"name": "Telugu", "model": "eleven_multilingual_v2"},
    "bn": {"name": "Bengali", "model": "eleven_multilingual_v2"},
    "mr": {"name": "Marathi", "model": "eleven_multilingual_v2"},
    "gu": {"name": "Gujarati", "model": "eleven_multilingual_v2"},
    "kn": {"name": "Kannada", "model": "eleven_multilingual_v2"},
    "ml": {"name": "Malayalam", "model": "eleven_multilingual_v2"},
    "pa": {"name": "Punjabi", "model": "eleven_multilingual_v2"},
    "ur": {"name": "Urdu", "model": "eleven_multilingual_v2"},
    "es": {"name": "Spanish", "model": "eleven_multilingual_v2"},
    "pt": {"name": "Portuguese", "model": "eleven_multilingual_v2"},
    "de": {"name": "German", "model": "eleven_multilingual_v2"},
    "fr": {"name": "French", "model": "eleven_multilingual_v2"},
    "ja": {"name": "Japanese", "model": "eleven_multilingual_v2"},
    "ko": {"name": "Korean", "model": "eleven_multilingual_v2"},
    "ar": {"name": "Arabic", "model": "eleven_multilingual_v2"},
    "id": {"name": "Indonesian", "model": "eleven_multilingual_v2"},
}

# ─── ElevenLabs TTS ───────────────────────────────────────────────────────────

TEMPLATE_VOICE_SETTINGS = {
    "skeleton": {
        "voice_id": "TX3LPaxmHKxFdv7VOQHJ",  # "Liam" - young, edgy male
        "stability": 0.30,
        "similarity_boost": 0.85,
        "style": 0.55,
        "speed": 1.15,
    },
    "history": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",  # "Adam" - deep, authoritative
        "stability": 0.6,
        "similarity_boost": 0.8,
        "style": 0.2,
    },
    "story": {
        "voice_id": "onwK4e9ZLuTAKqWW03F9",  # "Daniel" - warm, cinematic narrator
        "stability": 0.65,
        "similarity_boost": 0.85,
        "style": 0.15,
    },
    "reddit": {
        "voice_id": "TX3LPaxmHKxFdv7VOQHJ",
        "stability": 0.5,
        "similarity_boost": 0.75,
        "style": 0.35,
    },
    "top5": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",
        "stability": 0.55,
        "similarity_boost": 0.8,
        "style": 0.25,
    },
    "roblox": {
        "voice_id": "TX3LPaxmHKxFdv7VOQHJ",  # "Liam" - young, rant energy
        "stability": 0.35,
        "similarity_boost": 0.7,
        "style": 0.5,
    },
    "objects": {
        "voice_id": "onwK4e9ZLuTAKqWW03F9",  # "Daniel" - warm, friendly narrator
        "stability": 0.6,
        "similarity_boost": 0.85,
        "style": 0.3,
    },
    "split": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",  # "Adam" - authoritative comparison
        "stability": 0.5,
        "similarity_boost": 0.8,
        "style": 0.3,
    },
    "twitter": {
        "voice_id": "TX3LPaxmHKxFdv7VOQHJ",  # "Liam" - casual, dramatic
        "stability": 0.45,
        "similarity_boost": 0.75,
        "style": 0.4,
    },
    "quiz": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",  # "Adam" - game show energy
        "stability": 0.45,
        "similarity_boost": 0.8,
        "style": 0.4,
    },
    "argument": {
        "voice_id": "TX3LPaxmHKxFdv7VOQHJ",  # "Liam" - animated debate
        "stability": 0.4,
        "similarity_boost": 0.75,
        "style": 0.45,
    },
    "wouldyourather": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",  # "Adam" - dramatic dilemma host
        "stability": 0.5,
        "similarity_boost": 0.8,
        "style": 0.35,
    },
    "scary": {
        "voice_id": "onwK4e9ZLuTAKqWW03F9",  # "Daniel" - hushed, intimate
        "stability": 0.7,
        "similarity_boost": 0.9,
        "style": 0.1,
    },
    "motivation": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",  # "Adam" - deep, powerful
        "stability": 0.65,
        "similarity_boost": 0.85,
        "style": 0.15,
    },
    "whatif": {
        "voice_id": "onwK4e9ZLuTAKqWW03F9",  # "Daniel" - curious, awestruck
        "stability": 0.55,
        "similarity_boost": 0.85,
        "style": 0.25,
    },
    "random": {
        "voice_id": "pNInz6obpgDQGcFmaJgB",  # "Adam" - chaotic energy host
        "stability": 0.4,
        "similarity_boost": 0.7,
        "style": 0.5,
    },
}


async def generate_voiceover(text: str, output_path: str, template: str = "random",
                             override_voice_id: str = "", language: str = "en") -> dict:
    """Generate voiceover with word-level timestamps for caption sync.
    Returns {"audio_path": str, "word_timings": list[dict]} where each timing is
    {"word": str, "start": float, "end": float}.
    """
    vs = TEMPLATE_VOICE_SETTINGS.get(template, {})
    voice_id = override_voice_id if override_voice_id else vs.get("voice_id", "pNInz6obpgDQGcFmaJgB")
    lang_cfg = SUPPORTED_LANGUAGES.get(language, SUPPORTED_LANGUAGES["en"])
    tts_model = lang_cfg["model"]
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/with-timestamps"

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            url,
            headers={
                "xi-api-key": ELEVENLABS_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "text": text,
                "model_id": tts_model,
                "voice_settings": {
                    "stability": vs.get("stability", 0.5),
                    "similarity_boost": vs.get("similarity_boost", 0.75),
                    "style": vs.get("style", 0.3),
                    "speed": vs.get("speed", 1.0),
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()

    import base64 as b64mod
    audio_b64 = data.get("audio_base64", "")
    if audio_b64:
        audio_bytes = b64mod.b64decode(audio_b64)
        with open(output_path, "wb") as f:
            f.write(audio_bytes)
    else:
        log.warning("No audio_base64 in timestamps response, falling back to standard endpoint")
        fallback_resp = await httpx.AsyncClient(timeout=120).post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
            json={"text": text, "model_id": tts_model,
                  "voice_settings": {"stability": vs.get("stability", 0.5),
                                     "similarity_boost": vs.get("similarity_boost", 0.75),
                                     "style": vs.get("style", 0.3)}},
        )
        fallback_resp.raise_for_status()
        with open(output_path, "wb") as f:
            f.write(fallback_resp.content)
        return {"audio_path": output_path, "word_timings": []}

    word_timings = _extract_word_timings(text, data.get("alignment", {}))
    log.info(f"Voiceover generated with {len(word_timings)} word timings: {output_path}")
    return {"audio_path": output_path, "word_timings": word_timings}


TEMPLATE_SFX_STYLES = {
    "skeleton": "dark eerie ambient drone, subtle horror atmosphere",
    "scary": "creepy horror atmosphere, tension building drone",
    "objects": "mysterious discovery sound, wonder ambient",
    "wouldyourather": "dramatic suspense, game show tension",
    "history": "epic cinematic atmosphere, dramatic orchestra hint",
}


async def generate_scene_sfx(visual_description: str, duration_sec: float,
                              output_path: str, template: str = "") -> str:
    """Generate a sound effect for a scene using ElevenLabs Sound Effects API.
    Returns the path to the generated SFX audio file, or empty string on failure."""
    if not ELEVENLABS_API_KEY:
        return ""

    style_hint = TEMPLATE_SFX_STYLES.get(template, "cinematic ambient atmosphere")
    sfx_prompt = f"{style_hint}, matching visual: {visual_description[:200]}"

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.elevenlabs.io/v1/sound-generation",
                headers={
                    "xi-api-key": ELEVENLABS_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "text": sfx_prompt,
                    "duration_seconds": min(duration_sec, 22.0),
                    "prompt_influence": 0.4,
                },
            )
            if resp.status_code != 200:
                log.warning(f"ElevenLabs SFX failed ({resp.status_code}): {resp.text[:200]}")
                return ""
            with open(output_path, "wb") as f:
                f.write(resp.content)
            if Path(output_path).stat().st_size > 0:
                log.info(f"SFX generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
                return output_path
            return ""
    except Exception as e:
        log.warning(f"SFX generation failed (non-fatal): {e}")
        return ""


def _probe_audio_duration_seconds(audio_path: str) -> float:
    """Best-effort audio duration probe using ffprobe."""
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                audio_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return float((proc.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _build_atempo_filter_chain(speed: float) -> str:
    """Build an ffmpeg atempo chain for any positive speed ratio."""
    if speed <= 0:
        return "atempo=1.0"
    parts = []
    remaining = float(speed)
    while remaining > 2.0:
        parts.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        parts.append("atempo=0.5")
        remaining /= 0.5
    parts.append(f"atempo={remaining:.6f}")
    return ",".join(parts)


async def _quintuple_check_scene_sfx(
    scenes: list,
    sfx_paths: list[str],
    template: str,
    job_id: str = "",
) -> list[str]:
    """Quintuple-check scene SFX alignment and retry mismatched clips once."""
    fixed = list(sfx_paths or [])
    while len(fixed) < len(scenes):
        fixed.append("")

    for i, scene in enumerate(scenes):
        expected = float(scene.get("duration_sec", 5) or 5)
        sfx = fixed[i] if i < len(fixed) else ""
        ok_exists = bool(sfx and Path(sfx).exists() and Path(sfx).stat().st_size > 0)
        actual = _probe_audio_duration_seconds(sfx) if ok_exists else 0.0
        ok_duration = ok_exists and abs(actual - expected) <= 1.5
        ok_order = i < len(fixed)
        ok_scene = bool(scene.get("visual_description", "").strip())
        ok_nonempty_prompt = ok_scene

        if ok_exists and ok_duration and ok_order and ok_scene and ok_nonempty_prompt:
            continue

        retry_out = str(TEMP_DIR / (f"{job_id}_sfx_retry_{i}.mp3" if job_id else f"sfx_retry_{i}_{int(time.time()*1000)}.mp3"))
        desc = scene.get("visual_description", "")
        retry = await generate_scene_sfx(desc, expected, retry_out, template=template)
        retry_ok = bool(retry and Path(retry).exists() and Path(retry).stat().st_size > 0)
        retry_dur = _probe_audio_duration_seconds(retry) if retry_ok else 0.0
        if retry_ok and abs(retry_dur - expected) <= 1.5:
            fixed[i] = retry
            log.info(f"[{job_id}] SFX scene {i+1} realigned on retry ({retry_dur:.2f}s vs expected {expected:.2f}s)")
        else:
            fixed[i] = ""
            log.warning(f"[{job_id}] SFX scene {i+1} failed alignment checks; using silence pad")

    return fixed


def _extract_word_timings(original_text: str, alignment: dict) -> list:
    """Convert ElevenLabs character-level alignment into word-level timings."""
    chars = alignment.get("characters", [])
    char_starts = alignment.get("character_start_times_seconds", [])
    char_ends = alignment.get("character_end_times_seconds", [])

    if not chars or not char_starts or not char_ends:
        return []
    if len(chars) != len(char_starts) or len(chars) != len(char_ends):
        return []

    words = []
    current_word = ""
    word_start = None

    for i, ch in enumerate(chars):
        if ch in (" ", "\n", "\t"):
            if current_word:
                words.append({
                    "word": current_word,
                    "start": word_start,
                    "end": char_ends[i - 1] if i > 0 else char_starts[i],
                })
                current_word = ""
                word_start = None
        else:
            if word_start is None:
                word_start = char_starts[i]
            current_word += ch

    if current_word and word_start is not None:
        words.append({
            "word": current_word,
            "start": word_start,
            "end": char_ends[-1],
        })

    return words


def generate_ass_subtitles(word_timings: list, output_path: str, resolution: str = "720p",
                           video_width: int = 0, video_height: int = 0, template: str = "") -> str:
    """Generate an ASS subtitle file with rapid single-word captions.
    Each word appears individually, large and bold, changing rapidly with every spoken word.
    High-retention viral TikTok/Reels style -- one word at a time, rapid fire.
    Supports both portrait (shorts) and landscape (product demo) layouts.
    """
    if video_width and video_height:
        res_w = video_width
        res_h = video_height
        is_landscape = res_w > res_h
    else:
        res_w = 1080 if resolution == "1080p" else 720
        res_h = 1920 if resolution == "1080p" else 1280
        is_landscape = False

    skeleton_pro_style = (template == "skeleton" and not is_landscape)

    if skeleton_pro_style:
        font_size = 84 if resolution == "1080p" else 60
        outline = 3 if resolution == "1080p" else 2
        shadow = 2
        margin_v = int(res_h * 0.14)
        spacing = 0
        scale_xy = 100
        primary = "&H00FFFFFF"
        secondary = "&H00E7F4FF"
        outline_color = "&H00303030"
        back_color = "&H70000000"
    elif is_landscape:
        font_size = max(36, int(res_h * 0.045))
        outline = 3
        shadow = 1
        margin_v = int(res_h * 0.08)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"
    else:
        font_size = 72 if resolution == "1080p" else 52
        outline = 5 if resolution == "1080p" else 4
        shadow = 2
        margin_v = int(res_h * 0.25)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"

    header = f"""[Script Info]
Title: NYPTID Captions
ScriptType: v4.00+
PlayResX: {res_w}
PlayResY: {res_h}
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Word,Noto Sans,{font_size},{primary},{secondary},{outline_color},{back_color},-1,0,0,0,{scale_xy},{scale_xy},{spacing},0,1,{outline},{shadow},2,20,20,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    def ts_to_ass(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        cs = int((seconds % 1) * 100)
        return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

    # Landscape demos often have faster narration pacing than shorts.
    # Use shorter per-word minimum display to keep captions visually in sync.
    MIN_DISPLAY = 0.12 if is_landscape else 0.25

    timed = []
    for wt in word_timings:
        word = wt["word"].strip()
        if not word:
            continue
        timed.append({"word": word, "start": wt["start"], "end": wt["end"]})

    events = []
    for i, wt in enumerate(timed):
        start = wt["start"]
        natural_end = wt["end"]
        next_start = timed[i + 1]["start"] if i + 1 < len(timed) else natural_end + 0.5
        end = max(natural_end, start + MIN_DISPLAY)
        # Prevent overlap with the next word so captions don't visually trail speech.
        max_end = (next_start - 0.01) if (i + 1 < len(timed)) else (natural_end + 0.5)
        end = min(end, max_end)
        # If speech is very fast, allow shorter windows rather than forcing laggy overlap.
        if end <= start:
            end = start + 0.04

        # Preserve natural casing for skeleton "editorial" look, keep uppercase for other templates.
        clean_word = wt["word"].replace("\\", "").replace("{", "").replace("}", "")
        safe_word = clean_word if skeleton_pro_style else clean_word.upper()

        if skeleton_pro_style:
            # Subtle pop/fade reads closer to hand-edited NLE captions.
            pop_in = r"{\blur0.6\fad(35,45)\fscx100\fscy100\t(0,90,\fscx104\fscy104)\t(90,170,\fscx100\fscy100)}"
        else:
            pop_in = r"{\fscx130\fscy130\t(0,60,\fscx105\fscy105)}"
        events.append(
            f"Dialogue: 0,{ts_to_ass(start)},{ts_to_ass(end)},Word,,0,0,0,,{pop_in}{safe_word}"
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(events))
        f.write("\n")

    log.info(f"ASS subtitle file generated: {output_path} ({len(events)} single-word captions)")
    return output_path


# ─── ComfyUI Image Generation with Upscaling ─────────────────────────────────

SKELETON_IMAGE_PROMPT_PREFIX = ""

SKELETON_IMAGE_STYLE_PREFIX = (
    "Photorealistic 3D studio render. Unreal Engine 5 quality. "
    "No illustration, no comic art, no anime, no drawing, no sketch."
)

SKELETON_MASTER_CONSISTENCY_PROMPT = (
    "MASTER CONSISTENCY RULES (apply to every scene): "
    "Keep one continuous visual universe across all scenes. Keep the same skeleton character identity, "
    "same skull shape, same limb proportions, same bone material, same eye style, same color grade, and same camera language. "
    "For VS videos, lock two identities and keep both stable scene-to-scene: Driver A = Formula 1, Driver B = Super Formula. "
    "Never swap identities. Never change art style. Never switch to illustration/comic/anime. "
    "Maintain photoreal cinematic studio quality in every frame. "
    "Outfits must remain role-accurate and fully opaque with realistic fabric folds and stitching. "
    "If a visual detail is missing, infer from topic role while preserving the same identity lock."
)

SKELETON_IMAGE_SUFFIX = (
    "Photorealistic 3D render, Unreal Engine 5, octane render, NOT illustration, NOT cartoon, NOT comic art. "
    "The character has a white SKULL for a head (not a human face) and BONY SKELETON HANDS, "
    "but the entire body from neck to feet is FULLY COVERED by the outfit described above. "
    "No bare ribcage, no exposed spine, no visible pelvis -- the clothes hide all bones below the neck. "
    "It looks like a real person in the outfit but with a clean glossy white bone skull instead of a face. "
    "NOT a real human. NOT a person with skin. The head MUST be a bare white bone skull with eyeballs. "
    "Solid clean teal-blue (#5AC8B8) studio backdrop, professional studio photography lighting."
)

TEMPLATE_KLING_MOTION = {
    "skeleton": "Ultra-smooth human-like natural motion: skeleton moves with realistic weight and momentum like a real person, fluid arm gestures, natural head turns with follow-through, subtle breathing chest rise-and-fall. Every joint articulates smoothly with no popping or snapping. Fingers move individually with lifelike dexterity. Eyeballs track and shift naturally with micro-saccades. Clothing sways and folds realistically with body movement showing fabric physics. Camera holds steady with very slight cinematic push-in. Professional studio lighting stays consistent. Zach D Films quality smooth cinematic motion, absolutely no robotic or jerky movement.",
    "history": "Epic cinematic camera movement: slow dolly forward through the scene, atmospheric particles drift, fabric and hair move in wind, fire flickers, dramatic lighting shifts. Film-quality motion with depth.",
    "story": "Emotional character animation: subtle facial expressions, natural body language, characters interact with environment. Cinematic camera slowly orbits or pushes in. Atmospheric lighting shifts to match mood.",
    "reddit": "Static with subtle motion: slight camera drift, ambient lighting changes, minimal character movement. Clean modern look.",
    "top5": "Dynamic reveal animation: dramatic camera push-in or orbit around subject, volumetric light beams shift, subject has powerful presence with minimal movement. Epic cinematic energy.",
    "random": "Chaotic energy: rapid unexpected motion, surreal physics, things morph and transform, wild camera movement. Maximum visual impact.",
    "roblox": "Roblox gameplay motion: character running forward on treadmill or obstacle course, smooth third-person camera follow, bouncy colorful environment, game-like movement.",
    "objects": "Subtle product photography motion: slow orbit around the object, gentle lighting shifts, slight zoom in, the object appears to breathe or pulse with personality. Smooth cinematic.",
    "split": "Split screen reveal: camera slowly pans across both sides showing the contrast, smooth transition between comparison elements, dramatic lighting shifts.",
    "twitter": "Modern motion graphics: smooth text animations, subtle camera drift, satisfying background footage with gentle movement, clean transitions.",
    "quiz": "Game show energy: dramatic zoom into answer reveal, spotlight movements, slight camera shake on reveals, bold color transitions between questions.",
    "argument": "Debate intensity: camera cuts between two sides, slight shake during heated moments, dramatic lighting shifts, confrontational energy building.",
    "wouldyourather": "Choice reveal: split screen animation revealing both options, dramatic pause before statistics, smooth transitions between dilemmas, building tension.",
    "scary": "Horror atmosphere: extremely slow camera drift through dark environments, subtle movements in shadows, flickering lights, creeping dread. Almost imperceptible motion that builds unease.",
    "motivation": "Epic cinematic: slow-motion camera sweep across landscape, golden light shifts, silhouette figure in the distance, wind and weather movement, inspirational energy.",
    "whatif": "Scientific visualization: transformation from normal to hypothetical, dramatic scale changes, time-lapse effects, before-and-after morphing, epic camera pullback to show scale.",
}

TEMPLATE_SFX_PROMPTS = {
    "skeleton": "Dark cinematic bass impact hit with eerie bone crack, dramatic low-end whoosh, horror tension riser",
    "history": "Epic orchestral low brass stinger with battle drums, cinematic war ambience, ancient world atmosphere",
    "story": "Emotional cinematic drone with subtle heartbeat, tension building string swell, dramatic mood shift",
    "reddit": "Clean modern UI notification transition whoosh, subtle digital ambience, social media pop",
    "top5": "Dramatic countdown reveal impact hit, deep bass drop, epic cinematic stinger with brass",
    "random": "Chaotic glitch sound effect with bass drop, surreal warping transition, energetic impact hit",
    "roblox": "Playful cartoon game sound effect, bouncy colorful pop, cheerful video game coin collect sound",
    "objects": "Smooth cinematic swoosh transition, elegant product reveal shimmer, satisfying mechanical click",
    "split": "Clean comparison split swoosh transition, dramatic side-by-side reveal impact, tension contrast hit",
    "twitter": "Modern social media notification whoosh, clean digital text pop transition, subtle tech ambience",
    "quiz": "Game show dramatic reveal stinger, suspenseful buzzer tension, audience gasps with anticipation",
    "argument": "Intense debate tension riser, dramatic confrontation bass hit, aggressive argument impact stinger",
    "wouldyourather": "Dramatic choice tension riser building to suspenseful reveal, decision point bass impact hit",
    "scary": "Deep horror drone with creaking door, eerie whisper ambience, jump scare tension riser stinger",
    "motivation": "Inspirational cinematic orchestra swell, uplifting epic brass rise, triumphant achievement stinger",
    "whatif": "Mind-bending sci-fi transition whoosh, reality warping bass drop, cosmic scale reveal impact",
}


async def generate_sfx_for_scene(scene_desc: str, template: str, duration_sec: float, output_path: str) -> str:
    """Generate a sound effect for a scene using ElevenLabs Sound Effects API."""
    if not ELEVENLABS_API_KEY:
        return ""
    base_sfx = TEMPLATE_SFX_PROMPTS.get(template, "Cinematic dramatic transition impact hit with bass")
    sfx_prompt = f"{base_sfx}. Scene: {scene_desc[:150]}"
    sfx_duration = min(max(duration_sec, 0.5), 22.0)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.elevenlabs.io/v1/sound-generation",
                headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                json={
                    "text": sfx_prompt,
                    "duration_seconds": sfx_duration,
                    "prompt_influence": 0.4,
                },
            )
            if resp.status_code not in (200, 201):
                log.warning(f"SFX generation failed ({resp.status_code}): {resp.text[:200]}")
                return ""
            with open(output_path, "wb") as f:
                f.write(resp.content)
        log.info(f"SFX generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
        return output_path
    except Exception as e:
        log.warning(f"SFX generation error (non-fatal): {e}")
        return ""


SKELETON_NEGATIVE_PROMPT = (
    "bare skeleton without clothes, naked skeleton, unclothed skeleton, skeleton with no outfit, "
    "anatomy model only, medical skeleton display, skeleton without accessories, "
    "cartoon, anime, low poly, plastic looking, toy, cute, chibi, "
    "skin, flesh, muscles, human face, realistic person, "
    "outdoor scene, room, environment, landscape, nature, buildings, "
    "dark background, black background, white background, "
    "blurry, low quality, watermark, text artifacts, deformed, "
    "bad anatomy, broken bones, dislocated joints, extra limbs, missing limbs, fused bones, "
    "transparent clothes, see-through clothes, x-ray clothes, invisible fabric, "
    "sheer material, translucent clothing, ghostly clothes, glass clothes, "
    "jpeg artifacts, pixelated, ugly, low resolution, "
    "glowing eyes, fire eyes, laser eyes, empty eye sockets, no eyes, hollow eyes, "
    "robotic motion, stiff pose, mannequin, puppet, jerky movement, unnatural pose"
)

HISTORY_IMAGE_PROMPT_PREFIX = (
    "Epic cinematic photorealistic historical scene, "
    "shot on ARRI Alexa with anamorphic lens, film grain, "
    "dramatic volumetric god rays and atmospheric haze, "
    "period-accurate costumes and architecture with ultra detailed textures, "
    "color graded like a Ridley Scott blockbuster, "
    "production design level of a $200M epic film, "
    "massive scale with armies or ruins or ancient cities, "
    "8k ultra HD, masterpiece quality, "
)

HISTORY_NEGATIVE_PROMPT = (
    "modern elements, cars, phones, electronics, contemporary clothing, "
    "cartoon, anime, low poly, plastic, toy, chibi, "
    "blurry, low quality, watermark, text, deformed, "
    "bad anatomy, jpeg artifacts, pixelated, ugly, "
    "bright cheerful lighting, flat lighting, studio background"
)

STORY_IMAGE_PROMPT_PREFIX = (
    "Cinematic masterpiece scene, Pixar quality 3D meets photorealistic cinematography, "
    "emotionally resonant composition with depth of field, "
    "dramatic volumetric lighting with motivated light sources, "
    "ray traced global illumination, atmospheric particles floating, "
    "lens flare, bokeh, film grain, color graded for emotional impact, "
    "character with consistent appearance centered in frame, "
    "richly detailed fantastical environment, 8k ultra HD, award-winning visual, "
)

STORY_NEGATIVE_PROMPT = (
    "cartoon, anime, low poly, flat shading, chibi, "
    "blurry, low quality, watermark, text artifacts, deformed, "
    "bad anatomy, jpeg artifacts, pixelated, ugly, "
    "multiple characters unless specified, inconsistent character design, "
    "flat lighting, boring composition, stock photo feel"
)

REDDIT_IMAGE_PROMPT_PREFIX = (
    "Photorealistic modern-day scene illustrating a dramatic life moment, "
    "cinematic photography with dramatic mood lighting, "
    "realistic person in contemporary clothing in a modern setting, "
    "emotional expression and body language visible, "
    "depth of field, warm or cool tones matching the mood, "
    "interior or urban environment with realistic details, "
    "8k ultra HD, photojournalism quality, "
)

REDDIT_NEGATIVE_PROMPT = (
    "cartoon, anime, 3D render, CGI look, fantasy, sci-fi, "
    "historical, period clothing, armor, medieval, "
    "blurry, low quality, watermark, deformed, "
    "bad anatomy, jpeg artifacts, pixelated, ugly, "
    "multiple people unless specified, skeleton, robot"
)

TOP5_IMAGE_PROMPT_PREFIX = (
    "Dramatic cinematic documentary photograph, "
    "hero-lit subject with bold chiaroscuro lighting, "
    "volumetric spotlight beams, deep shadows, "
    "rich color theme with intentional palette, "
    "the subject dominates the frame in a powerful pose or composition, "
    "anamorphic bokeh, film grain, depth of field, "
    "8k ultra HD, National Geographic meets movie poster quality, "
)

TOP5_NEGATIVE_PROMPT = (
    "cartoon, anime, low poly, chibi, cute, "
    "blurry, low quality, watermark, text, deformed, "
    "bad anatomy, jpeg artifacts, pixelated, ugly, "
    "flat lighting, boring composition, centered symmetrical, "
    "multiple unrelated subjects, cluttered background"
)

RANDOM_IMAGE_PROMPT_PREFIX = (
    "Hyper-detailed surreal digital art, vivid oversaturated colors, "
    "unexpected and absurd visual composition, "
    "extreme camera angle with dramatic perspective, "
    "mixing photorealistic and fantastical elements, "
    "bold neon lighting, chromatic aberration, glitch effects, "
    "trending on ArtStation, concept art masterpiece quality, "
    "8k ultra HD, maximum visual impact, "
)

RANDOM_NEGATIVE_PROMPT = (
    "boring, plain, simple, minimalist, subtle, "
    "blurry, low quality, watermark, deformed, "
    "bad anatomy, jpeg artifacts, pixelated, "
    "monochrome, grayscale, desaturated, muted colors"
)

ROBLOX_IMAGE_PROMPT_PREFIX = (
    "Roblox game screenshot, blocky character avatar running through colorful obstacle course, "
    "bright saturated colors, clean Roblox aesthetic, "
    "third-person view of character on treadmill or obby, "
    "cheerful lighting, game UI elements, "
)

ROBLOX_NEGATIVE_PROMPT = (
    "realistic human, photorealistic, dark horror, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, adult content"
)

OBJECTS_IMAGE_PROMPT_PREFIX = (
    "Photorealistic product photography of an everyday object, "
    "studio lighting with soft diffusion and subtle rim light, "
    "the object is the hero subject centered in frame, "
    "slightly anthropomorphized with personality, warm inviting tones, "
    "shallow depth of field, contextual background, "
    "Pixar-quality charm, 8k ultra HD, "
)

OBJECTS_NEGATIVE_PROMPT = (
    "cartoon, anime, sketch, clipart, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, dark, scary, "
    "multiple objects cluttered, messy background"
)

SPLIT_IMAGE_PROMPT_PREFIX = (
    "Cinematic split-screen comparison photograph, "
    "two contrasting scenes side by side with dramatic visual difference, "
    "strong color coding (warm vs cool), "
    "clean compositions that read well at half-width, "
    "photorealistic detail, dramatic lighting contrast, "
    "8k ultra HD, editorial quality, "
)

SPLIT_NEGATIVE_PROMPT = (
    "single scene, no contrast, boring, similar sides, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, flat lighting"
)

TWITTER_IMAGE_PROMPT_PREFIX = (
    "Modern clean digital aesthetic, dark mode color scheme, "
    "blues and whites on dark background, "
    "sleek typography, social media inspired visuals, "
    "satisfying or dramatic footage matching the topic, "
    "motion graphics feel, cinematic, "
    "8k ultra HD, contemporary design, "
)

TWITTER_NEGATIVE_PROMPT = (
    "old-fashioned, retro, historical, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, cluttered"
)

QUIZ_IMAGE_PROMPT_PREFIX = (
    "Bold vibrant game show aesthetic, "
    "bright colors with dark gradient background, "
    "large clean typography, dramatic lighting, "
    "spotlight effects, volumetric beams, "
    "themed visual matching the trivia topic, "
    "high energy presentation style, 8k, "
)

QUIZ_NEGATIVE_PROMPT = (
    "boring, plain, muted colors, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, dark, dreary"
)

ARGUMENT_IMAGE_PROMPT_PREFIX = (
    "Dramatic debate scene with two opposing sides, "
    "color-coded lighting (blue vs red), "
    "confrontational composition, split or face-to-face framing, "
    "cinematic tension, dramatic shadows, "
    "expressive characters or visual metaphors, "
    "8k ultra HD, documentary quality, "
)

ARGUMENT_NEGATIVE_PROMPT = (
    "peaceful, harmonious, agreement, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, flat lighting"
)

WYR_IMAGE_PROMPT_PREFIX = (
    "Dramatic split choice visual, two contrasting options, "
    "bold colors, cinematic lighting, "
    "each option looks equally compelling or terrifying, "
    "game show dramatic aesthetic, "
    "photorealistic scenarios, vivid detail, "
    "8k ultra HD, "
)

WYR_NEGATIVE_PROMPT = (
    "boring, plain, single option, no contrast, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly"
)

SCARY_IMAGE_PROMPT_PREFIX = (
    "Dark atmospheric horror cinematography, "
    "David Fincher color palette -- desaturated blues, greens, sickly yellows, "
    "shadows dominate 60% of the frame, "
    "abandoned environments, dark hallways, foggy landscapes, "
    "subtle wrongness in composition, things lurking in shadows, "
    "found-footage grain, film noir lighting, "
    "8k, dread-inducing atmosphere, "
)

SCARY_NEGATIVE_PROMPT = (
    "bright, cheerful, colorful, warm, sunny, "
    "cartoon, anime, cute, chibi, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, explicit gore"
)

MOTIVATION_IMAGE_PROMPT_PREFIX = (
    "Epic cinematic landscape photography, "
    "golden hour or dramatic weather (rain, fog, lightning), "
    "lone silhouette figure against vast dramatic backdrop, "
    "mountain peaks, ocean storms, city skylines, empty roads, "
    "warm golds and deep blues color grading, "
    "slow-motion texture quality, aspirational power, "
    "8k ultra HD, National Geographic meets movie quality, "
)

MOTIVATION_NEGATIVE_PROMPT = (
    "boring, flat, indoor, studio, "
    "cartoon, anime, chibi, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly, dark horror"
)

WHATIF_IMAGE_PROMPT_PREFIX = (
    "Photorealistic CGI scientific visualization, "
    "hypothetical scenario playing out at massive scale, "
    "before-and-after contrast, normal reality transforming, "
    "epic wide shots showing global-scale effects, "
    "dramatic color shifts indicating change, "
    "scientifically grounded yet visually spectacular, "
    "8k ultra HD, blockbuster VFX quality, "
)

WHATIF_NEGATIVE_PROMPT = (
    "boring, plain, small scale, mundane, "
    "cartoon, anime, chibi, "
    "blurry, low quality, watermark, deformed, "
    "jpeg artifacts, pixelated, ugly"
)

TEMPLATE_PROMPT_PREFIXES = {
    "skeleton": SKELETON_IMAGE_PROMPT_PREFIX,
    "history": HISTORY_IMAGE_PROMPT_PREFIX,
    "story": STORY_IMAGE_PROMPT_PREFIX,
    "reddit": REDDIT_IMAGE_PROMPT_PREFIX,
    "top5": TOP5_IMAGE_PROMPT_PREFIX,
    "random": RANDOM_IMAGE_PROMPT_PREFIX,
    "roblox": ROBLOX_IMAGE_PROMPT_PREFIX,
    "objects": OBJECTS_IMAGE_PROMPT_PREFIX,
    "split": SPLIT_IMAGE_PROMPT_PREFIX,
    "twitter": TWITTER_IMAGE_PROMPT_PREFIX,
    "quiz": QUIZ_IMAGE_PROMPT_PREFIX,
    "argument": ARGUMENT_IMAGE_PROMPT_PREFIX,
    "wouldyourather": WYR_IMAGE_PROMPT_PREFIX,
    "scary": SCARY_IMAGE_PROMPT_PREFIX,
    "motivation": MOTIVATION_IMAGE_PROMPT_PREFIX,
    "whatif": WHATIF_IMAGE_PROMPT_PREFIX,
}

TEMPLATE_NEGATIVE_PROMPTS = {
    "skeleton": SKELETON_NEGATIVE_PROMPT,
    "history": HISTORY_NEGATIVE_PROMPT,
    "story": STORY_NEGATIVE_PROMPT,
    "reddit": REDDIT_NEGATIVE_PROMPT,
    "top5": TOP5_NEGATIVE_PROMPT,
    "random": RANDOM_NEGATIVE_PROMPT,
    "roblox": ROBLOX_NEGATIVE_PROMPT,
    "objects": OBJECTS_NEGATIVE_PROMPT,
    "split": SPLIT_NEGATIVE_PROMPT,
    "twitter": TWITTER_NEGATIVE_PROMPT,
    "quiz": QUIZ_NEGATIVE_PROMPT,
    "argument": ARGUMENT_NEGATIVE_PROMPT,
    "wouldyourather": WYR_NEGATIVE_PROMPT,
    "scary": SCARY_NEGATIVE_PROMPT,
    "motivation": MOTIVATION_NEGATIVE_PROMPT,
    "whatif": WHATIF_NEGATIVE_PROMPT,
}

NEGATIVE_PROMPT = (
    "blurry, low quality, watermark, text artifacts, deformed, "
    "ugly, bad anatomy, bad proportions, duplicate, error, "
    "jpeg artifacts, low resolution, worst quality, lowres, "
    "oversaturated, undersaturated, noise, grain, pixelated"
)


WAN22_I2V_HIGH = "wan2.2_i2v_high_noise_14B_fp8_scaled.safetensors"
WAN22_I2V_LOW = "wan2.2_i2v_low_noise_14B_fp8_scaled.safetensors"


async def _run_comfyui_workflow(workflow: dict, output_node: str, output_type: str = "images") -> dict:
    """Submit a workflow to ComfyUI and wait for the specified output node to complete."""
    async with httpx.AsyncClient(timeout=900) as client:
        resp = await client.post(f"{COMFYUI_URL}/prompt", json={"prompt": workflow})
        if resp.status_code != 200:
            log.error(f"ComfyUI rejected workflow ({resp.status_code}): {resp.text[:1000]}")
        resp.raise_for_status()
        prompt_id = resp.json()["prompt_id"]

        for poll_i in range(450):
            await asyncio.sleep(2)
            history = await client.get(f"{COMFYUI_URL}/history/{prompt_id}")
            hist_data = history.json()
            if prompt_id in hist_data:
                outputs = hist_data[prompt_id].get("outputs", {})
                if output_node in outputs:
                    node_out = outputs[output_node]
                    if node_out.get(output_type):
                        return node_out
                    for key in ("videos", "gifs", "images"):
                        if node_out.get(key):
                            return node_out
                status = hist_data[prompt_id].get("status", {})
                if status.get("status_str") == "error":
                    raise RuntimeError(f"ComfyUI workflow error: {status.get('messages', 'unknown')}")
                if poll_i % 30 == 0 and poll_i > 0:
                    log.info(f"ComfyUI workflow still running... {poll_i * 2}s elapsed")
        raise TimeoutError("ComfyUI workflow timed out after 900s")


async def _download_comfyui_file(file_info: dict, output_path: str):
    """Download a generated file (image or video frame) from ComfyUI."""
    async with httpx.AsyncClient(timeout=120) as client:
        filename = file_info["filename"]
        subfolder = file_info.get("subfolder", "")
        ftype = file_info.get("type", "output")
        url = f"{COMFYUI_URL}/view?filename={filename}&subfolder={subfolder}&type={ftype}"
        resp = await client.get(url)
        with open(output_path, "wb") as f:
            f.write(resp.content)


GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"


_pending_training: dict[str, dict] = {}

async def _save_training_candidate(prompt: str, image_path: str, template: str = "", source: str = "grok", metadata: Optional[dict] = None) -> str:
    """Stage a prompt+image pair as a training candidate. Returns generation_id.
    Image is saved immediately but only promoted to 'accepted' via feedback."""
    gen_id = f"gen_{int(time.time() * 1000)}_{id(image_path) % 9999:04d}"
    try:
        img_dest = TRAINING_DATA_DIR / f"{gen_id}.png"
        txt_dest = TRAINING_DATA_DIR / f"{gen_id}.txt"
        shutil.copy2(image_path, str(img_dest))
        txt_dest.write_text(prompt, encoding="utf-8")
        _pending_training[gen_id] = {
            "prompt": prompt,
            "image_path": str(img_dest),
            "txt_path": str(txt_dest),
            "template": template,
            "source": source,
            "status": "pending",
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": metadata or {},
        }
        log.info(f"Training candidate staged: {gen_id} ({template or 'generic'}/{source})")
    except Exception as e:
        log.warning(f"Training candidate save failed (non-fatal): {e}")
    return gen_id


async def _sync_training_feedback_to_runpod(gen_id: str, entry: dict, status: str):
    """Sync accepted/rejected training examples to RunPod for continuous dataset growth."""
    if not RUNPOD_IMAGE_FEEDBACK_ENABLED:
        return
    image_path = entry.get("image_path", "")
    txt_path = entry.get("txt_path", "")
    if not image_path or not txt_path or not Path(image_path).exists() or not Path(txt_path).exists():
        return
    try:
        remote_root = RUNPOD_IMAGE_FEEDBACK_BASE_DIR.rstrip("/")
        remote_img_dir = f"{remote_root}/{status}/images"
        remote_txt_dir = f"{remote_root}/{status}/prompts"
        remote_meta_dir = f"{remote_root}/{status}/metadata"
        mkdir_cmd = (
            f"{RUNPOD_IMAGE_FEEDBACK_SSH} "
            f"'mkdir -p {remote_img_dir} {remote_txt_dir} {remote_meta_dir}'"
        )
        proc_mkdir = await asyncio.create_subprocess_shell(
            mkdir_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc_mkdir.communicate()
        if proc_mkdir.returncode != 0:
            log.warning(f"RunPod training mkdir failed for {gen_id}")
            return

        img_ext = Path(image_path).suffix.lower() or ".png"
        remote_img = f"{remote_img_dir}/{gen_id}{img_ext}"
        remote_txt = f"{remote_txt_dir}/{gen_id}.txt"
        scp_img_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{image_path}\" root@69.30.85.41:{remote_img}"
        scp_txt_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{txt_path}\" root@69.30.85.41:{remote_txt}"
        proc_img = await asyncio.create_subprocess_shell(
            scp_img_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, img_err = await proc_img.communicate()
        if proc_img.returncode != 0:
            log.warning(f"RunPod image sync failed for {gen_id}: {img_err.decode()[:200]}")
            return
        proc_txt = await asyncio.create_subprocess_shell(
            scp_txt_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, txt_err = await proc_txt.communicate()
        if proc_txt.returncode != 0:
            log.warning(f"RunPod prompt sync failed for {gen_id}: {txt_err.decode()[:200]}")
            return

        meta = {
            "generation_id": gen_id,
            "status": status,
            "template": entry.get("template", ""),
            "source": entry.get("source", ""),
            "created_at": entry.get("created_at", ""),
            "feedback_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": entry.get("metadata", {}),
        }
        meta_path = TEMP_DIR / f"{gen_id}_feedback.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=True), encoding="utf-8")
        try:
            remote_meta = f"{remote_meta_dir}/{gen_id}.json"
            scp_meta_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{str(meta_path)}\" root@69.30.85.41:{remote_meta}"
            proc_meta = await asyncio.create_subprocess_shell(
                scp_meta_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, meta_err = await proc_meta.communicate()
            if proc_meta.returncode != 0:
                log.warning(f"RunPod metadata sync failed for {gen_id}: {meta_err.decode()[:200]}")
        finally:
            meta_path.unlink(missing_ok=True)

        log.info(f"RunPod training sync complete: {gen_id} [{status}]")
    except Exception as e:
        log.warning(f"RunPod training feedback sync error for {gen_id}: {e}")


async def _mark_training_feedback(gen_id: str, accepted: bool, user_id: str = "", event: str = ""):
    """Mark a training candidate as accepted or rejected.
    Accepted pairs are logged to Supabase for LoRA training.
    Rejected pairs are cleaned up from disk."""
    entry = _pending_training.get(gen_id)
    if not entry:
        return
    status = "accepted" if accepted else "rejected"
    entry["status"] = status
    if user_id:
        entry.setdefault("metadata", {})["user_id"] = user_id
    if event:
        entry.setdefault("metadata", {})["event"] = event

    if not accepted:
        if entry.get("source") == "thumbnail_ai":
            try:
                img_path = entry.get("image_path", "")
                if img_path and Path(img_path).exists():
                    reject_dir = "/workspace/thumbnail_training/rejected"
                    ext = Path(img_path).suffix.lower() or ".png"
                    remote_path = f"{reject_dir}/{gen_id}{ext}"
                    ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, img_path, remote_path)
                    if not ok:
                        log.warning(f"Thumbnail reject sync file failed for {gen_id}: {err}")
            except Exception as e:
                log.warning(f"Thumbnail reject sync failed for {gen_id}: {e}")
        await _sync_training_feedback_to_runpod(gen_id, entry, status="rejected")
        for p in [entry.get("image_path"), entry.get("txt_path")]:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass
        _pending_training.pop(gen_id, None)
        log.info(f"Training candidate {gen_id} REJECTED and cleaned up")
        return

    if SUPABASE_URL and SUPABASE_ANON_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"{SUPABASE_URL}/rest/v1/training_data",
                    headers={
                        "apikey": SUPABASE_ANON_KEY,
                        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    },
                    json={
                        "prompt": entry["prompt"][:2000],
                        "image_filename": gen_id + ".png",
                        "template": entry.get("template", ""),
                        "source": entry.get("source", "grok"),
                        "status": "accepted",
                        "created_at": entry["created_at"],
                    },
                )
        except Exception as e:
            log.warning(f"Supabase training log failed (non-fatal): {e}")
    await _sync_training_feedback_to_runpod(gen_id, entry, status="accepted")
    if entry.get("source") == "thumbnail_ai":
        try:
            img_path = entry.get("image_path", "")
            if img_path and Path(img_path).exists():
                ext = Path(img_path).suffix.lower() or ".png"
                remote_path = f"{RUNPOD_TRAINING_DIR}/{gen_id}{ext}"
                ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, img_path, remote_path)
                if not ok:
                    log.warning(f"Thumbnail accept sync file failed for {gen_id}: {err}")
        except Exception as e:
            log.warning(f"Thumbnail accept sync failed for {gen_id}: {e}")
    log.info(f"Training candidate {gen_id} ACCEPTED -> training dataset")


def _file_to_data_image_url(image_path: str, max_bytes: int = 8 * 1024 * 1024) -> str:
    """Encode a local image file as a data URL for xAI image reference conditioning."""
    p = Path(image_path)
    if not p.exists() or p.stat().st_size == 0:
        return ""
    file_size = p.stat().st_size
    if file_size > max_bytes:
        log.warning(f"Skipping data URI encode for {p.name}: {file_size} bytes exceeds limit {max_bytes}")
        return ""
    ext = p.suffix.lower()
    mime = "image/png" if ext == ".png" else ("image/webp" if ext == ".webp" else "image/jpeg")
    b64 = base64.b64encode(p.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{b64}"


async def _generate_image_xai_direct(prompt: str, output_path: str, reference_image_url: str = "") -> dict:
    """Generate image directly via xAI API. No fal.ai needed.
    Returns {"local_path": str, "cdn_url": str}.
    """
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")

    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": XAI_IMAGE_MODEL,
        "prompt": prompt,
        "n": 1,
        "response_format": "url",
        "aspect_ratio": XAI_IMAGE_ASPECT_RATIO,
        "resolution": XAI_IMAGE_RESOLUTION,
    }
    if reference_image_url:
        payload["image_url"] = reference_image_url

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post("https://api.x.ai/v1/images/generations", headers=headers, json=payload)
                if resp.status_code in (200, 201):
                    data = resp.json().get("data", [])
                    if data and data[0].get("url"):
                        cdn_url = data[0]["url"]
                        dl = await client.get(cdn_url, follow_redirects=True)
                        if dl.status_code == 200:
                            with open(output_path, "wb") as f:
                                f.write(dl.content)
                            log.info(f"xAI direct image saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
                            gen_id = await _save_training_candidate(prompt, output_path, source="xai_direct")
                            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = (attempt + 1) * 5
                    log.warning(f"xAI image gen attempt {attempt+1} got {resp.status_code}, retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                # If reference-conditioning payload is rejected by provider, retry once without reference.
                if reference_image_url and resp.status_code in (400, 404, 422):
                    payload.pop("image_url", None)
                    reference_image_url = ""
                    log.warning("xAI image reference payload rejected; retrying without reference image")
                    continue
                raise RuntimeError(f"xAI image gen failed ({resp.status_code}): {resp.text[:200]}")
        except RuntimeError:
            raise
        except Exception as e:
            log.warning(f"xAI image gen attempt {attempt+1} error: {e}")
            await asyncio.sleep((attempt + 1) * 3)

    raise RuntimeError("xAI direct image generation failed after retries")


async def generate_image_grok(prompt: str, output_path: str, resolution: str = "720p", reference_image_url: str = "") -> dict:
    """Generate an image using Grok Imagine. Tries fal.ai first, falls back to direct xAI API.
    Returns {"local_path": str, "cdn_url": str} so Kling can use the URL directly.
    """
    if USE_FAL_GROK_IMAGE and FAL_AI_KEY:
        try:
            aspect = "9:16"
            headers = {
                "Authorization": "Key " + FAL_AI_KEY,
                "Content-Type": "application/json",
            }
            payload = {
                "prompt": prompt,
                "num_images": 1,
                "aspect_ratio": aspect,
                "output_format": "png",
            }
            if reference_image_url:
                payload["image_url"] = reference_image_url

            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(GROK_IMAGINE_URL, headers=headers, json=payload)
                if resp.status_code not in (200, 201):
                    raise RuntimeError("Grok Imagine via fal.ai failed (" + str(resp.status_code) + "): " + resp.text[:300])
                data = resp.json()

            images = data.get("images", [])
            if not images:
                raise RuntimeError("Grok Imagine returned no images")

            cdn_url = images[0].get("url", "")
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                img_resp = await client.get(cdn_url)
                if img_resp.status_code != 200:
                    raise RuntimeError("Failed to download Grok image")
                with open(output_path, "wb") as f:
                    f.write(img_resp.content)

            log.info(f"Grok Imagine (fal.ai) saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
            gen_id = await _save_training_candidate(prompt, output_path, source="grok_imagine")
            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
        except Exception as e:
            log.warning(f"Fal.ai Grok Imagine failed, falling back to direct xAI: {e}")

    log.info(f"Using direct xAI API image generation model={XAI_IMAGE_MODEL}")
    return await _generate_image_xai_direct(prompt, output_path, reference_image_url=reference_image_url)


SKELETON_LORA_NAME = "nyptid_skeleton_v1.safetensors"
SKELETON_LORA_STRENGTH = 0.85
SKELETON_TRIGGER_TOKEN = "nyptid_skeleton"
SKELETON_LORA_NEGATIVE = "blurry, low quality, text, watermark, deformed, ugly, bad anatomy, non-skeleton, human skin, flesh, muscles, realistic human, cartoon, anime, painting, 2D, illustration, transparent clothes, see-through clothes, x-ray clothes, invisible fabric, naked skeleton, broken bones, dislocated joints, extra limbs, missing limbs, empty eye sockets, no eyes, hollow eyes, robotic motion, stiff pose, jerky movement"


async def check_skeleton_lora_available() -> bool:
    """Check if the skeleton LoRA exists on the ComfyUI server."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{COMFYUI_URL}/object_info/LoraLoader")
            if resp.status_code == 200:
                data = resp.json()
                lora_list = data.get("LoraLoader", {}).get("input", {}).get("required", {}).get("lora_name", [[]])[0]
                return SKELETON_LORA_NAME in lora_list
    except Exception:
        pass
    return False


async def generate_image_skeleton_lora(prompt: str, output_path: str, resolution: str = "720p") -> str:
    """Generate skeleton image using fine-tuned LoRA on ComfyUI SDXL."""
    config = RESOLUTION_CONFIGS[resolution]
    lora_prompt = f"{SKELETON_TRIGGER_TOKEN}, {prompt}"

    workflow = {
        "4": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
        },
        "10": {
            "class_type": "LoraLoader",
            "inputs": {
                "lora_name": SKELETON_LORA_NAME,
                "strength_model": SKELETON_LORA_STRENGTH,
                "strength_clip": SKELETON_LORA_STRENGTH,
                "model": ["4", 0],
                "clip": ["4", 1],
            },
        },
        "5": {
            "class_type": "EmptyLatentImage",
            "inputs": {"width": config["gen_width"], "height": config["gen_height"], "batch_size": 1},
        },
        "6": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": lora_prompt, "clip": ["10", 1]},
        },
        "7": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": SKELETON_LORA_NEGATIVE, "clip": ["10", 1]},
        },
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 35,
                "cfg": 7.0,
                "sampler_name": "dpmpp_2m",
                "scheduler": "karras",
                "denoise": 1.0,
                "model": ["10", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["5", 0],
            },
        },
        "8": {
            "class_type": "VAEDecode",
            "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
        },
        "9": {
            "class_type": "SaveImage",
            "inputs": {"filename_prefix": "nyptid_skeleton_lora", "images": ["8", 0]},
        },
    }

    result = await _run_comfyui_workflow(workflow, "9", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    log.info(f"Skeleton LoRA image generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
    return output_path


async def generate_scene_image(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    template: str = "",
    reference_image_url: str = "",
) -> dict:
    """Generate a scene image. Priority for skeleton template: LoRA > Grok Imagine > SDXL.
    For other templates: Grok Imagine > SDXL.
    Returns {"local_path": str, "cdn_url": str | None}.
    """
    if template == "skeleton":
        try:
            lora_available = await check_skeleton_lora_available()
            if lora_available:
                await generate_image_skeleton_lora(prompt, output_path, resolution=resolution)
                log.info("Skeleton image generated via LoRA (zero API cost)")
                return {"local_path": output_path, "cdn_url": None}
        except Exception as e:
            log.warning(f"Skeleton LoRA generation failed, falling back to Grok Imagine: {e}")

    if FAL_AI_KEY or XAI_API_KEY:
        try:
            return await generate_image_grok(
                prompt,
                output_path,
                resolution=resolution,
                reference_image_url=reference_image_url,
            )
        except Exception as e:
            log.warning(f"Grok image generation failed (fal.ai + xAI direct), falling back to SDXL: {e}")

    await generate_image_comfyui(prompt, output_path, resolution=resolution, negative_prompt=negative_prompt)
    return {"local_path": output_path, "cdn_url": None}


async def generate_image_comfyui(prompt: str, output_path: str, resolution: str = "720p", negative_prompt: str = "") -> str:
    """Fallback: generate image via ComfyUI SDXL on RunPod."""
    config = RESOLUTION_CONFIGS[resolution]
    neg = negative_prompt or NEGATIVE_PROMPT

    workflow = {
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 30,
                "cfg": 7.5,
                "sampler_name": "dpmpp_2m",
                "scheduler": "karras",
                "denoise": 1.0,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["5", 0],
            },
        },
        "4": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
        },
        "5": {
            "class_type": "EmptyLatentImage",
            "inputs": {"width": config["gen_width"], "height": config["gen_height"], "batch_size": 1},
        },
        "6": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": prompt, "clip": ["4", 1]},
        },
        "7": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": neg, "clip": ["4", 1]},
        },
        "8": {
            "class_type": "VAEDecode",
            "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
        },
        "9": {
            "class_type": "SaveImage",
            "inputs": {"filename_prefix": "nyptid_gen", "images": ["8", 0]},
        },
    }

    if config.get("upscale"):
        workflow["10"] = {
            "class_type": "LatentUpscaleBy",
            "inputs": {
                "samples": ["3", 0],
                "scale_by": config["upscale_factor"],
                "upscale_method": "bislerp",
            },
        }
        workflow["11"] = {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 15,
                "cfg": 7.0,
                "sampler_name": "dpmpp_2m",
                "scheduler": "karras",
                "denoise": 0.4,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["10", 0],
            },
        }
        workflow["8"]["inputs"]["samples"] = ["11", 0]

    result = await _run_comfyui_workflow(workflow, "9", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def check_wan22_available() -> bool:
    """Check if the Wan 2.2 I2V models exist on the ComfyUI server."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{COMFYUI_URL}/object_info")
            if resp.status_code == 200:
                content = resp.text
                return WAN22_I2V_HIGH.split(".")[0] in content or "wan2.2" in content.lower()
    except Exception as e:
        log.warning(f"Wan 2.2 availability check failed: {e}")
    return False


RUNPOD_SSH_HOST = "root@69.30.85.41"
RUNPOD_SSH_PORT = "22118"
COMFYUI_INPUT_DIR = "/workspace/ComfyUI/input"

FAL_SUBMIT_URL = "https://queue.fal.run/fal-ai/kling-video/v2.1/standard/image-to-video"
FAL_STATUS_URL = "https://queue.fal.run/fal-ai/kling-video/v2.1/standard/image-to-video/requests"
FAL_UPLOAD_URL = "https://fal.run/fal-ai/fal-file-storage/upload"


async def _upload_image_to_fal(image_path: str) -> str:
    """Upload a local image to fal.ai CDN and return a public URL for it."""
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    headers = {"Authorization": "Key " + FAL_AI_KEY}
    img_bytes = Path(image_path).read_bytes()
    filename = "nyptid_" + str(int(time.time() * 1000)) + ".png"
    upload_url = "https://fal.ai/api/storage/upload/initiate"

    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(
            upload_url,
            headers={**headers, "Accept": "application/json", "Content-Type": "application/json"},
            json={"file_name": filename, "content_type": "image/png"},
        )
        if resp.status_code == 200:
            upload_info = resp.json()
            presigned = upload_info.get("upload_url") or upload_info.get("presigned_url")
            file_url = upload_info.get("file_url")
            if presigned and file_url:
                put_resp = await client.put(presigned, content=img_bytes, headers={"Content-Type": "image/png"})
                if put_resp.status_code in (200, 201):
                    log.info(f"Image uploaded to fal.ai CDN: {file_url[:80]}")
                    return file_url

        target_path = "uploads/" + filename
        rest_url = "https://api.fal.ai/v1/serverless/files/file/local/" + target_path
        import io
        files = {"file_upload": (filename, io.BytesIO(img_bytes), "image/png")}
        resp2 = await client.post(rest_url, headers=headers, files=files)
        if resp2.status_code in (200, 201):
            cdn_url = "https://api.fal.ai/v1/serverless/files/file/" + target_path
            log.info(f"Image uploaded to fal.ai REST: {cdn_url}")
            return cdn_url

    import base64
    log.warning("fal.ai CDN upload failed, using data URL fallback")
    b64 = base64.b64encode(img_bytes).decode()
    return "data:image/png;base64," + b64


async def animate_image_kling(image_path: str, prompt: str, output_clip_path: str, duration: str = "5", aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Use fal.ai Kling 2.1 Standard I2V to animate an image into a video clip.
    If image_cdn_url is provided (from Grok Imagine), skip the upload step.
    Returns the local path to the downloaded MP4 clip.
    """
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")

    if image_cdn_url:
        image_url = image_cdn_url
        log.info("Kling I2V: using existing CDN URL (from Grok Imagine)")
    else:
        image_url = await _upload_image_to_fal(image_path)

    log.info(f"Kling I2V: submitting job (duration={duration}s, ar={aspect_ratio})")

    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "image_url": image_url,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "negative_prompt": "blur, distort, low quality, watermark, text overlay, UI elements",
        "cfg_scale": 0.5,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(FAL_SUBMIT_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("Kling submit failed (" + str(resp.status_code) + "): " + resp.text[:300])
        submit_data = resp.json()

    request_id = submit_data.get("request_id")
    if not request_id:
        if submit_data.get("video", {}).get("url"):
            video_url = submit_data["video"]["url"]
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        raise RuntimeError("No request_id from Kling submit: " + json.dumps(submit_data)[:300])

    log.info(f"Kling I2V queued: request_id={request_id}")
    status_url = submit_data.get("status_url", FAL_STATUS_URL + "/" + request_id + "/status")
    result_url = submit_data.get("response_url", FAL_STATUS_URL + "/" + request_id)

    max_wait = 600
    poll_interval = 5
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        async with httpx.AsyncClient(timeout=30) as client:
            st_resp = await client.get(status_url, headers={"Authorization": "Key " + FAL_AI_KEY})
            if st_resp.status_code == 202:
                st_data = st_resp.json()
                status = st_data.get("status", "IN_PROGRESS")
                if elapsed % 30 == 0:
                    log.info(f"Kling I2V waiting... {elapsed}s elapsed, status={status}")
                continue
            if st_resp.status_code != 200:
                log.warning(f"Kling status poll HTTP {st_resp.status_code}: {st_resp.text[:200]}")
                continue
            st_data = st_resp.json()
            status = st_data.get("status", "")
            if status == "COMPLETED":
                break
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError("Kling generation failed: " + json.dumps(st_data)[:300])
            if elapsed % 30 == 0:
                log.info(f"Kling I2V waiting... {elapsed}s elapsed, status={status}")
        if poll_interval < 15:
            poll_interval = min(poll_interval + 2, 15)
    else:
        raise TimeoutError("Kling I2V timed out after " + str(max_wait) + "s")

    async with httpx.AsyncClient(timeout=60) as client:
        res_resp = await client.get(result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
        if res_resp.status_code != 200:
            raise RuntimeError("Kling result fetch failed: " + str(res_resp.status_code))
        result_data = res_resp.json()

    video_url = result_data.get("video", {}).get("url")
    if not video_url:
        raise RuntimeError("No video URL in Kling result: " + json.dumps(result_data)[:300])

    log.info(f"Kling I2V complete, downloading video from {video_url[:80]}...")
    await _download_url_to_file(video_url, output_clip_path)
    log.info(f"Kling clip saved: {output_clip_path} ({Path(output_clip_path).stat().st_size / 1024:.0f} KB)")
    return output_clip_path


async def _download_url_to_file(url: str, output_path: str):
    """Download a file from a URL to a local path using streaming writes."""
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        async with client.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise RuntimeError("Download failed (" + str(resp.status_code) + ") from " + url[:100])
            with open(output_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)


async def animate_image_grok_video(image_path: str, prompt: str, output_clip_path: str, duration_sec: float = 5, aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Animate an image via xAI Grok Imagine Video and download resulting MP4."""
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")
    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    duration = max(1, min(int(round(float(duration_sec))), 15))
    image_url = image_cdn_url or _file_to_data_image_url(image_path)
    if not image_url:
        raise RuntimeError("No source image URL for Grok video")

    payload = {
        "model": XAI_VIDEO_MODEL,
        "prompt": prompt,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "resolution": "720p",
        "image": {"url": image_url},
    }
    async with httpx.AsyncClient(timeout=60) as client:
        submit = await client.post("https://api.x.ai/v1/videos/generations", headers=headers, json=payload)
        if submit.status_code not in (200, 201):
            raise RuntimeError(f"Grok video submit failed ({submit.status_code}): {submit.text[:300]}")
        submit_data = submit.json()
    request_id = submit_data.get("request_id")
    if not request_id:
        raise RuntimeError("Grok video submit returned no request_id")

    poll_url = f"https://api.x.ai/v1/videos/{request_id}"
    max_wait = 900
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(4)
        elapsed += 4
        async with httpx.AsyncClient(timeout=30) as client:
            status_resp = await client.get(poll_url, headers={"Authorization": f"Bearer {XAI_API_KEY}"})
            if status_resp.status_code != 200:
                continue
            status_data = status_resp.json()
        status = str(status_data.get("status", "")).lower()
        if status == "done":
            video_url = status_data.get("video", {}).get("url")
            if not video_url:
                raise RuntimeError("Grok video done status missing video URL")
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        if status == "expired":
            raise RuntimeError("Grok video request expired")
    raise TimeoutError("Grok video timed out")


def _aspect_ratio_to_runway_ratio(aspect_ratio: str) -> str:
    if aspect_ratio == "9:16":
        return "720:1280"
    if aspect_ratio == "16:9":
        return "1280:720"
    allowed = {"1280:720", "720:1280", "1104:832", "960:960", "832:1104", "1584:672"}
    return aspect_ratio if aspect_ratio in allowed else "720:1280"


async def animate_image_runway_video(image_path: str, prompt: str, output_clip_path: str, duration_sec: float = 5, aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Animate an image via Runway image-to-video and download resulting MP4."""
    if not RUNWAY_API_KEY:
        raise RuntimeError("RUNWAY_API_KEY not configured")
    headers = {
        "Authorization": f"Bearer {RUNWAY_API_KEY}",
        "Content-Type": "application/json",
        "X-Runway-Version": RUNWAY_API_VERSION,
    }
    duration = max(2, min(int(round(float(duration_sec))), 10))
    image_url = image_cdn_url or _file_to_data_image_url(image_path, max_bytes=3_700_000)
    if not image_url:
        raise RuntimeError("No source image URL for Runway video (image too large for inline data URI; provide CDN URL)")

    payload = {
        "model": RUNWAY_VIDEO_MODEL,
        "promptText": prompt,
        "promptImage": image_url,
        "ratio": _aspect_ratio_to_runway_ratio(aspect_ratio),
        "duration": duration,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        submit = await client.post("https://api.dev.runwayml.com/v1/image_to_video", headers=headers, json=payload)
        if submit.status_code not in (200, 201):
            raise RuntimeError(f"Runway submit failed ({submit.status_code}): {submit.text[:300]}")
        submit_data = submit.json()
    task_id = submit_data.get("id")
    if not task_id:
        raise RuntimeError("Runway submit returned no task id")

    poll_url = f"https://api.dev.runwayml.com/v1/tasks/{task_id}"
    max_wait = 900
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(5)
        elapsed += 5
        async with httpx.AsyncClient(timeout=30) as client:
            status_resp = await client.get(poll_url, headers=headers)
            if status_resp.status_code != 200:
                continue
            status_data = status_resp.json()
        status = str(status_data.get("status", "")).upper()
        if status == "SUCCEEDED":
            output = status_data.get("output")
            video_url = None
            if isinstance(output, list) and output:
                first = output[0]
                if isinstance(first, str):
                    video_url = first
                elif isinstance(first, dict):
                    video_url = first.get("url") or first.get("uri")
            elif isinstance(output, dict):
                video_url = output.get("url") or output.get("uri") or output.get("video")
            if not video_url:
                raise RuntimeError("Runway done status missing output URL")
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError("Runway task failed: " + json.dumps(status_data)[:300])
    raise TimeoutError("Runway video timed out")


async def animate_scene(image_path: str, prompt: str, output_dir_path: str, scene_idx: int, job_ts: str, duration_sec: float = 5, num_frames: int = 81, image_cdn_url: str = None, prefer_wan: bool = False) -> dict:
    """Animate a scene image using Runway first, then Grok as fallback."""
    provider_errors = []

    if RUNWAY_API_KEY:
        runway_clip_path = str(Path(output_dir_path) / ("runway_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
        try:
            await animate_image_runway_video(
                image_path,
                prompt,
                runway_clip_path,
                duration_sec=duration_sec,
                aspect_ratio="9:16",
                image_cdn_url=image_cdn_url,
            )
            return {"type": "runway_clip", "path": runway_clip_path}
        except Exception as e:
            provider_errors.append("runway: " + str(e))
            log.warning(f"Runway scene animation failed, falling back to Grok: {e}")

    if USE_XAI_VIDEO and XAI_API_KEY:
        grok_clip_path = str(Path(output_dir_path) / ("grok_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
        try:
            await animate_image_grok_video(
                image_path,
                prompt,
                grok_clip_path,
                duration_sec=duration_sec,
                aspect_ratio="9:16",
                image_cdn_url=image_cdn_url,
            )
            return {"type": "grok_clip", "path": grok_clip_path}
        except Exception as e:
            provider_errors.append("grok: " + str(e))
            log.warning(f"Grok scene animation failed: {e}")

    if not provider_errors:
        raise RuntimeError("No video engine configured (set RUNWAY_API_KEY and/or XAI_API_KEY)")
    raise RuntimeError("All video providers failed: " + " | ".join(provider_errors))


async def _upload_image_to_comfyui(image_path: str) -> str:
    """Upload an image to ComfyUI's input directory via HTTP API."""
    filename = "nyptid_scene_" + str(int(time.time() * 1000)) + ".png"
    img_bytes = Path(image_path).read_bytes()
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{COMFYUI_URL}/upload/image",
            files={"image": (filename, img_bytes, "image/png")},
            data={"overwrite": "true"},
        )
        if resp.status_code != 200:
            raise RuntimeError(f"ComfyUI image upload failed ({resp.status_code}): {resp.text[:200]}")
        result = resp.json()
        uploaded_name = result.get("name", filename)
    log.info(f"Image uploaded to ComfyUI via HTTP: {uploaded_name}")
    return uploaded_name


async def animate_image_wan22(image_path: str, prompt: str, output_clip_path: str, num_frames: int = 81) -> str:
    """Animate an image via Wan 2.2 I2V on ComfyUI. Returns path to downloaded MP4."""
    uploaded_name = await _upload_image_to_comfyui(image_path)

    workflow = {
        "1": {
            "class_type": "UNETLoader",
            "inputs": {
                "unet_name": WAN22_I2V_HIGH,
                "weight_dtype": "fp8_e4m3fn",
            },
        },
        "3": {
            "class_type": "CLIPLoader",
            "inputs": {
                "clip_name": "umt5_xxl_fp8_e4m3fn_scaled.safetensors",
                "type": "wan",
            },
        },
        "4": {
            "class_type": "CLIPTextEncode",
            "inputs": {
                "text": prompt,
                "clip": ["3", 0],
            },
        },
        "5": {
            "class_type": "VAELoader",
            "inputs": {"vae_name": "wan2.2_vae.safetensors"},
        },
        "6": {
            "class_type": "CLIPVisionLoader",
            "inputs": {"clip_name": "clip_vision_h.safetensors"},
        },
        "6b": {
            "class_type": "CLIPVisionEncode",
            "inputs": {
                "clip_vision": ["6", 0],
                "image": ["7", 0],
                "crop": "center",
            },
        },
        "7": {
            "class_type": "LoadImage",
            "inputs": {"image": uploaded_name},
        },
        "8": {
            "class_type": "WanImageToVideo",
            "inputs": {
                "positive": ["4", 0],
                "negative": ["12", 0],
                "vae": ["5", 0],
                "width": 480,
                "height": 832,
                "length": num_frames,
                "batch_size": 1,
                "clip_vision_output": ["6b", 0],
                "start_image": ["7", 0],
            },
        },
        "11": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 30,
                "cfg": 3.0,
                "sampler_name": "uni_pc_bh2",
                "scheduler": "simple",
                "denoise": 1.0,
                "model": ["1", 0],
                "positive": ["8", 0],
                "negative": ["8", 2],
                "latent_image": ["8", 1],
            },
        },
        "12": {
            "class_type": "CLIPTextEncode",
            "inputs": {
                "text": "",
                "clip": ["3", 0],
            },
        },
        "13": {
            "class_type": "VAEDecode",
            "inputs": {
                "samples": ["11", 0],
                "vae": ["5", 0],
            },
        },
        "15": {
            "class_type": "CreateVideo",
            "inputs": {
                "images": ["13", 0],
                "fps": 16.0,
            },
        },
        "16": {
            "class_type": "SaveVideo",
            "inputs": {
                "video": ["15", 0],
                "filename_prefix": "nyptid_wan",
                "format": "mp4",
                "codec": "h264",
            },
        },
    }

    result = await _run_comfyui_workflow(workflow, "16", "videos")

    if not result.get("videos"):
        result = await _run_comfyui_workflow(workflow, "16", "gifs")
        if not result.get("gifs"):
            raise RuntimeError("Wan 2.2 produced no video output")
        vid_info = result["gifs"][0]
    else:
        vid_info = result["videos"][0]

    await _download_comfyui_file(vid_info, output_clip_path)
    file_size = Path(output_clip_path).stat().st_size
    log.info(f"Wan 2.2 video saved: {output_clip_path} ({file_size / 1024:.0f} KB)")
    return output_clip_path


# ─── FFmpeg Video Compositor ──────────────────────────────────────────────────

async def frames_to_clip(frame_paths: list, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Convert SVD frames into a video clip, stretched/looped to fill the scene duration."""
    frame_dir = Path(frame_paths[0]).parent
    num_frames = len(frame_paths)
    native_fps = 8
    native_duration = num_frames / native_fps

    drawtext = ""
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext = (
            ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )

    speed_factor = native_duration / duration if duration > 0 else 1.0
    speed_factor = max(0.25, min(speed_factor, 4.0))

    first_frame = Path(frame_paths[0]).name
    prefix = first_frame.rsplit("_", 1)[0]
    input_pattern = str(frame_dir / f"{prefix}_%04d.png")
    cmd = [
        "ffmpeg", "-y",
        "-framerate", str(native_fps),
        "-i", input_pattern,
        "-t", str(duration),
        "-vf", (
            f"setpts={1.0/speed_factor}*PTS,"
            f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,"
            f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black,"
            f"format=yuv420p"
            f"{drawtext}"
        ),
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        log.warning(f"SVD clip ffmpeg error: {stderr.decode()[:500]}")
        raise RuntimeError("Failed to create clip from SVD frames")
    return output_clip


def _ffmpeg_safe_text(text: str) -> str:
    """Escape text for FFmpeg drawtext filter."""
    import re
    t = re.sub(r"[^\w\s.,!?\-+=#&]", "", text)
    t = t.replace(":", "\\:").replace("'", "").replace("%", "")
    return t


async def static_image_to_clip(img_path: str, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Fallback: create a video clip from a static image with slow zoom."""
    base_vf = "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"

    drawtext_vf = base_vf
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext_vf = (
            base_vf
            + ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(img_path),
        "-t", str(duration),
        "-vf", drawtext_vf,
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0 and text_overlay:
        log.warning(f"Drawtext failed, retrying without text overlay: {stderr.decode()[-200:]}")
        cmd_plain = [
            "ffmpeg", "-y",
            "-loop", "1", "-i", str(img_path),
            "-t", str(duration),
            "-vf", base_vf,
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            "-r", "30",
            str(output_clip),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd_plain, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode()[-300:]
        log.error(f"FFmpeg static clip error: {err}")
        raise RuntimeError("FFmpeg failed on static image clip: " + err[-100:])
    return output_clip


async def kling_clip_to_scene(kling_clip: str, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Re-encode a Kling MP4 clip to exact output dimensions, trim/loop to duration, add text overlay."""
    drawtext = ""
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext = (
            ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )
    vf = (
        "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,"
        + "pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,"
        + "format=yuv420p"
        + drawtext
    )
    cmd = [
        "ffmpeg", "-y",
        "-i", str(kling_clip),
        "-t", str(duration),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 and text_overlay:
        log.warning(f"Kling clip drawtext failed, retrying without: {stderr.decode()[-200:]}")
        vf_plain = (
            "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,"
            + "pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,"
            + "format=yuv420p"
        )
        cmd[cmd.index("-vf") + 1] = vf_plain
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode()[-300:]
        log.error(f"FFmpeg Kling clip error: {err}")
        raise RuntimeError("FFmpeg failed on Kling clip: " + err[-100:])
    return output_clip


async def _merge_sfx_track(sfx_paths: list[str], scenes: list, output_path: str) -> str:
    """Concatenate per-scene SFX clips into one continuous audio track.
    Pads missing scenes with silence to keep timing aligned."""
    job_ts = str(int(time.time() * 1000))
    padded_clips = []

    for i, scene in enumerate(scenes):
        dur = scene.get("duration_sec", 5)
        if i == len(scenes) - 1:
            dur += 1.0
        sfx = sfx_paths[i] if i < len(sfx_paths) else ""

        padded_path = str(TEMP_DIR / f"sfx_pad_{i}_{job_ts}.mp3")
        if sfx and Path(sfx).exists() and Path(sfx).stat().st_size > 0:
            cmd = [
                "ffmpeg", "-y", "-i", sfx,
                "-af", f"apad=whole_dur={dur},afade=t=in:st=0:d=0.15,afade=t=out:st={max(dur - 0.3, 0)}:d=0.3",
                "-t", str(dur), "-ar", "44100", "-ac", "1",
                padded_path,
            ]
        else:
            cmd = [
                "ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=mono",
                "-t", str(dur), padded_path,
            ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        padded_clips.append(padded_path)

    concat_file = TEMP_DIR / f"sfx_concat_{job_ts}.txt"
    with open(concat_file, "w") as f:
        for p in padded_clips:
            f.write(f"file '{Path(p).resolve()}'\n")

    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(concat_file), "-c:a", "libmp3lame", "-ar", "44100",
        output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()

    for p in padded_clips:
        Path(p).unlink(missing_ok=True)
    concat_file.unlink(missing_ok=True)

    if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
        log.info(f"SFX track merged: {output_path}")
        return output_path
    return ""


async def _composite_video_on_runpod(scene_clips: list[Path], audio_path: str, output_path: str, subtitle_path: str = None, sfx_track: str = "") -> str:
    """Run final concat + merge on RunPod to reduce Render RAM pressure."""
    run_id = f"cmp_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    remote_dir = f"{RUNPOD_COMPOSITOR_BASE_DIR.rstrip('/')}/{run_id}"
    remote_merged = f"{remote_dir}/merged.mp4"
    remote_output = f"{remote_dir}/final.mp4"

    ok, err = await asyncio.to_thread(
        _run_remote_cmd_blocking,
        RUNPOD_COMPOSITOR_HOST,
        RUNPOD_COMPOSITOR_SSH_PORT,
        f"mkdir -p '{remote_dir}'",
    )
    if not ok:
        raise RuntimeError(f"RunPod mkdir failed: {err}")

    local_concat = TEMP_DIR / f"remote_concat_{run_id}.txt"
    local_concat.write_text("", encoding="utf-8")
    uploaded_remote_files = []
    try:
        for i, clip in enumerate(scene_clips):
            remote_clip = f"{remote_dir}/scene_{i}.mp4"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                str(clip),
                remote_clip,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod clip upload failed ({clip.name}): {up_err}")
            uploaded_remote_files.append(remote_clip)

        remote_audio = f"{remote_dir}/voice.mp3"
        up_ok, up_err = await asyncio.to_thread(
            _upload_file_to_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            audio_path,
            remote_audio,
        )
        if not up_ok:
            raise RuntimeError(f"RunPod audio upload failed: {up_err}")

        remote_sub = ""
        if subtitle_path and Path(subtitle_path).exists():
            remote_sub = f"{remote_dir}/captions.ass"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                subtitle_path,
                remote_sub,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod subtitle upload failed: {up_err}")

        remote_sfx = ""
        if sfx_track and Path(sfx_track).exists():
            remote_sfx = f"{remote_dir}/sfx.mp3"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                sfx_track,
                remote_sfx,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod sfx upload failed: {up_err}")

        concat_lines = "".join([f"file '{p}'\n" for p in uploaded_remote_files])
        local_concat.write_text(concat_lines, encoding="utf-8")
        remote_concat = f"{remote_dir}/concat.txt"
        up_ok, up_err = await asyncio.to_thread(
            _upload_file_to_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            str(local_concat),
            remote_concat,
        )
        if not up_ok:
            raise RuntimeError(f"RunPod concat upload failed: {up_err}")

        concat_cmd = (
            f"ffmpeg -y -f concat -safe 0 -i '{remote_concat}' "
            f"-c:v libx264 -preset fast -pix_fmt yuv420p '{remote_merged}'"
        )
        ok, err = await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            concat_cmd,
        )
        if not ok:
            raise RuntimeError(f"RunPod concat failed: {err}")

        if remote_sub and remote_sfx:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' -i '{remote_sfx}' "
                f"-vf \"ass={remote_sub}\" "
                f"-filter_complex \"[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];"
                f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]\" "
                f"-map 0:v -map \"[aout]\" -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        elif remote_sub:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' "
                f"-vf \"ass={remote_sub}\" -af apad=pad_dur=0.8 "
                f"-c:v libx264 -preset fast -pix_fmt yuv420p -c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        elif remote_sfx:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' -i '{remote_sfx}' "
                f"-filter_complex \"[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];"
                f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]\" "
                f"-map 0:v -map \"[aout]\" -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        else:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' "
                f"-af apad=pad_dur=0.8 -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )

        ok, err = await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            merge_cmd,
        )
        if not ok:
            raise RuntimeError(f"RunPod final merge failed: {err}")

        dl_ok, dl_err = await asyncio.to_thread(
            _download_file_from_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            remote_output,
            output_path,
        )
        if not dl_ok:
            raise RuntimeError(f"RunPod output download failed: {dl_err}")

        if not Path(output_path).exists() or Path(output_path).stat().st_size == 0:
            raise RuntimeError("RunPod output file missing after download")
        return output_path
    finally:
        local_concat.unlink(missing_ok=True)
        await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            f"rm -rf '{remote_dir}'",
        )


async def composite_video(
    scenes: list,
    scene_assets: list,
    audio_path: str,
    output_path: str,
    resolution: str = "720p",
    use_svd: bool = False,
    subtitle_path: str = None,
    sfx_paths: list[str] = None,
) -> str:
    """Composite scene clips into final MP4 with optional burned-in captions and SFX.
    scene_assets: list of dicts with keys: image, frames, kling_clip
    subtitle_path: optional ASS subtitle file for word-synced captions
    sfx_paths: optional per-scene SFX audio files to mix under voiceover
    """
    config = RESOLUTION_CONFIGS[resolution]
    out_w = config["output_width"]
    out_h = config["output_height"]

    job_ts = str(int(time.time() * 1000))
    concat_file = TEMP_DIR / ("concat_" + job_ts + ".txt")
    scene_clips = []

    num_scenes = len(scenes)
    for i, (scene, asset) in enumerate(zip(scenes, scene_assets)):
        duration = scene.get("duration_sec", 4)
        if i == num_scenes - 1:
            duration += 1.0
        text_overlay = "" if subtitle_path else scene.get("text_overlay", "")
        clip_name = "scene_" + str(i) + "_" + job_ts + ".mp4"
        clip_path = str(TEMP_DIR / clip_name)

        if asset.get("kling_clip"):
            try:
                await kling_clip_to_scene(
                    asset["kling_clip"], duration, clip_path,
                    out_w, out_h, text_overlay, resolution,
                )
                scene_clips.append(Path(clip_path))
                continue
            except Exception as e:
                log.warning(f"Kling clip processing failed for scene {i}: {e}")

        if use_svd and asset.get("frames"):
            try:
                await frames_to_clip(
                    asset["frames"], duration, clip_path,
                    out_w, out_h, text_overlay, resolution,
                )
                scene_clips.append(Path(clip_path))
                continue
            except Exception as e:
                log.warning(f"SVD clip failed for scene {i}, falling back to static: {e}")

        await static_image_to_clip(
            asset["image"], duration, clip_path,
            out_w, out_h, text_overlay, resolution,
        )
        scene_clips.append(Path(clip_path))

    existing_clips = [c for c in scene_clips if c.exists() and c.stat().st_size > 0]
    if not existing_clips:
        raise RuntimeError("No scene clips were created -- nothing to composite")
    log.info(f"Compositing {len(existing_clips)} scene clips into video")

    sfx_track = ""
    if sfx_paths and any(sfx_paths):
        sfx_track_path = str(TEMP_DIR / ("sfx_full_" + job_ts + ".mp3"))
        sfx_track = await _merge_sfx_track(sfx_paths, scenes, sfx_track_path)

    if RUNPOD_COMPOSITOR_ENABLED:
        try:
            await _composite_video_on_runpod(
                existing_clips,
                audio_path,
                output_path,
                subtitle_path=subtitle_path,
                sfx_track=sfx_track,
            )
            for clip in scene_clips:
                clip.unlink(missing_ok=True)
            if sfx_track:
                Path(sfx_track).unlink(missing_ok=True)
            log.info("Final compositing offloaded to RunPod")
            return str(output_path)
        except Exception as e:
            if not RUNPOD_COMPOSITOR_FALLBACK_LOCAL:
                raise
            log.warning(f"RunPod compositing failed, falling back local: {e}")

    with open(concat_file, "w") as f:
        for clip in existing_clips:
            f.write("file '" + str(clip.resolve()) + "'\n")

    merged_video = TEMP_DIR / ("merged_" + job_ts + ".mp4")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(concat_file),
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        str(merged_video),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr_concat = await proc.communicate()
    if proc.returncode != 0:
        err_msg = stderr_concat.decode()[-500:]
        log.error(f"FFmpeg concat error: {err_msg}")
        raise RuntimeError("FFmpeg failed to concat scene clips: " + err_msg[-200:])

    if not merged_video.exists() or merged_video.stat().st_size == 0:
        raise RuntimeError("FFmpeg concat produced no output file")

    has_sfx = sfx_track and Path(sfx_track).exists()

    if subtitle_path and Path(subtitle_path).exists():
        sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", sfx_track,
                "-vf", f"ass={sub_abs}",
                "-filter_complex", "[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-vf", f"ass={sub_abs}",
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        log.info(f"Burning captions from {subtitle_path}" + (" + SFX" if has_sfx else ""))
    else:
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", sfx_track,
                "-filter_complex", "[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr_merge = await proc.communicate()
    if proc.returncode != 0:
        if subtitle_path:
            log.warning(f"Subtitle burn-in failed, retrying without: {stderr_merge.decode()[-300:]}")
            cmd_fallback = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd_fallback, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, stderr_merge = await proc.communicate()
        if proc.returncode != 0:
            err_msg = stderr_merge.decode()[-500:]
            log.error(f"FFmpeg final merge error: {err_msg}")
            raise RuntimeError("FFmpeg failed to merge video + audio: " + err_msg[-200:])

    if not Path(output_path).exists() or Path(output_path).stat().st_size == 0:
        raise RuntimeError("FFmpeg produced no final output file")

    for clip in scene_clips:
        clip.unlink(missing_ok=True)
    concat_file.unlink(missing_ok=True)
    merged_video.unlink(missing_ok=True)
    if sfx_track:
        Path(sfx_track).unlink(missing_ok=True)

    log.info(f"Video composited successfully: {Path(output_path).stat().st_size / 1024 / 1024:.1f} MB")
    return str(output_path)


# ─── Full Generation Pipeline ─────────────────────────────────────────────────

async def run_generation_pipeline(job_id: str, template: str, topic: str, resolution: str = "720p", language: str = "en"):
    try:
        jobs[job_id]["status"] = "generating_script"
        jobs[job_id]["progress"] = 5
        lang_name = SUPPORTED_LANGUAGES.get(language, {}).get("name", "English")
        log.info(f"[{job_id}] Generating script for '{topic}' ({template}, {resolution}, {lang_name})")

        lang_instruction = ""
        if language != "en":
            lang_instruction = f"\n\nIMPORTANT: Write ALL narration text in {lang_name}. The visual_description fields should remain in English (for image generation), but ALL narration/voiceover text MUST be in {lang_name}."
        script_data = await generate_script(template, topic, extra_instructions=lang_instruction)
        scenes = script_data.get("scenes", [])
        if not scenes:
            raise ValueError("Script generation returned no scenes")

        runway_video_enabled = bool(RUNWAY_API_KEY)
        grok_video_enabled = USE_XAI_VIDEO and bool(XAI_API_KEY)
        use_video_engine = runway_video_enabled or grok_video_enabled
        use_video = use_video_engine
        if template == "reddit":
            use_video = False
        if template != "reddit" and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (RUNWAY_API_KEY/XAI_API_KEY)")
        if runway_video_enabled:
            mode_label = "Runway Image-to-Video"
        elif grok_video_enabled:
            mode_label = "Grok Imagine Video"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video_engine else "image"

        jobs[job_id]["status"] = "generating_images"
        jobs[job_id]["progress"] = 10
        jobs[job_id]["total_scenes"] = len(scenes)
        log.info(f"[{job_id}] Script ready: {len(scenes)} scenes. Mode: {mode_label}, {resolution}")

        prompt_prefix = TEMPLATE_PROMPT_PREFIXES.get(template, "")
        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        gen_ts = str(int(time.time() * 1000))

        skeleton_anchor = ""
        skeleton_reference_image_url = SKELETON_GLOBAL_REFERENCE_IMAGE_URL if template == "skeleton" else ""
        if template == "skeleton" and scenes:
            s1_desc = scenes[0].get("visual_description", "")
            outfit_match = re.search(r'[Ww]earing\s+(.{20,200}?)(?:\.|,\s*(?:standing|holding|facing|looking|posed))', s1_desc)
            if outfit_match:
                skeleton_anchor = f"CONSISTENCY ANCHOR -- every skeleton in this video wears: {outfit_match.group(1).strip()}. "

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            jobs[job_id]["progress"] = 10 + int((step_base / total_steps) * 55)
            jobs[job_id]["status"] = "generating_images"

            if template == "skeleton":
                vis_desc = scene.get("visual_description", "")
                full_prompt = (
                    SKELETON_IMAGE_STYLE_PREFIX + " "
                    + SKELETON_MASTER_CONSISTENCY_PROMPT + " "
                    + skeleton_anchor + vis_desc + " " + SKELETON_IMAGE_SUFFIX
                )
            else:
                full_prompt = prompt_prefix + scene.get("visual_description", "")
            img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=template,
                reference_image_url=skeleton_reference_image_url if template == "skeleton" else "",
            )
            if template == "skeleton" and not skeleton_reference_image_url and i == 0:
                skeleton_reference_image_url = _file_to_data_image_url(img_path)
            cdn_url = img_result.get("cdn_url")
            engine_name = "Skeleton LoRA" if (template == "skeleton" and not cdn_url) else ("Grok Imagine" if cdn_url else "SDXL")
            log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated ({engine_name})")

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                jobs[job_id]["status"] = "animating_scenes"
                jobs[job_id]["progress"] = 10 + int(((step_base + 1) / total_steps) * 55)
                kling_motion = TEMPLATE_KLING_MOTION.get(template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt,
                        str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5),
                        image_cdn_url=cdn_url,
                        prefer_wan=(template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")

            scene_assets.append(asset)

        jobs[job_id]["status"] = "generating_voice"
        jobs[job_id]["progress"] = 70
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template=template, language=language)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=template)
            log.info(f"[{job_id}] Word-synced captions generated: {len(word_timings)} words ({lang_name})")

        jobs[job_id]["status"] = "generating_sfx"
        jobs[job_id]["progress"] = 78
        sfx_paths = []
        for i, scene in enumerate(scenes):
            sfx_out = str(TEMP_DIR / (job_id + "_sfx_" + str(i) + ".mp3"))
            desc = scene.get("visual_description", "")
            dur = scene.get("duration_sec", 5)
            sfx_file = await generate_scene_sfx(desc, dur, sfx_out, template=template)
            sfx_paths.append(sfx_file)
        sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)
        log.info(f"[{job_id}] SFX generated: {sum(1 for s in sfx_paths if s)}/{len(scenes)} scenes")

        jobs[job_id]["status"] = "compositing"
        jobs[job_id]["progress"] = 82
        log.info(f"[{job_id}] Compositing final video at {resolution}...")

        output_filename = template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(scenes, scene_assets, audio_path, output_path, resolution=resolution, use_svd=use_video, subtitle_path=subtitle_path, sfx_paths=sfx_paths)

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
                frame_dir = Path(asset["frames"][0]).parent
                if frame_dir.exists():
                    shutil.rmtree(frame_dir, ignore_errors=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", topic),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        await _update_project_by_job(job_id, {
            "status": "rendered",
            "output_file": output_filename,
            "title": script_data.get("title", topic),
        })
        log.info(f"[{job_id}] COMPLETE: {output_filename} ({resolution}, {mode_label})")

    except Exception as e:
        log.error(f"[{job_id}] Pipeline failed: {e}", exc_info=True)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await _update_project_by_job(job_id, {"status": "error", "error": str(e)})


# ─── API Endpoints ────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    template: str
    prompt: str
    resolution: str = "720p"
    language: str = "en"
    mode: str = "auto"
    scenes: list = []


class SceneImageRequest(BaseModel):
    prompt: str
    scene_index: int = 0
    session_id: str = ""
    template: str = "skeleton"
    resolution: str = "720p"


class FinalizeRequest(BaseModel):
    session_id: str
    template: str = "skeleton"
    resolution: str = "720p"
    language: str = "en"
    narration: str = ""
    scenes: list = []


_creative_sessions: dict = {}
_creative_sessions_lock = asyncio.Lock()


def _prune_creative_sessions(max_age_seconds: int = 72 * 3600):
    now = time.time()
    stale_ids = [
        sid for sid, sess in _creative_sessions.items()
        if now - float(sess.get("created_at", now)) > max_age_seconds
    ]
    for sid in stale_ids:
        _creative_sessions.pop(sid, None)


def _load_creative_sessions_from_disk():
    if not CREATIVE_SESSION_PERSISTENCE_ENABLED:
        return
    if not CREATIVE_SESSIONS_FILE.exists():
        return
    try:
        data = json.loads(CREATIVE_SESSIONS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _creative_sessions.clear()
            _creative_sessions.update(data)
            _prune_creative_sessions()
            log.info(f"Loaded {len(_creative_sessions)} creative sessions from disk")
    except Exception as e:
        log.warning(f"Failed to load creative sessions store: {e}")


def _save_creative_sessions_to_disk():
    if not CREATIVE_SESSION_PERSISTENCE_ENABLED:
        return
    try:
        _prune_creative_sessions()
        tmp_path = CREATIVE_SESSIONS_FILE.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(_creative_sessions, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(CREATIVE_SESSIONS_FILE)
    except Exception as e:
        log.warning(f"Failed to persist creative sessions store: {e}")


_load_creative_sessions_from_disk()


def _load_projects_store():
    if not PROJECTS_STORE_FILE.exists():
        return
    try:
        data = json.loads(PROJECTS_STORE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _projects.clear()
            _projects.update(data)
            log.info(f"Loaded {len(_projects)} projects from disk")
    except Exception as e:
        log.warning(f"Failed to load projects store: {e}")


def _save_projects_store():
    try:
        tmp_path = PROJECTS_STORE_FILE.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(_projects, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(PROJECTS_STORE_FILE)
    except Exception as e:
        log.warning(f"Failed to persist projects store: {e}")


def _new_project_id() -> str:
    return f"prj_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"


async def _create_or_update_project(project_id: str, data: dict):
    async with _projects_lock:
        existing = _projects.get(project_id, {})
        merged = {**existing, **data}
        merged["project_id"] = project_id
        merged["updated_at"] = time.time()
        if "created_at" not in merged:
            merged["created_at"] = time.time()
        _projects[project_id] = merged
        _save_projects_store()


async def _update_project_by_job(job_id: str, fields: dict):
    async with _projects_lock:
        target_id = None
        for pid, p in _projects.items():
            if p.get("job_id") == job_id:
                target_id = pid
                break
        if not target_id:
            return
        _projects[target_id] = {**_projects[target_id], **fields, "updated_at": time.time()}
        _save_projects_store()


async def _update_project_by_session(user_id: str, session_id: str, fields: dict):
    async with _projects_lock:
        target_id = None
        for pid, p in _projects.items():
            if p.get("user_id") == user_id and p.get("session_id") == session_id:
                target_id = pid
                break
        if not target_id:
            return
        _projects[target_id] = {**_projects[target_id], **fields, "updated_at": time.time()}
        _save_projects_store()


_load_projects_store()


@app.post("/api/creative/script")
async def creative_generate_script(req: GenerateRequest, request: Request = None):
    """Phase 1: Generate script + scenes for user review. Returns editable scene list."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    _ensure_template_allowed(req.template, user)
    lang_name = SUPPORTED_LANGUAGES.get(req.language, {}).get("name", "English")
    lang_instruction = ""
    if req.language != "en":
        lang_instruction = f"\n\nIMPORTANT: Write ALL narration text in {lang_name}. The visual_description fields should remain in English (for image generation), but ALL narration/voiceover text MUST be in {lang_name}."
    script_data = await generate_script(req.template, req.prompt, extra_instructions=lang_instruction)
    scenes = script_data.get("scenes", [])
    if not scenes:
        raise HTTPException(500, "Script generation returned no scenes")
    session_id = f"cs_{int(time.time())}_{random.randint(1000, 9999)}"
    async with _creative_sessions_lock:
        _creative_sessions[session_id] = {
            "session_id": session_id,
            "user_id": user["id"],
            "template": req.template,
            "topic": req.prompt,
            "resolution": req.resolution,
            "language": req.language,
            "script_data": script_data,
            "scenes": scenes,
            "scene_images": {},
            "created_at": time.time(),
        }
        _save_creative_sessions_to_disk()
    return {
        "session_id": session_id,
        "title": script_data.get("title", req.prompt),
        "scenes": [
            {
                "index": i,
                "narration": s.get("narration", ""),
                "visual_description": s.get("visual_description", ""),
                "duration_sec": s.get("duration_sec", 5),
            }
            for i, s in enumerate(scenes)
        ],
    }


@app.post("/api/creative/session")
async def creative_create_session(body: dict, request: Request = None):
    """Create an empty creative session (no script generation). User builds scenes manually."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    template = body.get("template", "skeleton")
    _ensure_template_allowed(template, user)
    session_id = f"cs_{int(time.time())}_{random.randint(1000, 9999)}"
    async with _creative_sessions_lock:
        _creative_sessions[session_id] = {
            "session_id": session_id,
            "user_id": user["id"],
            "template": template,
            "topic": body.get("topic", "Untitled"),
            "resolution": body.get("resolution", "720p"),
            "language": body.get("language", "en"),
            "script_data": {"title": body.get("topic", "Untitled"), "tags": []},
            "scenes": [],
            "scene_images": {},
            "reference_image_url": "",
            "created_at": time.time(),
        }
        _save_creative_sessions_to_disk()
    project_id = _new_project_id()
    await _create_or_update_project(project_id, {
        "user_id": user["id"],
        "template": template,
        "topic": body.get("topic", "Untitled"),
        "mode": "creative",
        "status": "draft",
        "resolution": body.get("resolution", "720p"),
        "language": body.get("language", "en"),
        "session_id": session_id,
        "scene_count": 0,
    })
    log.info(f"Creative session created: {session_id} for user {user['id']}")
    return {"session_id": session_id, "project_id": project_id}


@app.post("/api/creative/reference-image")
async def creative_reference_image(
    session_id: str = Form(...),
    reference_image: UploadFile = File(...),
    request: Request = None,
):
    """Upload optional creative reference style image and persist it for all scene generations."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _creative_sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")
    if not reference_image.content_type or not reference_image.content_type.startswith("image/"):
        raise HTTPException(400, "Reference file must be an image")

    raw = await reference_image.read()
    if not raw:
        raise HTTPException(400, "Reference image is empty")
    if len(raw) > 8 * 1024 * 1024:
        raise HTTPException(400, "Reference image must be <= 8MB")

    mime = reference_image.content_type or "image/png"
    data_url = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
    session["reference_image_url"] = data_url
    if session.get("template") == "skeleton":
        # Keep legacy skeleton key in sync for downstream compatibility.
        session["skeleton_reference_image"] = data_url
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    return {"ok": True}


@app.get("/api/creative/session/{session_id}/status")
async def creative_session_status(session_id: str, request: Request = None):
    """Return lightweight status for restoring Creative Control UI state after refresh."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _creative_sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")
    return {
        "session_id": session_id,
        "has_reference_image": bool(session.get("reference_image_url") or session.get("skeleton_reference_image")),
        "template": session.get("template", ""),
        "topic": session.get("topic", ""),
        "scene_count": len(session.get("scenes", [])),
    }


@app.post("/api/creative/scene-image")
async def creative_scene_image(req: SceneImageRequest, request: Request = None):
    """Generate (or regenerate) an image for a specific scene. Unlimited regenerations."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _creative_sessions.get(req.session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")

    if "scene_images" not in session:
        session["scene_images"] = {}

    while len(session["scenes"]) <= req.scene_index:
        session["scenes"].append({"visual_description": "", "narration": "", "duration_sec": 5})

    prev_img = session["scene_images"].get(req.scene_index, {})
    prev_gen_id = prev_img.get("generation_id")
    if prev_gen_id:
        asyncio.create_task(_mark_training_feedback(prev_gen_id, accepted=False, user_id=user.get("id", ""), event="regenerate"))

    template = session.get("template", req.template)
    resolution = session.get("resolution", req.resolution)
    neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
    full_prompt = req.prompt
    reference_image_url = session.get("reference_image_url", "")
    skeleton_reference_image_url = session.get("skeleton_reference_image", "")
    if template == "skeleton" and not (reference_image_url or skeleton_reference_image_url):
        skeleton_reference_image_url = SKELETON_GLOBAL_REFERENCE_IMAGE_URL

    if template == "skeleton":
        full_prompt = (
            SKELETON_IMAGE_STYLE_PREFIX + " "
            + SKELETON_MASTER_CONSISTENCY_PROMPT + " "
            + full_prompt + " " + SKELETON_IMAGE_SUFFIX
        )

    scene_reference = reference_image_url
    if template == "skeleton" and not scene_reference:
        scene_reference = skeleton_reference_image_url

    img_path = str(TEMP_DIR / f"{req.session_id}_scene_{req.scene_index}.png")
    img_result = await generate_scene_image(
        full_prompt,
        img_path,
        resolution=resolution,
        negative_prompt=neg_prompt,
        template=template,
        reference_image_url=scene_reference,
    )
    if template == "skeleton" and req.scene_index == 0 and not (session.get("skeleton_reference_image") or session.get("reference_image_url")):
        session["skeleton_reference_image"] = _file_to_data_image_url(img_path)

    gen_id = img_result.get("generation_id", "")

    import base64 as b64mod
    img_bytes = Path(img_path).read_bytes()
    img_b64 = b64mod.b64encode(img_bytes).decode()

    session["scene_images"][req.scene_index] = {
        "path": img_path,
        "cdn_url": img_result.get("cdn_url"),
        "prompt": req.prompt,
        "generation_id": gen_id,
    }

    while len(session["scenes"]) <= req.scene_index:
        session["scenes"].append({"narration": "", "visual_description": "", "duration_sec": 5})
    session["scenes"][req.scene_index]["visual_description"] = req.prompt
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    await _update_project_by_session(user.get("id", ""), req.session_id, {
        "status": "draft",
        "scene_count": len(session["scenes"]),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
    })

    return {
        "scene_index": req.scene_index,
        "image_data": f"data:image/png;base64,{img_b64}",
        "prompt_used": req.prompt,
        "generation_id": gen_id,
    }


@app.post("/api/creative/scene-feedback")
async def creative_scene_feedback(body: dict, request: Request = None):
    """Mark a generated image as accepted (user moved on) or rejected (user regenerated)."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    gen_id = body.get("generation_id", "")
    accepted = body.get("accepted", True)
    if gen_id:
        await _mark_training_feedback(gen_id, accepted=accepted, user_id=user.get("id", ""), event="scene_feedback")
    return {"ok": True, "generation_id": gen_id, "status": "accepted" if accepted else "rejected"}


@app.put("/api/creative/scene/{session_id}/{scene_index}")
async def creative_update_scene(session_id: str, scene_index: int, body: dict, request: Request = None):
    """Update a scene's narration or visual description."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _creative_sessions.get(session_id)
    if not session or session["user_id"] != user["id"]:
        raise HTTPException(404, "Session not found")
    if scene_index >= len(session["scenes"]):
        raise HTTPException(400, "Invalid scene index")
    scene = session["scenes"][scene_index]
    if "narration" in body:
        scene["narration"] = body["narration"]
    if "visual_description" in body:
        scene["visual_description"] = body["visual_description"]
    if "duration_sec" in body:
        scene["duration_sec"] = body["duration_sec"]
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    await _update_project_by_session(user.get("id", ""), session_id, {
        "status": "draft",
        "scene_count": len(session["scenes"]),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
    })
    return {"ok": True, "scene": scene}


@app.post("/api/creative/finalize")
async def creative_finalize(req: FinalizeRequest, background_tasks: BackgroundTasks, request: Request = None):
    """Phase 3: Run the full pipeline using the user's scenes + images."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _creative_sessions.get(req.session_id)
    if not session or session["user_id"] != user["id"]:
        raise HTTPException(404, "Session not found")
    _ensure_template_allowed(session.get("template", req.template), user)

    if req.narration:
        session["narration"] = req.narration
    if req.scenes:
        session["scenes"] = req.scenes
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    if not session["scenes"]:
        raise HTTPException(400, "No scenes provided")

    user_plan = user.get("plan", "free")
    if user_plan == "admin":
        user_plan = "pro"
    plan_limits = PLAN_LIMITS.get(user_plan, PLAN_LIMITS["free"])
    resolution = session.get("resolution", req.resolution)
    total_duration = sum(float(s.get("duration_sec", 5) or 5) for s in session.get("scenes", []))
    if total_duration > float(plan_limits.get("max_duration_sec", 60)):
        raise HTTPException(400, f"Creative project exceeds plan duration limit ({int(plan_limits.get('max_duration_sec', 60))}s).")
    # The 12-scene cap was for legacy WAN-heavy skeleton renders.
    # With Runway as primary, allow longer skeleton projects.
    if (not RUNWAY_API_KEY) and session.get("template") == "skeleton" and len(session.get("scenes", [])) > 12:
        raise HTTPException(400, "Skeleton Creative projects are limited to 12 scenes when Runway is unavailable.")

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": session["template"],
        "topic": session["topic"],
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id"),
        "created_at": time.time(),
    }
    await _update_project_by_session(user.get("id", ""), req.session_id, {
        "status": "rendering",
        "job_id": job_id,
        "scene_count": len(session.get("scenes", [])),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
    })

    for si_data in session.get("scene_images", {}).values():
        gen_id = si_data.get("generation_id")
        if gen_id:
            asyncio.create_task(_mark_training_feedback(gen_id, accepted=True, user_id=user.get("id", ""), event="finalize"))

    await _enqueue_generation_job(job_id, user_plan, _run_creative_pipeline, (job_id, session, resolution))
    return {"job_id": job_id}


async def _run_creative_pipeline(job_id: str, session: dict, resolution: str):
    """Run generation using pre-approved creative session scenes."""
    try:
        template = session["template"]
        scenes = session["scenes"]
        language = session.get("language", "en")
        scene_images = session.get("scene_images", {})
        script_data = session.get("script_data", {})

        runway_video_enabled = bool(RUNWAY_API_KEY)
        grok_video_enabled = USE_XAI_VIDEO and bool(XAI_API_KEY)
        use_video_engine = runway_video_enabled or grok_video_enabled
        use_video = use_video_engine
        if not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (RUNWAY_API_KEY/XAI_API_KEY)")
        gen_ts = str(int(time.time() * 1000))

        jobs[job_id]["status"] = "generating_images"
        jobs[job_id]["progress"] = 10
        jobs[job_id]["total_scenes"] = len(scenes)

        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        reference_image_url = session.get("reference_image_url", "")
        skeleton_reference_image_url = session.get("skeleton_reference_image", "")
        if template == "skeleton" and not (reference_image_url or skeleton_reference_image_url):
            skeleton_reference_image_url = SKELETON_GLOBAL_REFERENCE_IMAGE_URL

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            jobs[job_id]["progress"] = 10 + int((step_base / total_steps) * 55)

            pre_gen = scene_images.get(i) or scene_images.get(str(i))
            if pre_gen and Path(pre_gen["path"]).exists():
                img_path = pre_gen["path"]
                cdn_url = pre_gen.get("cdn_url")
                log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using pre-approved image")
            else:
                full_prompt = scene.get("visual_description", "")
                img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
                img_result = await generate_scene_image(
                    full_prompt,
                    img_path,
                    resolution=resolution,
                    negative_prompt=neg_prompt,
                    template=template,
                    reference_image_url=(reference_image_url or skeleton_reference_image_url) if template == "skeleton" else reference_image_url,
                )
                if template == "skeleton" and not (reference_image_url or skeleton_reference_image_url) and i == 0:
                    skeleton_reference_image_url = _file_to_data_image_url(img_path)
                    session["skeleton_reference_image"] = skeleton_reference_image_url
                cdn_url = img_result.get("cdn_url")
                log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated fresh")

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                jobs[job_id]["status"] = "animating_scenes"
                jobs[job_id]["progress"] = 10 + int(((step_base + 1) / total_steps) * 55)
                kling_motion = TEMPLATE_KLING_MOTION.get(template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt, str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5), image_cdn_url=cdn_url,
                        prefer_wan=(template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")

            scene_assets.append(asset)

        jobs[job_id]["status"] = "generating_voice"
        jobs[job_id]["progress"] = 70
        full_narration = session.get("narration", "") or " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template=template, language=language)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=template)

        jobs[job_id]["status"] = "generating_sfx"
        jobs[job_id]["progress"] = 78
        sfx_paths = []
        for i, scene in enumerate(scenes):
            sfx_out = str(TEMP_DIR / (job_id + "_sfx_" + str(i) + ".mp3"))
            desc = scene.get("visual_description", "")
            dur = scene.get("duration_sec", 5)
            sfx_file = await generate_scene_sfx(desc, dur, sfx_out, template=template)
            sfx_paths.append(sfx_file)
        sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)
        log.info(f"[{job_id}] SFX generated: {sum(1 for s in sfx_paths if s)}/{len(scenes)} scenes")

        jobs[job_id]["status"] = "compositing"
        jobs[job_id]["progress"] = 82
        output_filename = template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(scenes, scene_assets, audio_path, output_path, resolution=resolution, use_svd=use_video, subtitle_path=subtitle_path, sfx_paths=sfx_paths)

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", session.get("topic", "")),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        await _update_project_by_job(job_id, {
            "status": "rendered",
            "output_file": output_filename,
            "title": script_data.get("title", session.get("topic", "")),
        })
        log.info(f"[{job_id}] CREATIVE PIPELINE COMPLETE: {output_filename}")

        sid = session.get("session_id")
        if sid:
            async with _creative_sessions_lock:
                _creative_sessions.pop(sid, None)
                _save_creative_sessions_to_disk()

    except Exception as e:
        log.error(f"[{job_id}] Creative pipeline failed: {e}", exc_info=True)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await _update_project_by_job(job_id, {"status": "error", "error": str(e)})


@app.get("/api/languages")
async def list_languages():
    return {"languages": [{"code": k, "name": v["name"]} for k, v in SUPPORTED_LANGUAGES.items()]}


@app.get("/api/health")
async def health():
    skeleton_lora = await check_skeleton_lora_available()
    wan_ready = await check_wan22_available()
    runway_video_enabled = bool(RUNWAY_API_KEY)
    grok_video_enabled = USE_XAI_VIDEO and bool(XAI_API_KEY)
    if runway_video_enabled and grok_video_enabled:
        video_engine = "Runway (primary) + Grok fallback"
    elif runway_video_enabled:
        video_engine = "Runway Image-to-Video"
    elif grok_video_enabled:
        video_engine = "Grok Imagine Video"
    elif wan_ready:
        video_engine = "Wan 2.2 (RunPod)"
    elif FAL_AI_KEY:
        video_engine = "Kling 2.1 Standard"
    else:
        video_engine = "Static"
    return {
        "status": "online",
        "engine": "NYPTID Studio Engine v3.0",
        "kling_enabled": bool(FAL_AI_KEY),
        "wan22_ready": wan_ready,
        "video_engine": video_engine,
        "comfyui_url": COMFYUI_URL[:50],
        "skeleton_lora": skeleton_lora,
        "image_engine_skeleton": (
            "Skeleton LoRA (local)"
            if skeleton_lora
            else (f"xAI {XAI_IMAGE_MODEL}" if XAI_API_KEY else "SDXL")
        ),
    }


@app.post("/api/admin/comfyui-url")
async def set_comfyui_url(body: dict, user: dict = Depends(require_auth)):
    """Admin-only: update the ComfyUI URL at runtime (e.g. after cloudflared restart)."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    global COMFYUI_URL
    new_url = body.get("url", "").strip().rstrip("/")
    if not new_url:
        raise HTTPException(400, "url required")
    COMFYUI_URL = new_url
    wan_ready = await check_wan22_available()
    return {"ok": True, "comfyui_url": COMFYUI_URL, "wan22_ready": wan_ready}


@app.get("/api/admin/training-stats")
async def training_stats(user: dict = Depends(require_auth)):
    """Admin: get training data collection stats."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    pairs = list(TRAINING_DATA_DIR.glob("*.png"))
    accepted = sum(1 for e in _pending_training.values() if e["status"] == "accepted")
    rejected = sum(1 for e in _pending_training.values() if e["status"] == "rejected")
    pending = sum(1 for e in _pending_training.values() if e["status"] == "pending")
    return {
        "total_on_disk": len(pairs),
        "accepted": accepted,
        "rejected": rejected,
        "pending_review": pending,
        "disk_mb": round(sum(p.stat().st_size for p in pairs) / (1024 * 1024), 1),
    }


@app.get("/api/admin/analytics")
async def admin_analytics(user: dict = Depends(require_auth)):
    """Admin dashboard analytics: active usage + paid tier totals + monthly revenue estimate."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")

    active_job_statuses = {"queued", "generating_script", "generating_images", "animating_scenes", "generating_voice", "generating_sfx", "compositing", "analyzing"}
    active_jobs = [j for j in jobs.values() if j.get("status") in active_job_statuses]
    active_generations = len(active_jobs)
    active_generating_users = len({j.get("user_id") for j in active_jobs if j.get("user_id")})

    tier_counts = {"starter": 0, "creator": 0, "pro": 0, "demo_pro": 0}
    monthly_revenue_usd = 0.0
    revenue_source = "none"

    if STRIPE_SECRET_KEY:
        try:
            subs = stripe_lib.Subscription.list(status="all", limit=100, expand=["data.items.data.price"])
            for sub in subs.auto_paging_iter():
                sub_status = sub.get("status")
                if sub_status not in ("active", "trialing", "past_due", "unpaid"):
                    continue
                for item in sub.get("items", {}).get("data", []):
                    price = item.get("price", {}) or {}
                    price_id = price.get("id", "")
                    plan = STRIPE_PRICE_TO_PLAN.get(price_id)
                    if plan in tier_counts:
                        qty = int(item.get("quantity", 1) or 1)
                        tier_counts[plan] += qty
                        monthly_revenue_usd += ((price.get("unit_amount") or 0) / 100.0) * qty
            revenue_source = "stripe"
        except Exception as e:
            log.warning(f"Admin analytics stripe read failed: {e}")

    if revenue_source != "stripe" and SUPABASE_URL and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY):
        # Fallback: infer subscriber counts from profile plans if Stripe is unavailable.
        try:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    f"{SUPABASE_URL}/rest/v1/profiles?select=plan&limit=5000",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if resp.status_code == 200:
                    for row in resp.json():
                        p = row.get("plan")
                        if p in tier_counts:
                            tier_counts[p] += 1
                    revenue_source = "profiles"
        except Exception as e:
            log.warning(f"Admin analytics profiles fallback failed: {e}")

    active_users_signins_15m = 0
    if SUPABASE_URL and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY):
        try:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            now_utc = datetime.now(timezone.utc)
            async with httpx.AsyncClient(timeout=20) as client:
                users_resp = await client.get(
                    f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if users_resp.status_code == 200:
                    users_data = users_resp.json()
                    user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                    for u in user_list:
                        ts = u.get("last_sign_in_at")
                        if not ts:
                            continue
                        try:
                            signed_in_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            if (now_utc - signed_in_at).total_seconds() <= 15 * 60:
                                active_users_signins_15m += 1
                        except Exception:
                            continue
        except Exception as e:
            log.warning(f"Admin analytics active-users fetch failed: {e}")

    return {
        "active_generations": active_generations,
        "queue_depth": len(_queued_job_meta),
        "queue_workers": JOB_QUEUE_WORKERS,
        "active_users_generating": active_generating_users,
        "active_users_signins_15m": active_users_signins_15m,
        "active_users_estimate": max(active_generating_users, active_users_signins_15m),
        "subscribers_by_tier": tier_counts,
        "total_paid_subscribers": sum(tier_counts.values()),
        "monthly_revenue_usd": round(monthly_revenue_usd, 2),
        # We currently expose gross subscription revenue as a profit proxy.
        "monthly_profit_usd": round(monthly_revenue_usd, 2),
        "revenue_source": revenue_source,
    }


@app.get("/api/config")
async def public_config():
    return {
        "supabase_url": SUPABASE_URL,
        "supabase_anon_key": SUPABASE_ANON_KEY,
        "stripe_enabled": bool(STRIPE_SECRET_KEY),
        "plans": {
            name: {k: v for k, v in limits.items()}
            for name, limits in PLAN_LIMITS.items()
        },
        "prices": {v: k for k, v in STRIPE_PRICE_TO_PLAN.items()},
    }


@app.get("/api/me")
async def get_me(user: dict = Depends(require_auth)):
    plan = user.get("plan", "free")
    email = user.get("email", "")
    is_admin = email in ADMIN_EMAILS or plan == "admin"
    if plan == "admin":
        limits = PLAN_LIMITS["pro"]
        limits = {**limits, "videos_per_month": 9999}
    else:
        limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
    has_demo = plan == "demo_pro" or is_admin
    return {
        "id": user["id"],
        "email": email,
        "plan": plan if plan != "admin" else "pro",
        "role": "admin" if is_admin else "user",
        "limits": limits,
        "demo_access": has_demo,
        "demo_price_id": DEMO_PRO_PRICE_ID,
    }


@app.post("/api/generate")
async def generate_short(req: GenerateRequest, background_tasks: BackgroundTasks, request: Request = None):
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")
    if not ELEVENLABS_API_KEY:
        raise HTTPException(500, "ELEVENLABS_API_KEY not configured")

    user = await get_current_user_from_request(request) if request else None
    _ensure_template_allowed(req.template, user)
    user_plan = "free"
    if user:
        user_plan = user.get("plan", "free")
        if user_plan == "admin":
            user_plan = "pro"

    plan_limits = PLAN_LIMITS.get(user_plan, PLAN_LIMITS["free"])

    resolution = req.resolution if req.resolution in RESOLUTION_CONFIGS else "720p"
    if not plan_limits.get("priority", False) and resolution == "1080p":
        resolution = "720p"

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": req.template,
        "topic": req.prompt,
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id") if user else None,
        "created_at": time.time(),
    }
    language = req.language if req.language in SUPPORTED_LANGUAGES else "en"
    if user:
        project_id = _new_project_id()
        await _create_or_update_project(project_id, {
            "user_id": user.get("id"),
            "template": req.template,
            "topic": req.prompt,
            "mode": "auto",
            "status": "rendering",
            "resolution": resolution,
            "language": language,
            "job_id": job_id,
        })
        jobs[job_id]["project_id"] = project_id

    await _enqueue_generation_job(job_id, user_plan, run_generation_pipeline, (job_id, req.template, req.prompt, resolution, language))

    return {"status": "accepted", "job_id": job_id}


@app.get("/api/status/{job_id}")
async def job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    return jobs[job_id]


@app.get("/api/download/{filename}")
async def download_video(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Video not found")
    return FileResponse(str(path), media_type="video/mp4", filename=filename)


CLONE_ANALYSIS_PROMPT = """You are a viral video reverse-engineering expert. Analyze the source video and extract its EXACT winning formula so it can be replicated on a new topic.

You will receive:
1. Context about a viral short (uploaded video metadata, audio timing, or description)
2. A new topic to apply the viral formula to

Your job: figure out the EXACT structure, pacing, and style of the source video and replicate it beat-for-beat on the new topic.

Analyze these elements:
- HOOK: What made someone stop scrolling in the first 1-3 seconds? (question? claim? shock?)
- PACING: How fast are cuts? What is the average scene duration?
- VISUAL STYLE: 3D skeletons? Cinematic? Text-heavy? What background color?
- NARRATION STYLE: Fast? Punchy? How many sentences per scene?
- TEXT OVERLAYS: One word at a time? Full sentences? Bold impact font?
- STRUCTURE: VS comparison? Countdown? Story arc? How is info revealed?
- RETENTION TRICKS: Money flying, size comparisons, face-offs, shocking numbers?

TEMPLATE DEFINITIONS (pick the one that matches BEST):
- "skeleton" = 3D skeleton characters wearing topic-relevant outfits on teal/green studio background. VS comparisons, career/earnings breakdowns. One-word bold captions. Example: "NASCAR vs F1 Driver Who Makes More Money"
- "history" = Epic cinematic historical scenes. Battles, empires, ancient events. Dramatic narrator, god rays, film grain. 2-4 word caption phrases. Example: "What Happened to the Roman Legion That Vanished"
- "story" = Cinematic AI visual stories with emotional arc. Pixar/UE5 quality. Consistent character across scenes. Poetic narration. Minimal captions. Example: "The Last Lighthouse Keeper"
- "reddit" = Reddit story narration. First-person dramatic stories (AITA, TIFU). Photorealistic modern-day scenes illustrating the story. Dialogue/reaction captions. Example: "AITA for Kicking Out My Sister"
- "top5" = Ranked countdown lists (#5 to #1). Each item dramatically different. Documentary quality visuals. Numbered captions. Example: "Top 5 Most Expensive Things Ever Sold"
- "random" = Chaotic, fast-paced, unpredictable content. Every scene wildly different. Surreal visuals. 1-3 word reaction captions. Example: "Things That Should Not Exist"

Output MUST be valid JSON:
{
  "detected_template": "skeleton|history|story|reddit|top5|random",
  "viral_analysis": {
    "hook_type": "What kind of hook (shock/question/claim/visual)",
    "pacing": "fast|medium|slow",
    "avg_scene_duration": 3.5,
    "scene_count": 10,
    "tone": "Description of voice/narration tone",
    "retention_tricks": ["trick1", "trick2"],
    "what_made_it_viral": "1-2 sentence summary"
  },
  "optimized_prompt": "An enhanced prompt that combines the viral formula with the new topic. This should be detailed enough to pass directly to the script generator."
}"""


async def extract_audio_from_video(video_path: str) -> str | None:
    """Extract audio track from a video file for transcription."""
    audio_path = video_path.rsplit(".", 1)[0] + "_audio.mp3"
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vn", "-acodec", "libmp3lame", "-q:a", "4",
        audio_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()
    if proc.returncode == 0 and Path(audio_path).exists():
        return audio_path
    return None


async def transcribe_audio_with_grok(audio_path: str) -> str:
    """Use ffmpeg to get audio duration then estimate narration from file size for context."""
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(audio_path)]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    duration = 0
    if proc.returncode == 0:
        try:
            data = json.loads(stdout.decode())
            duration = float(data.get("format", {}).get("duration", 0))
        except Exception:
            pass
    return f"Audio duration: {duration:.1f}s"


async def analyze_viral_video(topic: str, video_description: str, transcript_hint: str = "") -> dict:
    user_parts = []
    user_parts.append("Source viral video context: " + video_description)
    if transcript_hint:
        user_parts.append("Audio/timing info from source: " + transcript_hint)
    user_parts.append("New topic to apply the viral formula to: " + topic)
    user_msg = "\n\n".join(user_parts)

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": CLONE_ANALYSIS_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.6,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in clone analysis response")
        return json.loads(content[start:end])


async def extract_video_metadata(file_path: str) -> dict:
    """Extract basic metadata from uploaded video using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", str(file_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            data = json.loads(stdout.decode())
            duration = float(data.get("format", {}).get("duration", 0))
            streams = data.get("streams", [])
            video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
            return {
                "duration_sec": round(duration, 1),
                "width": int(video_stream.get("width", 0)),
                "height": int(video_stream.get("height", 0)),
                "fps": video_stream.get("r_frame_rate", "30/1"),
            }
    except Exception as e:
        log.warning(f"ffprobe metadata extraction failed: {e}")
    return {}


async def generate_clone_script(template: str, topic: str, viral_analysis: dict) -> dict:
    """Generate a script that replicates the source video's exact formula on a new topic."""
    base_prompt = TEMPLATE_SYSTEM_PROMPTS.get(template, TEMPLATE_SYSTEM_PROMPTS["random"])

    hook_type = viral_analysis.get("hook_type", "claim")
    pacing = viral_analysis.get("pacing", "fast")
    avg_dur = viral_analysis.get("avg_scene_duration", 3.5)
    scene_count = viral_analysis.get("scene_count", 10)
    tone = viral_analysis.get("tone", "energetic and punchy")
    tricks = viral_analysis.get("retention_tricks", [])
    what_viral = viral_analysis.get("what_made_it_viral", "")

    clone_override = (
        "\n\nCRITICAL CLONE INSTRUCTIONS -- you MUST follow these EXACTLY:\n"
        "You are cloning a proven viral video. Replicate its formula precisely.\n"
        "- Hook type: " + str(hook_type) + " -- your opening MUST use this exact hook style\n"
        "- Pacing: " + str(pacing) + " -- match this exact energy level\n"
        "- Target scene count: " + str(scene_count) + " scenes\n"
        "- Average scene duration: " + str(avg_dur) + " seconds\n"
        "- Narration tone: " + str(tone) + "\n"
        "- Retention tricks to replicate: " + ", ".join(tricks) + "\n"
        "- Why the original went viral: " + str(what_viral) + "\n"
        "\nDo NOT write generic content. Do NOT say things like 'dive into the world of' or "
        "'buckle up'. Write EXACTLY like the source video's style -- punchy, direct, zero filler. "
        "Every single word must earn its place. If the source was a skeleton comparing things, "
        "YOU compare things the same way. Match the structure beat-for-beat.\n"
        "\nNarration must be SHORT and PUNCHY -- 1-2 sentences max per scene. "
        "No yapping. No fluff. Every sentence is a hook or a fact bomb."
    )

    full_prompt = base_prompt + clone_override

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": full_prompt},
                    {"role": "user", "content": "Clone this viral formula onto new topic: " + topic},
                ],
                "temperature": 0.7,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in clone script response")
        return json.loads(content[start:end])


async def run_clone_pipeline(job_id: str, topic: str, video_path: str | None, resolution: str = "720p"):
    try:
        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 5
        log.info(f"[{job_id}] Clone: analyzing viral video for topic '{topic}'")

        video_context = topic
        transcript_hint = ""
        meta = {}
        if video_path:
            meta = await extract_video_metadata(video_path) or {}
            if meta:
                video_context = (
                    "Source video file uploaded: "
                    + str(meta.get("duration_sec", "?")) + "s long, "
                    + str(meta.get("width", "?")) + "x" + str(meta.get("height", "?")) + " resolution"
                )
            audio_path = await extract_audio_from_video(video_path)
            if audio_path:
                transcript_hint = await transcribe_audio_with_grok(audio_path)
                Path(audio_path).unlink(missing_ok=True)

        analysis = await analyze_viral_video(topic, video_context, transcript_hint)
        detected_template = analysis.get("detected_template", "random")
        viral_info = analysis.get("viral_analysis", {})

        jobs[job_id]["template"] = detected_template
        jobs[job_id]["viral_analysis"] = viral_info
        jobs[job_id]["progress"] = 12
        log.info(f"[{job_id}] Clone analysis: template={detected_template}, hook={viral_info.get('hook_type', '?')}, scenes={viral_info.get('scene_count', '?')}")

        if video_path:
            Path(video_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "generating_script"
        jobs[job_id]["progress"] = 15
        script_data = await generate_clone_script(detected_template, topic, viral_info)
        scenes = script_data.get("scenes", [])
        if not scenes:
            raise ValueError("Clone script generation returned no scenes")

        runway_video_enabled = bool(RUNWAY_API_KEY)
        grok_video_enabled = USE_XAI_VIDEO and bool(XAI_API_KEY)
        use_video_engine = runway_video_enabled or grok_video_enabled
        use_video = use_video_engine
        if detected_template == "reddit":
            use_video = False
        if detected_template != "reddit" and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (RUNWAY_API_KEY/XAI_API_KEY)")
        if runway_video_enabled:
            mode_label = "Runway Image-to-Video"
        elif grok_video_enabled:
            mode_label = "Grok Imagine Video"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video_engine else "image"

        jobs[job_id]["status"] = "generating_images"
        jobs[job_id]["progress"] = 20
        jobs[job_id]["total_scenes"] = len(scenes)
        log.info(f"[{job_id}] Clone script ready: {len(scenes)} scenes. Mode: {mode_label}, {resolution}")

        prompt_prefix = TEMPLATE_PROMPT_PREFIXES.get(detected_template, "")
        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(detected_template, NEGATIVE_PROMPT)
        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        gen_ts = str(int(time.time() * 1000))

        clone_skeleton_anchor = ""
        clone_skeleton_reference_image_url = SKELETON_GLOBAL_REFERENCE_IMAGE_URL if detected_template == "skeleton" else ""
        if detected_template == "skeleton" and scenes:
            s1_desc = scenes[0].get("visual_description", "")
            outfit_match = re.search(r'[Ww]earing\s+(.{20,200}?)(?:\.|,\s*(?:standing|holding|facing|looking|posed))', s1_desc)
            if outfit_match:
                clone_skeleton_anchor = f"CONSISTENCY ANCHOR -- every skeleton in this video wears: {outfit_match.group(1).strip()}. "

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            jobs[job_id]["progress"] = 20 + int((step_base / total_steps) * 50)
            jobs[job_id]["status"] = "generating_images"

            if detected_template == "skeleton":
                vis_desc = scene.get("visual_description", "")
                full_prompt = (
                    SKELETON_IMAGE_STYLE_PREFIX + " "
                    + SKELETON_MASTER_CONSISTENCY_PROMPT + " "
                    + clone_skeleton_anchor + vis_desc + " " + SKELETON_IMAGE_SUFFIX
                )
            else:
                full_prompt = prompt_prefix + scene.get("visual_description", "")
            img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=detected_template,
                reference_image_url=clone_skeleton_reference_image_url if detected_template == "skeleton" else "",
            )
            if detected_template == "skeleton" and not clone_skeleton_reference_image_url and i == 0:
                clone_skeleton_reference_image_url = _file_to_data_image_url(img_path)
            cdn_url = img_result.get("cdn_url")
            engine_name = "Skeleton LoRA" if (detected_template == "skeleton" and not cdn_url) else ("Grok Imagine" if cdn_url else "SDXL")
            log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated ({engine_name})")

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                jobs[job_id]["status"] = "animating_scenes"
                jobs[job_id]["progress"] = 20 + int(((step_base + 1) / total_steps) * 50)
                kling_motion = TEMPLATE_KLING_MOTION.get(detected_template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt,
                        str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5),
                        image_cdn_url=cdn_url,
                        prefer_wan=(detected_template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")

            scene_assets.append(asset)

        jobs[job_id]["status"] = "generating_voice"
        jobs[job_id]["progress"] = 75
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template=detected_template)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=detected_template)
            log.info(f"[{job_id}] Word-synced captions generated: {len(word_timings)} words")

        jobs[job_id]["status"] = "compositing"
        jobs[job_id]["progress"] = 85
        log.info(f"[{job_id}] Compositing final video at {resolution}...")

        output_filename = detected_template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(scenes, scene_assets, audio_path, output_path, resolution=resolution, use_svd=use_video, subtitle_path=subtitle_path)

        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
                frame_dir = Path(asset["frames"][0]).parent
                if frame_dir.exists():
                    shutil.rmtree(frame_dir, ignore_errors=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", topic),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        log.info(f"[{job_id}] COMPLETE: {output_filename} ({resolution}, {mode_label})")

    except Exception as e:
        log.error(f"[{job_id}] Clone pipeline failed: {e}", exc_info=True)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        if video_path:
            Path(video_path).unlink(missing_ok=True)


@app.post("/api/clone")
async def clone_video(
    topic: str = Form(...),
    resolution: str = Form("720p"),
    file: UploadFile = File(None),
    background_tasks: BackgroundTasks = None,
):
    if not XAI_API_KEY or not ELEVENLABS_API_KEY:
        raise HTTPException(500, "API keys not configured")

    res = resolution if resolution in RESOLUTION_CONFIGS else "720p"

    video_path = None
    if file and file.filename:
        video_path = str(TEMP_DIR / f"clone_upload_{int(time.time())}.mp4")
        with open(video_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)

    job_id = f"clone_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": "analyzing...",
        "topic": topic,
        "resolution": res,
        "created_at": time.time(),
    }

    await _enqueue_generation_job(job_id, "free", run_clone_pipeline, (job_id, topic, video_path, res))
    return {"status": "accepted", "job_id": job_id}


@app.get("/api/jobs")
async def list_jobs():
    return {jid: {k: v for k, v in j.items() if k != "output_file"} for jid, j in jobs.items()}


@app.get("/api/projects")
async def list_projects(request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    uid = user.get("id", "")
    rows = [p for p in _projects.values() if p.get("user_id") == uid]
    rows.sort(key=lambda p: p.get("updated_at", 0), reverse=True)
    drafts = [p for p in rows if p.get("status") in ("draft", "rendering")]
    renders = [p for p in rows if p.get("status") in ("rendered", "error")]
    return {"drafts": drafts, "renders": renders, "total": len(rows)}


@app.get("/api/projects/{project_id}")
async def get_project(project_id: str, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    proj = _projects.get(project_id)
    if not proj or proj.get("user_id") != user.get("id"):
        raise HTTPException(404, "Project not found")
    return {"project": proj}


# ─── Stripe Payments ──────────────────────────────────────────────────────────

class CheckoutRequest(BaseModel):
    price_id: str


@app.post("/api/checkout")
async def create_checkout(req: CheckoutRequest, user: dict = Depends(require_auth)):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    if req.price_id not in STRIPE_PRICE_TO_PLAN:
        raise HTTPException(400, "Invalid price ID")

    try:
        session = stripe_lib.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            line_items=[{"price": req.price_id, "quantity": 1}],
            success_url=f"{SITE_URL}?payment=success",
            cancel_url=f"{SITE_URL}?payment=cancelled",
            client_reference_id=user["id"],
            customer_email=user["email"],
            metadata={"user_id": user["id"], "plan": STRIPE_PRICE_TO_PLAN[req.price_id]},
        )
        return {"checkout_url": session.url}
    except Exception as e:
        log.error(f"Stripe checkout error: {e}")
        raise HTTPException(500, f"Payment error: {str(e)}")


@app.post("/api/stripe-webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe_lib.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except Exception as e:
        log.error(f"Webhook signature verification failed: {e}")
        raise HTTPException(400, "Invalid signature")

    if event.get("type") == "checkout.session.completed":
        session_data = event["data"]["object"]
        user_id = session_data.get("client_reference_id") or session_data.get("metadata", {}).get("user_id")
        plan = session_data.get("metadata", {}).get("plan", "starter")

        if user_id and SUPABASE_URL:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    await client.post(
                        f"{SUPABASE_URL}/rest/v1/profiles",
                        headers={
                            "apikey": svc_key,
                            "Authorization": f"Bearer {svc_key}",
                            "Content-Type": "application/json",
                            "Prefer": "resolution=merge-duplicates",
                        },
                        json={"id": user_id, "plan": plan},
                    )
                log.info(f"Stripe webhook: user {user_id} upgraded to {plan}")
            except Exception as e:
                log.error(f"Failed to update plan for {user_id}: {e}")

    elif event.get("type") == "customer.subscription.deleted":
        sub = event["data"]["object"]
        customer_email = sub.get("customer_email", "")
        log.info(f"Subscription cancelled for {customer_email}")

    return {"status": "ok"}


# ─── Admin: set plan for a user (admin-only) ─────────────────────────────────

class SetPlanRequest(BaseModel):
    email: str
    plan: str


@app.post("/api/admin/set-plan")
async def admin_set_plan(req: SetPlanRequest, user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS and user.get("plan") != "admin":
        raise HTTPException(403, "Admin access required")
    if req.plan not in list(PLAN_LIMITS.keys()) + ["admin"]:
        raise HTTPException(400, f"Invalid plan. Options: {list(PLAN_LIMITS.keys())}")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL:
        raise HTTPException(500, "Supabase not configured")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            target_id = None
            if users_resp.status_code == 200:
                users_data = users_resp.json()
                user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                for u in user_list:
                    if u.get("email") == req.email:
                        target_id = u["id"]
                        break

            if not target_id:
                return {"error": f"User {req.email} not found in Supabase auth"}

            await client.post(
                f"{SUPABASE_URL}/rest/v1/profiles",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates",
                },
                json={"id": target_id, "plan": req.plan, "role": "admin" if req.plan == "admin" else "user"},
            )
        return {"status": "ok", "email": req.email, "plan": req.plan}
    except Exception as e:
        log.error(f"Admin set-plan error: {e}")
        raise HTTPException(500, str(e))


# ─── User Feedback Collection ─────────────────────────────────────────────────

class FeedbackRequest(BaseModel):
    job_id: str = ""
    rating: int
    comment: str = ""
    template: str = ""
    language: str = ""
    feature: str = ""


@app.post("/api/feedback")
async def submit_feedback(req: FeedbackRequest, user: dict = Depends(require_auth)):
    if req.rating < 1 or req.rating > 5:
        raise HTTPException(400, "Rating must be 1-5")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        raise HTTPException(500, "Feedback storage not configured")

    feedback_data = {
        "user_id": user["id"],
        "email": user.get("email", ""),
        "job_id": req.job_id,
        "rating": req.rating,
        "comment": req.comment[:2000] if req.comment else "",
        "template": req.template,
        "language": req.language,
        "feature": req.feature or "general",
        "plan": user.get("plan", "free"),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/feedback",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=feedback_data,
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(f"Feedback insert failed ({resp.status_code}): {resp.text[:200]}")
                raise HTTPException(500, "Failed to save feedback")
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Feedback submit error: {e}")
        raise HTTPException(500, "Failed to save feedback")

    log.info(f"Feedback received: {req.rating}/5 from {user.get('email', '?')} for {req.feature} (job: {req.job_id[:20]})")
    return {"status": "ok"}


@app.get("/api/admin/feedback")
async def get_all_feedback(user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS and user.get("plan") != "admin":
        raise HTTPException(403, "Admin access required")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        return {"feedback": []}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/feedback?order=created_at.desc&limit=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                avg_rating = sum(f.get("rating", 0) for f in data) / len(data) if data else 0
                return {
                    "feedback": data,
                    "total": len(data),
                    "avg_rating": round(avg_rating, 2),
                }
    except Exception as e:
        log.error(f"Feedback fetch error: {e}")

    return {"feedback": [], "total": 0, "avg_rating": 0}


# ─── Startup: seed accounts ──────────────────────────────────────────────────

SEED_ACCOUNTS = {
    "omatic657@gmail.com": {"plan": "admin", "role": "admin"},
    "alwakmyhem@gmail.com": {"plan": "pro", "role": "user"},
}


@app.on_event("startup")
async def seed_profiles():
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        log.warning("Supabase not configured, skipping profile seeding")
        return

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if users_resp.status_code != 200:
                log.warning(f"Could not list users for seeding (status {users_resp.status_code})")
                return

            users_data = users_resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data

            for email, profile in SEED_ACCOUNTS.items():
                user_id = None
                for u in user_list:
                    if u.get("email") == email:
                        user_id = u["id"]
                        break
                if not user_id:
                    log.info(f"Seed user {email} not found in auth yet (will be seeded on first login)")
                    continue

                resp = await client.post(
                    f"{SUPABASE_URL}/rest/v1/profiles",
                    headers={
                        "apikey": svc_key,
                        "Authorization": f"Bearer {svc_key}",
                        "Content-Type": "application/json",
                        "Prefer": "resolution=merge-duplicates",
                    },
                    json={"id": user_id, "plan": profile["plan"], "role": profile["role"]},
                )
                log.info(f"Seeded {email} -> {profile['plan']} (status {resp.status_code})")
    except Exception as e:
        log.warning(f"Profile seeding failed: {e}")


# ─── Thumbnail System ─────────────────────────────────────────────────────────

THUMBNAIL_DIR = Path("thumbnails")
THUMBNAIL_DIR.mkdir(exist_ok=True)
THUMBNAIL_UPLOAD_DIR = THUMBNAIL_DIR / "library"
THUMBNAIL_UPLOAD_DIR.mkdir(exist_ok=True)
THUMBNAIL_OUTPUT_DIR = THUMBNAIL_DIR / "generated"
THUMBNAIL_OUTPUT_DIR.mkdir(exist_ok=True)

THUMBNAIL_RUNPOD_HOST = os.getenv("THUMBNAIL_RUNPOD_HOST", "root@69.30.85.41")
THUMBNAIL_RUNPOD_SSH_PORT = os.getenv("THUMBNAIL_RUNPOD_SSH_PORT", "22118")
RUNPOD_SSH = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {THUMBNAIL_RUNPOD_SSH_PORT} {THUMBNAIL_RUNPOD_HOST}"
RUNPOD_TRAINING_DIR = "/workspace/thumbnail_training/images"
LORA_NAME = "nyptid_thumbnails.safetensors"


def _thumbnail_scp_cmd(local_path: str, remote_path: str) -> str:
    return (
        f"scp -o StrictHostKeyChecking=no -P {THUMBNAIL_RUNPOD_SSH_PORT} "
        f"\"{local_path}\" {THUMBNAIL_RUNPOD_HOST}:{remote_path}"
    )


def _thumbnail_target_user_host() -> tuple[str, str]:
    if "@" in THUMBNAIL_RUNPOD_HOST:
        user, host = THUMBNAIL_RUNPOD_HOST.split("@", 1)
        return user, host
    return "root", THUMBNAIL_RUNPOD_HOST


def _sftp_mkdir_p(sftp, remote_dir: str):
    parts = [p for p in remote_dir.strip("/").split("/") if p]
    cur = "/"
    for part in parts:
        cur = f"{cur}{part}/"
        try:
            sftp.stat(cur)
        except Exception:
            sftp.mkdir(cur)


def _sync_file_to_runpod_blocking(local_path: str, remote_path: str) -> tuple[bool, str]:
    if not Path(local_path).exists():
        return False, "local file missing"

    if paramiko is not None:
        user, host = _thumbnail_target_user_host()
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(THUMBNAIL_RUNPOD_SSH_PORT),
                username=user,
                timeout=20,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            remote_parent = str(Path(remote_path).parent).replace("\\", "/")
            _sftp_mkdir_p(sftp, remote_parent)
            sftp.put(local_path, remote_path)
            return True, ""
        except Exception as e:
            log.warning(f"Paramiko thumbnail sync failed, falling back to scp: {e}")
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    cmd = _thumbnail_scp_cmd(local_path, remote_path)
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp failed")[:300]


def _runpod_target_user_host(host_value: str) -> tuple[str, str]:
    if "@" in host_value:
        user, host = host_value.split("@", 1)
        return user, host
    return "root", host_value


def _run_remote_cmd_blocking(host_value: str, port_value: str, remote_cmd: str) -> tuple[bool, str]:
    user, host = _runpod_target_user_host(host_value)
    if paramiko is not None:
        client = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            _, stdout, stderr = client.exec_command(remote_cmd, timeout=1800)
            out = stdout.read().decode(errors="ignore")
            err = stderr.read().decode(errors="ignore")
            code = stdout.channel.recv_exit_status()
            if code == 0:
                return True, out
            return False, (err or out or f"remote exit {code}")[:500]
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if client:
                    client.close()
            except Exception:
                pass

    ssh_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {port_value} "
        f"{host_value} \"{remote_cmd}\""
    )
    proc = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, proc.stdout[:500]
    return False, (proc.stderr or proc.stdout or "ssh failed")[:500]


def _download_file_from_runpod_blocking(host_value: str, port_value: str, remote_path: str, local_path: str) -> tuple[bool, str]:
    local_parent = Path(local_path).parent
    local_parent.mkdir(parents=True, exist_ok=True)
    user, host = _runpod_target_user_host(host_value)

    if paramiko is not None:
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            sftp.get(remote_path, local_path)
            return True, ""
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    scp_cmd = (
        f"scp -o StrictHostKeyChecking=no -P {port_value} "
        f"{host_value}:{remote_path} \"{local_path}\""
    )
    proc = subprocess.run(scp_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp download failed")[:500]


def _upload_file_to_runpod_blocking(host_value: str, port_value: str, local_path: str, remote_path: str) -> tuple[bool, str]:
    if not Path(local_path).exists():
        return False, "local file missing"
    user, host = _runpod_target_user_host(host_value)

    if paramiko is not None:
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            remote_parent = str(Path(remote_path).parent).replace("\\", "/")
            _sftp_mkdir_p(sftp, remote_parent)
            sftp.put(local_path, remote_path)
            return True, ""
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    scp_cmd = (
        f"scp -o StrictHostKeyChecking=no -P {port_value} "
        f"\"{local_path}\" {host_value}:{remote_path}"
    )
    proc = subprocess.run(scp_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp upload failed")[:500]


async def sync_thumbnail_to_runpod(local_path: str) -> tuple[bool, str]:
    """SCP a thumbnail to RunPod's training images directory."""
    try:
        remote_path = f"{RUNPOD_TRAINING_DIR.rstrip('/')}/{Path(local_path).name}"
        ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, local_path, remote_path)
        if ok:
            log.info(f"Synced {Path(local_path).name} to RunPod training dir")
            return True, ""
        log.warning(f"RunPod sync failed: {err}")
        return False, err
    except Exception as e:
        err = str(e)
        log.warning(f"RunPod thumbnail sync error: {err}")
        return False, err


async def check_lora_status() -> dict:
    """Check if the LoRA trainer is running and if a trained LoRA exists on RunPod."""
    local_count = len([p for p in THUMBNAIL_UPLOAD_DIR.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}]) if THUMBNAIL_UPLOAD_DIR.exists() else 0
    try:
        cmd = (
            f"{RUNPOD_SSH} '"
            "ls -la /workspace/ComfyUI/models/loras/nyptid_thumbnails.safetensors 2>/dev/null; "
            "cat /workspace/thumbnail_training/output/training_state.json 2>/dev/null; "
            "test -f /workspace/thumbnail_training/output/training.lock && echo TRAINING_ACTIVE || echo TRAINING_IDLE; "
            "ls /workspace/thumbnail_training/images/ 2>/dev/null | wc -l"
            "'"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        output = stdout.decode()

        has_lora = "nyptid_thumbnails.safetensors" in output and "No such file" not in output
        is_training = "TRAINING_ACTIVE" in output
        lines = output.strip().split("\n")
        image_count = 0
        for line in lines:
            if line.strip().isdigit():
                image_count = int(line.strip())

        state = {}
        try:
            for line in lines:
                if line.strip().startswith("{"):
                    state = json.loads(line.strip())
                    break
        except Exception:
            pass

        return {
            "lora_available": has_lora,
            "is_training": is_training,
            "total_images": image_count,
            "local_library_images": local_count,
            "trained_images": state.get("image_count", 0),
            "version": state.get("version", 0),
            "last_train": state.get("last_train", 0),
        }
    except Exception as e:
        log.warning(f"LoRA status check failed: {e}")
        return {
            "lora_available": False,
            "is_training": False,
            "total_images": 0,
            "local_library_images": local_count,
            "trained_images": 0,
            "version": 0,
            "last_train": 0,
        }


@app.get("/api/thumbnails/training-status")
async def training_status(user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    return await check_lora_status()


@app.post("/api/thumbnails/sync-library")
async def sync_thumbnail_library(user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    files = [p for p in THUMBNAIL_UPLOAD_DIR.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}]
    if not files:
        return {"status": "no_files", "queued": 0, "synced": 0, "failed": 0}

    synced = 0
    failed = 0
    failed_files = []
    for p in files:
        ok, err = await sync_thumbnail_to_runpod(str(p))
        if ok:
            synced += 1
        else:
            failed += 1
            failed_files.append({"file": p.name, "error": err[:180]})

    status = "complete" if failed == 0 else ("partial" if synced > 0 else "failed")
    log.info(f"Thumbnail library sync finished: status={status}, synced={synced}, failed={failed}, total={len(files)}")
    return {
        "status": status,
        "queued": len(files),
        "synced": synced,
        "failed": failed,
        "failed_files": failed_files[:10],
    }


@app.post("/api/thumbnails/upload")
async def upload_thumbnails(
    files: list[UploadFile] = File(...),
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(require_auth),
):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    saved = []
    for file in files:
        if not file.filename:
            continue
        ext = Path(file.filename).suffix.lower()
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        ts = int(time.time() * 1000)
        safe_name = str(ts) + "_" + str(random.randint(1000, 9999)) + ext
        dest = THUMBNAIL_UPLOAD_DIR / safe_name
        with open(dest, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        saved.append({
            "id": safe_name,
            "name": file.filename,
            "size": dest.stat().st_size,
            "url": "/api/thumbnails/library/" + safe_name,
        })
        if background_tasks:
            background_tasks.add_task(sync_thumbnail_to_runpod, str(dest))
    return {"uploaded": len(saved), "files": saved}


@app.get("/api/thumbnails/library")
async def list_thumbnails(user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    files = []
    for f in sorted(THUMBNAIL_UPLOAD_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if f.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
            files.append({
                "id": f.name,
                "name": f.name,
                "size": f.stat().st_size,
                "url": f"/api/thumbnails/library/{f.name}",
                "created_at": f.stat().st_mtime,
            })
    return {"files": files, "total": len(files)}


class ThumbnailFeedbackRequest(BaseModel):
    generation_id: str
    accepted: bool


@app.post("/api/thumbnails/feedback")
async def thumbnail_feedback(req: ThumbnailFeedbackRequest, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    await _mark_training_feedback(
        req.generation_id,
        accepted=req.accepted,
        user_id=user.get("id", ""),
        event="thumbnail_feedback",
    )
    return {"ok": True, "generation_id": req.generation_id, "status": "accepted" if req.accepted else "rejected"}


@app.get("/api/thumbnails/library/{filename}")
async def serve_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    path = THUMBNAIL_UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Thumbnail not found")
    mime = "image/png" if path.suffix == ".png" else "image/jpeg" if path.suffix in (".jpg", ".jpeg") else "image/webp"
    return FileResponse(str(path), media_type=mime)


@app.delete("/api/thumbnails/library/{filename}")
async def delete_thumbnail(filename: str, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    path = THUMBNAIL_UPLOAD_DIR / filename
    if path.exists():
        path.unlink()
    return {"status": "deleted"}


@app.get("/api/thumbnails/generated/{filename}")
async def serve_generated_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    path = THUMBNAIL_OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Generated thumbnail not found")
    mime = "image/png"
    return FileResponse(str(path), media_type=mime)


THUMBNAIL_ANALYSIS_PROMPT = """You are an elite YouTube thumbnail design strategist and art director. You analyze what makes thumbnails get clicks and generate precise SDXL image prompts to create thumbnails that outperform human designers.

You understand:
- Color psychology (red/yellow = urgency, blue = trust, contrast = attention)
- Face/emotion science (shocked expressions get 2-3x CTR)
- Text placement rules (big bold text, 3-5 words max, high contrast)
- Composition (rule of thirds, leading lines, depth)
- Platform-specific optimization (1920x1080 output, mobile-first readability)

When given a video description, style reference, or sketch, you produce a detailed SDXL image generation prompt that will create a click-worthy thumbnail.

Output MUST be valid JSON:
{
  "prompt": "Detailed SDXL prompt for the thumbnail image. Include: subject, composition, lighting, colors, text elements, style, camera angle. Be extremely specific about every visual element. 8k quality, professional YouTube thumbnail.",
  "negative_prompt": "Elements to avoid in the generation",
  "title_text": "The 3-5 word overlay text for the thumbnail (if applicable, empty string if none)",
  "style_notes": "Brief description of the design strategy being used"
}"""


THUMBNAIL_STYLE_TRANSFER_PROMPT = """You are an elite YouTube thumbnail design strategist. You will receive:
1. A description of the STYLE to emulate (from a reference thumbnail the user likes)
2. A description of what the NEW thumbnail should show

Your job: merge the visual style of the reference with the new content to create a prompt that produces a thumbnail in that exact style but with new content.

Analyze the style reference for: color palette, composition style, lighting mood, text treatment, overall aesthetic.
Then apply that exact style to the new content.

Output MUST be valid JSON:
{
  "prompt": "Detailed SDXL prompt combining the reference style with new content. 8k quality, professional YouTube thumbnail, 1920x1080 composition.",
  "negative_prompt": "Elements to avoid",
  "title_text": "The overlay text for this thumbnail (3-5 words, empty string if none)",
  "style_notes": "How the reference style was adapted"
}"""


THUMBNAIL_SCREENSHOT_PROMPT = """You are an elite YouTube thumbnail analyst and designer. The user has provided a description of their YouTube channel's existing thumbnails (or a description of what has worked for them before).

Your job: identify the patterns that made those thumbnails successful, then generate a NEW thumbnail prompt that follows the same winning formula but feels fresh and evolved.

Analyze for: recurring color schemes, text styles, composition patterns, emotional triggers, branding elements.

Output MUST be valid JSON:
{
  "prompt": "Detailed SDXL prompt for a new thumbnail following the user's proven style. 8k quality, professional YouTube thumbnail, 1920x1080.",
  "negative_prompt": "Elements to avoid",
  "title_text": "Overlay text (3-5 words, empty if none)",
  "style_notes": "Pattern analysis of what works for this channel",
  "patterns_detected": ["pattern1", "pattern2", "pattern3"]
}"""


class ThumbnailGenerateRequest(BaseModel):
    mode: str  # "describe", "style_transfer", "screenshot_analysis"
    description: str
    style_reference_id: str = ""
    sketch_image_id: str = ""
    screenshot_description: str = ""


async def _generate_thumbnail_prompt(req: ThumbnailGenerateRequest) -> dict:
    if req.mode == "style_transfer":
        system_prompt = THUMBNAIL_STYLE_TRANSFER_PROMPT
        user_msg = f"STYLE REFERENCE: {req.description}\n\nNEW THUMBNAIL CONTENT: {req.screenshot_description or req.description}"
    elif req.mode == "screenshot_analysis":
        system_prompt = THUMBNAIL_SCREENSHOT_PROMPT
        user_msg = f"CHANNEL THUMBNAIL ANALYSIS: {req.screenshot_description or req.description}\n\nNEW VIDEO TO MAKE THUMBNAIL FOR: {req.description}"
    else:
        system_prompt = THUMBNAIL_ANALYSIS_PROMPT
        user_msg = f"Create a viral YouTube thumbnail for: {req.description}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.7,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in thumbnail AI response")
        return json.loads(content[start:end])


async def _check_lora_exists() -> bool:
    """Quick check if the trained LoRA file exists in ComfyUI."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(COMFYUI_URL + "/object_info/LoraLoader")
            if resp.status_code == 200:
                data = resp.json()
                lora_info = data.get("LoraLoader", {}).get("input", {}).get("required", {})
                lora_names = lora_info.get("lora_name", [[]])[0]
                return LORA_NAME in lora_names
    except Exception:
        pass
    return False


async def _enforce_thumbnail_1080(output_path: str) -> str:
    """Force final thumbnail image to exactly 1920x1080."""
    fixed_out = str(Path(output_path).with_name(Path(output_path).stem + "_1920x1080.png"))
    cmd = [
        "ffmpeg", "-y",
        "-i", output_path,
        "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1",
        fixed_out,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Failed to enforce 1920x1080 thumbnail: " + stderr.decode()[-200:])
    fixed = Path(fixed_out)
    if not fixed.exists() or fixed.stat().st_size == 0:
        raise RuntimeError("1920x1080 thumbnail output missing")
    Path(output_path).unlink(missing_ok=True)
    fixed.rename(output_path)
    return output_path


async def _generate_thumbnail_image(prompt: str, negative_prompt: str, output_path: str, style_ref_path: str = "") -> str:
    """Generate a thumbnail using SDXL via ComfyUI, with trained LoRA if available."""
    use_lora = await _check_lora_exists()
    if use_lora:
        log.info("Using trained thumbnail LoRA for generation")

    model_source = ["1", 0]
    clip_source = ["1", 1]
    if use_lora:
        model_source = ["99", 0]
        clip_source = ["99", 1]

    lora_node = {}
    if use_lora:
        lora_node = {
            "99": {
                "class_type": "LoraLoader",
                "inputs": {
                    "lora_name": LORA_NAME,
                    "strength_model": 0.75,
                    "strength_clip": 0.75,
                    "model": ["1", 0],
                    "clip": ["1", 1],
                },
            },
        }

    if style_ref_path and Path(style_ref_path).exists():
        uploaded_name = await _upload_image_to_comfyui(style_ref_path)
        workflow = {
            "1": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            **lora_node,
            "2": {
                "class_type": "LoadImage",
                "inputs": {"image": uploaded_name},
            },
            "3": {
                "class_type": "VAEEncode",
                "inputs": {"pixels": ["2", 0], "vae": ["1", 2]},
            },
            "4": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": prompt, "clip": clip_source},
            },
            "5": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": negative_prompt, "clip": clip_source},
            },
            "6": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 30,
                    "cfg": 7.0,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": 0.65,
                    "model": model_source,
                    "positive": ["4", 0],
                    "negative": ["5", 0],
                    "latent_image": ["3", 0],
                },
            },
            "7": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["6", 0], "vae": ["1", 2]},
            },
            "8": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_thumb", "images": ["7", 0]},
            },
        }
    else:
        workflow = {
            "1": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            **lora_node,
            "2": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": 1920, "height": 1080, "batch_size": 1},
            },
            "3": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": prompt, "clip": clip_source},
            },
            "4": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": negative_prompt, "clip": clip_source},
            },
            "5": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 30,
                    "cfg": 7.5,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": 1.0,
                    "model": model_source,
                    "positive": ["3", 0],
                    "negative": ["4", 0],
                    "latent_image": ["2", 0],
                },
            },
            "6": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["5", 0], "vae": ["1", 2]},
            },
            "7": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_thumb", "images": ["6", 0]},
            },
        }

    result = await _run_comfyui_workflow(workflow, "8" if style_ref_path else "7", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


@app.post("/api/thumbnails/generate")
async def generate_thumbnail(req: ThumbnailGenerateRequest, background_tasks: BackgroundTasks, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")

    job_id = f"thumb_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "type": "thumbnail",
        "mode": req.mode,
        "created_at": time.time(),
    }

    async def _run_thumbnail_pipeline():
        try:
            jobs[job_id]["status"] = "analyzing"
            jobs[job_id]["progress"] = 10
            log.info(f"[{job_id}] Thumbnail gen: mode={req.mode}")

            ai_result = await _generate_thumbnail_prompt(req)
            thumb_prompt = ai_result.get("prompt", req.description)
            thumb_negative = ai_result.get("negative_prompt", NEGATIVE_PROMPT)
            title_text = ai_result.get("title_text", "")
            style_notes = ai_result.get("style_notes", "")

            jobs[job_id]["status"] = "generating"
            jobs[job_id]["progress"] = 30
            jobs[job_id]["ai_analysis"] = {
                "title_text": title_text,
                "style_notes": style_notes,
                "patterns": ai_result.get("patterns_detected", []),
            }

            style_ref_path = ""
            if req.style_reference_id:
                ref_path = THUMBNAIL_UPLOAD_DIR / req.style_reference_id
                if ref_path.exists():
                    style_ref_path = str(ref_path)

            output_name = f"{job_id}.png"
            output_path = str(THUMBNAIL_OUTPUT_DIR / output_name)
            await _generate_thumbnail_image(thumb_prompt, thumb_negative, output_path, style_ref_path)

            if title_text:
                try:
                    safe_title = title_text.replace("'", "'\\''").replace(":", "\\:")
                    titled_out = str(THUMBNAIL_OUTPUT_DIR / (job_id + "_titled.png"))
                    vf_filter = (
                        "drawtext=text='" + safe_title + "':"
                        "fontsize=72:fontcolor=white:borderw=5:bordercolor=black:"
                        "x=(w-text_w)/2:y=h*0.78:"
                        "fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                    )
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", output_path,
                        "-vf", vf_filter,
                        titled_out,
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    _, stderr = await proc.communicate()
                    if proc.returncode == 0:
                        titled_path = Path(titled_out)
                        if titled_path.exists():
                            Path(output_path).unlink(missing_ok=True)
                            titled_path.rename(output_path)
                except Exception as e:
                    log.warning(f"[{job_id}] Title overlay failed, using without text: {e}")

            await _enforce_thumbnail_1080(output_path)
            thumb_gen_id = await _save_training_candidate(
                thumb_prompt,
                output_path,
                template="thumbnail",
                source="thumbnail_ai",
                metadata={
                    "mode": req.mode,
                    "title_text": title_text,
                    "user_id": user.get("id", ""),
                },
            )

            jobs[job_id]["status"] = "complete"
            jobs[job_id]["progress"] = 100
            jobs[job_id]["output_file"] = output_name
            jobs[job_id]["output_url"] = f"/api/thumbnails/generated/{output_name}"
            jobs[job_id]["generation_id"] = thumb_gen_id
            log.info(f"[{job_id}] Thumbnail COMPLETE: {output_name}")

        except Exception as e:
            log.error(f"[{job_id}] Thumbnail pipeline failed: {e}", exc_info=True)
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)

    background_tasks.add_task(_run_thumbnail_pipeline)
    return {"status": "accepted", "job_id": job_id}


# ─── Product Demo Video Pipeline ──────────────────────────────────────────────

DEMO_DIR = Path("demo_uploads")
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
    import base64, gc

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


COMPRESS_THRESHOLD_MB = 50


async def compress_video_if_needed(video_path: str, job_id: str, label: str = "demo") -> str:
    """Compress video to 720p if file exceeds COMPRESS_THRESHOLD_MB. Returns path (possibly new)."""
    file_size_mb = Path(video_path).stat().st_size / (1024 * 1024)
    if file_size_mb <= COMPRESS_THRESHOLD_MB:
        return video_path

    log.info(f"[{job_id}] {label} video is {file_size_mb:.0f}MB (>{COMPRESS_THRESHOLD_MB}MB), compressing to 720p...")
    jobs[job_id]["status"] = f"compressing_{label}"
    jobs[job_id]["progress"] = jobs[job_id].get("progress", 0)
    jobs[job_id]["compress_info"] = {
        "label": label,
        "original_size_mb": round(file_size_mb, 1),
        "target": "720p",
    }

    compressed_path = video_path.rsplit(".", 1)[0] + "_compressed.mp4"
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", "scale=-2:720",
        "-c:v", "libx264", "-preset", "fast", "-crf", "28",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        compressed_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    _, stderr_data = await proc.communicate()

    if proc.returncode == 0 and Path(compressed_path).exists():
        new_size_mb = Path(compressed_path).stat().st_size / (1024 * 1024)
        log.info(f"[{job_id}] {label} compressed: {file_size_mb:.0f}MB -> {new_size_mb:.0f}MB")
        jobs[job_id]["compress_info"]["compressed_size_mb"] = round(new_size_mb, 1)
        Path(video_path).unlink(missing_ok=True)
        return compressed_path
    else:
        log.warning(f"[{job_id}] {label} compression failed, using original. stderr: {stderr_data.decode()[-300:]}")
        Path(compressed_path).unlink(missing_ok=True)
        return video_path


async def run_demo_pipeline(job_id: str, demo_path: str, ref_path: str, face_path: str,
                            product_name: str, reference_notes: str,
                            pip_position: str = "bottom-right", voice_id: str = ""):
    """Full product demo video generation pipeline."""
    try:
        ref_style = ""
        if ref_path and Path(ref_path).exists():
            jobs[job_id]["status"] = "analyzing_reference"
            jobs[job_id]["progress"] = 3
            log.info(f"[{job_id}] Analyzing reference video for style...")
            ref_analysis = await analyze_screen_recording(ref_path)
            ref_style = (
                f"Match the style, pacing, and energy of this reference video: "
                f"{ref_analysis.get('description', '')}. "
                f"Duration: {ref_analysis.get('duration', 0):.0f}s, "
                f"{ref_analysis.get('frame_count', 0)} key frames analyzed."
            )
            Path(ref_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 5
        log.info(f"[{job_id}] Demo pipeline: analyzing demo video...")

        analysis = await analyze_screen_recording(demo_path)
        jobs[job_id]["analysis"] = {
            "duration": analysis["duration"],
            "frame_count": analysis["frame_count"]
        }
        jobs[job_id]["progress"] = 20
        log.info(f"[{job_id}] Screen analyzed: {analysis['frame_count']} frames, {analysis['duration']:.1f}s")

        jobs[job_id]["status"] = "scripting"
        jobs[job_id]["progress"] = 25
        log.info(f"[{job_id}] Generating demo script...")

        full_ref_notes = reference_notes
        if ref_style:
            full_ref_notes = ref_style + ("\n" + reference_notes if reference_notes else "")
        script_data = await generate_demo_script(analysis, product_name, full_ref_notes)
        jobs[job_id]["script"] = script_data
        jobs[job_id]["progress"] = 40
        log.info(f"[{job_id}] Script ready: {len(script_data.get('segments', []))} segments")

        jobs[job_id]["status"] = "generating_voice"
        jobs[job_id]["progress"] = 45
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(seg.get("text", seg.get("narration", "")) for seg in script_data.get("segments", []))
        audio_path = str(TEMP_DIR / (job_id + "_demo_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template="motivation", override_voice_id=voice_id)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])
        jobs[job_id]["progress"] = 60

        # Keep demo narration aligned to the uploaded recording duration without rushed speech.
        target_demo_dur = float(analysis.get("duration", 0.0) or 0.0)
        source_audio_dur = _probe_audio_duration_seconds(audio_path)
        if target_demo_dur > 0.2 and source_audio_dur > 0.2:
            speed_ratio = source_audio_dur / target_demo_dur
            if source_audio_dur < (target_demo_dur - 0.08):
                fit_audio_path = str(TEMP_DIR / (job_id + "_demo_voice_fit.mp3"))
                # Only slow down short narration; never speed up long narration (sounds rushed).
                gentle_ratio = max(speed_ratio, 0.88)
                atempo_chain = _build_atempo_filter_chain(gentle_ratio)
                fit_cmd = [
                    "ffmpeg", "-y", "-i", audio_path,
                    "-af", f"{atempo_chain},apad=pad_dur={target_demo_dur + 0.25:.3f}",
                    "-t", f"{target_demo_dur:.3f}",
                    "-c:a", "libmp3lame", "-b:a", "192k",
                    fit_audio_path,
                ]
                fit_proc = await asyncio.create_subprocess_exec(
                    *fit_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                _, fit_err = await fit_proc.communicate()
                if fit_proc.returncode == 0 and Path(fit_audio_path).exists():
                    Path(audio_path).unlink(missing_ok=True)
                    audio_path = fit_audio_path
                    if word_timings:
                        remapped = []
                        for wt in word_timings:
                            start = max(0.0, float(wt.get("start", 0.0)) / gentle_ratio)
                            end = max(start + 0.01, float(wt.get("end", start)) / gentle_ratio)
                            if start <= target_demo_dur + 0.15:
                                remapped.append({
                                    "word": wt.get("word", ""),
                                    "start": start,
                                    "end": min(end, target_demo_dur + 0.15),
                                })
                        word_timings = remapped
                    jobs[job_id]["audio_sync"] = {
                        "source_duration": round(source_audio_dur, 3),
                        "target_duration": round(target_demo_dur, 3),
                        "speed_ratio": round(gentle_ratio, 5),
                        "mode": "slowdown_and_pad",
                    }
                    log.info(f"[{job_id}] Demo voiceover fit to video duration ({source_audio_dur:.2f}s -> {target_demo_dur:.2f}s)")
                else:
                    log.warning(f"[{job_id}] Demo voiceover duration-fit failed, using original audio: {(fit_err.decode(errors='ignore')[-200:])}")
            else:
                jobs[job_id]["audio_sync"] = {
                    "source_duration": round(source_audio_dur, 3),
                    "target_duration": round(target_demo_dur, 3),
                    "speed_ratio": 1.0,
                    "mode": "no_speedup",
                }

        subtitle_path = None
        if word_timings:
            demo_w, demo_h = 1920, 1080
            try:
                probe_cmd = [
                    "ffprobe", "-v", "error", "-show_entries", "stream=width,height",
                    "-of", "csv=p=0:s=x", demo_path
                ]
                p = await asyncio.create_subprocess_exec(
                    *probe_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                out, _ = await p.communicate()
                dims = out.decode().strip().split("\n")[0]
                if "x" in dims:
                    demo_w, demo_h = int(dims.split("x")[0]), int(dims.split("x")[1])
            except Exception:
                pass
            subtitle_path = str(TEMP_DIR / (job_id + "_demo_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, video_width=demo_w, video_height=demo_h)

        jobs[job_id]["status"] = "generating_face"
        jobs[job_id]["progress"] = 65

        talking_head_path = None
        has_face = False

        try:
            if face_path and Path(face_path).exists():
                log.info(f"[{job_id}] Generating talking head animation...")
                talking_head_path = str(TEMP_DIR / (job_id + "_talking_head.mp4"))
                await generate_talking_head(face_path, audio_path, talking_head_path)
                has_face = True
                log.info(f"[{job_id}] Talking head generated")
            else:
                log.info(f"[{job_id}] Face PiP disabled (no face image uploaded)")
                talking_head_path = None
                has_face = False
            jobs[job_id]["progress"] = 80
        except Exception as face_err:
            log.warning(f"[{job_id}] Talking head failed ({face_err}), continuing without face PiP")
            talking_head_path = None
            has_face = False
            jobs[job_id]["progress"] = 80

        jobs[job_id]["status"] = "compositing"
        jobs[job_id]["progress"] = 85
        log.info(f"[{job_id}] Compositing demo video...")

        output_filename = f"demo_{job_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)

        if has_face and talking_head_path and Path(talking_head_path).exists():
            await composite_demo_video(
                demo_path, talking_head_path, audio_path,
                output_path, subtitle_path=subtitle_path,
                pip_position=pip_position
            )
        else:
            sub_filter = ""
            if subtitle_path and Path(subtitle_path).exists():
                sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
                sub_filter = f"-vf ass={sub_abs}"
            cmd = ["ffmpeg", "-y", "-i", demo_path, "-i", audio_path]
            if sub_filter:
                cmd += ["-vf", f"ass={sub_abs}"]
            cmd += [
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                output_path
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if not Path(output_path).exists():
                cmd_simple = ["ffmpeg", "-y", "-i", demo_path, "-i", audio_path,
                              "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                              "-c:a", "aac", "-b:a", "192k", "-shortest", output_path]
                proc = await asyncio.create_subprocess_exec(
                    *cmd_simple, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                await proc.communicate()

        jobs[job_id]["progress"] = 95

        if talking_head_path:
            Path(talking_head_path).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["output_url"] = f"/api/download/{output_filename}"
        log.info(f"[{job_id}] Demo COMPLETE: {output_filename}")

    except Exception as e:
        log.error(f"[{job_id}] Demo pipeline failed: {e}", exc_info=True)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)

    finally:
        Path(demo_path).unlink(missing_ok=True)
        if ref_path and Path(ref_path).exists():
            Path(ref_path).unlink(missing_ok=True)
        if face_path and Path(face_path).exists():
            Path(face_path).unlink(missing_ok=True)


@app.get("/api/voices")
async def list_voices():
    """List available ElevenLabs voices for the user to choose from."""
    if not ELEVENLABS_API_KEY:
        raise HTTPException(500, "ElevenLabs API key not configured")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            "https://api.elevenlabs.io/v1/voices",
            headers={"xi-api-key": ELEVENLABS_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
    voices = []
    for v in data.get("voices", []):
        voices.append({
            "voice_id": v["voice_id"],
            "name": v.get("name", "Unknown"),
            "category": v.get("category", ""),
            "description": v.get("labels", {}).get("description", ""),
            "gender": v.get("labels", {}).get("gender", ""),
            "accent": v.get("labels", {}).get("accent", ""),
            "age": v.get("labels", {}).get("age", ""),
            "preview_url": v.get("preview_url", ""),
        })
    return {"voices": voices}


@app.post("/api/voices/preview")
async def preview_voice(request: Request):
    """Generate a short voice preview with a given voice_id."""
    if not ELEVENLABS_API_KEY:
        raise HTTPException(500, "ElevenLabs API key not configured")
    body = await request.json()
    voice_id = body.get("voice_id", "")
    if not voice_id:
        raise HTTPException(400, "voice_id required")
    preview_text = body.get("text", "Hey there! This is a quick preview of what I sound like. Pretty cool, right?")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
            json={
                "text": preview_text,
                "model_id": "eleven_turbo_v2_5",
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.75, "style": 0.3},
            },
        )
        if resp.status_code != 200:
            raise HTTPException(resp.status_code, f"ElevenLabs error: {resp.text[:200]}")
    from fastapi.responses import Response
    return Response(content=resp.content, media_type="audio/mpeg",
                    headers={"Content-Disposition": f"inline; filename=preview_{voice_id}.mp3"})


@app.post("/api/demo")
async def create_demo_video(
    background_tasks: BackgroundTasks,
    demo_video: UploadFile = File(...),
    reference_video: Optional[UploadFile] = File(None),
    face_image: Optional[UploadFile] = File(None),
    product_name: str = Form(""),
    reference_notes: str = Form(""),
    pip_position: str = Form("bottom-right"),
    voice_id: str = Form(""),
    request: Request = None,
):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")

    user_email = user.get("email", "")
    user_plan = user.get("plan", "free")
    is_admin = user_email in ADMIN_EMAILS or user_plan == "admin"
    if not is_admin:
        raise HTTPException(403, "Product Demo is admin-only right now.")
    has_demo = user_plan == "demo_pro" or is_admin
    if not has_demo:
        raise HTTPException(403, "Product Demo requires the Demo Pro plan ($150/mo). Upgrade to access this feature.")

    job_id = f"demo_{int(time.time()*1000)}_{random.randint(1000,9999)}"

    demo_ext = Path(demo_video.filename or "video.mp4").suffix or ".mp4"
    demo_path = str(DEMO_DIR / (job_id + "_demo" + demo_ext))
    with open(demo_path, "wb") as f:
        while chunk := await demo_video.read(1024 * 1024):
            f.write(chunk)

    ref_path = ""
    if reference_video and reference_video.filename:
        ref_ext = Path(reference_video.filename).suffix or ".mp4"
        ref_path = str(DEMO_DIR / (job_id + "_reference" + ref_ext))
        with open(ref_path, "wb") as f:
            while chunk := await reference_video.read(1024 * 1024):
                f.write(chunk)

    face_path = ""
    if face_image and face_image.filename:
        face_ext = Path(face_image.filename).suffix or ".png"
        face_path = str(DEMO_DIR / (job_id + "_face" + face_ext))
        with open(face_path, "wb") as f:
            while chunk := await face_image.read(1024 * 1024):
                f.write(chunk)

    jobs[job_id] = {
        "status": "queued", "progress": 0, "type": "demo",
        "product_name": product_name,
        "created_at": time.time()
    }

    background_tasks.add_task(
        run_demo_pipeline, job_id, demo_path, ref_path, face_path,
        product_name, reference_notes, pip_position, voice_id
    )

    return {"status": "accepted", "job_id": job_id}


# ─── Static Files ─────────────────────────────────────────────────────────────

_default_dist_dir = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
dist_dir = Path(os.getenv("FRONTEND_DIST_DIR", str(_default_dist_dir))).resolve()
if dist_dir.exists():
    log.info(f"Serving frontend dist from: {dist_dir}")
    app.mount("/", StaticFiles(directory=str(dist_dir), html=True), name="static")
else:
    log.warning(f"Frontend dist directory not found: {dist_dir}")


if __name__ == "__main__":
    for f in OUTPUT_DIR.iterdir():
        if f.suffix == ".mp4" and f.stat().st_mtime < time.time() - 86400:
            f.unlink(missing_ok=True)
    uvicorn.run("backend:app", host="0.0.0.0", port=8081, reload=True)
