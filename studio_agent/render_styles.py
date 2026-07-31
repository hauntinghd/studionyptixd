"""Canonical render-style registry for Studio Agent shortform production."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from pathlib import Path
import os
import re

PipelineKind = Literal["skeleton_host", "styled_t2i"]

DEFAULT_RENDER_STYLE = "cinematic"
# historical_18th_century rides the same styled_t2i QA/retry contract as
# ultra_realism and is required for the history channels' period look.
#: Styles the product exposes. This was pinned to four while the others were
#: being validated, which left twenty finished styles in the registry that the
#: picker, natural-language selection and the agent could not reach at all.
#: They are not separate machinery - every non-skeleton style runs the same
#: styled_t2i pipeline as ultra_realism and cinematic, so exposing them is a
#: curation decision rather than a code one.
#:
#: Populated after registration at the bottom of this module, because the
#: registry does not exist yet at this point in the file. Narrow it with
#: STUDIO_LAUNCH_RENDER_STYLES (comma-separated keys) to stage a subset again.
LAUNCH_RENDER_STYLE_KEYS: frozenset[str] = frozenset()

#: Styles proven end to end with a tuned QA/retry contract. The rest are
#: reachable but not yet hardened; this set records which is which instead of
#: the distinction being lost once the gate opened.
VALIDATED_RENDER_STYLE_KEYS = frozenset(
    {"skeleton_host", "ultra_realism", "cinematic", "historical_18th_century"}
)

# Explicit spoken/style-picker names must resolve to a registry key before a
# previous session default is considered. This prevents an old Skeleton job
# from bleeding into an unrelated ultra-realistic, historical, or finance job.
STYLE_TEXT_ALIASES: dict[str, tuple[str, ...]] = {
    "skeleton_host": ("skeleton anatomical", "skeleton host", "skeleton style", "skeleton"),
    "cinematic": ("cinematic", "film still", "documentary look"),
    "ultra_realism": ("ultra realism", "ultra-realism", "ultrarealism", "photorealistic", "photo realistic"),
    "historical_18th_century": ("18th century historical", "18th-century historical", "18th century", "1700s historical"),
    "comic_realism": ("comic realism",), "comic_book": ("comic book",),
    "bw_comic": ("b&w comic", "black and white comic", "black-and-white comic"),
    "dark_comic": ("dark comic",), "dark_cartoon": ("dark cartoon",),
    "adult_cartoon": ("adult cartoon",), "cute_anime": ("cute anime", "anime"),
    "studio_ghibli": ("studio ghibli", "ghibli"), "pixar": ("pixar",),
    "claymation": ("claymation", "clay animation"),
    "disney_90s": ("90s disney", "1990s disney"), "simpsons": ("simpsons",),
    "creepy_cartoon_v1": ("creepy cartoon v1", "creepy cartoon 1"),
    "creepy_cartoon_v2": ("creepy cartoon v2", "creepy cartoon 2"),
    "illustrated_book": ("illustrated book", "storybook"), "whiteboard": ("whiteboard",),
    "lego": ("lego", "toy brick"), "minecraft": ("minecraft", "voxel"),
    "low_poly": ("low poly", "low-poly"),
    "hand_drawn_2d": ("2d hand-drawn", "hand drawn 2d", "hand-drawn 2d", "2d hand drawn"),
}

def _default_preview_root() -> Path:
    app_data = os.getenv("APP_DATA_DIR")
    if app_data:
        return Path(app_data) / "studio_agent_style_previews"
    return Path("ref_frames/style_previews")


STYLE_PREVIEW_DIR = Path(os.getenv("STYLE_PREVIEW_DIR", str(_default_preview_root())))
try:
    STYLE_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    from studio_agent.fs_paths import data_root, ensure_dir

    STYLE_PREVIEW_DIR = ensure_dir(data_root() / "studio_agent_style_previews")

STYLE_PREVIEW_VIDEO_DIR = Path(os.getenv("STYLE_PREVIEW_VIDEO_DIR", str(STYLE_PREVIEW_DIR / "video")))
try:
    STYLE_PREVIEW_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    from studio_agent.fs_paths import ensure_dir

    STYLE_PREVIEW_VIDEO_DIR = ensure_dir(STYLE_PREVIEW_DIR / "video")

STYLE_PREVIEW_SEED = 424242
STYLE_PREVIEW_VERSION = "v2"

STYLE_PREVIEW_SCENE = (
    "A single faceless red mannequin hacker seated at a glowing computer workstation in a dark intelligence room, "
    "one hand on the keyboard, one monitor showing abstract green code, cinematic desk light, clean premium composition. "
    "No readable text, no letters, no logos."
)

STYLE_PREVIEW_MOTION = (
    "Subtle premium motion: slow camera push-in, monitor glow flickers softly, the mannequin shifts one hand on the keyboard, "
    "background lights pulse gently. Preserve the exact art style and character identity."
)

STYLE_PREVIEW_SUBJECTS: dict[str, str] = {
    "cinematic": (
        "A non-human faceless red mannequin investigator with smooth plastic surface and no human skin, seated at a "
        "glowing computer workstation in a dark intelligence room, premium cinematic desk light, dramatic shadows, "
        "serious film still energy."
    ),
    "ultra_realism": (
        "A real adult human investigative analyst at a glowing computer workstation in a modern intelligence room, "
        "natural skin, realistic eyes, believable clothing, documentary-grade realism."
    ),
    "historical_18th_century": (
        "An adult 18th-century gentleman scholar alive in the 1700s, powdered hair, linen shirt, waistcoat and coat, "
        "studying papers by candlelight in a period room with old instruments and ledgers."
    ),
    "comic_realism": (
        "An original modern comic protagonist, expressive adult detective with a sharp jacket, seated at a workstation, "
        "grounded proportions with comic-rendered lighting and dramatic panel composition."
    ),
    "comic_book": (
        "An original colorful comic book hero analyst, bold costume-inspired jacket, expressive face, action-panel pose "
        "at a glowing workstation, saturated inks and dynamic comic energy."
    ),
    "bw_comic": (
        "An original black-and-white noir comic detective, trench coat, strong silhouette, seated at a moody workstation, "
        "inked shadows and cross-hatched atmosphere."
    ),
    "dark_comic": (
        "An original dark comic antihero analyst, brooding adult character in a black coat, intense eyes, eerie workstation "
        "lighting, gritty graphic-novel mood."
    ),
    "dark_cartoon": (
        "An original stylized dark cartoon hacker character, oversized expressive eyes, angular hoodie, spooky workstation "
        "in a shadowy room, playful but ominous."
    ),
    "adult_cartoon": (
        "An original mature animated sitcom-style adult analyst, casual jacket, dry expression, seated at a workstation "
        "with clean animated staging."
    ),
    "cute_anime": (
        "An original adult anime tech analyst with bright expressive eyes and soft hair, stylish jacket, seated at a glowing "
        "workstation in a cozy high-tech room."
    ),
    "studio_ghibli": (
        "An original gentle adult inventor-scholar in a hand-painted study, warm lamp light, soft expressive face, papers "
        "and small machines around a humble workstation."
    ),
    "pixar": (
        "An original appealing 3D animated adult inventor character with expressive face and stylized proportions, seated "
        "at a polished workstation with warm cinematic lighting."
    ),
    "claymation": (
        "An original handmade clay stop-motion adult detective figure with visible clay texture, tiny jacket, seated at a "
        "miniature workstation set with practical lights."
    ),
    "disney_90s": (
        "An original 1990s hand-drawn adventure hero analyst, expressive adult face, crisp jacket, animated workstation "
        "scene with classic cel-animation charm."
    ),
    "simpsons": (
        "An original yellow-skin satirical cartoon adult analyst, simple rounded shapes, seated at a workstation in a "
        "clean comedic animated room."
    ),
    "creepy_cartoon_v1": (
        "An original eerie cartoon investigator with long limbs, hollow-eyed expression, hunched at a glowing workstation "
        "inside a strange dim room."
    ),
    "creepy_cartoon_v2": (
        "An original surreal creepy cartoon researcher, warped smile, stylized face, seated at a workstation with odd "
        "dreamlike shadows."
    ),
    "illustrated_book": (
        "An original illustrated storybook scholar, gentle adult character with layered clothing, reading notes beside a "
        "small glowing workstation in a richly illustrated room."
    ),
    "whiteboard": (
        "A simple original adult presenter character drawn in clean marker lines beside a sketched computer workstation, "
        "clear whiteboard explainer composition."
    ),
    "lego": (
        "An original toy brick minifigure detective with tiny jacket and simple face, seated at a brick-built computer "
        "workstation in a miniature investigation room."
    ),
    "minecraft": (
        "An original blocky voxel adventurer analyst, square head and pixel clothing, seated at a block-built computer "
        "workstation in a voxel intelligence room."
    ),
    "low_poly": (
        "An original low-poly adult tech analyst, faceted face and jacket, seated at a geometric computer workstation "
        "with clean angular lighting."
    ),
    "hand_drawn_2d": (
        "An original hand-drawn 2D adult investigator, visible pencil and ink lines, seated at a workstation in a storyboard "
        "panel with expressive pose."
    ),
}

STYLE_PREVIEW_MOTIONS: dict[str, str] = {
    "cinematic": "slow camera push-in, monitor glow flickers softly, mannequin hand shifts on keyboard",
    "ultra_realism": "subtle breathing, small eye movement, fingers tap once, realistic monitor glow",
    "historical_18th_century": "candlelight flickers, scholar turns slightly toward the papers, soft camera drift",
    "comic_realism": "comic-panel push-in, character eyes narrow, light streaks move across the frame",
    "comic_book": "dynamic comic zoom, cape/jacket edge flicks, bold color glow pulses",
    "bw_comic": "noir shadows slide, slow detective glance, cross-hatched lighting shimmer",
    "dark_comic": "moody push-in, eyes glint, background shadows pulse subtly",
    "dark_cartoon": "cartoon blink, monitor glow bounces, spooky room sway",
    "adult_cartoon": "small comedic blink, hand gesture, clean animated camera drift",
    "cute_anime": "soft anime blink, hair moves slightly, gentle glowing particles",
    "studio_ghibli": "warm lamp flicker, soft hand movement, paper edges rustle gently",
    "pixar": "expressive eyebrow raise, hand taps keyboard, polished 3D camera push",
    "claymation": "stop-motion style tiny head turn, practical light flicker, handmade camera nudge",
    "disney_90s": "classic animation blink, hand gesture, warm cel-light shimmer",
    "simpsons": "simple cartoon blink, head tilt, monitor glow flicker",
    "creepy_cartoon_v1": "slow unsettling head tilt, shadows crawl, monitor glow pulses",
    "creepy_cartoon_v2": "surreal blink, warped shadow movement, dreamlike camera drift",
    "illustrated_book": "storybook parallax, lamp flicker, page edges move gently",
    "whiteboard": "marker-line drawing animates subtly, presenter hand moves, simple camera pan",
    "lego": "toy minifigure head turns, brick light flickers, tiny stop-motion movement",
    "minecraft": "blocky head turn, pixel light pulses, stepped camera movement",
    "low_poly": "geometric camera orbit, faceted light shifts, small hand movement",
    "hand_drawn_2d": "pencil-line shimmer, hand-drawn blink, storyboard camera push",
    "skeleton_host": "skeleton eyes shift, bony fingers tap keyboard, glass body catches light",
}


@dataclass(frozen=True)
class RenderStyle:
    key: str
    label: str
    group: str
    prompt_prefix: str
    negative_prompt: str
    pipeline: PipelineKind
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "group": self.group,
            "pipeline": self.pipeline,
            "description": self.description,
        }


_BASE_NEG = (
    "text, watermark, logo, blur, low quality, deformed hands, extra fingers, "
    "multiple heads, child, duplicate people, melted anatomy"
)

_SKELETON_NEG_EXTRA = ""

_STYLED_NEG_SKELETON_BAN = (
    ", anatomical skeleton, ivory bones, exposed ribcage, bone mascot, "
    "translucent glass body shell, skull face, NYPTID skeleton host"
)


def _styled(neg_extra: str = "") -> str:
    return _BASE_NEG + _STYLED_NEG_SKELETON_BAN + neg_extra


RENDER_STYLES: dict[str, RenderStyle] = {}


def _register(style: RenderStyle) -> None:
    RENDER_STYLES[style.key] = style


_register(RenderStyle(
    key="skeleton_host",
    label="Skeleton (Anatomical)",
    group="Niche",
    pipeline="skeleton_host",
    description=(
        "Its own niche: canonical ivory bone + glass-shell host for comparison / explainer shorts. "
        "Pick this art style when you want the skeleton look — same as Cryptic Science format."
    ),
    prompt_prefix="",
    negative_prompt=_BASE_NEG,
))

_register(RenderStyle(
    key="cinematic",
    label="Cinematic",
    group="Realism",
    pipeline="styled_t2i",
    description="Premium documentary / film still — natural lighting, shallow depth, photoreal humans.",
    prompt_prefix=(
        "Cinematic photoreal film still, premium documentary grade, natural skin texture, "
        "physically plausible lighting, shallow depth of field, 35mm lens character, "
        "editorial composition, believable environments. "
    ),
    negative_prompt=_styled(", cartoon, anime, illustration"),
))

_register(RenderStyle(
    key="ultra_realism",
    label="Ultra realism",
    group="Realism",
    pipeline="styled_t2i",
    description="Hyperreal / ultra-detailed photographic render.",
    prompt_prefix=(
        "Ultra-photoreal hyperrealism, 8K detail discipline, pore-level skin texture where appropriate, "
        "accurate materials, studio-grade lighting, razor-sharp focus, no stylization. "
    ),
    negative_prompt=_styled(", cartoon, anime, painterly, illustration"),
))

_register(RenderStyle(
    key="comic_realism",
    label="Comic realism",
    group="Comic",
    pipeline="styled_t2i",
    description="Painterly comic with realistic proportions and lighting.",
    prompt_prefix=(
        "Comic realism illustration — bold inked contours with realistic shading and anatomy, "
        "graphic novel premium grade, dramatic lighting, saturated but grounded colors. "
    ),
    negative_prompt=_styled(),
))

_register(RenderStyle(
    key="comic_book",
    label="Comic book (color)",
    group="Comic",
    pipeline="styled_t2i",
    description="Modern color comic — DC/Marvel dynamic linework (ZeroTier-class).",
    prompt_prefix=(
        "Modern color comic book art, bold black linework, cel-shaded primary colors, "
        "dynamic action framing, speed lines where appropriate, Jim Lee / Francis Manapul grade. "
        "NOT photoreal photograph. Full costumes on characters. "
    ),
    negative_prompt=_styled(", photograph, photorealistic 3D render"),
))

_register(RenderStyle(
    key="bw_comic",
    label="B&W comic",
    group="Comic",
    pipeline="styled_t2i",
    description="Black and white comic ink — noir graphic novel.",
    prompt_prefix=(
        "Black and white comic ink illustration, high-contrast noir linework, "
        "halftone shading, graphic novel page quality, dramatic shadows. Monochrome only. "
    ),
    negative_prompt=_styled(", color, photograph"),
))

_register(RenderStyle(
    key="dark_comic",
    label="Dark comic",
    group="Comic",
    pipeline="styled_t2i",
    description="Moody, high-contrast dark comic aesthetic.",
    prompt_prefix=(
        "Dark mature comic book art, heavy chiaroscuro, muted palette with blood-red accents, "
        "gritty ink textures, Vertigo / mature DC tone, cinematic panel composition. "
    ),
    negative_prompt=_styled(),
))

_register(RenderStyle(
    key="dark_cartoon",
    label="Dark cartoon",
    group="Animation",
    pipeline="styled_t2i",
    description="Stylized dark cartoon — bold shapes, moody palette.",
    prompt_prefix=(
        "Dark stylized cartoon, bold graphic shapes, moody limited palette, "
        "thick outlines, expressive silhouettes, adult animation mood without gore fetish. "
    ),
    negative_prompt=_styled(", photoreal photograph"),
))

_register(RenderStyle(
    key="adult_cartoon",
    label="Adult cartoon",
    group="Animation",
    pipeline="styled_t2i",
    description="Mature animated sitcom / late-night cartoon tone (not explicit).",
    prompt_prefix=(
        "Adult animated sitcom style, flat cel colors, thick clean outlines, "
        "expressive caricature proportions, comedic staging, broadcast animation finish. "
        "Family-unfriendly humor allowed in tone but NO nudity or explicit sexual content. "
    ),
    negative_prompt=_styled(", photoreal, nudity, explicit sexual content, pornographic"),
))

_register(RenderStyle(
    key="cute_anime",
    label="Cute anime",
    group="Animation",
    pipeline="styled_t2i",
    description="Soft anime — large eyes, pastel-friendly, kawaii-adjacent.",
    prompt_prefix=(
        "Cute anime illustration, soft cel shading, clean linework, expressive eyes, "
        "pastel-friendly palette, vertical key visual composition, premium anime key art. "
    ),
    negative_prompt=_styled(", photoreal photograph, horror, gore"),
))

_register(RenderStyle(
    key="studio_ghibli",
    label="Studio Ghibli",
    group="Animation",
    pipeline="styled_t2i",
    description="Hand-painted Ghibli-inspired warmth and wonder.",
    prompt_prefix=(
        "Studio Ghibli inspired hand-painted animation still, soft watercolor backgrounds, "
        "gentle natural light, whimsical atmosphere, painterly foliage, warm humanist framing. "
    ),
    negative_prompt=_styled(", photoreal photograph, harsh neon, horror"),
))

_register(RenderStyle(
    key="pixar",
    label="Pixar",
    group="Animation",
    pipeline="styled_t2i",
    description="3D animated feature film — Pixar-grade appeal lighting.",
    prompt_prefix=(
        "Pixar-style 3D animated feature still, appealing character shapes, "
        "subsurface skin, soft global illumination, family-film production design. "
    ),
    negative_prompt=_styled(", photoreal human photograph, horror"),
))

_register(RenderStyle(
    key="claymation",
    label="Claymation",
    group="Animation",
    pipeline="styled_t2i",
    description="Handmade stop-motion clay look — tactile models, fingerprints, miniature sets.",
    prompt_prefix=(
        "Premium claymation stop-motion animation still, tactile clay character surfaces, "
        "subtle fingerprints and handmade model texture, miniature set lighting, charming physical craft. "
    ),
    negative_prompt=_styled(", photoreal photograph, smooth CGI, anime"),
))

_register(RenderStyle(
    key="disney_90s",
    label="90s Disney",
    group="Animation",
    pipeline="styled_t2i",
    description="1990s Disney Renaissance hand-drawn animation.",
    prompt_prefix=(
        "1990s Disney Renaissance hand-drawn animation still, expressive line art, "
        "rich painted backgrounds, musical-theater staging energy, classic cel animation. "
    ),
    negative_prompt=_styled(", 3D CGI, photoreal photograph"),
))

_register(RenderStyle(
    key="simpsons",
    label="Simpsons",
    group="Animation",
    pipeline="styled_t2i",
    description="Yellow-skin satirical sitcom cartoon.",
    prompt_prefix=(
        "Simpsons-style satirical TV cartoon, flat cel colors, overbite caricature faces, "
        "Springfield sitcom staging, thick black outlines, comedy background gags optional. "
    ),
    negative_prompt=_styled(", photoreal, anime"),
))

_register(RenderStyle(
    key="creepy_cartoon_v1",
    label="Creepy cartoon v1",
    group="Animation",
    pipeline="styled_t2i",
    description="Unsettling cartoon — off-kilter proportions, liminal dread.",
    prompt_prefix=(
        "Unsettling creepy cartoon v1, slightly wrong proportions, liminal empty backgrounds, "
        "muted sickly palette, horror-comedy cartoon tension, no gore. "
    ),
    negative_prompt=_styled(", photoreal, gore, explicit violence"),
))

_register(RenderStyle(
    key="creepy_cartoon_v2",
    label="Creepy cartoon v2",
    group="Animation",
    pipeline="styled_t2i",
    description="Creepier variant — sharper shadows, more dread.",
    prompt_prefix=(
        "Creepy cartoon v2, harsh shadow shapes, wide unblinking eyes, "
        "distorted perspective, horror anthology cartoon framing, dread without splatter. "
    ),
    negative_prompt=_styled(", photoreal, gore, explicit violence"),
))

_register(RenderStyle(
    key="historical_18th_century",
    label="18th century historical",
    group="Realism",
    pipeline="styled_t2i",
    description="Period-accurate 1700s oil-painting / engraving historical illustration.",
    prompt_prefix=(
        "18th century historical illustration, period-accurate clothing and architecture, "
        "oil-painting or copper-engraving documentary tone, muted earth pigments, "
        "museum diorama gravitas, no modern objects. "
    ),
    negative_prompt=_styled(", modern clothing, smartphones, neon, skeleton mascot"),
))

_register(RenderStyle(
    key="illustrated_book",
    label="Illustrated book",
    group="Specialty",
    pipeline="styled_t2i",
    description="Premium children's / literary book illustration spread.",
    prompt_prefix=(
        "Premium illustrated storybook spread, painterly page art, readable focal subject, "
        "warm narrative framing, literary picture-book finish. "
    ),
    negative_prompt=_styled(),
))

_register(RenderStyle(
    key="whiteboard",
    label="Whiteboard",
    group="Specialty",
    pipeline="styled_t2i",
    description="Explainer whiteboard doodle — markers on white surface.",
    prompt_prefix=(
        "Whiteboard explainer illustration, black and colored marker lines on white board, "
        "simple icons, arrows, stick figures upgraded to clear diagrams, educator framing. "
    ),
    negative_prompt=_styled(", photoreal photograph, 3D render"),
))

_register(RenderStyle(
    key="lego",
    label="LEGO",
    group="Specialty",
    pipeline="styled_t2i",
    description="LEGO brick diorama — minifigures and studded plastic world.",
    prompt_prefix=(
        "LEGO brick diorama, plastic minifigure characters, studded block environments, "
        "toy photography lighting, official LEGO aesthetic without trademark logos. "
    ),
    negative_prompt=_styled(", photoreal human, photograph of real people"),
))

_register(RenderStyle(
    key="minecraft",
    label="Minecraft",
    group="Specialty",
    pipeline="styled_t2i",
    description="Blocky voxel Minecraft-style world.",
    prompt_prefix=(
        "Minecraft voxel block world, cubic characters and terrain, "
        "game-capture cinematic angle, torchlight and biome color blocks. "
    ),
    negative_prompt=_styled(", smooth photoreal human"),
))

_register(RenderStyle(
    key="low_poly",
    label="Low poly",
    group="Specialty",
    pipeline="styled_t2i",
    description="Low-poly 3D art — faceted minimalist 3D.",
    prompt_prefix=(
        "Low-poly 3D art, faceted geometric surfaces, minimalist shapes, "
        "clean gradient lighting, indie game key art composition. "
    ),
    negative_prompt=_styled(", photoreal photograph"),
))

_register(RenderStyle(
    key="hand_drawn_2d",
    label="2D hand-drawn",
    group="Specialty",
    pipeline="styled_t2i",
    description="Traditional 2D hand-drawn animation frame.",
    prompt_prefix=(
        "Traditional 2D hand-drawn animation frame, visible pencil and ink lines, "
        "cel paint fills, artisan animation craftsmanship, vertical storyboard panel. "
    ),
    negative_prompt=_styled(", 3D CGI, photoreal photograph"),
))


def _resolve_launch_render_style_keys() -> frozenset[str]:
    """Every registered style, unless an explicit subset is configured."""
    raw = str(os.getenv("STUDIO_LAUNCH_RENDER_STYLES", "") or "").strip()
    if not raw:
        return frozenset(RENDER_STYLES)
    wanted = {part.strip().lower() for part in raw.split(",") if part.strip()}
    allowed = wanted & set(RENDER_STYLES)
    # An override that names nothing real must not silently disable every style.
    return frozenset(allowed) if allowed else frozenset(RENDER_STYLES)


# Registration is complete by this point, so the launch set can finally be built.
LAUNCH_RENDER_STYLE_KEYS = _resolve_launch_render_style_keys()


def list_render_styles() -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    # Every registered style is exposed. VALIDATED_RENDER_STYLE_KEYS marks the
    # ones already proven end to end, so the picker can show that distinction
    # rather than hiding the rest entirely.
    for style in RENDER_STYLES.values():
        if style.key not in LAUNCH_RENDER_STYLE_KEYS:
            continue
        d = style.to_dict()
        still_path = _style_preview_path(style.key)
        video_path = _style_preview_video_path(style.key)
        still_ready = still_path.is_file() and still_path.stat().st_size > 1024
        video_ready = video_path.is_file() and video_path.stat().st_size > 1024
        d["preview_ready"] = still_ready
        d["preview_video_ready"] = video_ready
        # Planning reads must never create billable provider work. Only advertise
        # media that is already present in the cache; the GET routes are likewise
        # cache-only and return 404 when these fields are absent.
        if still_ready:
            d["preview_url"] = f"/api/studio-agent/style-preview/{style.key}"
        if video_ready:
            d["preview_video_url"] = f"/api/studio-agent/style-preview/{style.key}/video"
        groups.setdefault(style.group, []).append(d)
    ordered = []
    for group in ("Realism", "Comic", "Animation", "Specialty", "Niche"):
        for item in groups.get(group, []):
            ordered.append(item)
    return ordered


def get_render_style(key: str) -> RenderStyle:
    k = str(key or "").strip().lower()
    if k in RENDER_STYLES:
        return RENDER_STYLES[k]
    raise KeyError(
        f"Unknown render_style '{key}'. Call list_render_styles. "
        f"Default for most channels: {DEFAULT_RENDER_STYLE}"
    )


def explicit_render_style_from_text(user_text: str | None) -> str | None:
    """Return a style key only when the user actually named an art style."""
    text = " ".join(str(user_text or "").lower().replace("_", " ").split())
    matches: list[tuple[int, str]] = []
    for key, aliases in STYLE_TEXT_ALIASES.items():
        for alias in aliases:
            if re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", text):
                matches.append((len(alias), key))
    return max(matches, key=lambda item: item[0])[1] if matches else None


def resolve_render_style(
    explicit: str | None,
    *,
    session_style: str | None = None,
    user_text: str | None = None,
) -> RenderStyle:
    # Natural-language style selection is a hard lock. The picker remains the
    # fallback, never an invisible override of the user's newest request.
    spoken = explicit_render_style_from_text(user_text)
    for candidate in (spoken, explicit, session_style, DEFAULT_RENDER_STYLE):
        if candidate and str(candidate).strip():
            style = _style_from_candidate(str(candidate).strip())
            if style is not None and style.key in LAUNCH_RENDER_STYLE_KEYS:
                return style
    return RENDER_STYLES[DEFAULT_RENDER_STYLE]


def _style_from_candidate(value: str) -> RenderStyle | None:
    """Resolve a registry key OR a human name like "skeleton" / "ghibli".

    Aliases used to apply only to spoken text, so an explicit or stored value of
    "skeleton" matched no registry key (it is `skeleton_host`) and fell through
    to the cinematic default. That failed silently: the wrong art style was
    rendered and charged for, with nothing reporting a mismatch. Every
    resolution path now understands the same names the creator does.
    """
    try:
        return get_render_style(value)
    except KeyError:
        pass
    alias_key = explicit_render_style_from_text(value)
    if not alias_key:
        return None
    try:
        return get_render_style(alias_key)
    except KeyError:
        return None


def is_skeleton_style(style: RenderStyle | str) -> bool:
    if isinstance(style, RenderStyle):
        return style.pipeline == "skeleton_host"
    try:
        return get_render_style(style).pipeline == "skeleton_host"
    except KeyError:
        return False


def _style_preview_path(key: str) -> Path:
    return STYLE_PREVIEW_DIR / f"{key}-{STYLE_PREVIEW_VERSION}.png"


def _style_preview_video_path(key: str) -> Path:
    return STYLE_PREVIEW_VIDEO_DIR / f"{key}-{STYLE_PREVIEW_VERSION}.mp4"


def get_cached_style_preview_path(key: str) -> Path | None:
    """Return a validated cached still without generating provider work."""
    style = get_render_style(key)
    if style.key not in LAUNCH_RENDER_STYLE_KEYS:
        raise KeyError(f"Render style '{key}' is not available for launch")
    path = _style_preview_path(style.key)
    return path if path.is_file() and path.stat().st_size > 1024 else None


def get_cached_style_preview_video_path(key: str) -> Path | None:
    """Return a validated cached motion preview without generating work."""
    style = get_render_style(key)
    if style.key not in LAUNCH_RENDER_STYLE_KEYS:
        raise KeyError(f"Render style '{key}' is not available for launch")
    path = _style_preview_video_path(style.key)
    return path if path.is_file() and path.stat().st_size > 1024 else None


def _style_preview_prompt(style: RenderStyle) -> str:
    subject = STYLE_PREVIEW_SUBJECTS.get(style.key, STYLE_PREVIEW_SCENE)
    base_visual = (
        "Single high-quality vertical 9:16 keyframe, premium showcase still that demonstrates the exact art style, "
        "distinct original subject designed for this specific style category, no text, no typography, no letters, "
        "no logos, no watermarks, no speech bubbles, clean edges, high visual impact for a style reference gallery. "
    )
    return (style.prompt_prefix.strip() + " " + base_visual + subject).strip()[:3500]


def get_style_preview_path(key: str) -> Path:
    """Generate (or return cached) a single hero preview still for the style picker grid.
    Uses cheap Seedream v4.5 (or canonical edit for skeleton) so we can show visual cards
    like the reference grids. One image per style is sufficient and very cheap.
    """
    path = _style_preview_path(key)
    if path.exists() and path.stat().st_size > 1024:
        return path

    style = get_render_style(key)

    if is_skeleton_style(style):
        from skeleton_ai.canonical_edit import generate_still_edit

        skel_prompt = (
            "Glass transparent anatomical skeleton with ivory bones and realistic eyes, same NYPTID skeleton host, "
            "wearing a black hoodie and black pants, seated at a glowing computer workstation in a dark intelligence room. "
            "Edit only wardrobe, pose, props, and background; preserve the exact skeleton identity. No text, no labels."
        )
        generate_still_edit(skel_prompt, path, seed=STYLE_PREVIEW_SEED)
    else:
        from skeleton_ai.styled_stills import generate_still_t2i

        generate_still_t2i(
            _style_preview_prompt(style),
            path,
            negative_prompt=(
                style.negative_prompt
                + ", text, typography, letters, words, watermark, logo"
                + ("" if style.key == "cinematic" else ", mannequin, faceless mannequin, red mannequin, plastic dummy")
            ),
            seed=STYLE_PREVIEW_SEED,
        )
    return path


def get_style_preview_video_path(key: str) -> Path:
    """Generate or return a cached i2v preview clip for one art style.

    This is intentionally separate from still preview generation so opening the
    style picker does not automatically burn 24 i2v jobs. The frontend requests
    the clip when a user hovers/focuses a style card.
    """
    video_path = _style_preview_video_path(key)
    if video_path.exists() and video_path.stat().st_size > 1024:
        return video_path

    still_path = get_style_preview_path(key)
    from skeleton_ai.i2v_engine import generate as generate_i2v

    generate_i2v(
        still_path,
        (
            "Subtle premium motion: "
            + STYLE_PREVIEW_MOTIONS.get(key, STYLE_PREVIEW_MOTION)
            + ". Preserve the exact art style and character identity."
        ),
        video_path,
        video_model=os.getenv("STYLE_PREVIEW_VIDEO_MODEL", "seedance"),
        duration_sec=int(os.getenv("STYLE_PREVIEW_VIDEO_SECONDS", "4")),
    )
    return video_path
