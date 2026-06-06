"""Canonical render-style registry for Studio Agent shortform production."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from pathlib import Path
import os

PipelineKind = Literal["skeleton_host", "styled_t2i"]

DEFAULT_RENDER_STYLE = "cinematic"

STYLE_PREVIEW_DIR = Path(os.getenv("STYLE_PREVIEW_DIR", "ref_frames/style_previews"))
STYLE_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)


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


def list_render_styles() -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for style in RENDER_STYLES.values():
        d = style.to_dict()
        # Include preview URL so frontend can render visual grid like the reference style galleries.
        # Previews generated on-demand (very cheap 1x Seedream per style) and cached.
        d["preview_url"] = f"/api/studio-agent/style-preview/{style.key}"
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


def resolve_render_style(
    explicit: str | None,
    *,
    session_style: str | None = None,
) -> RenderStyle:
    for candidate in (explicit, session_style, DEFAULT_RENDER_STYLE):
        if candidate and str(candidate).strip():
            try:
                return get_render_style(str(candidate).strip())
            except KeyError:
                continue
    return RENDER_STYLES[DEFAULT_RENDER_STYLE]


def is_skeleton_style(style: RenderStyle | str) -> bool:
    if isinstance(style, RenderStyle):
        return style.pipeline == "skeleton_host"
    try:
        return get_render_style(style).pipeline == "skeleton_host"
    except KeyError:
        return False


def get_style_preview_path(key: str) -> Path:
    """Generate (or return cached) a single hero preview still for the style picker grid.
    Uses cheap Seedream v4.5 (or canonical edit for skeleton) so we can show visual cards
    like the reference grids. One image per style is sufficient and very cheap.
    """
    path = STYLE_PREVIEW_DIR / f"{key}.png"
    # Always (re)generate on access for now so that prompt improvements (better visual hero examples, explicit "no text")
    # take effect immediately after deploy. One cheap Seedream per style is the design point; caching is secondary.
    # if path.exists() and path.stat().st_size > 1024: return path

    style = get_render_style(key)
    # Force a concrete, visual "hero example" frame that demonstrates the style,
    # not a typography card. Explicitly ban text/labels so the preview is a pure visual sample.
    base_visual = (
        "Single high-quality vertical 9:16 keyframe, striking central subject or character in a dramatic but clear composition, "
        "premium showcase still that demonstrates the exact art style, no text, no typography, no letters, no logos, no watermarks, "
        "no speech bubbles, clean edges, high visual impact for style reference gallery."
    )
    hero = (style.prompt_prefix.strip() + " " + base_visual).strip()

    if is_skeleton_style(style):
        from skeleton_ai.canonical_edit import generate_still_edit
        # For skeleton, force the glass anatomical look from the provided example
        skel_prompt = (
            "Glass transparent anatomical skeleton with ivory bones, realistic eyes, "
            "dynamic hero pose holding a basketball on a professional court at night, "
            "cinematic lighting through the glass shell, premium 3D render reference, "
            "no text, no labels. "
        ) + base_visual
        generate_still_edit(skel_prompt, path, seed=424242)
    else:
        from skeleton_ai.styled_stills import generate_still_t2i
        generate_still_t2i(
            hero,
            path,
            negative_prompt=style.negative_prompt + ", text, typography, letters, words, watermark, logo",
            seed=424242,
        )
    return path
