"""PB Lies — 3D cast kit, color roster, and scene stills.

Workflow:
  1. Master cast (3 angles, t2i once)     → approved/
  2. Color roster (edit from master front) → roster/   (~$0.04 each)
  3. Scene stills (edit master + roster ref) → stills/  (~$0.04 each)

Artifact control: single ref for recolors, locked seed, strict NEG, edit-not-t2i
for all scenes after master exists.

Run:
  python long_form/pb_lies_cast_kit.py --refs-only
  python long_form/pb_lies_cast_kit.py --roster              # all 40 variants
  python long_form/pb_lies_cast_kit.py --roster --limit 8    # starter palette
  python long_form/pb_lies_cast_kit.py --scenes              # all Mockingbird stills
  python long_form/pb_lies_cast_kit.py --scene scene_01_classified_desk
  python long_form/pb_lies_cast_kit.py --scene scene_01 --variant white_suit_default

EM reuse: fork PB roster to yellow porcelain after approval:
  python long_form/pb_to_em_cast_fork.py
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fal_client

from long_form.pb_lies_mockingbird_scenes import MOCKINGBIRD_SCENES
from long_form.pipeline import SEEDREAM_URL, _download, _fal_post

SEEDREAM_EDIT_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"

OUT_ROOT = Path(r"D:/recaps/pb_lies/cast_kit_white3d")
APPROVED_DIR = OUT_ROOT / "approved"
ROSTER_DIR = OUT_ROOT / "roster"
STILLS_DIR = OUT_ROOT / "stills"
DOWNLOADS = Path.home() / "Downloads"
CAST_SEED = 420017
COST_PER_IMAGE = 0.04

# Locked on every scene still — do not omit.
PB_LIES_WATERMARK = (
    "Small subtle grey sans-serif watermark text reading exactly PB LIES "
    "in the bottom-left corner, semi-transparent, never centered, never large."
)

MESH_LOCK = (
    "PB LIES CAST LOCK: IDENTICAL faceless 3D mannequin mesh — smooth matte "
    "oval head with NO eyes NO mouth NO nose NO hair, stylized adult male "
    "proportions, high-end Blender Octane render. Same body topology every time."
)

ARTIFACT_GUARD = (
    "Exactly ONE figure in frame unless scene explicitly says crowd. "
    "Anatomically correct hands with five fingers, no extra limbs, no melted "
    "geometry, no facial features, no duplicate bodies, no blur smear on edges, "
    "clean hard-surface 3D, sharp suit fabric folds."
)

NEG = (
    "real human face, photographic skin, eyes, mouth, nose, hair, teeth, "
    "anime, cartoon 2D, low poly blocky, yellow porcelain, skeleton, "
    "extra fingers, extra arms, deformed hands, duplicate person, twins, "
    "busy background clutter, illegible gibberish text blocks, watermark "
    "except small PB LIES bottom-left, blurry, jpeg artifacts"
)

LIGHTING_BIBLE = (
    "Cinematic 3D documentary lighting: cold desaturated fill, single warm rim "
    "or forensic accent edge light, deep shadows, reflective dark floor when "
    "interior void. Clean octane-style render, NOT flat illustration."
)

CAST_SHEETS = [
    {
        "id": "cast_master_front",
        "prompt": (
            f"{MESH_LOCK} Full-body front view, neutral standing pose, "
            "matte white head and hands, dark charcoal suit white shirt black tie, "
            "plain medium-grey seamless studio backdrop, soft even lighting, "
            "character reference sheet, 16:9."
        ),
    },
    {
        "id": "cast_master_34",
        "prompt": (
            f"{MESH_LOCK} Chest-up three-quarter view facing camera left, "
            "matte white head, dark charcoal suit white shirt, plain medium-grey "
            "seamless studio backdrop, character reference sheet, 16:9."
        ),
    },
    {
        "id": "cast_master_walk",
        "prompt": (
            f"{MESH_LOCK} Full-body mid-stride walking pose, matte white head, "
            "dark charcoal suit, plain medium-grey seamless studio backdrop, "
            "character reference sheet, 16:9."
        ),
    },
]

# 40 color / wardrobe variants — pick per scene from cast_kit.json.
# Generated via edit from cast_master_front.png only ($0.04 each).
CAST_ROSTER: list[dict[str, Any]] = [
    {"id": "white_suit_default", "color": "matte white head and hands", "wardrobe": "charcoal suit white shirt black tie", "tags": ["default", "CIA", "desk"]},
    {"id": "white_labcoat", "color": "matte white head and hands", "wardrobe": "white lab coat over light blue shirt", "tags": ["lab", "science", "medical"]},
    {"id": "grey_suit_graphite", "color": "matte light grey head and hands", "wardrobe": "graphite suit white shirt", "tags": ["corporate", "neutral"]},
    {"id": "grey_labcoat", "color": "matte light grey head and hands", "wardrobe": "grey lab coat", "tags": ["lab", "clinical"]},
    {"id": "black_silhouette", "color": "matte black head and hands", "wardrobe": "black suit black shirt", "tags": ["void", "classified", "night"]},
    {"id": "charcoal_shadow", "color": "matte dark charcoal head and hands", "wardrobe": "black suit", "tags": ["dark", "interrogation"]},
    {"id": "bone_ivory", "color": "matte warm ivory cream head and hands", "wardrobe": "tan suit white shirt", "tags": ["archive", "warm"]},
    {"id": "manila_tan", "color": "matte manila-folder tan head and hands", "wardrobe": "beige suit white shirt", "tags": ["documents", "folder"]},
    {"id": "forensic_red", "color": "matte white head with subtle red rim glow on edges", "wardrobe": "dark suit", "tags": ["classified", "danger", "red"]},
    {"id": "cold_blue_clinical", "color": "matte white head with cool blue ambient tint", "wardrobe": "white lab coat", "tags": ["lab", "cold", "blue"]},
    {"id": "teal_hologram", "color": "matte white head with faint teal cyan edge light", "wardrobe": "dark suit", "tags": ["tech", "wireframe", "screen"]},
    {"id": "amber_archive", "color": "matte white head with warm amber rim light", "wardrobe": "brown suit", "tags": ["warm", "library", "old"]},
    {"id": "steel_blue_suit", "color": "matte pale blue-grey head and hands", "wardrobe": "navy suit white shirt", "tags": ["government", "formal"]},
    {"id": "olive_military", "color": "matte white head", "wardrobe": "olive drab military jacket dark pants", "tags": ["military", "declassified"]},
    {"id": "white_shirt_rolled", "color": "matte white head and hands", "wardrobe": "white dress shirt rolled sleeves no jacket", "tags": ["casual", "work"]},
    {"id": "redacted_black", "color": "matte black head and hands", "wardrobe": "black turtleneck", "tags": ["minimal", "void"]},
    {"id": "pale_ghost", "color": "matte desaturated pale grey-white head", "wardrobe": "light grey suit", "tags": ["flashback", "memory"]},
    {"id": "warm_sepia", "color": "matte sepia-tinted white head", "wardrobe": "1970s brown suit wide lapels", "tags": ["period", "70s", "CIA"]},
    {"id": "cool_silver", "color": "matte silver-grey metallic sheen head", "wardrobe": "silver-grey suit", "tags": ["modern", "tech"]},
    {"id": "green_matrix", "color": "matte white head with faint green terminal glow", "wardrobe": "black suit", "tags": ["computer", "terminal"]},
    {"id": "purple_interrogation", "color": "matte white head with purple rim from one side", "wardrobe": "dark suit", "tags": ["interview", "room"]},
    {"id": "orange_warning", "color": "matte white head with orange accent edge light", "wardrobe": "dark coveralls", "tags": ["hazard", "warning"]},
    {"id": "cyan_frost", "color": "matte frost-white head with icy cyan tint", "wardrobe": "white winter coat", "tags": ["cold", "outdoor", "snow"]},
    {"id": "copper_bronze", "color": "matte bronze-tinted head and hands", "wardrobe": "brown tweed jacket", "tags": ["academic", "professor"]},
    {"id": "white_trench", "color": "matte white head", "wardrobe": "beige trench coat dark suit underneath", "tags": ["detective", "rain"]},
    {"id": "navy_uniform", "color": "matte white head", "wardrobe": "dark navy uniform-style jacket", "tags": ["agency", "badge"]},
    {"id": "pink_neon", "color": "matte white head with subtle magenta neon rim", "wardrobe": "black suit", "tags": ["city", "night", "neon"]},
    {"id": "yellow_caution", "color": "matte white head with yellow caution rim", "wardrobe": "dark suit", "tags": ["tape", "crime scene"]},
    {"id": "deep_green_forest", "color": "matte white head", "wardrobe": "dark green field jacket", "tags": ["outdoor", "camp"]},
    {"id": "sand_desert", "color": "matte sand-tan head and hands", "wardrobe": "khaki suit", "tags": ["desert", "middle east"]},
    {"id": "white_hoodie", "color": "matte white head", "wardrobe": "grey hoodie dark pants", "tags": ["whistleblower", "casual"]},
    {"id": "red_tie_only", "color": "matte white head and hands", "wardrobe": "black suit white shirt bright red tie", "tags": ["accent", "formal"]},
    # ── Extended palette (33–40) for Mockingbird + EM fork coverage ──
    {"id": "broadcast_amber", "color": "matte white head with warm golden amber studio glow", "wardrobe": "grey suit white shirt", "tags": ["TV", "broadcast", "1960s"]},
    {"id": "typewriter_sepia", "color": "matte sepia-tinted white head", "wardrobe": "white shirt brown vest loosened tie journalist", "tags": ["press", "1950s", "typewriter"]},
    {"id": "wire_press_steel", "color": "matte cool steel-grey head", "wardrobe": "dark suit white shirt", "tags": ["wire", "teletype", "press"]},
    {"id": "stamp_red_classified", "color": "matte white head with intense red classified-stamp glow on edges", "wardrobe": "black suit white shirt", "tags": ["classified", "stamp", "chapter"]},
    {"id": "newsprint_grey", "color": "matte newsprint grey head with subtle ink-smudge texture", "wardrobe": "rolled sleeves white shirt no jacket", "tags": ["newsroom", "layout", "print"]},
    {"id": "cream_journalist", "color": "matte warm cream head and hands", "wardrobe": "1940s brown vest white shirt", "tags": ["journalist", "period", "press"]},
    {"id": "midnight_navy_void", "color": "matte white head with deep midnight navy ambient tint", "wardrobe": "navy suit black tie", "tags": ["night", "ops", "void"]},
    {"id": "parchment_archivist", "color": "matte parchment tan head and hands", "wardrobe": "brown archivist vest white shirt", "tags": ["archive", "records", "vault"]},
]

SCENES: dict[str, dict[str, Any]] = MOCKINGBIRD_SCENES


def _ensure_fal() -> None:
    key = (os.environ.get("FAL_AI_KEY") or os.environ.get("FAL_KEY") or "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY not set — load D:\\Games\\asd\\.env")
    os.environ["FAL_KEY"] = key


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)
            break


def _variant_by_id(vid: str) -> dict[str, Any]:
    for v in CAST_ROSTER:
        if v["id"] == vid:
            return v
    raise KeyError(f"Unknown variant: {vid}")


def _build_variant_prompt(variant: dict[str, Any]) -> str:
    return (
        f"{MESH_LOCK} {ARTIFACT_GUARD} "
        f"Full-body front view, neutral standing pose. "
        f"Recolor: {variant['color']}. Wardrobe: {variant['wardrobe']}. "
        "Plain medium-grey seamless studio backdrop, soft even lighting, "
        "character reference sheet, 16:9. NO environment, NO props."
    )


def _build_scene_prompt(variant: dict[str, Any], scene_delta: str) -> str:
    return (
        f"{MESH_LOCK} {ARTIFACT_GUARD} {LIGHTING_BIBLE} "
        f"Character colors: {variant['color']}. Wardrobe: {variant['wardrobe']}. "
        f"{scene_delta} 16:9 cinematic. {PB_LIES_WATERMARK}"
    )


def _gen_t2i(prompt: str, out_path: Path, *, seed: int = CAST_SEED) -> Path:
    if out_path.exists() and out_path.stat().st_size > 1024:
        print(f"  [skip] {out_path.name}")
        return out_path
    data = _fal_post(
        SEEDREAM_URL,
        {
            "prompt": prompt[:3500],
            "negative_prompt": NEG,
            "image_size": "auto_2K",
            "num_images": 1,
            "seed": seed,
            "enable_safety_checker": True,
        },
        timeout_s=240,
    )
    images = data.get("images") or []
    if not images or not (url := (images[0].get("url"))):
        raise RuntimeError(f"t2i failed: {data}")
    _download(url, out_path, timeout_s=120)
    print(f"  [t2i] {out_path.name} ({out_path.stat().st_size // 1024} KB)")
    return out_path


def _gen_edit(
    prompt: str,
    refs: list[Path],
    out_path: Path,
    *,
    seed: int = CAST_SEED,
) -> Path:
    if out_path.exists() and out_path.stat().st_size > 1024:
        print(f"  [skip] {out_path.name}")
        return out_path
    if not refs:
        raise RuntimeError("No reference images")
    _ensure_fal()
    image_urls = [fal_client.upload_file(str(p)) for p in refs[:3]]
    data = _fal_post(
        SEEDREAM_EDIT_URL,
        {
            "prompt": prompt[:3500],
            "image_urls": image_urls,
            "negative_prompt": NEG,
            "image_size": "auto_2K",
            "num_images": 1,
            "seed": seed,
        },
        timeout_s=240,
    )
    images = data.get("images") or []
    if not images or not (url := (images[0].get("url"))):
        raise RuntimeError(f"edit failed: {data}")
    _download(url, out_path, timeout_s=120)
    print(f"  [edit] {out_path.name} ({out_path.stat().st_size // 1024} KB)")
    return out_path


def _master_front() -> Path:
    p = APPROVED_DIR / "cast_master_front.png"
    if not p.exists():
        raise RuntimeError("Missing cast_master_front.png — run --refs-only first")
    return p


def _scene_refs(variant_id: str) -> list[Path]:
    refs: list[Path] = [_master_front()]
    roster = ROSTER_DIR / f"{variant_id}.png"
    if roster.exists() and roster.stat().st_size > 1024:
        refs.append(roster)
    for name in ("cast_master_34.png", "cast_master_walk.png"):
        p = APPROVED_DIR / name
        if p.exists():
            refs.append(p)
    return refs[:3]


def generate_master_refs() -> float:
    cost = 0.0
    print("=== Master cast (3 × t2i, $0.04 each) ===")
    for sheet in CAST_SHEETS:
        _gen_t2i(sheet["prompt"], APPROVED_DIR / f"{sheet['id']}.png")
        cost += COST_PER_IMAGE
    return cost


def generate_roster(*, limit: int | None = None) -> float:
    cost = 0.0
    variants = CAST_ROSTER[:limit] if limit else CAST_ROSTER
    master = _master_front()
    print(f"=== Color roster ({len(variants)} × edit, $0.04 each) ===")
    ROSTER_DIR.mkdir(parents=True, exist_ok=True)
    for v in variants:
        out = ROSTER_DIR / f"{v['id']}.png"
        _gen_edit(_build_variant_prompt(v), [master], out, seed=CAST_SEED)
        cost += COST_PER_IMAGE
    return cost


def generate_scene(scene_key: str, variant_id: str | None = None) -> float:
    spec = SCENES[scene_key]
    vid = variant_id or spec.get("variant") or "white_suit_default"
    variant = _variant_by_id(vid)
    out = STILLS_DIR / f"{spec['id']}_{vid}.png"
    STILLS_DIR.mkdir(parents=True, exist_ok=True)
    ch = spec.get("chapter", "?")
    print(f"=== Scene {scene_key} ch={ch} variant={vid} ===")
    prompt = _build_scene_prompt(variant, spec["prompt_delta"])
    _gen_edit(prompt, _scene_refs(vid), out)
    dl = DOWNLOADS / "PB_Lies_Mockbird_Stills" / f"{spec['id']}.png"
    dl.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(out, dl)
    print(f"  Downloads: {dl}")
    return COST_PER_IMAGE


def generate_all_scenes(*, skip_existing: bool = True) -> float:
    cost = 0.0
    keys = sorted(SCENES.keys(), key=lambda k: SCENES[k].get("id", k))
    print(f"=== Mockingbird stills ({len(keys)} scenes × edit) ===")
    for key in keys:
        spec = SCENES[key]
        vid = spec.get("variant") or "white_suit_default"
        out = STILLS_DIR / f"{spec['id']}_{vid}.png"
        if skip_existing and out.exists() and out.stat().st_size > 1024:
            print(f"  [skip scene] {out.name}")
            continue
        cost += generate_scene(key)
    return cost


def _write_roster_json() -> None:
    meta = {
        "cast_seed": CAST_SEED,
        "mesh_lock": MESH_LOCK,
        "watermark": PB_LIES_WATERMARK,
        "artifact_guard": ARTIFACT_GUARD,
        "style_reference": "https://www.youtube.com/watch?v=bUcMQH3vVm8",
        "approved_dir": str(APPROVED_DIR),
        "roster_dir": str(ROSTER_DIR),
        "variants": CAST_ROSTER,
        "scenes": SCENES,
        "rules": {
            "master_once": "Generate approved/ master angles once via t2i",
            "roster_via_edit": "Each color variant = edit from cast_master_front.png only",
            "scene_via_edit": "Each scene = edit from master + roster variant + optional angle",
            "never_t2i_scene": "Do not t2i full scenes after master exists",
            "watermark": "PB LIES bottom-left on every scene still",
        },
    }
    (OUT_ROOT / "cast_kit.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def main() -> None:
    _load_env()
    _ensure_fal()
    ap = argparse.ArgumentParser(description="PB Lies 3D cast kit + roster + scenes")
    ap.add_argument("--refs-only", action="store_true", help="Master 3-angle cast only")
    ap.add_argument("--roster", action="store_true", help="Generate color variant roster")
    ap.add_argument("--limit", type=int, default=None, help="Cap roster count")
    ap.add_argument("--scenes", action="store_true", help="All Mockingbird scene stills")
    ap.add_argument("--scene", type=str, default=None, help="Scene key from SCENES dict")
    ap.add_argument("--variant", type=str, default=None, help="Roster variant id for --scene")
    ap.add_argument("--force", action="store_true", help="Regenerate even if PNG exists")
    args = ap.parse_args()

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    APPROVED_DIR.mkdir(parents=True, exist_ok=True)

    cost = 0.0
    if args.refs_only or (
        not args.roster and not args.scenes and not args.scene
    ):
        cost += generate_master_refs()

    if args.roster:
        cost += generate_roster(limit=args.limit)

    if args.scenes:
        cost += generate_all_scenes(skip_existing=not args.force)

    if args.scene:
        if args.scene not in SCENES:
            raise SystemExit(f"Unknown scene. Options: {list(SCENES.keys())[:5]}... ({len(SCENES)} total)")
        cost += generate_scene(args.scene, args.variant)

    if not args.roster and not args.scenes and not args.scene and not args.refs_only:
        cost += generate_scene("scene_01_classified_desk")

    _write_roster_json()
    print(f"\n=== DONE — est fal ~${cost:.2f} | roster={len(CAST_ROSTER)} variants defined ===")
    print(f"Master: {APPROVED_DIR}")
    print(f"Roster: {ROSTER_DIR}")


if __name__ == "__main__":
    main()
