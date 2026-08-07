"""
Skeleton dataset v3 — adds the Cryptic Science signature aesthetic
that v1/v2 MISSED:
  - Large realistic eyeballs inside the eye sockets
  - Translucent gel/glass body silhouette wrapping the skeleton

Casey validated the LoRA trained on v1+v2 produces correct full-body
anatomical skeletons in any outfit, but the signature "eyeballs in
sockets + rubbery translucent shell" look is absent. That look is
what differentiates the Cryptic Science lane from generic 3D-skeleton
renders.

v3 generates a smaller, highly-curated set (~250 images) with the
full aesthetic baked into every prompt. Retraining the LoRA on v3
should finally produce the canonical look consistently.

Budget: 250 images at $0.02/img = $5 generation + $2 retrain = $7.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from collections import defaultdict
from pathlib import Path

import httpx
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

load_dotenv()
FAL_AI_KEY = os.getenv("FAL_AI_KEY", "")
GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"
OUTPUT_DIR = Path("skeleton_training_dataset")
CAPTIONS_DIR = OUTPUT_DIR / "captions"

TRIGGER_TOKEN = "nyptid_skeleton"

# v3 BASE_STYLE — explicit large eyeballs + translucent gel shell,
# consistent across every image so the LoRA learns the aesthetic.
BASE_STYLE = (
    "Photorealistic 3D render, Unreal Engine 5 quality, octane render, 8K resolution. "
    "Glossy ivory chrome white anatomical human skeleton with subtle metallic reflections. "
    "LARGE realistic 3D EYEBALLS inside both eye sockets with dark pupils and visible irises, "
    "eyeballs looking directly forward. "
    "TRANSLUCENT glossy gel-like body silhouette shell wrapping around the skeleton form, "
    "clear rubbery translucent skin outline over the bones so the skull + ribcage + pelvis + "
    "limb bones are clearly visible THROUGH the translucent shell. "
    "Signature Cryptic Science aesthetic: eyeballs + translucent shell are mandatory."
)

BACKGROUND = (
    "Solid clean teal mint green (#5AC8B8) studio backdrop with smooth gradient lighting. "
    "No environments, rooms, or outdoor scenes."
)

# Trimmed and hand-picked outfit set — 60 categories that cover the
# biggest variety axes (period, anime, fantasy, sci-fi, subculture,
# cultural, service) without the niche filler.
OUTFITS = [
    # period
    ("1920s_fbi_agent", "dark pinstripe three-piece wool suit, white collared shirt, black silk necktie, black fedora, polished black oxford shoes"),
    ("1920s_gangster", "charcoal pinstripe suit, white pocket square, black fedora tilted, Tommy gun in one hand, spectator shoes"),
    ("1940s_soldier", "olive drab US Army uniform, M1 helmet, canvas webbing, combat boots, rifle slung"),
    ("victorian_gent", "black tailcoat, white vest, cravat, pocket watch chain, top hat, cane"),
    ("1970s_disco", "white polyester suit with wide lapels, silk shirt unbuttoned, platform shoes, gold medallion"),
    ("1980s_yuppie", "boxy wide-shoulder suit, suspenders, rolex, slicked back hair style implied"),
    ("1950s_greaser", "white t-shirt, leather jacket, blue jeans rolled up, black leather boots, pompadour implied"),
    ("roman_legionary", "lorica segmentata armor, gladius sword, scutum shield, red tunic, sandals, crested helmet"),
    ("egyptian_pharaoh", "nemes headdress with gold and blue stripes, white kilt, wide gold collar, crook and flail"),
    ("viking_raider", "horned helmet, chainmail hauberk, fur cloak, round shield, battle axe, leather boots"),
    # anime
    ("shounen_protagonist", "orange training gi with white collar, headband with metal plate, open-toed sandals"),
    ("gojo_sorcerer", "black slim-fit high-collar uniform with silver trim, white blindfold over eye sockets, combat boots"),
    ("hokage_ninja", "white ceremonial cloak with red flame trim over black ninja gi, straw kasa hat, forehead protector"),
    ("titan_scout", "brown leather ODM gear harness, cream long coat with wing crest, white pants, knee-high brown boots"),
    ("hero_academy", "white double-breasted uniform jacket with red piping, red tie, grey pleated pants, utility belt"),
    ("demon_slayer", "black gakuran jacket, haori with geometric pattern, hakama pants, tabi socks, katana"),
    ("mecha_pilot", "tight white plugsuit with neon blue seams, helmet with visor, chunky space boots"),
    ("anime_ronin", "weathered dark blue kimono with faded patterns, straw sandals, loose obi belt, katana on hip"),
    # fantasy
    ("wizard_archmage", "deep blue velvet robe with embroidered silver runes, pointed wizard hat, long wooden staff"),
    ("elf_ranger", "hooded green cloak, leather chest piece, quiver of arrows, longbow, leaf brooch"),
    ("dragon_knight", "scaled silver plate armor etched with dragon motifs, horned helm, flaming longsword"),
    ("dark_sorcerer", "tattered black hooded robe with purple lining, bone staff, glowing green wisps"),
    ("paladin_crusader", "polished white plate armor with red cross emblem, tabard, longsword in both hands"),
    # sci-fi
    ("cyberpunk_hacker", "black leather trench coat with LED trim, mirrored techwear visor, cargo pants, combat boots"),
    ("space_marine", "chunky gunmetal grey power armor, full-face visored helmet, pulse rifle, tactical harness"),
    ("starship_captain", "fitted red command uniform with rank insignia, black pants with red stripe, boots"),
    ("cyber_samurai", "black neon-trimmed techno-kimono, chrome katana with LED edge, visored mask"),
    ("neo_noir_detective", "long black trench coat over charcoal suit, wide-brim fedora with red LED trim, futuristic pistol"),
    ("galactic_bounty_hunter", "weathered beskar-style armor, cracked visor helmet, jet pack, wrist blaster"),
    # subculture
    ("biker_rocker", "black leather biker jacket with patches, bandana, ripped jeans, heavy chain wallet, boots"),
    ("goth_vampire", "long black Victorian cloak with red velvet lining, ornate silver cross, skinny black trousers"),
    ("punk_rocker", "leather vest covered in pins, ripped plaid pants, combat boots, safety pins, chain from belt"),
    ("hip_hop_streetwear", "oversized graphic tee, designer jeans, chunky dad sneakers, baseball cap backwards, gold chains"),
    ("skater", "loose graphic tee, baggy cargo shorts, vans slip-ons, skateboard under arm, wrist guards"),
    # villain / action
    ("spy_tuxedo", "tailored black tuxedo with satin lapels, white shirt, black bow tie, silenced pistol"),
    ("assassin_hood", "matte black tactical hood, fitted black combat bodysuit, wrist blade, soft-soled boots"),
    ("mob_boss", "double-breasted charcoal suit with pocket square, wide-brim fedora, Cuban cigar, pinky ring"),
    ("private_detective", "tan trench coat over rumpled suit, fedora, notebook in hand, cigarette smoke rising"),
    ("corporate_villain", "all-black turtleneck under black blazer, black slacks, rimless glasses"),
    # cultural
    ("samurai", "full samurai armor with kabuto helmet, katana drawn, menacing stance"),
    ("muay_thai_fighter", "red muay thai shorts with gold trim, mongkol headband, prajioud arm bands, wrapped fists"),
    ("cowboy", "leather cowboy hat, denim jacket with fringe, cowboy boots with spurs, bandana"),
    ("sumo_wrestler", "traditional mawashi belt, topknot hair style implied on skull, bare feet, wide-stance squat"),
    ("tibetan_monk", "deep maroon and saffron monk robes, wooden prayer beads, barefoot, meditative standing pose"),
    # fashion / performer
    ("runway_model", "avant-garde sculpted white couture gown, dramatic shoulders, towering heels, editorial pose"),
    ("pop_star_stage", "glittering silver metallic jumpsuit, over-the-knee boots, wireless microphone, stage stance"),
    ("ballet_dancer", "classical white tutu and leotard, pointe shoes ribbon-laced, tiara, arabesque pose, arms extended"),
    ("circus_ringmaster", "red velvet tailcoat with gold trim, top hat, white jodhpurs, tall riding boots, whip"),
    # service / trades
    ("lawyer", "dark charcoal suit, white shirt with French cuffs, silk tie, leather briefcase, wingtip oxfords"),
    ("teacher", "tweed blazer with elbow patches, chalk-dusted trousers, reading glasses, stack of books"),
    ("nurse", "light blue scrubs, stethoscope, white sneakers, nurse's cap, clipboard"),
    ("chef", "double-breasted white chef coat with black buttons, tall toque hat, checkered pants, apron"),
    ("surgeon", "teal surgical scrubs, white lab coat, stethoscope, surgical cap, latex gloves"),
    ("firefighter", "full turnout gear with reflective yellow stripes, fire helmet, oxygen tank, heavy boots"),
    ("astronaut", "white NASA spacesuit with American flag patch, space helmet held at side, heavy boots"),
    ("police_officer", "dark blue police uniform with badge, utility belt with radio, police cap, black boots"),
    ("pilot", "navy blue airline pilot uniform with gold stripes on sleeves, pilot cap, aviator sunglasses"),
    ("construction_worker", "orange high-visibility vest, hard hat, work jeans, steel-toe boots, tool belt"),
    ("business_ceo", "tailored navy pinstripe three-piece suit, silk tie, cufflinks, expensive watch, leather shoes"),
    ("race_driver", "NASCAR racing suit with sponsor patches, racing gloves, racing boots, helmet tucked under arm"),
]

POSES = [
    "standing with arms crossed confidently, feet shoulder-width apart",
    "pointing directly at camera with right index finger, other hand on hip",
    "standing in power pose with fists on hips, chest out",
    "arms raised in victory celebration, fists pumped overhead",
    "leaning forward assertively with both hands on knees",
    "fighting stance with one fist forward, back knee bent, ready to strike",
    "holding a prop in right hand raised above shoulder, left hand on hip",
    "three-quarter view turned 30 degrees, looking at camera over shoulder",
]

CAMERAS = [
    "full body wide shot, camera at eye level, skeleton centered in frame",
    "slight low angle hero shot looking up at the skeleton, dramatic perspective",
    "medium shot from chest to feet, camera at chest height with slight upward angle",
    "three-quarter view from the left, skeleton turned 30 degrees, full body visible",
]

LIGHTINGS = [
    "Professional studio photography lighting: key light from upper-left, strong rim light on bone edges",
    "Dramatic split lighting from the side, rim light on edges",
    "Soft diffused studio lighting from all angles, minimal shadows",
    "High-contrast cinematic lighting with strong backlight creating a halo glow",
]


def build_prompt(outfit: str, pose: str, camera: str, lighting: str) -> str:
    parts = [
        BASE_STYLE,
        f"Single skeleton wearing {outfit}. {pose.capitalize()}.",
        BACKGROUND,
        camera.capitalize() + ".",
        lighting + ".",
        "Slight depth of field blur on background.",
    ]
    return " ".join(parts)


def build_caption(label: str, pose: str) -> str:
    return (
        f"{TRIGGER_TOKEN}, single skeleton with large realistic eyeballs, translucent gel body shell, "
        f"{label}, {pose.split(',')[0].strip()}, teal studio backdrop, photorealistic 3D render"
    )


async def generate_one_image(prompt: str, output_path: str, retries: int = 3) -> bool:
    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "num_images": 1,
        "aspect_ratio": "9:16",
        "output_format": "png",
    }
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(GROK_IMAGINE_URL, headers=headers, json=payload)
                if resp.status_code not in (200, 201):
                    print(f"  [WARN] fal returned {resp.status_code} attempt {attempt+1}")
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                data = resp.json()
            images = data.get("images", [])
            if not images:
                print(f"  [WARN] no images attempt {attempt+1}")
                await asyncio.sleep(5)
                continue
            url = images[0].get("url", "")
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                img_resp = await client.get(url)
                if img_resp.status_code == 200:
                    with open(output_path, "wb") as f:
                        f.write(img_resp.content)
                    return True
        except Exception as e:
            print(f"  [ERROR] attempt {attempt+1} failed: {e}")
            await asyncio.sleep(5 * (attempt + 1))
    return False


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples-per-outfit", type=int, default=4,
                        help="Images generated per outfit category (default 4 × 60 outfits = 240 images)")
    args = parser.parse_args()

    if not FAL_AI_KEY:
        print("ERROR: FAL_AI_KEY not set")
        return
    OUTPUT_DIR.mkdir(exist_ok=True)
    CAPTIONS_DIR.mkdir(exist_ok=True)

    random.seed(2026)
    rng = random.Random(2026)

    # v3 indices start at 3000 to avoid collision with v1 (0-114) and v2
    # (1000-2199 covering outfit + action variants).
    img_idx = 3000
    prompts: list[tuple[int, str, str, str]] = []
    for label, outfit in OUTFITS:
        for _ in range(args.samples_per_outfit):
            pose = rng.choice(POSES)
            camera = rng.choice(CAMERAS)
            lighting = rng.choice(LIGHTINGS)
            prompt = build_prompt(outfit, pose, camera, lighting)
            caption = build_caption(label, pose)
            prompts.append((img_idx, prompt, caption, label))
            img_idx += 1

    total = len(prompts)
    print(f"\n{'='*60}")
    print(f"  Skeleton Dataset v3 (translucent + eyeballs)")
    print(f"  Outfits: {len(OUTFITS)}  samples/outfit: {args.samples_per_outfit}")
    print(f"  Total: {total} images  est cost ~${total * 0.02:.2f}")
    print(f"{'='*60}\n")

    success = 0
    failed = 0
    skipped = 0
    batch_size = 6

    for i in range(0, total, batch_size):
        batch = prompts[i:i + batch_size]
        tasks = []
        for idx, prompt, caption, label in batch:
            safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in label)[:40]
            filename = f"skeleton_{idx:04d}_{safe}.png"
            filepath = str(OUTPUT_DIR / filename)
            if Path(filepath).exists():
                skipped += 1
                continue
            tasks.append((idx, prompt, caption, filepath, filename))

        async def gen(idx, prompt, caption, filepath, filename):
            nonlocal success, failed
            ok = await generate_one_image(prompt, filepath)
            if ok:
                (CAPTIONS_DIR / filename.replace(".png", ".txt")).write_text(caption)
                (CAPTIONS_DIR / filename.replace(".png", "_prompt.txt")).write_text(prompt)
                size_kb = Path(filepath).stat().st_size / 1024
                print(f"  [{success + failed + 1}/{total - skipped}] OK: {filename} ({size_kb:.0f} KB)")
                success += 1
            else:
                print(f"  [{success + failed + 1}/{total - skipped}] FAILED: {filename}")
                failed += 1

        if tasks:
            await asyncio.gather(*[gen(*t) for t in tasks])
            print(f"  --- spent ${success * 0.02:.2f}, success {success}/{total - skipped} ---")

    print(f"\n{'='*60}")
    print(f"  DONE. Success {success}, failed {failed}, skipped {skipped}")
    print(f"  Estimated cost: ${success * 0.02:.2f}")
    print(f"{'='*60}")

    manifest = {
        "version": "v3",
        "total_images_this_run": success,
        "trigger_token": TRIGGER_TOKEN,
        "style_signature": "large realistic eyeballs + translucent gel body shell (Cryptic Science canonical)",
        "outfits_covered": len(OUTFITS),
        "samples_per_outfit": args.samples_per_outfit,
        "spent_usd_estimate": round(success * 0.02, 2),
    }
    (OUTPUT_DIR / "dataset_manifest_v3.json").write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {OUTPUT_DIR / 'dataset_manifest_v3.json'}")


if __name__ == "__main__":
    asyncio.run(main())
