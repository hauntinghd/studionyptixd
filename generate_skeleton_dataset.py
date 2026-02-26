"""
NYPTID Skeleton Fine-Tuning Dataset Generator
Generates 200+ training images using Grok Imagine for DreamBooth/LoRA SDXL training.
Each image: photorealistic 3D skeleton in outfit, teal studio backdrop, 9:16 portrait.
"""

import asyncio
import httpx
import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
FAL_AI_KEY = os.getenv("FAL_AI_KEY", "")
GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"
OUTPUT_DIR = Path("skeleton_training_dataset")
CAPTIONS_DIR = OUTPUT_DIR / "captions"

TRIGGER_TOKEN = "nyptid_skeleton"

PROFESSIONS = [
    ("NASCAR driver", "full NASCAR racing suit with sponsor patches, racing gloves, racing boots, helmet tucked under arm"),
    ("Formula 1 driver", "F1 racing suit with team colors and sponsor logos, racing gloves, racing boots, open-face helmet"),
    ("surgeon", "teal surgical scrubs, white lab coat, stethoscope around neck, surgical cap, latex gloves"),
    ("firefighter", "full turnout gear with reflective yellow stripes, fire helmet, oxygen tank on back, heavy boots"),
    ("astronaut", "white NASA spacesuit with American flag patch, space helmet held at side, heavy boots, mission patches"),
    ("chef", "double-breasted white chef coat with black buttons, tall chef hat (toque), checkered pants, kitchen apron"),
    ("police officer", "dark blue police uniform with badge, utility belt with radio, police cap, black boots"),
    ("military soldier", "full camouflage combat uniform, tactical vest, combat helmet, military boots, dog tags"),
    ("pilot", "navy blue airline pilot uniform with gold stripes on sleeves, pilot cap, aviator sunglasses"),
    ("construction worker", "orange high-visibility vest, hard hat, work jeans, steel-toe boots, tool belt"),
    ("boxer", "red boxing shorts with gold trim, boxing gloves, boxing boots, championship belt"),
    ("basketball player", "NBA jersey with number and team logo, basketball shorts, high-top sneakers, headband"),
    ("football player", "full football uniform with pads, football helmet held at side, cleats, team jersey"),
    ("soccer player", "soccer jersey with number, soccer shorts, soccer cleats, shin guards visible"),
    ("scientist", "white lab coat over dress shirt, safety goggles on forehead, latex gloves, clipboard"),
    ("business CEO", "tailored navy pinstripe three-piece suit, silk tie, cufflinks, expensive watch, leather shoes"),
    ("rapper", "oversized designer hoodie, heavy gold chains, diamond-encrusted watch, designer sneakers, baseball cap sideways"),
    ("cowboy", "leather cowboy hat, denim jacket with fringe, cowboy boots with spurs, belt with large buckle, bandana"),
    ("ninja", "full black ninja outfit (shinobi shozoku), ninja mask covering skull face, katana sword on back"),
    ("medieval knight", "polished silver plate armor with helmet visor up, red cape, broadsword, shield with crest"),
    ("surgeon in OR", "full surgical gown, surgical mask pulled down around neck, surgical loupes, scrub cap"),
    ("rock star", "leather jacket with metal studs, ripped jeans, combat boots, electric guitar, chain necklace"),
    ("pirate", "tricorn hat, long red coat with gold buttons, eye patch, cutlass sword, leather boots, gold earring"),
    ("samurai", "full samurai armor (yoroi) with kabuto helmet, katana drawn, menacing stance"),
    ("hockey player", "full hockey gear with team jersey, hockey helmet with cage visor, hockey stick, skates"),
    ("tennis player", "white polo shirt, tennis shorts, tennis shoes, headband, tennis racket, wristbands"),
    ("golfer", "polo shirt tucked into khaki pants, golf cap, golf glove on one hand, golf club"),
    ("DJ", "headphones around neck, graphic tee, bomber jacket, sneakers, standing behind turntables"),
    ("doctor", "white lab coat, stethoscope, dress shirt and tie underneath, slacks, dress shoes"),
    ("mechanic", "oil-stained navy coveralls, red shop rag in pocket, wrench in hand, steel-toe boots"),
]

POSES = [
    "standing with arms crossed confidently, feet shoulder-width apart",
    "pointing directly at camera with right index finger, other hand on hip",
    "leaning forward assertively with both hands on knees",
    "gesturing with both hands spread wide, palms up, shrugging",
    "holding a prop in right hand raised above shoulder, left hand on hip",
    "standing in power pose with fists on hips, chest out",
    "mid-stride walking toward camera, one arm swinging",
    "crouching slightly with one knee bent, ready stance",
    "arms raised in victory celebration, fists pumped overhead",
    "sitting on a stool, one leg crossed over the other, relaxed",
    "leaning against an invisible wall with arms folded, casual",
    "both arms extended to sides with palms down, presenting",
    "one hand behind head scratching, other hand gesturing confusion",
    "thumbs up with both hands, big confident stance",
    "hands together in prayer/thinking position near chest",
]

PROPS = [
    "stacks of hundred dollar bills floating in the air around the skeleton",
    "a golden trophy held triumphantly overhead",
    "a briefcase overflowing with cash in one hand",
    "gold bars stacked on a pedestal beside the skeleton",
    "a diamond-encrusted crown being placed on the skull",
    "a oversized novelty check showing $1,000,000",
    "a miniature luxury sports car on a display stand nearby",
    "falling confetti and money bills raining down",
    "a glowing neon VS sign between two characters",
    "coins spilling out of a treasure chest at their feet",
    "a large gold medal hanging around the neck",
    "a microphone held up to the jaw as if speaking",
    "a clipboard with charts and graphs showing upward trends",
    "a stopwatch in one hand showing a fast time",
    "",
]

CAMERA_ANGLES = [
    "medium shot from chest to feet, camera at chest height with slight upward angle",
    "full body wide shot, camera at eye level, skeleton centered in frame",
    "slight low angle hero shot looking up at the skeleton, dramatic perspective",
    "medium close-up from waist up, shallow depth of field on background",
    "three-quarter view from the left, skeleton turned 30 degrees, full body visible",
    "three-quarter view from the right, skeleton turned 30 degrees away, looking back at camera",
    "slight overhead angle looking down at skeleton, full body visible, heroic framing",
]

LIGHTING_VARIATIONS = [
    "Professional studio photography lighting: key light from upper-left, fill light from right, strong rim light on bone edges",
    "Dramatic split lighting from the side, one half brightly lit, other in shadow, rim light on edges",
    "Soft diffused studio lighting from all angles, minimal shadows, clean and even illumination",
    "High-contrast cinematic lighting with strong backlight creating a halo glow, dark shadows on front",
    "Warm golden hour-style studio lighting from the right, subtle orange tint on bones",
]

BASE_STYLE = "Photorealistic 3D render, Unreal Engine 5 quality, octane render, 8K resolution. Glossy ivory chrome white anatomical human skeleton bones with subtle metallic reflections, clean and polished surfaces."
BACKGROUND = "Solid clean teal mint green (#5AC8B8) studio backdrop with smooth gradient lighting. No environments, rooms, or outdoor scenes."

VS_SCENE_PAIRS = [
    ("NASCAR driver", "Formula 1 driver"),
    ("surgeon", "dentist in white coat with dental mirror"),
    ("business CEO", "rapper"),
    ("astronaut", "pilot"),
    ("boxer", "MMA fighter in fight shorts and gloves"),
    ("basketball player", "football player"),
    ("firefighter", "police officer"),
    ("chef", "fast food worker in uniform and visor"),
    ("scientist", "engineer in hard hat and safety vest"),
    ("medieval knight", "samurai"),
]


def build_prompt(profession_name: str, outfit: str, pose: str, prop: str, camera: str, lighting: str, is_vs: bool = False, second_outfit: str = "") -> str:
    parts = [BASE_STYLE]
    if is_vs:
        parts.append(f"TWO glossy white skeletons facing each other with dramatic split lighting between them. Left skeleton wearing {outfit}. Right skeleton wearing {second_outfit}. Both in {pose}.")
    else:
        parts.append(f"Single glossy white anatomical skeleton wearing {outfit}. {pose.capitalize()}.")

    if prop:
        parts.append(prop.capitalize() + ".")
    parts.append(BACKGROUND)
    parts.append(camera.capitalize() + ".")
    parts.append(lighting + ".")
    parts.append("Slight depth of field blur on background. Polished bone surfaces with subtle metallic reflections.")
    return " ".join(parts)


def build_caption(profession_name: str, outfit: str, pose: str, prop: str, camera: str, lighting: str, is_vs: bool = False) -> str:
    parts = [TRIGGER_TOKEN]
    if is_vs:
        parts.append("versus scene, two skeletons facing each other")
    else:
        parts.append("single skeleton")
    parts.append(profession_name)
    pose_short = pose.split(",")[0].strip()
    parts.append(pose_short)
    if prop:
        prop_short = prop.split(" floating")[0].split(" held")[0].split(" in ")[0][:50]
        parts.append(prop_short)
    cam_short = camera.split(",")[0].strip()
    parts.append(cam_short)
    light_short = lighting.split(":")[0].strip()
    parts.append(light_short)
    parts.append("teal studio backdrop")
    parts.append("photorealistic 3D render")
    return ", ".join(parts)


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
                    print(f"  [WARN] Grok returned {resp.status_code} on attempt {attempt+1}: {resp.text[:200]}")
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                data = resp.json()

            images = data.get("images", [])
            if not images:
                print(f"  [WARN] No images returned on attempt {attempt+1}")
                await asyncio.sleep(5)
                continue

            cdn_url = images[0].get("url", "")
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                img_resp = await client.get(cdn_url)
                if img_resp.status_code == 200:
                    with open(output_path, "wb") as f:
                        f.write(img_resp.content)
                    return True
        except Exception as e:
            print(f"  [ERROR] Attempt {attempt+1} failed: {e}")
            await asyncio.sleep(5 * (attempt + 1))

    return False


async def main():
    if not FAL_AI_KEY:
        print("ERROR: FAL_AI_KEY not set in .env")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)
    CAPTIONS_DIR.mkdir(exist_ok=True)

    prompts = []
    img_idx = 0

    import random
    random.seed(42)

    for prof_name, outfit in PROFESSIONS:
        for pose_i in range(3):
            pose = POSES[(hash(prof_name) + pose_i) % len(POSES)]
            prop = PROPS[(hash(prof_name) + pose_i) % len(PROPS)]
            camera = CAMERA_ANGLES[(hash(prof_name) + pose_i) % len(CAMERA_ANGLES)]
            lighting = LIGHTING_VARIATIONS[(hash(prof_name) + pose_i) % len(LIGHTING_VARIATIONS)]

            prompt = build_prompt(prof_name, outfit, pose, prop, camera, lighting)
            caption = build_caption(prof_name, outfit, pose, prop, camera, lighting)
            prompts.append((img_idx, prompt, caption, prof_name))
            img_idx += 1

    for left, right in VS_SCENE_PAIRS:
        left_outfit = dict(PROFESSIONS).get(left, left)
        right_outfit = right if " in " in right else dict(PROFESSIONS).get(right, right)
        pose = "both in confident power stance facing each other"
        prop = "a glowing neon VS sign between two characters"
        camera = "full body wide shot, camera at eye level, both skeletons centered in frame"
        lighting = "Dramatic split lighting from the side, one half brightly lit, other in shadow, rim light on edges"

        prompt = build_prompt(left, left_outfit, pose, prop, camera, lighting, is_vs=True, second_outfit=right_outfit)
        caption = build_caption(f"{left} vs {right}", "", pose, prop, camera, lighting, is_vs=True)
        prompts.append((img_idx, prompt, caption, f"vs_{left}_{right}"))
        img_idx += 1

    extra_money_scenes = [
        "Photorealistic 3D render, Unreal Engine 5. Single glossy white anatomical skeleton wearing a tailored black tuxedo with bow tie, standing in confident pose with both arms raised, hundred dollar bills cascading down like rain all around. Solid teal mint green studio backdrop. Professional studio lighting with rim light. 8K.",
        "Photorealistic 3D render, Unreal Engine 5. Single glossy white anatomical skeleton wearing a red championship boxing robe, standing with arms crossed, a massive stack of gold bars on a pedestal beside them. Solid teal mint green studio backdrop. Dramatic split lighting. 8K.",
        "Photorealistic 3D render, Unreal Engine 5. Single glossy white anatomical skeleton wearing casual streetwear hoodie and jeans, holding up an oversized novelty check showing $1,000,000 with both hands. Solid teal mint green studio backdrop. Soft studio lighting. 8K.",
        "Photorealistic 3D render, Unreal Engine 5. Two glossy white skeletons side by side, winner skeleton on left slightly taller wearing golden crown and royal cape, loser skeleton on right slumped with empty pockets. Solid teal mint green studio backdrop. Dramatic lighting. 8K.",
        "Photorealistic 3D render, Unreal Engine 5. Single glossy white anatomical skeleton wearing a judges robe and white wig, holding a golden gavel, sitting in a high-backed chair. Solid teal mint green studio backdrop. Professional studio lighting. 8K.",
    ]
    for extra_prompt in extra_money_scenes:
        caption = f"{TRIGGER_TOKEN}, single skeleton, money scene, special prop, professional studio lighting, teal studio backdrop, photorealistic 3D render"
        prompts.append((img_idx, extra_prompt, caption, "extra_money"))
        img_idx += 1

    close_up_prompts = []
    for prof_name, outfit in PROFESSIONS[:10]:
        p = f"Photorealistic 3D render, Unreal Engine 5. Close-up of glossy white anatomical skeleton skull and upper chest wearing {outfit}. Skull facing camera directly, jaw slightly open as if speaking. Solid teal mint green studio backdrop. Professional studio photography lighting with strong rim light on bone edges. Slight depth of field. 8K."
        c = f"{TRIGGER_TOKEN}, close-up, {prof_name}, skull facing camera, teal studio backdrop, photorealistic 3D render"
        close_up_prompts.append((img_idx, p, c, f"closeup_{prof_name}"))
        img_idx += 1

    prompts.extend(close_up_prompts)

    total = len(prompts)
    print(f"\n{'='*60}")
    print(f"  NYPTID Skeleton Dataset Generator")
    print(f"  Total images to generate: {total}")
    print(f"  Output: {OUTPUT_DIR.absolute()}")
    print(f"  Estimated cost: ~${total * 0.02:.2f} (Grok Imagine)")
    print(f"{'='*60}\n")

    success = 0
    failed = 0
    batch_size = 4
    cost_per_image = 0.02

    for batch_start in range(0, total, batch_size):
        batch = prompts[batch_start:batch_start + batch_size]
        tasks = []
        for idx, prompt, caption, label in batch:
            filename = f"skeleton_{idx:04d}_{label.replace(' ', '_')[:30]}.png"
            filepath = str(OUTPUT_DIR / filename)

            if Path(filepath).exists():
                print(f"  [{idx+1}/{total}] SKIP (exists): {filename}")
                caption_path = str(CAPTIONS_DIR / filename.replace(".png", ".txt"))
                if not Path(caption_path).exists():
                    with open(caption_path, "w") as f:
                        f.write(caption)
                success += 1
                continue

            tasks.append((idx, prompt, caption, filepath, filename))

        async def gen_with_caption(idx, prompt, caption, filepath, filename):
            nonlocal success, failed
            ok = await generate_one_image(prompt, filepath)
            if ok:
                caption_path = str(CAPTIONS_DIR / filename.replace(".png", ".txt"))
                with open(caption_path, "w") as f:
                    f.write(caption)

                prompt_log_path = str(CAPTIONS_DIR / filename.replace(".png", "_prompt.txt"))
                with open(prompt_log_path, "w") as f:
                    f.write(prompt)

                size_kb = Path(filepath).stat().st_size / 1024
                print(f"  [{idx+1}/{total}] OK: {filename} ({size_kb:.0f} KB)")
                success += 1
            else:
                print(f"  [{idx+1}/{total}] FAILED: {filename}")
                failed += 1

        await asyncio.gather(*[gen_with_caption(*t) for t in tasks])

        if tasks:
            spent = (success + failed) * cost_per_image
            print(f"  --- Batch done. Progress: {success + failed}/{total} | OK: {success} | Failed: {failed} | Spent: ~${spent:.2f} ---")
            await asyncio.sleep(1)

    print(f"\n{'='*60}")
    print(f"  DONE!")
    print(f"  Success: {success}/{total}")
    print(f"  Failed:  {failed}/{total}")
    print(f"  Total cost: ~${success * cost_per_image:.2f}")
    print(f"  Images: {OUTPUT_DIR.absolute()}")
    print(f"  Captions: {CAPTIONS_DIR.absolute()}")
    print(f"{'='*60}")

    manifest = {
        "total_images": success,
        "trigger_token": TRIGGER_TOKEN,
        "base_model": "stabilityai/stable-diffusion-xl-base-1.0",
        "training_method": "DreamBooth LoRA",
        "image_size": "9:16 portrait (1024x1824 for SDXL training)",
        "style": "Photorealistic 3D skeleton, teal backdrop, UE5 quality",
        "professions_covered": len(PROFESSIONS),
        "vs_scenes": len(VS_SCENE_PAIRS),
        "close_ups": 10,
        "extra_scenes": len(extra_money_scenes),
    }
    with open(OUTPUT_DIR / "dataset_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n  Manifest saved to {OUTPUT_DIR / 'dataset_manifest.json'}")


if __name__ == "__main__":
    asyncio.run(main())
