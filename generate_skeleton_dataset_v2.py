"""
NYPTID Skeleton LoRA Training Dataset — Expanded Batch v2.

Goal: generate ~1,200 canonical Cryptic Science skeleton images covering
a far wider variety of outfits, poses, environments, and action cues than
the v1 115-image set. Feeds into LoRA training so that the render pipeline
hits flawless cross-scene identity regardless of topic (1920s FBI, anime
world, Roman centurion, cyberpunk, etc.).

Casey budget: $25–$41 of $43 fal balance. At Grok Imagine $0.02/image,
budget supports 1,250–2,050 images. Target: ~1,200 new (plus 115 existing
= ~1,315 total dataset).

Reuses the v1 caption + manifest format so the existing train_skeleton_lora.py
DreamBooth LoRA pipeline consumes both v1 + v2 together with zero schema
changes.
"""

import asyncio
import json
import os
import random
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()
FAL_AI_KEY = os.getenv("FAL_AI_KEY", "")
GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"
OUTPUT_DIR = Path("skeleton_training_dataset")
CAPTIONS_DIR = OUTPUT_DIR / "captions"

TRIGGER_TOKEN = "nyptid_skeleton"

# v1 already had 30 profession outfits. v2 bumps to 130 by adding:
#   - historical / period (1920s gangster, 1940s soldier, etc.)
#   - anime / manga styles (shounen protagonist, Gojo-style sorcerer, hokage)
#   - fantasy (wizard, elf ranger, dragon knight)
#   - sci-fi (cyberpunk, space marine, mecha pilot)
#   - subculture (biker, punk, goth, emo, grunge)
#   - cultural (samurai, cowboy, viking, mariachi, kabuki)
#   - villain / action (anti-hero, detective, spy, assassin)
#   - casual / streetwear variants
# Purpose: teach the LoRA that the SKELETON is the identity constant while
# outfit is fully variable.
PROFESSIONS_EXT = [
    # v1 carry-over set (already in dataset — skip-if-exists handles dedupe)
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

    # v2 EXPANSIONS — historical / period
    ("1920s FBI agent", "dark pinstripe three-piece wool suit, white collared shirt, black silk necktie, black fedora, polished black leather oxford shoes"),
    ("1920s gangster", "charcoal pinstripe suit, white pocket square, black fedora tilted, Tommy gun in one hand, two-tone spectator shoes"),
    ("1940s soldier WWII", "olive drab US Army uniform, M1 helmet, canvas webbing, black combat boots, M1 Garand rifle slung"),
    ("Victorian gentleman", "black tailcoat, white vest, cravat, pocket watch chain, top hat, walking cane, black leather shoes"),
    ("1970s disco", "white polyester suit with wide lapels, silk shirt unbuttoned, platform shoes, gold medallion chain, afro wig style"),
    ("1980s yuppie", "boxy wide-shoulder suit, suspenders, rolex, brick-sized cellular phone in hand, slicked back hair"),
    ("1950s greaser", "white t-shirt, leather jacket, blue jeans rolled up, black leather boots, slicked hair pompadour"),
    ("Roman legionary", "lorica segmentata armor, gladius sword, scutum shield, red tunic, hobnailed sandals, crested helmet"),
    ("Egyptian pharaoh", "nemes headdress with gold and blue stripes, pleated white kilt, wide gold collar necklace, ornate sandals, crook and flail"),
    ("Viking raider", "horned helmet, chainmail hauberk, fur cloak, round wooden shield, battle axe, leather boots wrapped with laces"),

    # v2 EXPANSIONS — anime / manga
    ("anime shounen protagonist", "orange track jumpsuit with white collar, headband with metal plate, open-toed sandals, spiked hair-style implied on skull"),
    ("Gojo-style sorcerer", "black slim-fit high-collar uniform with silver trim, blindfold over eye sockets, dark fitted pants, black combat boots"),
    ("hokage-style ninja", "white ceremonial cloak with red flame trim over black ninja gi, straw kasa hat, forehead protector, ninja sandals"),
    ("titan-slayer scout", "brown leather straps of ODM gear harness, cream long coat with wing crest, tight white pants, knee-high brown boots"),
    ("hero academy student", "white double-breasted uniform jacket with red piping, red tie, grey pleated pants, white utility belt"),
    ("demon slayer corps", "black gakuran school-uniform style jacket, haori with geometric pattern, hakama pants, tabi socks and zori sandals, katana"),
    ("mecha pilot", "tight white pilot plugsuit with glowing neon blue seams, helmet with visor, chunky space boots"),
    ("magical girl hero", "sailor-style top with big bow, short pleated skirt, thigh-high socks, strappy shoes, oversized hair ribbons"),
    ("anime samurai ronin", "weathered dark blue kimono with faded patterns, straw sandals, loose obi belt, katana on hip, frayed edges"),
    ("shinobi teacher", "sleeveless dark green flak vest over long-sleeved blue undershirt, forehead protector, standard ninja trousers taped at ankles"),

    # v2 EXPANSIONS — fantasy
    ("wizard archmage", "deep blue velvet robe with embroidered silver runes, pointed wizard hat, long wooden staff with glowing crystal orb"),
    ("elf ranger", "hooded green cloak, leather chest piece, quiver of arrows on back, longbow in hand, leaf-shaped brooch clasp"),
    ("dragon knight", "scaled silver plate armor etched with dragon motifs, horned helm, flaming longsword, black dragonhide cape"),
    ("dark sorcerer", "tattered black hooded robe with purple inner lining, bone staff, glowing green eyes visible through hood"),
    ("dwarven blacksmith", "thick leather apron over chainmail, heavy forging hammer, braided beard implied on skull, riveted iron boots"),
    ("orc warlord", "spiked pauldrons, crude iron chest plate, loin cloth, massive double-bladed axe, tusked warhelm"),
    ("high priestess", "white and gold temple robe with wide sleeves, golden laurel headpiece, ceremonial staff, sandals with gold straps"),
    ("bard troubadour", "colorful patchwork doublet, feathered cap, leather lute strapped across back, pointed pixie boots"),
    ("paladin crusader", "polished white plate armor with red cross emblem, tabard, longsword in both hands, halo-style backlight implied"),
    ("shapeshifter druid", "natural-weave forest cloak with leaf patterns, antlered headpiece, gnarled wooden staff, barefoot"),

    # v2 EXPANSIONS — sci-fi / cyberpunk
    ("cyberpunk hacker", "black leather trench coat with LED trim, mirrored techwear visor, cargo pants with tech pockets, combat boots, glowing wrist HUD"),
    ("space marine", "chunky sci-fi power armor in gunmetal grey with team colors, full-face visored helmet, pulse rifle, tactical harness"),
    ("starship captain", "fitted red command uniform with rank insignia, black pants with red stripe, knee-high black boots, authoritative stance"),
    ("dystopian rebel", "hooded survival jacket, tactical vest with ammo pouches, goggles on forehead, jeans with duct-tape patches, combat boots"),
    ("android diplomat", "sleek silver-chrome body suit with seam lighting, minimalist headpiece, smooth polished boots, diplomatic sash"),
    ("cyber samurai", "black neon-trimmed techno-kimono, chrome katana with LED edge, visored mask, magnetic-tread boots"),
    ("neo-noir detective", "long black trench coat over charcoal suit, wide-brim fedora with red LED pinstripe, futuristic pistol, smoke implied"),
    ("galactic bounty hunter", "weathered beskar-style armor, cracked visor helmet, jet pack, wrist blaster, utility cape of rough fabric"),
    ("mecha engineer", "orange jumpsuit with reflective stripes, techwear safety goggles, tool-vest with wrenches and scanners, heavy work boots"),
    ("lunar astronaut 2090", "sleek white spacesuit with neon blue trim, transparent half-visor helmet, sleek boots, chest-panel HUD glow"),

    # v2 EXPANSIONS — subculture / streetwear
    ("biker rocker", "black leather biker jacket with patches, bandana on skull, ripped jeans, heavy chain wallet, chunky black boots"),
    ("goth vampire", "long black Victorian cloak with red velvet lining, ornate silver cross, skinny black trousers, pointed dress shoes, pale makeup implied"),
    ("emo kid", "black skinny jeans, band t-shirt under zip-up hoodie, studded belt, worn converse sneakers, fringed hair style implied"),
    ("grunge musician", "oversized plaid flannel shirt over worn tee, torn jeans, doc marten boots, electric guitar over shoulder, knit beanie"),
    ("punk rocker", "leather vest covered in pins, mohawk implied, ripped plaid pants, combat boots, safety pins through outfit, chain from belt"),
    ("hip-hop streetwear", "oversized graphic tee, designer jeans, chunky dad sneakers, baseball cap backwards, gold layered chains, wrist bands"),
    ("skater", "loose graphic tee, baggy cargo shorts, vans slip-ons, skateboard under arm, wrist guards, backwards cap"),
    ("rave kid", "neon tank top, fluorescent shorts, kandi bracelets up both arms, platform rave boots, glow-stick necklace, fanny pack"),
    ("preppy ivy leaguer", "navy blazer with crest, white oxford shirt, khaki chinos, brown loafers with tassels, rep-striped tie, boat-shoe posture"),
    ("hipster barista", "plaid shirt with rolled sleeves, selvedge denim, suspenders, wax-finish boots, round wire glasses, apron"),

    # v2 EXPANSIONS — action / villain
    ("spy tuxedo", "tailored black tuxedo with satin lapels, white dress shirt, black bow tie, silenced pistol in hand, black leather gloves"),
    ("assassin hood", "matte black tactical hood, fitted black combat bodysuit, wrist-mounted blade, soft-soled boots, utility belt with throwing knives"),
    ("mob boss", "double-breasted charcoal suit with pocket square, wide-brim fedora, Cuban cigar, pinky ring, thick gold watch chain"),
    ("private detective noir", "tan trench coat over rumpled suit, fedora, notebook in hand, smoke rising from cigarette, wingtip shoes"),
    ("thief cat burglar", "fitted black turtleneck, tight black slim pants, lockpicks in breast pocket, thin black gloves, soft-soled ninja boots"),
    ("anarchist", "black hoodie pulled up, black bandana mask over lower skull, tactical cargo pants, combat boots, molotov cocktail in hand"),
    ("masked vigilante", "dark red hooded vigilante outfit with crossed buckles, utility harness, steel-toed boots, domino mask, fingerless gloves"),
    ("corporate villain", "slick all-black turtleneck under black blazer, black slacks, designer black loafers, rimless glasses, sinister posture"),
    ("mafia hitman", "tight black suit with crisp white shirt, black leather gloves, hat pulled low, suitcase with contents implied"),
    ("sniper", "head-to-toe ghillie suit, rifle slung, dirt-streaked face paint on skull, prone-stance implied, digital-camo wrap"),

    # v2 EXPANSIONS — cultural / global
    ("kabuki performer", "elaborate kabuki kimono with bold patterns, red and white kumadori-style face paint on skull, wooden geta sandals, dramatic stance"),
    ("muay thai fighter", "red muay thai shorts with gold trim, mongkol headband, prajioud arm bands, hand-wrapped fists, shirtless torso"),
    ("mariachi singer", "embroidered black charro suit with silver buttons, sombrero with embroidery, wide bow tie, brass instrument in hand"),
    ("flamenco dancer", "fitted short black bolero jacket, tight dress pants, red sash, black dance boots with heel, guitar slung, dramatic pose"),
    ("sumo wrestler", "traditional mawashi belt, topknot hair style implied on skull, bare feet, wide-stance squat, arms spread wide"),
    ("masai warrior", "crimson red shuka wrapped around body, beaded neck collars, tall wooden spear, sandals, one-footed balance stance"),
    ("maori warrior", "traditional piupiu grass skirt, moko tattoo patterns across bones, taiaha staff weapon, bare feet, haka-performance stance"),
    ("tibetan monk", "deep maroon and saffron monk robes, shaved-head implied on skull, wooden prayer beads, barefoot, meditative standing pose"),
    ("inuit hunter", "thick fur-lined parka with hood, sealskin pants, mukluk boots, carved bone harpoon, snow-goggles on forehead"),
    ("zulu warrior", "leopard-print skirt, beaded ankle rings, large cowhide shield, iklwa short spear, bare chest bones"),

    # v2 EXPANSIONS — high-fashion / avant-garde
    ("runway model haute couture", "avant-garde sculpted white couture gown, dramatic shoulders, bold statement earrings, towering heels, editorial pose"),
    ("pop star stage", "glittering silver metallic jumpsuit, over-the-knee boots, wireless microphone, fog-lit stage stance, long cape trailing"),
    ("fashion week editor", "oversized trench coat, statement sunglasses, high-end boots, bold designer bag, aggressive striding pose"),
    ("vogue cover star", "architectural black dress with cutouts, stacks of pearl necklaces, heeled boots, head tilted pose for fashion cover"),
    ("dancer ballet", "classical white tutu and leotard, pointe shoes ribbon-laced, tiara, graceful arabesque pose, arms extended"),
    ("circus ringmaster", "red velvet tailcoat with gold trim, top hat, white jodhpurs, tall riding boots, bullhorn and whip"),
    ("streetwear influencer", "matching beige tech-utility set, bucket hat, chunky dad sneakers, expensive smartphone in hand, cross-body designer bag"),
    ("old hollywood star", "silver sequined gown with fur stole, finger-wave hair style implied, long cigarette holder, pearl choker"),

    # v2 EXPANSIONS — service & trades
    ("lawyer", "dark charcoal suit, white shirt with French cuffs, silk tie, leather briefcase, rimmed glasses, wingtip oxfords"),
    ("accountant", "grey suit, white shirt, conservative tie, thin-rimmed glasses, calculator in pocket, leather briefcase"),
    ("teacher", "tweed blazer with elbow patches, chalk-dusted trousers, reading glasses on chain, stack of books, loafers"),
    ("librarian", "cardigan over modest dress shirt, pencil skirt, reading glasses, stack of ancient books, sensible heels"),
    ("gardener", "canvas overalls with dirt stains, wide-brimmed straw hat, leather work gloves, shovel, worn muck boots"),
    ("plumber", "blue coveralls, tool belt heavy with wrenches, work boots, newsboy cap, wrench in hand, red shop rag"),
    ("electrician", "navy work jumpsuit, hard hat, tool-laden belt, voltage tester in hand, safety goggles on forehead, work boots"),
    ("nurse", "light blue scrubs, stethoscope, white sneakers, nurse's cap, clipboard with patient chart, name badge"),
    ("dental hygienist", "pink medical scrubs, dental mask pulled down, gloves, dental pick tool in hand, protective eye shield"),
    ("barber", "white apron, straight razor in hand, slick 1920s barbershop style, bow tie, rolled-up shirt sleeves with garters"),
]

# v2 pose variations — expanded from v1's 15 to 25 for more diversity.
POSES_EXT = [
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
    "fighting stance with one fist forward, back knee bent, ready to strike",
    "dramatic jumping mid-air with one leg kicked forward, arms spread",
    "looking over shoulder dramatically, half-turned away from camera",
    "kneeling on one knee with head bowed, hand on heart respectfully",
    "waving at camera with big smile implied, other hand on hip",
    "holding an imaginary sword in two-handed ready guard, weight balanced",
    "pointing dramatically into the distance with one arm fully extended",
    "sitting cross-legged floor pose, meditation style, palms on knees",
    "action run forward with outstretched arm leading, full-body in motion",
    "bent over laughing with hand on belly, other hand slapping thigh",
]

# v2 camera angles — 10 variations.
CAMERA_ANGLES_EXT = [
    "medium shot from chest to feet, camera at chest height with slight upward angle",
    "full body wide shot, camera at eye level, skeleton centered in frame",
    "slight low angle hero shot looking up at the skeleton, dramatic perspective",
    "medium close-up from waist up, shallow depth of field on background",
    "three-quarter view from the left, skeleton turned 30 degrees, full body visible",
    "three-quarter view from the right, skeleton turned 30 degrees away, looking back at camera",
    "slight overhead angle looking down at skeleton, full body visible, heroic framing",
    "extreme low angle looking up from ground level, skeleton towering over camera",
    "close-up portrait shot from shoulders up, skull filling the frame",
    "wide establishing shot with skeleton small in frame, emphasizing backdrop",
]

# v2 lighting — 8 variations.
LIGHTING_VARIATIONS_EXT = [
    "Professional studio photography lighting: key light from upper-left, fill light from right, strong rim light on bone edges",
    "Dramatic split lighting from the side, one half brightly lit, other in shadow, rim light on edges",
    "Soft diffused studio lighting from all angles, minimal shadows, clean and even illumination",
    "High-contrast cinematic lighting with strong backlight creating a halo glow, dark shadows on front",
    "Warm golden hour-style studio lighting from the right, subtle orange tint on bones",
    "Cool blue-toned studio lighting, moody and mysterious, rim light picking up edges",
    "Theatrical spotlight from directly above, dramatic pool of light on skeleton, rest in shadow",
    "Neon accent lighting from below, tinted pink and cyan, futuristic vibe, specular highlights",
]

BASE_STYLE = "Photorealistic 3D render, Unreal Engine 5 quality, octane render, 8K resolution. Glossy ivory chrome white anatomical human skeleton bones with subtle metallic reflections, clean and polished surfaces."
BACKGROUND = "Solid clean teal mint green (#5AC8B8) studio backdrop with smooth gradient lighting. No environments, rooms, or outdoor scenes."


def build_prompt(outfit: str, pose: str, camera: str, lighting: str) -> str:
    parts = [BASE_STYLE]
    parts.append(f"Single glossy white anatomical skeleton wearing {outfit}. {pose.capitalize()}.")
    parts.append(BACKGROUND)
    parts.append(camera.capitalize() + ".")
    parts.append(lighting + ".")
    parts.append("Slight depth of field blur on background. Polished bone surfaces with subtle metallic reflections.")
    return " ".join(parts)


def build_caption(profession_name: str, pose: str, camera: str, lighting: str) -> str:
    parts = [TRIGGER_TOKEN, "single skeleton", profession_name]
    parts.append(pose.split(",")[0].strip())
    parts.append(camera.split(",")[0].strip())
    parts.append(lighting.split(":")[0].strip())
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
                    print(f"  [WARN] fal returned {resp.status_code} on attempt {attempt+1}: {resp.text[:200]}")
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

    # Budget / spend control. At Grok Imagine ~$0.02/image, SPEND_USD_BUDGET
    # caps how many images we'll attempt. Leaves Casey a ~$2 buffer in the
    # fal account at the default cap.
    SPEND_USD_BUDGET = float(os.getenv("SPEND_BUDGET_USD", "22.0"))
    COST_PER_IMAGE = 0.02
    MAX_IMAGES = int(SPEND_USD_BUDGET / COST_PER_IMAGE)

    random.seed(2026)
    rng = random.Random(2026)

    # Start img_idx at 1000 so v2 indices never collide with v1 (which used
    # skeleton_0000 through skeleton_0114). This also makes LoRA-training
    # set-membership inspection trivial later.
    img_idx = 1000
    prompts: list[tuple[int, str, str, str]] = []  # (idx, prompt, caption, label)

    for prof_name, outfit in PROFESSIONS_EXT:
        # 8 pose/camera/lighting combinations per outfit — ~8 samples per
        # profession keeps the LoRA from collapsing any single outfit while
        # still letting wildcards (anime, fantasy, sci-fi) hit ~8 images each.
        combo_count = 10
        for _ in range(combo_count):
            pose = rng.choice(POSES_EXT)
            camera = rng.choice(CAMERA_ANGLES_EXT)
            lighting = rng.choice(LIGHTING_VARIATIONS_EXT)

            prompt = build_prompt(outfit, pose, camera, lighting)
            caption = build_caption(prof_name, pose, camera, lighting)
            prompts.append((img_idx, prompt, caption, prof_name.replace(" ", "_")))
            img_idx += 1

    # Also add a small set of action/fighting cross-variants to lock in the
    # "skeleton fighting Goku/Gojo" use case Casey specifically called out.
    ACTION_SCENES = [
        ("shounen_vs_gojo", "skeleton in orange training gi squaring off against blue-uniformed rival sorcerer", "dynamic fighting stance mid-exchange, both feet off ground"),
        ("titan_scout_attack", "skeleton in ODM gear harness launching off a rooftop with swords drawn", "mid-air attacking pose, swords extended, cape streaming"),
        ("demon_slayer_breath", "skeleton in demon slayer uniform with water-breathing technique swirl visible", "downward katana slash, water ribbon motion lines, focused stance"),
        ("hero_smash", "skeleton in hero academy gym uniform cocking back a massive punch", "wind-up attack pose, lightning crackle implied on fist"),
        ("dark_sorcerer_cast", "skeleton in black robes conjuring purple energy in both palms", "arms outstretched casting a spell, energy implied between hands"),
        ("cyber_samurai_duel", "skeleton in neon-edge cyber samurai armor drawing a glowing katana", "iaido draw mid-motion, one knee forward, katana half-drawn"),
        ("space_marine_firing", "skeleton in full power armor firing a pulse rifle", "crouched firing stance, muzzle flash implied, spent casings falling"),
        ("vigilante_rooftop", "skeleton in masked vigilante outfit perched on rooftop edge", "squatting rooftop pose, cape billowing, one hand on edge"),
    ]
    for label, outfit_line, pose_line in ACTION_SCENES:
        for _ in range(6):
            camera = rng.choice(CAMERA_ANGLES_EXT)
            lighting = rng.choice(LIGHTING_VARIATIONS_EXT)
            full_prompt_body = f"Single glossy white anatomical skeleton, {outfit_line}. {pose_line.capitalize()}."
            parts = [BASE_STYLE, full_prompt_body, BACKGROUND, camera.capitalize() + ".", lighting + ".", "Slight depth of field blur. Polished bone surfaces."]
            prompt = " ".join(parts)
            caption = f"{TRIGGER_TOKEN}, single skeleton, {label}, {pose_line.split(',')[0]}, {camera.split(',')[0]}, teal studio backdrop, photorealistic 3D render"
            prompts.append((img_idx, prompt, caption, label))
            img_idx += 1

    # Clip to budget
    if len(prompts) > MAX_IMAGES:
        print(f"  Prompt pool is {len(prompts)} but budget caps at {MAX_IMAGES}. Trimming.")
        prompts = prompts[:MAX_IMAGES]

    total = len(prompts)
    print(f"\n{'='*60}")
    print(f"  NYPTID Skeleton Dataset v2 Generator")
    print(f"  Target images: {total}")
    print(f"  Budget cap: ${SPEND_USD_BUDGET:.2f} @ ${COST_PER_IMAGE}/image = {MAX_IMAGES} max")
    print(f"  Output: {OUTPUT_DIR.absolute()}")
    print(f"  Estimated cost: ~${total * COST_PER_IMAGE:.2f}")
    print(f"{'='*60}\n")

    success = 0
    failed = 0
    skipped_existing = 0
    batch_size = 6  # fal handles ~20 concurrent; stay well under

    for batch_start in range(0, total, batch_size):
        batch = prompts[batch_start:batch_start + batch_size]
        tasks = []
        for idx, prompt, caption, label in batch:
            safe_label = "".join(c if c.isalnum() or c in "_-" else "_" for c in label)[:40]
            filename = f"skeleton_{idx:04d}_{safe_label}.png"
            filepath = str(OUTPUT_DIR / filename)
            if Path(filepath).exists():
                skipped_existing += 1
                continue
            tasks.append((idx, prompt, caption, filepath, filename))

        async def gen_with_caption(idx, prompt, caption, filepath, filename):
            nonlocal success, failed
            ok = await generate_one_image(prompt, filepath)
            if ok:
                (CAPTIONS_DIR / filename.replace(".png", ".txt")).write_text(caption)
                (CAPTIONS_DIR / filename.replace(".png", "_prompt.txt")).write_text(prompt)
                size_kb = Path(filepath).stat().st_size / 1024
                print(f"  [{success + failed + 1}/{total - skipped_existing}] OK: {filename} ({size_kb:.0f} KB)")
                success += 1
            else:
                print(f"  [{success + failed + 1}/{total - skipped_existing}] FAILED: {filename}")
                failed += 1

        if tasks:
            await asyncio.gather(*[gen_with_caption(*t) for t in tasks])
            spent = success * COST_PER_IMAGE
            if spent >= SPEND_USD_BUDGET:
                print(f"\n  *** Budget cap ${SPEND_USD_BUDGET:.2f} reached at ${spent:.2f}. Halting. ***\n")
                break
            print(f"  --- Batch done. OK: {success} | Failed: {failed} | Skipped (already exist): {skipped_existing} | Spent: ~${spent:.2f} ---")
            await asyncio.sleep(1)

    print(f"\n{'='*60}")
    print(f"  DONE.")
    print(f"  Success (new images): {success}")
    print(f"  Failed:  {failed}")
    print(f"  Skipped (already existed): {skipped_existing}")
    print(f"  Total cost (estimated): ~${success * COST_PER_IMAGE:.2f}")
    print(f"  Images dir: {OUTPUT_DIR.absolute()}")
    print(f"{'='*60}")

    manifest = {
        "version": "v2",
        "total_images_this_run": success,
        "trigger_token": TRIGGER_TOKEN,
        "base_model": "stabilityai/stable-diffusion-xl-base-1.0",
        "training_method": "DreamBooth LoRA",
        "image_size": "9:16 portrait (1024x1824 for SDXL training)",
        "style": "Photorealistic 3D skeleton, teal backdrop, UE5 quality",
        "professions_covered": len(PROFESSIONS_EXT),
        "action_scenes": len(ACTION_SCENES),
        "budget_usd": SPEND_USD_BUDGET,
        "spent_usd_estimate": round(success * COST_PER_IMAGE, 2),
    }
    (OUTPUT_DIR / "dataset_manifest_v2.json").write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest saved to {OUTPUT_DIR / 'dataset_manifest_v2.json'}")


if __name__ == "__main__":
    asyncio.run(main())
