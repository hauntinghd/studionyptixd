SKELETON_IMAGE_PROMPT_PREFIX = ""

SKELETON_IMAGE_STYLE_PREFIX = (
    "Photorealistic 3D studio render. Unreal Engine 5 quality. "
    "No illustration, no comic art, no anime, no drawing, no sketch."
)

SKELETON_MASTER_CONSISTENCY_PROMPT = (
    "MASTER CONSISTENCY RULES (apply to every scene): "
    "Keep one continuous visual universe across all scenes. Keep the same skeleton character identity, "
    "same skull shape, same limb proportions, same bone material, same eye style, same color grade, and same camera language. "
    "For VS videos, lock two identities and keep both stable scene-to-scene as Character A and Character B. "
    "Never swap identities. Never change art style. Never switch to illustration/comic/anime. "
    "Maintain photoreal cinematic studio quality in every frame. "
    "Use glossy white bone material with subtle glass-like specular highlights, and realistic reflective eyeballs in every scene. "
    "Outfits must remain role-accurate and fully opaque with realistic fabric folds and stitching. "
    "OUTFIT LOCK: keep the exact same primary outfit design for each locked character across all scenes (same base suit, same main colors, same sponsor/logo placement language, same gloves, same shoes, same accessories) unless the prompt explicitly requests a change. "
    "For motorsport topics, lock one racing-livery identity per character and keep sponsor/logo families, patch positions, and suit color blocking consistent scene-to-scene. "
    "Do not randomly switch brands, uniform style, logo layout, or colorway mid-video. "
    "If a visual detail is missing, infer from topic role while preserving the same identity lock."
)

SKELETON_IMAGE_SUFFIX = (
    "Photorealistic 3D render, Unreal Engine 5, octane render, NOT illustration, NOT cartoon, NOT comic art. "
    "The character has a white SKULL for a head (not a human face) and BONY SKELETON HANDS, "
    "but the entire body from neck to feet is FULLY COVERED by the outfit described above. "
    "No bare ribcage, no exposed spine, no visible pelvis -- the clothes hide all bones below the neck. "
    "It looks like a real person in the outfit but with a clean glossy white bone skull instead of a face, with glass-like highlights. "
    "NOT a real human. NOT a person with skin. The head MUST be a bare white bone skull with eyeballs. "
    "Solid clean teal-blue (#5AC8B8) studio backdrop, professional studio photography lighting. "
    "Preserve outfit continuity across the whole video: same suit identity, same logo family/placement, same color blocking."
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
    "inconsistent outfit between scenes, changing sponsor logos every scene, mismatched racing suit branding, "
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
    "disciplined people training hard (gym, running, boxing, athletic drills) and focused self-improvement moments, "
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
