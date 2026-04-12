SKELETON_IMAGE_PROMPT_PREFIX = ""

SKELETON_IMAGE_STYLE_PREFIX = (
    "Photorealistic 3D cinematic character render, Unreal Engine 5 quality. "
    "The skeleton character has a smooth transparent glass-like body shell encasing the full bone structure, "
    "with realistic human-like eyes (visible iris, pupil, wet reflections, natural eye color). "
    "The glass skin catches and refracts lighting creating subtle caustic highlights on the bones inside. "
    "Premium commercial lighting, clean subject separation, realistic materials, natural contrast, crisp detail. "
    "No illustration, no cartoon, no anime, no sketch."
)

SKELETON_MASTER_CONSISTENCY_PROMPT = (
    "MASTER CONSISTENCY RULES (apply to every scene): "
    "One continuous visual universe. Keep the same canonical skeleton identity: skull proportions, eye size/spacing, bone finish, clearly visible translucent body silhouette, and color grade. "
    "For VS videos, lock Character A and Character B identities and never swap them. "
    "Do not change baseline anatomy scene-to-scene. Default to no clothing, uniforms, armor, or costume swaps unless the scene explicitly requests a specific outfit. "
    "Maintain bright readability, premium cinematic detail, visible environmental context, and stable but varied camera language scene-to-scene. "
    "Do not collapse the character into the same blank backdrop or repeated centered hero shot every time."
)

SKELETON_IMAGE_SUFFIX = (
    "Character anatomy rules: ivory-white anatomical skeleton encased in a transparent glass-like body shell, with realistic natural human eyes (visible iris, pupil, wet reflections, natural brown or amber eye color — NOT glowing, NOT hollow, NOT empty sockets). "
    "A clearly visible translucent glass-skin body silhouette around the entire figure is REQUIRED in every scene, but no full human skin face. "
    "Default to no clothing, uniforms, armor, masks, or costumes on the skeleton body unless the scene explicitly requests a specific outfit. "
    "Composition priority: keep the skeleton prominent and mobile-readable while preserving a rich topic-matched background or readable cutaway context. "
    "Vary framing by scene instead of repeating the same centered medium hero shot. "
    "Render as premium photoreal cinematic output with believable materials, natural perspective, clean edges, and readable facial skull detail."
)

TEMPLATE_KLING_MOTION = {
    "skeleton": "Ultra-smooth human-like natural motion: skeleton moves with realistic weight and momentum like a real person, fluid arm gestures, natural head turns with follow-through, subtle breathing chest rise-and-fall. Every joint articulates smoothly with no popping or snapping. Fingers move individually with lifelike dexterity. Eyeballs track and shift naturally with micro-saccades. The translucent body silhouette must remain clearly visible and follow motion naturally with the bones (no clothing physics). Add micro-beat camera accents every ~1-1.5 seconds (gentle punch-in, lateral drift, or slight push) to keep momentum high without jitter. Professional studio lighting stays consistent and bright. Zach D Films quality smooth cinematic motion, absolutely no robotic or jerky movement.",
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
    "clothed skeleton, skeleton in uniform, skeleton in armor, skeleton in racing suit, skeleton with costume, "
    "helmet covering skull, mask covering skull, heavy face paint, "
    "bare-bone skeleton without translucent body silhouette, bones-only look with no translucent body shell, "
    "anatomy model with tiny eyes, medical chart style diagram, "
    "cartoon, anime, low poly, plastic looking, toy, cute, chibi, "
    "skin, flesh, muscles, human face, realistic person, "
    "empty background with no context, plain background, generic blank set, seamless studio void, featureless backdrop, low-detail environment, "
    "flat washed lighting, underexposed muddy shadows, "
    "blurry, low quality, watermark, text artifacts, deformed, "
    "bad anatomy, broken bones, dislocated joints, extra limbs, missing limbs, fused bones, "
    "transparent clothing, see-through clothing, x-ray costume, invisible costume, "
    "jpeg artifacts, pixelated, ugly, low resolution, "
    "inconsistent skull geometry between scenes, changing eye size between scenes, mismatched bone shape between scenes, "
    "glowing eyes, fire eyes, laser eyes, empty eye sockets, no eyes, hollow eyes, "
    "robotic motion, stiff pose, mannequin, puppet, jerky movement, unnatural pose, "
    "no text, no words, no letters, no writing, no readable text, no captions, no titles, no watermark"
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
    "Cinematic photoreal scene, Unreal Engine 5 grade realism with filmic cinematography, "
    "emotionally resonant composition with depth of field and grounded scene detail, "
    "dramatic volumetric lighting with motivated light sources, "
    "ray traced global illumination, atmospheric particles floating, "
    "lens flare, bokeh, film grain, color graded for emotional impact, "
    "richly detailed cinematic environment, 8k ultra HD, award-winning visual, "
)

STORY_NEGATIVE_PROMPT = (
    "cartoon, anime, low poly, flat shading, chibi, cgi, overly rendered cg face, plastic skin, waxy skin, "
    "blurry, low quality, watermark, text artifacts, deformed, "
    "bad anatomy, jpeg artifacts, pixelated, ugly, malformed hands, extra fingers, fused fingers, uncanny face, "
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
    "oversaturated, undersaturated, noise, grain, pixelated, "
    "no text, no words, no letters, no writing, no readable text"
)

WAN22_I2V_HIGH = "wan2.2_i2v_high_noise_14B_fp8_scaled.safetensors"
WAN22_I2V_LOW = "wan2.2_i2v_low_noise_14B_fp8_scaled.safetensors"
WAN22_T2V_HIGH = "wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors"
WAN22_T2V_LOW = "wan2.2_t2v_low_noise_14B_fp8_scaled.safetensors"
