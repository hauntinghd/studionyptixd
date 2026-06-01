# Tactical Playbook — AI Host: Retired Teacher / Educator

> **DEPRECATED (2026-04-29).** Superseded by `T1-ai-host-setup.md` (profession matrix). Load T1 instead. Preserved for reference only.

## The goal

Generate a reference image of a retired teacher (or current educator) sitting in their home study or living room, photographed as if they had set up a webcam to record a YouTube video. The output must read as: **"this is a real retired teacher who taught for 30 years and now wants to share what she knows,"** not as **"this is an actor playing 'kindly teacher' in a stock photo."**

This playbook is the canonical "warm authority figure for senior content" reference. It works for channels in retirement-finance (the surviving-spouse audience trusts retired teachers more than financial advisors), home-management content for retirees, faith-adjacent teaching content, or any niche where the "wise educator" register is the brand.

This playbook applies to:
- Retirement finance commentary aimed at the surviving-spouse / widow demographic
- Genealogy / family history channels
- Home-management / household tips for retirees
- Educational explainer channels with a "teacher" register
- Faith-adjacent or moral-instruction content
- The retired-teacher composite character used in personal-finance authority channel example episodes (retired Cleveland teacher)

## The non-negotiable details

These MUST be present or the image fails:

1. **Visible teacher signaling WITHOUT corporate gloss** — a single shelf of books visible behind, possibly a small framed certificate (NOT a wall of degrees), maybe a globe or a framed map. The emphasis is on lived-in literacy, not credential display.
2. **Home setting that reads as "teacher's home"** — slightly cluttered with personal items, books on shelves (not perfectly arranged), maybe a small desk with papers, a coffee mug. Could be a home study, a kitchen table, a sunroom, or a living room.
3. **Warm soft lighting from window + lamp** — natural light is the dominant register for this archetype. Teachers in studio light read like substitute teachers in a corporate training video. Real homes have soft light.
4. **Mid-shot framing, slightly off-center** — head, shoulders, with personal home environment visible. The room IS the brand.
5. **Age 60-75 default** — older than active teaching, younger than "frail elderly." The sweet spot is "still sharp, has lived experience, recently retired."
6. **Calm, slightly warm expression** — soft smile or warm-neutral. NOT broad smiling (looks fake), NOT serious-stern (looks like principal). The expression is "I have something I want to teach you, and I'm patient with you."
7. **Casual but tidy wardrobe** — cardigan over a blouse or shirt is the canonical look. Plus possibly a delicate necklace or earrings for female variant. NEVER a suit. NEVER pajamas-casual.
8. **Direct or near-direct eye contact** — engaged, present, addressing the viewer.
9. **Subtle imperfection** — slightly askew sweater, hair not perfectly styled, a bookmark sticking out of a book on the desk. The eye registers these as "real person" within 0.3 seconds.
10. **Photorealistic rendering** — never illustrated, never anime, never stylized.

## The do-not list

These ruin the output:

- ❌ Standing in front of a chalkboard (cliché; teacher is at home, not at school)
- ❌ Wearing a "teacher" outfit costume (apple brooch, school-themed sweater) — registers as costume
- ❌ Wall of framed diplomas (over-credentialed)
- ❌ Stack of red-pen-graded papers prominently displayed (cliché)
- ❌ Holding an apple or globe to the camera (forced visual metaphor)
- ❌ Three-point cinematic lighting
- ❌ Stock-photo "kindly teacher" smile (uncanny)
- ❌ Wearing a suit jacket or blazer (too corporate; teachers at home are casual)
- ❌ Glasses pushed up onto forehead pose (cliché, "teaching" register)
- ❌ Setting that looks like a school classroom (we are at HOME)
- ❌ Smudged red lipstick or perfect makeup (professional photography aesthetic, not real life)

## The locked prompt template

```
A [AGE: 65-72] year old [GENDER: female/male] retired American schoolteacher sitting at a small wooden table in their home study or kitchen, photographed as if they had set up a webcam to record a YouTube video. They are wearing a [WARDROBE: warm cardigan over a simple blouse / cardigan over collared shirt / soft sweater] in [COLOR: cream / soft gray / warm earth tone / muted sage], slightly worn-looking but clean. Hair: shoulder-length or short, gray or silver-white, slightly imperfect (not styled). Behind them: a wooden bookshelf with rows of books not perfectly aligned (some leaning, a few well-worn), a single small framed certificate or family photo on the wall, possibly a small plant or a globe. On the table: a coffee mug, a couple of papers, a bookmarked book. The room is gently lit by warm afternoon sunlight from a window to the left and a small lamp providing additional warm fill. The subject is framed mid-shot from chest up, slightly off-center, looking just slightly to the right of the camera with a calm, warm expression — slight smile, eyes engaged, NOT staring intensely. Photorealistic, captured as if with a basic webcam at eye level or slightly below. Realistic age detail with natural lines around the eyes, NO skin smoothing, hair NOT perfectly styled. Aesthetic: real retired teacher in her actual home, NOT a school setting, NOT a studio, NOT stock-photo. 16:9, photorealistic, warm natural color, no filter.
```

## Model selection

| Model | Performance | Notes |
|---|---|---|
| **Nano Banana (Gemini 3 Pro Image)** | ⭐⭐⭐⭐⭐ Best | Excellent at warm-authority register, age detail, soft lighting |
| Flux 1.1 Pro | ⭐⭐⭐⭐ Good | Photoreal but tends to over-style |
| Midjourney v7 | ⭐⭐⭐ OK | Renders too cinematic by default |
| DALL-E 3 | ⭐⭐ Weak | Stock-photo aesthetic |

**Default: Nano Banana.**

## Common variations

### Older retired teacher (75-85, "wise grandmother who taught for 40 years")
Modify: increase age range. Add: silver-white hair, more pronounced age lines, perhaps reading glasses on a chain. Useful for genealogy, family history, or strong-elder-authority register.

### Younger active teacher (40-55, "current teacher sharing professional advice")
Modify: decrease age range. Wardrobe slightly less casual, more fitted. Setting could include a small "teacher prep" corner. Useful for active-teaching advice content.

### Male variant
Wardrobe: cardigan or sweater vest over collared shirt. Hair: short, gray. Same room aesthetic. Less cliché-prone than female because "male teacher" register is less visually loaded in stock photography.

### Specialty signaling
- **English / literature teacher:** more books, possibly poetry or classic literature visible. Pen and notebook on desk.
- **Math teacher:** simpler setup, maybe a small blackboard with chalk visible somewhere subtle.
- **History teacher (overlaps with war veteran for some channels):** historical maps, framed black-and-white photos, possibly era-specific small artifacts.
- **Art teacher:** colorful items, art books, maybe paint-splattered apron visible on a chair.
- **Music teacher:** sheet music visible, possibly an instrument leaning against a wall.

### Linda variant (retired-teacher composite, personal-finance authority channel example episode)
The composite for Linda specifically: 70-year-old female, retired Cleveland teacher, 34-year career, lives in suburban Ohio. Wardrobe: cardigan over blouse, soft gray-silver hair shoulder-length. Setting: wooden table, modest bookshelf, framed family photos visible. Slight lined hands. Calm, slightly weary expression (she's lived through stuff, including the example episode's SSA overpayment ordeal). When the agent generates Linda for thumbnails or B-roll inserts, this is the locked reference.

## When to use

This playbook is called by:
- **Skill 02 — Thumbnail Design** for senior-targeted educational content
- **Skill 06 — Storyboard / Scene Breakdown** when establishing a teacher-host or composite character
- **Skill 09 — Image Generation Prompting** for retired teacher characters
- **Composite character ingestion** — when the script references a "Linda" or similar retired-teacher composite, this playbook generates the visual

## Integration with Skill 03 (Voice DNA)

This visual playbook pairs with several voice archetypes depending on use:
- **V5 Mentor Coach** — for educational explainer channels where the teacher is sharing knowledge
- **V15 Wise Elder** — for genealogy / family history / faith content
- **V13 Reluctant Witness** — when used as a victim composite in finance/scam-exposure content (the Linda retired-teacher composite)

Match visual age + voice register. A 65-year-old visual with a Hype Showman voice creates immediate dissonance.

## Sample-then-confirm gate

Before locking this character for a channel:
1. Generate 4 variants (different age within range, different room style, different wardrobe color)
2. Surface all 4 to the user
3. User picks one OR asks for adjustments
4. Lock to Channel Memory

## Anti-patterns specific to retired teacher hosts

- **The "stack of papers being graded" cliché** — never render this. Stock-photo register.
- **The "apple on desk" forced metaphor** — kills authenticity instantly.
- **The chalkboard / classroom setting** — the host is at HOME, not at school. School settings register as costume.
- **Posed reading-glasses-pushed-up-onto-forehead** — cliché, reads as "playing teacher."
- **Bright fluorescent lighting** — schools use fluorescent, real homes use warm tungsten or natural. Wrong light kills the at-home register instantly.
- **Multiple "teacher of the year" plaques** — over-credentialed, reads as ego.
- **Excessive school-themed decor** (pencil-cup with apple icons, "World's Best Teacher" mug) — costume register.
- **The "warm-glowing-saint" lighting** — some AI models default to halo-like backlight when prompted for "kindly teacher." Constrain explicitly with "natural daylight, no halo."

## Why these rules

The retired teacher archetype is one of the most over-stocked-photographed in the world, which means audience pattern recognition for "fake teacher" is strong. Senior audiences specifically can spot a generic AI "teacher composite" within seconds. The differentiator is **specific imperfection**: a real teacher's home is not a curated set; a real teacher's clothes are not perfectly coordinated; a real teacher's expression is not a held smile.

Senior audiences trust retired teachers more than they trust financial advisors, lawyers, or doctors for one simple reason: **teachers spent their working lives explaining things to people who didn't understand**. The trust signal is "she will explain this to me at my pace." The visual must support that signal: warm, patient, present, lived-in. Anything that registers as "performance" breaks it.

For finance content where the teacher serves as a victim composite (the Linda retired-teacher composite), the visual register is slightly weighted — she's been through something, and you can see it in her expression. Not despair. Not anger. A kind of patient endurance. This is what makes Linda's story land in the script — the visual reinforces the character's specific human texture.

## Update log

This playbook is current as of April 2026. Update when:
- Image gen models improve warm-authority rendering
- New cultural / regional teacher variants needed (international channels)
- The retired-teacher composite shifts in our content (e.g., new named character beyond Linda)
- The "warm authority" register evolves
