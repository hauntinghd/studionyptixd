---
name: authentic-setting
description: >-
  Setting matrix for authentic environment generation (workspace, exterior, period). Load when designing a believable scene/environment for an AI host or narrative shot.
---

# Tactical Playbook T2 — Authentic Setting Generation

This is the master playbook for generating any room or environment that reads as a **real place** rather than a styled set. Used for backgrounds in host shots, B-roll cutaways, scene establishments, and any moment where the setting carries narrative weight.

The single rule: **a real place has imperfection.** A real kitchen has dishes in the sink. A real home office has a stack of papers slightly askew. A real classroom has fingerprints on the whiteboard. The agent's job is to render imperfection deliberately — never as accident, never as overdone clutter.

This playbook replaces per-room playbooks. The agent fills the **Setting Matrix** (§3) with situation-specific variables and runs the universal rules.

---

## 1. The job of authentic setting generation

A setting does three things:
1. **Establishes context** — viewer knows where the action is happening
2. **Reinforces the channel's brand** — settings repeat across videos and become recognizable
3. **Pre-screens the audience** — a corporate setting attracts corporate viewers; a lived-in home attracts trust-seeking viewers

The agent's job is to generate a setting that:
- Matches the niche register (authentic / cinematic / stylized)
- Carries period and demographic accuracy when relevant
- Has enough imperfection to feel real
- Has enough cleanliness to feel intentional
- Reads correctly at thumbnail size AND full screen

## 2. The universal non-negotiables

Apply across every setting:

1. **Subtle imperfection is mandatory** — every real room has slightly mismatched items, slight clutter, evidence of recent use. A perfectly arranged room reads as "set design."
2. **One dominant element per setting** — kitchen = food prep area is dominant; office = desk is dominant; bedroom = bed is dominant. Multiple equally-weighted elements confuse the eye.
3. **Period accuracy** — every prop must be of the correct era. A 1970s kitchen with a 2024 microwave is an instant credibility kill.
4. **Lighting matches the setting type** — homes have warm lamps and window light; offices have fluorescent or natural; clinical settings have bright cool. Mismatch destroys realism.
5. **Cultural / regional accuracy** — American kitchen ≠ Japanese kitchen ≠ Italian kitchen. The agent matches the channel's audience demographic.
6. **Wear and patina where appropriate** — wood furniture has scratches, walls have small marks, things show age. Brand-new everything reads as "showroom."
7. **Avoid stock-photo composition** — center-framed wide shot with a "perfect" angle reads as stock. Slightly off-angle, slightly imperfect framing reads as real.
8. **Photorealistic by default** — never illustrated, anime, 3D-rendered unless the channel niche calls for stylized (per T4 register decision).

## 3. The setting matrix

The agent fills this matrix per channel/scene. Each row represents a setting type with the variables that adjust.

| Setting type | Lighting | Dominant element | Period markers | Imperfection cues | Common props | Anti-pattern to avoid |
|---|---|---|---|---|---|---|
| **Home office** | Warm lamp + window | Wood desk | Era books, period computer if any | Coffee mug, papers slightly askew, leaning books | Bookshelf, plant, framed credential, family photo | Skyscraper view, glass walls, minimalist empty |
| **Home kitchen** | Warm overhead + window | Cooking surface | Era appliances (matter for retro) | Dish towel on counter, fruit bowl, recipe paper | Dishes, cookbooks, small plant, magnetic notes on fridge | Show-kitchen perfect, no signs of use |
| **Living room** | Mixed lamp + window | Sofa or fireplace | Era TV/electronics | Throw pillow askew, blanket on couch, magazines on table | Bookshelf, photos, plants, art | Hotel-lobby polished, magazine-perfect |
| **Garage / workshop** | Overhead utility + work lamp | Workbench | Era tools, vehicle if any | Tool spread (not perfectly hung), shavings on bench, calendar with grease marks | Tools, vehicle, work stool, oil stains, posters | Suspiciously clean, organized garage influencer aesthetic |
| **Den / study (older male / war veteran)** | Warm lamp, fireplace if any | Leather chair OR wood desk | Era photos, period books | Slightly weathered furniture, books showing wear, casual prop placement | Leather chair, wood desk, single shadow box, framed photos, fireplace | Memorial wall (over-decorated), too tidy |
| **Classroom** | Fluorescent + window | Whiteboard or chalkboard | Era school decor | Marker smudges, posted student work, mismatched chairs | Desks, board, posters, books, projector | Empty pristine room, "perfect classroom" |
| **Doctor's office (clinical)** | Bright cool + soft lamp | Exam table or desk | Era equipment | Paperwork on counter, otherwise clean | Stethoscope, charts, anatomical posters, equipment | Showroom-empty, magazine-perfect |
| **Lawyer's office** | Warm lamp + window | Wood desk | Era legal books | Stack of papers, cup of coffee, slightly worn chair | Legal books, framed bar cert, leather chair, wood desk | Glass-tower corporate, sterile |
| **Cabin / rural setting** | Warm fireplace + window | Wood furniture or stove | Era rural items | Wood chips by fireplace, hand-knit blanket, well-worn rocking chair | Wood stove, rustic furniture, plaid blankets | Pinterest-perfect "cabin core" aesthetic |
| **Modern apartment / loft** | Mixed natural + accent | Living area or kitchen | Contemporary decor | Lived-in books, art on walls, plants | Modern furniture, art, plants, books, electronics | Empty Instagram-loft aesthetic |
| **Library** | Warm overhead + reading lamps | Shelves of books | Era furniture if visible | Books leaning, stack on table, reading light on | Books, reading chair, table, wood shelves | Pristine corporate library, no signs of recent use |
| **Outdoor / porch / yard** | Natural daylight | Porch chair OR yard space | Era house exterior visible | Slightly worn porch furniture, plants, used items | Rocking chair, plants, garden tools, wind chimes | Magazine-perfect "outdoor living" aesthetic |
| **Hospital room** | Bright fluorescent | Bed or equipment | Era medical equipment | Wires neatly arranged but visible, slight clutter on counter | Bed, IV stand, monitors, chair for visitor | Movie-set dramatic lighting, theatrical |
| **Courtroom** | Bright overhead + windows | Bench or witness stand | Era legal furniture | Slightly worn wood, water glass, pens | Bench, flag, seal, wooden benches | Theatrical movie courtroom, dramatic |
| **Bank vault / interior** | Cool overhead | Vault door OR teller area | Era banking decor | Slight wear on counters, visible security | Vault door, teller windows, security camera | Heist-movie dramatic, exaggerated |
| **Studio (when called for)** | Three-point cinematic | Subject area | Modern professional | Minimal — studios ARE clean | Lights, backdrop, equipment | This entry exists ONLY when channel niche calls for studio aesthetic |

### Filling the matrix

User says: "I need a B-roll cutaway showing a 1970s American kitchen for a history segment about how meals changed."

Agent recognizes: Setting = home kitchen, period = 1970s, region = American.

From matrix row "Home kitchen":
- Lighting: warm overhead + window (specific to era — fluorescent ring lights weren't common in '70s home kitchens; use overhead bulb or pendant)
- Dominant element: cooking surface (electric coil stove for 1970s)
- Period markers: 1970s appliances (avocado green or harvest gold appliances were the era), wood paneling on walls, linoleum flooring
- Imperfection cues: dish towel folded over oven handle, fruit bowl with banana and apples, recipe card on counter, magnetic letters on fridge
- Common props: rotary dial wall phone, ceramic canisters, decorative roosters or mushrooms (1970s kitsch)
- Anti-pattern: anything modern (digital displays, stainless steel, granite counters)

The agent fills the master prompt template (§5) with these variables.

## 4. The home vs corporate vs hybrid decision tree

Before generating a setting, the agent decides which register fits:

**Home register** (90% of channel content):
- Senior finance, doctor advice, faith content, retirement, war veteran storytelling, retired teacher
- Lighting: warm, mixed
- Imperfection: present and visible
- Props: personal items welcome
- Goal: trust signal

**Corporate register** (rare, only when niche demands):
- News commentary, business analysis, specific financial advisor channels
- Lighting: more controlled, less warm
- Imperfection: minimal
- Props: professional only
- Goal: gravitas signal

**Hybrid register** (some channels):
- Tech reviews (clean home but not corporate), some science channels
- Lighting: clean but warm
- Imperfection: present but minimized
- Props: thoughtful curation
- Goal: "professional but human" signal

**The studio register** (only when niche IS studio production):
- Music videos, channel trailers, commercial content
- Lighting: cinematic three-point
- Imperfection: zero
- Props: deliberate
- Goal: production-value signal

The agent never defaults to studio. Only studio when explicitly required.

## 5. The master prompt template

```
A [SETTING TYPE] in [LOCATION/ERA — e.g., "an American suburban home, 2026" or "a 1970s American kitchen"], photographed [STYLE — e.g., "as if a real homeowner snapped a candid phone photo" or "in cinematic establishing shot style"]. The dominant element of the room is [DOMINANT ELEMENT]. Visible details include: [IMPERFECTION CUES — list 3-5 specific lived-in details], [PERIOD MARKERS — list 2-4 specific era cues], and [COMMON PROPS — list 3-5 contextually appropriate items]. The lighting is [LIGHTING DESCRIPTION matched to setting type]. Composition: [FRAMING — slightly off-center / rule of thirds / centered] with [DEPTH — full focus / mid-depth / shallow background blur]. Aesthetic: [REGISTER — authentic lived-in / slightly polished / cinematic — based on niche]. Photorealistic, natural color grade, 16:9, no filter.
```

The agent fills variables from the matrix + channel context + period/region requirements.

## 6. Period accuracy rules

For any non-contemporary setting, the agent runs period research:

**1970s American home:**
- Appliances: avocado green or harvest gold, electric stoves, white-only refrigerators with rounded corners
- Wallpaper: floral patterns, especially in kitchens and bathrooms
- Phone: rotary dial OR push-button beige wall phone
- Lighting: incandescent bulbs, no LED, no fluorescent ring lights
- TV: large console wood-cabinet TV, antenna visible
- Furniture: shag carpet, wood paneling, brown and orange palette

**1980s American home:**
- Appliances: white or black, dishwashers becoming standard
- Phone: push-button, possibly cordless near end of decade
- Lighting: incandescent, occasional fluorescent
- TV: tube TV, VCR appearing
- Furniture: pastel palette in some homes, brass fixtures

**1990s American home:**
- Appliances: white or biscuit
- Phone: cordless standard
- TV: tube TV, larger; VCR/DVD on shelf
- Computer: beige tower PC visible
- Furniture: floral patterns, oak furniture, beige walls

**2000s-2010s American home:**
- Appliances: stainless steel becoming standard mid-2000s
- Phone: landline still common, cell phones increasing
- TV: flat-screen by mid-2000s
- Furniture: granite counters, hardwood floors, neutral palette

**Contemporary (2020s-2026):**
- Appliances: stainless steel or matte black
- Phone: smartphone visible
- TV: 4K flat screen, wall-mounted common
- Smart home: visible Amazon Echo, Nest thermostat possible
- Furniture: open floor plans, white/gray neutral, plants prominent

The agent verifies period accuracy when the script references a specific decade. One wrong-era prop kills credibility for history-aware viewers.

## 7. Cultural / regional accuracy rules

**American (default unless specified):** Western interiors, English-language signage, American electrical outlets (Type A/B), American appliance styling

**British / UK:** Smaller rooms, kettle on counter (always), Type G outlets, often more compact appliances

**Japanese:** Tatami mats, sliding doors, smaller fixtures, specific cooking appliances (rice cooker visible)

**Italian / Mediterranean:** Open kitchens, stovetop espresso maker, ceramic tile, warm earth palette

**Scandinavian:** Light wood, white walls, minimalism with personal touches, natural light prominent

**Indian:** Tile floors, distinct kitchen organization (less wall cabinets, more shelving), specific cooking equipment

The agent matches setting culture to channel audience demographic. A finance channel for American retirees needs American settings; a global tech channel may need region-neutral.

## 8. Lighting per setting type

Lighting is the single most important authenticity signal. Wrong lighting kills realism instantly.

| Setting | Default lighting | Why |
|---|---|---|
| Home (kitchen, living room, study, bedroom) | Warm overhead + window OR warm lamp + window | Real homes have warm tungsten light + cool natural; mix is realistic |
| Garage / workshop | Cool overhead utility + warm work lamp | Functional spaces have functional light |
| Office (corporate) | Cool fluorescent or LED + window | Offices are cooler than homes |
| Office (home) | Warm lamp + window | Home offices read like rooms, not corporate spaces |
| Classroom | Cool fluorescent + window | Schools use commercial lighting |
| Hospital / clinical | Cool bright LED or fluorescent | Clinical = bright cool by convention |
| Cabin / rural | Warm fireplace + warm bulb | Rustic = warm everywhere |
| Outdoor day | Natural daylight with shadows | Real sun has direction; flat lighting reads fake |
| Outdoor evening | Warm sunset / golden hour | Magic hour signals atmosphere |
| Outdoor night | Mixed sources — moon, streetlight, window glow | Real night isn't pitch black |
| Library | Warm overhead + warm reading lamps | Books deserve warm light |
| Studio (only when niche calls) | Three-point cinematic | The exception |

When the user requests a setting, the agent picks lighting from this table by default. Override only with explicit user direction.

## 9. Prop density rules

The number of visible items in a setting is a calibrated decision:

- **Sparse (0-3 visible items):** modernist, minimalist, sterile. Reads as "studio" or "magazine spread." Use rarely.
- **Modest (4-8 visible items):** real lived-in space without clutter. Default for most home settings. The sweet spot.
- **Dense (9-15 visible items):** active workspace, well-used room. Good for kitchens, workshops, dens.
- **Cluttered (16+ visible items):** maximalist. Use only for specific characters (obsessive collector, hoarder, dense workshop).

The agent biases toward Modest by default. Densify only when the setting calls for active use.

## 10. Anti-patterns

**A1 — Showroom aesthetic.** Suspiciously clean, perfectly arranged, no signs of recent use. Reads as "set design."

**A2 — Stock photo composition.** Centered wide shot, evenly distributed elements, "perfect" framing. Reads as commercial.

**A3 — Period mismatch.** Even ONE wrong-era prop kills credibility. Era research is non-negotiable.

**A4 — Cultural mismatch.** American kitchen rendered for Japanese viewer or vice versa. Mismatched kitchen specifics break trust.

**A5 — Wrong lighting register.** Home with three-point cinematic lighting reads fake. Office with warm lamps reads like a hotel.

**A6 — Forced clutter.** Trying too hard to be "lived in" produces obvious staging. Real clutter has logic — books leaning toward each other, papers stacked, items grouped by use.

**A7 — Pinterest-core aesthetic.** Trying to look like a magazine "cabin" or "modern farmhouse" or "Scandi minimalism" reads as influencer staging, not real home.

**A8 — Suspect technology.** Modern devices in retro settings. 1970s kitchen with a smart speaker is a credibility kill.

**A9 — Over-saturated period decor.** "1970s kitchen" doesn't mean every prop screams "1970s." Real 1970s kitchens had some 1960s leftovers and some new-then items. Mix the era markers subtly.

**A10 — Aspect ratio mismatch.** Generating a 16:9 setting then cropping to 9:16 for vertical loses the dominant element. Generate at the right ratio from the start.

## 11. Worked examples

### Example 1 — Personal-finance authority home office (financial advisor)

**Goal:** Background for the personal-finance authority channel's talking-head shots and B-roll inserts

**Matrix lookup:** Home office row
**Lighting:** Warm lamp + window from frame-left
**Dominant element:** Wood desk
**Period:** Contemporary (2026)
**Imperfection cues:** Coffee mug, slightly stacked papers, slightly leaning books on shelf
**Props:** Bookshelf with tax/finance books, single framed CPA cert, small plant, family photo
**Anti-pattern avoided:** No skyscraper view, no glass walls, no multiple framed degrees

**Filled prompt:**
```
A modest American home office, photographed as if a real person snapped a candid photo at their desk. The dominant element is a worn wooden desk by a window. Visible details: a coffee mug to the left of a laptop, a small stack of slightly askew papers, a desk lamp casting warm light, a bookshelf behind with tax reference books and a few personal photos, a single framed CPA certification on the wall (not prominently centered), a small potted plant. Warm afternoon natural light from a window to the left, with the desk lamp providing warm fill. Composition: rule of thirds, slightly off-center on the desk, with the rest of the room reading clearly. Aesthetic: real home office of a tax professional, NOT corporate office, NOT studio. Photorealistic, 16:9, natural color, no filter.
```

### Example 2 — 1970s American kitchen for history B-roll

**Goal:** B-roll cutaway for a personal-finance authority segment about cost-of-living changes since the 1970s

**Matrix lookup:** Home kitchen, period = 1970s, region = American
**Lighting:** Warm overhead pendant + window
**Dominant element:** Cooking surface (electric stove)
**Period markers:** Avocado green appliances, wood paneling, linoleum floor, rotary dial phone, decorative roosters on wall
**Imperfection cues:** Dish towel on oven handle, recipe card on counter, fruit bowl with banana, magnetic letters on fridge
**Props:** Refrigerator, electric stove, ceramic canisters, wall phone, small radio
**Anti-pattern avoided:** No stainless steel, no smart speakers, no LED lighting, no granite counters

**Filled prompt:**
```
A 1970s American suburban home kitchen, photographed as if a homeowner casually took a phone photo, slightly off-angle. Avocado green refrigerator and electric coil stove dominate one wall. Wood paneling on the other walls, linoleum floor with a worn pattern. On the counter: a yellow ceramic canister set, a fruit bowl with bananas and apples, a handwritten recipe card slightly askew, a small AM radio. A rotary dial wall phone in beige hangs on one wall. Decorative roosters on a small shelf. Magnetic letters on the fridge door. A dish towel folded over the oven handle. Lit by an overhead pendant light with warm incandescent bulb, plus afternoon sunlight from a small window. Composition: rule of thirds, slightly off-center, capturing the kitchen feel. Aesthetic: real 1970s American suburban kitchen, NOT magazine-staged, NOT modern with retro decor. Photorealistic, 16:9, slightly warm color grade matching the era, no filter.
```

### Example 3 — Cabin / war veteran's den

**Goal:** Background for war veteran storytelling channel

**Matrix lookup:** Den / study (older male / war veteran)
**Lighting:** Warm lamp, slight fireplace glow
**Dominant element:** Wood desk OR leather chair
**Period markers:** Era-specific photos visible (Vietnam-era if Vietnam vet)
**Imperfection cues:** Worn leather, books with bookmarks, slight wood patina, cap on desk (not on head)
**Props:** Single shadow box (subtle), folded flag in case, era photos, books, fireplace
**Anti-pattern avoided:** Wall of medals, dramatic flag backdrop, military memorabilia overload

**Filled prompt:**
```
A modest American den or home study with wood paneling and a brick fireplace, photographed as if a real older homeowner. Worn leather armchair near the fireplace, wood desk against the opposite wall. Visible details: a single small shadow box on the wall with subtly visible medals (not prominent), a folded American flag in a triangular case on a shelf, a few framed photos from the late 1960s and early 1970s in faded color, books on military history and general literature on a wood shelf. A unit cap rests on the desk (not worn). The fireplace has slight ash, the leather chair shows years of use. Warm tungsten light from a single floor lamp on the right, slight glow from the fireplace, hint of late afternoon sunlight from a window on the left. Composition: rule of thirds, capturing the warm lived-in feel. Aesthetic: real older veteran's home den, NOT memorial set, NOT military-themed restaurant decor. Photorealistic, 16:9, warm natural color, no filter.
```

## 12. Sample-then-confirm gate

For any setting that becomes a recurring brand element (host backdrop, episode opener, signature B-roll location):

1. Generate 4 variants with seed variation
2. Surface to user
3. User picks one, locks to channel memory
4. Reference image becomes canonical for that setting

For one-off B-roll settings (per-episode cutaways): generate 2 variants, surface, user picks. Lower stakes than recurring backdrops.

## 13. Runtime checklist

Before any setting generation:
- [ ] Setting matrix row identified
- [ ] Period accuracy verified if non-contemporary
- [ ] Cultural/regional accuracy verified
- [ ] Lighting matched to setting type
- [ ] Prop density calibrated (default Modest)
- [ ] Imperfection cues included (3-5 specific items)
- [ ] Anti-patterns checked (no showroom, no stock-photo composition)
- [ ] Sample variants generated
- [ ] User approval on key recurring backdrops

## Update log

Current as of April 2026. Update when:
- New setting types needed (add row to matrix)
- Period accuracy databases need refreshing (e.g., new decade emerges)
- Cultural variants expand
- Lighting standards shift across niches
