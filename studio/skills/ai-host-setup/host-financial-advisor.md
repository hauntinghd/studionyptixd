# Tactical Playbook — AI Host: Financial Advisor / Tax Professional

> **DEPRECATED (2026-04-29).** Superseded by `T1-ai-host-setup.md` (profession matrix). Load T1 instead. Preserved for reference only.

## The goal

Generate a reference image of a financial advisor or tax professional sitting in their home office, looking authentic — like a real EA / CPA / fiduciary advisor turned on their webcam to record a YouTube video for retirees. This is the visual archetype for a personal-finance authority channel (IRS / tax accountant register), an economic-explainer channel, and any senior-finance authority channel.

The output must read as: **"this is a real tax professional warning me about something the IRS is doing,"** not as **"this is a stock-photo financial advisor in a glass-tower office."** Senior finance audiences specifically reward "this person is at their actual desk, not in a corporate ad," because they associate gloss with the predatory advisors who burned them.

This playbook applies to:
- IRS/tax authority channels (personal-finance authority register)
- Retirement planning channels
- Bank-rule / financial-protection channels
- Senior finance authority channels in general

## The non-negotiable details

These MUST be present or the image fails:

1. **Visible professional credibility WITHOUT corporate gloss** — a single framed certification on the wall (CPA, EA, CFP) OR a tax-code reference book on the desk OR a Federal Reserve poster. ONE of these, not all three. Multiple credentials displayed reads "trying too hard."
2. **Home office setting, not corporate** — wood desk, books on a shelf, a window, a desk lamp. NOT skyscraper view. NOT glass conference room. NOT modern minimalist co-working space.
3. **Natural lighting from a window + a desk lamp** — looks like a real room with real lighting. NEVER three-point cinematic.
4. **Mid-shot framing, slight upward angle as if a webcam is on the desk** — head, shoulders, chest, top of desk visible. NOT studio portrait close-up.
5. **Subtle imperfections in the room** — a coffee mug, slightly askew picture frame, a few papers stacked on the desk. Real-person clutter signals authenticity.
6. **Near-direct eye contact with camera** — they're addressing the viewer, not posing.
7. **Wardrobe: collared button-down or polo, OR collared shirt with light sweater** — NOT a suit (too corporate, reads "salesman"). NOT a sport coat over t-shirt (too casual). The middle is the sweet spot.
8. **Age 35-55 default; older variants 55-70** — younger reads inexperienced for tax authority. Older works but must match voice DNA. A personal-finance authority channel's avatar typically reads ~30-35; per the avatar-age-consistency rule we never claim more years than the avatar shows.
9. **Photorealistic rendering** — never illustrated, anime, 3D-render, or stylized.
10. **Calm, slightly serious expression** — neutral or very slight concern. NOT smiling broadly (untrustworthy in finance), NOT scowling (intimidating), NOT staring intensely (uncanny).

## The do-not list

These ruin the output:

- ❌ Glass tower / skyscraper office view (reads "Wall Street salesman")
- ❌ Suit and tie (too corporate, registers as "trying to sell me something")
- ❌ Three-point cinematic lighting setup
- ❌ Multiple framed degrees / certifications on the wall (over-credentialed reads insecure)
- ❌ Stock-photo "thumbs up" or "pointing at camera" pose
- ❌ Aggressive crossed-arms confrontational pose
- ❌ Open laptop with stock charts visible (cliché)
- ❌ Money / dollar bills / gold visible in frame (greedy register, wrong audience)
- ❌ Crystal-clean shaved face + perfect skin smoothing (uncanny-valley AI tell)
- ❌ Holding a coffee mug "casually" (forced authenticity)
- ❌ Young, baby-faced rendering (reads "scammy crypto guy")
- ❌ White-walled empty room (looks like a studio, not a home office)

## The locked prompt template

```
A [AGE: 38-50] year old [GENDER: male/female] tax professional or financial advisor sitting at a wooden home office desk, photographed as if they had set up a basic webcam to record a YouTube video about retirement and tax strategy. They are wearing a [WARDROBE: collared button-down shirt / polo / button-down with light cardigan] in [COLOR: navy blue / charcoal / muted earth tone], top button undone. On the desk in front of them: a few papers, a coffee mug, a lamp. Behind them: a wooden bookshelf with tax reference books and a couple of personal photos, ONE single framed certification (CPA, EA, or CFP) on the wall, a small plant or framed family photo. The room is warmly lit by natural daylight from a window to the left of frame, with a desk lamp providing warm fill light. The subject is framed mid-shot from chest up, slightly off-center to the left, looking just to the right of the camera with a calm, slightly serious expression — attentive and professional, NOT smiling broadly. The camera angle is slight upward as if a webcam is propped on a desk monitor, eye level or just below. Photorealistic, captured as if with a basic webcam — realistic skin texture with subtle imperfections, NO smoothing, NO cinematic grade. Aesthetic: real tax professional at their home desk, NOT studio shoot, NOT corporate magazine, NOT Wall Street office. 16:9, photorealistic, natural color, no filter.
```

## Model selection

| Model | Performance | Notes |
|---|---|---|
| **Nano Banana (Gemini 3 Pro Image)** | ⭐⭐⭐⭐⭐ Best | Reliably renders home-office authenticity over corporate gloss when prompted clearly |
| Flux 1.1 Pro | ⭐⭐⭐⭐ Good | Photoreal but tends to add cinematic key light unless explicitly constrained |
| Midjourney v7 | ⭐⭐⭐ OK | Defaults to overly polished "professional headshot" style; needs aggressive de-stylizing prompts |
| DALL-E 3 | ⭐⭐ Weak | Stock-photo aesthetic by default |

**Default: Nano Banana.** Falls back to Flux Pro with explicit "no cinematic, no studio" emphasis if Nano Banana outputs read too clean.

## Common variations

### Older advisor (60-70, "retired CPA who teaches")
Modify: increase age range. Add: "salt-and-pepper or fully gray hair, reading glasses on chain or worn on bridge of nose, slightly more weathered hands visible." Useful for channels positioned as "decades of experience" — but verify voice DNA matches.

### Female advisor variant
Wardrobe: blouse, cardigan, or simple shell. Hair: shoulder-length professional. Personal items: a coffee mug, small plant, framed photo of family. Same realism rules. Female advisor variants particularly outperform on senior-female-targeted finance content.

### Specific specialty signaling
- **IRS / tax (personal-finance authority register):** add IRS code book OR Federal Tax Code reference visible on shelf. Single CPA or EA certification framed on wall.
- **Retirement planning (Roth conversions, Social Security):** add Social Security Administration reference book + small calculator on desk.
- **Estate planning:** add legal-pad with handwritten notes visible, fountain pen on desk.
- **Bank protection / scam exposure:** add a few federal agency seals (FinCEN, FDIC) framed small on the wall.

### Personal-finance authority channel specifically
The locked personal-finance authority visual register: roughly 35-year-old male with slightly young appearance (HeyGen avatar baseline), neutral-warm slight-concern expression, single CPA/EA framed certification, navy or charcoal collared shirt, wood desk, books visible, warm window light. ALWAYS pair with V2 Federal Credentialed Expert voice. NEVER claim years of experience in script (per locked feedback rule).

## When to use

This playbook is called by:
- **Skill 02 — Thumbnail Design** when generating thumbnails for senior finance / IRS / retirement channels
- **Skill 06 — Storyboard / Scene Breakdown** when establishing a host reference for talking-head episodes
- **Skill 09 — Image Generation Prompting** as a sub-routine when generating a financial advisor character
- **Skill 05 — Reference Channel Ingestion** when matching a competitor channel like an economic-explainer channel

## Integration with Skill 03 (Voice DNA)

This visual playbook pairs with **V2 Federal Credentialed Expert** voice archetype from Skill 03. The face you generate must match the voice you'll synthesize — visual age and voice age must align within ~10 years. The agent always picks visual age + voice age in the same step.

## Sample-then-confirm gate

Before locking this character for a channel:
1. Generate 4 variants (different seeds, slight wardrobe + age variation)
2. Surface all 4 to the user
3. User picks one OR asks for adjustments
4. Once locked, the character reference goes into Channel Memory as the avatar reference for HeyGen, thumbnail face source, and any "host shot" generation downstream

## Anti-patterns specific to financial advisor hosts

- **The "stock photo handshake" failure** — never render the host shaking hands, holding a clipboard while pointing, or staring at a stock chart. These are stock-photo cliches.
- **The "money behind them" failure** — visible money, gold, dollar bills, or stock tickers in the background reads as predatory. Senior finance audience is suspicious of wealth-display.
- **The "trust me" stare** — overly intense direct-camera eye contact reads uncanny. Slight off-camera angle is more authentic.
- **The "expensive watch" detail** — visible luxury watch on the wrist reads "scammy financial guru." Either no watch or a basic functional one.
- **Over-credentialed walls** — three or four diplomas in a row reads insecure. One.
- **The "open laptop with chart" cliché** — every fake financial advisor stock photo has this. Avoid.
- **The "thumbs up to camera" pose** — instant trust-killer.

## Why these rules

The senior finance audience has been burned for decades by predatory financial salesmen, scammy crypto bros, and overly-polished "wealth advisors" whose primary skill is closing the sale. The single trust signal that beats every credential is **looking like a real tax person at their actual desk** rather than a corporate actor.

Channels that use studio-polished hosts in this niche underperform reliably. Channels that use authentic home-office hosts (an economic-explainer channel's host is the gold-standard reference — a real-looking person at a real desk, with real personal items visible) consistently win. A personal-finance authority channel's avatar should be deliberately designed in the "real EA at home" register, not the "TV financial advisor" register, because the audience pre-screens out the latter.

The single most important detail: **lighting must look like real home lighting**, not studio. The eye registers studio key/fill/rim light within 0.3 seconds and the trust signal flips to "this is an ad."

## Update log

This playbook is current as of April 2026. Update when:
- Image gen models change face rendering quality
- The senior finance niche aesthetic shifts (currently locked on "authentic at-home professional")
- New financial credential variants emerge (digital-age certifications, etc.)
- The locked personal-finance authority avatar is materially updated
