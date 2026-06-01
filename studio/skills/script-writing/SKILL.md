---
name: script-writing
description: >-
  Writes the 25,000-char script with voice + 12-beat structure. Load when writing or revising a script. Companion: beat-anatomy.md (production-quality worked examples for all 12 beats + voice re-skinning guide).
---

# Skill 03 — Script Writing

This is the operational knowledge an AI YouTube agent needs to write a perfect long-form script for any video in any niche, with a deep system for establishing a character voice first and shaping every paragraph through that voice. Every paragraph below is a concrete rule the agent applies.

This is the longest skill because it carries three layers stacked on top of each other:
1. **Voice** — who is speaking, how they sound, what they would never say
2. **Structure** — the retention-engineered beat sequence that holds attention
3. **Format** — the technical rules that make the script work for the chosen voice generator

Most YouTube script advice gets two of these and skips voice entirely. The voice layer is the load-bearing differentiator.

---

## 1. The job of a script

A script does five things in approximately 25,000 characters:

1. **Pays off the title-thumbnail click** within the first 30 seconds — failure here collapses retention immediately
2. **Holds the viewer through the valley-of-death moments** at 30%, 60%, and 80% of the video
3. **Delivers the satisfaction the title promised** — the contract has to close or the channel takes a satisfaction hit
4. **Speaks in a distinct, recognizable voice** — the channel's audience returns for the voice as much as the topic
5. **Embeds the conversion machinery** — lead magnet CTA, affiliate, subscribe ask, share prompt — without breaking voice

The agent never writes a "generic" script. Every script is voice-first: figure out who is talking, then write what they would actually say in that situation.

Quote from Ira Glass (This American Life): *"The story is only as compelling as the voice telling it. You can have the best premise in the world, and if the voice is wrong, nobody listens."*

Quote from a top viral-challenge creator (2024): *"Pacing is everything. If you watch our scripts, every 8 seconds something has to change — a new beat, a new visual, a new tension. The audience can't be allowed to drift."*

These two quotes capture the dual demand: the script must be a distinct voice AND a relentless retention engine. The agent solves both simultaneously by establishing voice before structure, then enforcing retention rules within that voice.

## 2. The optimization target

The agent optimizes scripts against **predicted satisfaction-weighted watch time**, identical to title and thumbnail strategy. The metric trio is CTR × AVD × Satisfaction, and the script controls AVD and Satisfaction directly.

Specifically, the script is judged by:
- **30-second retention** — what % of viewers are still there at 0:30. Below 70% is critical failure.
- **Mid-video retention floors** — the dips at the 30%, 60%, 80% timestamps. These are where weak scripts shed 15-30% of remaining viewers.
- **Average view duration** — total minutes watched per view. The headline AVD metric.
- **Satisfaction signals** — likes per view, comments per view, share rate, return rate.
- **Voice recognition** — repeat-viewer retention. Audiences come back for the voice. This compounds.

A script that plays well in the first 30 seconds and fails at the 60% valley loses to a script that plays modestly in the first 30 but holds through valleys. The agent always writes for AVD over CTR-bait openings.

## 3. The voice establishment system

This is the new heart of the skill. Most YouTube script generators skip voice. The agent does not.

### The voice DNA template

Before writing a single sentence of script, the agent fills out the channel's voice DNA. This template is loaded into channel memory and referenced at every generation step.

```
Voice DNA — [channel name]

ARCHETYPE: [primary archetype + secondary if hybrid]
WHO IS SPEAKING: [character + credentials + personal stake]
WHO ARE THEY SPEAKING TO: [primary audience + emotional state]
EMOTIONAL REGISTER: [authority / warmth / outrage / curiosity / threat / etc.]
PACE: [slow-deliberate / mid / fast-urgent]
SENTENCE RHYTHM: [long unfolding / short staccato / mixed varied]
VOCABULARY LEVEL: [grade 8 / grade 12 / specialist]
SIGNATURE TRANSITIONS: [3-5 phrases the voice always uses]
SIGNATURE OPENERS: [the way this voice starts paragraphs]
TABOO WORDS: [what this voice would NEVER say]
RECURRING REFERENCES: [domain references the voice naturally pulls from]
RELATIONSHIP TO AUDIENCE: [authority / peer / confidant / teacher / preacher]
HUMOR REGISTER: [none / dry / warm / chaotic / dark]
```

The agent fills this in three ways, in order of preference:

1. **From a reference channel** the user provides ("write it like a long-form mystery-documentary channel", "make it sound like an economic-explainer channel"). The agent runs the competitor voice-mining protocol (§10) and extracts the DNA from 5-10 transcripts.
2. **From the user's word-vomit + niche conventions.** If user describes a "deadpan history shorts channel," the agent matches to a deadpan-history archetype and adapts.
3. **From the user themselves**, when the user is the host. The agent asks 2-3 voice-revealing questions ("describe how you'd explain this to a friend at dinner") and extracts patterns.

Once filled, the DNA gets locked into channel memory and shapes every generation thereafter. When the user says "make it more deadpan" or "less formal," the agent edits the DNA fields and regenerates.

### The 15 voice archetypes

Each archetype has a deep spec the agent loads when matched. The agent never invents a new archetype on the fly — it picks the best fit from the library, then tunes it via memory rules.

#### V1 — Documentary Authority

**Examples:** a science-explainer channel, a long-form mystery-documentary channel, a tech-industrial-history channel, Adam Curtis-style documentary, How to Cook That (Ann Reardon)
**Pace:** Slow-deliberate. Each sentence completes its thought.
**Sentence rhythm:** Mostly long unfolding sentences with embedded clauses. Occasional short punctuating sentence.
**Vocabulary:** Specialist-adjacent — uses technical terms but defines them in passing.
**Emotional register:** Calm curiosity. Never excited. Never outraged. Wonder is the closest emotion.
**Audience relationship:** Equal-intelligent peer. Never talks down. Never explains the obvious.
**Signature transitions:** "But here's what's interesting..." / "And this is where it gets strange..." / "What follows from this is..." / "Consider..."
**Signature openers:** Often opens with a specific date, person, or seemingly mundane detail that becomes the through-line.
**Taboo words:** "guys," "epic," "insane," "you won't believe," "buckle up," any hype-stack vocabulary.
**Recurring references:** Cites studies, named experts, primary sources.
**Humor register:** Dry, occasional. Never the goal.
**Sample first 30 seconds:**
> "On the morning of October 23rd, 1972, a small experiment in a basement laboratory in Berkeley produced a result the researcher did not expect. He published the result quietly. Three years later, the same result was reproduced by a completely independent team in Geneva. Forty years later, that single observation became the foundation for an entire branch of physics. This is the story of that observation, and why it took thirty seven years for the rest of the world to understand what it meant."

#### V2 — Federal Credentialed Expert

**Examples:** a personal-finance authority channel (IRS Accountant), a medical-authority channel (doctor health authority), Mike Vestil-style finance authority
**Pace:** Mid, with deliberate pauses on critical words.
**Sentence rhythm:** Mixed. Long explanatory sentences alternate with short hammer sentences. The hammers carry the weight.
**Vocabulary:** Professional but plain English. Translates jargon. Uses specific form numbers, code sections, dollar amounts.
**Emotional register:** Authoritative + protectively outraged. Speaking on behalf of the audience against an opaque system.
**Audience relationship:** Older mentor talking to peer. Treats audience as adult capable of handling specifics.
**Signature transitions:** "Here is what almost no [audience] is told..." / "Let me show you exactly..." / "Here is the part that..." / "And the consequence is..."
**Signature openers:** Often opens with a hypothetical specific scenario the audience will recognize ("If you walked into your bank tomorrow morning to withdraw $20,000...").
**Taboo words:** "guys," any vague qualifier ("a lot," "kind of"), "let's dive in," "in this video we'll explore," any AI-cliche transition.
**Recurring references:** Federal statutes, code sections, agency names, specific dollar figures, real victim composites.
**Humor register:** None. Serious throughout.
**Sample first 30 seconds:**
> "If you receive a Social Security retirement check, a Supplemental Security Income payment, or any other monthly benefit administered by the Social Security Administration, the next four days are the most consequential four days of your year, and almost no retiree in America has been told what is about to happen."

#### V3 — War Veteran Storyteller

**Examples:** Combat docs, military YouTube, Joe Galloway-style war reportage, Ken Burns narrators
**Pace:** Slow. Lived-in. Pauses are part of the voice.
**Sentence rhythm:** Short to medium. Often repetition for emphasis. Fragments are allowed and used deliberately.
**Vocabulary:** Plain. Visceral. Concrete sensory detail. No abstractions.
**Emotional register:** Heavy. Earned. Never performed.
**Audience relationship:** Witness to listener. The narrator was there; the audience wasn't. Implicit obligation to honor.
**Signature transitions:** "What I remember is..." / "And then..." / "Years later..." / "I will tell you what nobody told us."
**Signature openers:** Opens with a specific sensory detail — what the narrator saw, smelled, heard.
**Taboo words:** Any modern internet slang. Any hype vocabulary. Any abstraction.
**Recurring references:** Specific places, dates, names, weapons, vehicles, the texture of moments.
**Humor register:** Dark, rare, lived-in.
**Sample first 30 seconds:**
> "It was raining the morning we crossed the river. November of 1968. I was nineteen years old and I had never been further from home than the next county over. The lieutenant told us the bridge was secured. The lieutenant was wrong. What I want to tell you, fifty seven years later, is what it actually felt like to learn that he was wrong."

#### V4 — Skeptical Investigator

**Examples:** an investigative-journalism channel, a geopolitics documentary channel, a news-explainer outlet's investigations, Dan Olson (Folding Ideas)
**Pace:** Fast-mid. Builds momentum as the case unfolds.
**Sentence rhythm:** Mixed. Question sentences alternate with answer sentences. The voice is asking and answering.
**Vocabulary:** Plain to specialist depending on terrain. Glossary moments are explicit.
**Emotional register:** Pursuit. The voice is on the trail of something. Skepticism is the default; when the voice expresses outrage, it's earned by the evidence.
**Audience relationship:** Detective with assistant. The voice is doing the investigating; the audience is along for the ride.
**Signature transitions:** "But something didn't add up." / "So I started digging." / "And then I found this." / "Here's what nobody is telling you."
**Signature openers:** Opens with a specific anomaly, claim, or unanswered question that becomes the through-line.
**Taboo words:** Hype words. Generic hooks. Anything that sounds like a guru.
**Recurring references:** Documents, screenshots, transcripts, named individuals, specific dollar amounts.
**Humor register:** Dry, occasional, when the absurdity of what's been found warrants it.
**Sample first 30 seconds:**
> "Last March, a YouTube channel with 2.3 million subscribers posted a video claiming they had cured Type 2 diabetes with a $7 supplement. The video has 14 million views. The supplement is owned by the channel's host. The studies they cite do not exist. I spent six weeks trying to find one — exactly one — peer-reviewed paper that supported their claim. Here is what I found instead."

#### V5 — Mentor Coach

**Examples:** a productivity creator, a personal-finance education creator, a business-author thought leader, a deep-work productivity author (audiobook narrator)
**Pace:** Mid-warm. Conversational but structured.
**Sentence rhythm:** Mixed. Often uses three-part structures ("First... Second... Third..."). Rule-of-three is the dominant rhythm.
**Vocabulary:** Educated but plain. Frameworks named. Concepts labeled.
**Emotional register:** Warm authority. The voice has figured something out and wants to share it.
**Audience relationship:** Older friend who has been where you are.
**Signature transitions:** "Here's the framework I use..." / "Three things changed for me..." / "The principle that matters is..."
**Signature openers:** Often opens with a personal anecdote that surfaces the lesson.
**Taboo words:** "Guys" (sometimes — depends on the specific mentor), any hype vocabulary.
**Recurring references:** Books, frameworks, named thinkers, personal experiments.
**Humor register:** Warm, self-deprecating.
**Sample first 30 seconds:**
> "Three years ago I was making seventy thousand dollars a year and I was miserable. I had a stable job, a routine, all the things people said I should want, and I felt like I was wasting my life. So I built a framework to figure out what to change. It's a three-step framework. I've now taught it to over forty thousand people. And in the next twelve minutes, I'm going to walk you through it."

#### V6 — Hype Showman

**Examples:** a top viral-challenge creator, Jake Paul (early), gaming hype, sports recap channels
**Pace:** Fast-urgent. Almost no pauses. Energy is the product.
**Sentence rhythm:** Short, punchy. Exclamations allowed. Repetition for emphasis. Sentence length almost never exceeds 15 words.
**Vocabulary:** Plain, conversational, sometimes profanity-adjacent (depending on channel). Slang accepted.
**Emotional register:** High energy throughout. Never settles.
**Audience relationship:** Friend who's just done something wild.
**Signature transitions:** "And then..." / "You won't believe..." / "Wait until you see this..." / "But here's the crazy part..."
**Signature openers:** Opens with the most extreme moment of the video, then rewinds.
**Taboo words:** Any complex vocabulary. Any subjunctive mood. Anything that slows pace.
**Recurring references:** The previous video, the channel's escalating stakes.
**Humor register:** Chaotic, broad.
**Sample first 30 seconds:**
> "I just spent five hundred thousand dollars to lock fifty people in a room for thirty days, and the last person to leave wins a million dollars. Watch what happens. This is going to be the craziest thing you've ever seen. We've got a former NFL player, a chess grandmaster, and a guy who eats nothing but cereal. And that's just three of them. The rest you're about to meet."

#### V7 — Deadpan Cynic

**Examples:** a deadpan-history channel, a how-to-adult lifestyle channel, an internet-culture history creator (some), a sardonic-history creator
**Pace:** Mid. Pauses on punchlines.
**Sentence rhythm:** Mixed. Often sets up a normal sentence and then undercuts it with a one-liner.
**Vocabulary:** Plain to slightly elevated. Vocabulary itself can be the joke.
**Emotional register:** Knowing, dry, never sincere about anything for too long.
**Audience relationship:** Equal who has seen everything you've seen and finds it all slightly absurd.
**Signature transitions:** "Now obviously..." / "As you'd expect..." / "Predictably..." / "Spoiler alert..."
**Signature openers:** Often opens with a deceptively simple statement that sets up the absurdity to come.
**Taboo words:** Sincerity. Earnestness. Anything that takes itself too seriously.
**Recurring references:** Pop culture, internet culture, the absurdity of history.
**Humor register:** Dry, observational, frequently self-deprecating about the format itself.
**Sample first 30 seconds:**
> "In the year 1518, in the city of Strasbourg, a woman named Frau Troffea started dancing. She did not stop. Within a week, thirty-four other people had joined her, also dancing, also unable to stop. Within a month, four hundred people were dancing in the streets, and at least fifteen of them had dropped dead from exhaustion. This is, predictably, a true story."

#### V8 — Erudite Professor

**Examples:** an economic-explainer channel, a logistics explainer channel, a geo-economic explainer, an engineering explainer channel
**Pace:** Slightly fast. Information density is the product.
**Sentence rhythm:** Long, clause-heavy. Often three or four clauses joined with commas.
**Vocabulary:** Specialist. Defines terms but doesn't apologize for them.
**Emotional register:** Confident, slightly British in feel even when not (a "lecturing" register).
**Audience relationship:** Senior professor to interested student.
**Signature transitions:** "What is interesting is..." / "Curiously..." / "This is to say..." / "It follows that..."
**Signature openers:** Often opens with a counterintuitive premise that the rest of the script supports.
**Taboo words:** Slang. Any vocabulary that breaks the lecture register.
**Recurring references:** Studies, papers, named economists/scientists/engineers, historical context.
**Humor register:** Wry, infrequent, almost always at the expense of an institution.
**Sample first 30 seconds:**
> "In 1973, an obscure economist named Arthur Laffer drew a curve on a napkin during a dinner with two White House staffers, neither of whom would later remember the conversation correctly. The curve, despite being demonstrably wrong in its strongest form, would go on to shape American tax policy for the next five decades, cost the federal government somewhere between four and twelve trillion dollars, and become one of the most cited and least understood economic ideas in modern history."

#### V9 — Best Friend Confidant

**Examples:** Beauty vloggers, lifestyle vloggers, a daily-vlog pioneer (sometimes), some real estate vloggers
**Pace:** Conversational. Tangents allowed.
**Sentence rhythm:** Loose. Sentences can run on. Parenthetical asides are part of the voice.
**Vocabulary:** Casual. Slang. First-person heavy.
**Emotional register:** Intimate, slightly performative. The voice is sharing something between friends.
**Audience relationship:** Best friend gossiping.
**Signature transitions:** "So anyway..." / "And then I was like..." / "Okay but..." / "Listen..."
**Signature openers:** Often opens mid-thought, as if continuing a conversation.
**Taboo words:** Any clinical or formal vocabulary. Any structural framing language.
**Recurring references:** Personal life, friends, brands the voice loves.
**Humor register:** Warm, self-deprecating, frequent.
**Sample first 30 seconds:**
> "Okay so I bought this lipstick last week and I have to talk about it because I genuinely don't know how this brand is allowed to charge what they're charging. Like, it's three hundred dollars. For a lipstick. And before you say it, yes, I returned it, but not before wearing it for a full day to see if maybe the price was justified. Spoiler: it was not."

#### V10 — News Anchor

**Examples:** a news-explainer outlet's explainer voice, Reuters style, Nick News, AP video voice
**Pace:** Steady, professional.
**Sentence rhythm:** Clean. Subject-verb-object. Active voice. Inverted pyramid (most important first).
**Vocabulary:** AP-style plain English. Avoids jargon, defines when needed.
**Emotional register:** Neutral. Concern allowed. Outrage forbidden.
**Audience relationship:** Trusted reporter to viewer.
**Signature transitions:** "What's at stake here is..." / "The questions remaining are..." / "What we know now is..."
**Signature openers:** Lead with the news. Date, place, action, consequence.
**Taboo words:** Editorializing. First-person opinion. Slang.
**Recurring references:** Sources cited as "officials say," "documents show," etc.
**Humor register:** None.
**Sample first 30 seconds:**
> "On April 15th, 2026, the Internal Revenue Service issued a final regulation under section 1099 of the federal tax code, lowering the reporting threshold for third-party payment networks to six hundred dollars. The change affects an estimated forty-seven million American taxpayers. Critics say it will create a paperwork burden for small sellers. Supporters say it closes a loophole. What we know about who actually wins and loses is the subject of this report."

#### V11 — Outraged Activist

**Examples:** Some news-explainer commentary, political YouTubers, social-justice creators, the angrier finance YouTubers
**Pace:** Building momentum. Starts mid, accelerates as outrage builds.
**Sentence rhythm:** Builds. Often uses anaphora (repeated sentence openings) for emphasis.
**Vocabulary:** Plain but impassioned. Strong action verbs.
**Emotional register:** Righteous outrage. The voice is angry and the audience is invited to share.
**Audience relationship:** Co-conspirator. "We" is the dominant pronoun.
**Signature transitions:** "And let me tell you why this matters." / "Here's what they don't want you to know." / "The truth is..."
**Signature openers:** Often opens with the outrage moment itself.
**Taboo words:** Calm hedging. "Both sides" framing. Any neutralizing vocabulary.
**Recurring references:** Named villains (corporations, politicians), specific dollar figures, victim stories.
**Humor register:** Bitter, occasional.
**Sample first 30 seconds:**
> "Three weeks ago, the largest insurance company in America denied chemotherapy coverage to a thirty-eight-year-old mother of two named Sarah, citing a preexisting condition clause that was supposed to have been illegal since 2010. They denied her in writing. They cc'd her doctor. And when her doctor called to fight it, they put him on hold for two hours. This is not a story about a bureaucratic mistake. This is the story of a system working exactly the way it was designed to work."

#### V12 — Curious Kid Adult

**Examples:** a science-engineering personality, a science-explainer channel (occasional), a curiosity-science creator, an experimental-science TV show
**Pace:** Mid-fast. Wonder pacing.
**Sentence rhythm:** Mixed, often question-led.
**Vocabulary:** Plain, occasionally elevated when explaining something specific.
**Emotional register:** Wonder. Genuine curiosity that hasn't been worn down.
**Audience relationship:** Smart curious peer ready to explore.
**Signature transitions:** "So I started wondering..." / "Which led me to ask..." / "And the answer turns out to be..."
**Signature openers:** Often opens with a question or a "what if" the voice has been mulling.
**Taboo words:** Anything cynical. Anything dismissive.
**Recurring references:** Personal experiments, hands-on tests, expert interviews.
**Humor register:** Warm, self-deprecating.
**Sample first 30 seconds:**
> "A few months ago I was at the beach with my kids and one of them asked me why the ocean is salty. And I realized, even though I'm an engineer who used to work at NASA, I didn't actually know. Like, I knew the rough answer — minerals from rivers — but I didn't know the actual mechanism. So I went home and spent a hundred and twenty hours figuring it out. And the real answer is way weirder than I expected."

#### V13 — Reluctant Witness

**Examples:** True crime narrators (Bailey Sarian, Eleanor Neale, Stephanie Soo), serious documentary
**Pace:** Slow, careful, weighted.
**Sentence rhythm:** Often short. Pause-heavy. The voice is bearing witness.
**Vocabulary:** Plain. Specific. Concrete.
**Emotional register:** Heavy, careful, mournful when warranted. Never sensational.
**Audience relationship:** Witness telling the story to someone who needs to know.
**Signature transitions:** "What happened next..." / "On the night of..." / "She would later tell investigators..."
**Signature openers:** Often opens with the date, the place, the victim's name in a quiet sentence.
**Taboo words:** Sensational vocabulary. Glorification of the perpetrator. Anything that reads tabloid.
**Recurring references:** Court records, police reports, named investigators, dates.
**Humor register:** None.
**Sample first 30 seconds:**
> "On the evening of August 12th, 1991, in Springfield, Missouri, three girls — Stacy McCall, age eighteen, Suzie Streeter, age nineteen, and Suzie's mother Sherrill Levitt, age forty-seven — disappeared from a small two-bedroom house on East Delmar Street. They left no note. They left their cars in the driveway. They left a half-finished cup of coffee on the kitchen counter. Thirty-five years later, no one knows what happened to them."

#### V14 — Drill Rapper Narrator

**Examples:** brick-narrative storytelling channel music videos, drill music narrators, propaganda content
**Pace:** Beat-locked. Tied to BPM (typically 130-145).
**Sentence rhythm:** Lyrical. Bar-locked. Often four-line stanzas.
**Vocabulary:** Slang, regional, threat vocabulary, escalating boasts.
**Emotional register:** Aggressive, defiant, performatively threatening.
**Audience relationship:** Performer to audience that knows the genre.
**Signature transitions:** Hook returns, ad-libs, drops.
**Signature openers:** Often opens with the hook line that will repeat.
**Taboo words:** Anything that breaks the genre register.
**Recurring references:** Genre-locked iconography, in-group references.
**Humor register:** Boasting register; humor is in the wordplay/escalation.
**Sample first stanza:**
> "You're not Jesus, you're not the savior, you ain't ever lived through what we lived through, you came to our country with your fancy talk, but you came to our country and we didn't ask you to..."

#### V15 — Wise Elder

**Examples:** Faith/Christian channels, some history channels, meditation/wellness
**Pace:** Slow, reverent.
**Sentence rhythm:** Long unfolding. Biblical/ancient cadence influence.
**Vocabulary:** Elevated but accessible. Old-soul register.
**Emotional register:** Reverent, contemplative.
**Audience relationship:** Elder to seeker.
**Signature transitions:** "Consider this..." / "What we have forgotten..." / "There is a story..."
**Signature openers:** Often opens with a parable or an ancient reference.
**Taboo words:** Slang. Hype. Modern cynicism.
**Recurring references:** Sacred texts, traditional wisdom, named saints/sages.
**Humor register:** Gentle, rare.
**Sample first 30 seconds:**
> "There is a story told by the early monks of the Egyptian desert about a young man who came to a wise teacher and asked how he might pray well. The teacher said nothing for a long time. Then he stood up, walked to the door of his cell, and said, 'Come back when you have learned to listen.' The young man left, returned a year later, and asked the same question again. This time, the teacher said, 'Sit. Be silent. We will begin.' This is, in effect, the entire teaching."

### How the agent picks voice

A decision tree the agent runs in approximately 5 seconds:

1. **Did the user name a reference channel?** If yes → run competitor voice mining (§10), match to closest archetype, customize.
2. **Did the user describe the desired tone in their word-vomit?** ("deadpan," "authoritative," "warm") → match descriptor to archetype.
3. **What is the niche?** → look up the niche's dominant archetype(s) (§7) and propose to user.
4. **What is the script's emotional register?** (rage / curiosity / threat / wonder / authority) → match to archetype.

The agent always proposes the voice to the user before writing, and confirms or adjusts based on feedback. After lock-in, the voice DNA goes into channel memory permanently.

## 4. The structural layer — the 12-beat retention model

After voice is locked, the agent applies a structural template adapted to the voice. The default 12-beat template (which we've validated across the IRS Accountant pipeline) works for long-form authority content. It's modified for other niches.

### The 12 beats (long-form authority default)

| Beat | Target chars | Function |
|---|---|---|
| 1. The Hook | 2,000 | First 30 seconds. Establish stakes, name the threat, promise the payoff. |
| 2. The 5-Promise Stack | 1,000 | List 5 specific things the video will deliver. Specificity sells. |
| 3. The Credential Intro | 300 | One paragraph. Voice's identity + credibility. No years claims. |
| 4. The Core Story / Concept | 3,500 | The patient/victim/case story OR the foundational concept. The longest single beat. |
| 5. The Mechanism Explained | 3,500 | Why the thing in beat 4 happens. Plain language explanation. |
| 6. Three Detailed Examples | 4,500 | Three composite cases applying the mechanism. Vary demographics. |
| 7. The Numbered List (5-7 items) | 5,000 | The actionable payoff. ~1,000 chars per item. Specific. |
| 8. The Bonus / Advanced Move | 1,500 | "Almost nobody is using this..." stickiness item. |
| 9. Lead Magnet CTA | 1,000 | The free guide / checklist. Specific URL. Specific value. |
| 10. Affiliate Pitch | 500 | One product, brief, disclosure-clean. |
| 11. The Close + Like/Subscribe | 1,500 | Restate stakes, call viewer to action. |
| 12. Share Prompt | 500 | "Send this to a friend who..." End with "I will see you in the next video." |

Total: ~25,000 chars. Voice generation time: ~25 minutes.

### Per-niche structural variants

The 12-beat default is for long-form authority. Other niches use modified structures:

**Vertical shorts (60-90s, Roblox-scenario channel style)**
- Hook (15-20s — "BRO! Imagine if..." opener)
- Premise expansion (15-20s)
- Cascading consequence (20-30s)
- Payoff + CTA (10-15s)
- Total ~3,000 chars, voice generation ~75-90s

**Music video / propaganda (brick-narrative storytelling channel style)**
- Hook line (repeated as chorus)
- Verse 1 (16 bars typically)
- Chorus
- Verse 2
- Chorus
- Bridge
- Chorus
- Outro
- Total ~600-900 words, BPM-locked

**News-hijack documentary (investigative-journalism / medical-authority style)**
- Hook with named villain (60-90s)
- Investigation setup (2 min)
- Evidence unfolds (8-12 min, multi-beat)
- Reveal + implication (2-3 min)
- CTA + close (1 min)

**Ambient / loop (ambient-soundscape channel style)**
- Brief 30-second narrative intro (or none)
- 60-180 minutes of looped content (no script needed)
- Brief outro (or none)

**Talking head educational (5-8 min)**
- Hook (45s)
- One core concept (2-3 min)
- Three quick examples (2 min)
- Action item (30s)
- Close (30s)

The agent picks the structural template based on niche, then writes within the locked voice.

## 5. The opening 30 seconds — the most important text the agent will ever write

The first 30 seconds of any script controls 60-80% of total retention outcome. If the opener fails, the rest of the script doesn't matter. The agent treats this section like a separate skill within the skill.

### The four hook architectures (validated across all niches)

These pair with the title's hook (Skill 01) for coherence:

**H1 — Forensic / Story-Led**
> "On the morning of [specific date], [specific person] [specific consequential action] — and [hint at what follows]."
Best for: documentary, news-hijack, true crime, war veteran, history.

**H2 — Credentialed Subversion**
> "Almost every [authority] will tell you [conventional wisdom]. They will tell you to [specific advice]. And for [audience], that advice is medically/financially/strategically wrong."
Best for: federal credentialed expert, mentor coach, skeptical investigator.

**H3 — Threat Alert / Time Anchor**
> "If you [audience-defining condition], the next [time period] are the most consequential [time period] of your year, and almost no one has been told what is about to happen."
Best for: federal credentialed expert, news anchor, outraged activist.

**H4 — 99% Framing / Curiosity Gap**
> "There is one specific [test / supplement / form / loophole / strategy] that ninety nine percent of [audience] do not know about, even though it has been [validated] for [years]."
Best for: mentor coach, mid-tier authority, health/finance.

### The pattern interrupt timestamps

Within the first 30 seconds, the agent embeds **pattern interrupts** at:

- **0:03** — first specific number, name, or place. Anchors attention.
- **0:07** — second specific anchor OR a tone shift (question, emphasis).
- **0:15** — the promise stack begins. The viewer learns what they're getting.
- **0:30** — the first beat ends and the second begins. The voice should signal a transition.

Failure to interrupt every 7-10 seconds in the opening = retention drop. The agent self-validates this.

### The promise stack (mandatory for long-form)

After the hook, the agent stacks a 5-item promise list. Each item is specific, not vague:

- ✗ Bad: "I'll show you 5 healthy habits."
- ✓ Good: "Promise number three is the exact thirty second test you can do at your kitchen table that catches early kidney decline two years before any blood test will."

The promise stack is the contract. Every promise must be paid off later in the script. The agent tracks which promises haven't been delivered and ensures coverage in the numbered list (Beat 7).

## 6. The retention curve and how scripts shape it

Three valleys of death where weak scripts shed viewers:

### The 30% valley (~7-8 min into a 25-min video)
Cause: viewers who clicked for the hook realize they're committed and re-evaluate.
Fix: a major reveal, story shift, or specific dollar/number anchor right before this point. Re-promise something.

### The 60% valley (~15 min)
Cause: cognitive fatigue. Viewers contemplating leaving.
Fix: introduce the third example or shift to the numbered list. Energy change.

### The 80% valley (~20 min, just before the CTA)
Cause: viewers sense the video is "winding down" and skip to the next.
Fix: the bonus advanced move (beat 8). Position it as "almost nobody knows this," which re-engages skim viewers.

The agent self-validates by asking: at the 30%, 60%, 80% timestamps in the script, is there a beat shift, a specific anchor, or an emotional register change? If not, regenerate that section.

### Open loops

Embedded throughout the script: "I'll explain why in a moment..." / "We'll come back to this..." / "There's a wrinkle here we need to revisit." Open loops force the brain to commit to staying through the resolution.

The agent embeds at minimum:
- One open loop at the end of beat 1 (resolved in beat 4)
- One at the end of beat 4 (resolved in beat 6)
- One at the end of beat 7 (resolved in beat 8)

## 7. Niche-specific script playbooks

Same 22 niches as the title and thumbnail skills. The agent loads the relevant playbook.

### Senior finance / IRS / retirement (personal-finance authority register)
- **Voice:** V2 Federal Credentialed Expert
- **Structure:** Full 12-beat default
- **Format:** Minimax (numbers as words, no symbols)
- **Length:** 25,000-27,000 chars
- **CTAs:** Lead magnet (Retiree Protection Checklist) + tax-software affiliate
- **Reference:** the personal-finance authority channel's recent episodes

### Tech / AI / dev tools
- **Voice:** V1 Documentary Authority OR V8 Erudite Professor
- **Structure:** Hook + 3 demonstration sections + verdict + CTA. ~10-15 min.
- **Format:** Standard (numbers as digits acceptable, code blocks ok)
- **Length:** 8,000-12,000 chars
- **CTAs:** Affiliate code, related video
- **Reference:** a fast-paced developer-news channel, a developer-commentary creator, a developer-tutorial creator

### Gaming / Roblox / Minecraft / Fortnite (long-form)
- **Voice:** V6 Hype Showman
- **Structure:** Hook + escalating challenge + climax + reveal + close
- **Format:** Conversational, exclamations allowed
- **Length:** 8,000-15,000 chars
- **Reference:** a top viral-challenge creator, gaming hype channels

### Vertical shorts (Roblox-scenario channel)
- **Voice:** V6 Hype Showman (vertical variant)
- **Structure:** 60-90s — hook, premise, escalation, payoff, CTA
- **Format:** Custom voice prompt (e.g., "chaotic Gen Z shitposter")
- **Length:** 1,500-3,000 chars
- **Reference:** Our Roblox-scenario channel pipeline

### Music videos / propaganda / drill (brick-narrative storytelling channel)
- **Voice:** V14 Drill Rapper Narrator
- **Structure:** Hook + verse-chorus-verse-chorus-bridge-chorus-outro
- **Format:** Lyrical, BPM-locked (typically 130-145)
- **Length:** 600-900 words for ~3.5-min track
- **Reference:** the brick-narrative storytelling channel's recent episodes

### News-hijack documentary (investigative-journalism / geopolitics documentary register)
- **Voice:** V4 Skeptical Investigator
- **Structure:** Hook with named villain + investigation + evidence + reveal + implication + CTA
- **Format:** Standard
- **Length:** 15,000-25,000 chars
- **Reference:** an investigative-journalism channel, a geopolitics documentary channel

### True crime
- **Voice:** V13 Reluctant Witness
- **Structure:** Date + place + victim + investigation + reveal + commemoration
- **Format:** Standard, careful with sensitive language
- **Length:** 10,000-20,000 chars

### Health / medical / supplements (medical-authority / doctor-personality health register)
- **Voice:** V2 Federal Credentialed Expert (medical variant)
- **Structure:** Full 12-beat with patient story focus
- **Format:** Minimax
- **Length:** 22,000-27,000 chars
- **CTAs:** Free guide + supplement affiliate
- **Reference:** Doctor advice prompt template at `senior-health-yt/prompts/`

### Real estate / home
- **Voice:** V5 Mentor Coach OR V8 Erudite Professor
- **Structure:** Hook + framework + examples + action items + CTA
- **Length:** 10,000-15,000 chars

### Beauty / fashion / makeup
- **Voice:** V9 Best Friend Confidant
- **Structure:** Open mid-thought + product reveal + reaction + verdict
- **Length:** 5,000-10,000 chars

### Cooking / food (Babish, Joshua Weissman)
- **Voice:** V12 Curious Kid Adult OR V8 Erudite Professor
- **Structure:** Hook + ingredients reveal + technique + tasting + verdict
- **Length:** 5,000-10,000 chars

### History / explainer (long-form mystery-documentary / deadpan-history / side-project history register)
- **Voice:** V1 Documentary Authority OR V7 Deadpan Cynic
- **Structure:** Cold open with date/specific detail + setup + complication + resolution + reflection
- **Length:** 10,000-25,000 chars

### Science (science-explainer / science-animation register)
- **Voice:** V1 Documentary Authority OR V12 Curious Kid Adult
- **Structure:** Question + intuition + experiment + counter-intuition + resolution
- **Length:** 8,000-15,000 chars

### Vlog / lifestyle
- **Voice:** V9 Best Friend Confidant
- **Structure:** Mid-thought open + day's events + reflection
- **Length:** 5,000-10,000 chars

### Crypto / finance trading
- **Voice:** V11 Outraged Activist OR V6 Hype Showman
- **Structure:** Threat alert + market context + analysis + action + CTA
- **Length:** 8,000-15,000 chars

### How-to / education
- **Voice:** V5 Mentor Coach
- **Structure:** Hook + framework + step-by-step + recap + CTA
- **Length:** 8,000-15,000 chars

### Fitness / bodybuilding
- **Voice:** V5 Mentor Coach OR V6 Hype Showman
- **Structure:** Wound-first hook + program reveal + execution + transformation expectation + CTA
- **Length:** 5,000-12,000 chars

### Faith / Christian / religion
- **Voice:** V15 Wise Elder
- **Structure:** Parable + lesson + scripture + application + closing prayer
- **Length:** 8,000-15,000 chars

### Politics / commentary
- **Voice:** V11 Outraged Activist OR V4 Skeptical Investigator
- **Structure:** News anchor + analysis + named villain + outrage + call to action
- **Length:** 8,000-15,000 chars

### Travel
- **Voice:** V12 Curious Kid Adult OR V9 Best Friend Confidant
- **Structure:** Arrival + first impression + exploration + payoff moment + recommendation
- **Length:** 5,000-12,000 chars

### Reaction
- **Voice:** V9 Best Friend Confidant OR V6 Hype Showman
- **Structure:** Reaction in real time, minimal scripting
- **Length:** Often unscripted; if scripted, 2,000-5,000 chars

### Documentary long-form (tech-industrial-history / logistics explainer register)
- **Voice:** V8 Erudite Professor
- **Structure:** Cold open + thesis + evidence chain + counter-arguments + synthesis
- **Length:** 15,000-30,000 chars

### Ambient / sleep / focus loop
- **Voice:** None typically (or V15 Wise Elder for rare narrated openings)
- **Structure:** Optional 30-second intro narration
- **Length:** Minimal

## 8. The format rules

The script's voice generator dictates format. Three main formats:

### Minimax format

For our IRS, medical-authority, doctor-advice channels using Minimax TTS:
- Numbers as words: "twenty seven percent" not "27%"
- No symbols: % → "percent," $ → "dollars," & → "and"
- No formatting markers (no bullets, no headers, no bold)
- Pure prose only

### ElevenLabs format

For our news-hijack, science, history channels using ElevenLabs:
- Numbers can be digits ("$3,400" or "three thousand four hundred dollars" — both work)
- Limited symbol use ($ ok, % ok in moderation)
- No formatting markers
- Pacing tags allowed in some configs (`<break time="0.5s">`)

### Custom voice format (e.g., Roblox Minimax custom)

For our Roblox-scenario channel using a custom Minimax voice:
- Voice prompt established once ("chaotic Gen Z shitposter")
- Script written in matching cadence
- Slang, exclamations, fragments allowed
- Format follows the voice's natural patterns

The agent picks format based on the channel's voice generator and the voice DNA, then enforces format rules at every paragraph.

## 9. The anti-AI cliche filter

Post-July-2025 YouTube inauthentic content policy specifically targets AI-generated tells. The agent runs every script through this filter and rewrites any matches.

### The hard ban list

| Phrase | Replacement |
|---|---|
| "In this video, we'll explore" | (delete; just start with the hook) |
| "Let's dive in" | (delete) |
| "Buckle up" | (delete) |
| "The world of [topic]" | (delete) |
| "Delve into" | replace with "go through" or "look at" |
| "It's important to note" | (delete; just state the note) |
| "It's worth mentioning" | (delete; just mention it) |
| "Without further ado" | (delete) |
| "Let's get started" | (delete) |
| "I hope you found this helpful" | replace with channel-specific close |
| "Have you ever wondered" | (delete; ask a sharper question) |
| "Welcome back to the channel" | (delete) |
| "Today we're talking about" | (delete; start with the hook) |
| "But wait, there's more" | (delete) |
| "The bottom line is" | replace with specific summary |
| "At the end of the day" | (delete) |
| "Stay tuned" | (delete) |
| "Without a doubt" | (delete; weakens claim) |
| "It goes without saying" | (delete) |
| "Needless to say" | (delete) |
| "The fact of the matter is" | (delete) |
| "Game changer" | (delete; specific outcome instead) |
| "Crystal clear" | (delete) |
| "Nestled" | (delete; bizarre AI tell) |
| "Tapestry of [things]" | (delete; AI tell) |
| "In the realm of" | replace with "in" |
| "Embark on" | replace with "start" |
| "Plethora of" | replace with "many" |
| "Treasure trove" | (delete) |
| "Cutting edge" | (delete) |

### The structural cliché filter

Beyond word-level: the agent never writes paragraphs that:
- Begin with "When it comes to..."
- End with "...and that's a wrap"
- Use "first and foremost," "last but not least"
- Sandwich a list between "Without further ado, let's begin" and "There you have it"

### The transition rewrite

After draft, the agent reads each paragraph's first sentence in sequence. If the rhythm sounds AI-generated (every paragraph starts with a transitional phrase), it rewrites. Real voices vary their paragraph openings — some start with "Then..." some with a fact, some mid-thought.

## 10. The competitor voice mining protocol

When the user references a channel ("write it like a long-form mystery-documentary channel"), the agent runs:

**Step 1 — Pull 5-10 transcripts** of recent videos from the reference channel via youtube-transcript API or similar. Aim for 30-60 minutes of total transcript.

**Step 2 — Extract sentence-rhythm samples.** Pull 20 random sentences across transcripts. Analyze:
- Average sentence length
- Average clauses per sentence
- Use of questions, fragments
- Punctuation patterns

**Step 3 — Extract vocabulary register.** Tag each sentence by vocabulary level (plain / educated / specialist). Look for the distribution.

**Step 4 — Extract signature transitions.** Find phrases that appear 3+ times across transcripts. These are the channel's signature transitions.

**Step 5 — Extract emotional register.** Pick sentences where the voice is at peak emotion (positive or negative). Tag the register (rage, wonder, authority, intimacy).

**Step 6 — Extract pacing markers.** Note where the voice slows down (long sentences, descriptive passages) vs speeds up (short hammer sentences, lists).

**Step 7 — Build the voice DNA template.** Fill in every field.

**Step 8 — Match to closest archetype.** Pick the V1-V15 archetype that's closest, then customize via the DNA fields.

**Step 9 — Apply with differentiation.** Write in the matched DNA but introduce two deliberate differentiators (specific vocabulary, signature transition variation) so the user's voice is the channel's, not a clone.

The agent never plagiarizes — phrases unique to the reference channel are excluded. The agent steals the rhythm, register, and structural pattern, not specific phrasings.

## 11. The generation workflow

When the user provides a topic:

**Step 1 — Voice check.** Is the channel voice DNA already locked? If yes, load. If no, run voice establishment (§3) and propose to user.

**Step 2 — Structural template selection.** Match the niche to the structural variant (§7). Default to 12-beat for long-form authority.

**Step 3 — Hook architecture selection.** Pick from H1-H4 based on niche + voice + topic. Title alignment is critical — the hook must pay off the title's promise.

**Step 4 — Write the opening 30 seconds in voice.** This is the most important text. Spend disproportionate care here. Validate pattern interrupts at 3, 7, 15, 30s.

**Step 5 — Write through 12 beats sequentially.** The voice DNA is loaded as system context for every beat. Inject memory rules. Apply niche conventions.

**Step 6 — Self-edit via anti-AI filter (§9).** Scan for banned phrases. Rewrite paragraphs that fail.

**Step 7 — Self-validate retention curve.** Check beat shifts at 30%, 60%, 80%. Verify open loops are resolved.

**Step 8 — Format-apply (§8).** Apply Minimax / ElevenLabs / custom format rules. Convert numbers, symbols, etc.

**Step 9 — Surface to user with diff capability.** Show the script. The user reads, gives surgical feedback.

**Step 10 — Iterate via feedback-to-rule extraction (§12).** Each feedback message is parsed for rule extraction. Rules go to memory.

## 12. The feedback-to-rule extraction system

When the user gives script feedback, the agent extracts both:

1. **The local edit** — apply to this specific draft.
2. **The permanent rule** — save to channel memory, applied to all future scripts.

### Examples of feedback parsing

**Feedback:** "Cut the years-of-experience line."
- Local edit: remove the line from this draft.
- Permanent rule: add to channel memory under VOICE: "no years-of-experience claims" (a personal-finance authority channel-specific rule).

**Feedback:** "Make it more deadpan."
- Local edit: revise tone of next 3 paragraphs.
- Permanent rule: update voice DNA — emotional register field shifts toward V7 Deadpan Cynic blend.

**Feedback:** "Patricia is 72, not 71."
- Local edit: change the age in this script.
- Permanent rule: only if "Patricia" is a recurring composite character. If yes, save to channel memory as composite character roster.

**Feedback:** "I want a 5-promise stack opener."
- Local edit: restructure the hook.
- Permanent rule: add to channel STRUCTURE memory: "always use 5-promise stack opener after hook."

### The classifier rule

The agent classifies feedback as **local-only** or **permanent** based on language cues:
- Permanent: "always," "never," "from now on," "for all my videos," "I want every script to..."
- Local: "this video," "for this one," "just here," (or no temporal qualifier at all)

When ambiguous, the agent asks: "Is this just for this script, or do you want me to apply it to every future video?"

## 13. Anti-patterns

Script patterns that fail across the board. The agent never produces these.

**A1 — Generic opener.** "Hey guys, in today's video we're going to talk about..."
**A2 — Subjunctive vibes.** "What if you could..." "Imagine if..." (Roblox vertical excepted)
**A3 — Self-referential framing.** "In my journey of researching this, I came across..."
**A4 — Padding sentences.** "It's important to understand that, before we get into the details..."
**A5 — AI-cliche transition stack.** Every paragraph starting with "Now," "Now," "Now,"
**A6 — Promise without specificity.** "I'll show you how to..." (with no measurable outcome)
**A7 — Vague stakes.** "This could change everything." (Specific consequence missing)
**A8 — Disclaimer dump.** Three paragraphs of "this is not financial advice" mid-script.
**A9 — Apologizing for video length.** "I know this is long but..."
**A10 — Breaking voice for housekeeping.** "Quick note before we continue..." (Breaks voice immersion)
**A11 — Reference-heavy without grounding.** Citing five studies in a row without connecting them to the audience's life.
**A12 — Perfect resolution.** Ending with a clean bow. Real voices leave threads.
**A13 — Over-promising in CTA.** "This guide will change your life." (Specific value claim instead.)
**A14 — Subjunctive close.** "I hope you'll join me." Replace with specific next-step.

## 14. Five worked examples

The agent applies the full skill across niches.

### Example 1 — Personal-finance authority channel, IRS senior finance (V2 Federal Credentialed Expert)

**Topic:** A new $20K bank rule change.
**Voice DNA loaded:** V2 — federal credentialed expert, no years claims, mid-pace, mixed sentence rhythm, signature transitions ("Here is what almost no retiree is told..."), Minimax format.
**Hook architecture:** H3 Threat Alert.
**Structural template:** 12-beat default, 25,500 chars.
**Opening 30s sample:**
> "If you walked into your bank tomorrow morning to withdraw twenty thousand dollars in cash for a roof repair, a medical procedure, a down payment on a car for your grandson, or just because you wanted the cash in a safe at home, here is what would happen, in this exact order, without your knowledge or consent..."
*This is an example episode opener. Voice = V2. Hook = H3.*

### Example 2 — Roblox-scenario channel short (V6 Hype Showman, vertical)

**Topic:** "What if a teacher OWNED Roblox?"
**Voice DNA loaded:** V6 vertical variant — chaotic Gen Z shitposter, fast pace, fragments, exclamations, custom Minimax voice.
**Hook architecture:** Custom — opens with "BRO! Imagine if..." (channel signature).
**Structural template:** Vertical short — 60-90s, ~3,000 chars.
**Opening 30s sample:**
> "BRO! Imagine if a TEACHER actually OWNED Roblox. Like — your fifth grade history teacher just buys the entire game. Right? Detentions become Robux fines. Homework is now a battle pass. The principal? Replaced by an admin. And the absolute worst part — no, listen — the WORST part is..."

### Example 3 — Brick-narrative storytelling channel music video (V14 Drill Rapper Narrator)

**Topic:** "You're Not Jesus" — satirical music video.
**Voice DNA loaded:** V14 — drill rapper, beat-locked at 136 BPM, lyrical bars.
**Hook architecture:** Hook line that becomes chorus.
**Structural template:** Music video — hook + verse-chorus-verse-chorus-bridge-chorus-outro, ~700 words.
**Opening sample:**
> "You're not Jesus, you're not the savior / You ain't ever lived through what we lived through / You came to our country with your fancy talk / But you came to our country and we didn't ask you to..."

### Example 4 — Doctor advice senior health (V2 Federal Credentialed Expert, medical variant)

**Topic:** Joint pain and supplement mistakes.
**Voice DNA loaded:** V2 medical variant — registered nurse / geriatric physician, warm authority, mid-pace, Minimax format.
**Hook architecture:** H4 99% Framing.
**Structural template:** 12-beat default, 25,500 chars.
**Opening 30s sample:**
> "If you take a glucosamine supplement for joint pain, the next sixty seconds may be the most important sixty seconds you spend on health information this year. Because the supplement that nine in ten retired Americans take for joint pain is, according to the largest meta-analysis ever conducted on the compound, statistically indistinguishable from placebo. And the reason your doctor has not told you this is..."

### Example 5 — Documentary long-form science explainer (V1 Documentary Authority)

**Topic:** Why Roman concrete still works.
**Voice DNA loaded:** V1 — documentary authority, calm curiosity, slow-deliberate pace, long unfolding sentences, ElevenLabs format.
**Hook architecture:** H1 Forensic.
**Structural template:** 15-min documentary, 12,000 chars.
**Opening 30s sample:**
> "On a slope above the Bay of Naples, exposed to two thousand years of salt water, sits a concrete sea wall that has not only failed to deteriorate — it has, by every metric available to modern materials science, gotten stronger with age. The Romans who built it in the year 79 AD did not understand chemistry. They had no concept of crystallography. They were not, in the modern sense of the word, scientists. And yet the substance they made has outlasted every concrete formula invented since."

## 15. The expert quote bank

**Ira Glass (This American Life):** "The story is only as compelling as the voice telling it. You can have the best premise in the world, and if the voice is wrong, nobody listens."

**A top viral-challenge creator (2024):** "Pacing is everything. If you watch our scripts, every 8 seconds something has to change — a new beat, a new visual, a new tension. The audience can't be allowed to drift."

**Aaron Sorkin (on dialogue):** "Intention and obstacle. If you don't have intention and obstacle, you don't have a scene. You just have words."

**William Zinsser (On Writing Well):** "Good prose is lean. The secret of good writing is to strip every sentence to its cleanest components. Every word that serves no function, every long word that could be a short word, every adverb that carries the same meaning that's already in the verb — these are the thousand and one adulterants that weaken the strength of a sentence."

**Roy Peter Clark (Writing Tools):** "The voice is the writer's fingerprint. It's not a single trait. It's the cumulative effect of word choice, sentence rhythm, what gets emphasized, what gets buried."

**Werner Herzog (on documentary narration):** "I am not a director who hides behind the material. The material requires me to enter it. The audience can hear the difference."

**Dan Harmon (story circle):** "A character goes into the unknown, gets what they wanted, but pays a price. That's every story. The script is just the dressing."

**Backlinko (data, 2024):** "Videos with first-30-second retention above 75% have a 4.7× higher chance of being recommended on Browse than videos below 60%."

**Tubular (data, 2025):** "Voice-driven authority channels (single-host, locked register) have 38% higher repeat-viewer rate than format-driven channels in matched niches."

**A science-explainer channel host:** "There's a difference between a script that feels written and a script that feels spoken. The audience can hear the difference within ten seconds."

## 16. Runtime checklist

Before any script surfaces to the user:

- [ ] Voice DNA loaded for this channel
- [ ] Structural template selected for niche
- [ ] Hook architecture matched to voice + title
- [ ] Opening 30s contains pattern interrupts at 3, 7, 15, 30s
- [ ] 5-promise stack present (long-form) and each promise will be paid off
- [ ] All 12 beats present and within character count targets
- [ ] Memory rules injected into all generation contexts
- [ ] Anti-AI cliche filter run; no banned phrases
- [ ] Retention valleys (30%, 60%, 80%) have beat shifts or specific anchors
- [ ] Open loops embedded and resolved
- [ ] Format applied (Minimax / ElevenLabs / custom)
- [ ] CTAs in place (lead magnet + affiliate per channel rules)
- [ ] Total character count within 25,000-27,000 (long-form) or per-niche target
- [ ] Voice consistency check — read top of beat 1, mid of beat 7, top of beat 11. Does the voice sound like the same person?

If any check fails, regenerate that section. Never surface a failing script.

---

## Update log

This skill is current as of April 2026. Update when:
- A new dominant voice archetype emerges (last addition was V14 Drill Rapper Narrator in 2024 with the rise of music-video propaganda channels)
- YouTube changes how it weights AVD vs Satisfaction signals
- Major creator publishes new script methodology
- New AI-cliche tells emerge from frontier models that need ban-list updates

The raw research for the title and thumbnail skills is at `projects/rookcast/knowledge/research/`. Update this skill when those research files reveal new voice patterns from competitor analysis.
