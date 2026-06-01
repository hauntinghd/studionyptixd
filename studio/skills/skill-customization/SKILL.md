---
name: skill-customization
description: >-
  How to author a new skill or fork an existing one in this runtime. Load when the user asks to (a) create a new skill ("write a skill that does X", "add a skill for Y", "turn this workflow into a skill") OR (b) make a STRUCTURAL change to how an existing skill behaves on THIS channel ("for this channel, the thumbnail step should always X", "stop loading the avatar phase here", "make script-writing use a 6-beat structure instead of 12"). Use this skill whenever the user asks to add, replace, restr
---

# Authoring and forking skills

This skill covers two related operations:

1. **Authoring** — drafting a brand-new SKILL.md (and optional companion `.md` files) for a workflow that doesn't have one yet.
2. **Forking** — making a per-channel structural change to an existing skill in this runtime.

Most of the writing guidance below applies to both. The runtime mechanics (auto-mirroring, fork-vs-Notes, `ask_user` confirmation) live in the second half.

---

## Capture intent

Start by understanding what the user actually wants. The current conversation may already contain a workflow they want to capture (e.g., they say "turn this into a skill"). Extract answers from the history first — the steps taken, corrections made, the input/output formats observed. Confirm the gaps before proceeding.

1. What should this skill enable the agent to do?
2. When should this skill trigger? (what user phrases / contexts)
3. What's the expected output format?

For an *edit* (forking an existing skill), additionally clarify: what behavior does the user want changed, and is the change structural enough to justify a fork rather than a `CHANNEL.md → Notes` entry? See "Fork vs Notes" below before writing anything.

Proactively ask about edge cases, dependencies, and success criteria. Come prepared with context to reduce burden on the user.

---

## Anatomy of a skill in this runtime

```
skill-name/
├── SKILL.md (required)
│   ├── YAML frontmatter (name, description required)
│   └── Markdown body
└── Companion .md files (optional)
    └── bank.md / cookbook.md / library.md / shot-bank.md / config-bank.md / etc.
```

Companion files are loaded with the regular `Read` tool, only when the SKILL.md body tells the agent to.

This runtime does NOT bundle `scripts/`, `references/`, or `assets/` directories the way some other skill ecosystems do. Reference content lives in plain `.md` companions alongside SKILL.md.

---

## Progressive disclosure

Skills use a three-level loading system. Knowing which level something lives at is the most important architectural choice you'll make:

1. **Metadata** (`name` + `description`) — Always in the agent's context (~100 words). This is the only thing visible until the skill triggers, so it must contain *all* the "when to use" information.
2. **SKILL.md body** — In context whenever the agent calls `read_skill` for this slug. Aim for under 500 lines.
3. **Companion `.md` files** — Loaded only when SKILL.md instructs the agent to `Read` them.

These limits are approximate, but every line in SKILL.md is paid for in tokens every time the skill triggers — prune.

**Key patterns:**

- Keep SKILL.md under 500 lines. If you're approaching the limit, push detail into a companion `.md` and link to it with clear pointers about *when* to read.
- Reference companion files clearly from SKILL.md with a one-line description of what's in each.
- For large companion files (>300 lines), include a table of contents at the top.

**Domain organization** — when one skill supports multiple variants, organize by variant in companions:

```
ai-host-setup/
├── SKILL.md (matrix + selection logic)
├── host-doctor.md
├── host-financial-advisor.md
└── host-teacher.md
```

The agent reads only the variant SKILL.md tells it to.

---

## Frontmatter

The frontmatter has two required fields:

- **name** — skill identifier (kebab-case, matches the directory name).
- **description** — when to trigger and what the skill does. This is the primary triggering mechanism. Include both *what* the skill does AND *specific contexts* for when to use it. All "when to use" info goes here, not in the body.

The agent has a tendency to *under*-trigger skills — to not load them when they'd be useful. Make descriptions a little bit "pushy". Instead of:

> "How to design a thumbnail."

write:

> "How to design a thumbnail. Make sure to use this skill whenever the user mentions thumbnails, click-through, A/B variants, or wants to redesign the visual hook for a video, even if they don't explicitly ask for a 'thumbnail.'"

The pushiness is doing real work — it surfaces the skill on adjacent phrasings the agent would otherwise miss.

---

## Writing the body

### Style

Explain *why* things matter rather than leaning on heavy-handed musty MUSTs. The model is smart and has good theory of mind — when given the reason behind a rule, it can generalize to edge cases instead of failing the moment a real case doesn't match an example. If you find yourself writing ALWAYS or NEVER in all caps, or super-rigid structures, that's a yellow flag — reframe and explain the reasoning. That's a more humane, more powerful, and more effective approach.

Try to make the skill general, not super-narrow to specific examples. Write a draft, then look at it with fresh eyes and improve it.

### Patterns

Prefer the imperative form in instructions ("Pick a structure", not "You should pick a structure").

**Defining output formats** — write the template inline:

```markdown
## Report structure
Use this exact template:
# [Title]
## Executive summary
## Key findings
## Recommendations
```

**Examples** — include them; they pull weight that prose can't:

```markdown
## Commit message format
**Example 1:**
Input: Added user authentication with JWT tokens
Output: feat(auth): implement JWT-based authentication
```

### Principle of "no surprise"

A skill's contents must not surprise the user in their intent if described. Don't go along with requests to create misleading skills or skills designed to facilitate unauthorized access, data exfiltration, or other malicious activity. Roleplay-style or creative scenarios are fine.

---

## Improving an existing skill

When the user asks to improve a skill — whether they're editing one they wrote or one that shipped with the channel:

1. **Generalize from the feedback.** A skill is used many times across many prompts. If you over-fit to the one example the user just complained about, you'll fix today's prompt and break tomorrow's. Rather than fiddly overfitty patches or oppressive MUSTs, reach for different metaphors and patterns.

2. **Keep the prompt lean.** Remove things that aren't pulling their weight. Read the skill cold and ask: what is this paragraph actually doing for the agent? If you can't answer in one sentence, cut it.

3. **Explain the why.** If you find yourself writing ALWAYS or NEVER, that's a yellow flag — reframe and explain the reasoning so the model understands why the thing matters.

4. **Look for repeated work.** If multiple uses of the skill end up with the agent independently producing similar artifacts (a checklist, a structure, a starter prompt), bundle that into a companion `.md` once and have the skill reference it. Saves every future invocation from reinventing the wheel.

Take your time. Write a draft revision, then look at it anew and improve. Get into the head of the user — what do they want, what do they need?

---

## Runtime mechanics: forking in this app

Skills under `/workspace/.claude/skills/<slug>/SKILL.md` are a **two-layer system**:

1. **Global pack** — the platform's default skill catalog. Source of truth lives in the host repo, shipped to every channel on every session boot.
2. **Channel overrides** — per-channel forks layered on top. When a channel forks a skill, that file replaces the global one for that channel only.

The agent sees one merged tree. There's no "global vs override" distinction at read time — `read_skill("<slug>")` returns whatever the channel sees, period.

### Fork vs `CHANNEL.md → Notes`

| Signal | Where it goes |
|---|---|
| "I prefer the title to be punchier on this channel" | CHANNEL.md → Notes |
| "Always use a colder color palette for thumbnails here" | CHANNEL.md → Notes |
| "Don't load the avatar phase, this is a voiceover channel" | Fork (rewrite the FLOW.md or onboarding skill conditional) |
| "Use a 6-beat script structure instead of 12 for this channel" | Fork `script-writing` |
| "The thumbnail-design skill should always do A/B/C steps in a different order" | Fork `thumbnail-design` |

Rule of thumb: **if the change is a value the global skill already considers ("which palette?", "punchier?"), it's a Note. If the change replaces logic the global skill encoded ("different beats", "different phases", "different selection algorithm"), it's a fork.**

When in doubt, prefer Notes — Notes are read by every skill at run time and don't fork the channel off the global default. Forking means the channel stops getting global-skill updates upstream; that's the right call sometimes, but it's a real cost.

### How to fork

You don't need a special tool. Editing the SKILL.md **is** the fork:

```
read_skill("thumbnail-design")
# returns the current contents (global, or already-forked if previously edited)

Edit("/workspace/.claude/skills/thumbnail-design/SKILL.md", ...)
# writes the change. The runtime auto-mirrors this to S3 at
# channels/<id>/.claude/skills/thumbnail-design/SKILL.md so it persists
# across sessions. From now on, this channel sees the edit, not the
# global default.
```

Same for companion files (`bank.md`, `composition-bank.md`, etc.) — same path, same auto-persistence.

**You cannot edit global skills from inside a session.** The runtime's persistence layer is wired to the channel, not the global pack. If you Edit a SKILL.md, you're forking it for this channel. There is no "save globally" path; that requires a repo change by the developer.

### Authoring a brand-new skill in this runtime

Same mechanics. `Write` a new file at `/workspace/.claude/skills/<new-slug>/SKILL.md` and the runtime auto-persists it to channel storage. The new skill becomes available in the agent's skills index on the next session boot for this channel only.

If the user wants the new skill global (shipped to every channel), it's a developer-side change. Preserve the channel-local version they wrote, and offer to draft a summary they can hand off to the developer.

### Confirm before forking or shipping a new skill

Forks and new channel-local skills are durable and only revert through manual re-paste. Before forking or creating, fire `ask_user` with `type: "confirm"`:

```
ask_user({
  questions: [{
    id: "fork_confirm",
    prompt: "I want to fork the `thumbnail-design` skill for THIS channel and change the candidate count from 4 to 2. Future sessions on this channel will use the forked version. Globally-shipped updates to thumbnail-design won't propagate here. Lock it in?",
    type: "confirm"
  }]
})
```

If they say yes, edit. If they say no, fall back to writing the constraint to CHANNEL.md Notes ("Thumbnails: only generate 2 candidates per video") and let the global skill consume the note.

### Reverting a fork

There's no clean "delete the override" gesture today. To revert, manually re-paste the global skill content into the override file. The fork then becomes content-identical to global, and the override file just sits there, harmlessly mirroring global. If the user explicitly asks to "throw away the fork on `<slug>`":

1. Tell them: "I can't directly delete a fork from inside a session — but I can paste the global default back over it, which has the same effect."
2. The global default lives in the host repo. If you don't have a copy on disk, ask the user to supply one OR offer to preserve the fork's intent in CHANNEL.md Notes and revert the file from a developer-facing tool.

### What overrides DON'T do

- They don't override Tier-0 runtime docs (`PIPELINE_OVERVIEW.md`, etc.). Those are platform-wide invariants — record local exceptions in `CHANNEL.md → Notes` instead.
- They don't compose with the global skill — overrides REPLACE wholesale, file-by-file. If the global `thumbnail-design/SKILL.md` is updated upstream, the fork is frozen at whatever was written. That's why "fork only for structural changes" is the rule.

---

## Anti-patterns

- **Forking a skill to record a single channel preference.** Use Notes.
- **Forking without confirming.** Always `ask_user` confirm; the user owns the fork moment.
- **Editing a SKILL.md and assuming it doesn't persist.** It does. Every Edit/Write under `/workspace/.claude/skills/` is auto-mirrored to channel storage.
- **Trying to edit a global skill so other channels benefit.** Not possible from a session. Tell the user it's a developer-side change and offer to summarize what they want changed.
- **Authoring a new skill with a vague description.** The description is the only thing in context until `read_skill` runs. If it doesn't describe specific trigger contexts, the skill never fires when it should.
- **Skipping the draft-then-revise pass.** First drafts of skills are reliably bloated and over-fit. Read it cold before locking in.
