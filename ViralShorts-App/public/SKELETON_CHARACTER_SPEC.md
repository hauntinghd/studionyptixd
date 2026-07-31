# MrSkeleWelly character spec

`skeleton-gold-standard-reference.png` is the channel owner's gold standard:
"this skeleton image should be gold standard for how the skeleton should always
look." It is the authority on what the character *is*. This file records what
that image establishes, because two of its traits were previously mistaken for
rendering defects.

## Correct by design — never "fix" these

| Trait | Note |
|---|---|
| **Smooth polished ivory cranium** | No sutures, no temporal ridges, no bone texture on the dome. The featureless glossy skull is the character. |
| **Large round eyes with visible irises** | Seated in the sockets, prominent, slightly cartoon. Not a proportion error. |
| **Clear glossy transparent shell** | High refraction over ivory bone. |

These were briefly listed as structural defects (`skull_detail_failure`,
`eye_consistency_failure`) on the assumption that a featureless dome and large
eyes were failures. They were removed once the gold standard showed otherwise.
Blocking on them failed renders for looking correct, and "fixing" them produced
a different mascot. Genuine deviation is caught as identity drift against the
master reference instead - the reference defines the character, not a word list.

## Genuine defects — the quality bar the gold standard sets

| Defect | What correct looks like |
|---|---|
| Hand topology | Five digits per hand **including a thumb**, individually jointed, distinct phalanges |
| Shell integrity | Clean refraction, no stray scratch lines or cracks |
| Bone continuity | Every bone connected; nothing floating or detached |
| Dental arcade | Individual teeth defined, not a blank jaw |

## Why the gold standard is not the master reference

The gold standard is an action shot holding a basketball in an arena. Seedream
edit preserves the reference *scene*, not only the character: deriving a neutral
anchor from it kept the basketball and the arena in every attempt. That is the
same gym-prop leakage the negative prompts already fight.

So the identity anchor stays a neutral A-pose on a plain dark backdrop
(`canonical-skeleton-master-hires.png`), lifted to the gold standard's quality
bar rather than replaced by it. `canonical-skeleton-master-hires.prev.png` is
the pre-lift master, kept for one-file rollback.
