# Studio release bar

"Zero artifacting" is not a release bar. It is not a property code can hold, it
cannot be measured, and it cannot be failed — which means it cannot be passed
either. Across an animated slate of ~1,164 clips, flawless output would require a
per-clip defect rate near 1 in 21,000, on the worst-case subject for video
diffusion. Aiming there produced six paid canary runs and one finished video.

This document replaces that goal with one a render can actually be marked
against. It is deliberately achievable and deliberately falsifiable.

Machine-readable thresholds live in `studio_agent/release_bar.py`. This file is
the reasoning; that file is the authority.

---

## The three clauses

### 1. Render completion rate ≥ 95%

Measured across a fleet of renders, not a single one. A render **completes** when
it produces a finished video that is free of the three structural failures:

| Failure | Definition | How it is measured |
|---|---|---|
| Content-policy death | The job died on a provider content rejection | Job outcome |
| Freeze | Any frozen span ≥ 0.5s | `ffmpeg freezedetect`, on the finished file |
| Desync | Video duration differs from narration by > 0.10s | `ffprobe` on both |

Freeze and desync are called out specifically because both have shipped. The mux
pads video to the narration clock by cloning the final frame, so a beat plan that
under-covers the narration ends the video on a still image. That is invisible to
every test in the suite and obvious to every viewer.

### 2. Per-clip visible-defect rate ≤ 5%

Measured by **frame extraction and inspection**, never by a QA verdict and never
by a passing test. A clip counts as defective if an inspected frame from it shows
any defect a viewer would notice.

At twelve clips per short, 5% permits roughly one defective clip every two
videos.

**A render with no inspection evidence is not release-ready.** Unknown is not a
pass. The evaluator refuses to mark a render against this clause without
extracted-frame evidence, and says so as a distinct reason rather than silently
scoring it.

### 3. At most 1 defective beat per finished video, repairable in one attempt

Two conditions, both required:

- No more than **one** beat in the finished video carries a visible defect.
- That beat must have been **repaired within a single attempt**.

The second condition is what keeps cost bounded. A defect needing three attempts
is not a defect, it is a structural limitation being paid for repeatedly — and it
belongs in the STRUCTURAL bucket, which fails to a human instead of spending
money. Repair thrash is how a single short once cost $18.71.

---

## What this bar deliberately does not promise

**It does not promise flawless frames.** Hands, cranial detail, and eye
consistency are the current dominant defect classes, and they are reproduced by
the model on essentially every draw. Regeneration does not fix them; it re-rolls
them. They are addressed upstream at the reference still, not by retrying clips.

**It does not treat a green test suite as evidence of video quality.** The suite
proves the code did what it was told. Only extracted frames prove what the viewer
sees.

---

## Verdict shape

`evaluate_render()` returns a verdict with a per-clause breakdown and an overall
`release_ready` boolean. `evaluate_fleet()` aggregates completion rate across
renders, since clause 1 is only meaningful in aggregate.

A render is release-ready when every applicable clause passes **and** the
evidence needed to judge it is present.

## Overriding thresholds

Every threshold is environment-overridable for experiments
(`STUDIO_BAR_MIN_COMPLETION_RATE`, `STUDIO_BAR_MAX_CLIP_DEFECT_RATE`,
`STUDIO_BAR_MAX_DEFECTIVE_BEATS`, `STUDIO_BAR_MAX_REPAIR_ATTEMPTS`,
`STUDIO_BAR_FREEZE_MIN_SEC`, `STUDIO_BAR_DESYNC_TOLERANCE_SEC`). Overrides are
recorded in the verdict so a run cannot quietly grade itself on an easier scale.
