# Canonical Skeleton — Studio production lock

**Master still:** `ViralShorts-App/public/canonical-skeleton-master.png`  
**Live URL:** https://studio.nyptidindustries.com/canonical-skeleton-master.png

## How every Skeleton AI short is rendered

1. **Identity** — fixed from the master PNG (gym reference). Never regenerated from scratch per scene.
2. **Each scene still** — `fal-ai/bytedance/seedream/v4.5/edit` with `image_urls: [master (+ optional uniform roster)]`.
3. **Prompt** — describes only background, props, pose, and outfit. Character lock text forbids redesign.
4. **Uniform** — when a beat has an outfit, we optionally cache a roster still (edit master → outfit only), then pass it as a second ref for scene edits.
5. **Animation** — user picks i2v: `seedance` | `pixverse` | `kling_pro` (Studio Agent + Create Audio tab). Stills are not selectable.

## Code paths (all use edit)

| Entry | Module |
|-------|--------|
| Skeleton AI Create panel (`/api/skeleton-ai/scenes`, `/generate`) | `skeleton_ai/pipeline.py`, `skeleton_ai_router.py` |
| Studio Create template `skeleton` | `backend.py` `run_generation_pipeline`, `generate_scene_image` |
| Clone / viral detect → skeleton | `generate_scene_image` early exit |

## Env (Fly / `.env`)

```bash
SKELETON_USE_SEEDREAM_EDIT=true
SKELETON_GLOBAL_REFERENCE_IMAGE_URL=https://studio.nyptidindustries.com/canonical-skeleton-master.png
```

Set `SKELETON_USE_SEEDREAM_EDIT=false` only for emergency fallback to legacy ERNIE T2I.

## Local test

```powershell
python ops/test_one_skeleton_scene.py --model seedream_edit --scene "your background/props only"
```
