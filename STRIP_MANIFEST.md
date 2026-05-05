# Studio Strip-Down Manifest — 2026-05-05

Casey directive: gut everything except Catalyst, Refunds, Waitlist, Product Analysis. Rebuild Skeleton AI short-form first.

## Phase 1 — DELETE: cruft / debug / test / experimental scratch

```
check_active_5m.py          one-shot debug
check_db.py                 one-shot db check
check_detail.py             one-shot
check_imports_v2.py         one-shot
check_markets.py            one-shot
check_markets_endpoint.py   one-shot
check_nodes.py              one-shot
check_nodes2.py             one-shot
check_search_v2.py          one-shot
check_structure.py          one-shot
check_time.py               one-shot
check_wan_i2v.py            one-shot

debug_5m.py                 debug
debug_discovery.py          debug
debug_file.py               debug
debug_gamma.py              debug
debug_ids.py                debug
debug_libs.py               debug

test_book.py                test scratch
test_comfy.py               test scratch
test_discovery.py           test scratch
test_gen.py                 test scratch
test_once.py                test scratch
test_price_direct.py        test scratch
test_proxy.py               test scratch
test_run.py                 test scratch
api_test.py                 test scratch

scan_100.py                 scratch
deep_diag.py                debug
deep_scan.py                debug
deep_scan_file.py           debug

dump_api.py                 scratch
dump_gamma.py               scratch

shadow_v7_hyper.py          dead experiment
shadow_v8_hyper.py          dead experiment
shadow_v9_ultra.py          dead experiment
shadow_watcher.py           dead experiment

fix_t2v.py                  one-shot fix
find_real.py                discovery scratch
find_slugs.py               discovery scratch

market_debug.py             debug
discover_markets.py         scratch
inspect_market_v2.py        scratch
diagnostic.py               debug
diagnostic_log.py           debug

net_test.py                 test scratch
fast_dl.py                  one-shot
launcher.py                 dead launcher
fal_gate.py                 dead gate

generate_skeleton_dataset.py        OBSOLETE (LoRA training abandoned)
generate_skeleton_dataset_v2.py     OBSOLETE

OpenClaw_Studio.py          different project (move to /d/recaps/openclaw_archive/)
openclaw_web.py             different project
```

## Phase 2 — DELETE: duplicate viral-shorts sub-apps (3 of 4)

```
Sniper-App/         208K   different app (sniper trading bot — different project)
viral_shorts_app/    31K   duplicate
app_viral_shorts/    17K   duplicate
ViralShorts-App/    7.6M   KEEP if active, otherwise toss
```

## Phase 3 — DELETE: dead pipeline modules (Create / Clone / Long-form / Thumbnail / Auto-Clip)

These get rebuilt fresh for Skeleton AI; the existing implementations are abandoned.

```
video_pipeline.py           150 KB — long-form pipeline (REBUILD from scratch)
backend_script_prompts.py    58 KB — old script-gen prompts (REBUILD with Grok)
backend_image_prompts.py     41 KB — model dispatch (KEEP for reference, may reuse)
backend_url_ingest.py               url ingest for Clone tab — DELETE
backend_demo.py                     Product Demo tab — DELETE
```

## Phase 4 — KEEP UNTOUCHED (Catalyst + Refunds + Waitlist + Product Analysis)

```
catalyst.py                          API routes (bulk-ingest, etc.)
catalyst_backfill.py                 backfill
backend_catalyst_blueprint.py        Flask blueprint
backend_catalyst_core.py             core ranking logic
backend_catalyst_learning.py         harvest + learning
backend_catalyst_profiles.py         per-channel profiles
backend_catalyst_reference.py        reference videos
backend_youtube_catalyst_routes.py   YT API routes

youtube.py                           YouTube API client
youtube_quota.py                     quota infra
youtube_cache.py                     cache layer
analytics.py                         engagement tracking

billing.py                           Stripe + PayPal + Refunds
auth.py                              OAuth + sessions
routes.py                            route definitions

migrations/                          Supabase schema
ops/                                 deploy scripts
runpod-serverless/                   RunPod templates
Dockerfile                           container build
backend_requirements.txt             deps
```

## Phase 5 — backend.py (1.16 MB monolith)

DEFER. backend.py contains a mix of Catalyst routes (KEEP) + Create/Clone/Long-form/Thumb/Auto-Clip routes (DELETE). Surgical extraction is risky — Catalyst code lives next to dead code. 

Plan:
1. Build NEW `skeleton_ai/` module ALONGSIDE backend.py (no surgery needed)
2. Once Skeleton AI is live + proven, return and carve dead routes out of backend.py
3. Eventually retire backend.py once all live routes are in clean modules

## Phase 6 — Frontend strip

```
KEEP nav tabs:
  • Home / Dashboard
  • Catalyst tab
  • (Admin) Refunds tab
  • (Admin) Product Analysis tab — demoted to admin-only
  • Waitlist (existing flow stays)
  • Create tab (new — Skeleton AI ONLY for now, rest disabled)

DELETE nav tabs:
  • Clone
  • Long Form
  • Thumbnails
  • Product Demo
  • Auto Clipper
```

## What gets rebuilt

```
skeleton_ai/
  pipeline.py             end-to-end orchestrator
  scripting_grok.py       xAI Grok 4.1 Fast Reasoning client
  stills_seedream.py      seedream v4.5 with canonical prompt
  i2v_seedance.py         Seedance 2.0 default i2v (Kling 2.1 Pro premium upgrade)
  voice_elevenlabs.py     ElevenLabs TTS client
  compose.py              ffmpeg compose: trim + caption burn + concat + mux
  captions.py             two-tier caption renderer (orange/white + black stroke)
  prompts/
    base_style.py         canonical spec prompt fragment
    scene_templates.py    per-role scene generators
    idea_lists.py         Human Limits / Marvel vs DC / Ancient History / Futuristic Socrates

skeleton_ai_routes.py     Flask routes (POST /api/skeleton-ai/generate, etc)
skeleton_ai_credit.py     AC deduction integration with billing.py
```
