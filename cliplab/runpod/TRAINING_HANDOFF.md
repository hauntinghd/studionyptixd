# ClipLab RunPod Training Handoff

**For:** the Cursor agent wiring RunPod training + custom inference  
**From:** Studio ClipLab scaffold (repo worktree)  
**Goal:** Replace LLM-only virality scoring + OpenCV face reframe with **your trained weights** on the shared RunPod network volume.

---

## What Studio already ships

| Component | Location | Status |
|-----------|----------|--------|
| ClipLab API | `cliplab_router.py` → `/api/cliplab/*` | Admin-only lane |
| Transcript ingest | `cliplab/transcribe.py` | VTT + Fal Whisper |
| Segment ranking | `cliplab/intelligence.py` | Fal/OpenRouter LLM + RunPod hook |
| Face reframe | `cliplab/reframe.py` | OpenCV Haar/DNN + RunPod hook |
| Render | `cliplab/render.py` | ffmpeg 9:16 + karaoke captions |
| Model registry | `cliplab/model_registry.py` | Reads volume JSON |
| Training stubs | `cliplab/runpod/train_*.py` | Replace with real torch loops |
| Inference stub | `cliplab/runpod/inference_handler.py` | Deploy as **separate** RunPod endpoint |

---

## Volume layout (create on RunPod network volume)

```text
/runpod-volume/studio/cliplab/
├── datasets/
│   ├── cliplab_feedback.jsonl      # virality training rows (from Studio user feedback)
│   ├── cliplab_reframe.jsonl       # face trajectory labels
│   └── cliplab_thumbnails.jsonl    # optional — shared with ThumbLab packaging model
├── models/
│   ├── model_registry.json         # active checkpoint pointers (copy from repo seed)
│   ├── virality/v1/
│   │   ├── model.pt
│   │   └── config.json
│   └── reframe/v1/
│       ├── tracker.pt
│       └── config.json
└── exports/                        # bootstrap datasets exported from Studio
```

Seed registry on volume:

```bash
mkdir -p /runpod-volume/studio/cliplab/{datasets,models,exports}
cp cliplab/runpod/model_registry.json /runpod-volume/studio/cliplab/models/model_registry.json
```

---

## Environment variables (RunPod template + Fly Studio API)

### Main Studio API (`nyptid-studio` / RunPod serverless bridge)

| Key | Purpose |
|-----|---------|
| `STUDIO_APP_DATA_DIR` | `/runpod-volume/studio` |
| `CLIPLAB_VIRALITY_BACKEND` | `local_llm` → flip to `runpod_custom_v1` after training |
| `CLIPLAB_REFRAME_BACKEND` | `opencv_face` → flip to `runpod_face_v1` after training |
| `RUNPOD_CLIPLAB_ENDPOINT_ID` | **Separate** endpoint for ClipLab inference handler |
| `RUNPOD_API_KEY` | Same account key |
| `FAL_AI_KEY` | Whisper transcription fallback |

### ClipLab inference endpoint (dedicated)

| Key | Value |
|-----|-------|
| `STUDIO_APP_DATA_DIR` | `/runpod-volume/studio` |
| Handler | `python cliplab/runpod/inference_handler.py` |
| GPU | T4 or better for reframe; CPU ok for virality MLP |
| Volume | Same `NETWORK_VOLUME_ID` as Studio |

---

## Dataset schemas

### `cliplab_feedback.jsonl` (virality reranker)

One JSON object per line:

```json
{
  "prompt": "find every pricing complaint",
  "transcript_snippet": "the pricing model is the biggest blocker...",
  "segment_start": 184.2,
  "segment_end": 197.8,
  "virality_score": 87,
  "kept": true,
  "published": true,
  "edited_hook": "",
  "channel_id": "UCA_cn0-EW2UbBsyEA0TahNA",
  "source_video_id": "clipvid_123"
}
```

**Label priority:** `published=true` > `kept=true` > LLM `virality_score`  
**Negative examples:** `kept=false` rows — important for reranker.

Studio writes these via `POST /api/cliplab/feedback`.

### `cliplab_reframe.jsonl` (face tracker)

```json
{
  "video_path": "/runpod-volume/studio/cliplab/exports/sample_001.mp4",
  "frames": [
    {"t": 0.0, "cx": 960, "cy": 520, "face_w": 210, "face_h": 210, "confidence": 0.92}
  ],
  "crop_mode": "9:16",
  "source": "opencv_bootstrap"
}
```

Bootstrap: run OpenCV tracker on your millions of clips → human QA subset → fine-tune.

---

## Training commands (RunPod GPU pod with volume mounted)

```bash
cd /workspace/repo   # git clone this worktree
pip install torch torchvision sentence-transformers

# 1. Virality reranker
python cliplab/runpod/train_virality_scorer.py \
  --dataset /runpod-volume/studio/cliplab/datasets/cliplab_feedback.jsonl \
  --out /runpod-volume/studio/cliplab/models/virality/v1 \
  --epochs 5

# 2. Face reframe tracker
python cliplab/runpod/train_face_reframe.py \
  --dataset /runpod-volume/studio/cliplab/datasets/cliplab_reframe.jsonl \
  --out /runpod-volume/studio/cliplab/models/reframe/v1 \
  --epochs 10
```

Replace stub saves in `train_*.py` with real training — stubs exist so the pipeline is wired before weights exist.

---

## Activate trained models

Edit `/runpod-volume/studio/cliplab/models/model_registry.json`:

```json
{
  "virality_scorer": {
    "active": "runpod_custom_v1",
    "checkpoints": {
      "runpod_custom_v1": {
        "path": "virality/v1/model.pt",
        "config": "virality/v1/config.json",
        "status": "ready"
      }
    }
  },
  "face_reframe": {
    "active": "runpod_face_v1",
    "checkpoints": {
      "runpod_face_v1": {
        "path": "reframe/v1/tracker.pt",
        "config": "reframe/v1/config.json",
        "status": "ready"
      }
    }
  }
}
```

Set on Studio API:

```bash
CLIPLAB_VIRALITY_BACKEND=runpod_custom_v1
CLIPLAB_REFRAME_BACKEND=runpod_face_v1
RUNPOD_CLIPLAB_ENDPOINT_ID=<your-cliplab-endpoint-id>
```

Redeploy Studio (Fly) or restart RunPod workers — **no frontend changes required**.

---

## Inference contract (implement in `inference_handler.py`)

### `task: score_segments`

**Input:**

```json
{
  "task": "score_segments",
  "prompt": "user prompt",
  "transcript_excerpt": "first 8k chars",
  "segments": [{"start": 1, "end": 30, "virality_score": 70, "transcript_snippet": "..."}],
  "weights_path": "/runpod-volume/studio/cliplab/models/virality/v1/model.pt"
}
```

**Output:**

```json
{
  "segments": [
    {"start": 1, "end": 30, "virality_score": 91, "model_source": "runpod_custom_v1"}
  ]
}
```

Called from `cliplab/intelligence.py` → `_score_with_runpod()`.

### `task: reframe_trajectory`

**Input:**

```json
{
  "task": "reframe_trajectory",
  "video_path": "/path/on/shared/volume.mp4",
  "start_sec": 120,
  "duration_sec": 45,
  "weights_path": "/runpod-volume/studio/cliplab/models/reframe/v1/tracker.pt"
}
```

**Output:**

```json
{
  "trajectory": [
    {"t": 120.0, "cx": 980, "cy": 540, "face_w": 200, "face_h": 200, "confidence": 0.94}
  ]
}
```

Called from `cliplab/reframe.py` → `_runpod_face_trajectory()`.

---

## Deploy ClipLab inference endpoint (separate from Studio API)

1. **Dockerfile** — extend `runpod-serverless/Dockerfile` or new `cliplab/runpod/Dockerfile`:
   - Base: `runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel`
   - `COPY cliplab ./cliplab`
   - `CMD python cliplab/runpod/inference_handler.py`

2. **Create endpoint** via RunPod dashboard or `runpod-serverless/upsert_runpod_endpoint.py` pattern with new template name `cliplab-inference`.

3. **Mount same network volume** as `nyptid-studio` so weights path matches.

4. **Smoke test:**

```bash
curl -X POST "https://api.runpod.ai/v2/$ENDPOINT_ID/runsync" \
  -H "Authorization: Bearer $RUNPOD_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"input":{"task":"health"}}'
```

---

## Studio API routes (for integration testing)

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/cliplab/status` | Registry + backend readiness |
| POST | `/api/cliplab/ingest/upload` | Upload long video |
| POST | `/api/cliplab/ingest/youtube` | YouTube URL + auto captions |
| POST | `/api/cliplab/analyze` | Prompt → ranked segments |
| POST | `/api/cliplab/render` | Render selected segments 9:16 |
| POST | `/api/cliplab/feedback` | Log keep/publish for training |
| GET | `/api/cliplab/clips/{video_id}/{filename}` | Download rendered clip |

Poll job status: `GET /api/status/{job_id}` (same as ThumbLab).

---

## ThumbLab shared training (optional)

Thumbnail vision A/B feedback can export to:

`/runpod-volume/studio/cliplab/datasets/cliplab_thumbnails.jsonl`

Fields: `reference_scores`, `ab_scoring`, `kept`, `ctr_outcome` — same reranker family as packaging/virality.

---

## Checklist for RunPod agent

- [ ] Network volume has `cliplab/models/model_registry.json`
- [ ] Import millions of clips → bootstrap `cliplab_reframe.jsonl` via OpenCV batch
- [ ] Import clip performance data → `cliplab_feedback.jsonl`
- [ ] Replace stub training loops with torch models
- [ ] Deploy `cliplab-inference` endpoint with volume mount
- [ ] Set `RUNPOD_CLIPLAB_ENDPOINT_ID` on Studio
- [ ] Flip registry `active` to `runpod_custom_v1` / `runpod_face_v1`
- [ ] Run end-to-end: ingest → analyze → render → feedback → retrain loop

---

## Repo files to read first

1. `cliplab/intelligence.py` — RunPod virality hook  
2. `cliplab/reframe.py` — RunPod face hook  
3. `cliplab/model_registry.py` — weight loading  
4. `cliplab_router.py` — HTTP surface  
5. `heliosclip_plan_totalbuild.md` — product spec  

Questions → Casey. Do **not** merge ClipLab into `video_pipeline.py`; keep isolated like ThumbLab.
