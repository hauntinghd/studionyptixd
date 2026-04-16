"""
Upsert RunPod serverless template + endpoint via GraphQL.
Designed to run AFTER the Docker image is already built+pushed by GH Actions.

Reads config from .runpod.env + repo-root .env (same sourcing as deploy.sh).

Usage:
    python runpod-serverless/upsert_runpod_endpoint.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
RUNPOD_GQL = "https://api.runpod.io/graphql"

TEMPLATE_NAME = "studio-backend-serverless"
ENDPOINT_NAME = "studio-backend"

# Env keys baked into the RunPod template.  Format: "KEY" or "KEY:default".
ENV_KEYS = [
    "APP_DATA_DIR:/runpod-volume/studio",
    "SITE_URL",
    "XAI_API_KEY",
    "ELEVENLABS_API_KEY",
    "FAL_AI_KEY",
    "PIKZELS_API_KEY",
    "ALGROW_API_KEY",
    "COMFYUI_URL",
    "RUNWAYML_API_SECRET",
    "SUPABASE_URL",
    "SUPABASE_ANON_KEY",
    "SUPABASE_JWT_SECRET",
    "SUPABASE_SERVICE_KEY",
    "YOUTUBE_API_KEY",
    "YOUTUBE_API_KEYS",
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REDIRECT_URI",
    "STRIPE_SECRET_KEY",
    "STRIPE_WEBHOOK_SECRET",
    "STRIPE_TOPUP_PUBLIC_ENABLED",
    "PAYPAL_CLIENT_ID",
    "PAYPAL_CLIENT_SECRET",
    "PAYPAL_ENV:live",
    "PAYPAL_WEBHOOK_ID",
    "IMAGE_PROVIDER_ORDER:fal",
    "XAI_IMAGE_FALLBACK_ENABLED:true",
    "HIDREAM_ENABLED:false",
    "HIDREAM_EDIT_ENABLED:false",
    "SKELETON_REQUIRE_WAN22:false",
    "RUNPOD_IMAGE_FEEDBACK_ENABLED:false",
    "RUNPOD_COMPOSITOR_ENABLED:false",
    "CHATSTORY_FORCE_LOCAL_TTS:false",
    "CHATSTORY_ELEVENLABS_MODEL_ID:eleven_multilingual_v2",
    "FAL_IMAGE_BACKUP_MODEL:imagen4_fast",
    "XAI_IMAGE_MODEL:grok-imagine-image-pro",
    "XAI_VIDEO_MODEL:grok-imagine-video",
    "PIKZELS_THUMBNAIL_MODEL:pkz-3",
    "PIKZELS_RECREATE_MODEL:pkz-3",
    "PIKZELS_TITLE_MODEL:pkz-3",
    "RUNPOD_API_KEY",
    "RUNPOD_COMPOSITOR_ENDPOINT_ID",
]


def load_envs() -> None:
    """Source repo .env then .runpod.env into os.environ (same order as deploy.sh)."""
    for f in [REPO_ROOT / ".env", SCRIPT_DIR / ".runpod.env"]:
        if not f.exists():
            continue
        for line in f.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def build_env_list() -> list[dict]:
    """Build [{key, value}, ...] from ENV_KEYS using current os.environ."""
    out: list[dict] = []
    for entry in ENV_KEYS:
        key = entry.split(":")[0]
        default = entry[len(key) + 1:] if ":" in entry else ""
        value = os.environ.get(key, default)
        if not value:
            continue
        out.append({"key": key, "value": value})
    return out


def gql(api_key: str, query: str, variables: dict) -> dict:
    with httpx.Client(timeout=45) as c:
        r = c.post(
            f"{RUNPOD_GQL}?api_key={api_key}",
            headers={"Content-Type": "application/json"},
            json={"query": query, "variables": variables},
        )
    data = r.json()
    if "errors" in data:
        print(f"GraphQL errors: {json.dumps(data['errors'], indent=2)}", file=sys.stderr)
        raise SystemExit(2)
    return data


def main() -> int:
    load_envs()
    api_key = os.environ.get("RUNPOD_API_KEY", "").strip()
    registry = os.environ.get("DOCKER_REGISTRY", "").strip()
    image_tag = os.environ.get("IMAGE_TAG", "studio-backend:v1.0").strip()
    volume_id = os.environ.get("NETWORK_VOLUME_ID", "").strip()
    app_data_dir = os.environ.get("STUDIO_APP_DATA_DIR", "/runpod-volume/studio").strip()

    if not api_key:
        print("ERROR: RUNPOD_API_KEY missing", file=sys.stderr)
        return 2
    if not registry:
        print("ERROR: DOCKER_REGISTRY missing", file=sys.stderr)
        return 2

    full_image = f"{registry}/{image_tag}"
    env_list = build_env_list()
    # Force APP_DATA_DIR to the volume mount
    found = False
    for e in env_list:
        if e["key"] == "APP_DATA_DIR":
            e["value"] = app_data_dir
            found = True
            break
    if not found:
        env_list.append({"key": "APP_DATA_DIR", "value": app_data_dir})

    # Build GHCR registry auth for RunPod to pull private images.
    # Format: base64("username:token") — RunPod docs call this "dockerRegistryAuth".
    import base64, subprocess
    ghcr_auth = ""
    if "ghcr.io" in registry:
        try:
            gh_token = subprocess.check_output(
                ["gh", "auth", "token"], stderr=subprocess.DEVNULL, text=True
            ).strip()
            gh_user = subprocess.check_output(
                ["gh", "api", "user", "--jq", ".login"],
                stderr=subprocess.DEVNULL, text=True,
                env={**os.environ, "MSYS_NO_PATHCONV": "1"},
            ).strip()
            if gh_token and gh_user:
                ghcr_auth = base64.b64encode(f"{gh_user}:{gh_token}".encode()).decode()
                print(f"GHCR auth:  {gh_user} (token {gh_token[:8]}...)")
        except Exception as e:
            print(f"WARNING: could not build GHCR auth ({e}). RunPod may fail to pull if image is private.")

    print(f"Image:      {full_image}")
    print(f"Volume ID:  {volume_id or '<none>'}")
    print(f"Env vars:   {len(env_list)}")
    print(f"Registry auth: {'yes' if ghcr_auth else 'no'}")
    print()

    # ---- Create/reuse container registry auth (for private GHCR images) ----
    registry_auth_id = ""
    if ghcr_auth and gh_token and gh_user:
        print("Creating container registry auth for GHCR...")
        try:
            auth_data = gql(api_key, """
                mutation($input: SaveRegistryAuthInput!) {
                    saveRegistryAuth(input: $input) { id name }
                }
            """, {
                "input": {
                    "name": "ghcr-studio",
                    "username": gh_user,
                    "password": gh_token,
                }
            })
            registry_auth_id = auth_data["data"]["saveRegistryAuth"]["id"]
            print(f"  registry auth id: {registry_auth_id}")
        except SystemExit:
            print("  WARNING: could not create registry auth. Trying without it.")

    # ---- Upsert template ----
    print("Upserting RunPod template...")
    template_input: dict = {
        "name": TEMPLATE_NAME,
        "imageName": full_image,
        "dockerArgs": "",
        "ports": "",
        "volumeInGb": 0,
        "containerDiskInGb": 50,
        "env": env_list,
    }
    if registry_auth_id:
        template_input["containerRegistryAuthId"] = registry_auth_id
    tmpl_data = gql(api_key, """
        mutation($input: SaveTemplateInput!) {
            saveTemplate(input: $input) { id name }
        }
    """, {"input": template_input})
    template_id = tmpl_data["data"]["saveTemplate"]["id"]
    print(f"  template id: {template_id}")

    # ---- Upsert endpoint ----
    print("Upserting RunPod endpoint...")
    ep_data = gql(api_key, """
        mutation($input: EndpointInput!) {
            saveEndpoint(input: $input) { id name }
        }
    """, {
        "input": {
            "name": ENDPOINT_NAME,
            "templateId": template_id,
            "gpuIds": "CPU3C,CPU5C",
            "networkVolumeId": volume_id,
            "locations": "",
            "idleTimeout": 5,
            "scalerType": "QUEUE_DELAY",
            "scalerValue": 4,
            "workersMin": 0,
            "workersMax": 3,
            "flashboot": True,
        }
    })
    endpoint_id = ep_data["data"]["saveEndpoint"]["id"]
    print(f"  endpoint id: {endpoint_id}")
    print(f"  endpoint url: https://api.runpod.ai/v2/{endpoint_id}/runsync")

    # Persist endpoint ID to .runpod.env
    env_file = SCRIPT_DIR / ".runpod.env"
    if env_file.exists():
        text = env_file.read_text(encoding="utf-8")
        lines = text.splitlines()
        replaced = False
        for i, line in enumerate(lines):
            if line.startswith("RUNPOD_ENDPOINT_ID=") or line.startswith("# RUNPOD_ENDPOINT_ID="):
                lines[i] = f"RUNPOD_ENDPOINT_ID={endpoint_id}"
                replaced = True
                break
        if not replaced:
            lines.append(f"RUNPOD_ENDPOINT_ID={endpoint_id}")
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"  (saved RUNPOD_ENDPOINT_ID to .runpod.env)")

    print()
    print("Done. Next: wire CF Worker with:")
    print(f"  wrangler secret put RUNPOD_API_KEY")
    print(f"  wrangler secret put RUNPOD_ENDPOINT_ID  # {endpoint_id}")
    print(f"  wrangler deploy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
