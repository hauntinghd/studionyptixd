from __future__ import annotations

import re
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "contabo"


def _read(name: str) -> str:
    return (OPS / name).read_text(encoding="utf-8")


def _service_block(compose: str, service: str, next_service: str | None) -> str:
    start = compose.index(f"  {service}:\n")
    if next_service is not None:
        end = compose.index(f"  {next_service}:\n", start + 1)
    else:
        end = compose.index("\nnetworks:\n", start + 1)
    return compose[start:end]


def test_studio_runpod_mutators_are_fail_closed_tombstones() -> None:
    canonical_api = "https://api-studio.nyptidindustries.com"
    env_example = (
        ROOT / "runpod-serverless" / ".runpod.env.example"
    ).read_text(encoding="utf-8")
    paypal_patch = (
        ROOT / "runpod-serverless" / "patch_paypal_webhook.py"
    ).read_text(encoding="utf-8")

    assert f"GOOGLE_REDIRECT_URI={canonical_api}/api/oauth/google/youtube/callback" in env_example
    assert f"PAYPAL_WEBHOOK_URL={canonical_api}/api/paypal/webhook" in env_example
    assert f'NEW_URL = "{canonical_api}/api/paypal/webhook"' in paypal_patch
    assert "STUDIO_RUNPOD_PRODUCTION_ENABLED=false" in env_example
    assert "STUDIO_RUNPOD_LONGFORM_ENABLED=false" in env_example
    assert "RUNPOD_API_KEY=" in env_example
    assert "XAI_API_KEY=" in env_example

    mutators = (
        "deploy.sh",
        "create_runpod_volume.py",
        "patch_endpoint_scale.sh",
        "save_runpod_template.pl",
        "upsert_runpod_endpoint.py",
        "upsert_cliplab_endpoint.py",
    )
    forbidden_network_markers = (
        "api.runpod.io",
        "api.runpod.ai",
        "httpx",
        "requests",
        "urllib",
        "curl ",
        "saveTemplate",
        "mutation Save",
    )
    for name in mutators:
        source = (ROOT / "runpod-serverless" / name).read_text(encoding="utf-8")
        assert "RETIRED:" in source
        for marker in forbidden_network_markers:
            assert marker not in source

    assert not (ROOT / ".github" / "workflows" / "build-studio-serverless.yml").exists()

    legacy_origins = (
        "https://api.studio.nyptidindustries.com",
        "https://api.nyptidindustries.com",
    )
    for legacy_origin in legacy_origins:
        assert legacy_origin not in env_example
        assert legacy_origin not in paypal_patch


def test_release_image_embeds_frontend_identity_and_excludes_secret_files() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

    assert 'VITE_STUDIO_BUILD_ID="${FRONTEND_BUILD_ID}" npm run build' in dockerfile
    assert 'grep -R -F -q -- "${FRONTEND_BUILD_ID}" dist' in dockerfile
    assert dockerfile.index('VITE_STUDIO_BUILD_ID="${FRONTEND_BUILD_ID}" npm run build') < dockerfile.index(
        'grep -R -F -q -- "${FRONTEND_BUILD_ID}" dist'
    )

    ignored = {line.strip() for line in dockerignore}
    for pattern in (
        ".env.*",
        "**/.env.*",
        "client_secrets.json",
        "**/client_secrets.json",
        "*.key",
        "*.pem",
        "*.p12",
        "*.pfx",
        "**/.runpod.env",
    ):
        assert pattern in ignored


def test_ci_deploys_the_exact_published_manifest_digest() -> None:
    publish = (
        ROOT / ".github" / "workflows" / "publish-api-image.yml"
    ).read_text(encoding="utf-8")
    deploy_workflow = (
        ROOT / ".github" / "workflows" / "deploy-studio.yml"
    ).read_text(encoding="utf-8")
    deploy_script = _read("deploy.sh")
    lib = _read("lib.sh")

    assert "id: build_push" in publish
    assert "steps.build_push.outputs.digest" in publish
    assert "image_ref: ${{ steps.immutable_image.outputs.image_ref }}" in publish
    assert "image_ref: ${{ needs.build-and-publish.outputs.image_ref }}" in publish
    assert "tests/test_xai_runtime_hard_disable.py" in publish
    assert "tests/test_fly_cutover_evidence.py" in publish
    assert "tests/test_desktop_release_channel.py" in publish
    assert "cargo test --locked --test updater_release" in publish

    assert "docker\\.io/nyptid/nyptid-studio-api@sha256:[0-9a-f]{64}" in deploy_workflow
    assert '--image-ref "${image_ref}"' in deploy_workflow
    assert "docker pull \"${image_ref}\"" in deploy_script
    assert "docker tag \"${image_ref}\" \"${image}\"" in deploy_script
    assert "STUDIO_IMAGE_DIGEST_REF" in deploy_script
    assert "candidate CI image digest reference is invalid" in lib


def test_compose_has_exactly_one_api_and_one_local_redis() -> None:
    compose = _read("docker-compose.yml")
    services_section = compose.split("\nservices:\n", 1)[1].split("\nnetworks:\n", 1)[0]
    services = set(re.findall(r"^  ([a-z][a-z0-9-]+):$", services_section, flags=re.MULTILINE))
    assert services == {"redis", "studio-api"}

    redis = _service_block(compose, "redis", "studio-api")
    api = _service_block(compose, "studio-api", None)
    assert "\n    ports:" not in redis
    assert "appendonly yes" in redis
    assert "appendfsync everysec" in redis
    assert "maxmemory-policy noeviction" in redis
    assert "redis:7.4.2-alpine@sha256:" in redis
    assert "${STUDIO_REDIS_DIR:-/opt/studio/redis}" in redis

    assert "image: ${STUDIO_IMAGE:?" in api
    assert "pull_policy: never" in api
    assert "WEB_CONCURRENCY: \"1\"" in api
    assert "RUN_EMBEDDED_WORKER: \"true\"" in api
    assert "JOB_QUEUE_WORKERS: \"1\"" in api
    assert "REDIS_URL: redis://:" in api
    assert "deploy:\n      replicas: 1" in api
    assert "127.0.0.1:${STUDIO_LOOPBACK_PORT:-10000}:10000" in api
    assert "${STUDIO_DATA_DIR:-/opt/studio/data}" in api
    assert "backend_worker" not in compose


def test_compose_preserves_effective_production_policy() -> None:
    compose = _read("docker-compose.yml")
    required = {
        'STUDIO_ENVIRONMENT: production',
        'STUDIO_DEPLOYMENT_TARGET: contabo',
        'STUDIO_RELEASE_ID: ${EXPECTED_BUILD_ID:?',
        'SITE_URL: https://studio.nyptidindustries.com',
        'API_PUBLIC_URL: https://api-studio.nyptidindustries.com',
        'BILLING_SITE_URL: https://studio.nyptidindustries.com',
        'IMAGE_PROVIDER_ORDER: fal',
        'XAI_API_KEY: ""',
        'USE_XAI_VIDEO: "false"',
        'XAI_PUBLIC_RENDERS_ENABLED: "false"',
        'USE_FAL_GROK_IMAGE: "false"',
        'XAI_IMAGE_FALLBACK_ENABLED: "false"',
        'STUDIO_RUNPOD_PRODUCTION_ENABLED: "false"',
        'STUDIO_RUNPOD_LONGFORM_ENABLED: "false"',
        'RUNPOD_API_KEY: ""',
        'RUNPOD_CLIPLAB_ENDPOINT_ID: ""',
        'CLIPLAB_VIRALITY_BACKEND: local_llm',
        'CLIPLAB_REFRAME_BACKEND: opencv_face',
        'STUDIO_APP_DATA_DIR: /var/data',
        'STUDIO_SHORTS_ONLY: "false"',
        'STUDIO_FINALIZE_QA_REQUIRED: "true"',
        'EXPECTED_GIT_SHA: ${EXPECTED_GIT_SHA:?',
        'EXPECTED_BUILD_ID: ${EXPECTED_BUILD_ID:?',
    }
    for contract in required:
        assert contract in compose

    assert "external: true" in compose
    assert "name: ${CADDY_NETWORK:-deploy_default}" in compose


def test_container_health_requires_provenance_and_single_consumer() -> None:
    source = _read("container_healthcheck.py")
    compile(source, str(OPS / "container_healthcheck.py"), "exec")
    for marker in (
        "EXPECTED_GIT_SHA",
        "EXPECTED_BUILD_ID",
        "backend_commit",
        "frontend_bundle",
        "deployment_target",
        "release_id",
        "queue_mode",
        "queue_consumer_ready",
        "queue_consumer_running",
        'queue.get("workers") != 1',
    ):
        assert marker in source


def test_first_activation_is_fenced_and_stage_never_starts_api() -> None:
    deploy = _read("deploy.sh")
    stage = deploy.split("stage_candidate() {", 1)[1].split(
        "validate_first_cutover_fence() {", 1
    )[0]
    activate = deploy.split("activate_candidate() {", 1)[1]
    assert 'compose_for "${candidate_path}" up -d redis' in stage
    assert "up -d --no-deps studio-api" not in stage
    assert 'validate_first_cutover_fence "${fence}"' in activate
    assert '(( count <= 1 ))' in activate
    assert '(( count == 1 ))' in activate
    assert 'create_verified_api_container "${candidate}"' in activate
    assert 'compose_for "${candidate}" start studio-api' in activate
    assert "data_manifest.py\" check" in deploy
    assert deploy.index("data_manifest.py\" check") < deploy.index(
        'compose_for "${candidate}" start studio-api'
    )
    assert "--scale" not in deploy
    assert "backend_worker" not in deploy
    assert "normal activation cannot downgrade" in deploy
    assert "build_timestamp_from_id" in deploy
    assert "build_timestamp_from_id" in _read("lib.sh")

    readme = _read("README.md")
    final_sync = readme.index("rsync --archive --delete --partial")
    destination_attestation = readme.index("data_manifest.py attest", final_sync)
    refreshed_queue = readme.index(
        "--output /tmp/legacy-queue.json --require-drained",
        destination_attestation,
    )
    refreshed_files = readme.index(
        "--output /tmp/file-quiescence.json --require-drained",
        refreshed_queue,
    )
    stopped_machine = readme.index(
        'fly machine stop "$FLY_MACHINE_ID"',
        refreshed_files,
    )
    assert final_sync < destination_attestation < refreshed_queue < refreshed_files < stopped_machine

    fence = _read("write_cutover_fence.sh")
    assert "fly_cutover_evidence.py" in fence
    assert "LEGACY_EVIDENCE_BUNDLE_SHA256" in fence
    assert "machine-list-before.json" in fence
    assert "machine-list-after.json" in fence
    assert "DATA_READY_ATTESTATION_SHA256" in fence
    assert "DATA_MANIFEST_SHA256" in fence
    assert "DATA_FILE_COUNT" in fence
    assert "DATA_TOTAL_BYTES" in fence
    assert "data_manifest.py\" check" in fence

    fly = (ROOT / "fly.toml").read_text(encoding="utf-8")
    assert "ROLLBACK ONLY" in fly
    assert "auto_start_machines = false" in fly
    assert "min_machines_running = 0" in fly

    lib = _read("lib.sh")
    assert "candidate image ID is invalid" in lib
    assert "create_verified_api_container" in lib
    assert "candidate container ran before immutable image verification" in lib
    assert "STUDIO_IMAGE_ID" in deploy
    assert "running container image ID does not match" in _read("smoke.sh")
    assert "refusing to replace non-symlink lifecycle path" in lib
    assert "activation preflight found a non-symlink lifecycle path" in deploy

    root_readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "The canonical production backend runs on Contabo" in root_readme
    assert "flyctl deploy --remote-only" not in root_readme

    render = (ROOT / "render.yaml").read_text(encoding="utf-8")
    assert "DISABLED LEGACY BLUEPRINT" in render
    assert "autoDeploy: false" in render
    assert 'RUN_EMBEDDED_WORKER\n        value: "false"' in render

    retired_fly_secrets = (
        ROOT / "ops" / "fly_migrate_secrets.sh"
    ).read_text(encoding="utf-8")
    assert "Fly secret migration is retired" in retired_fly_secrets
    assert "flyctl" not in retired_fly_secrets


def test_smoke_and_rollback_fail_closed() -> None:
    smoke = _read("smoke.sh")
    for marker in (
        "backend_commit",
        "frontend_bundle",
        "deployment_target",
        "release_id",
        'payload.get("queue_mode") == "redis"',
        'consumer.get("workers") != 1',
        "xai_image_fallback_enabled",
        "image_provider_order",
        "cliplab_virality_backend",
        "cliplab_reframe_backend",
        "cliplab_runpod_configured",
        "running_api_container_count",
        "--origin-token-file",
        "origin-auth.curl",
    ):
        assert marker in smoke

    rollback = _read("rollback.sh")
    assert "prepare-legacy-after-ingress-is-blocked" in rollback
    assert "reverse-data-sync-is-verified-and-fly-is-still-copy-only" in rollback
    assert "consumer.disabled" in rollback
    assert '(( count == 0 ))' in rollback
    assert 'create_verified_api_container "${previous}"' in rollback
    assert 'create_verified_api_container "${active}"' in rollback
    assert 'compose_for "${previous}" start studio-api' in rollback
    assert "fly machine start" not in rollback
    assert "queue_drain_snapshot" in rollback
    assert "queue-drained.attestation" in rollback
    assert "reverse-manifests" in rollback
    assert "--authorize-legacy" in rollback
    assert 'data_manifest.py" verify' in rollback
    assert "legacy-start.ready" in rollback
    assert "--force" not in rollback


def test_caddy_authenticates_worker_and_exposes_only_large_upload_bypasses() -> None:
    caddy = _read("Caddyfile.studio")
    assert "api-studio.nyptidindustries.com" in caddy
    assert "studio.82.197.67.155.sslip.io" in caddy
    assert "api-studio.nyptidindustries.com, studio." not in caddy
    assert "max_size 30GB" in caddy
    assert "reverse_proxy studio-api:10000" in caddy
    assert "header_up Host api-studio.nyptidindustries.com" in caddy
    assert "flush_interval -1" in caddy
    assert "response_header_timeout 1h" in caddy
    assert "Connection close" not in caddy
    assert "Upgrade" not in caddy  # Caddy handles upgrades automatically.
    assert "@workerCurrent" in caddy
    assert "@workerPrevious" in caddy
    assert "v1.{$STUDIO_ORIGIN_TOKEN}" in caddy
    assert "v1.{$STUDIO_ORIGIN_TOKEN_PREVIOUS}" in caddy
    assert caddy.count("^v1\\.[A-Za-z0-9_-]{43,128}$") == 2
    assert caddy.count("header_up -X-NYPTID-Studio-Origin-Token") == 3
    assert "request>headers>X-Nyptid-Studio-Origin-Token delete" in caddy
    assert "method POST OPTIONS" in caddy
    for path in (
        "/api/cliplab/ingest/upload",
        "/api/catalyst/hub/reference-video-analysis/manual",
        "/api/studio-agent/sessions/*/attachments/video",
        "/api/thumbnails/upload-video",
    ):
        assert f"path {path}" in caddy
    assert '"direct_origin_forbidden"' in caddy
    assert "respond `" in caddy
    assert "` 403" in caddy

    bootstrap = _read("Caddyfile.studio.bootstrap")
    assert "TRANSIENT ROLLOUT CONFIG ONLY" in bootstrap
    assert "header_up -X-NYPTID-Studio-Origin-Token" in bootstrap
    assert "request>headers>X-Nyptid-Studio-Origin-Token delete" in bootstrap
    assert "direct_origin_forbidden" not in bootstrap


def test_caddy_site_replacement_preserves_other_sites_and_json_braces(
    tmp_path: Path,
) -> None:
    live = tmp_path / "Caddyfile"
    replacement = tmp_path / "site.caddy"
    backups = tmp_path / "backups"
    live.write_text(
        "cliplab.example {\n\trespond \"ok\"\n}\n\n"
        "studio.example {\n\trespond `{\"old\":true}` 200\n}\n",
        encoding="utf-8",
    )
    replacement.write_text(
        "# replacement policy\n"
        "studio.example {\n\trespond `{\"new\":true,\"nested\":{\"ok\":true}}` 403\n}\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            sys.executable,
            str(OPS / "replace_caddy_site.py"),
            "--caddyfile",
            str(live),
            "--site-block",
            str(replacement),
            "--hostname",
            "studio.example",
            "--backup-dir",
            str(backups),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    updated = live.read_text(encoding="utf-8")
    assert 'cliplab.example {\n\trespond "ok"\n}' in updated
    assert '{"new":true,"nested":{"ok":true}}' in updated
    assert '{"old":true}' not in updated
    backup_rows = list(backups.glob("Caddyfile.*.bak"))
    assert len(backup_rows) == 1
    assert '{"old":true}' in backup_rows[0].read_text(encoding="utf-8")


def test_origin_token_is_caddy_only_and_never_committed_as_a_value() -> None:
    caddy_env = _read("caddy.env.example")
    assert "STUDIO_ORIGIN_TOKEN=\n" in caddy_env
    assert "STUDIO_ORIGIN_TOKEN_PREVIOUS=\n" in caddy_env

    caddy_override = _read("caddy-compose.override.yml")
    assert (
        "STUDIO_ORIGIN_TOKEN: "
        "${STUDIO_ORIGIN_TOKEN:?STUDIO_ORIGIN_TOKEN is required}"
    ) in caddy_override
    assert "STUDIO_ORIGIN_TOKEN_PREVIOUS: ${STUDIO_ORIGIN_TOKEN_PREVIOUS:-}" in caddy_override

    api_compose = _read("docker-compose.yml")
    api_env = _read("studio.env.example")
    assert "STUDIO_ORIGIN_TOKEN:" not in api_compose
    assert "\nSTUDIO_ORIGIN_TOKEN=" not in api_env

    wrangler = (ROOT / "runpod-serverless" / "wrangler.toml").read_text(
        encoding="utf-8"
    )
    assert "secret put STUDIO_ORIGIN_TOKEN" in wrangler
    assert "\nSTUDIO_ORIGIN_TOKEN =" not in wrangler

    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "ops/contabo/caddy.env" in gitignore
    assert "ops/contabo/caddy.*.env" in gitignore


def test_secret_template_contains_no_secret_values_or_redis_override() -> None:
    env_text = _read("studio.env.example")
    sensitive = {
        "ANTHROPIC_API_KEY",
        "FAL_AI_KEY",
        "FAL_AI_KEY_2",
        "FAL_AI_KEY_3",
        "FAL_AI_KEY_4",
        "FAL_AI_KEY_5",
        "FAL_AI_KEY_6",
        "ELEVENLABS_API_KEY",
        "OPENROUTER_API_KEY",
        "YOUTUBE_API_KEY",
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REDIRECT_URI",
        "SUPABASE_URL",
        "SUPABASE_ANON_KEY",
        "SUPABASE_JWT_SECRET",
        "SUPABASE_SERVICE_KEY",
        "STRIPE_SECRET_KEY",
        "STRIPE_WEBHOOK_SECRET",
        "PAYPAL_CLIENT_ID",
        "PAYPAL_CLIENT_SECRET",
        "PAYPAL_WEBHOOK_ID",
        "ALGROW_API_KEY",
        "PIKZELS_API_KEY",
        "OPUSCLIP_API_KEY",
        "STUDIO_ERROR_WEBHOOK_URL",
        "XAI_API_KEY",
    }
    values: dict[str, str] = {}
    for raw_line in env_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value
    assert sensitive <= values.keys()
    assert all(values[key] == "" for key in sensitive)
    assert "REDIS_URL" not in values
    assert "RUN_EMBEDDED_WORKER" not in values


def test_backup_excludes_secrets_and_has_integrity_and_retention() -> None:
    backup = _read("backup.sh")
    assert "redis-cli SAVE" in backup
    assert "sha256sum" in backup
    assert "BACKUP_RETENTION_COUNT" in backup
    assert "studio.env" not in backup
    assert "base.env" not in backup
    assert 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli SAVE' in backup
    assert 'printf \'%s\' "$REDIS_PASSWORD"' not in backup
    assert "docker cp nyptid-studio-redis:/data/dump.rdb" in backup
    assert '"redis_snapshot":"dump.rdb"' in backup
    assert 'rsync --archive --numeric-ids --delete -- "${redis_dir}/"' not in backup
    assert 'docker cp nyptid-studio-redis:/data/appendonlydir' not in backup


def test_shell_scripts_do_not_enable_trace_or_dump_environment() -> None:
    scripts = (
        "lib.sh",
        "prepare_host.sh",
        "deploy.sh",
        "write_cutover_fence.sh",
        "smoke.sh",
        "rollback.sh",
        "backup.sh",
        "watchdog.sh",
    )
    for name in scripts:
        source = _read(name)
        assert "set -x" not in source
        assert "\nprintenv" not in source
        assert "\nenv\n" not in source


def test_watchdog_respects_lifecycle_lock_and_legacy_disable_fence() -> None:
    watchdog = _read("watchdog.sh")
    assert 'flock -n 9 || exit 0' in watchdog
    assert 'consumer.disabled' in watchdog
    assert '(( count <= 1 ))' in watchdog
    assert 'create_verified_api_container "${active}"' in watchdog
    assert 'compose_for "${active}" start studio-api' in watchdog
    assert "unattested image ID" in watchdog
    assert "--scale" not in watchdog


def test_data_manifest_is_deterministic_and_full_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_path = OPS / "data_manifest.py"
    spec = importlib.util.spec_from_file_location("contabo_data_manifest", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module._assert_root_private_file = lambda path: None
    module._assert_safe_data_dir = lambda path: path.resolve(strict=True)
    monkeypatch.setattr(module.os, "fchmod", lambda _fd, _mode: None, raising=False)

    data = tmp_path / "data"
    (data / "a").mkdir(parents=True)
    (data / "a.txt").write_bytes(b"root")
    (data / "a" / "nested.bin").write_bytes(b"\x00\x01\x02")

    first = tmp_path / "first.manifest"
    with first.open("wb") as handle:
        count, total = module._write_manifest_stream(data, handle)
    os.chmod(first, 0o600)
    first_summary = module.summarize_manifest(first)
    assert count == first_summary.file_count == 2
    assert total == first_summary.total_bytes == 7

    second = tmp_path / "second.manifest"
    with second.open("wb") as handle:
        module._write_manifest_stream(data, handle)
    os.chmod(second, 0o600)
    assert first.read_bytes() == second.read_bytes()

    (data / "a.txt").write_bytes(b"changed")
    with pytest.raises(module.ManifestError, match="does not match"):
        module.verify_tree(data, first)

    changed = tmp_path / "changed.manifest"
    with changed.open("wb") as handle:
        module._write_manifest_stream(data, handle)
    os.chmod(changed, 0o600)
    assert module.summarize_manifest(changed).manifest_sha256 != first_summary.manifest_sha256

    truncated = tmp_path / "truncated.manifest"
    truncated.write_bytes(first.read_bytes()[:-3])
    with pytest.raises(module.ManifestError, match="malformed"):
        module.summarize_manifest(truncated)

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(module.ManifestError, match="data tree is empty"):
        with (tmp_path / "empty.manifest").open("wb") as handle:
            module._write_manifest_stream(empty, handle)


def test_data_manifest_and_fly_copy_only_runbook_fail_closed() -> None:
    tool = _read("data_manifest.py")
    compile(tool, str(OPS / "data_manifest.py"), "exec")
    for marker in (
        "NYPTID_STUDIO_DATA_MANIFEST_V1",
        "data tree is empty",
        "file changed while being hashed",
        "O_NOFOLLOW",
        "data manifest output cannot be a symlink",
        "migrated-data-ready",
        "reverse-destination",
        "MANIFEST_SHA256",
        "FILE_COUNT",
        "TOTAL_BYTES",
    ):
        assert marker in tool

    readme = _read("README.md")
    assert '--command "sleep infinity"' in readme
    assert "--autostart=false" in readme
    assert "--restart=no" in readme
    assert "--skip-health-checks" in readme
    assert "rsync --archive --delete --partial" in readme
    assert (
        'restrict,command="/usr/bin/rrsync -wo /opt/studio/data" ssh-ed25519'
        in readme
    )
    assert "/var/data/ root@82.197.67.155:/\"" in readme
    assert "/var/data/ root@82.197.67.155:/opt/studio/data/" not in readme
    assert "data_manifest.py attest" in readme
    assert 'fly machine stop "$FLY_MACHINE_ID"' in readme
    assert "machine-list-before.json" in _read("capture_fly_cutover_evidence.sh")
    assert "machine-list-after.json" in _read("capture_fly_cutover_evidence.sh")
    assert "legacy-start.ready" in readme
    assert "accepts data loss is\nunsupported" in readme

    copy_only_update = readme.split(
        'fly machine update "$FLY_MACHINE_ID"', 1
    )[1].split("```", 1)[0]
    assert "--skip-start" not in copy_only_update
    update_at = readme.index('fly machine update "$FLY_MACHINE_ID"')
    export_at = readme.index("rsync --archive --delete --partial")
    stop_at = readme.index('fly machine stop "$FLY_MACHINE_ID"', export_at)
    capture_at = readme.index("capture_fly_cutover_evidence.sh", stop_at)
    assert update_at < export_at < stop_at < capture_at

    rollback_heading = readme.index("## Rollback")
    reverse_start_at = readme.index(
        "fly machine start <machine-id> --app nyptid-studio",
        rollback_heading,
    )
    reverse_proof_at = readme.index(
        'pgrep -af \\"[u]vicorn|backend:[a]pp|backend_[w]orker.py\\"',
        reverse_start_at,
    )
    reverse_attest_at = readme.index(
        "python /tmp/data_manifest.py attest",
        reverse_proof_at,
    )
    assert rollback_heading < reverse_start_at < reverse_proof_at < reverse_attest_at


def test_file_quiescence_preserves_shortform_human_review_jobs(
    tmp_path: Path,
) -> None:
    module_path = OPS / "file_quiescence.py"
    spec = importlib.util.spec_from_file_location("contabo_file_quiescence", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    data = tmp_path / "data"
    sessions = data / "studio_agent_sessions"
    result_dir = data / "skeleton_output" / "job_review"
    sessions.mkdir(parents=True)
    result_dir.mkdir(parents=True)
    (sessions / "sa_review.json").write_text(
        '{"session_id":"sa_review","active_jobs":'
        '[{"job_id":"job_review","kind":"shortform"}]}',
        encoding="utf-8",
    )
    (result_dir / "result.json").write_text(
        '{"status":"awaiting_scene_review"}',
        encoding="utf-8",
    )

    snapshot = module.inspect_sessions(sessions, captured_at_epoch=1)

    assert snapshot["drained"] is True
    assert snapshot["total_blockers"] == 0
    assert snapshot["counts"]["active_jobs"] == 0
    assert snapshot["quiescent_job_count"] == 1


def test_file_quiescence_still_blocks_running_or_unknown_shortform_jobs(
    tmp_path: Path,
) -> None:
    module_path = OPS / "file_quiescence.py"
    spec = importlib.util.spec_from_file_location(
        "contabo_file_quiescence_running",
        module_path,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    data = tmp_path / "data"
    sessions = data / "studio_agent_sessions"
    running = data / "skeleton_output" / "job_running"
    sessions.mkdir(parents=True)
    running.mkdir(parents=True)
    (sessions / "sa_running.json").write_text(
        '{"session_id":"sa_running","active_jobs":['
        '{"job_id":"job_running","kind":"shortform"},'
        '{"job_id":"job_unknown","kind":"shortform"}]}',
        encoding="utf-8",
    )
    (running / "result.json").write_text(
        '{"status":"running"}',
        encoding="utf-8",
    )

    snapshot = module.inspect_sessions(sessions, captured_at_epoch=1)

    assert snapshot["drained"] is False
    assert snapshot["total_blockers"] == 2
    assert snapshot["counts"]["active_jobs"] == 2
    assert snapshot["quiescent_job_count"] == 0
