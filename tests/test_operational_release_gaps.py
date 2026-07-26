from __future__ import annotations

from pathlib import Path

import studio_alerts
import studio_release_notes


ROOT = Path(__file__).resolve().parents[1]
CONTABO = ROOT / "ops" / "contabo"


def test_release_alert_wrapper_preserves_bounded_release_identity(monkeypatch) -> None:
    captured: list[tuple[tuple, dict]] = []

    def capture(*args, **kwargs) -> None:
        captured.append((args, kwargs))

    monkeypatch.setattr(studio_alerts, "send_alert", capture)
    studio_alerts.send_release(
        "Studio release",
        "Backend-owned production command contract.",
        version="1.0.2",
        release_id="release-" + ("a" * 200),
    )

    assert len(captured) == 1
    args, kwargs = captured[0]
    assert args == (
        "success",
        "Studio release",
        "Backend-owned production command contract.",
    )
    assert kwargs["context"]["version"] == "1.0.2"
    assert kwargs["context"]["release_id"] == ("release-" + ("a" * 200))[:160]


def test_publish_release_note_can_announce_without_startup_attribute_error(monkeypatch) -> None:
    persisted: list[dict] = []
    announced: set[str] = set()
    sent: list[dict] = []

    monkeypatch.setattr(studio_release_notes, "_load_persisted", lambda: list(persisted))
    monkeypatch.setattr(
        studio_release_notes,
        "_save_persisted",
        lambda rows: persisted.__setitem__(slice(None), list(rows)),
    )
    monkeypatch.setattr(studio_release_notes, "_load_announced", lambda: set(announced))
    monkeypatch.setattr(
        studio_release_notes,
        "_save_announced",
        lambda rows: announced.update(rows),
    )
    monkeypatch.setattr(
        studio_release_notes.studio_alerts,
        "send_release",
        lambda title, body, **kwargs: sent.append(
            {"title": title, "body": body, **kwargs}
        ),
    )

    row = studio_release_notes.publish_release_note(
        release_id="release-test",
        title="Release test",
        body="Safe startup announcement",
        version="1.0.2",
    )

    assert row["id"] == "release-test"
    assert announced == {"release-test"}
    assert sent == [
        {
            "title": "Release test",
            "body": "Safe startup announcement",
            "version": "1.0.2",
            "release_id": "release-test",
        }
    ]


def test_github_keeps_verification_and_image_publish_when_contabo_is_not_ready() -> None:
    workflow = (
        ROOT / ".github" / "workflows" / "publish-api-image.yml"
    ).read_text(encoding="utf-8")

    assert "verify-candidate:" in workflow
    assert "build-and-publish:" in workflow
    assert "contabo-deploy-readiness:" in workflow
    assert "CONTABO_AUTO_DEPLOY_ENABLED: ${{ vars.CONTABO_AUTO_DEPLOY_ENABLED }}" in workflow
    assert "ready=false" in workflow
    assert "CONTABO_SSH_USER must be a dedicated non-root release principal" in workflow
    assert "needs.contabo-deploy-readiness.outputs.ready == 'true'" in workflow
    assert workflow.index("build-and-publish:") < workflow.index(
        "contabo-deploy-readiness:"
    ) < workflow.index("deploy-production:")


def test_restic_configuration_contains_no_credentials_and_requires_https() -> None:
    example = (CONTABO / "restic-s3.env.example").read_text(encoding="utf-8")
    script = (CONTABO / "restic_offsite.sh").read_text(encoding="utf-8")
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    values: dict[str, str] = {}
    for raw_line in example.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value

    assert values["RESTIC_REPOSITORY"] == ""
    assert values["AWS_ACCESS_KEY_ID"] == ""
    assert values["AWS_SECRET_ACCESS_KEY"] == ""
    assert values["RESTIC_PASSWORD_FILE"] == "/opt/studio/shared/restic-password"
    assert "assert_private_file \"${config}\"" in script
    assert "assert_private_file \"${password_file}\"" in script
    assert "^s3:https://" in script
    assert "RESTIC_PASSWORD=" not in example
    assert 'printf \'%s\' "${secret_key}"' not in script
    assert "set -x" not in script
    for ignored in ("ops/contabo/restic-s3.env", "ops/contabo/restic-password"):
        assert ignored in gitignore
        assert ignored in dockerignore


def test_contabo_and_retired_runpod_contracts_are_stripe_only() -> None:
    env_example = (CONTABO / "studio.env.example").read_text(encoding="utf-8")
    compose = (CONTABO / "docker-compose.yml").read_text(encoding="utf-8")
    ops_lib = (CONTABO / "lib.sh").read_text(encoding="utf-8")
    runpod_example = (
        ROOT / "runpod-serverless" / ".runpod.env.example"
    ).read_text(encoding="utf-8")
    handoff = (
        ROOT / "runpod-serverless" / "HANDOFF_TO_CLAUDE.md"
    ).read_text(encoding="utf-8")

    assert "STRIPE_SECRET_KEY" in env_example
    assert "STRIPE_WEBHOOK_SECRET" in env_example
    assert "STRIPE_SECRET_KEY" in ops_lib
    assert "STRIPE_WEBHOOK_SECRET" in ops_lib
    assert "YOUTUBE_TOKEN_ENCRYPTION_KEY" in env_example
    assert "YOUTUBE_TOKEN_ENCRYPTION_KEY" in ops_lib
    assert "legacy PAYPAL_* entries must be removed" in ops_lib
    for source in (env_example, compose, runpod_example, handoff):
        assert "PAYPAL" not in source.upper()
    assert not (ROOT / "runpod-serverless" / "patch_paypal_webhook.py").exists()


def test_ci_and_local_deploy_share_one_mandatory_release_test_manifest() -> None:
    manifest_path = ROOT / "ops" / "release_backend_tests.txt"
    manifest = {
        line.strip()
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    workflow = (
        ROOT / ".github" / "workflows" / "publish-api-image.yml"
    ).read_text(encoding="utf-8")
    local_deploy = (ROOT / "ops" / "deploy_studio_agent.ps1").read_text(
        encoding="utf-8"
    )

    assert "tests" in manifest
    assert "test_unified_credits.py" in manifest
    assert "test_studio_agent_render_qa.py" in manifest
    assert "test_sdk.py" not in manifest
    assert "test_watcher.py" not in manifest
    assert "ops/release_backend_tests.txt" in workflow
    assert "ops\\release_backend_tests.txt" in local_deploy
    assert "Where-Object { Test-Path $_ }" not in local_deploy
    assert "& $uv run --python 3.11 python -m pytest" in local_deploy


def test_backup_publishes_local_archive_before_optional_encrypted_offsite_copy() -> None:
    backup = (CONTABO / "backup.sh").read_text(encoding="utf-8")
    service = (CONTABO / "studio-backup.service").read_text(encoding="utf-8")

    restore_index = backup.index("\nrestore_api\n")
    local_publish_index = backup.index('mv -- "${archive_partial}" "${archive}"')
    offsite_index = backup.index('bash "${release_dir}/ops/contabo/restic_offsite.sh"')
    retention_index = backup.index("mapfile -t expired")

    assert restore_index < local_publish_index < offsite_index < retention_index
    assert "No restic-s3.env is configured; this run remains local recovery only" in backup
    assert "Local recovery archive is intact, but encrypted off-host backup failed" in backup
    assert "local recovery succeeded but configured encrypted off-host backup failed" in backup
    assert backup.count(
        'rsync --archive --numeric-ids --delete -- "${data_dir}/" "${snapshot_dir}/data/"'
    ) == 2
    assert "Pre-copying /var/data while Studio remains online" in backup
    assert "Restart=on-failure" in service
    assert "RestartSec=15m" in service


def test_offsite_restore_check_is_isolated_and_hash_verifies_redis() -> None:
    script = (CONTABO / "restic_offsite.sh").read_text(encoding="utf-8")
    service = (
        CONTABO / "studio-offsite-restore-check.service"
    ).read_text(encoding="utf-8")
    timer = (
        CONTABO / "studio-offsite-restore-check.timer"
    ).read_text(encoding="utf-8")
    prepare = (CONTABO / "prepare_host.sh").read_text(encoding="utf-8")

    assert '.restic-restore-check.XXXXXX' in script
    assert 'restic restore latest' in script
    assert 'sha256sum "${restore_root}/redis/dump.rdb"' in script
    assert 'rm -rf -- "${restore_root}"' in script
    assert "ConditionPathExists=/opt/studio/shared/restic-s3.env" in service
    assert "restic_offsite.sh restore-check" in service
    assert "OnCalendar=*-*-01" in timer
    assert "studio-offsite-restore-check.timer" in prepare
