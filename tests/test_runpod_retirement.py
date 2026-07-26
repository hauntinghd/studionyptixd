from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from studio_agent import runpod_bridge, runpod_contract, runpod_worker


ROOT = Path(__file__).resolve().parents[1]

RUNPOD_OPERATOR_ENTRYPOINTS = (
    "check_runpod.sh",
    "runpod_setup.sh",
    "runpod_full_setup.sh",
    "ops/runpod_bump_image.pl",
    "ops/runpod_cycle_workers.pl",
    "runpod-serverless/create_runpod_volume.py",
    "runpod-serverless/deploy.sh",
    "runpod-serverless/diag_studio_api.py",
    "runpod-serverless/handler.py",
    "runpod-serverless/handler_minimal.py",
    "runpod-serverless/patch_endpoint_scale.sh",
    "runpod-serverless/save_runpod_template.pl",
    "runpod-serverless/upsert_cliplab_endpoint.py",
    "runpod-serverless/upsert_runpod_endpoint.py",
    "cliplab/runpod/_check_ep.py",
    "cliplab/runpod/_check_studio_env.py",
    "cliplab/runpod/_debug_ep.py",
    "cliplab/runpod/_deploy_novol.py",
    "cliplab/runpod/_final_deploy.py",
    "cliplab/runpod/_finish_deploy.py",
    "cliplab/runpod/_fix_template.py",
    "cliplab/runpod/_job_status.py",
    "cliplab/runpod/_new_ep_v3.py",
    "cliplab/runpod/_patch_studio.py",
    "cliplab/runpod/_rebootstrap.py",
    "cliplab/runpod/_repush.py",
    "cliplab/runpod/_test_endpoint.py",
    "cliplab/runpod/_test_minimal.py",
    "cliplab/runpod/_volume_sync.py",
    "cliplab/runpod/activate_registry.py",
    "cliplab/runpod/build_and_push.sh",
    "cliplab/runpod/deploy_cliplab_runpod.py",
    "cliplab/runpod/fix_cliplab_endpoint.py",
    "cliplab/runpod/push_weights_to_endpoint.py",
)

CLIPLAB_OFFLINE_RESEARCH_SOURCES = frozenset(
    {
        "bootstrap_feedback.py",
        "bootstrap_feedback_mass.py",
        "bootstrap_opencv_reframe.py",
        "bootstrap_reframe_mass.py",
        "inference_handler.py",
        "models_torch.py",
        "run_training.sh",
        "setup_volume.sh",
        "train_face_reframe.py",
        "train_production.sh",
        "train_production_pass.sh",
        "train_virality_scorer.py",
    }
)


@pytest.mark.parametrize(
    "value",
    ("1", "true", "yes", "on", "enabled", "TRUE", " stale "),
)
def test_stale_runpod_flags_and_keys_cannot_enable_execution(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", value)
    monkeypatch.setenv("STUDIO_RUNPOD_LONGFORM_ENABLED", value)
    monkeypatch.setenv("RUNPOD_API_KEY", "stale-key")
    monkeypatch.setenv("RUNPOD_ENDPOINT_ID", "stale-endpoint")
    monkeypatch.setenv("RUNPOD_DISPATCH_SECRET", "x" * 64)
    monkeypatch.setenv("RUNPOD_CLIPLAB_ENDPOINT_ID", "stale-cliplab")
    monkeypatch.setenv("CLIPLAB_VIRALITY_BACKEND", "runpod_custom_v1")
    monkeypatch.setenv("CLIPLAB_REFRAME_BACKEND", "runpod_face_v1")

    assert runpod_contract.runpod_production_enabled() is False
    assert runpod_contract.runpod_longform_enabled() is False
    assert runpod_bridge.runpod_configured() is False

    import cliplab.config as cliplab_config

    reloaded = importlib.reload(cliplab_config)
    assert reloaded.VIRALITY_BACKEND == "local_llm"
    assert reloaded.REFRAME_BACKEND == "opencv_face"
    assert reloaded.RUNPOD_CLIPLAB_ENDPOINT == ""
    assert reloaded.RUNPOD_CLIPLAB_URL == ""


def test_bridge_fails_before_credentials_or_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("retired RunPod path consulted credentials or network")

    monkeypatch.setattr(runpod_bridge, "runpod_api_key", forbidden)
    monkeypatch.setattr(runpod_bridge, "runpod_endpoint_id", forbidden)
    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", forbidden)

    for call in (
        lambda: runpod_bridge.preflight_runpod_endpoint(),
        lambda: runpod_bridge.get_runpod_job_status("legacy-job"),
        lambda: runpod_bridge.dispatch_production_tool(
            "start_shortform_generate",
            {},
            user_id="user",
        ),
    ):
        with pytest.raises(runpod_bridge.RunPodConfigurationError, match="permanently retired"):
            call()


def test_retired_worker_rejects_before_environment_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runpod_worker,
        "prepare_worker_environment",
        lambda: (_ for _ in ()).throw(AssertionError("worker touched its environment")),
    )
    result = runpod_worker.handler({"input": {"untrusted": True}})
    assert result["ok"] is False
    assert result["error"] == "runpod_retired"


def test_effective_public_health_is_runpod_free_with_stale_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "STUDIO_RUNPOD_PRODUCTION_ENABLED",
        "STUDIO_RUNPOD_LONGFORM_ENABLED",
        "RUNPOD_API_KEY",
        "RUNPOD_ENDPOINT_ID",
        "RUNPOD_DISPATCH_SECRET",
        "RUNPOD_CLIPLAB_ENDPOINT_ID",
    ):
        monkeypatch.setenv(name, "stale-secret-or-flag")
    monkeypatch.setenv("CLIPLAB_VIRALITY_BACKEND", "runpod_custom_v1")
    monkeypatch.setenv("CLIPLAB_REFRAME_BACKEND", "runpod_face_v1")

    import asyncio
    import backend

    payload = asyncio.run(backend._base_health_payload())
    assert payload["runpod_production_enabled"] is False
    assert payload["runpod_longform_enabled"] is False
    assert payload["runpod_control_configured"] is False
    assert payload["runpod_storage_configured"] is False
    assert payload["runpod_configured"] is False
    assert payload["cliplab_virality_backend"] == "local_llm"
    assert payload["cliplab_reframe_backend"] == "opencv_face"
    assert payload["cliplab_runpod_configured"] is False
    assert "stale-secret-or-flag" not in repr(payload)


def test_runpod_operator_inventory_is_fail_fast_and_network_free() -> None:
    forbidden = (
        "api.runpod.",
        "RUNPOD_API_KEY",
        "RUNPOD_ENDPOINT_ID",
        "RUNPOD_DISPATCH_SECRET",
        "httpx",
        "requests",
        "urllib",
        "curl ",
        "docker push",
        "saveTemplate",
        "saveEndpoint",
        "subprocess",
        "os.environ",
        "os.getenv",
    )
    for relative in RUNPOD_OPERATOR_ENTRYPOINTS:
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "RETIRED:" in source, relative
        for marker in forbidden:
            assert marker not in source, f"{relative} contains active marker {marker!r}"

    workflow = ROOT / ".github" / "workflows" / "build-studio-serverless.yml"
    assert not workflow.exists()

    discovered = {
        path.relative_to(ROOT).as_posix()
        for path in ROOT.glob("*runpod*.sh")
        if path.is_file()
    }
    discovered.add("check_runpod.sh")
    discovered.update(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "ops").glob("runpod*")
        if path.is_file()
    )
    discovered.update(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "runpod-serverless").iterdir()
        if path.is_file()
        and path.suffix in {".py", ".sh", ".pl"}
        and path.name != "patch_paypal_webhook.py"
    )
    discovered.update(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "cliplab" / "runpod").iterdir()
        if path.is_file()
        and path.suffix in {".py", ".sh"}
        and path.name not in CLIPLAB_OFFLINE_RESEARCH_SOURCES
    )
    assert discovered == set(RUNPOD_OPERATOR_ENTRYPOINTS)


def test_offline_training_sources_do_not_activate_runpod_runtime() -> None:
    sources = "\n".join(
        (ROOT / relative).read_text(encoding="utf-8")
        for relative in (
            "cliplab/runpod/run_training.sh",
            "cliplab/runpod/train_production.sh",
            "cliplab/runpod/train_production_pass.sh",
        )
    )
    assert "activate_registry.py" not in sources
    assert "CLIPLAB_VIRALITY_BACKEND=runpod" not in sources
    assert "CLIPLAB_REFRAME_BACKEND=runpod" not in sources
    assert "['active'] = 'runpod" not in sources
