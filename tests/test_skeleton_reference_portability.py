from __future__ import annotations

import json
import shutil

from skeleton_ai.styled_pipeline import (
    _persist_skeleton_reference,
    _resolve_skeleton_master_reference,
)


def test_staged_workspace_reference_wins_over_inaccessible_api_host_path(tmp_path) -> None:
    original = tmp_path / "api-host-upload.png"
    original.write_bytes(b"portable-skeleton-reference" * 100)

    api_workspace = tmp_path / "api-workspace"
    api_workspace.mkdir()
    (api_workspace / "job_spec.json").write_text(
        json.dumps({"reference_images": [str(original)]}),
        encoding="utf-8",
    )

    runtime_reference = _persist_skeleton_reference(api_workspace, str(original))

    assert runtime_reference == str((api_workspace / "reference.png").resolve())
    assert (api_workspace / "reference.png").read_bytes() == original.read_bytes()
    persisted_spec = json.loads((api_workspace / "job_spec.json").read_text(encoding="utf-8"))
    persisted_meta = json.loads(
        (api_workspace / "skeleton_reference.json").read_text(encoding="utf-8")
    )
    assert persisted_spec["reference_images"] == ["reference.png"]
    assert persisted_spec["skeleton_reference_image"] == "reference.png"
    assert persisted_meta["reference_image_url"] == "reference.png"

    staged_workspace = tmp_path / "runpod-workspace"
    shutil.copytree(api_workspace, staged_workspace)
    original.unlink()

    staged_runtime_reference = _persist_skeleton_reference(staged_workspace, str(original))
    resolved = _resolve_skeleton_master_reference(
        staged_workspace,
        [str(original)],
    )

    assert staged_runtime_reference == str((staged_workspace / "reference.png").resolve())
    assert resolved == str((staged_workspace / "reference.png").resolve())
    assert staged_workspace in __import__("pathlib").Path(resolved).parents
