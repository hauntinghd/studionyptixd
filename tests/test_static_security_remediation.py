from __future__ import annotations

import ast
from pathlib import Path

import pytest

import setup_supabase


ROOT = Path(__file__).resolve().parents[1]
HASH_FILES = (
    "backend_catalyst_learning.py",
    "studio_alerts.py",
    "train_thumbnail_lora.py",
    "youtube.py",
    "youtube_cache.py",
)


def _tree(filename: str) -> ast.AST:
    return ast.parse((ROOT / filename).read_text(encoding="utf-8"))


def test_nonsecurity_hash_sites_use_sha256_only() -> None:
    for filename in HASH_FILES:
        calls = [
            node.func.attr
            for node in ast.walk(_tree(filename))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        ]
        assert "md5" not in calls, filename
        assert "sha1" not in calls, filename
        assert "sha256" in calls, filename


def test_setup_requires_named_environment_without_secret_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in setup_supabase.REQUIRED_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError) as exc:
        setup_supabase._required_configuration()

    message = str(exc.value)
    for name in setup_supabase.REQUIRED_ENV_VARS:
        assert name in message


def test_setup_has_no_shell_install_or_literal_passwords() -> None:
    tree = _tree("setup_supabase.py")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert not (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "os"
                and node.func.attr == "system"
            )
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and key.value == "password":
                    assert not isinstance(value, ast.Constant)
