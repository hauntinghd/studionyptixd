"""Production QA reliability: audit_scene_correspondence must reject only real
garbage (visible artifacts / wrong content / empty void). Subjective staging
(adjacent similarity, narrative-beat expression, generic pose) is advisory and
must NOT fail a scene — that was what blocked every remake and burned money,
while the artifact bar ("no artifacting") is what actually matters.
"""
from __future__ import annotations

import studio_agent.visual_qa as vq


def _audit(tmp_path, monkeypatch, *, parsed=None, error=None):
    still = tmp_path / "still.png"
    still.write_bytes(b"not-a-real-image")
    # Skip real image prep + the vision provider; exercise only the decision logic.
    monkeypatch.setattr(vq, "_qa_jpeg", lambda src, dst: str(dst))
    monkeypatch.setattr(
        vq, "_run_semantic_vision",
        lambda candidates, prompt: ({"parsed": parsed} if parsed is not None else {"error": error}),
    )
    return vq.audit_scene_correspondence(still, scene_contract="a skeleton in a lab")


def test_artifact_free_but_staging_imperfect_passes(tmp_path, monkeypatch):
    r = _audit(tmp_path, monkeypatch, parsed={
        "pass": True, "confidence": 0.9,
        "wrong_or_artifact": False, "duplicate_adjacent": True,
        "narrative_mismatch": True, "generic_staging": True,
    })
    assert r["pass"] is True
    # Staging concerns recorded as advisory, never as a hard failure.
    assert set(r["advisory"]) >= {"duplicate_adjacent", "narrative_mismatch", "generic_staging"}
    assert "wrong_or_artifact" not in r["issues"]


def test_real_artifact_blocks(tmp_path, monkeypatch):
    r = _audit(tmp_path, monkeypatch, parsed={
        "pass": False, "confidence": 0.9, "wrong_or_artifact": True,
    })
    assert r["pass"] is False
    assert "wrong_or_artifact" in r["issues"]


def test_low_confidence_artifact_does_not_block(tmp_path, monkeypatch):
    # "When unsure, PASS" — an uncertain artifact call must not fail production.
    r = _audit(tmp_path, monkeypatch, parsed={
        "pass": False, "confidence": 0.3, "wrong_or_artifact": True,
    })
    assert r["pass"] is True


def test_qa_outage_soft_passes(tmp_path, monkeypatch):
    # A transient judge outage must not fail the whole production.
    r = _audit(tmp_path, monkeypatch, error="vision provider unavailable")
    assert r["pass"] is True
    assert "qa_unavailable" in r["advisory"]
