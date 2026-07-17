from __future__ import annotations

from decimal import Decimal

from skeleton_ai import canonical_edit, styled_stills
from studio_agent import production_costs, store
from studio_agent.image_model_catalog import (
    modal_seedream_request_headers,
    seedream_endpoint,
    seedream_model_profiles,
)


def test_real_seedream_endpoints_and_session_ids(monkeypatch):
    monkeypatch.delenv("MODAL_SEEDREAM_ENDPOINT_URL", raising=False)
    profiles = {row["id"]: row for row in seedream_model_profiles(fal_enabled=True)}
    assert set(profiles) == {"seedream_edit", "seedream_v4", "seedream_v5_lite"}
    assert seedream_endpoint("seedream_v4", edit=False) == "fal-ai/bytedance/seedream/v4/text-to-image"
    assert seedream_endpoint("seedream_v4", edit=True) == "fal-ai/bytedance/seedream/v4/edit"
    assert seedream_endpoint("seedream_v5_lite", edit=False) == "bytedance/seedream/v5/lite/text-to-image"
    assert seedream_endpoint("seedream_v5_lite", edit=True) == "bytedance/seedream/v5/lite/edit"
    assert store.normalize_image_model("seedream4") == "seedream_v4"
    assert store.normalize_image_model("seedream5_lite") == "seedream_v5_lite"


def test_modal_profile_is_conditional_and_uses_official_proxy_headers(monkeypatch):
    monkeypatch.setenv("MODAL_SEEDREAM_ENDPOINT_URL", "http://not-a-modal-endpoint.test")
    assert all(row["provider"] != "modal" for row in seedream_model_profiles())

    monkeypatch.setenv("MODAL_SEEDREAM_ENDPOINT_URL", "https://operator--seedream.modal.run")
    monkeypatch.setenv("MODAL_PROXY_TOKEN_ID", "token-id")
    monkeypatch.setenv("MODAL_PROXY_TOKEN_SECRET", "token-secret")
    monkeypatch.setenv("MODAL_SEEDREAM_AUTH_TOKEN", "custom-bearer")
    modal = [row for row in seedream_model_profiles() if row["provider"] == "modal"]
    assert [row["id"] for row in modal] == ["seedream_v5_lite_modal"]
    assert modal[0]["estimated_unit_usd"] is None
    headers = modal_seedream_request_headers()
    assert headers["Modal-Key"] == "token-id"
    assert headers["Modal-Secret"] == "token-secret"
    assert headers["Authorization"] == "Bearer custom-bearer"


def test_canonical_edit_routes_v4_and_v5_with_supported_payloads(monkeypatch, tmp_path):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(canonical_edit.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(
        canonical_edit,
        "resolve_master_reference_urls",
        lambda **kwargs: ["https://example.test/master.png"],
    )
    monkeypatch.setattr(canonical_edit, "_ensure_fal", lambda: None)
    monkeypatch.setattr(
        canonical_edit,
        "_queue_result",
        lambda endpoint, payload, timeout_sec: (
            calls.append((endpoint, payload)) or {"images": [{"url": "https://example.test/out.png"}]}
        ),
    )
    monkeypatch.setattr(canonical_edit, "_download", lambda _url, dest: dest.write_bytes(b"x" * 2048))

    canonical_edit.generate_still_edit("repair", tmp_path / "v4.png", image_model_id="seedream_v4")
    canonical_edit.generate_still_edit("repair", tmp_path / "v5.png", image_model_id="seedream_v5_lite")

    assert calls[0][0] == "fal-ai/bytedance/seedream/v4/edit"
    assert "negative_prompt" not in calls[0][1]
    assert calls[1][0] == "bytedance/seedream/v5/lite/edit"
    assert "negative_prompt" not in calls[1][1]
    assert "seed" not in calls[1][1]


def test_text_to_image_routes_selected_seedream_model(monkeypatch, tmp_path):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(styled_stills.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(styled_stills, "_ensure_fal", lambda: None)
    monkeypatch.setattr(
        styled_stills,
        "_queue_result",
        lambda endpoint, payload, timeout_sec: (
            calls.append((endpoint, payload)) or {"images": [{"url": "https://example.test/out.png"}]}
        ),
    )
    monkeypatch.setattr(styled_stills, "_download", lambda _url, dest: dest.write_bytes(b"x" * 2048))

    styled_stills.generate_still_t2i(
        "portrait",
        tmp_path / "v5.png",
        negative_prompt="bad",
        image_model_id="seedream_v5_lite",
    )
    assert calls[0][0] == "bytedance/seedream/v5/lite/text-to-image"
    assert "negative_prompt" not in calls[0][1]


def test_seedream_pricing_selects_each_real_fal_key(monkeypatch):
    calls: list[tuple[str, str, float]] = []

    def fake_unit(key: str, *, fallback_key: str, quantity: float = 1.0):
        calls.append((key, fallback_key, quantity))
        return Decimal("0.035000"), "test"

    monkeypatch.setattr(production_costs, "fal_unit_cost", fake_unit)
    production_costs.price_fal_image(edit=True, model_id="seedream_v4")
    production_costs.price_fal_image(edit=False, model_id="seedream_v5_lite")
    assert calls == [
        ("seedream_v4_edit", "seedream_v4_edit_per_image", 1),
        ("seedream_v5_lite", "seedream_v5_lite_per_image", 1),
    ]
