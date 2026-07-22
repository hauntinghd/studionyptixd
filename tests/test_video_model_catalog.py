from __future__ import annotations

from studio_agent.video_model_catalog import video_model_profiles


def _by_id(rows: list[dict]) -> dict[str, dict]:
    return {str(row["id"]): row for row in rows}


def test_video_catalog_uses_provider_price_and_exact_billing_unit() -> None:
    snapshot = {
        "source": "fal_api",
        "fetched_at": 1234.5,
        "prices": {
            "bytedance/seedance-2.0/image-to-video": {"unit_price": 0.014, "unit": "units"},
            "fal-ai/pixverse/v6/image-to-video": {"unit_price": 0.005, "unit": "seconds"},
        },
    }
    models = _by_id(video_model_profiles(pricing_snapshot=snapshot))
    assert models["seedance"]["estimated_unit_usd"] == 0.3024
    assert models["seedance"]["billing_unit"] == "second"
    assert models["seedance"]["pricing_live"] is True
    assert models["pixverse"]["estimated_unit_usd"] == 0.045
    assert models["pixverse"]["billing_unit"] == "second"


def test_video_catalog_has_concrete_safe_prices_without_live_rows() -> None:
    models = _by_id(video_model_profiles(pricing_snapshot={"source": "fallback", "prices": {}}))
    assert all(model.get("estimated_unit_usd") is not None for model in models.values())
    assert models["seedance"]["estimated_unit_usd"] > 0
    assert models["ltx_budget"]["estimated_unit_usd"] > 0


def test_video_catalog_exposes_only_fal_models() -> None:
    models = _by_id(video_model_profiles(pricing_snapshot={"source": "fallback", "prices": {}}))
    assert set(models) == {"seedance", "pixverse", "kling_pro", "ltx_budget"}
    assert all(model["provider"] == "fal" for model in models.values())


def test_video_catalog_is_empty_when_fal_is_unconfigured() -> None:
    rows = video_model_profiles(
        fal_enabled=False,
        pricing_snapshot={"source": "fallback", "prices": {}},
    )
    assert rows == []
