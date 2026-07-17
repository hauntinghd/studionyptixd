from __future__ import annotations

from decimal import Decimal

from long_form import fal_pricing
from skeleton_ai import i2v_engine
from studio_agent import production_costs


def test_fal_base_prices_become_effective_studio_render_rates() -> None:
    snapshot = {
        "source": "fal_api",
        "prices": {
            fal_pricing.ENDPOINTS["seedance_20_i2v"]: {"unit_price": 0.014, "unit": "units"},
            fal_pricing.ENDPOINTS["pixverse_v6"]: {"unit_price": 0.005, "unit": "seconds"},
        },
    }
    seedance, _ = fal_pricing.unit_cost(
        snapshot, "seedance_20_i2v", fallback_key="seedance_20_i2v_per_second", quantity=5
    )
    pixverse, _ = fal_pricing.unit_cost(
        snapshot, "pixverse_v6", fallback_key="pixverse_v6_per_second", quantity=5
    )
    assert seedance == 1.512
    assert pixverse == 0.225


def test_i2v_requests_pin_the_shape_used_by_pricing() -> None:
    seedance = i2v_engine._build_args(
        i2v_engine.SEEDANCE_ENDPOINT, "motion", "https://example.test/still.png", 5, "9:16"
    )
    pixverse = i2v_engine._build_args(
        i2v_engine.PIXVERSE_V6_ENDPOINT, "motion", "https://example.test/still.png", 5, "9:16"
    )
    assert seedance["resolution"] == "720p"
    assert seedance["generate_audio"] is False
    assert pixverse["resolution"] == "720p"
    assert pixverse["generate_audio_switch"] is False


def test_xai_i2v_costs_include_current_720p_rates_and_input_image_fee() -> None:
    legacy, _, _ = production_costs.price_xai_video("grok_imagine_video", seconds=5, resolution="720p")
    v15, _, _ = production_costs.price_xai_video("grok_imagine_video_15", seconds=5, resolution="720p")
    v15_1080, _, _ = production_costs.price_xai_video(
        "grok_imagine_video_15_1080p", seconds=5, resolution="1080p"
    )
    assert legacy == Decimal("0.352000")
    assert v15 == Decimal("0.710000")
    assert v15_1080 == Decimal("1.260000")
