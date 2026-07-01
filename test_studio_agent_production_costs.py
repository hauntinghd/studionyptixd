from studio_agent import production_costs


def test_cost_ledger_summarizes_and_attaches_to_progress(tmp_path):
    workspace = tmp_path / "job"
    workspace.mkdir()

    production_costs.record_event(
        workspace,
        stage="stills",
        provider="fal",
        operation="seedream_v45_edit",
        usd="0.040000",
        quantity=1,
        unit="image",
        scene_index=0,
    )
    production_costs.record_event(
        workspace,
        stage="animation",
        provider="fal",
        operation="pixverse_v6",
        usd="0.225000",
        quantity=5,
        unit="second",
        endpoint="fal-ai/pixverse/v6/image-to-video",
        request_id="req_123",
        scene_index=0,
    )

    summary = production_costs.load_summary(workspace)
    assert summary["total_usd_decimal"] == "0.265000"
    assert summary["by_stage_decimal"]["stills"] == "0.040000"
    assert summary["by_stage_decimal"]["animation"] == "0.225000"
    assert summary["by_provider_decimal"]["fal"] == "0.265000"
    assert summary["event_count"] == 2

    payload = production_costs.attach_to_progress(workspace, {"stage": "animate"})
    assert payload["cost"]["actual_usd_decimal"] == "0.265000"
    assert payload["cost"]["event_count"] == 2
