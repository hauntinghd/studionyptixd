import studio_agent.production_slots as slots


def test_local_production_slot_snapshot_and_release(monkeypatch):
    monkeypatch.setattr(slots, "_redis_ping", lambda: False)
    monkeypatch.setenv("STUDIO_PRODUCTION_I2V_SLOTS", "1")
    slots._local_active.clear()
    slots._local_waiting.clear()

    admission = slots.acquire_slot("i2v")
    try:
        assert admission.lane == "i2v"
        assert admission.active == 1
        snap = slots.slot_snapshot()
        assert snap["mode"] == "local"
        assert snap["lanes"]["i2v"]["active"] == 1
        assert snap["lanes"]["i2v"]["limit"] == 1
    finally:
        slots.release_slot(admission)

    assert slots.slot_snapshot()["lanes"]["i2v"]["active"] == 0
