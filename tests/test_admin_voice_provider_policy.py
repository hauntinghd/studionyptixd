from __future__ import annotations

import asyncio

import backend
from skeleton_ai import voice_fal


def test_admin_voice_diagnostics_are_static_fal_catalog(monkeypatch) -> None:
    monkeypatch.setattr(voice_fal, "configured", lambda: True)
    monkeypatch.setattr(
        voice_fal,
        "list_voices",
        lambda: [
            {"voice_id": "English_Trustworth_Man", "name": "Trustworthy Man"},
            {"voice_id": "English_CalmWoman", "name": "Calm Woman"},
        ],
    )

    payload = asyncio.run(backend._fal_voice_provider_snapshot(force_refresh=True))

    assert payload == {
        "source": "fal_minimax",
        "provider_ok": True,
        "count": 2,
        "warning": "",
        "age_sec": 0.0,
    }
