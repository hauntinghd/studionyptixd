import json
import os
import sys
import types
import unittest
from unittest.mock import patch

os.environ["REDIS_QUEUE_ENABLED"] = "0"
os.environ["REDIS_URL"] = ""
sys.modules.setdefault("stripe", types.SimpleNamespace())

from fastapi import FastAPI
from fastapi.testclient import TestClient

from studio_agent_router import build_studio_agent_router


class StudioAgentRouterControlTests(unittest.TestCase):
    def _client(self) -> TestClient:
        app = FastAPI()

        async def require_auth():
            return {"id": "owner", "email": "owner@example.com", "is_admin": True}

        app.include_router(
            build_studio_agent_router(
                require_auth=require_auth,
                is_admin_check=lambda user: True,
            )
        )
        return TestClient(app)

    def test_production_control_contract_is_exposed(self):
        response = self._client().get("/api/studio-agent/production-control")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("queue", data)
        self.assertIn("stage_gates", data)
        self.assertIn("approval_required_tools", data)
        self.assertIn("default_caps_usd", data)
        self.assertEqual(data["lanes"]["chat"], 100)
        self.assertIn("animate_production_scenes", data["approval_required_tools"])
        self.assertIn("await_scene_review", data["stage_gates"]["start_shortform_generate"])
        self.assertIn("scene_approval_required", data["stage_gates"]["animate_production_scenes"])
        self.assertTrue(data["queue"]["enabled"])

    def test_shortform_animate_endpoint_runs_i2v_and_returns_snapshot(self):
        job_id = "shortform123"
        with (
            patch(
                "studio_agent.tools.animate_production_scenes",
                return_value=json.dumps({"ok": True, "job_id": job_id, "animated": [0, 1], "failed": []}),
            ) as animate,
            patch(
                "studio_agent.jobs.get_job_snapshot",
                return_value={
                    "job_id": job_id,
                    "kind": "shortform",
                    "status": "awaiting_approval",
                    "animation_pending_count": 0,
                    "animation_complete_count": 2,
                },
            ),
        ):
            response = self._client().post(f"/api/studio-agent/jobs/{job_id}/animate")

        self.assertEqual(response.status_code, 200)
        animate.assert_called_once_with(job_id)
        data = response.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["snapshot"]["animation_complete_count"], 2)

    def test_shortform_finalize_blocks_when_requested_animation_is_missing(self):
        job_id = "shortform123"
        with patch(
            "studio_agent.tools.finalize_production",
            return_value=json.dumps({
                "status": "awaiting_animation",
                "job_id": job_id,
                "pending_animated_scenes": [0, 1],
            }),
        ):
            response = self._client().post(f"/api/studio-agent/jobs/{job_id}/finalize?kind=shortform")

        self.assertEqual(response.status_code, 409)
        data = response.json()
        self.assertEqual(data["detail"]["status"], "awaiting_animation")
        self.assertEqual(data["detail"]["pending_animated_scenes"], [0, 1])


if __name__ == "__main__":
    unittest.main()
