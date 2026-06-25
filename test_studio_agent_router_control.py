import os
import sys
import types
import unittest

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


if __name__ == "__main__":
    unittest.main()
