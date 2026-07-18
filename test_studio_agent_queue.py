import os
import sys
import types
import unittest


os.environ["REDIS_QUEUE_ENABLED"] = "0"
os.environ["REDIS_URL"] = ""
os.environ.setdefault("STUDIO_AGENT_QUEUE_ENABLED", "1")
try:
    import stripe  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent import queue as studio_queue


class StudioAgentQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await studio_queue.reset_queue_counters()

    async def asyncTearDown(self):
        await studio_queue.reset_queue_counters()

    async def test_queue_slot_releases_after_context(self):
        before = await studio_queue.queue_snapshot()
        self.assertEqual(before["active_sessions"], 0)
        self.assertIn("redis_available", before)
        self.assertIn("lease_count", before)
        self.assertIn("oldest_lease_expires_in_sec", before)

        async with studio_queue.studio_agent_slot(
            user_id="smoke-user",
            plan="creator",
            operation="continue_production",
            unlimited=False,
        ) as admission:
            data = admission.as_dict()
            self.assertGreaterEqual(data["active_sessions"], 1)
            self.assertIn(data["mode"], {"local", "redis"})
            self.assertEqual(data["queue_position"], 0)
            self.assertIn("reclaimed_stale", data)

        after = await studio_queue.queue_snapshot()
        self.assertEqual(after["active_sessions"], 0)

    async def test_owner_bypasses_queue(self):
        async with studio_queue.studio_agent_slot(
            user_id="owner",
            plan="owner",
            operation="chat",
            unlimited=False,
        ) as admission:
            self.assertEqual(admission.as_dict()["mode"], "bypass")

        after = await studio_queue.queue_snapshot()
        self.assertEqual(after["active_sessions"], 0)

    async def test_release_without_admission_does_not_underflow(self):
        await studio_queue.release_slot()
        after = await studio_queue.queue_snapshot()
        self.assertEqual(after["active_sessions"], 0)

    def test_fast_operations_bypass_queue(self):
        self.assertTrue(studio_queue.should_bypass_queue(operation="approve"))
        self.assertTrue(studio_queue.should_bypass_queue(operation="reject"))
        self.assertTrue(studio_queue.should_bypass_queue(operation="chat"))
        self.assertFalse(studio_queue.should_bypass_queue(operation="continue_production"))


if __name__ == "__main__":
    unittest.main()
