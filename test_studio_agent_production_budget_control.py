import json
import shutil
import sys
import types
import unittest
import uuid

sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent import jobs, production_budget


class ProductionBudgetControlTests(unittest.TestCase):
    def _metadata_for(self, tool_name, args):
        estimate = production_budget.estimate_tool_cost(tool_name, args)
        encoded = production_budget.with_budget_metadata("{}", estimate, args)
        return json.loads(encoded)

    def test_shortform_generation_defers_i2v_until_scene_approval(self):
        data = self._metadata_for(
            "start_shortform_generate",
            {
                "scene_count": 12,
                "duration_seconds": 60,
                "video_model": "seedance",
            },
        )

        budget = data["budget"]
        control = data["production_control"]

        self.assertEqual(budget["tool"], "start_shortform_generate")
        self.assertTrue(budget["breakdown"]["review_gate"])
        self.assertTrue(budget["breakdown"]["i2v_deferred_until_scene_approval"])
        self.assertEqual(budget["breakdown"]["video_seconds"], 0.0)
        self.assertEqual(control["lane"], "render")
        self.assertTrue(control["requires_approval"])
        self.assertTrue(control["resume_safe"])
        self.assertIn("await_scene_review", control["stage_gates"])
        self.assertTrue(control["durable_state"]["must_persist"])

    def test_full_auto_shortform_is_explicit_before_i2v_cost_is_included(self):
        data = self._metadata_for(
            "start_shortform_generate",
            {
                "scene_count": 12,
                "duration_seconds": 60,
                "video_model": "seedance",
                "animate": True,
                "_full_auto": True,
            },
        )

        budget = data["budget"]

        self.assertFalse(budget["breakdown"]["review_gate"])
        self.assertFalse(budget["breakdown"]["i2v_deferred_until_scene_approval"])
        self.assertGreater(budget["breakdown"]["video_seconds"], 0.0)
        self.assertGreater(budget["breakdown"]["video_usd"], 0.0)

    def test_batch_still_edits_are_render_lane_review_gated_and_persisted(self):
        data = self._metadata_for(
            "edit_production_scenes_still",
            {
                "job_id": "shortform-test",
                "scene_indices": [1, 2, 3],
                "scope": "character",
            },
        )

        budget = data["budget"]
        control = data["production_control"]

        self.assertEqual(budget["tool"], "edit_production_scenes_still")
        self.assertEqual(budget["breakdown"]["seedream_v45_edit_images"], 3)
        self.assertEqual(control["lane"], "render")
        self.assertFalse(control["requires_approval"])
        self.assertIn("await_scene_review", control["stage_gates"])
        self.assertEqual(control["durable_state"]["job_id"], "shortform-test")
        self.assertTrue(control["durable_state"]["must_persist"])

    def test_animation_requires_scene_approval_gate(self):
        data = self._metadata_for(
            "animate_production_scenes",
            {
                "job_id": "shortform-test",
                "scene_indices": [1, 2],
            },
        )

        control = data["production_control"]

        self.assertEqual(control["lane"], "render")
        self.assertTrue(control["requires_approval"])
        self.assertIn("scene_approval_required", control["stage_gates"])
        self.assertIn("await_animation_result", control["stage_gates"])
        self.assertTrue(control["resume_safe"])

    def test_shortform_review_snapshot_exposes_scene_approval_gate(self):
        job_id = f"test{uuid.uuid4().hex[:12]}"
        workspace = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            (workspace / "result.json").write_text(
                json.dumps(
                    {
                        "job_id": job_id,
                        "status": "awaiting_scene_review",
                        "topic": "Scene review test",
                    }
                ),
                encoding="utf-8",
            )
            (workspace / "scenes.json").write_text(
                json.dumps(
                    [
                        {
                            "index": 0,
                            "duration_sec": 5,
                            "narration": "test",
                            "approved_for_video": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            snap = jobs.get_job_snapshot(job_id, "shortform")

            self.assertEqual(snap["status"], "awaiting_approval")
            self.assertTrue(snap["awaiting_user_approval"])
            self.assertEqual(snap["queue_lane"], "render")
            self.assertFalse(snap["can_finalize"])
            self.assertIn("await_scene_review", snap["stage_gates"])
            self.assertIn("next_action", snap["production_control"])
            self.assertTrue(snap["production_control"]["durable_state"]["must_persist"])
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    def test_running_shortform_snapshot_exposes_resume_safe_render_lane(self):
        job_id = f"test{uuid.uuid4().hex[:12]}"
        workspace = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            (workspace / "progress.json").write_text(
                json.dumps({"progress": 44, "stage": "render", "detail": "Rendering"}),
                encoding="utf-8",
            )

            snap = jobs.get_job_snapshot(job_id, "shortform")

            self.assertEqual(snap["status"], "running")
            self.assertFalse(snap["awaiting_user_approval"])
            self.assertEqual(snap["queue_lane"], "render")
            self.assertIn("await_scene_review", snap["stage_gates"])
            self.assertTrue(snap["production_control"]["resume_safe"])
        finally:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
