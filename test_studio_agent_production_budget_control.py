import json
import os
import shutil
import sys
import time
import types
import unittest
import uuid
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

try:
    import stripe  # noqa: F401
except ModuleNotFoundError:
    # Keep this focused unit test importable in minimal environments without
    # replacing a real Stripe SDK for every later test in the same process.
    sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent import jobs, production_budget
from studio_agent import runner


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

        self.assertTrue(budget["breakdown"]["review_gate"])
        self.assertTrue(budget["breakdown"]["visual_proof_only"])
        self.assertTrue(budget["breakdown"]["i2v_deferred_until_scene_approval"])
        self.assertEqual(budget["breakdown"]["requested_video_seconds"], 60.0)
        self.assertEqual(budget["breakdown"]["video_seconds"], 0.0)
        self.assertEqual(budget["breakdown"]["video_usd"], 0.0)

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
        self.assertEqual(budget["breakdown"]["image_edit_count"], 3)
        self.assertTrue(budget["breakdown"]["image_model_pricing_unit"])
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

    def test_shortform_snapshot_does_not_downgrade_existing_mp4_to_scene_review(self):
        job_id = f"test{uuid.uuid4().hex[:12]}"
        workspace = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            video = workspace / "styled_short.mp4"
            video.write_bytes(b"0" * 2048)
            (workspace / "result.json").write_text(
                json.dumps(
                    {
                        "job_id": job_id,
                        "status": "scenes_approved",
                        "topic": "Finished short",
                        "video_path": str(video),
                    }
                ),
                encoding="utf-8",
            )
            (workspace / "progress.json").write_text(
                json.dumps({"stage": "scenes_approved", "progress": 85}),
                encoding="utf-8",
            )
            (workspace / "scenes.json").write_text(
                json.dumps(
                    [
                        {
                            "index": 0,
                            "approved_for_video": True,
                            "approved_for_animation": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            snap = jobs.get_job_snapshot(job_id, "shortform")
            result = json.loads((workspace / "result.json").read_text(encoding="utf-8"))
            progress = json.loads((workspace / "progress.json").read_text(encoding="utf-8"))

            self.assertEqual(snap["status"], "complete")
            self.assertEqual(snap["progress"], 100)
            self.assertFalse(snap["running"])
            self.assertIn("/api/studio-agent/jobs/", snap["mp4_url"])
            self.assertEqual(result["status"], "complete")
            self.assertEqual(progress["stage"], "complete")
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

    def test_stale_shortform_snapshot_reclaims_from_durable_job_spec(self):
        job_id = f"test{uuid.uuid4().hex[:12]}"
        workspace = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            (workspace / "job_spec.json").write_text(
                json.dumps({"category_key": "people_blogs", "topic": "Recovered short"}),
                encoding="utf-8",
            )
            (workspace / "progress.json").write_text(
                json.dumps({"progress": 20, "stage": "stills", "detail": "Scene 1/12 still"}),
                encoding="utf-8",
            )
            (workspace / "heartbeat.txt").write_text("old", encoding="utf-8")
            old_ts = time.time() - 10
            for name in ("job_spec.json", "progress.json", "heartbeat.txt"):
                os.utime(workspace / name, (old_ts, old_ts))

            with (
                patch.object(jobs, "SHORTFORM_RECLAIM_SEC", 1),
                patch.object(jobs, "SHORTFORM_STALE_SEC", 999999),
                patch.object(jobs, "_start_shortform_reclaim_job", return_value=True) as reclaim,
            ):
                snap = jobs.get_job_snapshot(job_id, "shortform")

            reclaim.assert_called_once_with(workspace, job_id)
            self.assertEqual(snap["status"], "running")
            self.assertEqual(snap["stage"], "restarting")
            self.assertEqual(snap["stage_label"], "Restarting")
            self.assertGreaterEqual(snap["progress"], 22)
            self.assertIn("Resuming from the saved job spec", snap["stage_detail"])
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    def test_competitor_poll_for_shortform_workspace_returns_shortform_snapshot(self):
        job_id = uuid.uuid4().hex[:12]
        workspace = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            (workspace / "progress.json").write_text(
                json.dumps({"progress": 30, "stage": "stills", "detail": "Scene 3/12 still"}),
                encoding="utf-8",
            )

            snap = jobs.get_job_snapshot(job_id, "competitor")

            self.assertEqual(snap["kind"], "shortform")
            self.assertEqual(snap["status"], "running")
            self.assertEqual(snap["progress"], 30)
            self.assertNotEqual(snap["title"], "Reference analysis")
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    def test_continue_shortform_executes_next_production_steps_without_model_chat(self):
        job_id = uuid.uuid4().hex[:12]
        session = {
            "session_id": f"sa_{uuid.uuid4().hex[:16]}",
            "user_id": "owner",
            "content_format": "short",
            "approval_mode": "confirm",
            "reasoning_depth": "deep",
            "messages": [],
            "active_jobs": [{"job_id": job_id, "kind": "competitor"}],
        }
        first_snapshot = {
            "job_id": job_id,
            "kind": "shortform",
            "status": "awaiting_approval",
            "total_scenes": 2,
            "approved_scene_count": 0,
            "animation_pending_count": 0,
            "title": "Recovered short",
        }
        final_snapshot = {
            "job_id": job_id,
            "kind": "shortform",
            "status": "complete",
            "progress": 100,
            "title": "Recovered short",
        }
        calls: list[str] = []

        @asynccontextmanager
        async def fake_slot(**_kwargs):
            yield

        def fake_execute(name, args, **_kwargs):
            calls.append(name)
            if name == "set_production_scenes_animate":
                self.assertFalse(args["animate"])
                return json.dumps({"ok": True, "job_id": job_id, "status": "scenes_approved"})
            if name == "finalize_production":
                return json.dumps({"status": "complete", "job_id": job_id, "video_path": "out.mp4"})
            raise AssertionError(f"unexpected tool {name}")

        with (
            patch.object(runner, "_recover_poll_target", return_value=(job_id, "shortform")),
            patch.object(runner, "get_job_snapshot", side_effect=[first_snapshot, final_snapshot]),
            patch.object(runner, "execute_tool_logged", side_effect=fake_execute),
            patch.object(runner.store, "get_session", return_value=session),
            patch.object(runner.store, "update_session"),
            patch.object(runner, "studio_agent_slot", fake_slot),
        ):
            result = asyncio.run(
                runner._continue_active_production(
                    session=session,
                    user_id="owner",
                    content_format="short",
                    emit=None,
                    membership_plan="owner",
                    billing_profile={"unlimited": True},
                )
            )

        self.assertIsNotNone(result)
        self.assertEqual(calls, ["set_production_scenes_animate", "finalize_production"])
        message = result["assistant_message"].lower()
        self.assertIn("short finished", message)
        self.assertIn("mp4", message)
        self.assertEqual(result["active_jobs"], [])

    def test_continue_shortform_prunes_completed_job_instead_of_reactivating_it(self):
        job_id = uuid.uuid4().hex[:12]
        session = {
            "session_id": f"sa_{uuid.uuid4().hex[:16]}",
            "user_id": "owner",
            "content_format": "short",
            "approval_mode": "confirm",
            "reasoning_depth": "deep",
            "messages": [],
            "active_jobs": [{"job_id": job_id, "kind": "shortform", "title": "Old finished short"}],
        }
        complete_snapshot = {
            "job_id": job_id,
            "kind": "shortform",
            "status": "complete",
            "progress": 100,
            "title": "Old finished short",
            "mp4_url": f"/api/studio-agent/jobs/{job_id}/media?kind=shortform",
        }

        with (
            patch.object(runner, "_recover_poll_target", return_value=(job_id, "shortform")),
            patch.object(runner, "get_job_snapshot", return_value=complete_snapshot),
            patch.object(runner.store, "get_session", return_value=session),
            patch.object(runner.store, "update_session") as update_session,
        ):
            result = asyncio.run(
                runner._continue_active_production(
                    session=session,
                    user_id="owner",
                    content_format="short",
                    emit=None,
                    membership_plan="owner",
                    billing_profile={"unlimited": True},
                )
            )

        self.assertIsNotNone(result)
        self.assertEqual(result["active_jobs"], [])
        message = result["assistant_message"].lower()
        self.assertIn("ready", message)
        self.assertIn("preview or download", message)
        update_session.assert_called()
        self.assertEqual(update_session.call_args.kwargs["active_jobs"], [])

    def test_run_turn_threads_membership_plan_into_continue_shortcut(self):
        session = {
            "session_id": f"sa_{uuid.uuid4().hex[:16]}",
            "user_id": "owner",
            "content_format": "short",
            "approval_mode": "confirm",
            "reasoning_depth": "deep",
            "messages": [],
            "active_jobs": [{"job_id": uuid.uuid4().hex[:12], "kind": "shortform"}],
        }
        seen: dict[str, str] = {}

        @asynccontextmanager
        async def fake_slot(**_kwargs):
            yield types.SimpleNamespace(mode="disabled", as_dict=lambda: {})

        async def fake_continue(**kwargs):
            seen["membership_plan"] = kwargs["membership_plan"]
            return {
                "session_id": session["session_id"],
                "assistant_message": "continued",
                "pending_actions": [],
                "active_jobs": session["active_jobs"],
                "approval_mode": "confirm",
                "reasoning_depth": "deep",
                "usage": {},
                "billing": {"credits_charged": 0, "provider_usd": 0.0},
            }

        with (
            patch.object(runner, "studio_agent_slot", fake_slot),
            patch.object(runner, "_continue_active_production", side_effect=fake_continue),
            patch.object(runner.store, "update_session", return_value=session),
            patch.object(runner.store, "touch_title_from_user_message"),
            patch.object(runner.memory, "observe_user_message"),
        ):
            result = asyncio.run(
                runner.run_turn(
                    session,
                    "continue",
                    membership_plan="owner",
                    billing_profile={"unlimited": True},
                )
            )

        self.assertEqual(result["assistant_message"], "continued")
        self.assertEqual(seen["membership_plan"], "owner")


if __name__ == "__main__":
    unittest.main()
