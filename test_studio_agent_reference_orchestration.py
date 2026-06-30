import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault("stripe", types.SimpleNamespace(api_key="", api_version=""))

from studio_agent import competitor, jobs, runner


class ReferenceOrchestrationTests(unittest.TestCase):
    def test_result_followups_force_deterministic_poll(self):
        for text in (
            "so what did you find?",
            "show me the results",
            "what are the findings?",
            "is the analysis done?",
        ):
            self.assertTrue(runner._is_job_status_followup(text), text)

    def test_competitor_snapshot_uses_stage_not_running_status(self):
        raw = {
            "job_id": "abc123",
            "status": "running",
            "stage": "extracting_audio",
            "percent": 90,
            "note": "Extracting audio",
        }
        with patch.object(competitor, "read_status", return_value=raw):
            snap = jobs._competitor_status("abc123")
        self.assertEqual(snap["status"], "running")
        self.assertEqual(snap["stage"], "extracting_audio")
        self.assertEqual(snap["progress"], 90)
        self.assertFalse(snap["analysis_ready"])

    def test_short_and_long_profiles_are_separate(self):
        short = competitor.analysis_profile("short")
        long = competitor.analysis_profile("long")
        self.assertIn("viewed_vs_swiped_away", short["channel_learning_metrics"])
        self.assertNotIn("viewed_vs_swiped_away", long["channel_learning_metrics"])
        self.assertIn("first_30_second_retention", long["channel_learning_metrics"])
        self.assertNotIn("first_30_second_retention", short["channel_learning_metrics"])
        self.assertNotIn("MrBeast", short["reference_archetypes"])

    def test_complete_status_overwrites_stale_audio_note(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            competitor._write_status(
                work,
                job_id="abc123",
                status="running",
                stage="extracting_audio",
                percent=90,
                note="Extracting audio",
            )
            competitor._write_status(
                work,
                status="complete",
                stage="complete",
                percent=100,
                note="Reference analysis complete.",
            )
            data = json.loads((work / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(data["stage"], "complete")
        self.assertEqual(data["note"], "Reference analysis complete.")

    def test_polled_reference_status_infers_competitor_without_kind(self):
        text = runner._format_polled_job_status(json.dumps({
            "job_id": "abcdef123456",
            "status": "running",
            "stage": "extracting_audio",
            "percent": 90,
            "analysis_profile": competitor.analysis_profile("short"),
        }))
        self.assertIn("reference analysis", text.lower())
        self.assertIn("extracting audio", text.lower())
        self.assertNotIn("production", text.lower())

    def test_complete_reference_status_returns_findings_and_conclusion(self):
        text = runner._format_polled_job_status(json.dumps({
            "job_id": "abcdef123456",
            "kind": "competitor",
            "status": "complete",
            "metadata": {"title": "The Reason You Never stay Consistant", "duration": 55},
            "analysis_profile": competitor.analysis_profile("short"),
            "pacing": {"avg_shot_sec": 5, "cut_count": 10, "duration_sec": 55, "hook_window_sec": 3},
            "frames": {"count": 11},
            "engagement": {"like_rate_pct": 3.2},
        }))
        self.assertIn("What I found from The Reason You Never stay Consistant", text)
        self.assertIn("average shot length 5.00s", text)
        self.assertIn("10 detected cuts", text)
        self.assertIn("Conclusion:", text)
        self.assertIn("fresh channel analytics and fresh public YouTube demand", text)

    def test_recover_poll_target_uses_explicit_competitor_kind_not_hex_shape(self):
        session = {
            "session_id": "s1",
            "active_jobs": [],
            "messages": [
                {
                    "role": "tool",
                    "content": json.dumps({"job_id": "abcdef123456", "kind": "competitor", "status": "running"}),
                }
            ],
        }
        with patch.object(runner.store, "get_session", return_value=session):
            self.assertEqual(runner._recover_poll_target(session), ("abcdef123456", "competitor"))

    def test_recover_poll_target_does_not_treat_12_hex_as_competitor_without_evidence(self):
        session = {
            "session_id": "s1",
            "active_jobs": [],
            "messages": [
                {
                    "role": "tool",
                    "content": json.dumps({"job_id": "abcdef123456", "status": "running"}),
                }
            ],
        }
        with patch.object(runner.store, "get_session", return_value=session):
            self.assertEqual(runner._recover_poll_target(session), ("abcdef123456", "shortform"))


if __name__ == "__main__":
    unittest.main()
