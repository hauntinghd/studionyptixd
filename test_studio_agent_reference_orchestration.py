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


if __name__ == "__main__":
    unittest.main()
