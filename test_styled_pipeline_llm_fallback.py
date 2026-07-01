import unittest

from skeleton_ai import styled_pipeline
from studio_agent.render_styles import get_render_style


class FailingGrok:
    def complete(self, *_args, **_kwargs):
        raise RuntimeError("Server error '500 Internal Server Error' for url 'https://fal.run/fal-ai/any-llm'")


class StyledPipelineLlmFallbackTests(unittest.TestCase):
    def test_analyze_script_styled_falls_back_when_any_llm_500s(self):
        style = get_render_style("comic_book")

        plan = styled_pipeline.analyze_script_styled(
            FailingGrok(),
            "He pulls away right after getting close. The pattern repeats whenever intimacy feels real.",
            style=style,
            category_label="People & Blogs",
            topic="The Real Reason Men Pull Away After Getting Close",
        )

        self.assertTrue(plan["local_fallback"])
        self.assertIn("characters", plan)
        self.assertIn("fallback_outfit", plan)
        self.assertIn(style.label, plan["topic_setting"])

    def test_derive_beat_visuals_styled_falls_back_when_any_llm_500s(self):
        style = get_render_style("comic_book")
        plan = {
            "characters": {},
            "topic_setting": "comic book relationship psychology short",
            "fallback_outfit": "dark jacket and clean modern styling",
        }

        outfit, action, motion = styled_pipeline.derive_beat_visuals_styled(
            FailingGrok(),
            "He goes quiet because closeness starts to feel like pressure.",
            "People & Blogs",
            style=style,
            plan=plan,
            visual_brief="male psychology, emotional distance, symbolic scene",
        )

        self.assertIn("dark jacket", outfit)
        self.assertIn("He goes quiet", action)
        self.assertIn("male psychology", action)
        self.assertIn("camera", motion.lower())

    def test_local_script_fallback_is_usable_narration(self):
        script = styled_pipeline._local_script_fallback(
            "The Real Reason Men Pull Away After Getting Close",
            "People & Blogs",
        )

        self.assertGreater(len(script), 300)
        self.assertNotIn("{", script)
        self.assertNotIn("```", script)
        self.assertGreaterEqual(len(styled_pipeline.split_script_into_beats(script, target_count=12)), 6)


if __name__ == "__main__":
    unittest.main()
