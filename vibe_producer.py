import json
import random

class YouTubeProducer:
    def __init__(self):
        self.topics = {
            "AI": [
                "Generative AI Workflow",
                "ChatGPT for Productivity",
                "Midjourney Thumbnail Secrets",
                "AI Video Editing with Runway",
                "Automating Content with Claude"
            ],
            "Video Editing": [
                "Fast Motion Graphics",
                "Color Grading for Beginners",
                "Sound Design Hacks",
                "Multi-Cam Editing Simplicity",
                "Exporting for Maximum Retention"
            ]
        }

    def generate_seo_metadata(self, topic_type, specific_topic):
        """Generates SEO optimized title, description, and tags."""
        
        # SEO Patterns:
        # 1. [Topic] for [Niche] in 2026
        # 2. How to [Action] with [Tool] (FAST)
        # 3. Stop doing [Mistake] with [Topic]
        
        patterns = [
            f"How to use {specific_topic} to 10x Your Productivity (2026)",
            f"{specific_topic}: The ONLY Guide You Need for Video Editing",
            f"Stop Wasting Time on {specific_topic} - Do THIS instead.",
            f"Why {specific_topic} is the Future of AI Content Creation"
        ]
        
        title = random.choice(patterns)
        
        description = (
            f"In this video, we dive deep into {specific_topic}. "
            f"Whether you are a beginner or looking to optimize your {topic_type} workflow, this 3-minute guide "
            f"will show you the exact steps to master {specific_topic} fast.\n\n"
            "Key Takeaways:\n"
            "- Step-by-step setup\n"
            "- Advanced shortcuts\n"
            "- Final optimization tips\n\n"
            "#AI #VideoEditing #HowTo #YouTubeGrowth #CreativeWorkflow"
        )
        
        tags = [topic_type, specific_topic, "2026", "Tutorial", "How To", "Quick Guide", "Efficiency"]
        
        return {
            "title": title,
            "description": description,
            "tags": tags
        }

    def generate_video_script(self, specific_topic):
        """Generates a structured script for a 3-minute video (approx 450 words)."""
        script = {
            "00:00-00:15": "Hook: Show the transformation/final result. 'In 3 minutes, you will master {specific_topic}.'",
            "00:15-00:45": "The Setup: Exactly what you need to get started. No fluff.",
            "00:45-02:15": "Core Workflow: 3 distinct steps with on-screen overlays.",
            "02:15-02:45": "The 'Vibe' Secret: One high-level tip to stand out.",
            "02:45-03:00": "Outro: Call to action. 'Subscribe for more AI workflows.'"
        }
        return script

    def generate_thumbnail_prompt(self, specific_topic):
        """Generates a prompt for an image generator (DALL-E/Midjourney style)."""
        return (
            f"A high-contrast, premium YouTube thumbnail for a video about {specific_topic}. "
            "Centerpiece: A sleek 3D glassmorphism representation of an AI brain or a professional video editor timeline. "
            "Colors: Electric purple and neon cyan gradients. "
            "Aesthetics: Dark mode, extremely detailed, cinematic lighting, 8k resolution, 'HOW TO' in large bold font."
        )

if __name__ == "__main__":
    producer = YouTubeProducer()
    # Example Generation
    topic = random.choice(producer.topics["AI"])
    metadata = producer.generate_seo_metadata("AI", topic)
    script = producer.generate_video_script(topic)
    prompt = producer.generate_thumbnail_prompt(topic)
    
    print(f"--- PRODUCTION SPEC FOR: {topic} ---")
    print(json.dumps(metadata, indent=2))
    print("\n--- SCRIPT STRUCTURE ---")
    print(json.dumps(script, indent=2))
    print("\n--- THUMBNAIL PROMPT ---")
    print(prompt)
