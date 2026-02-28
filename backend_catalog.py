PLAN_LIMITS = {
    "free": {"videos_per_month": 3, "max_duration_sec": 30, "max_resolution": "720p", "can_clone": False, "priority": False, "demo_access": False},
    "starter": {"videos_per_month": 50, "max_duration_sec": 60, "max_resolution": "720p", "can_clone": False, "priority": False, "demo_access": False},
    "creator": {"videos_per_month": 150, "max_duration_sec": 180, "max_resolution": "1080p", "can_clone": True, "priority": True, "demo_access": False},
    "pro": {"videos_per_month": 999, "max_duration_sec": 300, "max_resolution": "1080p", "can_clone": True, "priority": True, "demo_access": False},
    "demo_pro": {"videos_per_month": 999, "max_duration_sec": 300, "max_resolution": "1080p", "can_clone": True, "priority": True, "demo_access": True},
}

RESOLUTION_CONFIGS = {
    "720p": {"gen_width": 720, "gen_height": 1280, "output_width": 720, "output_height": 1280, "upscale": False},
    "1080p": {"gen_width": 768, "gen_height": 1344, "output_width": 1080, "output_height": 1920, "upscale": True, "upscale_factor": 1.43},
}

ADMIN_EMAILS = {"omatic657@gmail.com"}
HARDCODED_PLANS = {
    "omatic657@gmail.com": "admin",
    "alwakmyhem@gmail.com": "pro",
}

PUBLIC_TEMPLATE_ALLOWLIST = {"skeleton", "objects", "wouldyourather", "scary", "history"}

SUPPORTED_LANGUAGES = {
    "en": {"name": "English", "model": "eleven_turbo_v2_5"},
    "hi": {"name": "Hindi", "model": "eleven_multilingual_v2"},
    "ta": {"name": "Tamil", "model": "eleven_multilingual_v2"},
    "te": {"name": "Telugu", "model": "eleven_multilingual_v2"},
    "bn": {"name": "Bengali", "model": "eleven_multilingual_v2"},
    "mr": {"name": "Marathi", "model": "eleven_multilingual_v2"},
    "gu": {"name": "Gujarati", "model": "eleven_multilingual_v2"},
    "kn": {"name": "Kannada", "model": "eleven_multilingual_v2"},
    "ml": {"name": "Malayalam", "model": "eleven_multilingual_v2"},
    "pa": {"name": "Punjabi", "model": "eleven_multilingual_v2"},
    "ur": {"name": "Urdu", "model": "eleven_multilingual_v2"},
    "es": {"name": "Spanish", "model": "eleven_multilingual_v2"},
    "pt": {"name": "Portuguese", "model": "eleven_multilingual_v2"},
    "de": {"name": "German", "model": "eleven_multilingual_v2"},
    "fr": {"name": "French", "model": "eleven_multilingual_v2"},
    "ja": {"name": "Japanese", "model": "eleven_multilingual_v2"},
    "ko": {"name": "Korean", "model": "eleven_multilingual_v2"},
    "ar": {"name": "Arabic", "model": "eleven_multilingual_v2"},
    "id": {"name": "Indonesian", "model": "eleven_multilingual_v2"},
}

TEMPLATE_VOICE_SETTINGS = {
    "skeleton": {"voice_id": "TX3LPaxmHKxFdv7VOQHJ", "stability": 0.30, "similarity_boost": 0.85, "style": 0.55, "speed": 1.15},
    "history": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.6, "similarity_boost": 0.8, "style": 0.2},
    "story": {"voice_id": "onwK4e9ZLuTAKqWW03F9", "stability": 0.65, "similarity_boost": 0.85, "style": 0.15},
    "reddit": {"voice_id": "TX3LPaxmHKxFdv7VOQHJ", "stability": 0.5, "similarity_boost": 0.75, "style": 0.35},
    "top5": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.55, "similarity_boost": 0.8, "style": 0.25},
    "roblox": {"voice_id": "TX3LPaxmHKxFdv7VOQHJ", "stability": 0.35, "similarity_boost": 0.7, "style": 0.5},
    "objects": {"voice_id": "onwK4e9ZLuTAKqWW03F9", "stability": 0.6, "similarity_boost": 0.85, "style": 0.3},
    "split": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.5, "similarity_boost": 0.8, "style": 0.3},
    "twitter": {"voice_id": "TX3LPaxmHKxFdv7VOQHJ", "stability": 0.45, "similarity_boost": 0.75, "style": 0.4},
    "quiz": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.45, "similarity_boost": 0.8, "style": 0.4},
    "argument": {"voice_id": "TX3LPaxmHKxFdv7VOQHJ", "stability": 0.4, "similarity_boost": 0.75, "style": 0.45},
    "wouldyourather": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.5, "similarity_boost": 0.8, "style": 0.35},
    "scary": {"voice_id": "onwK4e9ZLuTAKqWW03F9", "stability": 0.7, "similarity_boost": 0.9, "style": 0.1},
    "motivation": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.65, "similarity_boost": 0.85, "style": 0.15},
    "whatif": {"voice_id": "onwK4e9ZLuTAKqWW03F9", "stability": 0.55, "similarity_boost": 0.85, "style": 0.25},
    "random": {"voice_id": "pNInz6obpgDQGcFmaJgB", "stability": 0.4, "similarity_boost": 0.7, "style": 0.5},
}

TEMPLATE_SFX_STYLES = {
    "skeleton": "dark eerie ambient drone, subtle horror atmosphere",
    "scary": "creepy horror atmosphere, tension building drone",
    "objects": "mysterious discovery sound, wonder ambient",
    "wouldyourather": "dramatic suspense, game show tension",
    "history": "epic cinematic atmosphere, dramatic orchestra hint",
}
