"""CrypticScience — What Google AI Mode Actually Changes (verified script v2).

Sources: Google Search blog + Pichai I/O 2026 keynote only.
Target ~8–9 min @ ~145 WPM with natural VO pacing.
"""
from __future__ import annotations

TITLE = "What Google AI Mode Actually Changes (Verified — I/O 2026)"

SCENES = [
    {
        "id": "01_hook",
        "chapter": "Cold open",
        "caption": "what actually changed?",
        "source_line": "Source: Google Search Blog · May 19, 2026",
        "narration": (
            "If you have seen headlines saying Google killed Search — that is not what Google announced. "
            "On May nineteenth, twenty twenty-six, at I/O, Google shipped the largest batch of Search "
            "changes in years. Most of it centers on AI Mode — their conversational Search layer — "
            "plus new agents, a rebuilt search box, and tools that generate custom interfaces on the fly. "
            "This video uses only Google's own posts and keynote language. "
            "We will separate what is live today, what arrives this summer, what requires a paid plan, "
            "and what Google never actually promised."
        ),
        "pexels_query": "person searching laptop browser night",
        "pexels_query_b": "google office technology campus",
    },
    {
        "id": "02_what_is",
        "chapter": "What AI Mode is",
        "caption": "ai mode vs ai overviews",
        "source_line": "Source: Google I/O 2026 · Liz Reid + Sundar Pichai",
        "narration": (
            "AI Mode is not a new app you download. It is a Search experience — "
            "you ask in plain language, Search reasons across the web, "
            "and you still get links and supporting articles. "
            "Google draws a line between AI Mode and AI Overviews. "
            "Overviews are the short AI summaries on many result pages. "
            "Pichai said Overviews now have two point five billion monthly active users. "
            "AI Mode is the deeper, back-and-forth layer — follow-up questions, multimodal inputs, "
            "and the place Google tests frontier features first. "
            "Search VP Liz Reid wrote that AI Mode passed one billion monthly users in just one year — "
            "and that queries inside AI Mode are more than doubling every quarter. "
            "Google also says that when people use AI-powered Search features, they search more over time — "
            "not less — because each question opens into a conversation instead of a single keyword box."
        ),
        "pexels_query": "smartphone search typing close up",
        "pexels_query_b": "woman reading phone screen cafe",
    },
    {
        "id": "03_live_today",
        "chapter": "Live today",
        "caption": "three changes live now",
        "source_line": "Source: Google Search Blog · May 19, 2026",
        "narration": (
            "Three upgrades are rolling out now, in every country and language where AI Mode is available. "
            "First — the model swap. Gemini three point five Flash is the new default inside AI Mode globally. "
            "Google positions Flash as sustained frontier performance for agents and coding — "
            "not a lightweight fallback. "
            "Second — the search box. Google calls this the biggest upgrade to the box in twenty-five years. "
            "It expands as you type, suggests full questions with AI instead of only keyword autocomplete, "
            "and accepts text, images, files, video, and Chrome tabs as inputs in one query. "
            "Google says you still get a range of results like today — not a single AI paragraph only. "
            "Third — continuity. You can ask a follow-up directly from an AI Overview and flow into AI Mode "
            "on desktop and mobile worldwide. Your context stays with you, "
            "and Google says supporting links get more relevant as the thread goes deeper."
        ),
        "pexels_query": "typing keyboard computer monitor",
        "pexels_query_b": "developer coding multiple monitors",
    },
    {
        "id": "04_agents",
        "chapter": "Search agents",
        "caption": "information agents",
        "source_line": "Source: Google Search Blog · May 19, 2026",
        "narration": (
            "The most structurally new feature is Search agents — and Google is clear: "
            "this is not fully live for everyone on day one. "
            "Information agents run in the background, twenty-four seven. "
            "You set a question once; the agent watches blogs, news sites, social posts, "
            "plus Google's live data for finance, shopping, and sports. "
            "When something changes, it sends a synthesized update and can help you act. "
            "Google's examples: apartment hunting — you brain-dump requirements once, "
            "the agent scans listings and notifies you when a match appears. "
            "Or tracking athlete sneaker collaborations so you do not miss a drop. "
            "You create an agent by adding keep me updated to a search. "
            "Active agents live in the side panel inside AI Mode. "
            "Launch is this summer — starting with Google AI Pro and Ultra subscribers first."
        ),
        "pexels_query": "smartphone notification alert hand",
        "pexels_query_b": "apartment city skyline window",
    },
    {
        "id": "05_generative_ui",
        "chapter": "Generative UI",
        "caption": "answers built on the fly",
        "source_line": "Source: Google Search Blog · May 19, 2026",
        "narration": (
            "Google is embedding Antigravity — its agentic coding platform — directly into Search. "
            "That means Search can assemble a custom response format for your question instead of "
            "defaulting to a text block. "
            "Google lists interactive visuals, tables, graphs, and simulations — "
            "for example understanding astrophysics concepts or how a watch mechanism works. "
            "These generative UI capabilities arrive this summer for everyone, free of charge. "
            "For recurring tasks — planning a wedding, managing a move, building a wellness routine — "
            "Search can go further and code persistent dashboards you revisit, "
            "pulling live reviews, maps, weather, and local data into a mini app tailored to you. "
            "Google compares them to lightweight apps for one job. "
            "That persistent dashboard tier comes in the following months — "
            "Pro and Ultra subscribers in the United States first."
        ),
        "pexels_query": "data dashboard analytics screen",
        "pexels_query_b": "3d animation technology hologram",
    },
    {
        "id": "06_personal",
        "chapter": "Personal Intelligence",
        "caption": "opt in — not automatic",
        "source_line": "Source: Google Search Blog · May 19, 2026",
        "narration": (
            "Personal Intelligence is expanding to nearly two hundred countries and territories "
            "across ninety-eight languages. Google notes no subscription is required for that expansion. "
            "This feature lets AI Mode use context from apps you explicitly connect — "
            "Gmail, Google Photos, and soon Google Calendar. "
            "Google repeats the control language: transparency, choice, and opt-in. "
            "You decide whether to connect; you can disconnect. "
            "That is different from agents watching the public web — this is your private context, "
            "only if you allow it. "
            "Separately, agentic booking expands in the United States this summer. "
            "You share criteria — like a private karaoke room for six on a Friday that serves food late — "
            "and Search surfaces pricing and availability with links to finish booking on the provider's site. "
            "For home repair, beauty, and pet care, Google says Search can call businesses on your behalf."
        ),
        "pexels_query": "calendar email laptop workspace",
        "pexels_query_b": "restaurant booking phone call",
    },
    {
        "id": "07_searchers",
        "chapter": "For searchers",
        "caption": "if you just use google",
        "source_line": "Source: Google I/O 2026 announcements",
        "narration": (
            "If you are a normal searcher, the practical shift is conversational depth. "
            "Fewer one-shot keyword queries — more threads where each follow-up narrows the answer. "
            "The new search box rewards full questions and multimodal inputs, "
            "so screenshots, PDFs, and open tabs become part of the query itself. "
            "Nothing in Google's posts says you must pay to use AI Mode's core model upgrade "
            "or the redesigned search box. "
            "The paid gate shows up mainly on background agents and early access to persistent mini apps. "
            "You also do not have to connect Gmail or Photos — Personal Intelligence stays off unless you turn it on."
        ),
        "pexels_query": "student researching library laptop",
        "pexels_query_b": "family using tablet home couch",
    },
    {
        "id": "08_creators",
        "chapter": "For creators",
        "caption": "if you publish online",
        "source_line": "Source: Google I/O 2026 announcements",
        "narration": (
            "If you publish online, Google's language still promises links to the web — "
            "but the surface users see first is increasingly AI-generated. "
            "Reid wrote that as conversations go deeper, supporting articles become more relevant — "
            "which implies citation inside AI answers may matter as much as classic blue-link ranking. "
            "Generative UI could also mean fewer clicks for simple factual queries "
            "when Search renders the answer inline. "
            "Google did not publish new Search Console metrics for AI citations in these posts — "
            "so treat creator impact as inference, not a announced stat. "
            "The verifiable part: Google is optimizing for time-in-Search via conversation, "
            "and that changes where attention lands even when links remain available."
        ),
        "pexels_query": "youtuber recording setup camera",
        "pexels_query_b": "blog writer analytics dashboard",
    },
    {
        "id": "09_limits",
        "chapter": "What Google did not say",
        "caption": "what google did not say",
        "source_line": "Verified limits · May 19, 2026",
        "narration": (
            "Here is what Google did not announce — and third-party headlines sometimes imply anyway. "
            "No date for removing traditional results or forcing every query through AI Mode. "
            "Information agents and persistent mini apps are staged — summer and coming months, "
            "not universally available on launch day. "
            "Personal Intelligence never turns on by itself; linking Gmail or Photos is manual. "
            "Google's posts do not guarantee that AI answers will always cite your site, "
            "or that Overviews will appear on every query. "
            "We are not selling a course, a tool, or a subscription in this video. "
            "The value is clarity: what shipped May nineteenth, what is queued for summer, "
            "and where the off switches are."
        ),
        "pexels_query": "reading news newspaper tablet",
        "pexels_query_b": "thinking man window city",
    },
    {
        "id": "10_cta",
        "chapter": "Close",
        "caption": "sources in description",
        "source_line": "blog.google · I/O 2026 Search posts",
        "narration": (
            "Primary sources are linked in the description — "
            "Google's Search I/O post by Liz Reid and Sundar Pichai's opening keynote, "
            "both dated May nineteenth, twenty twenty-six. "
            "If this helped you separate signal from panic, like the video so others find it. "
            "Comment what to explain next — AI agents, generative UI in Search, "
            "or how creators should adapt. Top requests become the next verified breakdown. "
            "Subscribe if you want platform changes explained with receipts, not hype."
        ),
        "pexels_query": "technology conference audience",
        "pexels_query_b": "subscribe like comment social media",
    },
]
