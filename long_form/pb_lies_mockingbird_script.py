"""Operation Mockingbird — per-scene narration (synced to visual beats).

Target ~15 min @ ~150 WPM. Scene 01 = SFX-only cold open (6s silence).
Each voiced scene ~55–70 words (~22–28s VO). Clips stretch/trim to match exactly.
"""
from __future__ import annotations

TITLE = "Operation Mockingbird: How the CIA Bought the American Press"

NARRATION: dict[str, str] = {
    "scene_01_classified_desk": "",
    "scene_02_chapter_program": (
        "Act One. The Program. Before we name the journalists, we have to name "
        "the machine that recruited them."
    ),
    "scene_03_oss_office_1947": (
        "In nineteen forty-seven, the United States stood at the edge of a new kind "
        "of war. Not fought with armies on a map — fought with headlines, radio "
        "voices, and the stories Americans trusted before breakfast. The OSS had "
        "just dissolved. The CIA was being born. And inside both institutions, "
        "a small group of men already understood that the next battlefield would "
        "be the front page."
    ),
    "scene_04_wisner_map_wall": (
        "Frank Wisner built the Office of Policy Coordination inside the CIA. "
        "His job was not to win battles overseas. It was to win the argument "
        "before the public ever knew there was one. Wisner mapped influence the "
        "way generals mapped terrain — who could be approached, who could be "
        "briefed, who would take a phone call after midnight and never ask why."
    ),
    "scene_05_cia_founding_stamp": (
        "When the Central Intelligence Agency was formally established in "
        "nineteen forty-seven, Wisner's shop inherited a simple premise: "
        "control the story, and you control the room. The founding documents "
        "talked about national security in abstract language. The operational "
        "files talked about editors, publishers, and columnists in concrete ones."
    ),
    "scene_06_manila_archive_shelf": (
        "The files were never meant to sit on open shelves. Manila folders. "
        "Typed memos. Routing slips stamped SECRET. Each one a receipt for "
        "something the press was never supposed to print. Researchers would "
        "spend decades chasing those folders through FOIA delays and partial "
        "declassification — because Mockingbird was designed to leave paper."
    ),
    "scene_07_void_corridor_walk": (
        "Mockingbird was not one memo with a signature at the bottom. It was "
        "a corridor of programs — propaganda, placement, pressure — all "
        "pointing the same direction. Some operations had code names. Some never "
        "had names at all. What they shared was a theory of American democracy: "
        "that citizens could be managed if the right stories arrived in the right "
        "order."
    ),
    "scene_08_library_pull_file": (
        "Historians would later argue about names and numbers. But the pattern "
        "was visible early: intelligence officers treating newsrooms the way "
        "generals treated supply lines. You do not need to own every outlet. You "
        "need to know which three phone calls still move the narrative on a slow "
        "news day — and who picks up when Langley rings."
    ),
    "scene_09_agency_badge_desk": (
        "Inside Langley, media influence was bureaucracy. Budget lines. "
        "Contact lists. Asset reports filed the same way a case officer "
        "files a dead drop. The language in those reports is cold on purpose. "
        "Editors become contacts. Contacts become access. Access becomes the "
        "ability to place a paragraph that reads like news and functions like policy."
    ),
    "scene_10_newspaper_headline_1950s": (
        "By the early fifties, the Cold War was a daily front page. Every "
        "editorial choice — what to amplify, what to bury — became a strategic "
        "decision whether they admitted it or not. A single headline could "
        "validate a covert program overseas or destroy a domestic critic. "
        "Mockingbird lived in that margin between journalism and statecraft."
    ),
    "scene_11_strategy_table_folders": (
        "Wisner called it a mighty Wurlitzer — one keyboard, many outlets. "
        "The CIA did not need to own every newspaper. It only needed to know "
        "which levers still moved. Funding could flow through foundations. "
        "Stories could arrive through friendly correspondents. Denials could be "
        "pre-written before the question was even asked in public."
    ),
    "scene_12_org_chart_screen": (
        "On paper it looked like a network diagram. Funds to front groups. "
        "Front groups to editors. Editors to readers. The American press "
        "reimagined as a relay station. That diagram is the part Mockingbird "
        "deniers hate most — because it turns a romantic institution into "
        "infrastructure. And infrastructure can be maintained."
    ),
    "scene_13_chapter_network": (
        "Act Two. The Network. If Act One was the blueprint, Act Two is the "
        "wiring — who got connected, and what it cost to stay connected."
    ),
    "scene_14_professor_journalist": (
        "Some recruits looked like journalists. Some looked like professors. "
        "The Church Committee would later document CIA relationships with "
        "media figures who moved between both worlds without announcing the "
        "handoff. A byline in the morning. A briefing in the afternoon. "
        "The public saw the article. It did not see the routing slip."
    ),
    "scene_15_corporate_newsroom": (
        "Major newsrooms in New York and Washington were not monoliths. They "
        "were desks, rivalries, and deadlines — which made them perfect targets "
        "for a patient agency with a long memory. Mockingbird did not require "
        "every reporter to be complicit. It required enough pressure points "
        "that the institution bent without looking like it bent at all."
    ),
    "scene_16_tv_broadcast_studio": (
        "Television changed the tempo. A thirty-second segment could reach "
        "millions. Mockingbird-era programs expanded from print into broadcast "
        "just as the living room became the battlefield. The visual language of "
        "trust — anchors, sets, ticker tape — became another surface the agency "
        "could learn to play."
    ),
    "scene_17_terminal_names_scroll": (
        "Internal lists circulated. Who could be approached. Who could be "
        "briefed off the record. Who would take a check and call it expenses. "
        "Those lists are the moral center of the scandal — not because every "
        "name on them committed a crime, but because the list itself proves "
        "the scale of the attempt."
    ),
    "scene_18_typewriter_desk": (
        "The mechanism was often mundane. A lunch. A tip. A planted story "
        "rewritten just enough to survive an editor's pencil. Influence "
        "wearing the costume of routine journalism. That is why Mockingbird "
        "is hard to prosecute in the court of public opinion — the crime "
        "looks like a normal Tuesday in a newsroom."
    ),
    "scene_19_corkboard_evidence": (
        "When journalists died young, or careers collapsed overnight, "
        "researchers drew strings on corkboards trying to separate coincidence "
        "from consequence. Mockingbird sits in that uncomfortable gray. "
        "Not every string leads back to Langley. But enough strings do that "
        "the board keeps getting rebuilt every decade."
    ),
    "scene_20_interview_room": (
        "Off-the-record meant off-the-record — unless you were the agency "
        "keeping the transcript. Interviews became intelligence products "
        "wrapped in courtesy. A source speaks freely because they trust the "
        "reporter. The reporter speaks carefully because someone else may be "
        "reading the notes later in a room with no cameras."
    ),
    "scene_21_wire_service_teletype": (
        "Wire services amplified everything downstream. Control the wire, and "
        "a single paragraph could become a thousand local headlines by morning. "
        "Mockingbird understood distribution physics better than most publishers "
        "did — because the agency's product was never ink. It was reach."
    ),
    "scene_22_newspaper_layout_table": (
        "Layout editors decided what sat above the fold. Placement is power. "
        "Mockingbird understood that power was often exercised in inches, not "
        "oratories. A story moved from page twelve to page one is a story "
        "reclassified as reality — even when the underlying facts never changed."
    ),
    "scene_23_classified_board_crowd": (
        "The scale is the part that still shocks. Investigators and journalists "
        "would claim hundreds of media relationships — not one rogue reporter, "
        "but a system. When you see multiple silhouettes in the same frame, "
        "that is the visual argument: Mockingbird was industrial, not individual."
    ),
    "scene_24_four_hundred_card": (
        "Four hundred journalists. That number comes from Carl Bernstein's "
        "nineteen seventy-seven Rolling Stone investigation — disputed, "
        "partially confirmed, never fully put to rest. It remains the headline "
        "Mockingbird cannot escape. Whether the exact count holds or not, "
        "the direction of the evidence is consistent: this was not a hobby."
    ),
    "scene_25_chapter_declassified": (
        "Act Three. Declassified. The program did not end because someone felt "
        "guilty. It ended because the country started reading its own files."
    ),
    "scene_26_congress_hearing": (
        "The nineteen seventies broke the seal. Watergate cracked public trust. "
        "The Church Committee pulled intelligence programs into fluorescent "
        "hearing rooms and made senators read the parts agencies hoped would "
        "stay classified. Mockingbird was not always named cleanly in open "
        "testimony — but media manipulation was. The language shifted from "
        "denial to managed admission."
    ),
    "scene_27_church_committee_files": (
        "Committee staff stacked binders high enough to hide a witness. "
        "Mockingbird was not always named cleanly in the open testimony — "
        "but media manipulation was. The files show a government that treated "
        "the First Amendment as a variable to solve for, not a boundary to respect."
    ),
    "scene_28_modern_laptop_leak": (
        "Leaks did not end with typewriters. Each new format — cable, digital, "
        "social — reopened the same question: who sets the frame when the frame "
        "is the product? A PDF on a laptop is still a manila folder. A thread "
        "is still a front page. The Wurlitzer learned new instruments."
    ),
    "scene_29_capitol_steps_walk": (
        "Washington runs on information advantage. The steps outside those "
        "hearing rooms are where public language gets sanitized and the raw "
        "language stays in the vault. Senators leave the camera and return to "
        "language that can be quoted. Staffers leave with language that cannot."
    ),
    "scene_30_world_map_ops": (
        "Mockingbird was American in origin, but the logic was global. "
        "Influence operations do not respect borders when the audience is "
        "everywhere the signal reaches. The map on the wall is a reminder: "
        "once you treat information as a weapon, every country with a printing "
        "press becomes terrain."
    ),
    "scene_31_modern_newsroom_night": (
        "Today's newsrooms glow at midnight with different tools — same tension. "
        "Sources, subsidies, silence. The Wurlitzer did not disappear. It "
        "learned new instruments. Algorithmic feeds replaced wire services. "
        "Influencers replaced columnists. The architecture changed. The incentive "
        "did not."
    ),
    "scene_32_cold_vault_archive": (
        "Declassification is slow because embarrassment is durable. Boxes "
        "leave the vault redacted line by redacted line — a drip feed of "
        "confirmations dressed as footnotes. Mockingbird teaches you to read "
        "what is missing as carefully as what remains."
    ),
    "scene_33_stamped_hazard_files": (
        "Every CONFIDENTIAL stamp is a small admission: someone knew this "
        "document could damage trust if it landed on the wrong desk. "
        "Mockingbird was built on that calculation — that the republic could "
        "absorb a managed story more easily than an unmanaged fact."
    ),
    "scene_34_minimal_single_document": (
        "One page. One redacted paragraph. That is often all the public gets — "
        "enough to know the program was real, never enough to map the whole "
        "machine. That gap is not an accident. It is the final product."
    ),
    "scene_35_return_desk_mirror": (
        "We opened on a folder that should not exist in daylight. We close "
        "with the same desk — because the story was never about a single "
        "operation. It was about who gets to write the first draft of history. "
        "The folder now reads declassified. The habit does not."
    ),
    "scene_36_end_tease_desk": (
        "Operation Mockingbird is the case file. The pattern is the warning. "
        "If you want the next declassified operation — Berlin Tunnel, MK-Ultra, "
        "or the program you think still has not surfaced — subscribe and tell us "
        "which file to open next in the comments."
    ),
}

SILENT_SCENE_SEC: dict[str, float] = {
    "scene_01_classified_desk": 15.0,
}

SFX_BY_BEAT: dict[str, str] = {
    "cold_open": (
        "classified document room tension, paper rustle, distant stamp, "
        "forensic red pulse, low documentary drone, no voice"
    ),
    "chapter_card": "deep cinematic hit, low brass sting, documentary chapter transition",
    "pause": "near silence, single low string note, archival room tone",
}


def _narration_for(key: str) -> str:
    """Return narration with pacing pad for sub-80-word scenes."""
    from long_form.pb_lies_mockingbird_scenes import MOCKINGBIRD_SCENES

    base = NARRATION.get(key, "").strip()
    if not base:
        return ""
    if len(base.split()) >= 80:
        return base
    spec = MOCKINGBIRD_SCENES.get(key, {})
    ch = int(spec.get("chapter") or 1)
    pads = {
        1: "The early files are the clearest — before the language turned euphemistic.",
        2: "The network only worked because each node could plausibly deny the whole.",
        3: "What survived declassification is the architecture — not the apology.",
    }
    return f"{base} {pads.get(ch, pads[3])}"
