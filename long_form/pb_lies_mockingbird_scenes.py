"""Operation Mockingbird — scene stills map (15 min, 3 chapters × 12 beats).

Each scene locks a roster variant + environment archetype (Nod Map diorama swap).
Imported by pb_lies_cast_kit.py for --scenes / --scene generation.
"""
from __future__ import annotations

from typing import Any

# 36 beats @ ~25s VO each ≈ 15 min with chapter cards + silence opens.
MOCKINGBIRD_SCENES: dict[str, dict[str, Any]] = {
    # ── Ch1: The Program (1940s–1950s origins) ──────────────────────────────
    "scene_01_classified_desk": {
        "id": "scene_01_classified_desk",
        "chapter": 1,
        "beat": "cold_open",
        "variant": "white_suit_default",
        "prompt_delta": (
            "SCENE: Single figure at black reflective table examining open manila "
            "folder and CIA document with bold redacted black bars. Dark void "
            "background with subtle forensic red glow on document edges. Medium-wide "
            "shot, Operation Mockingbird cold open, empty top third for title."
        ),
    },
    "scene_02_chapter_program": {
        "id": "scene_02_chapter_program",
        "chapter": 1,
        "beat": "chapter_card",
        "variant": "forensic_red",
        "prompt_delta": (
            "SCENE: Figure standing before massive dark wall with bold white chapter "
            "title text reading THE PROGRAM in clean sans-serif. Red classified stamp "
            "glow, minimal props, symmetrical composition, empty center for overlay."
        ),
    },
    "scene_03_oss_office_1947": {
        "id": "scene_03_oss_office_1947",
        "chapter": 1,
        "beat": "evidence",
        "variant": "warm_sepia",
        "prompt_delta": (
            "SCENE: 1940s government office diorama — figure at wooden desk with "
            "period typewriter, wall map of Europe, warm sepia tungsten lighting, "
            "manila folders, medium shot, OSS-era atmosphere."
        ),
    },
    "scene_04_wisner_map_wall": {
        "id": "scene_04_wisner_map_wall",
        "chapter": 1,
        "beat": "character_intro",
        "variant": "bone_ivory",
        "prompt_delta": (
            "SCENE: Figure in profile studying large wall map with red string pins "
            "connecting cities. Name plate UI space on lower third. Purple-red rim "
            "light from map side, investigative documentary framing, medium-wide."
        ),
    },
    "scene_05_cia_founding_stamp": {
        "id": "scene_05_cia_founding_stamp",
        "chapter": 1,
        "beat": "evidence",
        "variant": "steel_blue_suit",
        "prompt_delta": (
            "SCENE: Close-medium shot — figure holding declassified document with "
            "bold 1947 date and TOP SECRET stamp. Navy formal government office "
            "background blur, cold blue fill light."
        ),
    },
    "scene_06_manila_archive_shelf": {
        "id": "scene_06_manila_archive_shelf",
        "chapter": 1,
        "beat": "evidence",
        "variant": "manila_tan",
        "prompt_delta": (
            "SCENE: Figure pulling manila folder from floor-to-ceiling archive shelf "
            "aisle. Warm amber practical lamp, deep perspective down aisle, "
            "documentary evidence mood."
        ),
    },
    "scene_07_void_corridor_walk": {
        "id": "scene_07_void_corridor_walk",
        "chapter": 1,
        "beat": "transition",
        "variant": "charcoal_shadow",
        "prompt_delta": (
            "SCENE: Figure mid-stride walking down endless dark institutional "
            "corridor with single overhead fluorescent. Reflective floor, high "
            "contrast noir, full-body medium shot from behind-three-quarter."
        ),
    },
    "scene_08_library_pull_file": {
        "id": "scene_08_library_pull_file",
        "chapter": 1,
        "beat": "evidence",
        "variant": "amber_archive",
        "prompt_delta": (
            "SCENE: Old library reading room — figure at heavy wooden table opening "
            "thick dossier, brass desk lamp, tall bookshelves, warm amber pools of "
            "light, medium shot."
        ),
    },
    "scene_09_agency_badge_desk": {
        "id": "scene_09_agency_badge_desk",
        "chapter": 1,
        "beat": "evidence",
        "variant": "navy_uniform",
        "prompt_delta": (
            "SCENE: Figure seated at metal government desk with agency ID badge "
            "prop and stacked classified folders. Harsh overhead fluorescent, "
            "institutional green-grey walls, medium close-up."
        ),
    },
    "scene_10_newspaper_headline_1950s": {
        "id": "scene_10_newspaper_headline_1950s",
        "chapter": 1,
        "beat": "evidence",
        "variant": "pale_ghost",
        "prompt_delta": (
            "SCENE: Desaturated flashback — figure reading broadsheet newspaper "
            "with large headline space on front page. 1950s newsstand diorama, "
            "muted grey memory palette, medium shot."
        ),
    },
    "scene_11_strategy_table_folders": {
        "id": "scene_11_strategy_table_folders",
        "chapter": 1,
        "beat": "evidence",
        "variant": "red_tie_only",
        "prompt_delta": (
            "SCENE: Figure standing at long conference table covered in spread-out "
            "documents and newspaper clippings. Single warm spotlight from above, "
            "dark room edges, medium-wide power-table composition."
        ),
    },
    "scene_12_org_chart_screen": {
        "id": "scene_12_org_chart_screen",
        "chapter": 1,
        "beat": "mechanism",
        "variant": "teal_hologram",
        "prompt_delta": (
            "SCENE: Figure before large teal holographic wireframe organizational "
            "chart showing media nodes connected to central agency hub. Dark void "
            "room, UI chrome corners, reflective floor, medium-wide."
        ),
    },
    # ── Ch2: The Network (journalist infiltration) ───────────────────────────
    "scene_13_chapter_network": {
        "id": "scene_13_chapter_network",
        "chapter": 2,
        "beat": "chapter_card",
        "variant": "stamp_red_classified",
        "prompt_delta": (
            "SCENE: Figure before dark wall with bold white text THE NETWORK. "
            "Intense red classified stamp glow on figure edges, minimal environment."
        ),
    },
    "scene_14_professor_journalist": {
        "id": "scene_14_professor_journalist",
        "chapter": 2,
        "beat": "character_intro",
        "variant": "copper_bronze",
        "prompt_delta": (
            "SCENE: Figure at university office desk with tweed aesthetic, books "
            "and notepad, name plate UI space. Warm academic lighting, medium "
            "chest-up three-quarter shot."
        ),
    },
    "scene_15_corporate_newsroom": {
        "id": "scene_15_corporate_newsroom",
        "chapter": 2,
        "beat": "environment",
        "variant": "grey_suit_graphite",
        "prompt_delta": (
            "SCENE: Mid-century corporate newsroom diorama — figure at desk among "
            "empty typewriter stations and paper stacks. Cool fluorescent overhead, "
            "wide medium shot showing depth of newsroom."
        ),
    },
    "scene_16_tv_broadcast_studio": {
        "id": "scene_16_tv_broadcast_studio",
        "chapter": 2,
        "beat": "environment",
        "variant": "broadcast_amber",
        "prompt_delta": (
            "SCENE: Retro 1960s television broadcast studio — figure beside large "
            "period TV camera and desk with ON AIR light. Warm golden amber studio "
            "lights, medium-wide shot."
        ),
    },
    "scene_17_terminal_names_scroll": {
        "id": "scene_17_terminal_names_scroll",
        "chapter": 2,
        "beat": "mechanism",
        "variant": "green_matrix",
        "prompt_delta": (
            "SCENE: Figure silhouetted before green phosphor terminal screen showing "
            "scrolling list of journalist names in monospace text. Dark room, green "
            "terminal glow on figure edges, medium shot."
        ),
    },
    "scene_18_typewriter_desk": {
        "id": "scene_18_typewriter_desk",
        "chapter": 2,
        "beat": "evidence",
        "variant": "typewriter_sepia",
        "prompt_delta": (
            "SCENE: Figure seated at desk typing on vintage typewriter, paper "
            "carriage visible, coffee cup, 1950s journalist office, sepia-warm "
            "practical lamp, medium close-up from slight angle."
        ),
    },
    "scene_19_corkboard_evidence": {
        "id": "scene_19_corkboard_evidence",
        "chapter": 2,
        "beat": "evidence",
        "variant": "yellow_caution",
        "prompt_delta": (
            "SCENE: Large corkboard covered in polaroid photos of same mannequin "
            "silhouette, red string connections, red banner reading UNTIMELY DEATH "
            "style layout. Figure standing before board studying connections, "
            "medium-wide Nod Map evidence wall."
        ),
    },
    "scene_20_interview_room": {
        "id": "scene_20_interview_room",
        "chapter": 2,
        "beat": "environment",
        "variant": "purple_interrogation",
        "prompt_delta": (
            "SCENE: Sparse interrogation interview room — figure seated at metal "
            "table under single purple-tinted overhead bulb. Bare concrete walls, "
            "one-way mirror suggestion, medium shot."
        ),
    },
    "scene_21_wire_service_teletype": {
        "id": "scene_21_wire_service_teletype",
        "chapter": 2,
        "beat": "mechanism",
        "variant": "wire_press_steel",
        "prompt_delta": (
            "SCENE: Wire-service room with teletype machines and paper tape on "
            "floor. Figure reading incoming wire printout, cool steel-blue "
            "industrial lighting, medium shot."
        ),
    },
    "scene_22_newspaper_layout_table": {
        "id": "scene_22_newspaper_layout_table",
        "chapter": 2,
        "beat": "evidence",
        "variant": "newsprint_grey",
        "prompt_delta": (
            "SCENE: Figure arranging newspaper layout pages on lighted tracing "
            "table, grey newsprint aesthetic, newsroom background blur, medium "
            "overhead shot angle."
        ),
    },
    "scene_23_classified_board_crowd": {
        "id": "scene_23_classified_board_crowd",
        "chapter": 2,
        "beat": "mechanism",
        "variant": "black_silhouette",
        "prompt_delta": (
            "SCENE: Classified evidence board with mugshot-style panels and red "
            "laser ring UI element. THREE identical mannequin silhouettes in crowd "
            "formation before board — same mesh, dark void background, wide shot."
        ),
    },
    "scene_24_four_hundred_card": {
        "id": "scene_24_four_hundred_card",
        "chapter": 2,
        "beat": "number_card",
        "variant": "forensic_red",
        "prompt_delta": (
            "SCENE: Figure standing in dark void beside massive floating UI number "
            "card reading 400 with subtitle JOURNALISTS below in clean sans-serif. "
            "Forensic red rim light, minimal environment, medium-wide."
        ),
    },
    # ── Ch3: Exposure & Legacy (Church Committee → today) ───────────────────
    "scene_25_chapter_declassified": {
        "id": "scene_25_chapter_declassified",
        "chapter": 3,
        "beat": "chapter_card",
        "variant": "orange_warning",
        "prompt_delta": (
            "SCENE: Figure before dark wall with bold white text DECLASSIFIED. "
            "Orange caution accent rim light, hazard-tape aesthetic subtle on edges."
        ),
    },
    "scene_26_congress_hearing": {
        "id": "scene_26_congress_hearing",
        "chapter": 3,
        "beat": "environment",
        "variant": "olive_military",
        "prompt_delta": (
            "SCENE: 1970s congressional hearing room diorama — figure at witness "
            "table with gooseneck microphones, wood panel walls, warm overhead "
            "lights, medium-wide institutional shot."
        ),
    },
    "scene_27_church_committee_files": {
        "id": "scene_27_church_committee_files",
        "chapter": 3,
        "beat": "evidence",
        "variant": "cold_blue_clinical",
        "prompt_delta": (
            "SCENE: Figure at table with towering stacks of Church Committee "
            "hearing binders and declassified report covers. Cold clinical blue "
            "fluorescent light, medium shot."
        ),
    },
    "scene_28_modern_laptop_leak": {
        "id": "scene_28_modern_laptop_leak",
        "chapter": 3,
        "beat": "modern_beat",
        "variant": "white_hoodie",
        "prompt_delta": (
            "SCENE: Contemporary dim room — figure at desk with laptop showing "
            "redacted document PDF glow on faceless head rim. Cool monitor light "
            "only, whistleblower aesthetic, medium close-up."
        ),
    },
    "scene_29_capitol_steps_walk": {
        "id": "scene_29_capitol_steps_walk",
        "chapter": 3,
        "beat": "transition",
        "variant": "white_trench",
        "prompt_delta": (
            "SCENE: Figure mid-stride ascending wide stone government steps "
            "outdoors, overcast daylight, beige trench coat, medium full-body shot "
            "from low angle."
        ),
    },
    "scene_30_world_map_ops": {
        "id": "scene_30_world_map_ops",
        "chapter": 3,
        "beat": "mechanism",
        "variant": "sand_desert",
        "prompt_delta": (
            "SCENE: Figure before large world map wall with pins on multiple "
            "continents connected by string. Warm khaki-toned war-room lighting, "
            "medium-wide geopolitical briefing room."
        ),
    },
    "scene_31_modern_newsroom_night": {
        "id": "scene_31_modern_newsroom_night",
        "chapter": 3,
        "beat": "legacy",
        "variant": "pink_neon",
        "prompt_delta": (
            "SCENE: Modern glass newsroom at night — figure at desk with city "
            "lights through windows, subtle magenta neon rim from signage, medium "
            "shot showing monitors and empty desks."
        ),
    },
    "scene_32_cold_vault_archive": {
        "id": "scene_32_cold_vault_archive",
        "chapter": 3,
        "beat": "evidence",
        "variant": "cyan_frost",
        "prompt_delta": (
            "SCENE: Cold document vault with rolling archive shelves — figure "
            "pulling boxed records, icy cyan fluorescent, breath-cold atmosphere, "
            "medium shot down aisle."
        ),
    },
    "scene_33_stamped_hazard_files": {
        "id": "scene_33_stamped_hazard_files",
        "chapter": 3,
        "beat": "evidence",
        "variant": "orange_warning",
        "prompt_delta": (
            "SCENE: Figure stamping CONFIDENTIAL on manila folder at industrial "
            "processing table, stacks of identical folders, orange warning accent "
            "light, medium close-up on hands and stamp."
        ),
    },
    "scene_34_minimal_single_document": {
        "id": "scene_34_minimal_single_document",
        "chapter": 3,
        "beat": "pause",
        "variant": "redacted_black",
        "prompt_delta": (
            "SCENE: Pure black void — single figure holding one glowing redacted "
            "document at chest height. Minimal composition, high contrast, medium "
            "shot centered."
        ),
    },
    "scene_35_return_desk_mirror": {
        "id": "scene_35_return_desk_mirror",
        "chapter": 3,
        "beat": "callback",
        "variant": "white_suit_default",
        "prompt_delta": (
            "SCENE: Callback to cold open — same black reflective table and manila "
            "folder but folder now closed with DECLASSIFIED stamp visible. Figure "
            "standing back from table, medium-wide, forensic red edge glow."
        ),
    },
    "scene_36_end_tease_desk": {
        "id": "scene_36_end_tease_desk",
        "chapter": 3,
        "beat": "outro",
        "variant": "forensic_red",
        "prompt_delta": (
            "SCENE: Figure at desk placing new manila folder labeled NEXT CASE "
            "FILE in bold readable text on tab. Dark void, red rim light, medium "
            "shot, space on right third for end-screen overlay."
        ),
    },
}
