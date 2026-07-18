from studio_agent import store


ONE_SCENE_PROOF_COMMIT = (
    'yes make it \N{EM DASH} render that plan for "1 scene first so i can see what it looks like, '
    'and yes we keep the skeleton-anatomy visual style", only 30 seconds'
)


def test_one_scene_first_commit_is_not_routed_as_existing_proof_expansion() -> None:
    assert not store.is_expand_short_request(ONE_SCENE_PROOF_COMMIT)
    assert store.is_hard_production_commit(ONE_SCENE_PROOF_COMMIT)
    assert store._is_visual_proof_request(ONE_SCENE_PROOF_COMMIT)


def test_scene_one_with_explicit_expansion_language_still_routes_to_expand() -> None:
    for prompt in (
        "Scene 1 looks good. Finish the short at 30 seconds.",
        "Keep scene one and make the remaining scenes for a 30 second video.",
        "I approve scene 1. Make the other five scenes.",
    ):
        assert store.is_expand_short_request(prompt)
        assert not store.is_hard_production_commit(prompt)
