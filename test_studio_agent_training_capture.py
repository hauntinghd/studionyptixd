import json
import tempfile
from pathlib import Path

from studio_agent import training_capture as tc


def _use_temp_root(root: Path) -> None:
    tc.CAPTURE_ROOT = root
    tc.CONSENT_DIR = root / "consent"
    tc.OUTBOX_DIR = root / "outbox"
    tc.DATASET_DIR = root / "datasets"
    tc.DELETION_DIR = root / "deletions"
    tc.COMPILER_STATE = root / "compiler_state.json"
    tc._sync_consent_supabase = lambda payload: None
    tc._sync_event_supabase = lambda payload: None


def test_capture_requires_explicit_opt_in():
    with tempfile.TemporaryDirectory() as tmp:
        _use_temp_root(Path(tmp))
        assert tc.capture_event("user-a", "user_turn", {"text": "hello"}) == ""
        tc.set_consent("user-a", training_opt_in=True)
        event_id = tc.capture_event("user-a", "user_turn", {"text": "hello"})
        assert event_id.startswith("te_")


def test_redacts_secrets_and_quarantines_youtube_data():
    with tempfile.TemporaryDirectory() as tmp:
        _use_temp_root(Path(tmp))
        tc.set_consent("user-b", training_opt_in=True)
        event_id = tc.capture_event(
            "user-b",
            "tool_call",
            {
                "authorization": "Bearer secret",
                "email": "person@example.com",
                "source": "youtube_analytics_live",
            },
        )
        row = json.loads((tc.OUTBOX_DIR / "user-b" / f"{event_id}.json").read_text(encoding="utf-8"))
        assert row["payload"]["authorization"] == "[REDACTED_SECRET]"
        assert row["payload"]["email"] == "[REDACTED_PII]"
        assert row["youtube_authorized_data"] is True
        assert row["trainable"] is False


def test_compiler_excludes_youtube_and_delete_removes_user_rows():
    with tempfile.TemporaryDirectory() as tmp:
        _use_temp_root(Path(tmp))
        tc.set_consent("user-c", training_opt_in=True)
        tc.capture_event("user-c", "user_turn", {"text": "make a video"})
        tc.capture_event("user-c", "tool_call", {"source": "youtube_analytics_live"})
        state = tc.compile_dataset()
        assert state["last_compiled_rows"] == 1
        assert state["last_quarantined_rows"] == 1
        dataset = next(tc.DATASET_DIR.glob("*.jsonl"))
        rows = [json.loads(line) for line in dataset.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 1
        assert rows[0]["payload"]["text"] == "make a video"
        deleted = tc.delete_user_training_data("user-c")
        assert deleted["deleted_rows"] >= 1
        assert dataset.read_text(encoding="utf-8") == ""


if __name__ == "__main__":
    test_capture_requires_explicit_opt_in()
    test_redacts_secrets_and_quarantines_youtube_data()
    test_compiler_excludes_youtube_and_delete_removes_user_rows()
    print("training capture tests passed")
