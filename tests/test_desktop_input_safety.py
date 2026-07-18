from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TAURI_ROOT = ROOT / "ViralShorts-App" / "src-tauri"
WINDOW_CONFIG = json.loads((TAURI_ROOT / "tauri.conf.json").read_text(encoding="utf-8"))
CAPABILITY = json.loads(
    (TAURI_ROOT / "capabilities" / "default.json").read_text(encoding="utf-8")
)


def test_desktop_window_cannot_become_an_invisible_input_overlay() -> None:
    [window] = WINDOW_CONFIG["app"]["windows"]

    assert window["fullscreen"] is False
    assert window["transparent"] is False
    assert window["alwaysOnTop"] is False
    assert window["focusable"] is True
    assert window["decorations"] is True
    assert window["visible"] is True
    assert window["skipTaskbar"] is False
    assert window["contentProtected"] is False
    assert window["dragDropEnabled"] is False

    serialized = json.dumps(WINDOW_CONFIG).lower()
    assert "--disable-gpu" not in serialized
    assert "mswebview2browserhittransparent" not in serialized


def test_native_cursor_and_click_through_mutations_are_explicitly_denied() -> None:
    permissions = set(CAPABILITY["permissions"])
    dangerous_suffixes = {
        "set-always-on-bottom",
        "set-always-on-top",
        "set-cursor-grab",
        "set-cursor-position",
        "set-cursor-visible",
        "set-ignore-cursor-events",
    }

    for suffix in dangerous_suffixes:
        assert f"core:window:allow-{suffix}" not in permissions
        assert f"core:window:deny-{suffix}" in permissions


def test_desktop_runtime_has_no_global_input_or_hid_apis() -> None:
    source_roots = (ROOT / "ViralShorts-App" / "src", TAURI_ROOT / "src")
    source = "\n".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for source_root in source_roots
        for path in source_root.rglob("*")
        if path.suffix in {".rs", ".ts", ".tsx", ".js", ".jsx"}
    )
    prohibited = (
        "requestPointerLock(",
        "SetWindowsHookEx",
        "RegisterRawInputDevices",
        "ClipCursor",
        "BlockInput",
        "SetCursorPos",
    )

    for api in prohibited:
        assert api not in source


def test_windows_runtime_filters_raw_devices_and_heals_only_its_stale_capture() -> None:
    source = (TAURI_ROOT / "src" / "lib.rs").read_text(encoding="utf-8")

    assert ".device_event_filter(tauri::DeviceEventFilter::Always)" in source
    assert "GetCapture" in source
    assert "ReleaseCapture" in source
    assert "GetAsyncKeyState" in source
    assert "VK_XBUTTON1" in source
    assert "VK_XBUTTON2" in source
    assert "GetCapture() } == hwnd && !windows_mouse_button_is_down()" in source

    cargo = (TAURI_ROOT / "Cargo.toml").read_text(encoding="utf-8")
    assert "Win32_UI_Input_KeyboardAndMouse" in cargo


def test_scene_inspector_releases_local_pointer_capture_on_every_exit_path() -> None:
    source = (
        ROOT
        / "ViralShorts-App"
        / "src"
        / "studio"
        / "components"
        / "agent"
        / "AgentJobDeliverable.tsx"
    ).read_text(encoding="utf-8")

    assert "!event.isPrimary || event.button !== 0" in source
    assert "releasePointerCapture" in source
    assert "onPointerCancel" in source
    assert "onLostPointerCapture" in source
    assert "window.addEventListener('blur'" in source
    assert "document.addEventListener('visibilitychange'" in source
