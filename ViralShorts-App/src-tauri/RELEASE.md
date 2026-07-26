# NYPTID Studio desktop release

## 1.0.2 trust cutover

Version 1.0.2 moves the desktop API and updater endpoint to
`https://api-studio.nyptidindustries.com` and deliberately starts a
Studio-specific updater trust chain.

Versions 1.0.0 and 1.0.1 trust the retired key and/or Fly updater endpoint.
They cannot auto-update to 1.0.2 and require one manual installer. Version
1.0.2 and later can use signed in-app updates through the canonical API.

## Signing custody

- Private key: `%USERPROFILE%\.tauri\nyptid-studio-updater.key`
- Password credential target:
  `tauri-updater-signing-password.com.nyptidindustries.studio.release`
- Embedded public-key document SHA-256:
  `7936caeec9b98979ccd1deebf7627d8234c633815c2c215eb8a5ef9a7342301b`

The private key is outside the repository and its Windows ACL is restricted to
the current user and SYSTEM. The password is stored only in Windows Credential
Manager. Never print either value or place either in source, logs, shell
history, CI variables, or the backend.

Before publishing 1.0.2, create two separate encrypted recovery copies: one for
the private-key file and one for the password. Losing either makes future
in-app updates impossible; replacing the key would require another manual
installer trust cutover.

## Backend artifact contract

The backend publishes exactly these three files (for the current version
`X.Y.Z`) from its `studio_releases` directory
(`/opt/studio/data/studio_releases/` on Contabo, mounted as
`/var/data/studio_releases` in the container):

- `NYPTID-Studio_X.Y.Z_x64-setup.exe`
- `NYPTID-Studio_X.Y.Z_x64-setup.exe.sig`
- `NYPTID-Studio_X.Y.Z_x64-setup.exe.sha256`

The `.sig` file is Tauri's base64-encoded Minisign signature. The `.sha256`
file contains the lowercase SHA-256 of the installer followed by a newline.
Do not stage or publish any installer until
`tests/updater_release.rs` verifies its signature against the public key
embedded in `tauri.conf.json`.

## Publishing a release (keep the desktop in sync with web)

The desktop app carries its own reviewed, signed UI, so it only stays current
if a matching signed build is published on every release. Missing this step is
exactly why 1.0.2 went stale relative to web. Do this each release:

1. Bump the version in all three places to the same `X.Y.Z` and commit:
   - `ViralShorts-App/src-tauri/tauri.conf.json` (`version`)
   - `ViralShorts-App/src-tauri/Cargo.toml` (`version`)
   - `backend.py` (`DESKTOP_RELEASE_VERSION`, and refresh `DESKTOP_RELEASE_NOTES`)
2. Build, sign, verify, and publish the installer to Contabo:
   ```powershell
   pwsh ops/release_desktop.ps1
   ```
   The script refuses to run if the three versions disagree, reads the signing
   key + password locally (key file + Windows Credential Manager — never CI,
   logs, or source), verifies the signature via `cargo test updater_release`,
   and uploads the three contract files over the pinned `cliplab-vps` SSH alias.
3. Deploy the backend so it advertises the new version (artifacts are already in
   place, so there is no 404/204 window):
   ```powershell
   pwsh ops/deploy_studio_agent.ps1
   ```
4. Verify `https://api-studio.nyptidindustries.com/api/desktop/releases/latest`
   shows the new version with `available: true`. Installed 1.0.2+ clients then
   auto-update on next launch (`installMode: passive`).
