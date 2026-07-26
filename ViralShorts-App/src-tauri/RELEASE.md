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

The backend publishes exactly these three files from its `studio_releases`
directory:

- `NYPTID-Studio_1.0.2_x64-setup.exe`
- `NYPTID-Studio_1.0.2_x64-setup.exe.sig`
- `NYPTID-Studio_1.0.2_x64-setup.exe.sha256`

The `.sig` file is Tauri's base64-encoded Minisign signature. The `.sha256`
file contains the lowercase SHA-256 of the installer followed by a newline.
Do not stage or publish any installer until
`tests/updater_release.rs` verifies its signature against the public key
embedded in `tauri.conf.json`.
