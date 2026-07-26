<#
.SYNOPSIS
  Build, sign, verify, and publish one NYPTID Studio desktop release so the
  installed app's built-in auto-updater brings every user in sync with web.

.DESCRIPTION
  This is the missing automation that let the desktop app go stale: web shipped
  a fix, but no matching signed desktop build was ever produced, so the updater
  had nothing newer to hand out.

  Run this whenever you cut a release (after bumping the version in
  tauri.conf.json / Cargo.toml / backend.py DESKTOP_RELEASE_VERSION and
  committing). It:

    1. Refuses to run unless the three version strings agree (drift guard).
    2. Builds the frontend + signed Tauri NSIS installer using the LOCAL
       updater key. The key and its password are read at runtime from your
       machine (key file + Windows Credential Manager) and are NEVER printed,
       committed, or placed in CI variables — matching src-tauri/RELEASE.md.
    3. Computes the .sha256 sidecar and verifies the Minisign signature against
       the public key embedded in tauri.conf.json (cargo test updater_release).
    4. Renames the artifacts to the exact backend contract filenames.
    5. Publishes the 3 files to Contabo /opt/studio/data/studio_releases/ over
       the pinned SSH alias, then re-verifies the SHA-256 on the server.

  It deliberately does NOT bump versions, commit, or deploy the backend. Do the
  version bump + commit first, run this, then deploy the backend the normal way
  (ops/deploy_studio_agent.ps1). Ordering matters: artifacts must reach Contabo
  BEFORE the backend advertises the new version, so publish (this script) then
  deploy.

.EXAMPLE
  # 1) bump version in the 3 files and commit
  # 2) publish the signed installer to Contabo:
  pwsh ops/release_desktop.ps1
  # 3) deploy the backend so it advertises the new version:
  pwsh ops/deploy_studio_agent.ps1
#>
[CmdletBinding()]
param(
    [string]$Version,
    [string]$SshAlias = "cliplab-vps",
    [string]$SshConfig = "$env:USERPROFILE\.ssh\cliplab_vps_config",
    [string]$RemoteReleaseDir = "/opt/studio/data/studio_releases",
    [string]$KeyPath = "$env:USERPROFILE\.tauri\nyptid-studio-updater.key",
    [string]$PasswordCredentialTarget = "tauri-updater-signing-password.com.nyptidindustries.studio.release",
    [switch]$SkipUpload,
    [switch]$KeepStaging
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$App = Join-Path $Root "ViralShorts-App"
$SrcTauri = Join-Path $App "src-tauri"

function Assert-Native([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed with exit code $LASTEXITCODE" }
}

function Get-JsonVersion([string]$Path) {
    (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json).version
}

# --- 1) Version drift guard -------------------------------------------------
$tauriConf = Join-Path $SrcTauri "tauri.conf.json"
$cargoToml = Join-Path $SrcTauri "Cargo.toml"
$backendPy = Join-Path $Root "backend.py"

$tauriVersion = Get-JsonVersion $tauriConf
$cargoVersion = (Select-String -LiteralPath $cargoToml -Pattern '^version\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"' |
    Select-Object -First 1).Matches[0].Groups[1].Value
$backendVersion = (Select-String -LiteralPath $backendPy -Pattern 'DESKTOP_RELEASE_VERSION\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"' |
    Select-Object -First 1).Matches[0].Groups[1].Value

if (-not $Version) { $Version = $tauriVersion }
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+$') { throw "Refusing: version '$Version' is not X.Y.Z" }
if ($tauriVersion -ne $Version -or $cargoVersion -ne $Version -or $backendVersion -ne $Version) {
    throw ("Version drift. tauri.conf.json=$tauriVersion, Cargo.toml=$cargoVersion, backend.py=$backendVersion, requested=$Version. " +
        "Bump all three to the same value and commit before publishing.")
}
Write-Host "==> Releasing NYPTID Studio desktop $Version" -ForegroundColor Cyan

if (-not (Test-Path -LiteralPath $KeyPath)) { throw "Updater private key not found: $KeyPath" }

# --- 2) Read signing password from Windows Credential Manager (never printed) -
Add-Type -Namespace Studio -Name Cred -MemberDefinition @'
[DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
private static extern bool CredReadW(string target, int type, int flags, out IntPtr credentialPtr);
[DllImport("advapi32.dll")] private static extern void CredFree(IntPtr cred);
[System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
private struct CREDENTIAL { public int Flags, Type; public IntPtr TargetName, Comment;
  public System.Runtime.InteropServices.ComTypes.FILETIME LastWritten; public int CredentialBlobSize;
  public IntPtr CredentialBlob; public int Persist, AttributeCount; public IntPtr Attributes, TargetAlias, UserName; }
public static string Read(string target) {
  IntPtr p; if (!CredReadW(target, 1, 0, out p)) throw new System.Exception("Credential '"+target+"' not found in Credential Manager");
  try { var c=(CREDENTIAL)System.Runtime.InteropServices.Marshal.PtrToStructure(p, typeof(CREDENTIAL));
    return System.Runtime.InteropServices.Marshal.PtrToStringUni(c.CredentialBlob, c.CredentialBlobSize/2); }
  finally { CredFree(p); }
}
'@
$signingPassword = [Studio.Cred]::Read($PasswordCredentialTarget)
if ([string]::IsNullOrWhiteSpace($signingPassword)) { throw "Empty signing password credential" }

# --- 3) Build + sign the NSIS installer -------------------------------------
$nsisDir = Join-Path $SrcTauri "target\release\bundle\nsis"
if (Test-Path $nsisDir) { Get-ChildItem $nsisDir -Filter *.exe -ErrorAction SilentlyContinue | Remove-Item -Force }

Push-Location $App
try {
    $env:TAURI_SIGNING_PRIVATE_KEY = (Get-Content -LiteralPath $KeyPath -Raw)
    $env:TAURI_SIGNING_PRIVATE_KEY_PASSWORD = $signingPassword
    Write-Host "==> tauri build --bundles nsis (frontend build runs via beforeBuildCommand)"
    & npx.cmd --no-install tauri build --bundles nsis
    Assert-Native "tauri build"
} finally {
    $env:TAURI_SIGNING_PRIVATE_KEY = $null
    $env:TAURI_SIGNING_PRIVATE_KEY_PASSWORD = $null
    $signingPassword = $null
    Pop-Location
}

$builtExe = Get-ChildItem $nsisDir -Filter *_x64-setup.exe | Sort-Object LastWriteTime | Select-Object -Last 1
if (-not $builtExe) { throw "No NSIS installer produced in $nsisDir" }
$builtSig = "$($builtExe.FullName).sig"
if (-not (Test-Path $builtSig)) { throw "Updater signature missing: $builtSig (is createUpdaterArtifacts true?)" }

# --- 4) Verify signature against the embedded public key --------------------
$env:NYPTID_UPDATER_VERIFY_ARTIFACT = $builtExe.FullName
$env:NYPTID_UPDATER_VERIFY_SIGNATURE = $builtSig
Push-Location $SrcTauri
try {
    & cargo test --locked --test updater_release
    Assert-Native "updater signature verification (cargo test updater_release)"
} finally {
    $env:NYPTID_UPDATER_VERIFY_ARTIFACT = $null
    $env:NYPTID_UPDATER_VERIFY_SIGNATURE = $null
    Pop-Location
}

# --- 5) Assemble the exact backend contract artifacts -----------------------
$stage = Join-Path $env:TEMP "studio-desktop-release\$Version"
if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
New-Item -ItemType Directory -Path $stage -Force | Out-Null

$contractName = "NYPTID-Studio_${Version}_x64-setup.exe"
$exeOut = Join-Path $stage $contractName
$sigOut = "$exeOut.sig"
$shaOut = "$exeOut.sha256"
Copy-Item $builtExe.FullName $exeOut -Force
Copy-Item $builtSig $sigOut -Force
$sha = (Get-FileHash -LiteralPath $exeOut -Algorithm SHA256).Hash.ToLower()
# lowercase sha + trailing newline, no BOM (backend reads it as plain text)
[System.IO.File]::WriteAllText($shaOut, "$sha`n", (New-Object System.Text.UTF8Encoding($false)))

Write-Host "==> Signed artifact ready:"
Write-Host "    $contractName  ($([math]::Round($builtExe.Length/1MB,2)) MB)"
Write-Host "    sha256 $sha"

if ($SkipUpload) {
    Write-Host "-SkipUpload set. Artifacts staged locally at:`n    $stage" -ForegroundColor Yellow
    return
}

# --- 6) Publish to Contabo over the pinned SSH alias ------------------------
if (-not (Test-Path -LiteralPath $SshConfig)) { throw "SSH config not found: $SshConfig" }
$sshArgs = @("-F", $SshConfig, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", $SshAlias)
$remoteStaging = "studio-desktop-release-$Version"

Write-Host "==> Uploading to $SshAlias staging (~/$remoteStaging)"
& ssh @sshArgs "rm -rf `"$remoteStaging`" && mkdir -p `"$remoteStaging`""
Assert-Native "prepare remote staging"
& scp -F $SshConfig -o BatchMode=yes $exeOut $sigOut $shaOut "${SshAlias}:$remoteStaging/"
Assert-Native "scp artifacts"

$publishScript = @"
set -Eeuo pipefail
as_root(){ if [ "`$(id -u)" -eq 0 ]; then "`$@"; else sudo -n "`$@"; fi; }
stg="`$HOME/$remoteStaging"
dst="$RemoteReleaseDir"
exe="$contractName"
# Verify the signed installer's SHA-256 on the server before publishing.
cd "`$stg"
sha256sum -c "`$exe.sha256"
as_root install -d -m 0755 "`$dst"
as_root install -m 0644 "`$stg/`$exe" "`$dst/`$exe"
as_root install -m 0644 "`$stg/`$exe.sig" "`$dst/`$exe.sig"
as_root install -m 0644 "`$stg/`$exe.sha256" "`$dst/`$exe.sha256"
$( if (-not $KeepStaging) { 'rm -rf "$stg"' } )
echo "PUBLISHED `$dst/`$exe"
"@
$publishScript | & ssh @sshArgs "bash -s"
Assert-Native "publish to $RemoteReleaseDir"

if (-not $KeepStaging) { Remove-Item $stage -Recurse -Force }

Write-Host ""
Write-Host "Desktop $Version published to Contabo." -ForegroundColor Green
Write-Host "Next:" -ForegroundColor Green
Write-Host "  1. Ensure the version bump is committed."
Write-Host "  2. Deploy the backend so it advertises ${Version}:"
Write-Host "       pwsh ops/deploy_studio_agent.ps1"
Write-Host "  3. Verify:  https://api-studio.nyptidindustries.com/api/desktop/releases/latest"
Write-Host "     should show version $Version and available:true."
Write-Host "  Installed 1.0.2+ apps then auto-update to $Version on next launch."
