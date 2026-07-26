param(
    [switch]$SkipTests,
    [switch]$DeployWorker,
    [switch]$DeployVercel,
    [string]$VercelProject = "studio-frontend-asd",
    [string]$VercelScope = "nyptids-projects",
    [string]$SshAlias = "cliplab-vps",
    [string]$SshConfig = "$env:USERPROFILE\.ssh\cliplab_vps_config"
)

# Build and deploy one immutable Studio candidate. A dirty worktree is refused
# because its files cannot be tied to the commit reported by /api/health.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

function Find-CommandPath {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string[]]$Candidates = @()
    )
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($p in $Candidates) {
        if (Test-Path $p) { return $p }
    }
    return $null
}

function Assert-NativeSuccess {
    param([Parameter(Mandatory = $true)][string]$Step)
    if ($LASTEXITCODE -ne 0) {
        throw "$Step failed with exit code $LASTEXITCODE"
    }
}

$git = Find-CommandPath "git"
if (-not $git) { throw "git is required to create a traceable release candidate" }

$dirty = @(& $git -c safe.directory=$($Root.Replace('\', '/')) status --porcelain)
Assert-NativeSuccess "git status"
if ($dirty.Count -gt 0) {
    throw "Refusing to deploy an uncommitted candidate. Commit the exact tested files first."
}

$gitSha = (& $git -c safe.directory=$($Root.Replace('\', '/')) rev-parse HEAD).Trim()
Assert-NativeSuccess "git rev-parse"
if ($gitSha -notmatch '^[0-9a-f]{40}$') { throw "Could not resolve a full Git SHA" }
$shortSha = $gitSha.Substring(0, 12)
$commitEpochRaw = (& $git -c safe.directory=$($Root.Replace('\', '/')) show -s --format=%ct $gitSha).Trim()
Assert-NativeSuccess "git commit timestamp"
if ($commitEpochRaw -notmatch '^[0-9]+$') { throw "Could not resolve the release commit timestamp" }
$commitUtc = [DateTimeOffset]::FromUnixTimeSeconds([int64]$commitEpochRaw).UtcDateTime
$buildId = "studio-{0}-{1}" -f $commitUtc.ToString('yyyyMMddTHHmmssZ'), $shortSha
$repoUrl = (& $git -c safe.directory=$($Root.Replace('\', '/')) remote get-url origin).Trim()
Assert-NativeSuccess "git origin URL"
if ($repoUrl -notmatch '^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:\.git)?$') {
    throw "The Contabo release fetch requires a public HTTPS GitHub origin URL"
}
if (-not $repoUrl.EndsWith(".git", [StringComparison]::OrdinalIgnoreCase)) {
    $repoUrl = "$repoUrl.git"
}

if (-not $SkipTests) {
    $uv = Find-CommandPath "uv" @(
        "$env:LOCALAPPDATA\hermes\bin\uv.exe",
        "$env:USERPROFILE\.local\bin\uv.exe"
    )
    if (-not $uv) {
        throw "uv is required to run release tests on production Python 3.11"
    }
    $testManifest = Join-Path $Root "ops\release_backend_tests.txt"
    if (-not (Test-Path -LiteralPath $testManifest -PathType Leaf)) {
        throw "Release test manifest is missing: $testManifest"
    }
    $tests = @(
        Get-Content -LiteralPath $testManifest -Encoding utf8 |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ -and -not $_.StartsWith("#") }
    )
    if ($tests.Count -eq 0) {
        throw "Release test manifest is empty"
    }
    foreach ($testPath in $tests) {
        if (-not (Test-Path -LiteralPath (Join-Path $Root $testPath))) {
            throw "Mandatory release test is missing: $testPath"
        }
    }
    $env:PYTHONDONTWRITEBYTECODE = "1"
    $env:STUDIO_RUNPOD_PRODUCTION_ENABLED = "0"
    $env:FAL_AI_KEY = ""
    $env:FAL_KEY = ""
    $env:XAI_API_KEY = ""
    & $uv run --python 3.11 python -m pytest -q @tests
    Assert-NativeSuccess "provider-free release tests"

    Push-Location (Join-Path $Root "ViralShorts-App")
    try {
        & npm.cmd run build
        Assert-NativeSuccess "frontend production build"
    } finally {
        Pop-Location
    }

    Push-Location (Join-Path $Root "runpod-serverless")
    try {
        & node --test worker-proxy.test.mjs
        Assert-NativeSuccess "Cloudflare-to-Contabo streaming ingress tests"
    } finally {
        Pop-Location
    }
}

$dirtyAfterTests = @(& $git -c safe.directory=$($Root.Replace('\', '/')) status --porcelain)
Assert-NativeSuccess "post-test git status"
if ($dirtyAfterTests.Count -gt 0) {
    throw "Release tests changed tracked source. Refusing to deploy a candidate that no longer matches $gitSha."
}

$ssh = Find-CommandPath "ssh"
if (-not $ssh) { throw "OpenSSH is required for the Contabo backend deployment" }
if (-not (Test-Path -LiteralPath $SshConfig -PathType Leaf)) {
    throw "SSH config was not found: $SshConfig"
}
if ($SshAlias -notmatch '^[A-Za-z0-9_.-]+$') { throw "SSH alias is invalid" }
$sshArgs = @(
    "-F", $SshConfig,
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=yes",
    $SshAlias
)

Write-Host "==> Testing pinned Contabo SSH target $SshAlias"
& $ssh @sshArgs 'if [ "$(id -u)" -eq 0 ]; then true; else sudo -n true; fi'
Assert-NativeSuccess "Contabo SSH readiness"

$activeProbe = @'
set -Eeuo pipefail
as_root() {
  if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo -n "$@"; fi
}
if as_root test -L /opt/studio/shared/active.env; then
  printf '__STUDIO_ACTIVE__=1\n'
else
  printf '__STUDIO_ACTIVE__=0\n'
fi
'@
$activeOutput = @($activeProbe | & $ssh @sshArgs "bash -s")
Assert-NativeSuccess "Contabo ownership probe"
$hadActive = $activeOutput -contains "__STUDIO_ACTIVE__=1"

$remoteStageScript = @'
set -Eeuo pipefail
sha="$1"
build_id="$2"
repository_url="$3"
repo_dir="/opt/studio/repo.git"
release_dir="/opt/studio/releases/${sha}"

as_root() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo -n "$@"
  fi
}
die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

[[ "${sha}" =~ ^[0-9a-f]{40}$ ]] || die "invalid release SHA"
[[ "${build_id}" =~ ^studio-[0-9]{8}T[0-9]{6}Z-${sha:0:12}$ ]] ||
  die "build ID does not match release SHA"
[[ "${repository_url}" =~ ^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\.git$ ]] ||
  die "unexpected repository URL"

as_root install -d -m 700 /opt/studio /opt/studio/releases
if ! as_root test -d "${repo_dir}"; then
  as_root git init --bare "${repo_dir}"
fi
[[ "$(as_root git --git-dir="${repo_dir}" rev-parse --is-bare-repository)" == "true" ]] ||
  die "${repo_dir} is not a bare repository"
as_root git --git-dir="${repo_dir}" fetch \
  --force \
  --no-tags \
  "${repository_url}" \
  "${sha}:refs/releases/${sha}"
fetched_sha="$(as_root git --git-dir="${repo_dir}" rev-parse "refs/releases/${sha}^{commit}")"
[[ "${fetched_sha}" == "${sha}" ]] || die "remote fetch did not resolve the exact release"

if as_root test -e "${release_dir}"; then
  [[ "$(as_root git -C "${release_dir}" rev-parse HEAD)" == "${sha}" ]] ||
    die "existing release directory points at another commit"
  [[ -z "$(as_root git -C "${release_dir}" status --porcelain)" ]] ||
    die "existing release directory is dirty"
else
  as_root git --git-dir="${repo_dir}" worktree add \
    --detach \
    "${release_dir}" \
    "refs/releases/${sha}"
fi

as_root bash "${release_dir}/ops/contabo/prepare_host.sh"
as_root bash "${release_dir}/ops/contabo/deploy.sh" stage --build-id "${build_id}"
as_root chmod -R a-w "${release_dir}"
'@

Write-Host "==> Staging immutable Contabo candidate $buildId ($gitSha)"
$stageCommand = "bash -s -- '$gitSha' '$buildId' '$repoUrl'"
$remoteStageScript | & $ssh @sshArgs $stageCommand
Assert-NativeSuccess "Contabo release stage"

if (-not $hadActive) {
    Write-Warning "No Contabo active.env existed. Candidate is staged only; first activation remains manually fenced."
    if ($DeployWorker -or $DeployVercel) {
        throw "Downstream deployment was requested, but the first Contabo cutover has not been manually activated."
    }
    Write-Host "Staged candidate: $buildId"
    return
}

$remoteActivateScript = @'
set -Eeuo pipefail
sha="$1"
build_id="$2"
release_dir="/opt/studio/releases/${sha}"
candidate="/opt/studio/shared/candidates/${build_id}.env"
as_root() {
  if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo -n "$@"; fi
}
as_root test -L /opt/studio/shared/active.env
as_root bash "${release_dir}/ops/contabo/deploy.sh" \
  activate \
  --candidate "${candidate}"
'@
$activateCommand = "bash -s -- '$gitSha' '$buildId'"
$remoteActivateScript | & $ssh @sshArgs $activateCommand
Assert-NativeSuccess "Contabo release activation"

$healthUrl = "https://api-studio.nyptidindustries.com/api/health"
$verified = $false
for ($attempt = 1; $attempt -le 60; $attempt++) {
    try {
        $health = Invoke-RestMethod -Uri $healthUrl -Method Get -TimeoutSec 20
        if (
            [string]$health.backend_commit -eq $gitSha -and
            [string]$health.frontend_bundle -eq $buildId -and
            [string]$health.release_id -eq $buildId -and
            [string]$health.deployment_target -eq "contabo" -and
            [string]$health.status -eq "online"
        ) {
            $verified = $true
            break
        }
    } catch {
        # The canonical edge may still be converging on the activated release.
    }
    Start-Sleep -Seconds 5
}
if (-not $verified) {
    throw "Canonical Studio health did not match Contabo commit $gitSha and build $buildId."
}
Write-Host "==> Contabo provenance verified: $gitSha / $buildId"

if ($DeployWorker) {
    $wrangler = Find-CommandPath "wrangler.cmd"
    if (-not $wrangler) { $wrangler = Find-CommandPath "wrangler" }
    if (-not $wrangler) { throw "wrangler is required when -DeployWorker is selected" }
    Push-Location (Join-Path $Root "runpod-serverless")
    try {
        & $wrangler deploy
        Assert-NativeSuccess "Cloudflare worker deploy"
    } finally {
        Pop-Location
    }
}

if ($DeployVercel) {
    $vercel = Find-CommandPath "vercel.cmd"
    if (-not $vercel) { $vercel = Find-CommandPath "vercel" }
    if (-not $vercel) { throw "vercel is required when -DeployVercel is selected" }
    # The Vercel project itself declares ViralShorts-App as its root. Link and
    # deploy from the repository root so that project setting remains valid;
    # .vercelignore limits the upload to the actual web application.
    Push-Location $Root
    try {
        & $vercel link --yes --scope $VercelScope --project $VercelProject
        Assert-NativeSuccess "Vercel project link"
        & $vercel --prod --yes --scope $VercelScope --build-env "VITE_STUDIO_BUILD_ID=$buildId"
        Assert-NativeSuccess "Vercel deploy"

        $studioUrl = "https://studio.nyptidindustries.com"
        $remoteIndex = (Invoke-WebRequest -UseBasicParsing -Uri "$studioUrl/?candidate=$buildId" -Headers @{ "Cache-Control" = "no-cache" } -TimeoutSec 30).Content
        $assetMatch = [regex]::Match($remoteIndex, 'assets/index-[A-Za-z0-9_-]+\.js')
        if (-not $assetMatch.Success) {
            throw "Could not identify the production Studio entry bundle"
        }
        $assetUrl = "$studioUrl/$($assetMatch.Value)"
        $remoteBundle = (Invoke-WebRequest -UseBasicParsing -Uri $assetUrl -Headers @{ "Cache-Control" = "no-cache" } -TimeoutSec 60).Content
        if (-not $remoteBundle.Contains($buildId)) {
            throw "The Studio custom domain is not serving candidate $buildId"
        }
        Write-Host "==> Vercel provenance verified: $buildId / $($assetMatch.Value)"
    } finally {
        Pop-Location
    }
}

Write-Host "Release candidate verified: $buildId"
