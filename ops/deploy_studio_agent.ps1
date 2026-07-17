param(
    [switch]$SkipTests,
    [switch]$DeployWorker,
    [switch]$DeployVercel
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
$buildId = "studio-{0}-{1}" -f (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'), $shortSha

if (-not $SkipTests) {
    $python = Find-CommandPath "py"
    if (-not $python) { throw "Python launcher (py) is required for release tests" }
    $tests = @(
        "tests/test_studio_command_layer.py",
        "tests/test_studio_command_runner.py",
        "tests/test_channel_data_natural_language.py",
        "tests/test_longform_release_contract.py",
        "tests/test_caption_alignment.py",
        "tests/test_longform_media_revision.py",
        "tests/test_studio_agent_job_ownership.py",
        "tests/test_billing_webhook_idempotency.py",
        "tests/test_unified_credits_settlement.py",
        "tests/test_short_caption_sync.py"
    ) | Where-Object { Test-Path $_ }
    $env:PYTHONDONTWRITEBYTECODE = "1"
    $env:STUDIO_RUNPOD_PRODUCTION_ENABLED = "0"
    $env:FAL_AI_KEY = ""
    $env:FAL_KEY = ""
    $env:XAI_API_KEY = ""
    & $python -3.12 -m pytest -q @tests
    Assert-NativeSuccess "provider-free release tests"

    Push-Location (Join-Path $Root "ViralShorts-App")
    try {
        & npm.cmd run build
        Assert-NativeSuccess "frontend production build"
    } finally {
        Pop-Location
    }
}

$fly = Find-CommandPath "fly" @(
    "$env:USERPROFILE\.fly\bin\fly.exe",
    "$env:LOCALAPPDATA\fly\bin\fly.exe",
    "C:\Users\casey\.fly\bin\fly.exe"
)
if (-not $fly) { throw "fly CLI is required for the Studio backend deployment" }

Write-Host "==> Deploying immutable Fly candidate $buildId ($gitSha)"
& $fly deploy --remote-only --build-arg "GIT_SHA=$gitSha" --build-arg "FRONTEND_BUILD_ID=$buildId"
Assert-NativeSuccess "Fly deploy"

$healthUrl = "https://nyptid-studio.fly.dev/api/health"
$verified = $false
for ($attempt = 1; $attempt -le 60; $attempt++) {
    try {
        $health = Invoke-RestMethod -Uri $healthUrl -Method Get -TimeoutSec 20
        if (
            [string]$health.backend_commit -eq $gitSha -and
            [string]$health.frontend_bundle -eq $buildId -and
            [string]$health.status -in @("online", "degraded")
        ) {
            $verified = $true
            break
        }
    } catch {
        # Fly may still be replacing the old machine; retry the immutable check.
    }
    Start-Sleep -Seconds 5
}
if (-not $verified) {
    throw "Fly returned no health payload matching commit $gitSha and build $buildId; candidate is not released."
}
Write-Host "==> Fly provenance verified: $gitSha / $buildId"

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
    Push-Location (Join-Path $Root "ViralShorts-App")
    try {
        & $vercel --prod --yes --build-env "VITE_STUDIO_BUILD_ID=$buildId"
        Assert-NativeSuccess "Vercel deploy"
    } finally {
        Pop-Location
    }
}

Write-Host "Release candidate verified: $buildId"
