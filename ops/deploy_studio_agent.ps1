# Deploy Studio Agent fixes: Fly backend, Cloudflare worker, Vercel frontend.
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

$fly = Find-CommandPath "fly" @(
    "$env:USERPROFILE\.fly\bin\fly.exe",
    "$env:LOCALAPPDATA\fly\bin\fly.exe",
    "C:\Users\casey\.fly\bin\fly.exe"
)

if ($fly) {
    Write-Host "==> Fly deploy ($fly)"
    & $fly deploy --remote-only
} else {
    Write-Warning "fly CLI not found - skip backend deploy or install: https://fly.io/docs/hands-on/install-flyctl/"
}

$wrangler = Find-CommandPath "wrangler.cmd" @()
if (-not $wrangler) { $wrangler = Find-CommandPath "wrangler" @() }
if ($wrangler) {
    Write-Host "==> Cloudflare worker (runpod-serverless)"
    Push-Location (Join-Path $Root "runpod-serverless")
    & $wrangler deploy
    Pop-Location
} else {
    Write-Warning "wrangler not found - skip worker deploy"
}

$vercel = Find-CommandPath "vercel.cmd" @()
if (-not $vercel) { $vercel = Find-CommandPath "vercel" @() }
if ($vercel) {
    Write-Host "==> Vercel production (ViralShorts-App)"
    Push-Location (Join-Path $Root "ViralShorts-App")
    & $vercel --prod --yes
    Pop-Location
} else {
    Write-Warning "vercel not found - build locally from ViralShorts-App with npm.cmd run build"
}

Write-Host "Done. Hard-refresh https://studio.nyptidindustries.com and smoke-test channel analytics + agent approve."
