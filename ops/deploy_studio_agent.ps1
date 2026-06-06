# Deploy Studio Agent fixes: Fly backend, Cloudflare worker, Vercel frontend.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

function Find-Fly {
    if (Get-Command fly -ErrorAction SilentlyContinue) { return "fly" }
    $candidates = @(
        "$env:USERPROFILE\.fly\bin\fly.exe",
        "$env:LOCALAPPDATA\fly\bin\fly.exe",
        "C:\Users\casey\.fly\bin\fly.exe"
    )
    foreach ($p in $candidates) {
        if (Test-Path $p) { return $p }
    }
    return $null
}

$fly = Find-Fly
if ($fly) {
    Write-Host "==> Fly deploy ($fly)"
    & $fly deploy --remote-only
} else {
    Write-Warning "fly CLI not found — skip backend deploy or install: https://fly.io/docs/hands-on/install-flyctl/"
}

if (Get-Command wrangler -ErrorAction SilentlyContinue) {
    Write-Host "==> Cloudflare worker (runpod-serverless)"
    Push-Location (Join-Path $Root "runpod-serverless")
    wrangler deploy
    Pop-Location
} else {
    Write-Warning "wrangler not found — skip worker deploy"
}

if (Get-Command vercel -ErrorAction SilentlyContinue) {
    Write-Host "==> Vercel production (ViralShorts-App)"
    Push-Location (Join-Path $Root "ViralShorts-App")
    vercel --prod
    Pop-Location
} else {
    Write-Warning "vercel not found — build locally: cd ViralShorts-App && npm run build"
}

Write-Host "Done. Hard-refresh https://studio.nyptidindustries.com and smoke-test channel analytics + agent approve."
