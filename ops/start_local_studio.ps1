param(
    [string]$SecretsFile = "D:\kames\.env",
    [switch]$EnableProductionWorker
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$frontendRoot = Join-Path $repoRoot "ViralShorts-App"
$logRoot = Join-Path $repoRoot ".local-run"

function Assert-PortAvailable([int]$Port) {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($listener) {
        $process = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
        $processName = if ($process) { $process.ProcessName } else { "unknown" }
        throw "Port $Port is already used by PID $($listener.OwningProcess) ($processName). Stop it explicitly before starting Studio."
    }
}

function Import-DotEnv([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Studio secrets file was not found: $Path"
    }
    foreach ($rawLine in [System.IO.File]::ReadAllLines($Path)) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            continue
        }
        $parts = $line.Split(@("="), 2, [System.StringSplitOptions]::None)
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($value.Length -ge 2) {
            $first = $value[0]
            $last = $value[$value.Length - 1]
            if (($first -eq '"' -and $last -eq '"') -or ($first -eq "'" -and $last -eq "'")) {
                $value = $value.Substring(1, $value.Length - 2)
            }
        }
        if ($name) {
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

Assert-PortAvailable 10000
Assert-PortAvailable 8080
Import-DotEnv $SecretsFile

# Local Studio defaults to the funded providers and never falls back to xAI or
# RunPod. Fly owns the durable Redis production queue; local Studio leaves that
# consumer off unless a developer deliberately supplies Redis and enables it.
$env:STUDIO_ENVIRONMENT = "development"
$env:STUDIO_AGENT_PRIMARY_PROVIDER = "anthropic"
$env:STUDIO_AGENT_MODEL = "claude-haiku-4-5-20251001"
$env:IMAGE_PROVIDER_ORDER = "fal"
$env:XAI_IMAGE_FALLBACK_ENABLED = "false"
$env:STUDIO_RUNPOD_PRODUCTION_ENABLED = "0"
$env:STUDIO_RUNPOD_LONGFORM_ENABLED = "0"
$env:RUNPOD_COMPOSITOR_ENABLED = "0"
$env:RUNPOD_IMAGE_FEEDBACK_ENABLED = "0"
$env:REDIS_QUEUE_ENABLED = "0"
$env:JOB_QUEUE_WORKERS = "1"
$env:RUN_EMBEDDED_WORKER = if ($EnableProductionWorker) { "true" } else { "false" }
$env:VITE_API_BASE_URL = "http://127.0.0.1:10000"

New-Item -ItemType Directory -Force -Path $logRoot | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backendStdout = Join-Path $logRoot "backend-$stamp.stdout.log"
$backendStderr = Join-Path $logRoot "backend-$stamp.stderr.log"
$frontendStdout = Join-Path $logRoot "vite-$stamp.stdout.log"
$frontendStderr = Join-Path $logRoot "vite-$stamp.stderr.log"

$backend = Start-Process -FilePath "py" `
    -ArgumentList @("-3.12", "-m", "uvicorn", "backend:app", "--host", "127.0.0.1", "--port", "10000", "--workers", "1") `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $backendStdout `
    -RedirectStandardError $backendStderr `
    -WindowStyle Hidden `
    -PassThru

$frontend = Start-Process -FilePath "npm.cmd" `
    -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1", "--port", "8080", "--strictPort") `
    -WorkingDirectory $frontendRoot `
    -RedirectStandardOutput $frontendStdout `
    -RedirectStandardError $frontendStderr `
    -WindowStyle Hidden `
    -PassThru

try {
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Milliseconds 500
        if ($backend.HasExited) {
            throw "Studio backend exited during startup. See $backendStderr"
        }
        if ($frontend.HasExited) {
            throw "Studio frontend exited during startup. See $frontendStderr"
        }
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:10000/api/health" -TimeoutSec 3
            $page = Invoke-WebRequest -Uri "http://127.0.0.1:8080/" -UseBasicParsing -TimeoutSec 3
            if ($health.status -in @("online", "degraded") -and $page.StatusCode -eq 200) {
                Write-Output "Studio is ready at http://127.0.0.1:8080"
                Write-Output "Backend PID: $($backend.Id); frontend launcher PID: $($frontend.Id)"
                Write-Output "Logs: $backendStdout and $frontendStdout"
                exit 0
            }
        }
        catch {
            # Continue polling until the deadline; startup can take several seconds.
        }
    } while ((Get-Date) -lt $deadline)
    throw "Studio did not become healthy within 45 seconds. See $backendStderr and $frontendStderr"
}
catch {
    Stop-Process -Id $backend.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $frontend.Id -Force -ErrorAction SilentlyContinue
    foreach ($port in @(10000, 8080)) {
        Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue |
            ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
    }
    throw
}
