param([int]$Port = 8765, [switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot
if (-not $SkipBuild) {
    Push-Location -LiteralPath (Join-Path $projectRoot 'frontend')
    try {
        & flutter pub get
        if ($LASTEXITCODE -ne 0) { throw 'Flutter dependency resolution failed' }
        & flutter build web --release --no-wasm-dry-run --pwa-strategy=none
        if ($LASTEXITCODE -ne 0) { throw 'Flutter build failed' }
    } finally { Pop-Location }
}
$env:PYTHONPATH = Join-Path $projectRoot 'src'
$env:CONSOLE_PORT = "$Port"
Write-Host "Open http://127.0.0.1:$Port"
& python -m ephemeraldb.console.app
