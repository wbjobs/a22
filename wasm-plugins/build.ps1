$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PluginsDir = Join-Path (Split-Path -Parent $ScriptDir) "plugins\wasm"

New-Item -ItemType Directory -Force -Path $PluginsDir | Out-Null

Write-Host "Building slow-request-filter..." -ForegroundColor Green

Push-Location (Join-Path $ScriptDir "slow-request-filter")

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Error "ERROR: Rust/Cargo not installed. Install from https://rustup.rs"
    exit 1
}

$installedTargets = rustup target list --installed
if ($installedTargets -notmatch "wasm32-unknown-unknown") {
    Write-Host "Adding wasm32-unknown-unknown target..."
    rustup target add wasm32-unknown-unknown
}

cargo build --release --target wasm32-unknown-unknown
if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed"
    exit $LASTEXITCODE
}

Copy-Item "target\wasm32-unknown-unknown\release\slow_request_filter.wasm" `
    (Join-Path $PluginsDir "slow-request-filter.wasm") -Force

Pop-Location

Write-Host "`nPlugin built:" -ForegroundColor Green
Get-ChildItem $PluginsDir | Format-Table Name, Length -AutoSize

Write-Host "`nDone." -ForegroundColor Green
