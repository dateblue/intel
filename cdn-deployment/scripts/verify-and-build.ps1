# Windows Go 1.27.3 smoke-test of custom core WITHOUT Naive/cronet or Android.
# Full Naive CGO and signed APK builds run in GitHub Actions build-slim.yml.
$ErrorActionPreference = 'Stop'
$root = (Get-Location).Path
if (-not (Test-Path (Join-Path $root 'go.mod'))) { throw 'First cd to D:\dev\sing-box' }
& go version
if ($LASTEXITCODE -ne 0) { throw 'Go is not installed' }
$tags = 'with_quic,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0'
& python scripts/verify_slim_profile.py
if ($LASTEXITCODE -ne 0) { throw 'Slim profile audit failed' }
& go test -tags $tags '-ldflags=-checklinkname=0' -count=1 ./transport/sbxhttp ./protocol/vless/encryption ./protocol/vless ./option ./route ./protocol/tun
if ($LASTEXITCODE -ne 0) { throw 'Unit tests failed; DO NOT push/deploy' }
New-Item -ItemType Directory -Force dist | Out-Null
# Test-only non-Naive binaries: no external cronet, Android SDK, or NDK needed.
$env:CGO_ENABLED='0'
$env:GOOS='windows'; $env:GOARCH='amd64'
& go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o dist/sing-box-cdn-smoke-windows-amd64.exe ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'Windows smoke build failed' }
$env:GOOS='linux'; $env:GOARCH='arm64'
& go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o dist/sing-box-cdn-smoke-linux-arm64 ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'Linux arm64 smoke build failed' }
$env:GOOS='linux'; $env:GOARCH='amd64'
& go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o dist/sing-box-cdn-smoke-linux-amd64 ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'Linux amd64 smoke build failed' }
Remove-Item Env:GOOS -ErrorAction SilentlyContinue
Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue
& .\dist\sing-box-cdn-smoke-windows-amd64.exe version
if ($LASTEXITCODE -ne 0) { throw 'Windows smoke binary failed to launch' }
Write-Host 'PASS: local tests and non-Naive smoke builds. Full Naive, Android APK, and runtime CDN checks remain for CI / devices.'
