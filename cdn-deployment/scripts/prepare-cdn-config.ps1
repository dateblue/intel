# Run from the EXTRACTED sing-box source root on Windows PowerShell.
# Creates a matching VLESS Encryption key pair locally and fills in the CDN JSON.
# Private key remains in local output; do NOT commit generated server JSON.
$ErrorActionPreference = 'Stop'
$Source = (Get-Location).Path
$Templates = Join-Path $Source 'cdn-deployment\config'
$Out = Join-Path $Source 'cdn-deployment\generated'
New-Item -ItemType Directory -Force $Out | Out-Null
$Generator = Join-Path $Out 'sb-vlessenc-keygen.exe'

$tags = 'with_quic,with_utls,with_clash_api,with_naive_outbound,with_musl,badlinkname,tfogo_checklinkname0'
& go build -trimpath -tags $tags '-ldflags=-checklinkname=0' -o $Generator ./cmd/sb-vlessenc-keygen
if ($LASTEXITCODE -ne 0) { throw 'Could not build ENC key generator' }

$lines = & $Generator
if ($LASTEXITCODE -ne 0) { throw 'ENC key generator failed' }
$pair = @($lines | Where-Object { $_ -match '^mlkem768x25519plus\.native\.1rtt\.' })
if ($pair.Length -ne 2) { throw 'Expected exactly two ENC keys' }

$s = Join-Path $Templates 'kr-server-inbound.template.json'
$c = Join-Path $Templates 'wrt-client-outbound.template.json'
$server = (Get-Content -Raw $s).Replace('__VLESS_ENC_SERVER_PRIVATE_SPEC__',$pair[0])
$client = (Get-Content -Raw $c).Replace('__VLESS_ENC_CLIENT_PUBLIC_SPEC__',$pair[1])
$dest1 = Join-Path $Out 'kr-server-inbound.json'
$dest2 = Join-Path $Out 'wrt-client-outbound.json'
[System.IO.File]::WriteAllText($dest1,$server,(New-Object System.Text.UTF8Encoding $false))
[System.IO.File]::WriteAllText($dest2,$client,(New-Object System.Text.UTF8Encoding $false))
Write-Host "Pair generated locally:`n  $dest1`n  $dest2"
Write-Host 'Server JSON contains a PRIVATE decryption key. Do not upload it to public repositories.'
