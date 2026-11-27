#!/usr/bin/env bash
# Local/remote smoke build only. Never installs or replaces the running sing-box.
set -euo pipefail
cd "$(dirname "$0")/.."
echo '[Go toolchain]'
go version
python3 scripts/verify_slim_profile.py
# These are compilation checks. They do not validate KR/SG real-world connectivity.
TAGS='with_quic,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0'
if [[ "${WITH_NAIVE:-0}" == '1' ]]; then
    TAGS="${TAGS},with_naive_outbound"
    echo '[Naive] Enabled: native Cronet build environment must already be installed.'
else
    echo '[Naive] Disabled for dependency-minimal smoke test; do NOT deploy this binary as your full profile.'
fi
printf '[Build tags] %s\n' "$TAGS"
go mod verify
go test -tags "$TAGS" ./transport/sbxhttp ./protocol/vless ./option ./common/tls ./common/sniff ./transport/device ./protocol/tun
mkdir -p dist
go build -trimpath -tags "$TAGS" -ldflags "-X github.com/sagernet/sing-box/constant.Version=1.15.0-alpha14-pqc-sbxhttp" -o dist/sing-box-alpha14-smoke ./cmd/sing-box
printf '\nSmoke build succeeded: %s/dist/sing-box-alpha14-smoke\n' "$PWD"
echo 'Do not replace production binary before validating ENC, Replay-v2, Reality/Vision, CDN and HY2/TUIC.'
