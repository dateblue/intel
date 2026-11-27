#!/usr/bin/env python3
"""Static source/profile guard usable without a Go compiler (Go 1.27 on user host)."""
from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parent.parent
errors = []

def check(cond, message):
    if not cond:
        errors.append(message)

def source(path):
    return (root / path).read_text(encoding="utf-8")

registry = source("include/registry.go")
quic = source("include/quic.go")
libbox = source("cmd/internal/build_libbox/main.go")
workflow = source(".github/workflows/build-slim.yml")

for marker in [
    "tun.RegisterInbound(registry)",
    "redirect.RegisterRedirect(registry)",
    "redirect.RegisterTProxy(registry)",
    "socks.RegisterInbound(registry)",
    "socks.RegisterOutbound(registry)",
    "vless.RegisterInbound(registry)",
    "vless.RegisterOutbound(registry)",
    "naive.RegisterInbound(registry)",
    "registerNaiveOutbound(registry)",
    "registerQUICInbounds(registry)",
    "registerQUICOutbounds(registry)",
    "fakeip.RegisterTransport(registry)",
    "group.RegisterSelector(registry)",
    "group.RegisterURLTest(registry)",
]:
    check(registry.count(marker) == 1, f"inbound/outbound or infrastructure registration: {marker}")

for marker in [
    "hysteria2.RegisterInbound(registry)",
    "hysteria2.RegisterOutbound(registry)",
    "tuic.RegisterInbound(registry)",
    "tuic.RegisterOutbound(registry)",
]:
    check(quic.count(marker) == 1, f"QUIC registration missing: {marker}")

for line in (registry + "\n" + quic).splitlines():
    if re.search(r"\b(?:hysteria|shadowsocks|snell|vmess|trojan|tor|ssh|shadowtls|anytls|mixed|http|masque)\.Register(?:In|Out)bound\(", line):
        errors.append("unexpected protocol registration: " + line.strip())
check("hysteria.RegisterInbound" not in quic and "hysteria.RegisterOutbound" not in quic,
      "Hysteria v1 should not be registered")

for path in [
    "option/v2ray_xhttp.go", "option/v2ray_xhttp_xmux_range.go",
    "transport/v2ray/client_xhttp_enabled.go", "transport/v2ray/client_xhttp_disabled.go",
    "transport/v2rayxhttp", "common/badh2/stream_error.go",
]:
    check(not (root / path).exists(), f"XHTTP file remains: {path}")
# Distinguish removed legacy Xray XHTTP from the new, independent SB-XHTTP.
# A plain substring search for "xhttp" would incorrectly reject "sbxhttp".
legacy_xhttp = re.compile(
    r"(?i)(?<![A-Za-z0-9_])xhttp(?![A-Za-z0-9_])|"
    r"v2rayxhttp|"
    r"(?<![A-Za-z0-9_])V2RayTransportTypeXHTTP(?![A-Za-z0-9_])|"
    r"(?<![A-Za-z0-9_])XHTTPOptions(?![A-Za-z0-9_])"
)
for path in ["constant/v2ray.go", "option/v2ray_transport.go", "transport/v2ray/transport.go"]:
    check(not legacy_xhttp.search(source(path)), f"Legacy XHTTP reference remains: {path}")

# Ensure BOTH ends of the private SB-XHTTP transport remain in this profile.
for path in [
    "transport/sbxhttp/client.go", "transport/sbxhttp/server.go",
    "transport/sbxhttp/conn.go", "transport/sbxhttp/conn_test.go",
]:
    check((root / path).is_file(), f"SB-XHTTP source missing: {path}")
for path, marker in [
    ("constant/v2ray.go", "V2RayTransportTypeSBXHTTP"),
    ("option/v2ray_transport.go", "V2RaySBXHTTPOptions"),
    ("transport/v2ray/transport.go", "sbxhttp.NewClient"),
    ("transport/v2ray/transport.go", "sbxhttp.NewServer"),
    ("protocol/vless/inbound.go", "V2RayTransportTypeSBXHTTP"),
    ("protocol/vless/outbound.go", "V2RayTransportTypeSBXHTTP"),
]:
    check(marker in source(path), f"SB-XHTTP registration missing: {path}: {marker}")
check("with_xhttp" not in workflow.split("BASE_TAGS:",1)[-1].split("\n",1)[0], "XHTTP build tag remains")
check("with_xhttp" not in libbox, "XHTTP libbox build tag remains")

# alpha14 already implements the standard ML-KEM hybrid TLS curve and uTLS.
# Reject accidental reintroduction of our redundant, fork-specific overrides.
check("X25519MLKEM768" in source("option/tls.go"), "official hybrid TLS curve missing")
check("MlkemEcdhe" in source("common/tls/reality_client.go"), "Reality hybrid-only auth key fallback missing")
check("KeyShare string" not in source("option/tls.go"), "duplicate Reality key_share option reintroduced")
check(not (root / "common/tls/utls_randomized_lx.go").exists(), "duplicate randomized uTLS override reintroduced")
check("lxRandomFingerprints" not in source("common/tls/utls_client.go"), "duplicate random-fingerprint patch reintroduced")

for tag in ["with_quic", "with_utls", "with_naive_outbound", "with_clash_api"]:
    check(tag in workflow, f"CLI tag missing: {tag}")
    check(tag in libbox, f"libbox tag missing: {tag}")
shared_line = next((line for line in libbox.splitlines() if line.strip().startswith('sharedTags = append(sharedTags, "with_quic"')), "")
for tag in ["with_wireguard", "with_openvpn", "with_openconnect", "with_usbip", "with_tailscale", "with_xhttp"]:
    check(tag not in shared_line, f"unwanted libbox shared tag: {tag}")
check("go-version: '1.27.3'" in workflow or "go-version: '1.27.4'" in workflow,
      "CI Go 1.27 version missing")

for path, marker in [
    ("option/tun.go", 'SniffOverrideDestination bool `json:"sniff_override_destination'),
    ("route/route.go", "metadata.SniffOverrideDestination"),
    ("protocol/tun/inbound.go", "sniffOverrideDestination"),
]:
    check(marker not in source(path), f"deprecated custom sniff shim remains: {path}")
# The clean baseline is the actual official v1.15.0-alpha.14 dependency set.
mod = source("go.mod")
check("go 1.26.11" in mod and "github.com/sagernet/quic-go v0.66.0-sing-box-mod.1" in mod,
      "official alpha14 Go/QUIC dependencies missing")
check("func (h *Inbound) References() ([]string, []string)" in source("protocol/vless/inbound.go"),
      "alpha14 inbound reference API regressed")
check('"__VLESS_UUID__"' in source("cdn-deployment/config/kr-server-inbound.template.json"),
      "CDN server template contains an unsanitized UUID")
check('"__VLESS_UUID__"' in source("cdn-deployment/config/wrt-client-outbound.template.json"),
      "CDN client template contains an unsanitized UUID")
if errors:
    print("FAIL: PQC slim source/profile consistency")
    for error in errors:
        print("  -", error)
    sys.exit(1)
print("PASS: PQC Slim + private SB-XHTTP in/out; official sniff route actions restored")
