package sbxhttp

// CDN edge selection is a client-only dial override. It does not change
// TLS SNI, HTTP Host/:authority, VLESS session ID, origin, or server routing.
// Different Anycast IPs can still converge on the same Cloudflare PoP.

import (
	"fmt"
	"net/netip"
)

func validateMultiCDNIPs(inputs []string, isCDN bool, laneCount int) ([]netip.Addr, error) {
	if len(inputs) == 0 {
		return nil, nil // retain v0.1 dial behavior, useful as same-IP control
	}
	if !isCDN {
		return nil, fmt.Errorf("sbxhttp: cdn_ips is only valid for CDN mode")
	}
	if laneCount == 0 || len(inputs) > 6 {
		return nil, fmt.Errorf("sbxhttp: cdn_ips requires down_connections > 0, at most 6 candidate IPs")
	}
	out := make([]netip.Addr, 0, len(inputs))
	seen := make(map[netip.Addr]struct{}, len(inputs))
	for _, input := range inputs {
		ip, err := netip.ParseAddr(input)
		if err != nil || ip.Zone() != "" {
			return nil, fmt.Errorf("sbxhttp: cdn_ips requires IP literals without ports or zone, got %q", input)
		}
		ip = ip.Unmap()
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			return nil, fmt.Errorf("sbxhttp: cdn_ips requires public routable IP, got %q", input)
		}
		if _, ok := seen[ip]; ok {
			return nil, fmt.Errorf("sbxhttp: cdn_ips contains duplicate IP %s", ip)
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	return out, nil
}

func multiEdgeForLane(ips []netip.Addr, lane int) netip.Addr {
	return ips[lane%len(ips)]
}
