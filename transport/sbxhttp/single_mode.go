package sbxhttp

import "errors"

// rejectMultiTCPOptions fails closed on removed MultiTCP settings. Keeping
// their JSON fields in V2RaySBXHTTPOptions makes old configurations fail loudly
// rather than silently fall back to a different wire protocol.
func rejectMultiTCPOptions(downConnections int, cdnIPs []string) error {
	if downConnections != 0 || len(cdnIPs) != 0 {
		return errors.New("sbxhttp v0.7: MultiTCP was removed; delete down_connections and cdn_ips from both client and server transport settings")
	}
	return nil
}
