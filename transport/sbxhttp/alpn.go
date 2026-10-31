package sbxhttp

// privateH2ALPNAllowed permits no ALPN only on the Reality-authenticated
// private SB-XHTTP transport. It is not an HTTP/2-over-TLS interoperability
// rule: RFC-conformant H2 connections must negotiate "h2" via ALPN.
// Reject explicitly negotiated alternative protocols.
func privateH2ALPNAllowed(protocol string) bool {
	return protocol == "" || protocol == "h2"
}
