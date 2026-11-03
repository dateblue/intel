package encryption

// SPEC 105: permit the existing sing-vmess Vision implementation to
// recognise an authenticated VLESS Encryption connection over HTTP/2.
// The upstream private registry is intentionally accessed by linkname;
// bumping sing-vmess requires the registry tests to be rerun.

import (
	"github.com/sagernet/sing-vmess/vless"
	N "github.com/sagernet/sing/common/network"
	"net"
	"reflect"
	"unsafe"
)

var _ = vless.FlowVision

//go:linkname visionTLSRegistry github.com/sagernet/sing-vmess/vless.tlsRegistry
var visionTLSRegistry []func(conn net.Conn) (loaded bool, netConn net.Conn, reflectType reflect.Type, reflectPointer uintptr)

func init() { visionTLSRegistry = append(visionTLSRegistry, castVisionConn) }
func castVisionConn(conn net.Conn) (loaded bool, netConn net.Conn, reflectType reflect.Type, reflectPointer uintptr) {
	commonConn, loaded := N.CastReader[*CommonConn](conn)
	if !loaded {
		return
	}
	return true, commonConn.Conn, reflect.TypeOf(commonConn).Elem(), uintptr(unsafe.Pointer(commonConn))
}
