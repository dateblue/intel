package encryption

import (
	"bytes"
	"github.com/sagernet/sing-vmess/vless"
	"net"
	"reflect"
	"testing"
)

func TestVisionAcceptsCommonConn(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	wrapped := &CommonConn{Conn: a}
	if _, err := vless.NewVisionConn(wrapped, wrapped, [16]byte{}, nil); err != nil {
		t.Fatalf("Vision did not recognise VLESS Encryption: %v", err)
	}
	found, raw, typ, ptr := castVisionConn(wrapped)
	if !found || raw != a || ptr == 0 {
		t.Fatalf("registry cast invalid: %v %T", found, raw)
	}
	f1, ok1 := typ.FieldByName("input")
	f2, ok2 := typ.FieldByName("rawInput")
	if !ok1 || !ok2 || f1.Type != reflect.TypeOf(bytes.Reader{}) || f2.Type != reflect.TypeOf(bytes.Buffer{}) {
		t.Fatal("Vision reflection fields mismatch")
	}
}
func TestVisionRegistryRegistered(t *testing.T) {
	for _, entry := range visionTLSRegistry {
		if reflect.ValueOf(entry).Pointer() == reflect.ValueOf(castVisionConn).Pointer() {
			return
		}
	}
	t.Fatal("Vision registry missing VLESS Encryption")
}
