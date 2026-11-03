package encryption

// Minimal explicitly validated subset of Xray VLESS Encryption's wire
// configuration. Both peers must use the same ordered NFS keys.
// Only native / 1rtt / ML-KEM768 + X25519 is accepted in this experimental
// compatibility implementation; 0rtt tickets and xorpub are disabled.

import (
	"encoding/base64"
	"fmt"
	"strings"
)

func parse(spec string) ([][]byte, error) {
	parts := strings.Split(spec, ".")
	if len(parts) != 5 || parts[0] != "mlkem768x25519plus" || parts[1] != "native" || parts[2] != "1rtt" {
		return nil, fmt.Errorf("unsupported VLESS encryption spec: require mlkem768x25519plus.native.1rtt.<key1>.<key2>")
	}
	keys := make([][]byte, 2)
	for i := 0; i < 2; i++ {
		if parts[i+3] == "" {
			return nil, fmt.Errorf("missing encryption key %d", i+1)
		}
		b, e := base64.RawURLEncoding.DecodeString(parts[i+3])
		if e != nil {
			return nil, fmt.Errorf("invalid encryption key %d: %w", i+1, e)
		}
		keys[i] = b
	}
	return keys, nil
}
func ParseClientSpec(spec string) (*Client, error) {
	keys, e := parse(spec)
	if e != nil {
		return nil, e
	}
	return NewClient(ClientConfig{MLKEMPublicKey: keys[0], X25519PublicKey: keys[1]})
}
func ParseServerSpec(spec string) (*Server, error) {
	keys, e := parse(spec)
	if e != nil {
		return nil, e
	}
	return NewServer(ServerConfig{MLKEMPrivateKey: keys[0], X25519PrivateKey: keys[1]})
}
func GenerateSpecs() (server, client string, err error) {
	secret, pub, e := GenerateServerConfig()
	if e != nil {
		return "", "", e
	}
	prefix := "mlkem768x25519plus.native.1rtt."
	server = prefix + base64.RawURLEncoding.EncodeToString(secret.MLKEMPrivateKey) + "." + base64.RawURLEncoding.EncodeToString(secret.X25519PrivateKey)
	client = prefix + base64.RawURLEncoding.EncodeToString(pub.MLKEMPublicKey) + "." + base64.RawURLEncoding.EncodeToString(pub.X25519PublicKey)
	return server, client, nil
}
