package sbxhttp

// This is an EXPERIMENTAL private SB-to-SB Vision carrier. It is deliberately
// different from Xray's VLESS Encryption wire format. The outer REALITY layer
// already authenticates and encrypts the link; the inner TLS 1.3 layer exists
// only to give sing-vmess Vision a real TLS connection to inspect and (when
// appropriate) bypass onto the HTTP/2 logical stream. Do not use it through
// an untrusted intermediary, or without outer REALITY authentication.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"time"
)

const privateVisionInnerALPN = "sbxhttp-vision/1"

func newPrivateVisionServerTLS() (*stdtls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	certTmpl := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour),
		DNSNames:              []string{"sbxhttp-vision.internal"},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, certTmpl, certTmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &stdtls.Config{
		MinVersion:   stdtls.VersionTLS13,
		MaxVersion:   stdtls.VersionTLS13,
		NextProtos:   []string{privateVisionInnerALPN},
		Certificates: []stdtls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}, nil
}

func privateVisionClientTLS(ctx context.Context, raw net.Conn) (net.Conn, error) {
	// The ephemeral certificate is authenticated exclusively by the already-
	// verified, authenticated REALITY envelope. This is *not* a generally safe
	// stand-alone TLS client, and this mode must never disable outer REALITY.
	inner := stdtls.Client(raw, &stdtls.Config{
		MinVersion: stdtls.VersionTLS13, MaxVersion: stdtls.VersionTLS13,
		ServerName:         "sbxhttp-vision.internal",
		InsecureSkipVerify: true, // Only safe inside mandatory authenticated REALITY.
		NextProtos:         []string{privateVisionInnerALPN},
	})
	if err := inner.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	if state := inner.ConnectionState(); state.NegotiatedProtocol != privateVisionInnerALPN {
		_ = inner.Close()
		return nil, errors.New("sbxhttp: incompatible private Vision inner ALPN")
	}
	// Return concrete *crypto/tls.Conn, not an opaque wrapper: sing-vmess
	// recognises this type when constructing its native XTLS VisionConn.
	return inner, nil
}

func privateVisionServerTLS(ctx context.Context, raw net.Conn, config *stdtls.Config) (net.Conn, error) {
	if config == nil {
		return nil, errors.New("sbxhttp: private Vision server config missing")
	}
	inner := stdtls.Server(raw, config)
	if err := inner.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	if state := inner.ConnectionState(); state.NegotiatedProtocol != privateVisionInnerALPN {
		_ = inner.Close()
		return nil, errors.New("sbxhttp: incompatible private Vision inner ALPN")
	}
	return inner, nil
}
