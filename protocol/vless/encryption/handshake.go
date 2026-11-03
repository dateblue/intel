// SPDX-License-Identifier: GPL-3.0-or-later
package encryption

// This intentionally implements 1-RTT, native appearance, exactly two NFS
// keys (ML-KEM768 and X25519) and no 0-RTT ticket reuse. It does not claim
// general compatibility with Xray's full configurable appearance grammar.

import (
	"bytes"
	"crypto/ecdh"
	"crypto/mlkem"
	"crypto/rand"
	"errors"
	"fmt"
	"net"

	"lukechampine.com/blake3"
)

const (
	kemPubLen      = 1184
	kemCipherLen   = 1088
	xPubLen        = 32
	sharedLen      = 32
	pfsPubLen      = kemPubLen + xPubLen
	replyLen       = kemCipherLen + xPubLen
	relayLen       = kemCipherLen + 32 + xPubLen // hash(next key) between the relays
	helloPrefix    = 16 + relayLen
	defaultPadding = 128
)

type ClientConfig struct {
	MLKEMPublicKey  []byte
	X25519PublicKey []byte
}
type ServerConfig struct {
	MLKEMPrivateKey  []byte
	X25519PrivateKey []byte
}

type Client struct {
	mlkem        *mlkem.EncapsulationKey768
	x25519       *ecdh.PublicKey
	kemPub, xPub []byte
}
type Server struct {
	mlkem        *mlkem.DecapsulationKey768
	x25519       *ecdh.PrivateKey
	kemPub, xPub []byte
}

func NewClient(cfg ClientConfig) (*Client, error) {
	m, err := mlkem.NewEncapsulationKey768(cfg.MLKEMPublicKey)
	if err != nil {
		return nil, fmt.Errorf("ML-KEM public key: %w", err)
	}
	x, err := ecdh.X25519().NewPublicKey(cfg.X25519PublicKey)
	if err != nil {
		return nil, fmt.Errorf("X25519 public key: %w", err)
	}
	return &Client{mlkem: m, x25519: x, kemPub: bytes.Clone(cfg.MLKEMPublicKey), xPub: bytes.Clone(cfg.X25519PublicKey)}, nil
}
func NewServer(cfg ServerConfig) (*Server, error) {
	m, err := mlkem.NewDecapsulationKey768(cfg.MLKEMPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("ML-KEM private key: %w", err)
	}
	x, err := ecdh.X25519().NewPrivateKey(cfg.X25519PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("X25519 private key: %w", err)
	}
	return &Server{mlkem: m, x25519: x, kemPub: m.EncapsulationKey().Bytes(), xPub: x.PublicKey().Bytes()}, nil
}
func GenerateServerConfig() (ServerConfig, ClientConfig, error) {
	m, err := mlkem.GenerateKey768()
	if err != nil {
		return ServerConfig{}, ClientConfig{}, err
	}
	x, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return ServerConfig{}, ClientConfig{}, err
	}
	return ServerConfig{m.Bytes(), x.Bytes()}, ClientConfig{m.EncapsulationKey().Bytes(), x.PublicKey().Bytes()}, nil
}
func makePad(r *recordAEAD, n int) []byte {
	// One authenticated 2-byte length followed by authenticated filler.
	var buf [2]byte
	buf[0], buf[1] = byte((n-18)>>8), byte(n-18)
	encLen := r.seal(nil, buf[:], nil)
	filler := make([]byte, n-18-16)
	_, _ = rand.Read(filler)
	encFiller := r.seal(nil, filler, nil)
	return append(encLen, encFiller...)
}
func readPad(r *recordAEAD, conn net.Conn) error {
	var sizeBytes [18]byte
	if err := readAll(conn, sizeBytes[:]); err != nil {
		return err
	}
	n, err := decryptLen(r, sizeBytes[:])
	if err != nil {
		return err
	}
	if n < 16 || n > 65535 {
		return errors.New("invalid encrypted padding length")
	}
	b := make([]byte, n)
	if err := readAll(conn, b); err != nil {
		return err
	}
	_, err = r.open(b[:0], b, nil)
	return err
}
func deriveCommon(raw net.Conn, useAES bool, secret, txCtx, rxCtx []byte) (*CommonConn, error) {
	tx, err := newRecordAEAD(txCtx, secret, useAES)
	if err != nil {
		return nil, err
	}
	rx, err := newRecordAEAD(rxCtx, secret, useAES)
	if err != nil {
		return nil, err
	}
	return &CommonConn{Conn: raw, tx: tx, rx: rx}, nil
}

// Handshake completes the Xray-compatible 1-RTT native ML-KEM768+X25519
// private-wire handshake over an existing authenticated SB-XHTTP stream.
func (c *Client) Handshake(raw net.Conn) (*CommonConn, error) {
	iv := make([]byte, 16)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	kemSecret, kemCT := c.mlkem.Encapsulate()
	ephX, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	xSecret, err := ephX.ECDH(c.x25519)
	if err != nil {
		return nil, err
	}
	hashX := blake3.Sum256(c.xPub)
	relay := make([]byte, 0, relayLen)
	relay = append(relay, kemCT...)
	relay = append(relay, hashX[:]...)
	relay = append(relay, ephX.PublicKey().Bytes()...)
	if len(relay) != relayLen {
		return nil, errors.New("invalid client relay")
	}
	// Mask hash(next key) with stream keyed by the first NFS secret.
	// Xray uses an AES-CTR NewCTR(secret, iv) transformation here.
	ctr, err := newCTR(kemSecret, iv)
	if err != nil {
		return nil, err
	}
	ctr.XORKeyStream(relay[kemCipherLen:kemCipherLen+32], relay[kemCipherLen:kemCipherLen+32])
	// The CTR stream continues into the first 32 bytes of the next relay.
	// Xray server unmasks those bytes after validating the hash chain.
	ctr.XORKeyStream(relay[kemCipherLen+32:], relay[kemCipherLen+32:])
	nfs, err := newRecordAEAD(iv, xSecret, true)
	if err != nil {
		return nil, err
	}
	decap, err := mlkem.GenerateKey768()
	if err != nil {
		return nil, err
	}
	xPFS, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	pfsPub := append(bytes.Clone(decap.EncapsulationKey().Bytes()), xPFS.PublicKey().Bytes()...)
	encryptedLen := encryptLen(nfs, len(pfsPub)+16)
	encryptedPFS := nfs.seal(nil, pfsPub, nil)
	pad := makePad(nfs, defaultPadding)
	hello := make([]byte, 0, helloPrefix+len(encryptedLen)+len(encryptedPFS)+len(pad))
	hello = append(hello, iv...)
	hello = append(hello, relay...)
	hello = append(hello, encryptedLen...)
	hello = append(hello, encryptedPFS...)
	hello = append(hello, pad...)
	if err := writeAll(raw, hello); err != nil {
		return nil, err
	}
	reply := make([]byte, replyLen+16)
	if err := readAll(raw, reply); err != nil {
		return nil, err
	}
	clearReply, err := nfs.openFixed(reply[:0], reply, nil)
	if err != nil {
		return nil, err
	}
	if len(clearReply) != replyLen {
		return nil, errors.New("invalid PFS reply")
	}
	pfsKem, err := decap.Decapsulate(clearReply[:kemCipherLen])
	if err != nil {
		return nil, err
	}
	peerX, err := ecdh.X25519().NewPublicKey(clearReply[kemCipherLen:])
	if err != nil {
		return nil, err
	}
	pfsX, err := xPFS.ECDH(peerX)
	if err != nil {
		return nil, err
	}
	united := make([]byte, 0, 96)
	united = append(united, pfsKem...)
	united = append(united, pfsX...)
	united = append(united, xSecret...)
	conn, err := deriveCommon(raw, true, united, pfsPub, clearReply)
	if err != nil {
		return nil, err
	}
	// The ticket and server padding are authenticated with the server's
	// normal transmit record AEAD, before ordinary framed data begins.
	ticket := make([]byte, 32)
	if err := readAll(raw, ticket); err != nil {
		return nil, err
	}
	plain, err := conn.rx.open(ticket[:0], ticket, nil)
	if err != nil {
		return nil, err
	}
	if len(plain) != 16 {
		return nil, errors.New("invalid ticket")
	}
	if err := readPad(conn.rx, raw); err != nil {
		return nil, err
	}
	return conn, nil
}

func (s *Server) Handshake(raw net.Conn) (*CommonConn, error) {
	pre := make([]byte, helloPrefix)
	if err := readAll(raw, pre); err != nil {
		return nil, err
	}
	iv := pre[:16]
	relays := pre[16:]
	kemSecret, err := s.mlkem.Decapsulate(relays[:kemCipherLen])
	if err != nil {
		return nil, err
	}
	ctr, err := newCTR(kemSecret, iv)
	if err != nil {
		return nil, err
	}
	ctr.XORKeyStream(relays[kemCipherLen:kemCipherLen+32], relays[kemCipherLen:kemCipherLen+32])
	h := blake3.Sum256(s.xPub)
	if !bytes.Equal(h[:], relays[kemCipherLen:kemCipherLen+32]) {
		return nil, errors.New("key chain mismatch")
	}
	// The client masks both the next-key hash and the following X25519
	// ephemeral public key using one continuous CTR stream. Restore the
	// latter with the advanced stream state before deriving the shared key.
	ctr.XORKeyStream(relays[kemCipherLen+32:], relays[kemCipherLen+32:])
	x, err := ecdh.X25519().NewPublicKey(relays[kemCipherLen+32:])
	if err != nil {
		return nil, err
	}
	if x.Bytes()[31] > 127 {
		return nil, errors.New("noncanonical X25519 public key")
	}
	xSecret, err := s.x25519.ECDH(x)
	if err != nil {
		return nil, err
	}
	lenEnc := make([]byte, 18)
	if err := readAll(raw, lenEnc); err != nil {
		return nil, err
	}
	// Xray automatically detects AES-GCM vs ChaCha20-Poly1305 using the
	// authenticated first encrypted length. Retry with a fresh nonce state.
	nfs, err := newRecordAEAD(iv, xSecret, true)
	if err != nil {
		return nil, err
	}
	firstCopy := bytes.Clone(lenEnc)
	l, aesErr := decryptLen(nfs, firstCopy)
	useAES := aesErr == nil
	if aesErr != nil {
		nfs, err = newRecordAEAD(iv, xSecret, false)
		if err != nil {
			return nil, err
		}
		l, err = decryptLen(nfs, lenEnc)
		if err != nil {
			return nil, errors.New("AEAD negotiation failed")
		}
	}
	if l != pfsPubLen+16 {
		return nil, errors.New("unexpected PFS length")
	}
	encryptedPub := make([]byte, l)
	if err := readAll(raw, encryptedPub); err != nil {
		return nil, err
	}
	clientPub, err := nfs.open(encryptedPub[:0], encryptedPub, nil)
	if err != nil {
		return nil, err
	}
	if err := readPad(nfs, raw); err != nil {
		return nil, err
	}
	peerKem, err := mlkem.NewEncapsulationKey768(clientPub[:kemPubLen])
	if err != nil {
		return nil, err
	}
	pfsKem, kemReply := peerKem.Encapsulate()
	peerX, err := ecdh.X25519().NewPublicKey(clientPub[kemPubLen:])
	if err != nil {
		return nil, err
	}
	pfsXKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	pfsX, err := pfsXKey.ECDH(peerX)
	if err != nil {
		return nil, err
	}
	replyPub := append(bytes.Clone(kemReply), pfsXKey.PublicKey().Bytes()...)
	united := make([]byte, 0, 96)
	united = append(united, pfsKem...)
	united = append(united, pfsX...)
	united = append(united, xSecret...)
	conn, err := deriveCommon(raw, useAES, united, replyPub, clientPub)
	if err != nil {
		return nil, err
	}
	reply := nfs.sealFixed(nil, replyPub, nil)
	ticket := make([]byte, 16)
	_, _ = rand.Read(ticket)
	// Xray's first two ticket bytes express seconds-to-live; zero disables
	// 0-RTT and ticket reuse. This server deliberately leaves it disabled.
	ticket[0], ticket[1] = 0, 0
	encTicket := conn.tx.seal(nil, ticket, nil)
	encPad := makePad(conn.tx, defaultPadding)
	hello := append(reply, encTicket...)
	hello = append(hello, encPad...)
	if err := writeAll(raw, hello); err != nil {
		return nil, err
	}
	// 0-RTT is deliberately disabled: the per-connection PFS handshake is
	// always mandatory, and no early application data is accepted.
	return conn, nil
}
