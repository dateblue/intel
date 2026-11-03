// SPDX-License-Identifier: GPL-3.0-or-later
// Experimental VLESS Encryption (1-RTT/native) protocol implementation.
// Xray wire reference: XTLS/Xray-core proxy/vless/encryption.
package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/blake3"
)

const maxRecordPlaintext = 8192
const maxRecordCiphertext = 16640

var maxNonce = [12]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}

type recordAEAD struct {
	aead  cipher.AEAD
	nonce [12]byte
}

func newRecordAEAD(context, secret []byte, useAES bool) (*recordAEAD, error) {
	key := make([]byte, 32)
	blake3.DeriveKey(key, string(context), secret)
	var a cipher.AEAD
	var err error
	if useAES {
		var block cipher.Block
		block, err = aes.NewCipher(key)
		if err == nil {
			a, err = cipher.NewGCM(block)
		}
	} else {
		a, err = chacha20poly1305.New(key)
	}
	if err != nil {
		return nil, err
	}
	return &recordAEAD{aead: a}, nil
}
func (r *recordAEAD) next() []byte {
	for i := 11; i >= 0; i-- {
		r.nonce[i]++
		if r.nonce[i] != 0 {
			break
		}
	}
	return r.nonce[:]
}
func (r *recordAEAD) seal(dst, plaintext, additional []byte) []byte {
	return r.aead.Seal(dst, r.next(), plaintext, additional)
}
func (r *recordAEAD) open(dst, ciphertext, additional []byte) ([]byte, error) {
	return r.aead.Open(dst, r.next(), ciphertext, additional)
}
func (r *recordAEAD) sealFixed(dst, plaintext, additional []byte) []byte {
	return r.aead.Seal(dst, maxNonce[:], plaintext, additional)
}
func (r *recordAEAD) openFixed(dst, ciphertext, additional []byte) ([]byte, error) {
	return r.aead.Open(dst, maxNonce[:], ciphertext, additional)
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return fmt.Errorf("invalid write length")
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
func readAll(r io.Reader, b []byte) error { _, err := io.ReadFull(r, b); return err }

// CommonConn implements the Xray VLESS Encryption record wire format.
// It intentionally keeps an embedded net.Conn for Vision's underlying-link path.
// Callers must NOT use the underlying Conn directly unless the Vision protocol
// has negotiated an explicit bypass of the encryption record layer.
// Concurrent Write calls and concurrent Read calls are serialized separately.
type CommonConn struct {
	net.Conn
	tx, rx     *recordAEAD
	txMu, rxMu sync.Mutex
	// Match the exact field types of Xray/sing-box-lx SPEC 105: Vision
	// reflectively accesses these unexported fields via the TLS registry.
	rawInput bytes.Buffer
	input    bytes.Reader
}

func (c *CommonConn) Write(b []byte) (int, error) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	total := 0
	for len(b) > 0 {
		n := len(b)
		if n > maxRecordPlaintext {
			n = maxRecordPlaintext
		}
		var header [5]byte
		header[0], header[1], header[2] = 23, 3, 3
		binary.BigEndian.PutUint16(header[3:], uint16(n+16))
		sealed := c.tx.seal(nil, b[:n], header[:])
		record := make([]byte, 5+len(sealed))
		copy(record, header[:])
		copy(record[5:], sealed)
		if err := writeAll(c.Conn, record); err != nil {
			return total, err
		}
		total += n
		b = b[n:]
	}
	return total, nil
}
func (c *CommonConn) Read(p []byte) (int, error) {
	c.rxMu.Lock()
	defer c.rxMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if c.input.Len() > 0 {
		return c.input.Read(p)
	}
	var header [5]byte
	if err := readAll(c.Conn, header[:]); err != nil {
		return 0, err
	}
	size := int(binary.BigEndian.Uint16(header[3:]))
	if header[0] != 23 || header[1] != 3 || header[2] != 3 || size < 17 || size > maxRecordCiphertext {
		return 0, errors.New("invalid VLESS Encryption record header")
	}
	c.rawInput.Reset()
	c.rawInput.Grow(size)
	// Retain the ciphertext backing buffer: Vision reads rawInput itself.
	_, _ = c.rawInput.Write(make([]byte, size))
	ciphertext := c.rawInput.Bytes()
	if err := readAll(c.Conn, ciphertext); err != nil {
		return 0, err
	}
	decoded, err := c.rx.open(ciphertext[:0], ciphertext, header[:])
	if err != nil {
		return 0, err
	}
	n := copy(p, decoded)
	if n < len(decoded) {
		c.input.Reset(decoded[n:])
	}
	return n, nil
}

func encryptLen(r *recordAEAD, n int) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(n))
	return r.seal(nil, buf, nil)
}
func decryptLen(r *recordAEAD, in []byte) (int, error) {
	b, err := r.open(in[:0], in, nil)
	if err != nil {
		return 0, err
	}
	if len(b) != 2 {
		return 0, errors.New("invalid length ciphertext")
	}
	return int(binary.BigEndian.Uint16(b)), nil
}
