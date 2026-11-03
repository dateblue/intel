// SPDX-License-Identifier: GPL-3.0-or-later
package encryption

import (
	"crypto/aes"
	"crypto/cipher"

	"lukechampine.com/blake3"
)

// The Xray NewCTR wire construction: BLAKE3 derive context "VLESS",
// AES-256-CTR with the per-connection 16-byte IV.
func newCTR(secret, iv []byte) (cipher.Stream, error) {
	key := make([]byte, 32)
	blake3.DeriveKey(key, "VLESS", secret)
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewCTR(b, iv), nil
}
