// Command sb-vlessenc-keygen prints a matching experimental VLESS
// Encryption/Decryption 1-RTT configuration pair. Run locally only, and
// protect the server's decryption string as a long-term private key.
package main

import (
	"fmt"
	"github.com/sagernet/sing-box/protocol/vless/encryption"
	"os"
)

func main() {
	server, client, err := encryption.GenerateSpecs()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("SERVER decryption (SECRET; do not share or commit):")
	fmt.Println(server)
	fmt.Println("\nCLIENT encryption (public key):")
	fmt.Println(client)
}
