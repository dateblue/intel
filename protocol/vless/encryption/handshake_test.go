package encryption

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"
)

func TestOneRTTNativeBidirectional(t *testing.T) {
	secret, pub, err := GenerateServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(secret)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := NewClient(pub)
	if err != nil {
		t.Fatal(err)
	}
	serverRaw, clientRaw := net.Pipe()
	defer serverRaw.Close()
	defer clientRaw.Close()
	_ = serverRaw.SetDeadline(time.Now().Add(8 * time.Second))
	_ = clientRaw.SetDeadline(time.Now().Add(8 * time.Second))
	type result struct {
		c   *CommonConn
		err error
	}
	ch := make(chan result, 1)
	go func() { c, e := srv.Handshake(serverRaw); ch <- result{c, e} }()
	clientConn, err := cli.Handshake(clientRaw)
	if err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	ss := <-ch
	if ss.err != nil {
		t.Fatalf("server handshake: %v", ss.err)
	}
	sendAndReceive := func(sender, receiver *CommonConn, size int) {
		input := make([]byte, size)
		if _, err := rand.Read(input); err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() { _, e := sender.Write(input); errCh <- e }()
		output := make([]byte, size)
		_, err := io.ReadFull(receiver, output)
		if err != nil {
			t.Fatal(err)
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(input, output) {
			t.Fatalf("mismatch %d", size)
		}
	}
	sendAndReceive(clientConn, ss.c, 73)
	sendAndReceive(ss.c, clientConn, 20481)
	sendAndReceive(clientConn, ss.c, 131072)
}

func TestInvalidPublicKey(t *testing.T) {
	_, err := NewClient(ClientConfig{MLKEMPublicKey: []byte("bad"), X25519PublicKey: []byte("bad")})
	if err == nil {
		t.Fatal("invalid key accepted")
	}
}

func TestRejectTamperedEncryptedRecord(t *testing.T) {
	secret, pub, err := GenerateServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(secret)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := NewClient(pub)
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	_ = a.SetDeadline(time.Now().Add(8 * time.Second))
	_ = b.SetDeadline(time.Now().Add(8 * time.Second))
	type result struct {
		c   *CommonConn
		err error
	}
	ch := make(chan result, 1)
	go func() { c, e := srv.Handshake(a); ch <- result{c, e} }()
	_, err = cli.Handshake(b)
	if err != nil {
		t.Fatal(err)
	}
	ss := <-ch
	if ss.err != nil {
		t.Fatal(ss.err)
	}
	bad := append([]byte{23, 3, 3, 0, 17}, make([]byte, 17)...)
	writeCh := make(chan error, 1)
	go func() { writeCh <- writeAll(b, bad) }()
	buf := make([]byte, 40)
	if _, err = ss.c.Read(buf); err == nil {
		t.Fatal("tampered record accepted")
	}
	if e := <-writeCh; e != nil {
		t.Fatal(e)
	}
}
