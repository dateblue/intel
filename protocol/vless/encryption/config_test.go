package encryption

import (
	"strings"
	"testing"
)

func TestSpecsRoundTrip(t *testing.T) {
	srv, cli, e := GenerateSpecs()
	if e != nil {
		t.Fatal(e)
	}
	if _, e := ParseServerSpec(srv); e != nil {
		t.Fatal(e)
	}
	if _, e := ParseClientSpec(cli); e != nil {
		t.Fatal(e)
	}
	if _, e := ParseServerSpec(strings.Replace(srv, ".1rtt.", ".0rtt.", 1)); e == nil {
		t.Fatal("accepted 0-RTT")
	}
}
