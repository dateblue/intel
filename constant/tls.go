package constant

const ACMETLS1Protocol = "acme-tls/1"

const (
	TLSEngineDefault = ""
	TLSEngineGo      = "go"
	TLSEngineApple   = "apple"
	TLSEngineWindows = "windows"
)

// Private Reality compatibility knob from the PQC branch. Default preserves
// the ClientHello fingerprint; hybrid verifies the PQC share is present.
const (
	RealityKeyShareDefault   = ""
	RealityKeyShareHybrid    = "hybrid"
	RealityKeyShareClassical = "classical"
)
