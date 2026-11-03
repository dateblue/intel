package vless

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/mux"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	VEnc "github.com/sagernet/sing-box/protocol/vless/encryption"
	"github.com/sagernet/sing-box/transport/v2ray"
	"github.com/sagernet/sing-vmess/packetaddr"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.VLESSOutboundOptions](registry, C.TypeVLESS, NewOutbound)
}

var (
	_ adapter.OutboundWithMultiplex   = (*Outbound)(nil)
	_ adapter.InterfaceUpdateListener = (*Outbound)(nil)
	_ adapter.IdleConnectionKeeper    = (*Outbound)(nil)
)

type Outbound struct {
	outbound.Adapter
	logger          logger.ContextLogger
	dialer          N.Dialer
	client          *vless.Client
	serverAddr      M.Socksaddr
	multiplexDialer *mux.Client
	tlsConfig       tls.Config
	tlsDialer       tls.Dialer
	transport       adapter.V2RayClientTransport
	packetAddr      bool
	encryption      *VEnc.Client
	xudp            bool
}

// sbxhttpTransportLogger is declared at package scope so the logger package
// is not shadowed by NewOutbound's logger parameter.
type sbxhttpTransportLogger interface {
	SetLogger(logger.ContextLogger)
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.VLESSOutboundOptions) (adapter.Outbound, error) {
	if options.Transport != nil && options.Transport.Type == C.V2RayTransportTypeSBXHTTP {
		if options.TLS == nil || options.TLS.Reality == nil || !options.TLS.Enabled || !options.TLS.Reality.Enabled {
			return nil, E.New("sbxhttp outbound requires Reality")
		}
		// Supported experimental Vision path: authenticated VLESS Encryption.
		// Refuse combining legacy inner TLS with the real encryption layer.
		if options.Encryption != "" && options.Encryption != "none" {
			if options.Transport.SBXHTTPOptions.VisionInnerTLS {
				return nil, E.New("sbxhttp VLESS encryption and vision_inner_tls are mutually exclusive")
			}
			if options.Flow != "" && options.Flow != "xtls-rprx-vision" {
				return nil, E.New("unsupported VLESS encrypted flow")
			}
		} else {
			if options.Flow != "" && !(options.Flow == "xtls-rprx-vision" && options.Transport.SBXHTTPOptions.VisionInnerTLS) {
				return nil, E.New("sbxhttp Vision without encryption requires private vision_inner_tls")
			}
			if options.Transport.SBXHTTPOptions.VisionInnerTLS != (options.Flow == "xtls-rprx-vision") {
				return nil, E.New("sbxhttp vision_inner_tls requires flow xtls-rprx-vision")
			}
		}
		if common.PtrValueOrDefault(options.Multiplex).Enabled {
			return nil, E.New("sbxhttp outbound does not support multiplex")
		}
	}
	if options.Encryption != "" && options.Encryption != "none" && (options.Transport == nil || options.Transport.Type != C.V2RayTransportTypeSBXHTTP) {
		return nil, E.New("experimental VLESS encryption currently requires private SB-XHTTP")
	}
	outboundDialer, err := dialer.New(ctx, options.DialerOptions, options.ServerIsDomain())
	if err != nil {
		return nil, err
	}
	outbound := &Outbound{
		Adapter:    outbound.NewAdapterWithDialerOptions(C.TypeVLESS, tag, options.Network.Build(), options.DialerOptions),
		logger:     logger,
		dialer:     outboundDialer,
		serverAddr: options.ServerOptions.Build(),
	}
	if options.TLS != nil {
		outbound.tlsConfig, err = tls.NewClientWithOptions(tls.ClientOptions{
			Context:       ctx,
			Logger:        logger,
			ServerAddress: options.Server,
			Options:       common.PtrValueOrDefault(options.TLS),
			KTLSCompatible: common.PtrValueOrDefault(options.Transport).Type == "" &&
				!common.PtrValueOrDefault(options.Multiplex).Enabled &&
				options.Flow == "",
		})
		if err != nil {
			return nil, err
		}
		outbound.tlsDialer = tls.NewDialer(outboundDialer, outbound.tlsConfig)
	}
	if options.Transport != nil {
		outbound.transport, err = v2ray.NewClientTransport(ctx, outbound.dialer, outbound.serverAddr, common.PtrValueOrDefault(options.Transport), outbound.tlsConfig)
		if err != nil {
			return nil, E.Cause(err, "create client transport: ", options.Transport.Type)
		}
		if loggable, ok := outbound.transport.(sbxhttpTransportLogger); ok {
			loggable.SetLogger(logger)
		}
	}
	if options.PacketEncoding == nil {
		outbound.xudp = true
	} else {
		switch *options.PacketEncoding {
		case "":
		case "packetaddr":
			outbound.packetAddr = true
		case "xudp":
			outbound.xudp = true
		default:
			return nil, E.New("unknown packet encoding: ", *options.PacketEncoding)
		}
	}
	outbound.client, err = vless.NewClient(options.UUID, options.Flow, logger)
	if err != nil {
		return nil, err
	}
	if options.Encryption != "" && options.Encryption != "none" {
		outbound.encryption, err = VEnc.ParseClientSpec(options.Encryption)
		if err != nil {
			return nil, E.Cause(err, "parse VLESS encryption")
		}
	}
	outbound.multiplexDialer, err = mux.NewClientWithOptions((*vlessDialer)(outbound), logger, common.PtrValueOrDefault(options.Multiplex))
	if err != nil {
		return nil, err
	}
	return outbound, nil
}

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if h.multiplexDialer == nil {
		switch N.NetworkName(network) {
		case N.NetworkTCP:
			h.logger.InfoContext(ctx, "outbound connection to ", destination)
		case N.NetworkUDP:
			h.logger.InfoContext(ctx, "outbound packet connection to ", destination)
		}
		return (*vlessDialer)(h).DialContext(ctx, network, destination)
	} else {
		switch N.NetworkName(network) {
		case N.NetworkTCP:
			h.logger.InfoContext(ctx, "outbound multiplex connection to ", destination)
		case N.NetworkUDP:
			h.logger.InfoContext(ctx, "outbound multiplex packet connection to ", destination)
		}
		return h.multiplexDialer.DialContext(ctx, network, destination)
	}
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if h.multiplexDialer == nil {
		h.logger.InfoContext(ctx, "outbound packet connection to ", destination)
		return (*vlessDialer)(h).ListenPacket(ctx, destination)
	} else {
		h.logger.InfoContext(ctx, "outbound multiplex packet connection to ", destination)
		return h.multiplexDialer.ListenPacket(ctx, destination)
	}
}

func (h *Outbound) MultiplexEnabled() bool {
	if h.multiplexDialer != nil {
		return true
	}
	multiplexTransport, isMultiplexTransport := h.transport.(adapter.V2RayMultiplexClientTransport)
	return isMultiplexTransport && multiplexTransport.MultiplexEnabled()
}

func (h *Outbound) InterfaceUpdated(ctx context.Context) {
	if h.transport != nil {
		// Some transports (notably SB-XHTTP) have a terminal Close method.
		// Interface changes must reset active connections without disabling
		// the outbound for all future dials.
		if resetter, ok := h.transport.(interface{ Reset() }); ok {
			resetter.Reset()
		} else {
			_ = h.transport.Close()
		}
	}
	if h.multiplexDialer != nil {
		h.multiplexDialer.Reset()
	}
}

func (h *Outbound) SetKeepIdleConnections(keep bool) {
	transportKeeper, isTransportKeeper := h.transport.(adapter.IdleConnectionKeeper)
	if isTransportKeeper {
		transportKeeper.SetKeepIdleConnections(keep)
	}
	if h.multiplexDialer != nil {
		h.multiplexDialer.SetKeepIdleConnections(keep)
	}
}

func (h *Outbound) CloseIdleConnections() {
	transportKeeper, isTransportKeeper := h.transport.(adapter.IdleConnectionKeeper)
	if isTransportKeeper {
		transportKeeper.CloseIdleConnections()
	}
	if h.multiplexDialer != nil {
		h.multiplexDialer.CloseIdleConnections()
	}
}

func (h *Outbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	if h.transport != nil {
		scope.Add(h.transport.Close)
	}
	if h.multiplexDialer != nil {
		scope.Add(h.multiplexDialer.Close)
	}
	return nil
}

type vlessDialer Outbound

func (h *vlessDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	var conn net.Conn
	var err error
	if h.transport != nil {
		conn, err = h.transport.DialContext(ctx)
	} else if h.tlsDialer != nil {
		conn, err = h.tlsDialer.DialTLSContext(ctx, h.serverAddr)
	} else {
		conn, err = h.dialer.DialContext(ctx, N.NetworkTCP, h.serverAddr)
	}
	if err != nil {
		return nil, err
	}
	if h.encryption != nil {
		var wrapped *VEnc.CommonConn
		wrapped, err = h.encryption.Handshake(conn)
		if err != nil {
			_ = conn.Close()
			return nil, E.Cause(err, "VLESS encryption handshake")
		}
		conn = wrapped
		h.logger.DebugContext(ctx, "VLESS encryption handshake completed")
	}
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		h.logger.InfoContext(ctx, "outbound connection to ", destination)
		return h.client.DialEarlyConn(conn, destination)
	case N.NetworkUDP:
		h.logger.InfoContext(ctx, "outbound packet connection to ", destination)
		if h.xudp {
			return h.client.DialEarlyXUDPPacketConn(conn, destination)
		} else if h.packetAddr {
			if destination.IsDomain() {
				return nil, E.New("packetaddr: domain destination is not supported")
			}
			packetConn, err := h.client.DialEarlyPacketConn(conn, M.Socksaddr{Fqdn: packetaddr.SeqPacketMagicAddress})
			if err != nil {
				return nil, err
			}
			return bufio.NewBindPacketConn(packetaddr.NewConn(packetConn, destination), destination), nil
		} else {
			return h.client.DialEarlyPacketConn(conn, destination)
		}
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
}

func (h *vlessDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	h.logger.InfoContext(ctx, "outbound packet connection to ", destination)
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	var conn net.Conn
	var err error
	if h.transport != nil {
		conn, err = h.transport.DialContext(ctx)
	} else if h.tlsDialer != nil {
		conn, err = h.tlsDialer.DialTLSContext(ctx, h.serverAddr)
	} else {
		conn, err = h.dialer.DialContext(ctx, N.NetworkTCP, h.serverAddr)
	}
	if err != nil {
		common.Close(conn)
		return nil, err
	}
	if h.encryption != nil {
		wrapped, e := h.encryption.Handshake(conn)
		if e != nil {
			_ = conn.Close()
			return nil, E.Cause(e, "VLESS encryption packet handshake")
		}
		conn = wrapped
	}
	if h.xudp {
		return h.client.DialEarlyXUDPPacketConn(conn, destination)
	} else if h.packetAddr {
		if destination.IsDomain() {
			return nil, E.New("packetaddr: domain destination is not supported")
		}
		conn, err := h.client.DialEarlyPacketConn(conn, M.Socksaddr{Fqdn: packetaddr.SeqPacketMagicAddress})
		if err != nil {
			return nil, err
		}
		return packetaddr.NewConn(conn, destination), nil
	} else {
		return h.client.DialEarlyPacketConn(conn, destination)
	}
}
