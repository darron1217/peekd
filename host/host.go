package host

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"log/slog"
	"net"
	"time"
)

type HostOption struct {
	ip         string
	port       int
	privateKey crypto.PrivKey
	userAgent  string
	rcMgr      network.ResourceManager
	connMgr    connmgr.ConnManager
}

type HostOptionFunc func(*HostOption)

func WithIP(ip string) HostOptionFunc {
	return func(o *HostOption) {
		o.ip = ip
	}
}

func WithPort(port int) HostOptionFunc {
	return func(o *HostOption) {
		o.port = port
	}
}

func WithPrivateKey(key crypto.PrivKey) HostOptionFunc {
	return func(o *HostOption) {
		o.privateKey = key
	}
}

func WithUserAgent(userAgent string) HostOptionFunc {
	return func(o *HostOption) {
		o.userAgent = userAgent
	}
}

func WithResourceManager(rcMgr network.ResourceManager) HostOptionFunc {
	return func(o *HostOption) {
		o.rcMgr = rcMgr
	}
}

func WithConnMgr(connMgr connmgr.ConnManager) HostOptionFunc {
	return func(o *HostOption) {
		o.connMgr = connMgr
	}
}

type Host struct {
	host.Host
}

func NewHost(opts ...HostOptionFunc) (*Host, error) {
	o := &HostOption{
		ip:         "127.0.0.1",
		port:       8080,
		privateKey: nil,
		userAgent:  "libp2p-host",
		rcMgr:      nil,
		connMgr:    connmgr.NullConnMgr{}, // TODO: need to custom connection manager?
	}

	for _, opt := range opts {
		opt(o)
	}

	if o.privateKey == nil {
		privateKey, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate secp256k1 private key")
		}
		o.privateKey = privateKey
	}

	if o.rcMgr == nil {
		// TODO: need to custom resource manager?
		rcMgr, err := rcmgr.NewResourceManager(
			rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create libp2p resource manager")
		}
		o.rcMgr = rcMgr
	}

	var multiaddr ma.Multiaddr
	var err error
	parsed := net.ParseIP(o.ip)
	if parsed == nil {
		return nil, errors.New("failed to parse ip address")
	}
	if parsed.To16() != nil {
		return nil, errors.New("does not support ipv6")
	}
	if parsed.To4() != nil {
		multiaddr, err = ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", o.ip, o.port))
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse tcp multiaddr")
		}
	}

	libp2pHost, err := libp2p.New(
		libp2p.ListenAddrs(multiaddr),
		libp2p.Identity(o.privateKey),
		libp2p.UserAgent(o.userAgent),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.NATPortMap(),
		libp2p.ResourceManager(o.rcMgr),
		libp2p.ConnectionManager(o.connMgr),
		//libp2p.Peerstore(), // TODO: need to impl?
		//libp2p.BandwidthReporter(), // TODO: need to impl?
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create libp2p host")
	}

	slog.With("multiaddr", multiaddr).
		With("pid", libp2pHost.ID().String()).
		Info("successfully created libp2p host")

	return &Host{
		libp2pHost,
	}, nil
}

func (h *Host) Serve(ctx context.Context) error {
	slog.Info("starting host service")
	defer slog.Info("stopping host service")

	notifiee := &network.NotifyBundle{
		ListenF:       listenFunc,
		ListenCloseF:  listenCloseFunc,
		ConnectedF:    connectedFunc,
		DisconnectedF: disconnectedFunc,
	}
	h.Network().Notify(notifiee)
	defer h.Network().StopNotify(notifiee)

	err := h.Network().Listen()
	if err != nil {
		return errors.Wrap(err, "failed to start libp2p network")
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if h.Network() == nil {
					continue
				}
				slog.With("inbound_peers", h.InboundPeerCount()).
					With("outbound_peers", h.OutboundPeerCount()).
					Info("connected peers status")
			}
		}
	}()

	<-ctx.Done()
	return ctx.Err()
}

func (h *Host) TotalPeerCount() int {
	peers := make(map[peer.ID]struct{})
	for _, conn := range h.Network().Conns() {
		peers[conn.RemotePeer()] = struct{}{}
	}
	return len(peers)
}

func (h *Host) InboundPeerCount() int {
	peers := make(map[peer.ID]struct{})
	for _, conn := range h.Network().Conns() {
		if conn.Stat().Direction != network.DirInbound {
			continue
		}
		peers[conn.RemotePeer()] = struct{}{}
	}
	return len(peers)
}

func (h *Host) OutboundPeerCount() int {
	peers := make(map[peer.ID]struct{})
	for _, conn := range h.Network().Conns() {
		if conn.Stat().Direction != network.DirOutbound {
			continue
		}
		peers[conn.RemotePeer()] = struct{}{}
	}
	return len(peers)
}
