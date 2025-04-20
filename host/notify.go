package host

import (
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"log/slog"
)

func listenFunc(_ network.Network, _ ma.Multiaddr) {}

func listenCloseFunc(_ network.Network, _ ma.Multiaddr) {}

func connectedFunc(net network.Network, conn network.Conn) {
	slog.With("peer_id", conn.RemotePeer()).
		With("multiaddr", conn.RemoteMultiaddr()).
		With("direction", conn.Stat().Direction).
		Debug("Peer Connected")
}

func disconnectedFunc(net network.Network, conn network.Conn) {
	slog.With("peer_id", conn.RemotePeer()).
		With("multiaddr", conn.RemoteMultiaddr()).
		With("direction", conn.Stat().Direction).
		Debug("Peer Disconnected")
}
