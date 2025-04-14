package host

import (
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"log/slog"
	"time"
)

type notifyEvent struct {
	notifyType string
	timestamp  time.Time
	payload    any
}

func listenNotifiee(network network.Network, multiaddr ma.Multiaddr) {
	slog.Debug("Listen")
}

func listenCloseNotifiee(network network.Network, multiaddr ma.Multiaddr) {
	slog.Debug("Close Listen")
}

func connectedNotifiee(network network.Network, conn network.Conn) {
	event := notifyEvent{
		notifyType: "CONNECTED",
		timestamp:  time.Now(),
		payload: struct {
			RemotePeer      string
			RemoteMultiaddr ma.Multiaddr
			Direction       string
			Opened          time.Time
			Limited         bool
		}{
			RemotePeer:      conn.RemotePeer().String(),
			RemoteMultiaddr: conn.RemoteMultiaddr(),
			Direction:       conn.Stat().Direction.String(),
			Opened:          conn.Stat().Opened,
			Limited:         conn.Stat().Limited,
		},
	}

	slog.With("event", event).
		Debug("Connected")
}

func disconnectedNotifiee(network network.Network, conn network.Conn) {
	event := notifyEvent{
		notifyType: "DISCONNECTED",
		timestamp:  time.Now(),
		payload: struct {
			RemotePeer      string
			RemoteMultiaddr ma.Multiaddr
			Direction       string
			Opened          time.Time
			Limited         bool
		}{
			RemotePeer:      conn.RemotePeer().String(),
			RemoteMultiaddr: conn.RemoteMultiaddr(),
			Direction:       conn.Stat().Direction.String(),
			Opened:          conn.Stat().Opened,
			Limited:         conn.Stat().Limited,
		},
	}

	slog.With("event", event).
		Debug("Disconnected")
}
