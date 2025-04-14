package peering

import (
	"context"
	"github.com/a41-official/peekd/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"log/slog"
	"time"
)

const ConnTimeout = 2 * time.Second

type Dialer struct {
	host      *host.Host
	discovers <-chan *peer.AddrInfo
}

func NewDialer(host *host.Host, discovers <-chan *peer.AddrInfo) *Dialer {
	return &Dialer{
		host:      host,
		discovers: discovers,
	}
}

func (d *Dialer) Serve(ctx context.Context) error {
	slog.Info("starting dialer service")
	defer slog.Info("stopping dialer service")

	for {
		var (
			addrInfo *peer.AddrInfo
			ok       bool
		)

		select {
		case <-ctx.Done():
			return nil
		case addrInfo, ok = <-d.discovers:
			if !ok {
				return nil
			}
		}

		if addrInfo.ID == d.host.ID() {
			continue
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, ConnTimeout)
		err := d.host.Connect(timeoutCtx, *addrInfo)
		cancel()
		if err != nil {
			slog.With("peer id", addrInfo.ID).
				Debug("failed to connect with peer")
			continue
		}
	}
}
