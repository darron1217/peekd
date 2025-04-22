package peering

import (
	"context"
	"github.com/a41-official/peekd/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"log/slog"
	"time"
)

const ConnTimeout = 2 * time.Second

type Dialer struct {
	host      *host.Host
	discovers <-chan *peer.AddrInfo
}

func NewDialer(host *host.Host, discovers <-chan *peer.AddrInfo) (*Dialer, error) {
	if host == nil {
		return nil, errors.New("host should be configured when creating dialer")
	}

	if discovers == nil {
		return nil, errors.New("discovers channel should be configured when creating dialer")
	}

	slog.Info("successfully created dialer")

	return &Dialer{
		host:      host,
		discovers: discovers,
	}, nil
}

func (d *Dialer) Serve(ctx context.Context) error {
	slog.Info("starting dialer service")
	defer slog.Info("stopping dialer service")

	for {
		if float64(d.host.OutboundPeerCount()) >= float64(d.host.TargetPeerCount())*0.2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case addrInfo, ok := <-d.discovers:
			if !ok {
				return nil
			}
			if addrInfo.ID == d.host.ID() {
				continue
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, ConnTimeout)
			err := d.host.Connect(timeoutCtx, *addrInfo)
			cancel()
			if err != nil {
				slog.With("peer_id", addrInfo.ID).
					Debug("failed to connect with peer")
				continue
			}
		}
	}
}
