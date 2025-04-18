package reqresp

import (
	"context"
	"github.com/a41-official/peekd/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"log/slog"
	"time"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 10 * time.Second
)

var (
	enc = encoder.SszNetworkEncoder{}
)

type ReqRespOption struct {
	host       *host.Host
	ethNetwork string
}

type ReqRespOptionFunc func(*ReqRespOption)

func WithHost(host *host.Host) ReqRespOptionFunc {
	return func(o *ReqRespOption) {
		o.host = host
	}
}

func WithEthNetwork(ethNetwork string) ReqRespOptionFunc {
	return func(o *ReqRespOption) {
		o.ethNetwork = ethNetwork
	}
}

type ReqResp struct {
	host       *host.Host
	ethNetwork string
}

func NewReqResp(opts ...ReqRespOptionFunc) (*ReqResp, error) {
	o := &ReqRespOption{
		host:       nil,
		ethNetwork: params.MainnetName,
	}

	for _, opt := range opts {
		opt(o)
	}

	if o.host == nil {
		return nil, errors.New("host must be configured when creating reqresp")
	}

	return &ReqResp{
		host:       o.host,
		ethNetwork: o.ethNetwork,
	}, nil
}

func (rr *ReqResp) Serve(ctx context.Context) error {
	slog.Info("starting reqresp service")
	defer slog.Info("stopping reqresp service")

	topics := []string{
		p2p.RPCPingTopicV1,
		p2p.RPCGoodByeTopicV1,
		p2p.RPCStatusTopicV1,
		p2p.RPCMetaDataTopicV1,
		p2p.RPCMetaDataTopicV2,
	}
	for _, topic := range topics {
		rr.host.SetStreamHandler(protocolID(topic), mappingTopicToHandler(rr.ethNetwork, topic))
	}

	<-ctx.Done()
	return ctx.Err()
}

func protocolID(topic string) protocol.ID {
	return protocol.ID(topic + "/" + encoder.ProtocolSuffixSSZSnappy)
}
