package reqresp

import (
	"context"
	"github.com/a41-official/peekd/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
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
	host *host.Host
}

type ReqRespOptionFunc func(*ReqRespOption)

func WithHost(host *host.Host) ReqRespOptionFunc {
	return func(o *ReqRespOption) {
		o.host = host
	}
}

type ReqResp struct {
	host *host.Host
}

func NewReqResp(opts ...ReqRespOptionFunc) (*ReqResp, error) {
	o := &ReqRespOption{
		host: nil,
	}

	for _, opt := range opts {
		opt(o)
	}

	if o.host == nil {
		return nil, errors.New("host must be configured when creating reqresp")
	}

	slog.Info("successfully created reqresp")

	return &ReqResp{
		host: o.host,
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
		rr.host.SetStreamHandler(protocolID(topic), mappingTopicToHandler(topic))
	}

	<-ctx.Done()
	return ctx.Err()
}

func protocolID(topic string) protocol.ID {
	return protocol.ID(topic + "/" + encoder.ProtocolSuffixSSZSnappy)
}
