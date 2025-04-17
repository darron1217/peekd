package reqresp

import (
	"context"
	"github.com/a41-official/peekd/eth"
	"github.com/a41-official/peekd/host"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"log/slog"
	"sync"
	"time"
)

type ReqRespOption struct {
	host         *host.Host
	ethNetwork   string
	readTimeout  time.Duration
	writeTimeout time.Duration
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

func WithReadTimeout(timeout time.Duration) ReqRespOptionFunc {
	return func(o *ReqRespOption) {
		o.readTimeout = timeout
	}
}

func WithWriteTimeout(timeout time.Duration) ReqRespOptionFunc {
	return func(o *ReqRespOption) {
		o.writeTimeout = timeout
	}
}

type ReqResp struct {
	host *host.Host

	metadataMu sync.RWMutex
	statusMu   sync.RWMutex

	metadata *ethtypes.MetaDataV1
	status   *ethtypes.Status
}

func NewReqResp(opts ...ReqRespOptionFunc) (*ReqResp, error) {
	o := &ReqRespOption{
		host:         nil,
		ethNetwork:   params.MainnetName,
		readTimeout:  time.Duration(0),
		writeTimeout: time.Duration(0),
	}

	for _, opt := range opts {
		opt(o)
	}

	if o.host == nil {
		return nil, errors.New("host must be configured when creating reqresp")
	}

	if o.readTimeout == time.Duration(0) {
		o.readTimeout = eth.GetBeaconChainConfig(o.ethNetwork).TtfbTimeoutDuration()
	}

	if o.writeTimeout == time.Duration(0) {
		o.writeTimeout = eth.GetBeaconChainConfig(o.ethNetwork).RespTimeoutDuration()
	}

	return &ReqResp{}, nil
}

func (rr *ReqResp) Serve(ctx context.Context) error {
	slog.Info("starting reqresp service")
	defer slog.Info("stopping reqresp service")

	<-ctx.Done()
	return nil
}
