package watcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"github.com/a41-official/peekd/gossip"
	"github.com/a41-official/peekd/host"
	"github.com/a41-official/peekd/peering"
	"github.com/a41-official/peekd/reqresp"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/thejerf/suture/v4"
)

// TODO
// log-level
// metric

const UserAgent = "peekd"

type WatcherOption struct {
	ecdsaPrivateKeyHex string
	ip                 string
	portUDP            int
	portTCP            int
	ethNetwork         string
}

type WatcherOptionFunc func(*WatcherOption)

func WithECDSAPrivateKeyHex(ecdsaPrivateKeyHex string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.ecdsaPrivateKeyHex = ecdsaPrivateKeyHex
	}
}

func WithIP(ip string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.ip = ip
	}
}

func WithPortUDP(port int) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.portUDP = port
	}
}

func WithPortTCP(port int) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.portTCP = port
	}
}

func WithEthNetwork(ethNetwork string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.ethNetwork = ethNetwork
	}
}

type Watcher struct {
	supervisor *suture.Supervisor
}

func NewWatcher(opts ...WatcherOptionFunc) (*Watcher, error) {
	o := &WatcherOption{
		ecdsaPrivateKeyHex: "",
		ip:                 "127.0.0.1",
		portUDP:            8080,
		portTCP:            8080,
		ethNetwork:         params.MainnetName,
	}

	for _, opt := range opts {
		opt(o)
	}

	ecdsaKey, secpKey, err := retrievePrivateKeys(o.ecdsaPrivateKeyHex)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve private keys")
	}

	supervisor := suture.NewSimple("watcher")

	localHost, err := host.NewHost(
		host.WithIP(o.ip),
		host.WithPort(o.portTCP),
		host.WithPrivateKey(secpKey),
		host.WithUserAgent(UserAgent),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create host")
	}

	discovery, err := peering.NewDiscovery(
		peering.WithPrivateKey(ecdsaKey),
		peering.WithIP(o.ip),
		peering.WithPortUDP(o.portUDP),
		peering.WithPortTCP(o.portTCP),
		peering.WithEthNetwork(o.ethNetwork),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create discovery")
	}

	dialer, err := peering.NewDialer(
		localHost,
		discovery.Discovers(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create dialer")
	}

	gossipSub, err := gossip.NewGossipSub(
		gossip.WithEthNetwork(o.ethNetwork),
		gossip.WithSupervisor(supervisor),
		gossip.WithHost(localHost),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create gossipSub")
	}

	reqResp, err := reqresp.NewReqResp(
		reqresp.WithHost(localHost),
		reqresp.WithEthNetwork(o.ethNetwork),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create reqresp")
	}

	supervisor.Add(localHost)
	supervisor.Add(discovery)
	supervisor.Add(dialer) // TODO: need to concurrent dial?
	supervisor.Add(gossipSub)
	supervisor.Add(reqResp)

	return &Watcher{
		supervisor: supervisor,
	}, nil
}

func retrievePrivateKeys(ecdsaKeyHex string) (*ecdsa.PrivateKey, crypto.PrivKey, error) {
	var (
		ecdsaKey *ecdsa.PrivateKey
		err      error
	)
	if ecdsaKeyHex != "" {
		ecdsaKey, err = gcrypto.HexToECDSA(ecdsaKeyHex)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse ecdsa private key")
		}
	} else {
		ecdsaKey, err = ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to generate ecdsa private key")
		}
	}

	ecdsaBytes := gcrypto.FromECDSA(ecdsaKey)
	if len(ecdsaBytes) != secp256k1.PrivKeyBytesLen {
		return nil, nil, errors.Errorf("expected secp256k1 data size to be %d, but actual %d", secp256k1.PrivKeyBytesLen, len(ecdsaBytes))
	}

	secpKey, err := crypto.UnmarshalSecp256k1PrivateKey(ecdsaBytes)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to unmarshal secp256k1 private key")
	}

	return ecdsaKey, secpKey, nil
}

func (w *Watcher) Serve(ctx context.Context) error {
	return w.supervisor.Serve(ctx)
}
