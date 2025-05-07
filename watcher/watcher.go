package watcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"

	"github.com/a41-official/peekd/eth"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/a41-official/peekd/gossip"
	"github.com/a41-official/peekd/host"
	"github.com/a41-official/peekd/peering"
	"github.com/a41-official/peekd/processor"
	"github.com/a41-official/peekd/repository"
	"github.com/a41-official/peekd/reqresp"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/pkg/errors"
	"github.com/thejerf/suture/v4"
)

const UserAgent = "peekd"

type WatcherOption struct {
	ecdsaPrivateKeyHex       string
	listenIP                 string
	portUDP                  int
	portTCP                  int
	targetPeers              int
	ethNetwork               string
	estimateActiveValidators uint64
	dbType                   string
	dbName                   string
	dbHost                   string
	dbPort                   int
	dbUser                   string
	dbPassword               string
	dbSecure                 bool
	nodeAlias                string
	nodeRegion               string
}

type WatcherOptionFunc func(*WatcherOption)

func WithECDSAPrivateKeyHex(ecdsaPrivateKeyHex string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.ecdsaPrivateKeyHex = ecdsaPrivateKeyHex
	}
}

func WithListenIP(ip string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.listenIP = ip
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

func WithTargetPeers(targetPeers int) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.targetPeers = targetPeers
	}
}

func WithEthNetwork(ethNetwork string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.ethNetwork = ethNetwork
	}
}

func WithEstimateActiveValidators(estimateActiveValidators uint64) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.estimateActiveValidators = estimateActiveValidators
	}
}

func WithDBType(dbType string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbType = dbType
	}
}

func WithDBName(dbName string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbName = dbName
	}
}

func WithDBHost(dbHost string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbHost = dbHost
	}
}

func WithDBPort(dbPort int) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbPort = dbPort
	}
}

func WithDBUser(dbUser string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbUser = dbUser
	}
}

func WithDBPassword(dbPassword string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbPassword = dbPassword
	}
}

func WithDBSecure(dbSecure bool) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.dbSecure = dbSecure
	}
}

func WithNodeAlias(nodeAlias string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.nodeAlias = nodeAlias
	}
}

func WithNodeRegion(nodeRegion string) WatcherOptionFunc {
	return func(o *WatcherOption) {
		o.nodeRegion = nodeRegion
	}
}

type Watcher struct {
	supervisor *suture.Supervisor
	repo       repository.Repository
}

func NewWatcher(opts ...WatcherOptionFunc) (*Watcher, error) {
	o := &WatcherOption{
		ecdsaPrivateKeyHex: "",
		listenIP:           "127.0.0.1",
		portUDP:            9090,
		portTCP:            9090,
		targetPeers:        100,
		ethNetwork:         params.MainnetName,
		nodeAlias:          "",
		nodeRegion:         "",
	}

	for _, opt := range opts {
		opt(o)
	}

	if err := eth.SetNetwork(o.ethNetwork); err != nil {
		return nil, errors.Wrap(err, "failed to set eth network")
	}

	// initialize repository
	repo, err := repository.NewRepository(
		repository.WithDBType(o.dbType),
		repository.WithDBName(o.dbName),
		repository.WithDBHost(o.dbHost),
		repository.WithDBPort(o.dbPort),
		repository.WithDBUser(o.dbUser),
		repository.WithDBPassword(o.dbPassword),
		repository.WithDBSecure(o.dbSecure),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize repository")
	}

	// initialize p2p
	ecdsaKey, secpKey, err := retrievePrivateKeys(o.ecdsaPrivateKeyHex)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve private keys")
	}

	supervisor := suture.NewSimple("watcher")

	localHost, err := host.NewHost(
		host.WithListenIP(o.listenIP),
		host.WithPort(o.portTCP),
		host.WithPrivateKey(secpKey),
		host.WithUserAgent(UserAgent),
		host.WithTargetPeers(o.targetPeers),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create host")
	}

	discovery, err := peering.NewDiscovery(
		peering.WithPrivateKey(ecdsaKey),
		peering.WithListenIP(o.listenIP),
		peering.WithPortUDP(o.portUDP),
		peering.WithPortTCP(o.portTCP),
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

	// initialize message processor
	messageProcessor := processor.NewBeaconMessageProcessor(
		processor.WithRepository(repo),
		processor.WithHost(localHost),
		processor.WithNodeAlias(o.nodeAlias),
		processor.WithNodeRegion(o.nodeRegion),
	)

	// initialize gossipSub
	gossipSub, err := gossip.NewGossipSub(
		gossip.WithEstimateActiveValidators(o.estimateActiveValidators),
		gossip.WithSupervisor(supervisor),
		gossip.WithHost(localHost),
		gossip.WithMessageProcessor(messageProcessor),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create gossipSub")
	}

	reqResp, err := reqresp.NewReqResp(
		reqresp.WithHost(localHost),
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
		repo:       repo,
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
	defer w.repo.Close()
	return w.supervisor.Serve(ctx)
}
