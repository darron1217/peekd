package peering

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"github.com/a41-official/peekd/eth"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/network/forks"
	pb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
	"net"

	"log/slog"
)

type DiscoveryOption struct {
	privateKey *ecdsa.PrivateKey

	ip      string
	portUDP int
	portTCP int

	ethNetwork string
}

type DiscoveryOptionFunc func(*DiscoveryOption)

func WithPrivateKey(key *ecdsa.PrivateKey) DiscoveryOptionFunc {
	return func(o *DiscoveryOption) {
		o.privateKey = key
	}
}

func WithIP(ip string) DiscoveryOptionFunc {
	return func(o *DiscoveryOption) {
		o.ip = ip
	}
}

func WithPortUDP(port int) DiscoveryOptionFunc {
	return func(o *DiscoveryOption) {
		o.portUDP = port
	}
}

func WithPortTCP(port int) DiscoveryOptionFunc {
	return func(o *DiscoveryOption) {
		o.portTCP = port
	}
}

func WithEthNetwork(ethNetwork string) DiscoveryOptionFunc {
	return func(o *DiscoveryOption) {
		o.ethNetwork = ethNetwork
	}
}

type Discovery struct {
	privateKey *ecdsa.PrivateKey
	node       *enode.LocalNode
	discovers  chan *peer.AddrInfo

	bootstrapNodes []*enode.Node
	forkDigest     [4]byte
}

func NewDiscovery(opts ...DiscoveryOptionFunc) (*Discovery, error) {
	o := &DiscoveryOption{
		privateKey: nil,
		ip:         "127.0.0.1",
		portUDP:    8080,
		portTCP:    8080,
		ethNetwork: params.MainnetName,
	}

	for _, opt := range opts {
		opt(o)
	}

	if o.privateKey == nil {
		privateKey, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate ecdsa private key")
		}
		o.privateKey = privateKey
	}

	memDB, err := enode.OpenDB("")
	if err != nil {
		return nil, errors.Wrap(err, "failed to open peer's database")
	}

	attestBitV := bitfield.NewBitvector64()
	for i := uint64(0); i < params.BeaconConfig().AttestationSubnetCount; i++ {
		attestBitV.SetBitAt(i, true)
	}

	syncBitV := bitfield.Bitvector4{byte(0x00)}
	for i := uint64(0); i < params.BeaconConfig().SyncCommitteeSubnetCount; i++ {
		syncBitV.SetBitAt(i, true)
	}

	genesisConfig := eth.GetGenesisConfig(o.ethNetwork)
	networkConfig := eth.GetBeaconNetworkConfig(o.ethNetwork)

	forkDigest, err := forks.CreateForkDigest(genesisConfig.GenesisTime, genesisConfig.GenesisValidatorRoot)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create fork digest")
	}

	nextForkVersion, nextForkEpoch, err := forks.NextForkData(slots.ToEpoch(slots.Since(genesisConfig.GenesisTime)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get next fork data")
	}

	forkID := &pb.ENRForkID{
		CurrentForkDigest: forkDigest[:],
		NextForkVersion:   nextForkVersion[:],
		NextForkEpoch:     nextForkEpoch,
	}

	forkIDBytes, err := forkID.MarshalSSZ()
	if err != nil {
		panic(err)
	}

	localNode := enode.NewLocalNode(memDB, o.privateKey)
	localNode.Set(enr.IP(o.ip))
	localNode.Set(enr.UDP(o.portUDP))
	localNode.Set(enr.TCP(o.portTCP))
	localNode.Set(enr.WithEntry(networkConfig.AttSubnetKey, attestBitV.Bytes()))
	localNode.Set(enr.WithEntry(networkConfig.SyncCommsSubnetKey, syncBitV.Bytes()))
	localNode.Set(enr.WithEntry(networkConfig.ETH2Key, forkIDBytes))

	bootstrapNodes := make([]*enode.Node, 0)
	for _, enrStr := range networkConfig.BootstrapNodes {
		bootstrapNode, err := enode.Parse(enode.ValidSchemes, enrStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse bootstrap node enr %s", enrStr)
		}
		bootstrapNodes = append(bootstrapNodes, bootstrapNode)
	}

	slog.With("local node id", localNode.ID().String()).
		With("local node ip", localNode.Node().IP().String()).
		With("local node udp port", localNode.Node().UDP()).
		With("local node tcp port", localNode.Node().TCP()).
		Info("successfully created discovery")

	return &Discovery{
		privateKey:     o.privateKey,
		node:           localNode,
		bootstrapNodes: bootstrapNodes,
		forkDigest:     forkDigest,
		discovers:      make(chan *peer.AddrInfo),
	}, nil
}

func (d *Discovery) Discovers() chan *peer.AddrInfo {
	return d.discovers
}

func (d *Discovery) Serve(ctx context.Context) error {
	slog.Info("starting discovery service")
	defer slog.Info("stopping discovery service")

	ip := d.node.Node().IP()
	var bindIP net.IP
	var networkVersion string

	switch {
	case ip.To4() != nil:
		bindIP = net.IPv4zero
		networkVersion = "udp4"
	default:
		return fmt.Errorf("invalid IP address: %s", ip)
	}

	udpAddr := &net.UDPAddr{
		IP:   bindIP,
		Port: d.node.Node().UDP(),
	}

	conn, err := net.ListenUDP(networkVersion, udpAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s:%d", bindIP, d.node.Node().UDP())
	}

	discvCfg := discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  d.bootstrapNodes,
	}

	listener, err := discover.ListenV5(conn, d.node, discvCfg)
	if err != nil {
		return errors.Wrap(err, "failed to start discv5 listener")
	}
	defer listener.Close()

	iterator := listener.RandomNodes()
	defer iterator.Close()
	defer close(d.discovers)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if !iterator.Next() {
			return nil
		}

		node := iterator.Node()
		if node.IP().IsPrivate() {
			continue
		}

		slog.With("enr", node.String()).
			With("node_id", node.ID().String()).
			Debug("discovered new node")

		addrInfo, err := extractPeerAddrInfo(node)
		if err != nil {
			slog.With("enr", node.String()).
				Error("failed to extract peer address info")
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case d.discovers <- addrInfo:
		}
	}
}

func extractPeerAddrInfo(node *enode.Node) (*peer.AddrInfo, error) {
	ecdsaPubKey := node.Pubkey()
	ecdsaPubBytes := gcrypto.FromECDSAPub(ecdsaPubKey)

	secp256k1PubKey, err := crypto.UnmarshalSecp256k1PublicKey(ecdsaPubBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal secp256k1 public key from node's ecdsa public key")
	}

	pid, err := peer.IDFromPublicKey(secp256k1PubKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract peer ID from node's secp256k1 public key")
	}

	var ipScheme string
	if v4 := node.IP().To4(); len(v4) == net.IPv4len {
		ipScheme = "ip4"
	} else if v6 := node.IP().To16(); len(v6) == net.IPv6len {
		ipScheme = "ip6"
	} else {
		return nil, fmt.Errorf("invalid ip scheme from node's ip %s", node.IP())
	}

	maddrs := make([]ma.Multiaddr, 0)
	if node.UDP() != 0 {
		maddr, err := ma.NewMultiaddr(fmt.Sprintf("/%s/%s/udp/%d", ipScheme, node.IP(), node.UDP()))
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse udp multiaddr")
		}
		maddrs = append(maddrs, maddr)
	}
	if node.TCP() != 0 {
		maddr, err := ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%d", ipScheme, node.IP(), node.TCP()))
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse tcp multiaddr")
		}
		maddrs = append(maddrs, maddr)
	}

	return &peer.AddrInfo{
		ID:    pid,
		Addrs: maddrs,
	}, nil
}
