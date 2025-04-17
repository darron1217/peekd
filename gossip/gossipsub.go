package gossip

import (
	"context"
	"fmt"
	"github.com/a41-official/peekd/eth"
	"github.com/a41-official/peekd/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/network/forks"
	"github.com/thejerf/suture/v4"
	"log/slog"
	"time"
)

type GossipSubOption struct {
	supervisor *suture.Supervisor
	host       *host.Host

	topics     []string
	ethNetwork string

	peerScoreInspectFunc pubsub.ExtendedPeerScoreInspectFn
	peerScorePeriod      time.Duration
}

type GossipSubOptionFunc func(*GossipSubOption)

func WithEthNetwork(ethNetwork string) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.ethNetwork = ethNetwork
	}
}

func WithTopics(topics ...string) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.topics = topics
	}
}

func WithPeerScoreInspectFunc(inspectFunc pubsub.ExtendedPeerScoreInspectFn) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.peerScoreInspectFunc = inspectFunc
	}
}

func WithPeerScorePeriod(period time.Duration) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.peerScorePeriod = period
	}
}

func WithSupervisor(sup *suture.Supervisor) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.supervisor = sup
	}
}

func WithHost(h *host.Host) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.host = h
	}
}

type GossipSub struct {
	supervisor           *suture.Supervisor
	host                 *host.Host
	peerScoreInspectFunc pubsub.ExtendedPeerScoreInspectFn
	peerScorePeriod      time.Duration
	ethNetwork           string
	topics               []string
}

func NewGossipSub(opts ...GossipSubOptionFunc) (*GossipSub, error) {
	o := &GossipSubOption{
		ethNetwork:           params.MainnetName,
		topics:               make([]string, 0),
		peerScoreInspectFunc: noopPeerScoreInspectFunc,
		peerScorePeriod:      12 * time.Second,
		supervisor:           suture.NewSimple("gossipSub"),
		host:                 nil,
	}

	for _, opt := range opts {
		opt(o)
	}

	if len(o.topics) == 0 {
		genesisConfig := eth.GetGenesisConfig(o.ethNetwork)
		forkDigest, err := forks.CreateForkDigest(genesisConfig.GenesisTime, genesisConfig.GenesisValidatorRoot)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create fork digest")
		}

		rawAllTopics := []string{
			p2p.BlockSubnetTopicFormat,
			p2p.BlobSubnetTopicFormat,
			p2p.AggregateAndProofSubnetTopicFormat,
			p2p.AttestationSubnetTopicFormat,
			p2p.SyncContributionAndProofSubnetTopicFormat,
			p2p.SyncCommitteeSubnetTopicFormat,
			p2p.ProposerSlashingSubnetTopicFormat,
			p2p.AttesterSlashingSubnetTopicFormat,
			p2p.BlsToExecutionChangeSubnetTopicFormat,
			p2p.ExitSubnetTopicFormat,
		}

		allTopics := make([]string, 0)
		for _, rawTopic := range rawAllTopics {
			subnetCnt, hasSubnet := eth.HasSubnets(o.ethNetwork, rawTopic)
			if hasSubnet {
				for i := uint64(0); i < subnetCnt; i++ {
					allTopics = append(allTopics, fmt.Sprintf(rawTopic, forkDigest, i)+"/"+encoder.ProtocolSuffixSSZSnappy)
				}
			} else {
				allTopics = append(allTopics, fmt.Sprintf(rawTopic, forkDigest)+"/"+encoder.ProtocolSuffixSSZSnappy)
			}
		}

		o.topics = allTopics
	}

	if o.host == nil {
		return nil, errors.New("host must be configured when creating gossipSub")
	}

	return &GossipSub{
		supervisor:           o.supervisor,
		host:                 o.host,
		peerScoreInspectFunc: o.peerScoreInspectFunc,
		peerScorePeriod:      o.peerScorePeriod,
		ethNetwork:           o.ethNetwork,
		topics:               o.topics,
	}, nil
}

// TODO: need to impl custom peer score inspect function

func noopPeerScoreInspectFunc(_ map[peer.ID]*pubsub.PeerScoreSnapshot) {
}

func (gs *GossipSub) Serve(ctx context.Context) error {
	slog.Info("starting gossipSub service")
	defer slog.Info("stopping gossipSub service")

	gossipSub, err := pubsub.NewGossipSub(
		ctx,
		gs.host,
		pubsub.WithPeerScoreInspect(gs.peerScoreInspectFunc, gs.peerScorePeriod),
		//pubsub.WithGossipSubParams(), // TODO: need to custom
		//pubsub.WithMessageIdFn(), // TODO: need to custom
	)
	if err != nil {
		return errors.Wrap(err, "failed to create gossipSub")
	}

	for _, topicName := range gs.topics {
		topic, err := gossipSub.Join(topicName)
		if err != nil {
			return errors.Wrapf(err, "failed to join topic %s", topicName)
		}

		subscription, err := topic.Subscribe()
		if err != nil {
			return errors.Wrapf(err, "failed to subscribe topic %s", topicName)
		}

		gs.supervisor.Add(newSubscription(gs.ethNetwork, gs.host.ID(), subscription))
	}

	return gs.supervisor.Serve(ctx)
}
