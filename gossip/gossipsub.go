package gossip

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p/core/peer"
	"log/slog"
	"sort"
	"time"

	"github.com/a41-official/peekd/eth"
	"github.com/a41-official/peekd/host"
	"github.com/a41-official/peekd/processor"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	ethmath "github.com/prysmaticlabs/prysm/v5/math"
	"github.com/prysmaticlabs/prysm/v5/network/forks"
	"github.com/thejerf/suture/v4"
)

const (
	gossipThreshold = -100.0

	gossipSubD   = 8
	gossipSubDlo = 6
	gossipSubDhi = 12

	gossipSubMCacheLen    = 6
	gossipSubMCacheGossip = 3

	gossipSubHeartbeatInterval = 700 * time.Millisecond
)

type GossipSubOption struct {
	supervisor *suture.Supervisor
	host       *host.Host

	topics                   []string
	estimateActiveValidators uint64

	peerScoreInspectPeriod time.Duration

	messageProcessor *processor.BeaconMessageProcessor
}

type GossipSubOptionFunc func(*GossipSubOption)

func WithEstimateActiveValidators(estimateActiveValidators uint64) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.estimateActiveValidators = estimateActiveValidators
	}
}

func WithTopics(topics ...string) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.topics = topics
	}
}

func WithPeerScoreInspectPeriod(period time.Duration) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.peerScoreInspectPeriod = period
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

func WithMessageProcessor(processor *processor.BeaconMessageProcessor) GossipSubOptionFunc {
	return func(o *GossipSubOption) {
		o.messageProcessor = processor
	}
}

type GossipSub struct {
	supervisor       *suture.Supervisor
	host             *host.Host
	peerScore        *peerScore
	topics           []string
	forkVersion      [4]byte
	beaconConfig     *params.BeaconChainConfig
	messageProcessor *processor.BeaconMessageProcessor
}

func NewGossipSub(opts ...GossipSubOptionFunc) (*GossipSub, error) {
	o := &GossipSubOption{
		estimateActiveValidators: 0,
		topics:                   make([]string, 0),
		peerScoreInspectPeriod:   12 * time.Second,
		supervisor:               suture.NewSimple("gossipSub"),
		host:                     nil,
	}

	for _, opt := range opts {
		opt(o)
	}
	if o.host == nil {
		return nil, errors.New("host must be configured when creating gossipSub")
	}

	if o.estimateActiveValidators == 0 {
		return nil, errors.New("number of estimate active validators must be more than 0 when creating gossipSub")
	}

	if len(o.topics) == 0 {
		genesisConfig := eth.GetGenesisConfig()
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
			// TODO: add light client topics
			// p2p.LightClientFinalityUpdateSubnetTopicFormat,
			// p2p.LightClientOptimisticUpdateSubnetTopicFormat,
		}

		allTopics := make([]string, 0)
		for _, rawTopic := range rawAllTopics {
			subnetCnt, hasSubnet := eth.HasSubnets(rawTopic)
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

	slog.With("topics", o.topics).
		Info("successfully created gossipSub")

	return &GossipSub{
		supervisor:       o.supervisor,
		host:             o.host,
		peerScore:        newPeerScore(o.estimateActiveValidators, o.topics, o.peerScoreInspectPeriod),
		topics:           o.topics,
		forkVersion:      eth.GetCurrentForkVersion(),
		beaconConfig:     eth.GetBeaconChainConfig(),
		messageProcessor: o.messageProcessor,
	}, nil
}

func (gs *GossipSub) Serve(ctx context.Context) error {
	slog.Info("starting gossipSub service")
	defer slog.Info("stopping gossipSub service")

	gossipSub, err := pubsub.NewGossipSub(
		ctx,
		gs.host,
		pubsub.WithGossipSubParams(gs.gossipSubParams()),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			return p2p.MsgID(gs.beaconConfig.GenesisValidatorsRoot[:], pmsg)
		}),
		pubsub.WithMaxMessageSize(gs.maxMessageSize()),
		pubsub.WithPeerScore(gs.peerScore.params()),
		pubsub.WithPeerScoreInspect(gs.peerScore.inspect, gs.peerScore.inspectPeriod),
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

		topicHandler := gs.mappingTopicToHandler(topicName)

		gs.supervisor.Add(newSubscription(gs.host.ID(), subscription, topicHandler))
	}

	go func() {
		ticker := time.NewTicker(eth.GetSlotDuration())
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				gs.prune()
			}
		}
	}()

	<-ctx.Done()
	return ctx.Err()
}

func (gs *GossipSub) isBadPeer(pid peer.ID) bool {
	if gs.peerScore.get(pid) < gossipThreshold {
		return true
	} else {
		return false
	}
}

func (gs *GossipSub) gossipSubParams() pubsub.GossipSubParams {
	gParams := pubsub.DefaultGossipSubParams()
	gParams.D = gossipSubD
	gParams.Dlo = gossipSubDlo
	gParams.Dhi = gossipSubDhi
	gParams.HistoryLength = gossipSubMCacheLen
	gParams.HistoryGossip = gossipSubMCacheGossip
	gParams.HeartbeatInterval = gossipSubHeartbeatInterval
	return gParams
}

func (gs *GossipSub) maxMessageSize() int {
	maxCompressedLen := encoder.MaxCompressedLen(eth.GetBeaconChainConfig().MaxPayloadSize)
	return int(ethmath.Max(maxCompressedLen+1024, 1024*1024))
}

func (gs *GossipSub) prune() {
	if gs.host.Network() == nil {
		return
	}

	type pair struct {
		id    peer.ID
		score float64
	}

	peerScores := make(map[peer.ID]float64)
	for _, conn := range gs.host.Network().Conns() {
		pid := conn.RemotePeer()
		score := gs.peerScore.get(pid)
		if prev, ok := peerScores[pid]; !ok || score < prev {
			peerScores[pid] = score
		}
	}

	pairs := make([]pair, 0)
	for pid, score := range peerScores {
		pairs = append(pairs, pair{pid, score})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].score < pairs[j].score
	})

	excess := gs.host.TotalPeerCount() - gs.host.TargetPeerCount()
	pruned := 0

	for i, p := range pairs {
		if !(i < excess || gs.isBadPeer(p.id)) {
			break
		}

		_ = gs.host.Network().ClosePeer(p.id)
		pruned += 1
		continue
	}

	if pruned > 0 {
		slog.With("count", pruned).
			Info("pruned peers")
	}
}
