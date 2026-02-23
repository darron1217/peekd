package gossip

import (
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/post-pectra/peekd/eth"
)

const (
	blockWeight                = 0.8
	aggregateWeight            = 0.5
	syncContributionWeight     = 0.2
	attestationTotalWeight     = 1
	syncCommitteesTotalWeight  = 0.4
	attesterSlashingWeight     = 0.05
	proposerSlashingWeight     = 0.05
	voluntaryExitWeight        = 0.05
	blsToExecutionChangeWeight = 0.05

	maxInMeshScore        = 10
	maxFirstDeliveryScore = 40

	decayToZero     = 0.01
	dampeningFactor = 90
)

var (
	maxScore = (maxInMeshScore + maxFirstDeliveryScore) * (blockWeight + aggregateWeight + syncContributionWeight + attestationTotalWeight +
		syncContributionWeight + attesterSlashingWeight + proposerSlashingWeight + voluntaryExitWeight + blsToExecutionChangeWeight)
)

type peerScore struct {
	estimateActiveValidators uint64
	topics                   []string
	inspectPeriod            time.Duration

	snapshotsMu sync.Mutex
	snapshots   map[peer.ID]*pubsub.PeerScoreSnapshot
}

func newPeerScore(estimateActiveValidators uint64, topics []string, inspectPeriod time.Duration) *peerScore {
	return &peerScore{
		estimateActiveValidators: estimateActiveValidators,
		topics:                   topics,
		inspectPeriod:            inspectPeriod,
		snapshots:                make(map[peer.ID]*pubsub.PeerScoreSnapshot),
	}
}

func (ps *peerScore) inspect(scores map[peer.ID]*pubsub.PeerScoreSnapshot) {
	ps.snapshotsMu.Lock()
	ps.snapshots = scores
	ps.snapshotsMu.Unlock()
}

func (ps *peerScore) get(pid peer.ID) float64 {
	ps.snapshotsMu.Lock()
	snapshot, ok := ps.snapshots[pid]
	ps.snapshotsMu.Unlock()

	if ok {
		return snapshot.Score
	} else {
		return 0
	}
}

func (ps *peerScore) params() (*pubsub.PeerScoreParams, *pubsub.PeerScoreThresholds) {
	topicParams := make(map[string]*pubsub.TopicScoreParams)
	for _, topic := range ps.topics {
		switch {
		case strings.Contains(topic, p2p.GossipBlockMessage):
			topicParams[topic] = ps.defaultTopicParams()
		case strings.Contains(topic, p2p.GossipBlobSidecarMessage):
			topicParams[topic] = ps.defaultTopicParams()
		case strings.Contains(topic, p2p.GossipAggregateAndProofMessage):
			topicParams[topic] = ps.defaultAggregateTopicParams()
		case strings.Contains(topic, p2p.GossipAttestationMessage):
			topicParams[topic] = ps.defaultAttestationTopicParams()
		case strings.Contains(topic, p2p.GossipContributionAndProofMessage):
			topicParams[topic] = ps.defaultSyncContributionTopicParams()
		case strings.Contains(topic, p2p.GossipSyncCommitteeMessage):
			topicParams[topic] = ps.defaultSyncSubnetTopicParams()
		case strings.Contains(topic, p2p.GossipProposerSlashingMessage):
			topicParams[topic] = ps.defaultProposerSlashingTopicParams()
		case strings.Contains(topic, p2p.GossipAttesterSlashingMessage):
			topicParams[topic] = ps.defaultAttesterSlashingTopicParams()
		case strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage):
			topicParams[topic] = ps.defaultBlsToExecutionChangeTopicParams()
		case strings.Contains(topic, p2p.GossipExitMessage):
			topicParams[topic] = ps.defaultVoluntaryExitTopicParams()
		default:
			slog.With("topic", topic).
				Warn("unknown gossip topic to peer scoring")
			topicParams[topic] = &pubsub.TopicScoreParams{}
		}
	}

	scoreParams := &pubsub.PeerScoreParams{
		Topics:                      topicParams,
		TopicScoreCap:               32.72,
		AppSpecificScore:            func(p peer.ID) float64 { return 0 },
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       ps.scoreDecay(10 * eth.GetEpochDuration()),
		DecayInterval:               eth.GetEpochDuration(),
		DecayToZero:                 decayToZero,
		RetainScore:                 100 * eth.GetEpochDuration(),
	}

	thresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}

	return scoreParams, thresholds
}

func (ps *peerScore) defaultTopicParams() *pubsub.TopicScoreParams {
	decayEpoch := uint64(5)
	twentyEpochDuration := 20 * eth.GetEpochDuration()
	blocksPerEpoch := uint64(eth.GetBeaconChainConfig().SlotsPerEpoch)
	meshWeight := -0.717
	return &pubsub.TopicScoreParams{
		TopicWeight:                     blockWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    1,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(twentyEpochDuration),
		FirstMessageDeliveriesCap:       23,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      ps.scoreDecay(time.Duration(decayEpoch) * eth.GetEpochDuration()),
		MeshMessageDeliveriesCap:        float64(blocksPerEpoch * decayEpoch),
		MeshMessageDeliveriesThreshold:  float64(blocksPerEpoch*decayEpoch) / 10,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 4 * eth.GetEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         ps.scoreDecay(time.Duration(decayEpoch) * eth.GetEpochDuration()),
		InvalidMessageDeliveriesWeight:  -140.4475,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultAggregateTopicParams() *pubsub.TopicScoreParams {
	comms := ps.estimateActiveValidators / uint64(eth.GetBeaconChainConfig().SlotsPerEpoch) / eth.GetBeaconChainConfig().TargetCommitteeSize
	switch {
	case comms > eth.GetBeaconChainConfig().MaxCommitteesPerSlot:
		comms = eth.GetBeaconChainConfig().MaxCommitteesPerSlot
	default:
		comms = 1
	}
	aggPerSlot := comms * eth.GetBeaconChainConfig().TargetAggregatorsPerCommittee

	firstMsgCap := ps.decayLimit(ps.scoreDecay(eth.GetEpochDuration()), float64(aggPerSlot*2/gossipSubD))
	firstMsgWeight := maxFirstDeliveryScore / firstMsgCap

	meshThreshold := ps.decayThreshold(ps.scoreDecay(eth.GetEpochDuration()), float64(aggPerSlot)/dampeningFactor)
	meshWeight := -ps.scoreByWeight(aggregateWeight, meshThreshold)
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     aggregateWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    firstMsgWeight,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       firstMsgCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      ps.scoreDecay(eth.GetEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * eth.GetEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         ps.scoreDecay(eth.GetEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore / aggregateWeight,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultAttestationTopicParams() *pubsub.TopicScoreParams {
	subnetCnt := eth.GetBeaconChainConfig().AttestationSubnetCount

	topicWeight := attestationTotalWeight / float64(subnetCnt)
	subnetWeight := ps.estimateActiveValidators / subnetCnt
	if subnetWeight == 0 {
		panic(errors.New("subnet weight cannot be 0"))
	}

	valsPerSlot := subnetWeight / uint64(eth.GetBeaconChainConfig().SlotsPerEpoch)
	if valsPerSlot == 0 {
		panic(errors.New("number of validators cannot be 0"))
	}

	comms := ps.estimateActiveValidators / uint64(eth.GetBeaconChainConfig().SlotsPerEpoch) / eth.GetBeaconChainConfig().TargetCommitteeSize
	switch {
	case comms > eth.GetBeaconChainConfig().MaxCommitteesPerSlot:
		comms = eth.GetBeaconChainConfig().MaxCommitteesPerSlot
	default:
		comms = 1
	}

	firstDecay := time.Duration(1)
	meshDecay := time.Duration(4)
	if comms >= 2*subnetCnt/uint64(eth.GetBeaconChainConfig().SlotsPerEpoch) {
		firstDecay = 4
		meshDecay = 16
	}

	rate := valsPerSlot * 2 / gossipSubD
	if rate == 0 {
		panic(errors.New("rate cannot be 0"))
	}

	firstMsgCap := ps.decayLimit(ps.scoreDecay(firstDecay*eth.GetEpochDuration()), float64(rate))
	firstMsgWeight := maxFirstDeliveryScore / firstMsgCap

	meshThreshold := ps.decayThreshold(ps.scoreDecay(meshDecay*eth.GetEpochDuration()), float64(valsPerSlot/dampeningFactor))
	meshWeight := -ps.scoreByWeight(topicWeight, meshThreshold)
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    firstMsgWeight,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(firstDecay * eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       firstMsgCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      ps.scoreDecay(meshDecay * eth.GetEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: eth.GetEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         ps.scoreDecay(meshDecay * eth.GetEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore / topicWeight,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultSyncContributionTopicParams() *pubsub.TopicScoreParams {
	aggPerSlot := eth.GetBeaconChainConfig().SyncCommitteeSubnetCount * eth.GetBeaconChainConfig().TargetAggregatorsPerSyncSubcommittee
	firstMsgCap := ps.decayLimit(ps.scoreDecay(eth.GetEpochDuration()), float64(aggPerSlot*2/gossipSubD))
	firstMsgWeight := maxFirstDeliveryScore / firstMsgCap

	meshThreshold := ps.decayThreshold(ps.scoreDecay(eth.GetEpochDuration()), float64(aggPerSlot)/dampeningFactor)
	meshWeight := -ps.scoreByWeight(syncContributionWeight, meshThreshold)
	meshCap := 4 * meshThreshold
	return &pubsub.TopicScoreParams{
		TopicWeight:                     syncContributionWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    firstMsgWeight,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       firstMsgCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      ps.scoreDecay(eth.GetEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: eth.GetEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         ps.scoreDecay(eth.GetEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore / syncContributionWeight,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultSyncSubnetTopicParams() *pubsub.TopicScoreParams {
	if ps.estimateActiveValidators > eth.GetBeaconChainConfig().SyncCommitteeSize {
		ps.estimateActiveValidators = eth.GetBeaconChainConfig().SyncCommitteeSize
	}

	subnetCnt := eth.GetBeaconChainConfig().SyncCommitteeSubnetCount

	topicWeight := syncCommitteesTotalWeight / float64(subnetCnt)
	subnetWeight := ps.estimateActiveValidators / subnetCnt
	if subnetWeight == 0 {
		panic(errors.New("subnet weight cannot be 0"))
	}

	firstDecay := time.Duration(1)
	meshDecay := time.Duration(4)

	rate := subnetWeight * 2 / gossipSubD
	if rate == 0 {
		panic(errors.New("rate cannot be 0"))
	}

	firstMsgCap := ps.decayLimit(ps.scoreDecay(firstDecay*eth.GetEpochDuration()), float64(rate))
	firstMsgWeight := maxFirstDeliveryScore / firstMsgCap

	meshThreshold := ps.decayThreshold(ps.scoreDecay(meshDecay*eth.GetEpochDuration()), float64(subnetWeight/dampeningFactor))
	meshWeight := -ps.scoreByWeight(topicWeight, meshThreshold)
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    firstMsgWeight,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(firstDecay * eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       firstMsgCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      ps.scoreDecay(meshDecay * eth.GetEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: eth.GetEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         ps.scoreDecay(meshDecay * eth.GetEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore / topicWeight,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultProposerSlashingTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     proposerSlashingWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    36,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(100 * eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       1,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultAttesterSlashingTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     attesterSlashingWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    36,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(100 * eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       1,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultBlsToExecutionChangeTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     blsToExecutionChangeWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    2,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(100 * eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       5,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) defaultVoluntaryExitTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     voluntaryExitWeight,
		TimeInMeshWeight:                maxInMeshScore / ps.inMeshCapTime(),
		TimeInMeshQuantum:               ps.inMeshUnitTime(),
		TimeInMeshCap:                   ps.inMeshCapTime(),
		FirstMessageDeliveriesWeight:    2,
		FirstMessageDeliveriesDecay:     ps.scoreDecay(100 * eth.GetEpochDuration()),
		FirstMessageDeliveriesCap:       5,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   ps.scoreDecay(ps.invalidDecayPeriod()),
	}
}

func (ps *peerScore) scoreDecay(totalDecay time.Duration) float64 {
	cnt := totalDecay / eth.GetSlotDuration()
	return math.Pow(decayToZero, 1/float64(cnt))
}

func (ps *peerScore) decayThreshold(decayRate, rate float64) float64 {
	return ps.decayLimit(decayRate, rate) * decayRate
}

func (ps *peerScore) decayLimit(decayRate, rate float64) float64 {
	if decayRate >= 1 {
		panic(errors.Errorf("got an invalid decayLimit rate: %f", decayRate))
	}
	return rate / (1 - decayRate)
}

func (ps *peerScore) scoreByWeight(weight, threshold float64) float64 {
	return maxScore / (weight * threshold * threshold)
}

func (ps *peerScore) inMeshUnitTime() time.Duration {
	return eth.GetSlotDuration()
}

func (ps *peerScore) inMeshCapTime() float64 {
	return float64(1 * time.Hour / ps.inMeshUnitTime())
}

func (ps *peerScore) invalidDecayPeriod() time.Duration {
	return 50 * eth.GetEpochDuration()
}
