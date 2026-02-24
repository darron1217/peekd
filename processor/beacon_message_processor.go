package processor

import (
	"context"
	"encoding/hex"
	"log/slog"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	"github.com/post-pectra/peekd/eth"
	"github.com/post-pectra/peekd/repository"
	ssz "github.com/prysmaticlabs/fastssz"
)

type PeerCounter interface {
	TotalPeerCount() int
}

// SlotCache stores message records for a specific slot
type SlotCache struct {
	Slot         uint64
	MessageStats map[string]*repository.SlotMessageStats // slot -> record
}

// BeaconMessageProcessor processes and stores beacon chain messages
type BeaconMessageProcessor struct {
	enc          encoder.NetworkEncoding
	repo         repository.Repository
	peerCounter  PeerCounter
	beaconConfig *params.BeaconChainConfig
	genesisTime  time.Time
	nodeAlias    string
	nodeRegion   string

	slotCache   *SlotCacheStore
	seenCounter *SeenCounter
	extractors  *slotExtractorRegistry

	// Channel to notify about new messages without blocking
	messageNotify chan struct{}
}

type BeaconMessageProcessorOption struct {
	repo        repository.Repository
	nodeAlias   string
	nodeRegion  string
	peerCounter PeerCounter
}

type BeaconMessageProcessorOptionFunc func(*BeaconMessageProcessorOption)

func WithRepository(repo repository.Repository) BeaconMessageProcessorOptionFunc {
	return func(o *BeaconMessageProcessorOption) {
		o.repo = repo
	}
}

func WithNodeAlias(nodeAlias string) BeaconMessageProcessorOptionFunc {
	return func(o *BeaconMessageProcessorOption) {
		o.nodeAlias = nodeAlias
	}
}

func WithNodeRegion(nodeRegion string) BeaconMessageProcessorOptionFunc {
	return func(o *BeaconMessageProcessorOption) {
		o.nodeRegion = nodeRegion
	}
}

func WithPeerCounter(pc PeerCounter) BeaconMessageProcessorOptionFunc {
	return func(o *BeaconMessageProcessorOption) {
		o.peerCounter = pc
	}
}

func NewBeaconMessageProcessor(opts ...BeaconMessageProcessorOptionFunc) *BeaconMessageProcessor {
	o := &BeaconMessageProcessorOption{
		nodeAlias:   "",
		nodeRegion:  "",
		peerCounter: nil,
	}

	for _, opt := range opts {
		opt(o)
	}

	processor := &BeaconMessageProcessor{
		repo:          o.repo,
		peerCounter:   o.peerCounter,
		nodeAlias:     o.nodeAlias,
		nodeRegion:    o.nodeRegion,
		enc:           encoder.SszNetworkEncoder{},
		beaconConfig:  eth.GetBeaconChainConfig(),
		genesisTime:   eth.GetGenesisConfig().GenesisTime,
		slotCache:     NewSlotCacheStore(),
		seenCounter:   NewSeenCounter(),
		extractors:    newSlotExtractorRegistry(),
		messageNotify: make(chan struct{}, 100), // Buffer to prevent blocking
	}
	slog.Info("successfully created beacon message processor")
	return processor
}

func (p *BeaconMessageProcessor) Process(ctx context.Context, msg *pubsub.Message, dst ssz.Unmarshaler) error {
	if err := p.enc.DecodeGossip(msg.Data, dst); err != nil {
		return errors.Wrap(err, "failed to decode gossip message")
	}

	kind, slot, err := p.extractors.extract(dst)
	if err != nil {
		return err
	}

	switch kind {
	case slotMessage:
		metadata := p.newSlotMetadata(msg, slot)
		p.processSlotMessageMetadata(metadata)
	case generalMessage:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
	}

	return nil
}

func (p *BeaconMessageProcessor) processGeneralMessageMetadata(
	metadata *GeneralMessageMetadata,
) error {
	slog.Debug("processing general message metadata", "topic", metadata.Topic, "msg_id", metadata.MsgID, "msg_size", metadata.MsgSize)

	forkVersion, messageType := parseEth2Topic(metadata.Topic)

	record := &repository.GeneralMessageHistory{
		ArrivalTime:   metadata.MsgArrival,
		ForkVersion:   forkVersion,
		Topic:         messageType,
		NodeRegion:    p.nodeRegion,
		NodeAlias:     p.nodeAlias,
		NodePeerCount: uint32(p.peerCounter.TotalPeerCount()),
		MessageID:     metadata.MsgID,
		SizeBytes:     uint32(metadata.MsgSize),
	}

	// async save to repository
	go func(r *repository.GeneralMessageHistory) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := p.repo.SaveGeneralMessageHistory(ctx, r); err != nil {
			slog.With("error", err).
				With("msg_id", r.MessageID).
				Error("failed to save general message history")
		}
	}(record)

	return nil
}

func (p *BeaconMessageProcessor) processSlotMessageMetadata(
	metadata *SlotMessageMetadata,
) error {

	slog.Debug("processing slot message metadata", "topic", metadata.Topic, "msg_id", metadata.MsgID, "msg_size", metadata.MsgSize, "msg_delay_in_slot", metadata.MsgDelayInSlot, "slot", metadata.Slot)

	slotStartTime := p.genesisTime.Add((time.Duration(metadata.Slot) * time.Second * time.Duration(p.beaconConfig.SecondsPerSlot)))
	forkVersion, messageType := parseEth2Topic(metadata.Topic)

	record := &repository.SlotMessageStats{
		Slot:             metadata.Slot,
		TopicGroup:       TopicToTopicGroup(metadata.Topic),
		ForkVersion:      forkVersion,
		Topic:            messageType,
		NodeRegion:       p.nodeRegion,
		NodeAlias:        p.nodeAlias,
		NodePeerCount:    uint32(p.peerCounter.TotalPeerCount()),
		MessageID:        metadata.MsgID,
		SlotStartTime:    slotStartTime,
		FirstArrivalTime: metadata.MsgArrival,
		LatencyMS:        uint32(metadata.MsgDelayInSlot.Milliseconds()),
		SizeBytes:        uint32(metadata.MsgSize),
	}

	if !p.slotCache.Add(metadata.Slot, metadata.MsgID, record) {
		slog.Error("libp2p pubsub message cannot be processed twice", "msg_id", metadata.MsgID)
	}

	// Notify the processor without blocking
	select {
	case p.messageNotify <- struct{}{}:
	default:
	}

	return nil
}

func (p *BeaconMessageProcessor) newGeneralMetadata(msg *pubsub.Message) *GeneralMessageMetadata {
	msgArrival := time.Now()
	return &GeneralMessageMetadata{
		PeerID:     msg.ReceivedFrom.String(),
		Topic:      msg.GetTopic(),
		MsgID:      hex.EncodeToString([]byte(msg.ID)),
		MsgSize:    len(msg.Data),
		MsgArrival: msgArrival,
	}
}

func (p *BeaconMessageProcessor) newSlotMetadata(msg *pubsub.Message, slot primitives.Slot) *SlotMessageMetadata {
	msgArrival := time.Now()
	return &SlotMessageMetadata{
		PeerID:         msg.ReceivedFrom.String(),
		Topic:          msg.GetTopic(),
		MsgID:          hex.EncodeToString([]byte(msg.ID)),
		MsgSize:        len(msg.Data),
		MsgArrival:     msgArrival,
		MsgDelayInSlot: p.getDelayInSlot(msgArrival, slot),
		Slot:           uint64(slot),
	}
}

func (p *BeaconMessageProcessor) getDelayInSlot(arrivalTime time.Time, slot primitives.Slot) time.Duration {
	// get slot time since genesis
	slotTime := p.genesisTime.Add((time.Duration(slot) * time.Second * time.Duration(p.beaconConfig.SecondsPerSlot)))

	// compare the arrival time to the base-slot time
	inSlotTime := arrivalTime.Sub(slotTime)
	return inSlotTime
}

// getSlotDuration returns the duration of a beacon slot from the beacon config
func (p *BeaconMessageProcessor) getSlotDuration() time.Duration {
	return time.Duration(p.beaconConfig.SecondsPerSlot) * time.Second
}

// Runs in the background to periodically flush completed slots to the database.
func (p *BeaconMessageProcessor) Serve(ctx context.Context) error {
	slog.Info("starting beacon message processor service")
	defer slog.Info("stopping beacon message processor service")

	flushInterval := time.Duration(p.beaconConfig.SecondsPerSlot) * time.Second
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Final flush before shutdown
			p.flushCompletedSlots()
			return nil
		case <-ticker.C:
			// Regular interval check
			p.flushCompletedSlots()
		case <-p.messageNotify:
			// A new message was processed, but we don't need to do anything immediately
			// The timer will handle flushing
		}
	}
}

// flushCompletedSlots identifies completed slots and saves them to the database
func (p *BeaconMessageProcessor) flushCompletedSlots() {
	currentSlot := uint64(time.Now().Unix()) / uint64(p.getSlotDuration().Seconds())

	for _, slot := range p.slotCache.CompletedSlots(currentSlot) {
		stats := p.slotCache.DrainSlot(slot)
		if len(stats) == 0 {
			continue
		}

		for _, stat := range stats {
			seenCount, ok := p.seenCounter.Pop(stat.MessageID)
			if ok {
				stat.SeenCount = seenCount
			} else {
				stat.SeenCount = 1
			}
		}
		if err := p.saveToRepository(stats); err != nil {
			slog.With("error", err).
				With("slot", stats[0].Slot).
				Error("failed to save slot message stats batch")
		}
	}
}

// saveToRepository saves the stats to the repository in batches
func (p *BeaconMessageProcessor) saveToRepository(stats []*repository.SlotMessageStats) error {
	if len(stats) == 0 {
		return nil
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.repo.SaveSlotMessageStatsMulti(ctx, stats); err != nil {
		return errors.Wrap(err, "failed to save slot message stats batch")
	}

	slog.With(
		"slot", stats[0].Slot,
		"message_count", len(stats),
	).Info("saved message batch to repository")

	return nil
}

// TODO: optimize string search
func TopicToTopicGroup(topic string) string {
	if strings.Contains(topic, p2p.GossipBlockMessage) {
		return p2p.GossipBlockMessage
	} else if strings.Contains(topic, p2p.GossipAggregateAndProofMessage) {
		return p2p.GossipAggregateAndProofMessage
	} else if strings.Contains(topic, p2p.GossipContributionAndProofMessage) {
		return p2p.GossipContributionAndProofMessage
	} else if strings.Contains(topic, p2p.GossipProposerSlashingMessage) {
		return p2p.GossipProposerSlashingMessage
	} else if strings.Contains(topic, p2p.GossipAttesterSlashingMessage) {
		return p2p.GossipAttesterSlashingMessage
	} else if strings.Contains(topic, p2p.GossipExitMessage) {
		return p2p.GossipExitMessage
	} else if strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage) {
		return p2p.GossipBlsToExecutionChangeMessage
	} else if strings.Contains(topic, p2p.GossipAttestationMessage) {
		return p2p.GossipAttestationMessage
	} else if strings.Contains(topic, p2p.GossipSyncCommitteeMessage) {
		return p2p.GossipSyncCommitteeMessage
	} else if strings.Contains(topic, p2p.GossipBlobSidecarMessage) {
		return p2p.GossipBlobSidecarMessage
	}

	return "unknown"
}

// get fork version and message type from the topic
func parseEth2Topic(topic string) (string, string) {
	parts := strings.Split(topic, "/")
	return parts[2], parts[3]
}

func (p *BeaconMessageProcessor) IncreaseSeenCountByRawID(msgIDRaw string) {
	p.seenCounter.IncreaseByRawID(msgIDRaw)
}
