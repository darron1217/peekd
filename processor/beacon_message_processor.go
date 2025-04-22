package processor

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/a41-official/peekd/eth"
	"github.com/a41-official/peekd/repository"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

// SlotCache stores message records for a specific slot
type SlotCache struct {
	Slot         uint64
	MessageStats map[string]*repository.SlotMessageStats // slot -> record
}

// BeaconMessageProcessor processes and stores beacon chain messages
type BeaconMessageProcessor struct {
	enc          encoder.NetworkEncoding
	repo         repository.Repository
	beaconConfig *params.BeaconChainConfig
	genesisTime  time.Time
	nodeAlias    string
	nodeRegion   string

	mu         sync.RWMutex
	slotCaches map[uint64]*SlotCache // slot -> cache

	seenMu     sync.Mutex
	seenCounts map[string]uint32 // msg_id -> seen_count

	// Channel to notify about new messages without blocking
	messageNotify chan struct{}

	// Control channels
	done    chan struct{}
	stopped bool
}

type BeaconMessageProcessorOption struct {
	repo       repository.Repository
	nodeAlias  string
	nodeRegion string
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

func NewBeaconMessageProcessor(opts ...BeaconMessageProcessorOptionFunc) *BeaconMessageProcessor {
	o := &BeaconMessageProcessorOption{
		nodeAlias:  "",
		nodeRegion: "",
	}

	for _, opt := range opts {
		opt(o)
	}

	processor := &BeaconMessageProcessor{
		repo:          o.repo,
		nodeAlias:     o.nodeAlias,
		nodeRegion:    o.nodeRegion,
		enc:           encoder.SszNetworkEncoder{},
		beaconConfig:  eth.GetBeaconChainConfig(),
		genesisTime:   eth.GetGenesisConfig().GenesisTime,
		slotCaches:    make(map[uint64]*SlotCache),
		seenCounts:    make(map[string]uint32),
		messageNotify: make(chan struct{}, 100), // Buffer to prevent blocking
		done:          make(chan struct{}),
	}

	// Start the background processor
	go processor.processLoop()

	slog.Info("successfully created beacon message processor")
	return processor
}

func (p *BeaconMessageProcessor) Process(ctx context.Context, msg *pubsub.Message, dst ssz.Unmarshaler) error {
	if err := p.enc.DecodeGossip(msg.Data, dst); err != nil {
		return errors.Wrap(err, "failed to decode gossip message")
	}

	switch d := dst.(type) {
	// --- global topics ---

	// beacon_block
	case *ethtypes.SignedBeaconBlock:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SignedBeaconBlockAltair:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SignedBeaconBlockBellatrix:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SignedBeaconBlockCapella:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SignedBeaconBlockDeneb:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SignedBeaconBlockElectra:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)

	// beacon_aggregate_and_proof
	case *ethtypes.SignedAggregateAttestationAndProof:
		metadata := p.newSlotMetadata(msg, d.Message.Aggregate.Data.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SignedAggregateAttestationAndProofElectra:
		metadata := p.newSlotMetadata(msg, d.Message.Aggregate.Data.Slot)
		p.processSlotMessageMetadata(metadata)

	// beacon_sync_committee_contribution_and_proof
	case *ethtypes.SignedContributionAndProof:
		metadata := p.newSlotMetadata(msg, d.Message.Contribution.Slot)
		p.processSlotMessageMetadata(metadata)

	// proposer_slashing
	case *ethtypes.ProposerSlashing:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)

	// attester_slashing
	case *ethtypes.AttesterSlashing:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
	case *ethtypes.AttesterSlashingElectra:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)

	// voluntary_exit
	case *ethtypes.VoluntaryExit:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)

	// bls to execution change
	case *ethtypes.BLSToExecutionChange:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)

	// --- subnet topics ---

	// beacon_attestation
	case *ethtypes.Attestation:
		metadata := p.newSlotMetadata(msg, d.Data.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.AttestationElectra:
		metadata := p.newSlotMetadata(msg, d.Data.Slot)
		p.processSlotMessageMetadata(metadata)
	case *ethtypes.SingleAttestation:
		metadata := p.newSlotMetadata(msg, d.Data.Slot)
		p.processSlotMessageMetadata(metadata)

	// sync committee message
	case *ethtypes.SyncCommitteeMessage:
		metadata := p.newSlotMetadata(msg, d.Slot)
		p.processSlotMessageMetadata(metadata)

	// sync committee contribution
	case *ethtypes.SyncCommitteeContribution:
		metadata := p.newSlotMetadata(msg, d.Slot)
		p.processSlotMessageMetadata(metadata)

	// blob sidecar
	case *ethtypes.BlobSidecar:
		metadata := p.newSlotMetadata(msg, d.SignedBlockHeader.Header.Slot)
		p.processSlotMessageMetadata(metadata)

	default:
		return fmt.Errorf("unsupported message type: %T", dst)
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
		NodePeerCount: 1, // TODO: get peer count
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

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get or create the slot cache
	cache, exists := p.slotCaches[metadata.Slot]
	if !exists {
		cache = &SlotCache{
			Slot:         metadata.Slot,
			MessageStats: make(map[string]*repository.SlotMessageStats),
		}
		p.slotCaches[metadata.Slot] = cache
	}

	// Get or create the message record
	record, exists := cache.MessageStats[metadata.MsgID]
	if !exists {
		slotStartTime := p.genesisTime.Add((time.Duration(metadata.Slot) * time.Second * time.Duration(p.beaconConfig.SecondsPerSlot)))
		forkVersion, messageType := parseEth2Topic(metadata.Topic)
		record = &repository.SlotMessageStats{
			Slot:             metadata.Slot,
			TopicGroup:       TopicToTopicGroup(metadata.Topic),
			ForkVersion:      forkVersion,
			Topic:            messageType,
			NodeRegion:       p.nodeRegion,
			NodeAlias:        p.nodeAlias,
			NodePeerCount:    1, // TODO: get peer count
			MessageID:        metadata.MsgID,
			SlotStartTime:    slotStartTime,
			FirstArrivalTime: metadata.MsgArrival,
			LatencyMS:        uint32(metadata.MsgDelayInSlot.Milliseconds()),
			SizeBytes:        uint32(metadata.MsgSize),
		}
		cache.MessageStats[metadata.MsgID] = record
	} else {
		slog.Error("libp2p pubsub message cannot be processed twice", "msg_id", metadata.MsgID)
		// TODO: handle this as a fatal error
	}

	// Notify the processor without blocking
	select {
	case p.messageNotify <- struct{}{}:
		// Successfully notified
	default:
		// Channel is full, but that's okay - the regular timer will catch up
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

// processLoop runs in the background to periodically flush completed slots to the database
func (p *BeaconMessageProcessor) processLoop() {
	// FlushInterval is how often to check for completed slots
	flushInterval := time.Duration(p.beaconConfig.SecondsPerSlot) * time.Second

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			// Final flush before shutdown
			p.flushCompletedSlots()
			return
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
	now := time.Now()
	slotDuration := p.getSlotDuration()
	currentSlot := uint64(now.Unix()) / uint64(slotDuration.Seconds())

	// Lock for reading the map and identifying slots to process
	p.mu.RLock()
	var slotsToFlush []uint64
	for slot, _ := range p.slotCaches {
		// Flush slots that are at least 2 slots old or haven't been modified in a while
		if slot < currentSlot-1 {
			slotsToFlush = append(slotsToFlush, slot)
		}
	}
	p.mu.RUnlock()

	// Process each slot
	for _, slot := range slotsToFlush {
		stats := p.prepareAndRemoveSlot(slot)
		if len(stats) > 0 {
			for _, stat := range stats {
				seenCount, ok := p.popSeenCount(stat.MessageID)
				if ok {
					stat.SeenCount = seenCount
				} else {
					stat.SeenCount = 1
				}
			}
			p.saveToRepository(stats)
		}
	}
}

// prepareAndRemoveSlot prepares stats entities and removes the slot from cache
func (p *BeaconMessageProcessor) prepareAndRemoveSlot(slot uint64) []*repository.SlotMessageStats {
	p.mu.Lock()
	cache, exists := p.slotCaches[slot]
	if !exists {
		p.mu.Unlock()
		return nil
	}

	// Remove the cache so we don't process it again
	delete(p.slotCaches, slot)
	p.mu.Unlock()

	// Prepare the stats
	stats := make([]*repository.SlotMessageStats, 0, len(cache.MessageStats))
	for _, messageStat := range cache.MessageStats {
		stats = append(stats, messageStat)
	}

	return stats
}

// saveToRepository saves the stats to the repository in batches
func (p *BeaconMessageProcessor) saveToRepository(stats []*repository.SlotMessageStats) error {
	if len(stats) == 0 {
		return nil
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Save the batch
	err := p.repo.SaveSlotMessageStatsMulti(ctx, stats)
	if err != nil {
		slog.With("error", err).
			With("slot", stats[0].Slot).
			With("message_count", len(stats)).
			Error("failed to save slot message stats batch")
		return err
	}

	slog.With(
		"slot", stats[0].Slot,
		"message_count", len(stats),
	).Info("saved message batch to repository")

	return nil
}

// Stop stops the processor and flushes any remaining data
func (p *BeaconMessageProcessor) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()

	// Signal the processor to stop
	close(p.done)
}

// GetCurrentSlotStats returns stats about the currently tracked slots
func (p *BeaconMessageProcessor) GetCurrentSlotStats() map[uint64]int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[uint64]int)
	for slot, cache := range p.slotCaches {
		stats[slot] = len(cache.MessageStats)
	}

	return stats
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

func (p *BeaconMessageProcessor) popSeenCount(msgID string) (uint32, bool) {
	p.seenMu.Lock()
	defer p.seenMu.Unlock()

	value, ok := p.seenCounts[msgID]
	if ok {
		delete(p.seenCounts, msgID)
		return value, true
	}

	return 0, false
}

func (p *BeaconMessageProcessor) IncreaseSeenCountByRawID(msgIDRaw string) {
	msgID := hex.EncodeToString([]byte(msgIDRaw))

	p.seenMu.Lock()
	p.seenCounts[msgID]++
	p.seenMu.Unlock()
}
