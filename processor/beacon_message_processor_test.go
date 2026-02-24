package processor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/post-pectra/peekd/repository"
)

type mockPeerCounter struct {
	count int
}

func (m *mockPeerCounter) TotalPeerCount() int {
	return m.count
}

type mockRepository struct {
	mu             sync.Mutex
	generalHistory []*repository.GeneralMessageHistory
	slotStats      [][]*repository.SlotMessageStats
	saveCh         chan struct{}
}

func newMockRepository() *mockRepository {
	return &mockRepository{
		saveCh: make(chan struct{}, 10),
	}
}

func (m *mockRepository) SaveGeneralMessageHistory(ctx context.Context, history *repository.GeneralMessageHistory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generalHistory = append(m.generalHistory, history)
	m.saveCh <- struct{}{}
	return nil
}

func (m *mockRepository) SaveSlotMessageStatsMulti(ctx context.Context, stats []*repository.SlotMessageStats) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slotStats = append(m.slotStats, stats)
	m.saveCh <- struct{}{}
	return nil
}

func (m *mockRepository) Close() error {
	return nil
}

func newTestProcessor(pc PeerCounter, repo repository.Repository) *BeaconMessageProcessor {
	return &BeaconMessageProcessor{
		repo:          repo,
		peerCounter:   pc,
		beaconConfig:  params.MainnetConfig(),
		genesisTime:   time.Unix(1606824023, 0),
		nodeAlias:     "test-node",
		nodeRegion:    "test-region",
		slotCaches:    make(map[uint64]*SlotCache),
		seenCounter:   NewSeenCounter(),
		messageNotify: make(chan struct{}, 100),
	}
}

func TestProcessSlotMessageMetadata_PeerCount(t *testing.T) {
	repo := newMockRepository()
	pc := &mockPeerCounter{count: 42}
	p := newTestProcessor(pc, repo)

	metadata := &SlotMessageMetadata{
		PeerID:         "test-peer",
		Topic:          "/eth2/6a95a1a9/beacon_block/ssz_snappy",
		MsgID:          "test-msg-001",
		MsgSize:        1024,
		MsgArrival:     time.Now(),
		MsgDelayInSlot: 500 * time.Millisecond,
		Slot:           100,
	}

	p.processSlotMessageMetadata(metadata)

	cache, ok := p.slotCaches[100]
	if !ok {
		t.Fatal("expected slot cache for slot 100")
	}
	record, ok := cache.MessageStats["test-msg-001"]
	if !ok {
		t.Fatal("expected message stats for test-msg-001")
	}
	if record.NodePeerCount != 42 {
		t.Errorf("expected NodePeerCount=42, got %d", record.NodePeerCount)
	}
}

func TestProcessSlotMessageMetadata_BasicFields(t *testing.T) {
	repo := newMockRepository()
	pc := &mockPeerCounter{count: 10}
	p := newTestProcessor(pc, repo)

	arrival := time.Now()
	metadata := &SlotMessageMetadata{
		PeerID:         "test-peer",
		Topic:          "/eth2/6a95a1a9/beacon_block/ssz_snappy",
		MsgID:          "test-msg-002",
		MsgSize:        2048,
		MsgArrival:     arrival,
		MsgDelayInSlot: 1 * time.Second,
		Slot:           200,
	}

	p.processSlotMessageMetadata(metadata)

	cache := p.slotCaches[200]
	record := cache.MessageStats["test-msg-002"]

	if record.Slot != 200 {
		t.Errorf("expected Slot=200, got %d", record.Slot)
	}
	if record.ForkVersion != "6a95a1a9" {
		t.Errorf("expected ForkVersion=6a95a1a9, got %s", record.ForkVersion)
	}
	if record.Topic != "beacon_block" {
		t.Errorf("expected Topic=beacon_block, got %s", record.Topic)
	}
	if record.NodeAlias != "test-node" {
		t.Errorf("expected NodeAlias=test-node, got %s", record.NodeAlias)
	}
	if record.NodeRegion != "test-region" {
		t.Errorf("expected NodeRegion=test-region, got %s", record.NodeRegion)
	}
	if record.MessageID != "test-msg-002" {
		t.Errorf("expected MessageID=test-msg-002, got %s", record.MessageID)
	}
	if record.SizeBytes != 2048 {
		t.Errorf("expected SizeBytes=2048, got %d", record.SizeBytes)
	}
	if record.LatencyMS != 1000 {
		t.Errorf("expected LatencyMS=1000, got %d", record.LatencyMS)
	}
	if !record.FirstArrivalTime.Equal(arrival) {
		t.Errorf("expected FirstArrivalTime=%v, got %v", arrival, record.FirstArrivalTime)
	}
}

func TestProcessSlotMessageMetadata_DuplicateMessage(t *testing.T) {
	repo := newMockRepository()
	pc := &mockPeerCounter{count: 10}
	p := newTestProcessor(pc, repo)

	metadata := &SlotMessageMetadata{
		PeerID:         "test-peer",
		Topic:          "/eth2/6a95a1a9/beacon_block/ssz_snappy",
		MsgID:          "test-msg-dup",
		MsgSize:        1024,
		MsgArrival:     time.Now(),
		MsgDelayInSlot: 500 * time.Millisecond,
		Slot:           100,
	}

	p.processSlotMessageMetadata(metadata)
	p.processSlotMessageMetadata(metadata)

	cache := p.slotCaches[100]
	if len(cache.MessageStats) != 1 {
		t.Errorf("expected 1 message stat for duplicate msg, got %d", len(cache.MessageStats))
	}
}

func TestProcessGeneralMessageMetadata_PeerCount(t *testing.T) {
	repo := newMockRepository()
	pc := &mockPeerCounter{count: 55}
	p := newTestProcessor(pc, repo)

	metadata := &GeneralMessageMetadata{
		PeerID:     "test-peer",
		Topic:      "/eth2/6a95a1a9/proposer_slashing/ssz_snappy",
		MsgID:      "test-msg-003",
		MsgSize:    512,
		MsgArrival: time.Now(),
	}

	p.processGeneralMessageMetadata(metadata)

	select {
	case <-repo.saveCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async save")
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.generalHistory) != 1 {
		t.Fatalf("expected 1 general history record, got %d", len(repo.generalHistory))
	}
	record := repo.generalHistory[0]
	if record.NodePeerCount != 55 {
		t.Errorf("expected NodePeerCount=55, got %d", record.NodePeerCount)
	}
}

func TestProcessGeneralMessageMetadata_BasicFields(t *testing.T) {
	repo := newMockRepository()
	pc := &mockPeerCounter{count: 10}
	p := newTestProcessor(pc, repo)

	arrival := time.Now()
	metadata := &GeneralMessageMetadata{
		PeerID:     "test-peer",
		Topic:      "/eth2/6a95a1a9/proposer_slashing/ssz_snappy",
		MsgID:      "test-msg-004",
		MsgSize:    256,
		MsgArrival: arrival,
	}

	p.processGeneralMessageMetadata(metadata)

	select {
	case <-repo.saveCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async save")
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	record := repo.generalHistory[0]
	if record.ForkVersion != "6a95a1a9" {
		t.Errorf("expected ForkVersion=6a95a1a9, got %s", record.ForkVersion)
	}
	if record.Topic != "proposer_slashing" {
		t.Errorf("expected Topic=proposer_slashing, got %s", record.Topic)
	}
	if record.NodeAlias != "test-node" {
		t.Errorf("expected NodeAlias=test-node, got %s", record.NodeAlias)
	}
	if record.NodeRegion != "test-region" {
		t.Errorf("expected NodeRegion=test-region, got %s", record.NodeRegion)
	}
	if record.MessageID != "test-msg-004" {
		t.Errorf("expected MessageID=test-msg-004, got %s", record.MessageID)
	}
	if record.SizeBytes != 256 {
		t.Errorf("expected SizeBytes=256, got %d", record.SizeBytes)
	}
}
