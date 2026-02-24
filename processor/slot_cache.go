package processor

import (
	"sync"

	"github.com/post-pectra/peekd/repository"
)

type SlotCacheStore struct {
	mu     sync.RWMutex
	caches map[uint64]*SlotCache
}

func NewSlotCacheStore() *SlotCacheStore {
	return &SlotCacheStore{
		caches: make(map[uint64]*SlotCache),
	}
}

// Add inserts a stat into the cache for the given slot and message ID.
// Returns true if the message was new, false if it was a duplicate.
func (s *SlotCacheStore) Add(slot uint64, msgID string, stat *repository.SlotMessageStats) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	cache, exists := s.caches[slot]
	if !exists {
		cache = &SlotCache{
			Slot:         slot,
			MessageStats: make(map[string]*repository.SlotMessageStats),
		}
		s.caches[slot] = cache
	}

	if _, exists := cache.MessageStats[msgID]; exists {
		return false
	}

	cache.MessageStats[msgID] = stat
	return true
}

// CompletedSlots returns slot numbers that are older than the given threshold.
func (s *SlotCacheStore) CompletedSlots(currentSlot uint64) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var slots []uint64
	for slot := range s.caches {
		if slot < currentSlot-1 {
			slots = append(slots, slot)
		}
	}
	return slots
}

// DrainSlot removes and returns all stats for the given slot.
func (s *SlotCacheStore) DrainSlot(slot uint64) []*repository.SlotMessageStats {
	s.mu.Lock()
	cache, exists := s.caches[slot]
	if !exists {
		s.mu.Unlock()
		return nil
	}
	delete(s.caches, slot)
	s.mu.Unlock()

	stats := make([]*repository.SlotMessageStats, 0, len(cache.MessageStats))
	for _, stat := range cache.MessageStats {
		stats = append(stats, stat)
	}
	return stats
}
