package processor

import (
	"encoding/hex"
	"sync"
)

func hexEncode(b []byte) string {
	return hex.EncodeToString(b)
}

type SeenCounter struct {
	mu     sync.Mutex
	counts map[string]uint32
}

func NewSeenCounter() *SeenCounter {
	return &SeenCounter{
		counts: make(map[string]uint32),
	}
}

func (sc *SeenCounter) IncreaseByRawID(rawID string) {
	msgID := hexEncode([]byte(rawID))

	sc.mu.Lock()
	sc.counts[msgID]++
	sc.mu.Unlock()
}

func (sc *SeenCounter) Pop(msgID string) (uint32, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	value, ok := sc.counts[msgID]
	if ok {
		delete(sc.counts, msgID)
		return value, true
	}

	return 0, false
}
