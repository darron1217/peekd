package processor

import (
	"testing"
	"time"

	"github.com/post-pectra/peekd/repository"
)

func TestSlotCacheStore_AddAndGet(t *testing.T) {
	store := NewSlotCacheStore()

	stat := &repository.SlotMessageStats{
		Slot:      100,
		MessageID: "msg-001",
		Topic:     "beacon_block",
	}

	added := store.Add(100, "msg-001", stat)
	if !added {
		t.Fatal("expected Add to return true for new message")
	}

	added = store.Add(100, "msg-001", stat)
	if added {
		t.Fatal("expected Add to return false for duplicate message")
	}
}

func TestSlotCacheStore_AddMultipleSlots(t *testing.T) {
	store := NewSlotCacheStore()

	store.Add(100, "msg-001", &repository.SlotMessageStats{Slot: 100, MessageID: "msg-001"})
	store.Add(100, "msg-002", &repository.SlotMessageStats{Slot: 100, MessageID: "msg-002"})
	store.Add(200, "msg-003", &repository.SlotMessageStats{Slot: 200, MessageID: "msg-003"})

	stats := store.DrainSlot(100)
	if len(stats) != 2 {
		t.Errorf("expected 2 stats for slot 100, got %d", len(stats))
	}

	stats = store.DrainSlot(200)
	if len(stats) != 1 {
		t.Errorf("expected 1 stat for slot 200, got %d", len(stats))
	}

	stats = store.DrainSlot(100)
	if len(stats) != 0 {
		t.Errorf("expected 0 stats after drain, got %d", len(stats))
	}
}

func TestSlotCacheStore_CompletedSlots(t *testing.T) {
	store := NewSlotCacheStore()

	store.Add(10, "msg-a", &repository.SlotMessageStats{Slot: 10, MessageID: "msg-a"})
	store.Add(20, "msg-b", &repository.SlotMessageStats{Slot: 20, MessageID: "msg-b"})
	store.Add(30, "msg-c", &repository.SlotMessageStats{Slot: 30, MessageID: "msg-c"})

	completed := store.CompletedSlots(25)
	if len(completed) != 2 {
		t.Fatalf("expected 2 completed slots (10, 20), got %d", len(completed))
	}

	has10, has20 := false, false
	for _, s := range completed {
		if s == 10 {
			has10 = true
		}
		if s == 20 {
			has20 = true
		}
	}
	if !has10 || !has20 {
		t.Errorf("expected slots 10 and 20 in completed, got %v", completed)
	}
}

func TestSlotCacheStore_DrainSlotReturnsAll(t *testing.T) {
	store := NewSlotCacheStore()

	for i := 0; i < 100; i++ {
		msgID := "msg-" + time.Now().Format("150405.000000000") + "-" + string(rune('a'+i%26))
		store.Add(42, msgID, &repository.SlotMessageStats{Slot: 42, MessageID: msgID})
	}

	stats := store.DrainSlot(42)
	if len(stats) != 100 {
		t.Errorf("expected 100 stats, got %d", len(stats))
	}

	stats = store.DrainSlot(42)
	if len(stats) != 0 {
		t.Errorf("expected 0 stats after drain, got %d", len(stats))
	}
}
