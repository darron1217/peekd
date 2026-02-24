package processor

import (
	"sync"
	"testing"
)

func TestSeenCounter_IncreaseAndPop(t *testing.T) {
	sc := NewSeenCounter()

	sc.IncreaseByRawID("raw-msg-1")
	sc.IncreaseByRawID("raw-msg-1")
	sc.IncreaseByRawID("raw-msg-1")

	hexID := hexEncodeString("raw-msg-1")
	count, ok := sc.Pop(hexID)
	if !ok {
		t.Fatal("expected Pop to return true for existing key")
	}
	if count != 3 {
		t.Errorf("expected count=3, got %d", count)
	}

	count, ok = sc.Pop(hexID)
	if ok {
		t.Fatal("expected Pop to return false after key was already popped")
	}
	if count != 0 {
		t.Errorf("expected count=0 for popped key, got %d", count)
	}
}

func TestSeenCounter_PopNonExistent(t *testing.T) {
	sc := NewSeenCounter()

	count, ok := sc.Pop("does-not-exist")
	if ok {
		t.Fatal("expected Pop to return false for non-existent key")
	}
	if count != 0 {
		t.Errorf("expected count=0, got %d", count)
	}
}

func TestSeenCounter_ConcurrentAccess(t *testing.T) {
	sc := NewSeenCounter()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			sc.IncreaseByRawID("concurrent-msg")
		}()
	}
	wg.Wait()

	hexID := hexEncodeString("concurrent-msg")
	count, ok := sc.Pop(hexID)
	if !ok {
		t.Fatal("expected Pop to return true")
	}
	if count != goroutines {
		t.Errorf("expected count=%d, got %d", goroutines, count)
	}
}

func TestSeenCounter_MultipleKeys(t *testing.T) {
	sc := NewSeenCounter()

	sc.IncreaseByRawID("msg-a")
	sc.IncreaseByRawID("msg-a")
	sc.IncreaseByRawID("msg-b")

	hexA := hexEncodeString("msg-a")
	hexB := hexEncodeString("msg-b")

	countA, ok := sc.Pop(hexA)
	if !ok || countA != 2 {
		t.Errorf("expected msg-a count=2, got %d (ok=%v)", countA, ok)
	}
	countB, ok := sc.Pop(hexB)
	if !ok || countB != 1 {
		t.Errorf("expected msg-b count=1, got %d (ok=%v)", countB, ok)
	}
}

func hexEncodeString(s string) string {
	return hexEncode([]byte(s))
}
