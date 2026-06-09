package state

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func TestQueue_RetainsMetadataAndFIFO(t *testing.T) {
	q := NewQueue()
	ctx := context.Background()

	// Push 11 snapshots to trigger the first snapshot to be dropped
	for i := 0; i < 11; i++ {
		q.Push(fmt.Sprintf("%d", i), []byte{byte(i)})
	}

	// First popped element must be the second pushed snapshot (index 1)
	tx, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Kind != "1" {
		t.Errorf("expected Kind 1, got %v", tx.Kind)
	}
	if !bytes.Equal(tx.Snapshot, []byte{1}) {
		t.Errorf("expected snapshot content, got %v", tx.Snapshot)
	}

	tx.Commit()
}

func TestQueue_RollbackMaintainsSnapshot(t *testing.T) {
	q := NewQueue()
	ctx := context.Background()

	q.Push("1", []byte("data"))
	q.Push("2", []byte("data"))

	tx, _ := q.Pop(ctx)
	if tx.Kind != "1" {
		t.Errorf("expected Kind 1, got %v", tx.Kind)
	}
	tx.Rollback()

	// After rolling back the transaction, the same snapshot is still at the top of the queue
	tx, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("pop after rollback failed: %v", err)
	}
	if tx.Kind != "1" {
		t.Errorf("expected Kind 1, got %v", tx.Kind)
	}
	tx.Commit()
}
