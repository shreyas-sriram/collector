package state

import (
	"context"
	"errors"
	"sync"
)

const DefaultCapacity = 10

// Queue of snapshots ready for submission to pganalyze
//   - Thread safe: Safe for concurrent use by multiple readers and writers.
//   - Limited capacity: If full, Push drops the oldest item to make room.
//   - Transactional: Pop locks the head item via a generation ID which is removed on Commit, and re-released on Rollback.
//   - Generation tracking: Increments a counter on every Push to uniquely ID each item.
//     If a Push evicts an in-flight item, its generation changes, safely ignoring late Commits or Rollbacks.
type Queue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	data      []QueueItem
	capacity  int
	head      int
	tail      int
	size      int
	activeGen uint64
	closed    bool
	nextGenID uint64
}

func NewQueue() *Queue {
	q := &Queue{
		data:     make([]QueueItem, DefaultCapacity),
		capacity: DefaultCapacity,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push adds an item to the tail. If full, it drops the oldest item.
func (q *Queue) Push(kind string, snapshot []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.nextGenID++
	// Drop oldest item when at capacity
	if q.size == q.capacity {
		// Invalidate evicted item's generation, so Commit and Rollback are no-ops.
		if q.activeGen == q.data[q.head].Generation {
			q.activeGen = 0
		}
		q.data[q.head] = QueueItem{}
		q.head = (q.head + 1) % q.capacity
		q.size--
	}
	q.data[q.tail] = QueueItem{
		Kind:       kind,
		Snapshot:   snapshot,
		Generation: q.nextGenID,
	}
	q.tail = (q.tail + 1) % q.capacity
	q.size++
	q.cond.Signal()
}

// Pop blocks until an item is ready, context cancels, or the queue closes.
// On success, it locks the head item and returns a Transaction handle.
func (q *Queue) Pop(ctx context.Context) (*Transaction, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Spin up sidecar watcher if we need to wait
	if q.size == 0 || q.activeGen != 0 {
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ctx.Done():
				q.cond.Broadcast()
			case <-done:
			}
		}()
		for (q.size == 0 || q.activeGen != 0) && !q.closed && ctx.Err() == nil {
			q.cond.Wait()
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if q.closed {
		return nil, errors.New("queue closed")
	}
	// Capture the transaction safely using exact Generation matching
	headQueueItem := q.data[q.head]
	q.activeGen = headQueueItem.Generation
	return &Transaction{
		Kind:       headQueueItem.Kind,
		Snapshot:   headQueueItem.Snapshot,
		generation: headQueueItem.Generation,
		q:          q,
	}, nil
}

// Close terminates the queue, unblocks waiting readers, and rejects new writes.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.cond.Broadcast()
	}
}

type QueueItem struct {
	Kind       string
	Snapshot   []byte
	Generation uint64
}

type Transaction struct {
	Kind       string
	Snapshot   []byte
	generation uint64
	q          *Queue
}

func (t *Transaction) Commit() {
	t.q.mu.Lock()
	defer t.q.mu.Unlock()
	// Validate that the transaction hasn't been evicted or superseded
	if t.q.activeGen != t.generation || t.q.data[t.q.head].Generation != t.generation {
		return
	}
	t.q.data[t.q.head] = QueueItem{}
	t.q.head = (t.q.head + 1) % t.q.capacity
	t.q.size--
	t.q.activeGen = 0
	t.q.cond.Broadcast()
}

func (t *Transaction) Rollback() {
	t.q.mu.Lock()
	defer t.q.mu.Unlock()
	// Validate that the transaction hasn't been evicted or superseded
	if t.q.activeGen != t.generation || t.q.data[t.q.head].Generation != t.generation {
		return
	}
	t.q.activeGen = 0
	t.q.cond.Broadcast()
}
