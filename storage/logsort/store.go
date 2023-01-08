package logsort

import (
	"container/heap"
	"sync"

	"github.com/commentlens/loghouse/storage"
)

type Store struct {
	mu      sync.Mutex
	limit   uint64
	reverse bool
	es      []*storage.LogEntry
}

func NewStore(limit uint64, reverse bool) *Store {
	return &Store{
		limit:   limit,
		reverse: reverse,
	}
}

func (s *Store) Len() int { return len(s.es) }

func (s *Store) Less(i, j int) bool {
	if s.reverse {
		return s.es[i].Time.Before(s.es[j].Time)
	}
	return s.es[i].Time.After(s.es[j].Time)
}

func (s *Store) Swap(i, j int) {
	s.es[i], s.es[j] = s.es[j], s.es[i]
}

func (s *Store) Push(x any) {
	s.es = append(s.es, x.(*storage.LogEntry))
}

func (s *Store) Pop() any {
	x := s.es[len(s.es)-1]
	s.es = s.es[:len(s.es)-1]
	return x
}

func (s *Store) Add(e *storage.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.es) > 0 && uint64(len(s.es)) >= s.limit {
		x := s.es[0]
		if s.reverse {
			if e.Time.Before(x.Time) {
				return
			}
		} else {
			if e.Time.After(x.Time) {
				return
			}
		}
	}

	heap.Push(s, e)
	for uint64(len(s.es)) > s.limit {
		heap.Pop(s)
	}
}

func (s *Store) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.es)) >= s.limit
}

func (s *Store) Get() []*storage.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	es := make([]*storage.LogEntry, len(s.es))
	for i := 0; i < len(es); i++ {
		es[len(es)-1-i] = heap.Pop(s).(*storage.LogEntry)
	}
	return es
}
