package label

import (
	"sort"
	"sync"
)

type Store struct {
	mu    sync.Mutex
	limit uint64
	m     map[string]*storeEntry
}

type storeEntry struct {
	l   []string
	ptr uint64
}

func NewStore(limit uint64) *Store {
	return &Store{
		limit: limit,
		m:     make(map[string]*storeEntry),
	}
}

func (s *Store) Add(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.m[key]; !ok {
		s.m[key] = &storeEntry{}
	}

	e := s.m[key]
	for _, lv := range e.l {
		if val == lv {
			return
		}
	}
	if uint64(len(e.l)) < s.limit {
		e.l = append(e.l, val)
	} else {
		e.l[e.ptr] = val
	}
	e.ptr = (e.ptr + 1) % s.limit
}

func (s *Store) Labels() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keys []string
	for key := range s.m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) LabelValues(key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.m[key]
	if !ok {
		return nil
	}
	vals := append([]string{}, e.l...)
	sort.Strings(vals)
	return vals
}
