package storage

import "time"

type Reader interface {
	Read(*ReadOptions) ([]*LogEntry, error)
	Count(*ReadOptions) (uint64, error)
}

type ReadOptions struct {
	Labels map[string]string
	Start  time.Time
	End    time.Time
	Limit  uint64
}
