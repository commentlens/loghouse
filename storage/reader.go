package storage

import (
	"time"
)

type Reader interface {
	Read(*ReadOptions) ([]*LogEntry, error)
}

type ReadOptions struct {
	Labels     map[string]string
	Start      time.Time
	End        time.Time
	FilterFunc func(*LogEntry) bool
	Limit      uint64
}

func Filter(es []*LogEntry, opts *ReadOptions) ([]*LogEntry, error) {
	var out []*LogEntry
	for _, e := range es {
		matchLabels := true
		for k, v := range opts.Labels {
			if v2, ok := e.Labels[k]; !ok || v != v2 {
				matchLabels = false
				break
			}
		}
		if !matchLabels {
			continue
		}
		if !opts.Start.IsZero() && opts.Start.After(e.Time) {
			continue
		}
		if !opts.End.IsZero() && opts.End.Before(e.Time) {
			continue
		}
		if opts.FilterFunc != nil && !opts.FilterFunc(e) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}
