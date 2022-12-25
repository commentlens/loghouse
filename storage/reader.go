package storage

import (
	"sort"
	"time"
)

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
		if opts.Limit != 0 && len(out) >= int(opts.Limit) {
			break
		}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}
