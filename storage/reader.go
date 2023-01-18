package storage

import (
	"context"
	"time"
)

type Reader interface {
	Read(context.Context, *ReadOptions) error
}

type ReadOptions struct {
	Labels      map[string]string
	Start       time.Time
	End         time.Time
	SummaryFunc func(LogSummary) bool
	FilterFunc  func(LogEntry) bool
	ResultFunc  func(LogEntry)
}

func MatchLabels(m, query map[string]string) bool {
	for k, v := range query {
		if v2, ok := m[k]; !ok || v != v2 {
			return false
		}
	}
	return true
}

func MatchLogEntry(e LogEntry, opts *ReadOptions) bool {
	if !MatchLabels(e.Labels, opts.Labels) {
		return false
	}
	if !opts.Start.IsZero() && opts.Start.After(e.Time) {
		return false
	}
	if !opts.End.IsZero() && opts.End.Before(e.Time) {
		return false
	}
	if opts.FilterFunc != nil && !opts.FilterFunc(e) {
		return false
	}
	return true
}
