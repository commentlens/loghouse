package filesystem

import (
	"sort"

	"github.com/commentlens/loghouse/storage"
)

func NewCompactReader() storage.Reader {
	return &compactReader{}
}

type compactReader struct {
	r reader
	c compactor
}

func (r *compactReader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	chunks, err := findFiles(WriteDir, WriteChunkFile)
	if err != nil {
		return nil, err
	}
	r.r.Chunks = chunks

	es1, err := r.r.Read(opts)
	if err != nil {
		return nil, err
	}
	es2, err := r.c.Read(opts)
	if err != nil {
		return nil, err
	}
	var out []*storage.LogEntry
	out = append(out, es1...)
	out = append(out, es2...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}
