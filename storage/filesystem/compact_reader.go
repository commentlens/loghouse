package filesystem

import (
	"context"
	"sort"
	"sync"

	"github.com/commentlens/loghouse/storage"
)

const (
	CompactReaderConcurrency = 32
)

func NewCompactReader() storage.Reader {
	return &compactReader{}
}

type compactReader struct{}

func (r *compactReader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	var wg sync.WaitGroup

	chIn := make(chan struct {
		Type string
		File string
	})
	chOut := make(chan []*storage.LogEntry)

	for i := 0; i < CompactReaderConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for t := range chIn {
				var r storage.Reader
				switch t.Type {
				case "chunk":
					r = NewReader([]string{t.File})
				case "index":
					r = newBlobReader([]string{t.File})
				}
				es, err := r.Read(opts)
				if err != nil {
					continue
				}
				if len(es) == 0 {
					continue
				}
				chOut <- es
			}
		}()
	}

	chunks, err := findFiles(WriteDir, WriteChunkFile)
	if err != nil {
		return nil, err
	}
	indexFiles, err := findFiles(CompactDir, CompactIndexFile)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer close(chOut)
		defer wg.Wait()
		defer close(chIn)

		for _, chunk := range chunks {
			select {
			case <-ctx.Done():
				return
			case chIn <- struct {
				Type string
				File string
			}{
				Type: "chunk",
				File: chunk,
			}:
			}
		}
		for _, indexFile := range indexFiles {
			select {
			case <-ctx.Done():
				return
			case chIn <- struct {
				Type string
				File string
			}{
				Type: "index",
				File: indexFile,
			}:
			}
		}
	}()
	var out []*storage.LogEntry
	var done bool
	for es := range chOut {
		if done {
			continue
		}
		out = append(out, es...)
		if opts.Limit > 0 && uint64(len(out)) >= opts.Limit {
			out = out[:opts.Limit]
			done = true
			cancel()
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}
