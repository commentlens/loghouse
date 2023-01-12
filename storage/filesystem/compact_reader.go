package filesystem

import (
	"context"
	"sync"

	"github.com/commentlens/loghouse/storage"
)

func NewCompactReader(readerCount int, reverse bool) storage.Reader {
	return &compactReader{
		readerCount: readerCount,
		reverse:     reverse,
	}
}

type compactReader struct {
	readerCount int
	reverse     bool
}

func (r *compactReader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	chIn := make(chan string)
	defer close(chIn)

	for i := 0; i < r.readerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for chunk := range chIn {
				cr := NewReader([]string{chunk})
				if r.reverse {
					var es []*storage.LogEntry
					nopts := *opts
					nopts.ResultFunc = func(e *storage.LogEntry) {
						es = append(es, e)
					}
					cr.Read(ctx, &nopts)
					for i := len(es) - 1; i >= 0; i-- {
						opts.ResultFunc(es[i])
					}
				} else {
					cr.Read(ctx, opts)
				}
			}
		}()
	}

	var chunks []string
	for _, dir := range []string{CompactDir, WriteDir} {
		files, err := findFiles(dir, WriteChunkFile)
		if err != nil {
			return err
		}
		chunks = append(chunks, files...)
	}
	if r.reverse {
		reverseStrings(chunks)
	}
	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chIn <- chunk:
		}
	}
	return nil
}

func reverseStrings(ss []string) {
	last := len(ss) - 1
	for i := 0; i < len(ss)/2; i++ {
		ss[i], ss[last-i] = ss[last-i], ss[i]
	}
}
