package filesystem

import (
	"context"
	"sync"

	"github.com/commentlens/loghouse/storage"
)

const (
	compactReaderConcurrency = 32
)

func NewCompactReader() storage.Reader {
	return &compactReader{}
}

type compactReader struct{}

func (r *compactReader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	chIn := make(chan string)
	defer close(chIn)

	for i := 0; i < compactReaderConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for chunk := range chIn {
				NewReader([]string{chunk}).Read(ctx, opts)
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

	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chIn <- chunk:
		}
	}
	return nil
}
