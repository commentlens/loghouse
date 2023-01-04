package filesystem

import (
	"context"
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

func (r *compactReader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	type readerTypeChunk string
	type readerTypeIndex string
	chIn := make(chan interface{})
	defer close(chIn)

	for i := 0; i < CompactReaderConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for t := range chIn {
				var r storage.Reader
				switch t := t.(type) {
				case readerTypeChunk:
					r = NewReader([]string{string(t)})
				case readerTypeIndex:
					r = newBlobReader([]string{string(t)})
				}
				r.Read(ctx, opts)
			}
		}()
	}

	var readers []interface{}
	indexFiles, err := findFiles(CompactDir, CompactIndexFile)
	if err != nil {
		return err
	}
	for _, indexFile := range indexFiles {
		readers = append(readers, readerTypeIndex(indexFile))
	}
	chunks, err := findFiles(WriteDir, WriteChunkFile)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		readers = append(readers, readerTypeChunk(chunk))
	}

	for _, rd := range readers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chIn <- rd:
		}
	}
	return nil
}
