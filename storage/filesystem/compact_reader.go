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

func (r *compactReader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	var wg sync.WaitGroup

	type readerTypeChunk string
	type readerTypeIndex string
	chIn := make(chan interface{})
	chOut := make(chan []*storage.LogEntry)

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

	var readers []interface{}
	indexFiles, err := findFiles(CompactDir, CompactIndexFile)
	if err != nil {
		return nil, err
	}
	for _, indexFile := range indexFiles {
		readers = append(readers, readerTypeIndex(indexFile))
	}
	chunks, err := findFiles(WriteDir, WriteChunkFile)
	if err != nil {
		return nil, err
	}
	for _, chunk := range chunks {
		readers = append(readers, readerTypeChunk(chunk))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer close(chOut)
		defer wg.Wait()
		defer close(chIn)

		for _, rd := range readers {
			select {
			case <-ctx.Done():
				return
			case chIn <- rd:
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
	return out, nil
}
