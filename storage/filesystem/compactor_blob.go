package filesystem

import (
	"bufio"
	"context"
	"encoding/json"
	"io"

	"github.com/commentlens/loghouse/storage"
)

const (
	BlobLineMaxSize = 10 * 1024 * 1024
)

func readIndex(r io.Reader, opts *storage.ReadOptions) ([]*compactIndex, error) {
	var indexList []*compactIndex
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var index compactIndex
		err := json.Unmarshal(scanner.Bytes(), &index)
		if err != nil {
			return nil, err
		}
		if !matchIndex(&index, opts) {
			continue
		}
		indexList = append(indexList, &index)
	}
	err := scanner.Err()
	if err != nil {
		return nil, err
	}
	return indexList, nil
}

func readBlob(ctx context.Context, r io.Reader, opts *storage.ReadOptions) error {
	buf := make([]byte, BlobLineMaxSize)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(buf, BlobLineMaxSize)
	for scanner.Scan() {
		var e storage.LogEntry
		err := e.UnmarshalJSON(scanner.Bytes())
		if err != nil {
			return err
		}
		if !storage.MatchLogEntry(&e, opts) {
			continue
		}
		opts.ResultFunc(&e)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return scanner.Err()
}

func writeIndex(w io.Writer, indexList []*compactIndex) error {
	enc := json.NewEncoder(w)
	for _, index := range indexList {
		err := enc.Encode(index)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeBlob(w io.Writer, es []*storage.LogEntry) error {
	enc := json.NewEncoder(w)
	for _, e := range es {
		err := enc.Encode(e)
		if err != nil {
			return err
		}
	}
	return nil
}
