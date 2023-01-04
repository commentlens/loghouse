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

func readIndex(r io.Reader) ([]*compactIndex, error) {
	var indexList []*compactIndex
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var index compactIndex
		err := json.Unmarshal([]byte(scanner.Text()), &index)
		if err != nil {
			return nil, err
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var e storage.LogEntry
		err := json.Unmarshal([]byte(scanner.Text()), &e)
		if err != nil {
			return err
		}
		if !storage.MatchLogEntry(&e, opts) {
			continue
		}
		opts.ResultFunc(&e)
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
