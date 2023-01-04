package filesystem

import (
	"bufio"
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

func readBlob(r io.Reader, opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	var es []*storage.LogEntry
	buf := make([]byte, BlobLineMaxSize)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(buf, BlobLineMaxSize)
	for scanner.Scan() {
		var e storage.LogEntry
		err := json.Unmarshal([]byte(scanner.Text()), &e)
		if err != nil {
			return nil, err
		}
		if !storage.MatchLogEntry(&e, opts) {
			continue
		}
		if opts.ResultFunc != nil {
			opts.ResultFunc(&e)
		} else {
			es = append(es, &e)
			if opts.Limit > 0 && uint64(len(es)) >= opts.Limit {
				es = es[:opts.Limit]
				return es, nil
			}
		}
	}
	err := scanner.Err()
	if err != nil {
		return nil, err
	}
	return es, nil
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
