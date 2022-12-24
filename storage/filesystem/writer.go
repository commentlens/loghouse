package filesystem

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/commentlens/loghouse/storage"
)

const (
	DataDir   = "data"
	TimeDir   = "2006-01-02-15"
	ChunkFile = "chunk.jsonl"
)

func NewWriter() storage.Writer {
	return &writer{}
}

type writer struct {
}

func HashLabels(labels map[string]string) (string, error) {
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var tuples []string
	for _, k := range keys {
		v := labels[k]
		tuples = append(tuples, k, v)
	}

	b, err := json.Marshal(tuples)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(b))
	return digest, nil
}

func LogEntryDir(e *storage.LogEntry) (string, error) {
	timeDir := e.Time.UTC().Format(TimeDir)
	hashDir, err := HashLabels(e.Labels)
	if err != nil {
		return "", err
	}
	dir := fmt.Sprintf("%s/%s/%s", DataDir, timeDir, hashDir)
	return dir, nil
}

func (w *writer) Write(es []*storage.LogEntry) error {
	for _, e := range es {
		err := func() error {
			var v struct{}
			err := json.Unmarshal(e.Data, &v)
			if err != nil {
				return err
			}

			dir, err := LogEntryDir(e)
			if err != nil {
				return err
			}

			if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
				err = os.MkdirAll(dir, 0777)
				if err != nil {
					return err
				}
			}
			chunkFile := fmt.Sprintf("%s/%s", dir, ChunkFile)

			f, err := os.OpenFile(chunkFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			defer f.Close()

			entryJSON, err := json.Marshal(e)
			if err != nil {
				return err
			}
			_, err = f.WriteString(fmt.Sprintf("%s\n", entryJSON))
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
