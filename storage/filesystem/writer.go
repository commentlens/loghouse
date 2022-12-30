package filesystem

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/commentlens/loghouse/storage"
)

const (
	WriteDir       = "data/incompact"
	WriteTimeDir   = "2006-01-02-15"
	WriteChunkFile = "chunk.jsonl"
)

func NewWriter() storage.Writer {
	return &writer{}
}

type writer struct{}

func logEntryDir(e *storage.LogEntry) (string, error) {
	timeDir := e.Time.UTC().Format(WriteTimeDir)
	hashDir, err := storage.HashLabels(e.Labels)
	if err != nil {
		return "", err
	}
	dir := fmt.Sprintf("%s/%s/%s", WriteDir, timeDir, hashDir)
	return dir, nil
}

func (w *writer) write(e *storage.LogEntry) error {
	var v struct{}
	err := json.Unmarshal(e.Data, &v)
	if err != nil {
		return err
	}

	dir, err := logEntryDir(e)
	if err != nil {
		return err
	}
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return err
	}
	chunkFile := fmt.Sprintf("%s/%s", dir, WriteChunkFile)

	f, err := os.OpenFile(chunkFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	return json.NewEncoder(f).Encode(e)
}

func (w *writer) Write(es []*storage.LogEntry) error {
	for _, e := range es {
		err := w.write(e)
		if err != nil {
			return err
		}
	}
	return nil
}
