package filesystem

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/commentlens/loghouse/storage"
)

const (
	WriteDir       = "data/incompact"
	WriteChunkFile = "chunk.jsonl"
)

func NewWriter() storage.Writer {
	return &writer{}
}

type writer struct{}

func logEntryDir(e *storage.LogEntry) (string, error) {
	hashDir, err := storage.HashLabels(e.Labels)
	if err != nil {
		return "", err
	}
	dir := fmt.Sprintf("%s/%s", WriteDir, hashDir)
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

	return writeBlob(f, []*storage.LogEntry{e})
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
