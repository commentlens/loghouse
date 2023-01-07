package filesystem

import (
	"fmt"
	"os"

	"github.com/commentlens/loghouse/storage"
)

const (
	WriteDir       = "data/incompact"
	WriteChunkFile = "chunk.loghouse"
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

func (w *writer) Write(es []*storage.LogEntry) error {
	if len(es) == 0 {
		return nil
	}
	dir, err := logEntryDir(es[0])
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

	return writeChunk(f, es)
}
