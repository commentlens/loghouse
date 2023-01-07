package filesystem

import (
	"fmt"
	"os"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
)

const (
	WriteDir       = "data/incompact"
	WriteChunkFile = "chunk.loghouse"
)

func NewWriter() storage.Writer {
	return &writer{}
}

type writer struct{}

func (w *writer) write(hash string, es []*storage.LogEntry) error {
	dir := fmt.Sprintf("%s/%s", WriteDir, hash)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return err
	}
	chunkFile := fmt.Sprintf("%s/%s", dir, WriteChunkFile)

	f, err := os.OpenFile(chunkFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	return chunkio.Write(f, es)
}

func (w *writer) Write(es []*storage.LogEntry) error {
	m := make(map[string][]*storage.LogEntry)
	for _, e := range es {
		h, err := storage.HashLabels(e.Labels)
		if err != nil {
			return err
		}
		m[h] = append(m[h], e)
	}
	for hash, es := range m {
		err := w.write(hash, es)
		if err != nil {
			return err
		}
	}
	return nil
}
