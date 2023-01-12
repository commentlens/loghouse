package filesystem

import (
	"errors"
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
	chunkFile := fmt.Sprintf("%s/%s", dir, WriteChunkFile)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return err
	}
	err = func() error {
		f, err := os.OpenFile(chunkFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0777)
		if err != nil {
			return err
		}
		defer f.Close()

		return chunkio.WriteHeader(f, &chunkio.Header{
			Labels: es[0].Labels,
		})
	}()
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	err = func() error {
		f, err := os.OpenFile(chunkFile, os.O_WRONLY|os.O_APPEND, 0777)
		if err != nil {
			return err
		}
		defer f.Close()

		return chunkio.WriteData(f, es, false)
	}()
	if err != nil {
		return err
	}
	return nil
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
