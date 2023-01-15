package filesystem

import (
	"context"
	"sync"
	"time"

	"github.com/commentlens/loghouse/storage"
)

const (
	compactBackgroundInterval = time.Minute
)

func NewCompactWriter() interface {
	storage.Writer
	BackgroundCompact(context.Context) error
} {
	return &compactWriter{}
}

type compactWriter struct {
	w  writer
	c  compactor
	mu sync.Mutex
}

func (w *compactWriter) BackgroundCompact(ctx context.Context) error {
	ticker := time.NewTicker(compactBackgroundInterval)
	defer ticker.Stop()

	for {
		err := w.c.Compact()
		if err != nil {
			return err
		}
		chunks, err := w.c.FindCompactibleChunk()
		if err != nil {
			return err
		}
		err = func() error {
			w.mu.Lock()
			defer w.mu.Unlock()

			return w.c.SwapChunk(chunks)
		}()
		if err != nil {
			return err
		}
		err = w.c.Compact()
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *compactWriter) Write(es []storage.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(es)
}
