package filesystem

import (
	"context"
	"sync"
	"time"

	"github.com/commentlens/loghouse/storage"
)

const (
	CompactBackgroundInterval = 10 * time.Second
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
	ticker := time.NewTicker(CompactBackgroundInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := w.c.Compact()
			if err != nil {
				return err
			}
			err = func() error {
				w.mu.Lock()
				defer w.mu.Unlock()

				return w.c.SwapChunk()
			}()
			if err != nil {
				return err
			}
			err = w.c.Compact()
			if err != nil {
				return err
			}
		}
	}
}

func (w *compactWriter) Write(es []*storage.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(es)
}
