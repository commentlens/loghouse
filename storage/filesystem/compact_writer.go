package filesystem

import (
	"context"
	"sync"
	"time"

	"github.com/commentlens/loghouse/storage"
)

const (
	CompactInterval = 10 * time.Second
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
	ticker := time.NewTicker(CompactInterval)
	defer ticker.Stop()

	err := w.c.Compact()
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := func() error {
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

	defer w.c.SwapChunk()

	return w.w.Write(es)
}
