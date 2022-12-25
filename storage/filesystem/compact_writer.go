package filesystem

import (
	"context"
	"sync"
	"time"

	"github.com/commentlens/loghouse/storage"
)

const (
	CompactIntv = 10 * time.Second
)

func NewCompactWriter() storage.Writer {
	return &compactWriter{}
}

type compactWriter struct {
	w  writer
	c  compactor
	mu sync.Mutex
}

func (w *compactWriter) BackgroundCompact(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(CompactIntv)
		defer ticker.Stop()

		w.c.Compact()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				func() {
					w.mu.Lock()
					defer w.mu.Unlock()

					w.c.SwapChunk()
				}()

				w.c.Compact()
			}
		}
	}()
}

func (w *compactWriter) Write(es []*storage.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	defer w.c.SwapChunk()

	return w.w.Write(es)
}
