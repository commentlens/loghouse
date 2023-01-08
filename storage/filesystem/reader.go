package filesystem

import (
	"bufio"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
)

const (
	readerBufferSize = 1024 * 1024
)

func findFiles(dir, name string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() != name {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func NewReader(chunks []string) storage.Reader {
	return &reader{Chunks: chunks}
}

type reader struct {
	Chunks []string
}

func (r *reader) read(ctx context.Context, chunk string, opts *storage.ReadOptions) error {
	f, err := os.Open(chunk)
	if err != nil {
		return err
	}
	defer f.Close()

	return chunkio.Read(ctx, bufio.NewReaderSize(f, readerBufferSize), &chunkio.ReadOptions{
		StorageReadOptions: *opts,
	})
}

func (r *reader) dryRead(ctx context.Context, chunk string, opts *storage.ReadOptions) (bool, error) {
	dryRunCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var first *storage.LogEntry
	err := r.read(dryRunCtx, chunk, &storage.ReadOptions{
		ResultFunc: func(e *storage.LogEntry) {
			if first == nil {
				first = e
				cancel()
			}
		},
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			select {
			case <-ctx.Done():
				return false, err
			default:
			}
		} else {
			return false, err
		}
	}
	if first == nil {
		return false, nil
	}
	if !storage.MatchLabels(first.Labels, opts.Labels) {
		return false, nil
	}
	return true, nil
}

func (r *reader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	for _, chunk := range r.Chunks {
		if strings.HasPrefix(chunk, CompactDir) {
			err := r.read(ctx, chunk, opts)
			if err != nil {
				return err
			}
		} else {
			ok, err := r.dryRead(ctx, chunk, opts)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			optsNoLabels := *opts
			optsNoLabels.Labels = nil
			err = r.read(ctx, chunk, &optsNoLabels)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
