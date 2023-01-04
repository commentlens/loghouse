package filesystem

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/commentlens/loghouse/storage"
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

func (r *reader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	for _, chunk := range r.Chunks {
		ok, err := func() (bool, error) {
			f, err := os.Open(chunk)
			if err != nil {
				return false, err
			}
			defer f.Close()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var first *storage.LogEntry
			err = readBlob(ctx, f, &storage.ReadOptions{
				ResultFunc: func(e *storage.LogEntry) {
					if first == nil {
						first = e
						cancel()
					}
				},
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				return false, err
			}
			if first == nil {
				return false, nil
			}
			if !storage.MatchLabels(first.Labels, opts.Labels) {
				return false, nil
			}
			if !opts.End.IsZero() && opts.End.Before(first.Time) {
				return false, nil
			}
			return true, nil
		}()
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		err = func() error {
			f, err := os.Open(chunk)
			if err != nil {
				return err
			}
			defer f.Close()

			optsNoLabel := *opts
			optsNoLabel.Labels = nil
			return readBlob(ctx, f, &optsNoLabel)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
