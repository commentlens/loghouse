package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
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
		err := func() error {
			f, err := os.Open(chunk)
			if err != nil {
				return err
			}
			defer f.Close()

			return chunkio.Read(ctx, f, opts)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
