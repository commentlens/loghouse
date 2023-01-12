package filesystem

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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

func (r *reader) read(ctx context.Context, chunk string, opts *storage.ReadOptions) error {
	var hdrs []*chunkio.Header
	err := func() error {
		f, err := os.Open(fmt.Sprintf("%s/%s", filepath.Dir(chunk), CompactHeaderFile))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		defer f.Close()

		r := bufio.NewReaderSize(f, chunkio.ReaderBufferSize)
		for {
			hdr, err := chunkio.ReadHeader(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			if !chunkio.MatchHeader(hdr, opts) {
				continue
			}
			hdrs = append(hdrs, hdr)
		}
		return nil
	}()
	if err != nil {
		return err
	}
	for _, hdr := range hdrs {
		err := func() error {
			f, err := os.Open(chunk)
			if err != nil {
				return err
			}
			defer f.Close()

			var r io.Reader = f
			if hdr.Size > 0 {
				r = io.NewSectionReader(f, int64(hdr.OffsetStart), int64(hdr.Size))
			}
			return chunkio.ReadData(ctx, hdr, bufio.NewReaderSize(r, chunkio.ReaderBufferSize), opts)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	for _, chunk := range r.Chunks {
		err := r.read(ctx, chunk, opts)
		if err != nil {
			return err
		}
	}
	return nil
}
