package filesystem

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
)

func findFiles(dir, name string) ([]string, error) {
	ds, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, d := range ds {
		path := fmt.Sprintf("%s/%s/%s", dir, d.Name(), name)
		_, err := os.Stat(path)
		if err == nil {
			paths = append(paths, path)
		}
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
