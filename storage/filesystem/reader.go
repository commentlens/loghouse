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
	"strings"

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
	if strings.HasPrefix(chunk, CompactDir) {
		var hdrs []*chunkio.Header
		err := func() error {
			f, err := os.Open(fmt.Sprintf("%s%s", strings.TrimSuffix(chunk, WriteChunkFile), CompactHeaderFile))
			if err != nil {
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

				sr := io.NewSectionReader(f, int64(hdr.OffsetStart), int64(hdr.Size))
				return chunkio.ReadData(ctx, hdr, bufio.NewReaderSize(sr, chunkio.ReaderBufferSize), opts)
			}()
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		f, err := os.Open(chunk)
		if err != nil {
			return err
		}
		defer f.Close()

		r := bufio.NewReaderSize(f, chunkio.ReaderBufferSize)
		hdr, err := chunkio.ReadHeader(r)
		if err != nil {
			return err
		}
		if !storage.MatchLabels(hdr.Labels, opts.Labels) {
			return nil
		}
		return chunkio.ReadData(ctx, hdr, r, opts)
	}
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
