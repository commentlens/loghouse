package filesystem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
	"github.com/commentlens/loghouse/storage/tlv"
)

func osReadDir(dir string) ([]os.DirEntry, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return f.ReadDir(-1)
}

func findFiles(dir, name string) ([]string, error) {
	return findSortFiles(dir, name, nil)
}

type lessFunc func(int, int) bool

func findSortFiles(dir, name string, f func([]os.DirEntry) (lessFunc, error)) ([]string, error) {
	ds, err := osReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if f != nil {
		less, err := f(ds)
		if err != nil {
			return nil, err
		}
		sort.Slice(ds, less)
	}
	var paths []string
	for _, d := range ds {
		path := fmt.Sprintf("%s/%s/%s", dir, d.Name(), name)
		fi, err := os.Stat(path)
		if err == nil && !fi.IsDir() {
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

		buf := chunkio.NewBuffer()
		defer chunkio.RecycleBuffer(buf)
		buf.Reset(f)
		for {
			hdr, err := chunkio.ReadHeader(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			hdrs = append(hdrs, hdr)
		}
		return nil
	}()
	if err != nil {
		return err
	}
	var indices []io.Reader
	if len(opts.Contains) > 0 {
		f, err := os.Open(fmt.Sprintf("%s/%s", filepath.Dir(chunk), CompactIndexFile))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		defer f.Close()

		buf := chunkio.NewBuffer()
		defer chunkio.RecycleBuffer(buf)
		buf.Reset(f)
		tr := tlv.NewReader(buf)
		for {
			off, n, err := tr.ReadSection()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			indices = append(indices, io.NewSectionReader(f, int64(off), int64(n)))
		}
	}
	for i, hdr := range hdrs {
		if !chunkio.MatchHeader(hdr, opts) {
			continue
		}
		if opts.SummaryFunc != nil {
			if !opts.SummaryFunc(storage.LogSummary{
				Labels: hdr.Labels,
				Start:  hdr.Start,
				End:    hdr.End,
				Count:  hdr.Count,
			}) {
				continue
			}
		}
		if len(opts.Contains) > 0 {
			ok, err := matchIndex(indices, i, opts)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
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
			buf := chunkio.NewBuffer()
			defer chunkio.RecycleBuffer(buf)
			buf.Reset(r)
			return chunkio.ReadData(ctx, hdr, buf, opts)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func matchIndex(indices []io.Reader, i int, opts *storage.ReadOptions) (bool, error) {
	if len(indices) <= i {
		return false, fmt.Errorf("corrupted index")
	}
	buf := chunkio.NewBuffer()
	defer chunkio.RecycleBuffer(buf)
	buf.Reset(indices[i])

	index, err := chunkio.ReadIndex(buf)
	if err != nil {
		return false, err
	}
	for _, s := range opts.Contains {
		if !index.Contains([]byte(s)) {
			return false, nil
		}
	}
	return true, nil
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
