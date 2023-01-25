package filesystem

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/commentlens/loghouse/storage"
	"github.com/sirupsen/logrus"
)

type CompactReaderOptions struct {
	ReaderCount int
	Reverse     bool
}

func NewCompactReader(opts *CompactReaderOptions) storage.Reader {
	return &compactReader{
		readerCount: opts.ReaderCount,
		reverse:     opts.Reverse,
	}
}

type compactReader struct {
	readerCount int
	reverse     bool
}

func (r *compactReader) read(ctx context.Context, chunks []string, opts *storage.ReadOptions) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	chIn := make(chan string)
	defer close(chIn)

	for i := 0; i < r.readerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for chunk := range chIn {
				cr := NewReader([]string{chunk})
				if r.reverse {
					var es []storage.LogEntry
					nopts := *opts
					nopts.ResultFunc = func(e storage.LogEntry) {
						es = append(es, e)
					}
					err := cr.Read(ctx, &nopts)
					if err != nil && !errors.Is(err, context.Canceled) {
						logrus.WithError(err).Warn(chunk)
					}
					for i := len(es) - 1; i >= 0; i-- {
						opts.ResultFunc(es[i])
					}
				} else {
					err := cr.Read(ctx, opts)
					if err != nil && !errors.Is(err, context.Canceled) {
						logrus.WithError(err).Warn(chunk)
					}
				}
			}
		}()
	}
	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chIn <- chunk:
		}
	}
	return nil
}

func (r *compactReader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	var dirs []string
	if r.reverse {
		dirs = []string{WriteDir, CompactDir}
	} else {
		dirs = []string{CompactDir, WriteDir}
	}
	for _, dir := range dirs {
		chunks, err := findSortFiles(dir, WriteChunkFile, func(ds []os.DirEntry) lessFunc {
			defaultLessFunc := func() lessFunc {
				if r.reverse {
					return func(i, j int) bool { return ds[i].Name() > ds[j].Name() }
				}
				return func(i, j int) bool { return ds[i].Name() < ds[j].Name() }
			}
			switch dir {
			case WriteDir:
				var mts []int64
				for _, d := range ds {
					fi, err := d.Info()
					if err != nil {
						return defaultLessFunc()
					}
					mts = append(mts, fi.ModTime().UnixMilli())
				}
				if r.reverse {
					return func(i, j int) bool { return mts[i] > mts[j] }
				}
				return func(i, j int) bool { return mts[i] < mts[j] }
			default:
				return defaultLessFunc()
			}
		})
		if err != nil {
			return err
		}
		err = r.read(ctx, chunks, opts)
		if err != nil {
			return err
		}
	}
	return nil
}
