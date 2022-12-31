package filesystem

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"

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

func (r *reader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	var es []*storage.LogEntry
	var done bool
	for _, chunk := range r.Chunks {
		err := func() error {
			f, err := os.Open(chunk)
			if err != nil {
				return err
			}
			defer f.Close()

			esBlob, err := readBlob(f, opts)
			if err != nil {
				return err
			}
			es = append(es, esBlob...)
			if opts.Limit > 0 && uint64(len(es)) >= opts.Limit {
				es = es[:opts.Limit]
				done = true
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
	}
	sort.SliceStable(es, func(i, j int) bool { return es[i].Time.Before(es[j].Time) })
	return es, nil
}
