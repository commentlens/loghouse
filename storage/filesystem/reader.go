package filesystem

import (
	"bufio"
	"encoding/json"
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
	for _, chunk := range r.Chunks {
		err := func() error {
			f, err := os.Open(chunk)
			if err != nil {
				return err
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				var e storage.LogEntry
				err := json.Unmarshal([]byte(scanner.Text()), &e)
				if err != nil {
					return err
				}
				out, err := storage.Filter([]*storage.LogEntry{&e}, opts)
				if err != nil {
					return err
				}
				es = append(es, out...)
			}
			return scanner.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	sort.SliceStable(es, func(i, j int) bool { return es[i].Time.Before(es[j].Time) })
	if opts.Limit > 0 && uint64(len(es)) > opts.Limit {
		es = es[:opts.Limit]
	}
	return es, nil
}
