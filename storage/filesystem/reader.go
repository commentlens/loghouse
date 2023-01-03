package filesystem

import (
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

func (r *reader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	var es []*storage.LogEntry
	var done bool
	for _, chunk := range r.Chunks {
		ok, err := func() (bool, error) {
			f, err := os.Open(chunk)
			if err != nil {
				return false, err
			}
			defer f.Close()

			esBlob, err := readBlob(f, &storage.ReadOptions{
				Limit: 1,
			})
			if err != nil {
				return false, err
			}
			if len(esBlob) != 1 {
				return false, nil
			}
			if !storage.MatchLabels(esBlob[0].Labels, opts.Labels) {
				return false, nil
			}
			if !opts.End.IsZero() && opts.End.Before(esBlob[0].Time) {
				return false, nil
			}
			return true, nil
		}()
		if err != nil {
			return nil, err
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
			esBlob, err := readBlob(f, &optsNoLabel)
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
	return es, nil
}
