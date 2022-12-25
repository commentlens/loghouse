package filesystem

import (
	"bufio"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/commentlens/loghouse/storage"
)

func ListChunks(chunkFileName string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(WriteDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() != chunkFileName {
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
				es = append(es, &e)
			}
			return scanner.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	return storage.Filter(es, opts)
}

func (r *reader) Count(opts *storage.ReadOptions) (uint64, error) {
	es, err := r.Read(opts)
	if err != nil {
		return 0, err
	}
	return uint64(len(es)), nil
}
