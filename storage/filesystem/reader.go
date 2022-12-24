package filesystem

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/commentlens/loghouse/storage"
)

func NewReader() storage.Reader {
	return &reader{}
}

type reader struct {
}

func (r *reader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	hash, err := HashLabels(opts.Labels)
	if err != nil {
		return nil, err
	}
	timeDirs, err := os.ReadDir(DataDir)
	if err != nil {
		return nil, err
	}
	var es []*storage.LogEntry
	for _, timeDir := range timeDirs {
		if !timeDir.IsDir() {
			continue
		}
		hashDirs, err := os.ReadDir(fmt.Sprintf("%s/%s", DataDir, timeDir.Name()))
		if err != nil {
			return nil, err
		}
		for _, hashDir := range hashDirs {
			if !timeDir.IsDir() {
				continue
			}
			if hashDir.Name() != hash {
				continue
			}
			err := func() error {
				f, err := os.Open(fmt.Sprintf("%s/%s/%s/%s", DataDir, timeDir.Name(), hashDir.Name(), ChunkFile))
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
	}
	return es, nil
}

func (r *reader) Count(opts *storage.ReadOptions) (uint64, error) {
	es, err := r.Read(opts)
	if err != nil {
		return 0, err
	}
	return uint64(len(es)), nil
}
