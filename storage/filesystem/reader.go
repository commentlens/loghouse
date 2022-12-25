package filesystem

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/commentlens/loghouse/storage"
)

func ListChunks() ([]string, error) {
	var chunks []string
	timeDirs, err := os.ReadDir(DataDir)
	if err != nil {
		return nil, err
	}
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
			chunkFiles, err := os.ReadDir(fmt.Sprintf("%s/%s/%s", DataDir, timeDir.Name(), hashDir.Name()))
			if err != nil {
				return nil, err
			}
			for _, chunkFile := range chunkFiles {
				if chunkFile.Name() != ChunkFile {
					continue
				}
				if chunkFile.IsDir() {
					continue
				}
				chunks = append(chunks, fmt.Sprintf("%s/%s/%s/%s", DataDir, timeDir.Name(), hashDir.Name(), ChunkFile))
			}
		}
	}
	return chunks, nil
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
