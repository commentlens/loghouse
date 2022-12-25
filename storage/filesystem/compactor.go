package filesystem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/klauspost/compress/s2"
	"github.com/oklog/ulid/v2"
)

const (
	CompactDir       = "data/compact"
	CompactIndexDir  = "index"
	CompactBlobDir   = "blob"
	CompactMaxAge    = time.Hour
	CompactMaxSize   = 1024 * 1024 * 100
	CompactChunkFile = "chunk.jsonl.tmp"
)

type compactor struct{}

type compactIndex struct {
	Labels       map[string]string
	Start        time.Time
	End          time.Time
	EntriesTotal uint64
	BytesTotal   uint64
}

func (c *compactor) writeChunks(chunks []string) error {
	compactID := ulid.Make()
	var bytesTotal uint64
	for _, chunk := range chunks {
		r := NewReader([]string{chunk})
		es, err := r.Read(&storage.ReadOptions{})
		if err != nil {
			return err
		}
		if len(es) == 0 {
			continue
		}
		buf := new(bytes.Buffer)
		enc := s2.NewWriter(buf)
		w := json.NewEncoder(enc)
		for _, e := range es {
			err := w.Encode(e.Data)
			if err != nil {
				return err
			}
		}
		err = enc.Close()
		if err != nil {
			return err
		}
		err = func() error {
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, CompactBlobDir), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, CompactBlobDir, compactID), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = f.Write(buf.Bytes())
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
		err = func() error {
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, CompactIndexDir), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, CompactIndexDir, compactID), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			defer f.Close()

			return json.NewEncoder(f).Encode(&compactIndex{
				Labels:       es[0].Labels,
				Start:        es[0].Time,
				End:          es[len(es)-1].Time,
				EntriesTotal: uint64(len(es)),
				BytesTotal:   uint64(buf.Len()),
			})
		}()
		if err != nil {
			return err
		}
		bytesTotal += uint64(buf.Len())
		if bytesTotal < CompactMaxSize {
			continue
		}
		compactID = ulid.Make()
		bytesTotal = 0
	}
	return nil
}

func (c *compactor) compact() error {
	chunks, err := ListChunks(CompactChunkFile)
	if err != nil {
		return err
	}
	err = c.writeChunks(chunks)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		err := os.RemoveAll(chunk)
		if err != nil {
			return err
		}
	}
	err = removeEmptyDir(WriteDir, CompactMaxAge)
	if err != nil {
		return err
	}
	return nil
}

func (c *compactor) Compact() error {
	err := c.compact()
	if err != nil {
		return err
	}
	err = swapChunk()
	if err != nil {
		return err
	}
	err = c.compact()
	if err != nil {
		return err
	}
	return nil
}

func chunkCompactible(chunk string) (bool, error) {
	finfo, err := os.Stat(chunk)
	if err != nil {
		return false, err
	}
	if time.Since(finfo.ModTime()) >= time.Hour {
		return true, nil
	}
	if finfo.Size() >= CompactMaxSize {
		return true, nil
	}
	return false, nil
}

func swapChunk() error {
	chunks, err := ListChunks(WriteChunkFile)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		ok, err := chunkCompactible(chunk)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		err = os.Rename(chunk, fmt.Sprintf("%s.tmp", chunk))
		if err != nil {
			return err
		}
	}
	return nil
}

func removeEmptyDir(dir string, after time.Duration) error {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if dir == path {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		mtime := fi.ModTime()
		if time.Since(mtime) < after {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return err
	}
	for i := len(paths) - 1; i >= 0; i-- {
		files, err := os.ReadDir(paths[i])
		if err != nil {
			return err
		}
		if len(files) > 0 {
			continue
		}
		err = os.RemoveAll(paths[i])
		if err != nil {
			return err
		}
	}
	return nil
}
