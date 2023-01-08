package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
	"github.com/djherbis/times"
	"github.com/oklog/ulid/v2"
)

const (
	CompactDir            = "data/compact"
	CompactChunkFile      = "chunk.loghouse.tmp"
	CompactChunkMinAge    = 2 * time.Hour
	CompactChunkMaxAge    = 8 * time.Hour
	CompactChunkMinSize   = 1024 * 1024 * 25
	CompactChunkMaxSize   = 1024 * 1024 * 100
	CompactChunkRemoveAge = 31 * 24 * time.Hour
)

type compactor struct{}

func (*compactor) Compact() error {
	return compact()
}

func (*compactor) SwapChunk() error {
	return swapChunk()
}

func compact() error {
	chunks, err := findFiles(WriteDir, CompactChunkFile)
	if err != nil {
		return err
	}
	err = compactChunks(chunks)
	if err != nil {
		return err
	}
	err = removeEmptyDir(WriteDir, CompactChunkMaxAge)
	if err != nil {
		return err
	}
	err = removeOldChunk(CompactDir, CompactChunkRemoveAge)
	if err != nil {
		return err
	}
	return nil
}

func compactChunks(chunks []string) error {
	chunkID := ulid.Make().String()
	var bytesTotal uint64
	for _, chunk := range chunks {
		r := NewReader([]string{chunk})
		var es []*storage.LogEntry
		err := r.Read(context.Background(), &storage.ReadOptions{
			ResultFunc: func(e *storage.LogEntry) {
				es = append(es, e)
			},
		})
		if err != nil {
			return err
		}
		if len(es) == 0 {
			continue
		}

		buf := new(bytes.Buffer)
		err = chunkio.Write(buf, es, &chunkio.WriteOptions{
			Compress: true,
		})
		if err != nil {
			return err
		}
		err = func() error {
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, chunkID), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, chunkID, WriteChunkFile), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
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
		err = os.RemoveAll(chunk)
		if err != nil {
			return err
		}
		bytesTotal += uint64(buf.Len())
		if bytesTotal < CompactChunkMaxSize {
			continue
		}
		chunkID = ulid.Make().String()
		bytesTotal = 0
	}
	return nil
}

func chunkCompactible(chunk string) (uint8, error) {
	fi, err := os.Stat(chunk)
	if err != nil {
		return 0, err
	}
	fsize := fi.Size()
	if fsize >= CompactChunkMaxSize {
		return 2, nil
	}
	if fsize >= CompactChunkMinSize {
		return 1, nil
	}
	t, err := times.Stat(chunk)
	if err != nil {
		return 0, err
	}
	age := time.Since(fi.ModTime())
	if t.HasBirthTime() {
		age = time.Since(t.BirthTime())
	}
	if age >= CompactChunkMaxAge {
		return 2, nil
	}
	if age >= CompactChunkMinAge {
		return 1, nil
	}
	return 0, nil
}

func swapChunk() error {
	chunks, err := findFiles(WriteDir, WriteChunkFile)
	if err != nil {
		return err
	}
	var nowChunks, laterChunks []string
	for _, chunk := range chunks {
		status, err := chunkCompactible(chunk)
		if err != nil {
			return err
		}
		switch status {
		case 2:
			nowChunks = append(nowChunks, chunk)
		case 1:
			laterChunks = append(laterChunks, chunk)
		}
	}
	var swappable [][]string
	if len(nowChunks) > 0 {
		swappable = append(swappable, nowChunks, laterChunks)
	}
	for _, chunks := range swappable {
		for _, chunk := range chunks {
			err := os.Rename(chunk, fmt.Sprintf("%s%s", strings.TrimSuffix(chunk, WriteChunkFile), CompactChunkFile))
			if err != nil {
				return err
			}
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

func removeOldChunk(dir string, after time.Duration) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if dir == path {
			return nil
		}
		chunkID, err := ulid.ParseStrict(d.Name())
		if err != nil {
			return nil
		}
		if time.Since(time.UnixMilli(int64(chunkID.Time()))) < after {
			return nil
		}
		os.RemoveAll(path)
		return nil
	})
}
