package filesystem

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

const (
	CompactDir       = "data/compact"
	CompactMaxAge    = time.Hour
	CompactMaxSize   = 1024 * 1024 * 100
	CompactMaxLine   = 1_000_000
	CompactChunkFile = "chunk.jsonl.tmp"
)

type compactor struct{}

func (c *compactor) compact() error {
	chunks, err := ListChunks(CompactChunkFile)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		err := os.RemoveAll(chunk)
		if err != nil {
			return err
		}
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
	err = removeEmptyDir(WriteDir, CompactMaxAge)
	if err != nil {
		return err
	}
	return nil
}

func countChunkLines(chunk string) (uint64, error) {
	var count uint64
	f, err := os.Open(chunk)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		count += 1
	}
	err = scanner.Err()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func isChunkReady(chunk string) (bool, error) {
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
	count, err := countChunkLines(chunk)
	if err != nil {
		return false, err
	}
	if count >= CompactMaxLine {
		return true, nil
	}
	return false, nil
}

func swapChunk() error {
	chunks, err := ListChunks(CompactChunkFile)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		ok, err := isChunkReady(chunk)
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
