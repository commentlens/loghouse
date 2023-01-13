package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/chunkio"
	"github.com/djherbis/times"
	"github.com/oklog/ulid/v2"
)

const (
	CompactDir            = "data/compact"
	CompactTmpFile        = "chunk.loghouse.tmp"
	CompactHeaderFile     = "header.loghouse"
	CompactChunkMinAge    = 2 * time.Hour
	CompactChunkMaxAge    = 8 * time.Hour
	CompactChunkMinSize   = 1024 * 1024 * 10
	CompactChunkMaxSize   = 1024 * 1024 * 40
	CompactChunkRemoveAge = 31 * 24 * time.Hour
)

type compactor struct{}

func (*compactor) Compact() error {
	return compact()
}

func (*compactor) FindCompactibleChunk() ([]string, error) {
	return findCompactibleChunk()
}

func (*compactor) SwapChunk(chunks []string) error {
	return swapChunk(chunks)
}

func compact() error {
	chunks, err := findFiles(WriteDir, CompactTmpFile)
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
		sort.SliceStable(es, func(i, j int) bool { return es[i].Time.Before(es[j].Time) })

		buf := new(bytes.Buffer)
		err = chunkio.WriteData(buf, es, true)
		if err != nil {
			return err
		}
		err = os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, chunkID), 0777)
		if err != nil {
			return err
		}
		err = func() error {
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, chunkID, WriteChunkFile), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
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
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, chunkID, CompactHeaderFile), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			defer f.Close()

			return chunkio.WriteHeader(f, &chunkio.Header{
				OffsetStart: bytesTotal,
				Size:        uint64(buf.Len()),
				Labels:      es[0].Labels,
				Start:       es[0].Time,
				End:         es[len(es)-1].Time,
				Compression: "s2",
			})
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

func findCompactibleChunk() ([]string, error) {
	chunks, err := findFiles(WriteDir, WriteChunkFile)
	if err != nil {
		return nil, err
	}
	var nowChunks, laterChunks []string
	for _, chunk := range chunks {
		status, err := chunkCompactible(chunk)
		if err != nil {
			return nil, err
		}
		switch status {
		case 2:
			nowChunks = append(nowChunks, chunk)
		case 1:
			laterChunks = append(laterChunks, chunk)
		}
	}
	var swappable []string
	if len(nowChunks) > 0 {
		swappable = append(swappable, nowChunks...)
		swappable = append(swappable, laterChunks...)
	}
	return swappable, nil
}

func swapChunk(chunks []string) error {
	for _, chunk := range chunks {
		err := os.Rename(chunk, fmt.Sprintf("%s/%s", filepath.Dir(chunk), CompactTmpFile))
		if err != nil {
			return err
		}
	}
	return nil
}

func removeEmptyDir(dir string, after time.Duration) error {
	ds, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, d := range ds {
		func() error {
			path := fmt.Sprintf("%s/%s", dir, d.Name())
			fi, err := d.Info()
			if err != nil {
				return nil
			}
			mtime := fi.ModTime()
			if time.Since(mtime) < after {
				return nil
			}
			files, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			if len(files) > 1 {
				return nil
			}
			if len(files) == 1 && files[0].Name() != CompactHeaderFile {
				return nil
			}
			os.RemoveAll(path)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func removeOldChunk(dir string, after time.Duration) error {
	ds, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, d := range ds {
		func() error {
			path := fmt.Sprintf("%s/%s", dir, d.Name())
			chunkID, err := ulid.ParseStrict(d.Name())
			if err != nil {
				return nil
			}
			if time.Since(time.UnixMilli(int64(chunkID.Time()))) < after {
				return nil
			}
			os.RemoveAll(path)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
