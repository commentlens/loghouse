package filesystem

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/klauspost/compress/s2"
	"github.com/oklog/ulid/v2"
)

const (
	CompactDir       = "data/compact"
	CompactChunkFile = "chunk.jsonl.tmp"
	CompactIndexFile = "index"
	CompactBlobFile  = "blob"
	CompactMaxAge    = time.Hour
	CompactMaxSize   = 1024 * 1024 * 100
)

type compactor struct{}

func (*compactor) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	return readBlobs(opts)
}

func (*compactor) Compact() error {
	return compact()
}

func (*compactor) SwapChunk() error {
	return swapChunk()
}

type compactIndex struct {
	Labels       map[string]string
	Start        time.Time
	End          time.Time
	EntriesTotal uint64
	BytesStart   uint64
	BytesEnd     uint64
}

func filterIndex(indexList []*compactIndex, opts *storage.ReadOptions) ([]*compactIndex, error) {
	var out []*compactIndex
	for _, index := range indexList {
		matchLabels := true
		for k, v := range opts.Labels {
			if v2, ok := index.Labels[k]; !ok || v != v2 {
				matchLabels = false
				break
			}
		}
		if !matchLabels {
			continue
		}
		if !opts.Start.IsZero() && opts.Start.After(index.End) {
			continue
		}
		if !opts.End.IsZero() && opts.End.Before(index.Start) {
			continue
		}
		out = append(out, index)
	}
	return out, nil
}

func readBlobs(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	indexFiles, err := findFiles(CompactDir, CompactIndexFile)
	if err != nil {
		return nil, err
	}
	var out []*storage.LogEntry
	for _, indexFile := range indexFiles {
		var indexList []*compactIndex
		err := func() error {
			f, err := os.Open(indexFile)
			if err != nil {
				return err
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				var index compactIndex
				err := json.Unmarshal([]byte(scanner.Text()), &index)
				if err != nil {
					return err
				}
				indexList = append(indexList, &index)
			}
			return scanner.Err()
		}()
		if err != nil {
			return nil, err
		}
		indexList, err = filterIndex(indexList, opts)
		if err != nil {
			return nil, err
		}
		if len(indexList) == 0 {
			continue
		}
		blobFile := fmt.Sprintf("%s%s", strings.TrimSuffix(indexFile, CompactIndexFile), CompactBlobFile)
		err = func() error {
			f, err := os.Open(blobFile)
			if err != nil {
				return err
			}
			defer f.Close()
			for _, index := range indexList {
				b := make([]byte, index.BytesEnd-index.BytesStart)
				_, err := f.ReadAt(b, int64(index.BytesStart))
				if err != nil {
					return err
				}
				scanner := bufio.NewScanner(s2.NewReader(bytes.NewReader(b)))
				for scanner.Scan() {
					var e storage.LogEntry
					err := json.Unmarshal([]byte(scanner.Text()), &e)
					if err != nil {
						return err
					}
					out = append(out, &e)
				}
				err = scanner.Err()
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	return storage.Filter(out, opts)
}

func writeBlobs(chunks []string) error {
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
			err := w.Encode(e)
			if err != nil {
				return err
			}
		}
		err = enc.Close()
		if err != nil {
			return err
		}
		err = func() error {
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, compactID), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, compactID, CompactBlobFile), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
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
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, compactID), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, compactID, CompactIndexFile), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			defer f.Close()

			return json.NewEncoder(f).Encode(&compactIndex{
				Labels:       es[0].Labels,
				Start:        es[0].Time,
				End:          es[len(es)-1].Time,
				EntriesTotal: uint64(len(es)),
				BytesStart:   bytesTotal,
				BytesEnd:     bytesTotal + uint64(buf.Len()),
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

func compact() error {
	chunks, err := findFiles(WriteDir, CompactChunkFile)
	if err != nil {
		return err
	}
	err = writeBlobs(chunks)
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
	chunks, err := findFiles(WriteDir, WriteChunkFile)
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
		err = os.Rename(chunk, fmt.Sprintf("%s%s", strings.TrimSuffix(chunk, WriteChunkFile), CompactChunkFile))
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
