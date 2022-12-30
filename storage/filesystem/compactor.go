package filesystem

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/klauspost/compress/s2"
	"github.com/oklog/ulid/v2"
)

const (
	CompactDir             = "data/compact"
	CompactChunkFile       = "chunk.jsonl.tmp"
	CompactChunkIdlePeriod = 6 * time.Hour
	CompactChunkMaxSize    = 1024 * 1024 * 100
	CompactIndexFile       = "index"
	CompactBlobFile        = "blob"
	CompactBlobCompression = "s2"
)

type compactor struct{}

func (*compactor) Compact() error {
	return compact()
}

func (*compactor) SwapChunk() error {
	return swapChunk()
}

type compactIndex struct {
	Labels map[string]string
	Start  time.Time
	End    time.Time

	BlobID      string
	BytesStart  uint64
	BytesEnd    uint64
	Compression string
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

type blobReader struct {
	IndexFiles []string
}

func newBlobReader(indexFiles []string) storage.Reader {
	return &blobReader{IndexFiles: indexFiles}
}

func (r *blobReader) Read(opts *storage.ReadOptions) ([]*storage.LogEntry, error) {
	var es []*storage.LogEntry
	for _, indexFile := range r.IndexFiles {
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
				var r io.Reader = io.NewSectionReader(f, int64(index.BytesStart), int64(index.BytesEnd)-int64(index.BytesStart))
				switch index.Compression {
				case "s2":
					r = s2.NewReader(r)
				}
				scanner := bufio.NewScanner(r)
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
	sort.SliceStable(es, func(i, j int) bool { return es[i].Time.Before(es[j].Time) })
	if opts.Limit > 0 && uint64(len(es)) > opts.Limit {
		es = es[:opts.Limit]
	}
	return es, nil
}

func writeBlobs(chunks []string) error {
	blobID := ulid.Make().String()
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
		compression := CompactBlobCompression
		var w io.Writer = buf
		switch compression {
		case "s2":
			w = s2.NewWriter(w)
		}
		err = func(w io.Writer) error {
			enc := json.NewEncoder(w)
			for _, e := range es {
				err := enc.Encode(e)
				if err != nil {
					return err
				}
			}
			if wc, ok := w.(io.WriteCloser); ok {
				return wc.Close()
			}
			return nil
		}(w)
		if err != nil {
			return err
		}
		err = func() error {
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, blobID), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, blobID, CompactBlobFile), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
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
			err := os.MkdirAll(fmt.Sprintf("%s/%s", CompactDir, blobID), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(fmt.Sprintf("%s/%s/%s", CompactDir, blobID, CompactIndexFile), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			defer f.Close()

			return json.NewEncoder(f).Encode(&compactIndex{
				Labels:      es[0].Labels,
				Start:       es[0].Time,
				End:         es[len(es)-1].Time,
				BlobID:      blobID,
				BytesStart:  bytesTotal,
				BytesEnd:    bytesTotal + uint64(buf.Len()),
				Compression: compression,
			})
		}()
		if err != nil {
			return err
		}
		bytesTotal += uint64(buf.Len())
		if bytesTotal < CompactChunkMaxSize {
			continue
		}
		blobID = ulid.Make().String()
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
	err = removeEmptyDir(WriteDir, CompactChunkIdlePeriod)
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
	if time.Since(finfo.ModTime()) >= CompactChunkIdlePeriod {
		return true, nil
	}
	if finfo.Size() >= CompactChunkMaxSize {
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
