package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/djherbis/times"
	"github.com/klauspost/compress/s2"
	"github.com/oklog/ulid/v2"
)

const (
	CompactDir             = "data/compact"
	CompactChunkFile       = "chunk.jsonl.tmp"
	CompactChunkMinAge     = 2 * time.Hour
	CompactChunkMaxAge     = 8 * time.Hour
	CompactChunkMinSize    = 1024 * 1024 * 25
	CompactChunkMaxSize    = 1024 * 1024 * 100
	CompactIndexFile       = "index"
	CompactBlobFile        = "blob"
	CompactBlobCompression = "s2"
	CompactBlobMaxAge      = 31 * 24 * time.Hour
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

func matchIndex(index *compactIndex, opts *storage.ReadOptions) bool {
	if !storage.MatchLabels(index.Labels, opts.Labels) {
		return false
	}
	if !opts.Start.IsZero() && opts.Start.After(index.End) {
		return false
	}
	if !opts.End.IsZero() && opts.End.Before(index.Start) {
		return false
	}
	return true
}

type blobReader struct {
	IndexFiles []string
}

func newBlobReader(indexFiles []string) storage.Reader {
	return &blobReader{IndexFiles: indexFiles}
}

func (r *blobReader) Read(ctx context.Context, opts *storage.ReadOptions) error {
	for _, indexFile := range r.IndexFiles {
		indexList, err := func() ([]*compactIndex, error) {
			f, err := os.Open(indexFile)
			if err != nil {
				return nil, err
			}
			defer f.Close()

			return readIndex(f, opts)
		}()
		if err != nil {
			return err
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
				optsNoLabel := *opts
				optsNoLabel.Labels = nil
				err := readBlob(ctx, r, &optsNoLabel)
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func compact() error {
	chunks, err := findFiles(WriteDir, CompactChunkFile)
	if err != nil {
		return err
	}
	err = writeIndexAndBlob(chunks)
	if err != nil {
		return err
	}
	err = removeEmptyDir(WriteDir, CompactChunkMaxAge)
	if err != nil {
		return err
	}
	err = removeOldBlob(CompactDir, CompactBlobMaxAge)
	if err != nil {
		return err
	}
	return nil
}

func writeIndexAndBlob(chunks []string) error {
	blobID := ulid.Make().String()
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
		compression := CompactBlobCompression
		var w io.Writer = buf
		switch compression {
		case "s2":
			w = s2.NewWriter(w)
		}
		err = func(w io.Writer) error {
			err := writeBlob(w, es)
			if err != nil {
				return err
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

			return writeIndex(f, []*compactIndex{{
				Labels:      es[0].Labels,
				Start:       es[0].Time,
				End:         es[len(es)-1].Time,
				BlobID:      blobID,
				BytesStart:  bytesTotal,
				BytesEnd:    bytesTotal + uint64(buf.Len()),
				Compression: compression,
			}})
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
		blobID = ulid.Make().String()
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

func removeOldBlob(dir string, after time.Duration) error {
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
		blobID, err := ulid.ParseStrict(d.Name())
		if err != nil {
			return nil
		}
		if time.Since(time.UnixMilli(int64(blobID.Time()))) < after {
			return nil
		}
		os.RemoveAll(path)
		return nil
	})
}
