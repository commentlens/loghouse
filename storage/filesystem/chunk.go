package filesystem

import (
	"context"
	"io"
	"sort"
	"time"

	"github.com/commentlens/loghouse/storage"
)

const (
	tlvChunkContainer = iota + 1
	tlvChunkHeader
	tlvChunkData
	tlvLabels
	tlvStart
	tlvEnd
	tlvCompression
	tlvTimeOffset
	tlvString
)

type chunkContainer struct {
	Header chunkHeader
	Data   []*chunkLogEntry
}

type chunkHeader struct {
	Labels      map[string]string
	Start       time.Time
	End         time.Time
	Compression string
}

type chunkLogEntry struct {
	Time time.Time
	Data string
}

func readChunk(ctx context.Context, r io.ReaderAt, opts *storage.ReadOptions) error {
	return nil
}

func writeChunk(w io.Writer, es []*storage.LogEntry) error {
	sort.SliceStable(es, func(i, j int) bool { return es[i].Time.Before(es[j].Time) })

	return nil
}
