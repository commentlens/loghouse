package filesystem

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	os.RemoveAll(WriteDir)

	w := NewWriter()

	es := []*storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test2",
			},
			Time: time.Now().UTC(),
			Data: []byte(`{"test":1}`),
		},
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test2",
			},
			Time: time.Now().UTC(),
			Data: []byte(`{"test":2}`),
		},
	}
	err := w.Write(es)
	require.NoError(t, err)

	chunks, err := findFiles(WriteDir, WriteChunkFile)
	require.NoError(t, err)

	r := NewReader(chunks)
	var esRead []*storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test2",
		},
		ResultFunc: func(e *storage.LogEntry) {
			esRead = append(esRead, e)
		},
	})
	require.NoError(t, err)
	require.Equal(t, es, esRead)
}
