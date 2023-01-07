package filesystem

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
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

	for _, e := range es {
		dir, err := logEntryDir(e)
		require.NoError(t, err)
		require.DirExists(t, dir)

		chunkFile := fmt.Sprintf("%s/%s", dir, WriteChunkFile)
		require.FileExists(t, chunkFile)
	}
}
