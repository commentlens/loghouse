package filesystem

import (
	"fmt"
	"os"
	"testing"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	os.RemoveAll(WriteDir)

	w := NewWriter()

	es := []storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test2",
			},
			Time: now(),
			Data: []byte(`{"test":1}`),
		},
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test2",
			},
			Time: now(),
			Data: []byte(`{"test":2}`),
		},
	}
	err := w.Write(es)
	require.NoError(t, err)

	for _, e := range es {
		hash, err := storage.HashLabels(e.Labels)
		require.NoError(t, err)
		dir := fmt.Sprintf("%s/%s", WriteDir, hash)
		require.DirExists(t, dir)

		chunkFile := fmt.Sprintf("%s/%s", dir, WriteChunkFile)
		require.FileExists(t, chunkFile)
	}

	err = w.Write(es)
	require.NoError(t, err)
}
