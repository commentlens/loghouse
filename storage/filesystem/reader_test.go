package filesystem

import (
	"os"
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	os.RemoveAll(DataDir)

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

	chunks, err := ListChunks()
	require.NoError(t, err)

	r := NewReader(chunks)
	esRead, err := r.Read(&storage.ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test2",
		},
	})
	require.NoError(t, err)
	require.Equal(t, es, esRead)

	esCount, err := r.Count(&storage.ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test2",
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(len(es)), esCount)
}
