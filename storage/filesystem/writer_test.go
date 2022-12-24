package filesystem

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	os.RemoveAll("data/")

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
		dir, err := LogEntryDir(e)
		require.NoError(t, err)
		require.DirExists(t, dir)

		chunkFile := fmt.Sprintf("%s/chunk.jsonl", dir)
		require.FileExists(t, chunkFile)

		entryJSON, err := json.Marshal(e)
		require.NoError(t, err)
		chunkLines, err := os.ReadFile(chunkFile)
		require.NoError(t, err)
		var found bool
		for _, line := range strings.Split(string(chunkLines), "\n") {
			if line == string(entryJSON) {
				found = true
			}
		}
		require.True(t, found)
	}
}
