package filesystem

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func dirfiles(dir string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func markChunkCompactible() error {
	return filepath.WalkDir(WriteDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		oldTime := time.Now().Add(-2 * CompactMaxAge)
		return os.Chtimes(path, oldTime, oldTime)
	})
}

func TestCompactor(t *testing.T) {
	os.RemoveAll(WriteDir)
	os.RemoveAll(CompactDir)

	w := NewWriter()

	es := []*storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test",
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
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test3",
			},
			Time: time.Now().UTC(),
			Data: []byte(`{"test":3}`),
		},
	}
	err := w.Write(es)
	require.NoError(t, err)

	c := compactor{}
	err = c.SwapChunk()
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	chunksBeforeCompact, err := dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, chunksBeforeCompact, 8)
	dataBeforeCompact, err := dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dataBeforeCompact, 0)

	err = markChunkCompactible()
	require.NoError(t, err)
	err = c.SwapChunk()
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	chunksAfterCompact, err := dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, chunksAfterCompact, 5)
	dataAfterCompact, err := dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dataAfterCompact, 5)

	err = markChunkCompactible()
	require.NoError(t, err)
	err = c.SwapChunk()
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	chunksAfterCompactAgain, err := dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, chunksAfterCompactAgain, 1)
	require.Equal(t, []string{WriteDir}, chunksAfterCompactAgain)
	dataAfterCompactAgain, err := dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dataAfterCompactAgain, 5)
}
