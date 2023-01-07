package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func dirfiles(dir string) ([]string, []string, error) {
	var dirs, files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return dirs, files, nil
}

func markChunkCompactible() error {
	return filepath.WalkDir(WriteDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		oldTime := time.Now().Add(-2 * CompactChunkMaxAge)
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

	dirs, files, err := dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 4)
	require.Len(t, files, 3)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 0)
	require.Len(t, files, 0)

	err = markChunkCompactible()
	require.NoError(t, err)
	err = c.SwapChunk()
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	dirs, files, err = dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 4)
	require.Len(t, files, 0)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 2)
	require.Len(t, files, 2)

	err = markChunkCompactible()
	require.NoError(t, err)
	err = c.SwapChunk()
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	dirs, files, err = dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	require.Len(t, files, 0)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 2)
	require.Len(t, files, 2)

	es2 := []*storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test4",
			},
			Time: time.Now().UTC(),
			Data: []byte(`{"test":4}`),
		},
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test5",
			},
			Time: time.Now().UTC(),
			Data: []byte(`{"test":5}`),
		},
	}
	err = w.Write(es2)
	require.NoError(t, err)

	dirs, files, err = dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 3)
	require.Len(t, files, 2)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 2)
	require.Len(t, files, 2)

	chunks, err := findFiles(CompactDir, WriteChunkFile)
	require.NoError(t, err)
	r := NewReader(chunks)

	var esReadNew []*storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"role": "test5",
		},
		ResultFunc: func(e *storage.LogEntry) {
			esReadNew = append(esReadNew, e)
		},
	})
	require.NoError(t, err)
	require.Len(t, esReadNew, 0)

	var esReadOld []*storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
		ResultFunc: func(e *storage.LogEntry) {
			esReadOld = append(esReadOld, e)
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esReadOld)
}

func TestCompactReadWriter(t *testing.T) {
	os.RemoveAll(WriteDir)
	os.RemoveAll(CompactDir)

	w := NewCompactWriter()
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

	r := NewCompactReader()
	var mu sync.Mutex

	var esReadBefore []*storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
		ResultFunc: func(e *storage.LogEntry) {
			mu.Lock()
			defer mu.Unlock()

			esReadBefore = append(esReadBefore, e)
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esReadBefore)

	err = markChunkCompactible()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = w.BackgroundCompact(ctx)
	require.ErrorIs(t, err, context.Canceled)

	err = os.RemoveAll(WriteDir)
	require.NoError(t, err)

	var esReadAfter []*storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
		ResultFunc: func(e *storage.LogEntry) {
			mu.Lock()
			defer mu.Unlock()

			esReadAfter = append(esReadAfter, e)
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esReadAfter)
}
