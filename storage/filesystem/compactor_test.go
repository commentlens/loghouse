package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

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
		oldTime := now().Add(-2 * CompactChunkMaxAge)
		return os.Chtimes(path, oldTime, oldTime)
	})
}

func TestCompactor(t *testing.T) {
	os.RemoveAll(WriteDir)
	os.RemoveAll(CompactDir)

	w := NewWriter()

	es := []storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test",
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
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test3",
			},
			Time: now(),
			Data: []byte(`{"test":3}`),
		},
	}
	err := w.Write(es)
	require.NoError(t, err)

	c := compactor{}
	chunks, err := c.FindCompactibleChunk()
	require.NoError(t, err)
	err = c.SwapChunk(chunks)
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	dirs, files, err := dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 4)
	require.Len(t, files, 6)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 0)
	require.Len(t, files, 0)

	err = markChunkCompactible()
	require.NoError(t, err)
	chunks, err = c.FindCompactibleChunk()
	require.NoError(t, err)
	err = c.SwapChunk(chunks)
	require.NoError(t, err)
	err = c.Compact()
	require.NoError(t, err)

	dirs, files, err = dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 4)
	require.Len(t, files, 3)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 2)
	require.Len(t, files, 3)

	err = markChunkCompactible()
	require.NoError(t, err)
	chunks, err = c.FindCompactibleChunk()
	require.NoError(t, err)
	err = c.SwapChunk(chunks)
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
	require.Len(t, files, 3)

	es2 := []storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test4",
			},
			Time: now(),
			Data: []byte(`{"test":4}`),
		},
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test5",
			},
			Time: now(),
			Data: []byte(`{"test":5}`),
		},
	}
	err = w.Write(es2)
	require.NoError(t, err)

	dirs, files, err = dirfiles(WriteDir)
	require.NoError(t, err)
	require.Len(t, dirs, 3)
	require.Len(t, files, 4)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 2)
	require.Len(t, files, 3)

	chunks, err = findFiles(CompactDir, WriteChunkFile)
	require.NoError(t, err)
	r := NewReader(chunks)

	var esReadNew []storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"role": "test5",
		},
		ResultFunc: func(e storage.LogEntry) {
			esReadNew = append(esReadNew, e)
		},
	})
	require.NoError(t, err)
	require.Len(t, esReadNew, 0)

	var esReadOld []storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
		ResultFunc: func(e storage.LogEntry) {
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
	es := []storage.LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test",
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
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test3",
			},
			Time: now(),
			Data: []byte(`{"test":3}`),
		},
	}
	err := w.Write(es)
	require.NoError(t, err)

	r := NewCompactReader(&CompactReaderOptions{
		ReaderCount: 1,
		Reverse:     false,
	})

	var esReadBefore []storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
		ResultFunc: func(e storage.LogEntry) {
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

	var esReadAfter []storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
		ResultFunc: func(e storage.LogEntry) {
			esReadAfter = append(esReadAfter, e)
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esReadAfter)

	var esReadContains []storage.LogEntry
	err = r.Read(context.Background(), &storage.ReadOptions{
		Contains: []string{`test`},
		ResultFunc: func(e storage.LogEntry) {
			esReadContains = append(esReadContains, e)
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esReadContains)
}
