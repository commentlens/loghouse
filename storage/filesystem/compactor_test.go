package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
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
		oldTime := time.Now().Add(-2 * CompactChunkIdlePeriod)
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
	require.Len(t, dirs, 5)
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
	require.Len(t, dirs, 5)
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
	require.Len(t, dirs, 4)
	require.Len(t, files, 2)
	dirs, files, err = dirfiles(CompactDir)
	require.NoError(t, err)
	require.Len(t, dirs, 2)
	require.Len(t, files, 2)

	indexFiles, err := findFiles(CompactDir, CompactIndexFile)
	require.NoError(t, err)
	br := newBlobReader(indexFiles)

	esRead, err := br.Read(&storage.ReadOptions{
		Labels: map[string]string{
			"role": "test5",
		},
	})
	require.NoError(t, err)
	require.Len(t, esRead, 0)

	esRead, err = br.Read(&storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esRead)
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
	esRead, err := r.Read(&storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esRead)

	ctx, cancel := context.WithTimeout(context.Background(), 2*CompactBackgroundInterval)
	defer cancel()

	err = w.BackgroundCompact(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	esRead, err = r.Read(&storage.ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, es, esRead)
}
