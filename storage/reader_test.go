package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFilterLabelAll(t *testing.T) {
	es := []*LogEntry{
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

	out, err := Filter(es, &ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test2",
		},
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestFilterLabelSubset(t *testing.T) {
	es := []*LogEntry{
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

	out, err := Filter(es, &ReadOptions{
		Labels: map[string]string{
			"app": "test",
		},
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestFilterLabelSubsetMismatch(t *testing.T) {
	es := []*LogEntry{
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

	out, err := Filter(es, &ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test3",
		},
	})
	require.NoError(t, err)
	require.Len(t, out, 0)
}

func TestFilterLabelMismatch(t *testing.T) {
	es := []*LogEntry{
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

	out, err := Filter(es, &ReadOptions{
		Labels: map[string]string{
			"app": "test1",
		},
	})
	require.NoError(t, err)
	require.Len(t, out, 0)
}

func TestFilterTime(t *testing.T) {
	es := []*LogEntry{
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

	out, err := Filter(es, &ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test2",
		},
		Start: time.Now().Add(-time.Hour),
		End:   time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestFilterTimeMismatch(t *testing.T) {
	es := []*LogEntry{
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

	out, err := Filter(es, &ReadOptions{
		Labels: map[string]string{
			"app":  "test",
			"role": "test2",
		},
		Start: time.Now().Add(time.Hour),
		End:   time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	require.Len(t, out, 0)
}
