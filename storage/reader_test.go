package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	es := []*LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test1",
			},
			Time: time.Now().UTC(),
			Data: []byte(`{"test":1}`),
		},
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test2",
			},
			Time: time.Now().UTC().Add(time.Hour),
			Data: []byte(`{"test":2}`),
		},
	}
	for _, test := range []struct {
		name        string
		readOptions *ReadOptions
		want        []*LogEntry
	}{
		{
			name: "match labels",
			readOptions: &ReadOptions{
				Labels: map[string]string{
					"app":  "test",
					"role": "test2",
				},
			},
			want: []*LogEntry{es[1]},
		},
		{
			name: "match some labels",
			readOptions: &ReadOptions{
				Labels: map[string]string{
					"app": "test",
				},
			},
			want: es,
		},
		{
			name: "mismatch labels",
			readOptions: &ReadOptions{
				Labels: map[string]string{
					"app": "test1",
				},
			},
		},
		{
			name: "mismatch some labels",
			readOptions: &ReadOptions{
				Labels: map[string]string{
					"app":  "test",
					"role": "test3",
				},
			},
		},
		{
			name: "no label",
			readOptions: &ReadOptions{
				Labels: map[string]string{},
			},
			want: es,
		},
		{
			name: "match time",
			readOptions: &ReadOptions{
				Labels: map[string]string{},
				Start:  time.Now().Add(-time.Hour),
				End:    time.Now(),
			},
			want: []*LogEntry{es[0]},
		},
		{
			name: "mismatch time",
			readOptions: &ReadOptions{
				Labels: map[string]string{},
				Start:  time.Now().Add(time.Hour),
				End:    time.Now().Add(-time.Hour),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := Filter(es, test.readOptions)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
