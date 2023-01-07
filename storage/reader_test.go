package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func TestMatchLogEntry(t *testing.T) {
	es := []*LogEntry{
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test1",
			},
			Time: now(),
			Data: `{"test":1}`,
		},
		{
			Labels: map[string]string{
				"app":  "test",
				"role": "test2",
			},
			Time: now().Add(time.Hour),
			Data: `{"test":2}`,
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
				Start:  now().Add(-time.Hour),
				End:    now(),
			},
			want: []*LogEntry{es[0]},
		},
		{
			name: "mismatch time",
			readOptions: &ReadOptions{
				Labels: map[string]string{},
				Start:  now().Add(time.Hour),
				End:    now().Add(-time.Hour),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got []*LogEntry
			for _, e := range es {
				if !MatchLogEntry(e, test.readOptions) {
					continue
				}
				got = append(got, e)
			}
			require.Equal(t, test.want, got)
		})
	}
}
