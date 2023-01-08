package logsort

import (
	"testing"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/stretchr/testify/require"
)

func mapUnixTimestamp(es []*storage.LogEntry) []int64 {
	var ts []int64
	for _, e := range es {
		ts = append(ts, e.Time.Unix())
	}
	return ts
}

func mapLogEntry(ts []int64) []*storage.LogEntry {
	var es []*storage.LogEntry
	for _, t := range ts {
		es = append(es, &storage.LogEntry{Time: time.Unix(t, 0)})
	}
	return es
}

func TestStore(t *testing.T) {
	for _, test := range []struct {
		limit   uint64
		reverse bool
		in      []int64
		want    []int64
	}{
		{
			limit:   2,
			reverse: false,
			in:      []int64{1, 4, 3, 2, 5},
			want:    []int64{1, 2},
		},
		{
			limit:   2,
			reverse: true,
			in:      []int64{1, 4, 3, 2, 5},
			want:    []int64{5, 4},
		},
	} {
		s := NewStore(test.limit, test.reverse)
		for i, e := range mapLogEntry(test.in) {
			s.Add(e)
			require.Equal(t, uint64(i+1) >= test.limit, s.IsFull())
		}
		require.Equal(t, test.want, mapUnixTimestamp(s.Get()))
		require.Equal(t, 0, s.Len())
	}
}
