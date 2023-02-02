package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogEntryDataValues(t *testing.T) {
	for _, test := range []struct {
		in     string
		length int
		want   []string
	}{
		{
			in:   `{"k": "1"}`,
			want: []string{"1", "k"},
		},
		{
			in:   `{"k": [1, 2, 3]}`,
			want: []string{"1", "2", "3", "k"},
		},
		{
			in:   `{"k": true}`,
			want: []string{"k", "true"},
		},
		{
			in:   `{"k": null}`,
			want: []string{"k", "null"},
		},
		{
			in:   `{"k": 1.2}`,
			want: []string{"1.2", "k"},
		},
		{
			in:   `{"k":{"k":3}}`,
			want: []string{"3", "k"},
		},
	} {
		d := LogEntryData(test.in)
		got, err := d.Values()
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}
