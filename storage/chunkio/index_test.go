package chunkio

import (
	"bytes"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestHashRunes(t *testing.T) {
	for _, test := range []struct {
		in     string
		length int
		want   []string
	}{
		{
			in:     "123456",
			length: 4,
			want:   []string{"1234", "2345", "3456"},
		},
		{
			in:     "😀🥰🤗🤢🥶",
			length: 2,
			want:   []string{"😀🥰", "🥰🤗", "🤗🤢", "🤢🥶"},
		},
		{
			in:     "12345",
			length: 1,
			want:   []string{"1", "2", "3", "4", "5"},
		},
		{
			in:     "123",
			length: 3,
			want:   []string{"123"},
		},
		{
			in:     "123",
			length: 5,
			want:   []string{},
		},
		{
			in:     "",
			length: 5,
			want:   []string{},
		},
	} {
		got := make(map[uint64]struct{})
		hashRunes([]byte(test.in), test.length, got)
		want := make(map[uint64]struct{})
		for _, s := range test.want {
			want[xxhash.Sum64([]byte(s))] = struct{}{}
		}
		require.Equal(t, want, got)
	}
}

func TestIndexEncode(t *testing.T) {
	var index1 Index
	err := index1.Build([][]byte{
		[]byte(`123456`),
		[]byte(`789`),
	})
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	err = WriteIndex(buf, &index1)
	require.NoError(t, err)

	index2, err := ReadIndex(buf)
	require.NoError(t, err)
	require.Equal(t, &index1, index2)

	for _, index := range []Index{
		index1, *index2,
	} {
		for _, test := range []struct {
			in       string
			contains bool
		}{
			{
				in:       `123`,
				contains: true,
			},
			{
				in:       `12`,
				contains: true,
			},
			{
				in:       `78`,
				contains: true,
			},
			{
				in:       `789`,
				contains: true,
			},
			{
				in:       `012`,
				contains: false,
			},
			{
				in:       `00`,
				contains: false,
			},
			{
				in:       `0`,
				contains: false,
			},
			{
				in:       `1`,
				contains: true,
			},
		} {
			require.True(t, test.contains == index.Contains(test.in))
		}
	}
}
