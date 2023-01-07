package tlv

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	for _, test := range []struct {
		b   []byte
		typ uint64
		val []byte
	}{
		{
			b:   []byte{1, 0},
			typ: 1,
			val: []byte{},
		},
		{
			b:   []byte{1, 1, 2},
			typ: 1,
			val: []byte{2},
		},
	} {
		r := NewReader(bytes.NewReader(test.b))
		typ, val, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, test.typ, typ)

		valBuf := new(bytes.Buffer)
		io.Copy(valBuf, val)
		require.Equal(t, test.val, valBuf.Bytes())
	}
}

func TestReadUint64(t *testing.T) {
	for _, test := range []struct {
		b    []byte
		want uint64
		off  int64
	}{
		{
			b:    []byte{123},
			want: 123,
			off:  1,
		},
		{
			b:    []byte{253, 0, 1},
			want: 1,
			off:  3,
		},
		{
			b:    []byte{254, 0, 0, 0, 1},
			want: 1,
			off:  5,
		},
		{
			b:    []byte{255, 0, 0, 0, 0, 0, 0, 0, 1},
			want: 1,
			off:  9,
		},
	} {
		r := NewReader(bytes.NewReader(test.b)).(*reader)
		got, err := r.readUint64()
		require.NoError(t, err)
		require.Equal(t, test.want, got)
		require.Equal(t, test.off, r.off)
	}
}
