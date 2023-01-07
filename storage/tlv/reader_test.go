package tlv

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	for _, test := range []struct {
		in  []byte
		typ uint64
		val []byte
	}{
		{
			in:  []byte{1, 0},
			typ: 1,
			val: []byte{},
		},
		{
			in:  []byte{1, 1, 2},
			typ: 1,
			val: []byte{2},
		},
	} {
		r := NewReader(bytes.NewReader(test.in))
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
		in   []byte
		want uint64
		off  int64
	}{
		{
			in:   []byte{123},
			want: 123,
			off:  1,
		},
		{
			in:   []byte{253, 0, 1},
			want: 1,
			off:  3,
		},
		{
			in:   []byte{254, 0, 0, 0, 1},
			want: 1,
			off:  5,
		},
		{
			in:   []byte{255, 0, 0, 0, 0, 0, 0, 0, 1},
			want: 1,
			off:  9,
		},
	} {
		r := NewReader(bytes.NewReader(test.in)).(*reader)
		got, err := r.readUint64()
		require.NoError(t, err)
		require.Equal(t, test.want, got)
		require.Equal(t, test.off, r.off)
	}
}
