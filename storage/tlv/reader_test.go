package tlv

import (
	"bufio"
	"bytes"
	"errors"
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

func TestReadSection(t *testing.T) {
	for _, test := range []struct {
		in  []byte
		off []uint64
		n   []uint64
	}{
		{
			in:  []byte{1, 0, 1, 1, 2},
			off: []uint64{0, 2},
			n:   []uint64{2, 3},
		},
	} {
		for _, rr := range []io.Reader{
			io.ReadSeeker(bytes.NewReader(test.in)),
			bufio.NewReader(bytes.NewReader(test.in)),
		} {
			r := NewReader(rr)
			for i := 0; ; i++ {
				off, n, err := r.ReadSection()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				require.Equal(t, test.off[i], off)
				require.Equal(t, test.n[i], n)
			}
		}
	}
}

func TestReadUint64(t *testing.T) {
	for _, test := range []struct {
		in   []byte
		want uint64
	}{
		{
			in:   []byte{123},
			want: 123,
		},
		{
			in:   []byte{253, 0, 1},
			want: 1,
		},
		{
			in:   []byte{254, 0, 0, 0, 1},
			want: 1,
		},
		{
			in:   []byte{255, 0, 0, 0, 0, 0, 0, 0, 1},
			want: 1,
		},
	} {
		r := NewReader(bytes.NewReader(test.in)).(*reader)
		got, err := r.readUint64()
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}
