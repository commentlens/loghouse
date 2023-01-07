package tlv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	for _, test := range []struct {
		typ  uint64
		val  []byte
		want []byte
	}{
		{
			typ:  1,
			val:  []byte{},
			want: []byte{0x1, 0x0},
		},
		{
			typ:  1,
			val:  []byte{2},
			want: []byte{0x1, 0x1, 0x2},
		},
	} {
		buf := new(bytes.Buffer)
		w := NewWriter(buf)
		err := w.Write(test.typ, test.val)
		require.NoError(t, err)
		require.Equal(t, test.want, buf.Bytes())
	}
}

func TestWriteUint64(t *testing.T) {
	for _, test := range []struct {
		in   uint64
		want []byte
	}{
		{
			in:   1,
			want: []byte{1},
		},
		{
			in:   1 << 8,
			want: []byte{0xfd, 0x1, 0x0},
		},
		{
			in:   1 << 16,
			want: []byte{0xfe, 0x0, 0x1, 0x0, 0x0},
		},
		{
			in:   1 << 32,
			want: []byte{0xff, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0},
		},
	} {
		buf := new(bytes.Buffer)
		w := NewWriter(buf).(*writer)
		err := w.writeUint64(test.in)
		require.NoError(t, err)
		require.Equal(t, test.want, buf.Bytes())
	}
}
