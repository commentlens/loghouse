package tlv

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	err := w.Write(1, []byte("123"))
	require.NoError(t, err)
	err = w.Write(2, []byte("test"))
	require.NoError(t, err)

	r := NewReader(bytes.NewReader(buf.Bytes()))

	typ, val, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, uint64(1), typ)
	valBuf := new(bytes.Buffer)
	io.Copy(valBuf, val)
	require.Equal(t, "123", valBuf.String())

	typ, val, err = r.Read()
	require.NoError(t, err)
	require.Equal(t, uint64(2), typ)
	valBuf.Reset()
	io.Copy(valBuf, val)
	require.Equal(t, "test", valBuf.String())
}
