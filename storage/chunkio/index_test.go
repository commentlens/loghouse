package chunkio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexContains(t *testing.T) {
	var index Index
	err := index.Build([][]byte{
		[]byte(`123456`),
		[]byte(`789`),
	})
	require.NoError(t, err)
	require.True(t, index.Contains([]byte(`123`)))
	require.True(t, index.Contains([]byte(`12`)))
	require.True(t, index.Contains([]byte(`78`)))
	require.True(t, index.Contains([]byte(`789`)))
	require.True(t, !index.Contains([]byte(`012`)))
	require.True(t, !index.Contains([]byte(`00`)))
}

func TestIndexEncode(t *testing.T) {
	var index Index
	err := index.Build(nil)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	err = WriteIndex(buf, &index)
	require.NoError(t, err)

	r := NewBuffer()
	r.Reset(buf)
	index2, err := ReadIndex(r)
	require.NoError(t, err)
	require.Equal(t, &index, index2)
}
