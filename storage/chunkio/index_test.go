package chunkio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, index1.L1, index2.L1)
	require.Equal(t, index1.L3, index2.L3)
	require.Equal(t, index1.L5, index2.L5)

	for _, test := range []Index{
		index1, *index2,
	} {
		require.True(t, test.Contains([]byte(`123`)))
		require.True(t, test.Contains([]byte(`12`)))
		require.True(t, test.Contains([]byte(`78`)))
		require.True(t, test.Contains([]byte(`789`)))
		require.True(t, !test.Contains([]byte(`012`)))
		require.True(t, !test.Contains([]byte(`00`)))
		require.True(t, !test.Contains([]byte(`0`)))
		require.True(t, test.Contains([]byte(`1`)))
	}
}
