package label

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	s := NewStore(2)
	s.Add("k1", "v1")

	require.Equal(t, []string{"k1"}, s.Labels())
	require.Equal(t, []string{"v1"}, s.LabelValues("k1"))
	require.Equal(t, []string(nil), s.LabelValues("k2"))

	s.Add("k1", "v1.1")
	require.Equal(t, []string{"v1", "v1.1"}, s.LabelValues("k1"))

	s.Add("k1", "v1.2")
	require.Equal(t, []string{"v1.1", "v1.2"}, s.LabelValues("k1"))

	s.Add("k1", "v1.2")
	require.Equal(t, []string{"v1.1", "v1.2"}, s.LabelValues("k1"))

	s.Add("k2", "v2")
	require.Equal(t, []string{"k1", "k2"}, s.Labels())
	require.Equal(t, []string{"v1.1", "v1.2"}, s.LabelValues("k1"))
	require.Equal(t, []string{"v2"}, s.LabelValues("k2"))
}
