package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashLabels(t *testing.T) {
	for _, test := range []struct {
		labels map[string]string
		want   string
	}{
		{
			labels: map[string]string{
				"k1": "v1",
			},
			want: "d58ce7d3759b45503322e95d86f3bc7c93c1bfc71a6cc93042d84d33865b66ec",
		},
		{
			labels: map[string]string{
				"k1": "v1",
				"k2": "v2",
			},
			want: "a87bea96215f615ef8c874cf6c9d0b933e0f589af46fd99a49e6b09cc834136d",
		},
		{
			labels: map[string]string{
				"k1": "v1",
				"k2": "v2",
				"k3": "v3",
			},
			want: "08b4c4b69688375c30d21667027f1525fa50e2904756eaf2a9d53a144bb20269",
		},
	} {
		t.Run(fmt.Sprintf("%v", test.labels), func(t *testing.T) {
			h, err := HashLabels(test.labels)
			require.NoError(t, err)
			require.Equal(t, test.want, h)
		})
	}
}
