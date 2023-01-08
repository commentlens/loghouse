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
			want: "20bc39314ca03fe360c5cd4534dafc82710f8ddb995db794a90c9be456579f5f",
		},
		{
			labels: map[string]string{
				"k1": "v1",
				"k2": "v2",
			},
			want: "47072db50a7794444779f95b83af2a7d60d9f248ed84b2d87ed84c72536c23de",
		},
		{
			labels: map[string]string{
				"k1": "v1",
				"k2": "v2",
				"k3": "v3",
			},
			want: "d34123fc0c3e59753f8cf9d1fd001f37be416c28c9b912a053decc500a32e090",
		},
	} {
		t.Run(fmt.Sprintf("%v", test.labels), func(t *testing.T) {
			h, err := HashLabels(test.labels)
			require.NoError(t, err)
			require.Equal(t, test.want, h)
		})
	}
}
