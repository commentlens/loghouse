package loki

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGjsonExtractLiterals(t *testing.T) {
	for _, test := range []struct {
		in   string
		want []string
	}{
		{
			in:   "name.last",
			want: []string{"last", "name"},
		},
		{
			in:   "age",
			want: []string{"age"},
		},
		{
			in:   "children.0",
			want: []string{"children"},
		},
		{
			in:   "friends.1.first",
			want: []string{"first", "friends"},
		},
		{
			in:   "child*.2",
			want: []string{"chil"},
		},
		{
			in:   "c?ildren.0",
			want: []string{"ildren"},
		},
		{
			in:   "c+ildren.0",
			want: []string{"c", "ildren"},
		},
		{
			in:   `fav\.movie`,
			want: []string{"fav.movie"},
		},
		{
			in:   `friends.#.age`,
			want: []string{"age", "friends"},
		},
	} {
		got, err := gjsonExtractLiterals(test.in)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}

func TestRegexpExtractLiterals(t *testing.T) {
	for _, test := range []struct {
		in   string
		want []string
	}{
		{
			in:   "name.last",
			want: []string{"last", "name"},
		},
		{
			in:   "age",
			want: []string{"age"},
		},
		{
			in:   "child.*2",
			want: []string{"2", "child"},
		},
		{
			in:   "c?ildren.0",
			want: []string{"0", "ildren"},
		},
		{
			in:   "c+ildren.0",
			want: []string{"0", "c", "ildren"},
		},
		{
			in:   `fav\.movie`,
			want: []string{"fav.movie"},
		},
		{
			in:   `friends.#.age`,
			want: []string{"#", "age", "friends"},
		},
		{
			in:   `(test)*(monkey){1,3}`,
			want: []string{"monkey"},
		},
		{
			in:   `(test)+(monkey){0,3}`,
			want: []string{"test"},
		},
		{
			in:   `^1$`,
			want: []string{"1"},
		},
		{
			in:   `(?i)K`,
			want: []string{"K"},
		},
	} {
		got, err := regexpExtractLiterals(test.in)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}
