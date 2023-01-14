package chunkio

import (
	"sync"

	"github.com/klauspost/compress/s2"
)

var s2Pool = sync.Pool{
	New: func() any {
		return s2.NewReader(nil)
	},
}

func newS2Reader() *s2.Reader {
	return s2Pool.Get().(*s2.Reader)
}

func recycleS2Reader(r *s2.Reader) {
	r.Reset(nil)
	s2Pool.Put(r)
}
